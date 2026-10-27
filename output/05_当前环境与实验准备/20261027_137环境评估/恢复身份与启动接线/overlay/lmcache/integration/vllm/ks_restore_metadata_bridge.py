"""Isolated CPU bridge; not installed in a Connector or EngineCore startup path."""
from __future__ import annotations

from dataclasses import dataclass

SIDECAR = "_ks_restore_tickets"


def operation_signature(op):
    """Keep real LoadStoreOp content, including APC skip and group boundaries."""
    blocks = op.block_ids
    groups = (tuple(blocks),) if blocks and isinstance(blocks[0], int) else tuple(tuple(group) for group in blocks)
    return (tuple(op.token_ids), groups, op.start, op.end, op.skip_first_n_tokens)


def _retrieves(metadata):
    requests = [item for item in metadata.requests if item.direction == "RETRIEVE"]
    ids = [item.request_id for item in requests]
    if any(not isinstance(value, str) or not value for value in ids) or len(set(ids)) != len(ids):
        raise ValueError("each retrieve must have one distinct real request ID")
    return requests


def _tickets(metadata, requests):
    tickets = getattr(metadata, SIDECAR, None)
    if not isinstance(tickets, dict) or set(tickets) != {item.request_id for item in requests}:
        raise ValueError("scheduler restore metadata sidecar is missing or mismatched")
    for item in requests:
        ticket = tickets[item.request_id]
        if (not isinstance(ticket, dict) or type(ticket.get("generation")) is not int
                or ticket["generation"] < 0
                or ticket.get("signature") != operation_signature(item.op)):
            raise ValueError("restore generation or original operation content changed")
    return tickets


class SchedulerRestoreLedger:
    """One ledger per scheduler lifetime; the scheduler supplies begin_restore."""
    def __init__(self):
        self._next_generation = 0
        self._active = {}

    def stamp(self, metadata, begin_restore):
        requests = _retrieves(metadata)
        if hasattr(metadata, SIDECAR):
            tickets = _tickets(metadata, requests)
            if any(self._active.get(item.request_id) != tickets[item.request_id]
                   for item in requests):
                raise ValueError("stale or cancelled metadata cannot register a restore")
            return {key: value["generation"] for key, value in tickets.items()}
        if any(item.request_id in self._active for item in requests):
            raise ValueError("previous restore remains active; explicit lifecycle completion is required")
        tickets = {}
        for item in requests:
            generation = self._next_generation
            self._next_generation += 1
            # Runs before metadata can leave the scheduler process.
            begin_restore(item.request_id, generation)
            ticket = {"generation": generation, "signature": operation_signature(item.op)}
            tickets[item.request_id] = ticket
            self._active[item.request_id] = dict(ticket)
        setattr(metadata, SIDECAR, tickets)
        return {key: value["generation"] for key, value in tickets.items()}

    def cancel(self, request_id, generation):
        active = self._active.get(request_id)
        if active is None or active["generation"] != generation:
            return False
        del self._active[request_id]
        return True


@dataclass(frozen=True)
class WorkerRanks:
    global_rank: int
    stage: int
    tp_rank: int
    pcp_rank: int

    @classmethod
    def from_parallel_groups(cls, vllm_config, parallel_strategy, groups=None):
        """Read initialized vLLM group interfaces; never use KV-shard worker_id."""
        if groups is None:
            from vllm.distributed import parallel_state as groups
        pc = vllm_config.parallel_config
        world, pp, tp = groups.get_world_group(), groups.get_pp_group(), groups.get_tp_group()
        if pc.prefill_context_parallel_size != 1 or pc.pipeline_parallel_size != 2:
            raise ValueError("this isolated bridge requires PP2 and PCP1")
        expected = (pc.world_size, pc.rank, pc.tensor_parallel_size, pc.pipeline_parallel_size)
        strategy = (parallel_strategy.vllm_world_size, parallel_strategy.vllm_worker_id,
                    parallel_strategy.tp_size, parallel_strategy.pp_size)
        actual = (world.world_size, world.rank, tp.world_size, pp.world_size)
        if actual != expected or strategy != expected or pc.world_size != tp.world_size * pp.world_size:
            raise ValueError("worker config, actual groups and adapter strategy disagree")
        if (type(world.rank) is not int or type(pp.rank_in_group) is not int
                or type(tp.rank_in_group) is not int
                or not 0 <= pp.rank_in_group < 2
                or not 0 <= tp.rank_in_group < tp.world_size
                or world.rank != pp.rank_in_group * tp.world_size + tp.rank_in_group):
            raise ValueError("unsupported global/PP/TP rank layout")
        return cls(world.rank, pp.rank_in_group, tp.rank_in_group, 0)


class WorkerRestoreBinding:
    """Bind the same broadcast metadata before adapter batched submission."""
    def __init__(self, ranks, identity_type):
        self.ranks = ranks
        self.identity_type = identity_type
        self._known = {}
        self._operations = {}
        self._cancelled = set()

    def bind_metadata(self, metadata):
        requests = _retrieves(metadata)
        tickets = _tickets(metadata, requests)
        planned = {}
        for item in requests:
            ticket = tickets[item.request_id]
            generation = ticket["generation"]
            previous = self._known.get(item.request_id)
            if ((item.request_id, generation) in self._cancelled
                    or (previous is not None and generation < previous["generation"])
                    or (previous is not None and generation == previous["generation"] and previous != ticket)):
                raise ValueError("worker rejected stale or changed restore metadata")
            planned[item.request_id] = dict(ticket)
        self._known.update(planned)
        self._operations.update({item.request_id: item.op for item in requests})

    def __call__(self, request_id, op):
        ticket = self._known.get(request_id)
        if (ticket is None or (request_id, ticket["generation"]) in self._cancelled
                or self._operations.get(request_id) is not op
                or ticket["signature"] != operation_signature(op)):
            raise ValueError("retrieve does not match its current scheduler restore metadata")
        return self.identity_type(
            request_id=request_id, restore_generation=ticket["generation"],
            stage_id=self.ranks.stage, tp_rank=self.ranks.tp_rank,
            pcp_rank=self.ranks.pcp_rank, global_worker_rank=self.ranks.global_rank)

    def cancel(self, request_id, generation):
        self._cancelled.add((request_id, generation))


def configure_constructed_adapter(adapter, binding, observer_type, proxy_factory,
                                  device_context_factory):
    """Proposed explicit init call; no automatic publisher or runtime install."""
    observer = observer_type(proxy_factory, device_context_factory)
    adapter.configure_ks_stage_observer(observer, binding)
    observer.start()
    return observer
