"""Explicit first-token PP2 executor wire; no device APIs on the core thread.

Commands share the original broadcast writer. Typed replies use the existing
independent per-worker channel and never enter the old model response MQ.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import threading
import time

from typed_stage_protocol import ReplyKind, StageReply, StageFailure, StageWorkerProtocol, TypedResultLedger


def trace_event(phase, *, key=None, stage=None, worker_id=None, requests=(), **values):
    directory = os.environ.get("KS_STAGE_TRACE_DIR")
    if not directory:
        return
    row = {"utc": datetime.now(timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns(),
           "pid": os.getpid(), "thread_id": threading.get_ident(), "phase": phase,
           "batch": key, "stage": stage, "worker_id": worker_id,
           "requests": requests, **values}
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    with (target / f"stage-{os.getpid()}.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def trace_worker(worker, key, phase):
    record = getattr(worker, "_ks_pp_stage_current", None) or {}
    tag = record.get("stage_tag") or {}
    trace_event(phase, key=key, stage=record.get("stage"), worker_id=getattr(worker, "_ks_stage_global_rank", None),
                requests=tag.get("requests", ()), timestamp_kind="host_observed")


def restore_trace_callback(binding, controller, restore_event_type):
    observed = set()
    def callback(event):
        binding(event)
        if isinstance(event, restore_event_type):
            identity = (event.request_id, event.restore_generation, event.stage)
            restore = controller._restores.get(event.request_id)
            if (restore is not None and restore.generation == event.restore_generation
                    and restore.ready[event.stage] == set(controller.stage_workers[event.stage])
                    and identity not in observed):
                observed.add(identity)
                trace_event("stage_kv_ready_observed", stage=event.stage,
                    requests=((event.request_id, event.restore_generation),),
                    front_kv_ready=restore.ready[0] == set(controller.stage_workers[0]),
                    all_kv_ready=all(restore.ready[s] == set(controller.stage_workers[s]) for s in (0, 1)))
        return None
    return callback


@dataclass(frozen=True)
class KSStageRPCCommand:
    command_id: str
    key: tuple[str, int]
    target_stage: int
    operation: str
    scheduler_output: object = None
    grammar_output: object = None
    final_output_consumed: bool = False

    def validate(self):
        if (not self.command_id or len(self.key) != 2 or not self.key[0]
                or type(self.key[1]) is not int or self.target_stage not in (0, 1)
                or self.operation not in {"prepare_input", "finalize_input", "execute", "sample",
                    "input_received", "input_ready", "model_ready", "release_ready", "release", "ack_refs"}):
            raise ValueError("invalid typed stage command")


@dataclass(frozen=True)
class KSStageWireReply:
    reply: StageReply
    sequence: int

    @property
    def worker_id(self): return self.reply.worker_id
    @property
    def stage(self): return self.reply.stage

    def validate(self, output_types):
        self.reply.validate(output_types)
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError("invalid stage wire sequence")


def publish_typed_reply(publisher, reply):
    with publisher.lock:
        if publisher.stopped.is_set() or publisher.failed.is_set():
            raise RuntimeError("typed stage publisher is unavailable")
        sequence = publisher.sequence + 1
        try:
            publisher.pending.put_nowait(KSStageWireReply(reply, sequence))
        except queue.Full:
            publisher.failed.set()
            raise RuntimeError("typed stage inbox is full") from None
        publisher.sequence = sequence


def handle_worker_command(proc, command, output_types, async_output_type):
    """Runs on the existing worker main thread; non-targets emit no old reply."""
    command.validate()
    if proc._ks_stage_index != command.target_stage:
        return
    worker = getattr(proc.worker, "worker", None) or proc.worker
    worker._ks_stage_global_rank = proc.rank
    protocol = getattr(proc, "_ks_typed_worker_protocol", None)
    if protocol is None:
        protocol = StageWorkerProtocol(worker, proc._ks_stage_index, proc.rank, output_types)
        proc._ks_typed_worker_protocol = protocol
    key, identity, operation = command.key, command.command_id, command.operation
    try:
        if operation == "prepare_input":
            reply = protocol.prepare_input(key, command.scheduler_output, identity)
        elif operation == "finalize_input":
            reply = protocol.finalize_input(key, identity)
        elif operation == "execute":
            reply = protocol.execute(key, command.scheduler_output, identity)
        elif operation == "sample":
            reply = protocol.sample(key, command.grammar_output, identity)
        elif operation == "release_ready":
            value = worker.pp_stage_release_ready(key, final_output_consumed=command.final_output_consumed)
            reply = StageReply(identity, key, proc._ks_stage_index, proc.rank,
                               operation, ReplyKind.DEVICE_READY, value)
        elif operation == "release":
            if proc._ks_stage_index == 0:
                reply = protocol.retire_front(key, identity)
            else:
                worker.release_pp_stage_batch(key, final_output_consumed=command.final_output_consumed)
                reply = StageReply(identity, key, 1, proc.rank, operation, ReplyKind.CONTROL_ACK)
        elif operation == "ack_refs":
            protocol.ack_core_consumed(key)
            reply = StageReply(identity, key, proc._ks_stage_index, proc.rank,
                               "release", ReplyKind.CONTROL_ACK)
        else:
            reply = protocol.query(key, operation, identity)
        # Use the real normal output conversion, not an ACK or a mock model
        # output. Keep the original Async output in protocol ownership until ACK.
        if reply.kind is ReplyKind.MODEL_RETURN and isinstance(reply.payload, async_output_type):
            reply = replace(reply, payload=reply.payload.get_output())
            trace_event("sample_async_output_resolved", key=key, stage=command.target_stage,
                        worker_id=proc.rank, model_output_type=type(reply.payload).__name__)
        reply.validate(output_types)
    except Exception as error:
        reply = StageReply(identity, key, proc._ks_stage_index, proc.rank,
                           "release" if operation == "ack_refs" else operation,
                           ReplyKind.ERROR, StageFailure(type(error).__name__, str(error)))
    publish_typed_reply(proc._ks_stage_publisher, reply)


class DeferredRPCFuture(Future):
    """Preserve a legacy RPC until all staged worker buffer owners retire."""
    def __init__(self):
        super().__init__()
        self.submitted = threading.Event()
        self.delegate = None

    def bind(self, delegate):
        self.delegate = delegate
        self.submitted.set()

    def result(self, timeout=None):
        if not self.submitted.wait(timeout):
            raise TimeoutError("legacy RPC is waiting for staged ownership retirement")
        if isinstance(self.delegate, Future):
            return self.delegate.result(timeout=timeout)
        return self.delegate


@dataclass
class _Batch:
    key: tuple[str, int]
    output: object
    execute_future: Future = field(default_factory=Future)
    sample_future: Future = field(default_factory=Future)
    grammar: object = None
    sample_requested: bool = False
    commands: dict = field(default_factory=dict)
    phase: str = "waiting"
    model_done: set = field(default_factory=set)
    sample_done: set = field(default_factory=set)
    buffer_retired: set = field(default_factory=set)
    core_consumed: bool = False
    final_recorded: bool = False


class FirstTokenStageDriver:
    def __init__(self, executor, controller, output_types, *, async_scheduling):
        if async_scheduling is not True:
            raise ValueError("first-token stage wire requires confirmed actual async_scheduling=True")
        self.executor, self.controller = executor, controller
        self.output_types = tuple(output_types)
        self.ledger = TypedResultLedger(controller.stage_workers, output_types, controller.max_inflight_batches)
        self.batches, self.pending = {}, {}
        self.counter = 0
        self.owner = threading.get_ident()
        self.sample_bindings, self.deferred = deque(), deque()
        self.failed = None

    def _owner(self):
        if threading.get_ident() != self.owner:
            raise RuntimeError("stage driver must remain on its core thread")
        if self.failed is not None:
            raise RuntimeError("stage wire failed; retain all batch/device ownership") from self.failed

    def admit(self, output):
        self._owner()
        tag = output.ks_stage_batch
        if (len(tag["requests"]) != 1 or output.total_num_scheduled_tokens != 1
                or output.scheduled_spec_decode_tokens or output.pending_structured_output_tokens):
            raise ValueError("stage wire accepts one first-token request, no scheduled spec/grammar")
        key = (tag["batch_id"], tag["generation"])
        self.controller.admit_batch(key[0], key[1], tag["requests"], tag["token_positions"])
        self.ledger.admit(key)
        batch = _Batch(key, output)
        self.batches[key] = batch
        self.sample_bindings.append(key)
        restore = self.controller._restores[tag["requests"][0][0]]
        trace_event("stage_batch_admitted", key=key, requests=tag["requests"],
                    front_kv_ready=restore.ready[0] == set(self.controller.stage_workers[0]),
                    all_kv_ready=all(restore.ready[s] == set(self.controller.stage_workers[s]) for s in (0, 1)))
        self.pump()
        return batch.execute_future

    def request_sample(self, grammar_output):
        self._owner()
        if not self.sample_bindings:
            raise RuntimeError("sample has no matching stage execute")
        key = self.sample_bindings.popleft()
        batch = self.batches[key]
        if batch.sample_requested:
            raise RuntimeError("duplicate sample request")
        batch.sample_requested, batch.grammar = True, grammar_output
        self.pump()
        return batch.sample_future

    def defer_legacy(self, submit):
        future = DeferredRPCFuture()
        self.deferred.append((future, submit))
        return future

    def _send(self, batch, stage, operation, **kwargs):
        self.counter += 1
        identity = f"typed:{self.counter}:{operation}:{stage}"
        command = KSStageRPCCommand(identity, batch.key, stage, operation, **kwargs)
        expected = set(self.controller.stage_workers[stage])
        state = {"command": command, "expected": expected, "replies": {}}
        self.pending[identity] = state
        batch.commands[(stage, operation)] = state
        if operation == "execute":
            trace_event("stage_execute_submitted", key=batch.key, stage=stage,
                        requests=batch.output.ks_stage_batch["requests"], timestamp_kind="cpu_submit_not_device_compute")
        self.executor.rpc_broadcast_mq.enqueue((command, (), {}, None))
        return state

    def accept(self, envelope):
        self._owner()
        envelope.validate(self.output_types)
        reply = envelope.reply
        state = self.pending.get(reply.command_id)
        if state is None or reply.worker_id not in state["expected"] or reply.worker_id in state["replies"]:
            raise ValueError("unknown, duplicate or wrong-rank typed reply")
        command = state["command"]
        expected_operation = "release" if command.operation == "ack_refs" else command.operation
        if reply.key != command.key or reply.stage != command.target_stage or reply.operation != expected_operation:
            raise ValueError("typed reply does not match its actual command/batch")
        state["replies"][reply.worker_id] = reply
        if reply.kind in (ReplyKind.MODEL_RETURN, ReplyKind.ERROR):
            self.ledger.accept(reply)
            if reply.kind is ReplyKind.MODEL_RETURN and reply.operation == "sample":
                trace_event("sample_model_output_received", key=reply.key, stage=reply.stage,
                            worker_id=reply.worker_id, model_output_type=type(reply.payload).__name__)
        if reply.kind is ReplyKind.ERROR:
            self.failed = RuntimeError(f"{reply.payload.error_type}: {reply.payload.message}")
            batch = self.batches[reply.key]
            for future in (batch.execute_future, batch.sample_future):
                if not future.done(): future.set_exception(self.failed)
        if self._complete(state):
            del self.pending[reply.command_id]

    @staticmethod
    def _complete(state):
        return bool(state) and set(state["replies"]) == state["expected"]

    def _done(self, batch, stage, operation):
        return self._complete(batch.commands.get((stage, operation)))

    def _query(self, batch, stage, operation, **kwargs):
        state = batch.commands.get((stage, operation))
        if state is None:
            self._send(batch, stage, operation, **kwargs)
            return False
        if not self._complete(state): return False
        if all(reply.device_complete for reply in state["replies"].values()): return True
        self._send(batch, stage, operation, **kwargs)
        return False

    def _aggregate(self, outputs):
        aggregator = self.executor.kv_output_aggregator
        if aggregator is not None:
            return aggregator.aggregate(outputs, output_rank=self.executor.output_rank)
        return outputs[self.executor.output_rank]

    def pump(self):
        self._owner()
        for batch in list(self.batches.values()):
            if batch.phase == "waiting":
                # One real receive buffer per back stage. Preserve both whole
                # batch slots; never overwrite another batch's activation owner.
                if self.controller._busy[1] is not None:
                    continue
                work = self.controller.take_ready(0)
                if work is None: continue
                if work.key != batch.key: raise RuntimeError("controller/front FIFO mismatch")
                self._send(batch, 1, "prepare_input", scheduler_output=batch.output)
                self._send(batch, 0, "execute", scheduler_output=batch.output)
                batch.phase = "front"
            if batch.phase == "front":
                if self._done(batch, 1, "prepare_input") and self._query(batch, 1, "input_received"):
                    if (1, "finalize_input") not in batch.commands:
                        self._send(batch, 1, "finalize_input")
                if self._done(batch, 1, "finalize_input") and self._query(batch, 1, "input_ready"):
                    for rank in self.controller.stage_workers[1]:
                        self.controller.mark_activation_received(batch.key, 1, rank, device_complete=True)
                if self._done(batch, 0, "execute") and self._query(batch, 0, "model_ready"):
                    for rank in self.controller.stage_workers[0]:
                        self.controller.mark_model_complete(batch.key, 0, rank, device_complete=True)
                    batch.model_done.add(0)
                if 0 in batch.model_done:
                    work = self.controller.take_ready(1)
                    if work is not None:
                        if work.key != batch.key: raise RuntimeError("controller/back FIFO mismatch")
                        trace_event("all_kv_ready_back_dispatch_eligible", key=batch.key, stage=1,
                                    requests=batch.output.ks_stage_batch["requests"], activation_ready=True)
                        self._send(batch, 1, "execute", scheduler_output=batch.output)
                        batch.phase = "back"
            if batch.phase == "back":
                if self._done(batch, 1, "execute") and not batch.execute_future.done():
                    # This is the real output-rank execute return, commonly
                    # None before sampling. Never substitute a CONTROL_ACK.
                    raw = batch.commands[(1, "execute")]["replies"][self.executor.output_rank].payload
                    batch.execute_future.set_result(raw)
                if self._done(batch, 1, "execute") and self._query(batch, 1, "model_ready"):
                    for rank in self.controller.stage_workers[1]:
                        self.controller.mark_model_complete(batch.key, 1, rank, device_complete=True)
                    batch.model_done.add(1)
                if 1 in batch.model_done and batch.sample_requested:
                    # Both calls are sent before waiting: the front's normal
                    # async sampled-token receive may wait for back sampling.
                    if (0, "sample") not in batch.commands:
                        self._send(batch, 0, "sample", grammar_output=batch.grammar)
                        self._send(batch, 1, "sample", grammar_output=batch.grammar)
                    if all(self._done(batch, stage, "sample") for stage in (0, 1)):
                        for stage in (0, 1):
                            for rank in self.controller.stage_workers[stage]:
                                self.controller.mark_sample_consumed(batch.key, stage, rank)
                        if not batch.final_recorded:
                            if not self.ledger.aggregate_samples(batch.key, self._aggregate):
                                raise RuntimeError("all-rank sample replies were not retained")
                            ready = self.ledger.peek_final()
                            if ready is None or ready[0] != batch.key:
                                raise RuntimeError("final sample violated whole-batch FIFO")
                            self.controller.mark_final_result(batch.key, ready[1])
                            batch.final_recorded = True
                            batch.sample_future.set_result(ready[1])
            if batch.core_consumed:
                for stage in (0, 1):
                    if stage in batch.buffer_retired: continue
                    if self._query(batch, stage, "release_ready", final_output_consumed=True):
                        if (stage, "release") not in batch.commands:
                            self._send(batch, stage, "release", final_output_consumed=True)
                        elif self._done(batch, stage, "release"):
                            for rank in self.controller.stage_workers[stage]:
                                self.controller.mark_stage_buffer_retired(batch.key, stage, rank, device_complete=True)
                            batch.buffer_retired.add(stage)
                            self._send(batch, stage, "ack_refs")
                if batch.buffer_retired == {0, 1} and all(self._done(batch, stage, "ack_refs") for stage in (0, 1)):
                    self.ledger.ack_core_consumed(batch.key)
                    commit = self.controller.pop_committable()
                    if commit is None or commit.key != batch.key:
                        raise RuntimeError("whole-batch ownership was not actually retired")
                    del self.batches[batch.key]
        if not self.batches:
            while self.deferred:
                future, submit = self.deferred.popleft()
                future.bind(submit())

    def core_consumed(self, output, actual_model_output):
        self._owner()
        tag = getattr(output, "ks_stage_batch", None)
        if tag is None: return
        key = (tag["batch_id"], tag["generation"])
        batch = self.batches[key]
        if not batch.sample_future.done() or batch.sample_future.result() is not actual_model_output:
            raise RuntimeError("core acknowledgment is not this batch's real final Future output")
        self.controller.mark_core_consumed(key)
        batch.core_consumed = True
        trace_event("core_fifo_model_output_consumed", key=key,
                    requests=batch.output.ks_stage_batch["requests"], model_output_type=type(actual_model_output).__name__)
        self.pump()
