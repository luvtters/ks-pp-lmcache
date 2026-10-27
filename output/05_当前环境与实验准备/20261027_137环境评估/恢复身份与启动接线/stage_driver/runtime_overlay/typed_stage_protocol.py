"""Typed phase replies and bounded payload ownership; no runtime dispatcher."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

_PENDING = object()
MODEL_OPS = frozenset(("execute", "sample"))
ACK_OPS = frozenset(("prepare_input", "finalize_input", "release"))
READY_OPS = frozenset(("input_received", "input_ready", "model_ready", "release_ready"))


class ReplyKind(Enum):
    CONTROL_ACK = "control_ack"
    MODEL_RETURN = "model_return"
    DEVICE_READY = "device_ready"
    ERROR = "error"


@dataclass(frozen=True)
class StageFailure:
    error_type: str
    message: str


@dataclass(frozen=True)
class StageReply:
    command_id: str
    key: tuple[str, int]
    stage: int
    worker_id: int
    operation: str
    kind: ReplyKind
    payload: object = None

    @property
    def device_complete(self):
        return self.kind is ReplyKind.DEVICE_READY and self.payload is True

    def validate(self, model_output_types):
        if (not isinstance(self.command_id, str) or not self.command_id
                or not isinstance(self.key, tuple) or len(self.key) != 2
                or not isinstance(self.key[0], str) or not self.key[0]
                or type(self.key[1]) is not int or self.key[1] < 0
                or type(self.stage) is not int or self.stage not in (0, 1)
                or type(self.worker_id) is not int or self.worker_id < 0
                or self.operation not in MODEL_OPS | ACK_OPS | READY_OPS):
            raise ValueError("invalid typed phase reply identity")
        if self.kind is ReplyKind.MODEL_RETURN:
            if self.operation not in MODEL_OPS or (self.payload is not None and not isinstance(self.payload, model_output_types)):
                raise ValueError("model return requires real ModelRunnerOutput/AsyncModelRunnerOutput or None")
        elif self.kind is ReplyKind.DEVICE_READY:
            if self.operation not in READY_OPS or type(self.payload) is not bool:
                raise ValueError("device readiness requires an explicit device-probe bool")
        elif self.kind is ReplyKind.CONTROL_ACK:
            if self.operation not in ACK_OPS or (self.payload is not None and not isinstance(self.payload, dict)):
                raise ValueError("control acknowledgement is not a model/device result")
        elif self.kind is ReplyKind.ERROR:
            if not isinstance(self.payload, StageFailure):
                raise ValueError("error reply requires explicit failure data")
        else:
            raise ValueError("unknown reply kind")


class TypedResultLedger:
    """Keep stage payloads until actual core FIFO consumption acknowledges them."""
    def __init__(self, stage_workers, model_output_types, max_inflight_batches=2):
        self.stage_workers = tuple(tuple(group) for group in stage_workers)
        if (len(self.stage_workers) != 2 or any(not group for group in self.stage_workers)
                or len(set(sum(self.stage_workers, ()))) != sum(map(len, self.stage_workers))
                or not model_output_types or type(max_inflight_batches) is not int or max_inflight_batches < 1):
            raise ValueError("explicit two-stage workers, model output types and capacity required")
        self.model_output_types = tuple(model_output_types)
        self.max_inflight_batches = max_inflight_batches
        self._batches = {}
        self._retired = set()

    @property
    def inflight_count(self):
        return len(self._batches)

    def admit(self, key):
        if key in self._batches or key in self._retired:
            raise ValueError("duplicate logical batch")
        if self.inflight_count >= self.max_inflight_batches:
            raise RuntimeError("whole-batch result ownership slots are full")
        self._batches[key] = {"replies": {}, "errors": [], "final": _PENDING}

    def accept(self, reply):
        reply.validate(self.model_output_types)
        if reply.worker_id not in self.stage_workers[reply.stage]:
            raise ValueError("reply worker does not belong to target stage")
        batch = self._batches.get(reply.key)
        if batch is None:
            raise ValueError("unknown or retired batch reply")
        identity = (reply.command_id, reply.stage, reply.worker_id)
        if identity in batch["replies"]:
            raise ValueError("duplicate phase reply")
        batch["replies"][identity] = reply
        if reply.kind is ReplyKind.ERROR:
            batch["errors"].append(reply)

    def payloads_for(self, key):
        return tuple(reply.payload for reply in self._batches[key]["replies"].values()
                     if reply.kind is ReplyKind.MODEL_RETURN)

    def aggregate_samples(self, key, aggregate):
        batch = self._batches[key]
        if batch["errors"]:
            raise RuntimeError("failed phase retains its batch ownership")
        replies = [reply for reply in batch["replies"].values()
                   if reply.kind is ReplyKind.MODEL_RETURN and reply.operation == "sample"]
        by_rank = {reply.worker_id: reply.payload for reply in replies}
        expected = self.stage_workers[0] + self.stage_workers[1]
        if len(replies) != len(by_rank):
            raise ValueError("sample was returned more than once by a worker")
        if set(by_rank) != set(expected):
            return False
        self.record_final(key, aggregate([by_rank[worker] for worker in expected]))
        return True

    def record_final(self, key, output):
        batch = self._batches[key]
        if batch["errors"]:
            raise RuntimeError("failed phase cannot become a final model result")
        if output is None or not isinstance(output, self.model_output_types):
            raise ValueError("None/control acknowledgement is not a final model output")
        if batch["final"] is not _PENDING:
            raise ValueError("duplicate final model output")
        batch["final"] = output

    def peek_final(self):
        if not self._batches:
            return None
        key = next(iter(self._batches))
        batch = self._batches[key]
        if batch["errors"] or batch["final"] is _PENDING:
            return None
        return key, batch["final"]

    def ack_core_consumed(self, key):
        ready = self.peek_final()
        if ready is None or ready[0] != key:
            raise RuntimeError("only the actual oldest final output may be acknowledged")
        batch = self._batches.pop(key)
        self._retired.add(key)
        return batch["final"]


class StageWorkerProtocol:
    """Wrap actual worker methods; result kind never substitutes for device proof."""
    def __init__(self, worker, stage, worker_id, model_output_types):
        self.worker = worker
        self.stage = stage
        self.worker_id = worker_id
        self.model_output_types = tuple(model_output_types)
        self._samples = {}
        self._protected = {}
        self._failed = {}

    def _reply(self, command_id, key, operation, kind, payload=None):
        reply = StageReply(command_id, key, self.stage, self.worker_id, operation, kind, payload)
        reply.validate(self.model_output_types)
        return reply

    def _failure(self, command_id, key, operation, error):
        failure = StageFailure(type(error).__name__, str(error))
        self._failed[key] = failure
        return self._reply(command_id, key, operation, ReplyKind.ERROR, failure)

    def _require_live(self, key):
        if key in self._failed:
            raise RuntimeError("failed stage retains ownership; coordinated abort is required")

    def execute(self, key, scheduler_output, command_id):
        try:
            self._require_live(key)
            output = self.worker.execute_pp_stage_model(key, scheduler_output)
            return self._reply(command_id, key, "execute", ReplyKind.MODEL_RETURN, output)
        except Exception as error:
            return self._failure(command_id, key, "execute", error)

    def prepare_input(self, key, scheduler_output, command_id):
        try:
            self._require_live(key)
            ack = self.worker.prepare_pp_stage_input(key, scheduler_output)
            return self._reply(command_id, key, "prepare_input", ReplyKind.CONTROL_ACK, ack)
        except Exception as error:
            return self._failure(command_id, key, "prepare_input", error)

    def finalize_input(self, key, command_id):
        try:
            self._require_live(key)
            ack = self.worker.finalize_pp_stage_input(key)
            return self._reply(command_id, key, "finalize_input", ReplyKind.CONTROL_ACK, ack)
        except Exception as error:
            return self._failure(command_id, key, "finalize_input", error)

    def query(self, key, operation, command_id):
        methods = {"input_received": "pp_stage_input_received", "input_ready": "pp_stage_input_ready",
                   "model_ready": "pp_stage_model_ready", "release_ready": "pp_stage_release_ready"}
        try:
            value = getattr(self.worker, methods[operation])(key)
            return self._reply(command_id, key, operation, ReplyKind.DEVICE_READY, value)
        except Exception as error:
            return self._failure(command_id, key, operation, error)

    def sample(self, key, grammar_output, command_id):
        try:
            self._require_live(key)
            if key in self._samples:
                raise RuntimeError("sample is already owned for this batch")
            runner = self.worker.model_runner
            self._protected[key] = (runner.kv_connector_output, runner.execute_model_state)
            output = self.worker.sample_tokens(grammar_output)
            reply = self._reply(command_id, key, "sample", ReplyKind.MODEL_RETURN, output)
            self._samples[key] = reply
            if runner.kv_connector_output is not None or runner.execute_model_state is not None:
                raise RuntimeError("sample returned without consuming runner state")
            return reply
        except Exception as error:
            return self._failure(command_id, key, "sample", error)

    def retire_front(self, key, command_id):
        try:
            self._require_live(key)
            if self.stage != 0 or key not in self._samples:
                raise RuntimeError("front retirement requires an actual sample return")
            runner = self.worker.model_runner
            if runner.kv_connector_output is not None or runner.execute_model_state is not None:
                raise RuntimeError("runner sample/Connector state is still owned")
            if not self.worker.pp_stage_release_ready(key):
                return self._reply(command_id, key, "release_ready", ReplyKind.DEVICE_READY, False)
            self.worker.release_pp_stage_batch(key)
            return self._reply(command_id, key, "release", ReplyKind.CONTROL_ACK)
        except Exception as error:
            return self._failure(command_id, key, "release", error)

    def held_sample(self, key):
        return self._samples[key]

    def ack_core_consumed(self, key):
        """Caller must have processed this batch's actual final FIFO output."""
        self._require_live(key)
        self._samples.pop(key)
        self._protected.pop(key, None)
