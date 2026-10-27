"""Build an opt-in, single-node candidate from the archived vLLM 0.23 file.

Only writes adjacent files; never imports vLLM, torch or contacts a server.
"""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = (HERE.parents[3] / "05_当前环境与实验准备" /
          "20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/executor/multiproc_executor.py")

# All inserted runtime definitions live in the real executor module, avoiding
# extra packages and avoiding cross-module pickle identity ambiguity.
DEFINITIONS = '''
# KS candidate: NOT_RUNTIME_INTEGRATED. No device/LMCache producer is installed.
_KS_STAGE_METHODS = {
    "input_received": "pp_stage_input_received",
    "input_ready": "pp_stage_input_ready",
    "model_ready": "pp_stage_model_ready",
    "release_ready": "pp_stage_release_ready",
    "release": "release_pp_stage_batch",
}
_KS_EVENT_KINDS = frozenset(("CONTROL_RESULT", "CONTROL_ERROR",
                           "KV_DEVICE_READY", "INPUT_DEVICE_READY",
                           "STAGE_DEVICE_DONE"))


def _ks_identity(event_id, batch_id, generation, stage):
    if not (type(event_id) is str and 0 < len(event_id) <= 128
            and type(batch_id) is str and 0 < len(batch_id) <= 128
            and type(generation) is int and generation >= 0
            and type(stage) is int and stage >= 0):
        raise ValueError("invalid stage event/command identity")


@dataclass(frozen=True)
class KSStageCommand:
    command_id: str
    batch_id: str
    generation: int
    target_stage: int
    operation: str
    final_output_consumed: bool = False

    def validate(self):
        _ks_identity(self.command_id, self.batch_id, self.generation,
                     self.target_stage)
        if (self.operation not in _KS_STAGE_METHODS
                or type(self.final_output_consumed) is not bool):
            raise ValueError("unsupported stage control command")


@dataclass(frozen=True)
class KSStageEvent:
    event_id: str
    batch_id: str
    generation: int
    stage: int
    worker_id: int
    sequence: int
    kind: str
    value: bool | str | None = None

    def validate(self):
        _ks_identity(self.event_id, self.batch_id, self.generation, self.stage)
        if (type(self.worker_id) is not int or self.worker_id < 0
                or type(self.sequence) is not int or self.sequence < 1
                or self.kind not in _KS_EVENT_KINDS
                or type(self.value) not in (bool, str, type(None))
                or (type(self.value) is str and len(self.value) > 512)):
            raise ValueError("invalid stage event payload")


class _KSStageEventPublisher:
    """One writer thread per worker; never runs model/HCCL/Future queries.

    Any future host observer must enqueue a previously obtained readiness fact.
    It must NOT query the same LMCache Future as an existing completion path.
    """
    def __init__(self, transport, failed, worker_id, stage):
        self.transport = transport
        self.failed = failed
        self.worker_id = worker_id
        self.stage = stage
        self.pending = queue.Queue(maxsize=64)
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.sequence = 0
        self.thread = Thread(target=self._run, daemon=True,
                             name="KSStageEventWriter")
        self.thread.start()

    def publish(self, event_id, batch_id, generation, kind, value=None):
        with self.lock:
            if self.stopped.is_set() or self.failed.is_set():
                raise RuntimeError("stage event channel unavailable")
            event = KSStageEvent(event_id, batch_id, generation, self.stage,
                                 self.worker_id, self.sequence + 1, kind, value)
            event.validate()
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.failed.set()
                raise RuntimeError("stage event inbox full") from None
            self.sequence += 1

    def _run(self):
        while not self.stopped.is_set():
            try:
                event = self.pending.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self.transport.put_nowait(event)
            except (queue.Full, OSError, ValueError):
                self.failed.set()
                return

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=0.2)
        # Never join a multiprocessing feeder whose reader may have exited.
        self.transport.cancel_join_thread()
        self.transport.close()


'''

EXECUTOR_METHODS = '''
    def _init_stage_event_channels(self, context, tp_size, pp_size, pcp_size):
        self._ks_stage_channels = {}
        self._ks_stage_sequences = {}
        self._ks_stage_pending = {}
        self._ks_stage_poll_owner = None
        self._ks_stage_rpc_owner = threading.get_ident()
        self._ks_stage_tp_size = tp_size * pcp_size
        self._ks_stage_count = pp_size
        if os.environ.get("KS_PP_STAGE_EVENTS") != "1":
            return
        if (self.parallel_config.nnodes_within_dp != 1
                or self.local_world_size != self.world_size):
            raise ValueError("KS stage sidechannel supports single-node only")
        for rank in range(self.world_size):
            self._ks_stage_channels[rank] = (context.Queue(maxsize=64),
                                              context.Event())

    def submit_stage_command(self, command):
        if not self._ks_stage_channels:
            raise RuntimeError("KS stage sidechannel is disabled")
        if threading.get_ident() != self._ks_stage_rpc_owner:
            raise RuntimeError("stage and ordinary RPC must share the core writer")
        if self.is_failed:
            raise RuntimeError("Executor failed.")
        if not isinstance(command, KSStageCommand):
            raise ValueError("expected KSStageCommand")
        command.validate()
        if command.target_stage >= self._ks_stage_count:
            raise ValueError("unknown PP stage")
        if command.command_id in self._ks_stage_pending:
            raise ValueError("duplicate pending stage command id")
        ranks = frozenset(rank for rank in self._ks_stage_channels
                          if rank // self._ks_stage_tp_size == command.target_stage)
        if not ranks:
            raise ValueError("target stage has no workers")
        self._ks_stage_pending[command.command_id] = [command, set(ranks)]
        try:
            # Same single broadcast writer; no FutureWrapper/model FIFO entry.
            self.rpc_broadcast_mq.enqueue((command, (), {}, None))
        except Exception:
            del self._ks_stage_pending[command.command_id]
            raise
        return ranks

    def poll_stage_events(self, max_per_worker=8):
        if type(max_per_worker) is not int or max_per_worker < 1:
            raise ValueError("max_per_worker must be positive")
        owner = threading.get_ident()
        if self._ks_stage_poll_owner not in (None, owner):
            raise RuntimeError("only one core reader may consume stage events")
        self._ks_stage_poll_owner = owner
        events = []
        for rank, (transport, failed) in self._ks_stage_channels.items():
            if failed.is_set():
                raise RuntimeError(f"stage event channel failed for worker {rank}")
            for _ in range(max_per_worker):
                try:
                    event = transport.get_nowait()
                except queue.Empty:
                    break
                if not isinstance(event, KSStageEvent):
                    raise ValueError("unexpected stage channel payload")
                event.validate()
                if (event.worker_id != rank
                        or event.stage != rank // self._ks_stage_tp_size
                        or event.sequence != self._ks_stage_sequences.get(rank, 0) + 1):
                    raise ValueError("stage event worker/sequence mismatch")
                if event.kind in ("CONTROL_RESULT", "CONTROL_ERROR"):
                    pending = self._ks_stage_pending.get(event.event_id)
                    if pending is None:
                        raise ValueError("unknown or duplicate stage command result")
                    command, ranks = pending
                    if (event.batch_id != command.batch_id
                            or event.generation != command.generation
                            or event.stage != command.target_stage or rank not in ranks):
                        raise ValueError("stage command result identity mismatch")
                    ranks.remove(rank)
                    if not ranks:
                        del self._ks_stage_pending[event.event_id]
                self._ks_stage_sequences[rank] = event.sequence
                events.append(event)
        return events

    def _close_stage_event_channels(self):
        for transport, _ in getattr(self, "_ks_stage_channels", {}).values():
            transport.cancel_join_thread()
            transport.close()
        self._ks_stage_channels = {}
        self._ks_stage_pending = {}

'''

WORKER_METHODS = '''
    def _init_stage_event_channel(self, stage_event_channel, vllm_config):
        self._ks_stage_publisher = None
        if stage_event_channel is None:
            return
        parallel = vllm_config.parallel_config
        self._ks_stage_index = self.rank // (parallel.tensor_parallel_size
                                            * parallel.prefill_context_parallel_size)
        transport, failed = stage_event_channel
        self._ks_stage_publisher = _KSStageEventPublisher(
            transport, failed, self.rank, self._ks_stage_index)
        # Interface only. No LMCache/stream/event observer is installed here.
        self.worker.ks_publish_stage_event = self._ks_stage_publisher.publish

    def _handle_stage_command(self, command):
        publisher = self._ks_stage_publisher
        if publisher is None:
            raise RuntimeError("stage command received without sidechannel")
        command.validate()
        if self._ks_stage_index != command.target_stage:
            return
        try:
            method = getattr(self.worker, _KS_STAGE_METHODS[command.operation])
            batch_key = (command.batch_id, command.generation)
            if command.operation in ("release_ready", "release"):
                value = method(batch_key,
                    final_output_consumed=command.final_output_consumed)
            else:
                value = method(batch_key)
            if type(value) not in (bool, type(None)):
                raise TypeError("stage controls may return only bool or None")
        except Exception as error:
            publisher.publish(command.command_id, command.batch_id,
                              command.generation, "CONTROL_ERROR",
                              f"{type(error).__name__}: {error}"[:512])
            return
        publisher.publish(command.command_id, command.batch_id,
                          command.generation, "CONTROL_RESULT", value)

'''


def replace_once(text, old, new):
    count = text.count(old)
    if count != 1:
        raise ValueError(f"source boundary mismatch ({count} matches): {old[:100]!r}")
    return text.replace(old, new, 1)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    text = replace_once(original, "class FutureWrapper(Future):", DEFINITIONS + "class FutureWrapper(Future):")
    text = replace_once(text, "        shared_worker_lock = context.Lock()",
                        "        self._init_stage_event_channels(context, tp_size, pp_size, pcp_size)\n        shared_worker_lock = context.Lock()")
    text = replace_once(text, "                        inherited_fds=inherited_fds,",
                        "                        inherited_fds=inherited_fds,\n                        stage_event_channel=self._ks_stage_channels.get(global_rank),")
    text = replace_once(text, "    def _get_parallel_sizes(self)", EXECUTOR_METHODS + "    def _get_parallel_sizes(self)")
    text = replace_once(text, "        logger.debug_once(\"[shutdown] Executor: complete\")",
                        "        self._close_stage_event_channels()\n        logger.debug_once(\"[shutdown] Executor: complete\")")
    # WorkerProc constructor has this exact trailing signature, distinct from factory.
    text = replace_once(text, "        is_driver_worker: bool,\n    ):\n        self.rank = rank",
                        "        is_driver_worker: bool,\n        stage_event_channel=None,\n    ):\n        self.rank = rank")
    text = replace_once(text, "        # Enable environment variable cache (e.g. assume no more",
                        "        self._init_stage_event_channel(stage_event_channel, vllm_config)\n\n        # Enable environment variable cache (e.g. assume no more")
    text = replace_once(text, "        inherited_fds: list[int] | None = None,\n    ) -> UnreadyWorkerProcHandle:",
                        "        inherited_fds: list[int] | None = None,\n        stage_event_channel=None,\n    ) -> UnreadyWorkerProcHandle:")
    text = replace_once(text, '            "is_driver_worker": is_driver_worker,\n            # Have the worker',
                        '            "is_driver_worker": is_driver_worker,\n            "stage_event_channel": stage_event_channel,\n            # Have the worker')
    text = replace_once(text, "    def monitor_death_pipe(self, death_pipe, shutdown_requested: threading.Event):",
                        WORKER_METHODS + "    def monitor_death_pipe(self, death_pipe, shutdown_requested: threading.Event):")
    text = replace_once(text, "    def shutdown(self):\n        if self.rpc_broadcast_mq is not None:",
                        "    def shutdown(self):\n        if publisher := getattr(self, \"_ks_stage_publisher\", None):\n            publisher.close()\n            self._ks_stage_publisher = None\n        if self.rpc_broadcast_mq is not None:")
    text = replace_once(text, "            try:\n                if isinstance(method, str):",
                        "            if isinstance(method, KSStageCommand):\n                # Always consume this envelope here: never emit an old RPC reply.\n                self._handle_stage_command(method)\n                continue\n            try:\n                if isinstance(method, str):")
    ast.parse(text)
    # Do not alter old reply semantics, model output handlers or Future FIFO.
    before, after = ast.parse(original), ast.parse(text)
    preserved = {"FutureWrapper", "collective_rpc", "handle_output", "enqueue_output",
                 "async_output_busy_loop", "execute_model", "sample_tokens"}
    def selected(tree):
        return {n.name: ast.dump(n, include_attributes=False) for n in ast.walk(tree)
                if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in preserved}
    assert selected(before) == selected(after), "legacy RPC/model result logic changed"
    (HERE / "multiproc_executor.py").write_text(text, encoding="utf-8")
    diff = "".join(difflib.unified_diff(original.splitlines(True), text.splitlines(True),
        fromfile="a/vllm/v1/executor/multiproc_executor.py",
        tofile="b/vllm/v1/executor/multiproc_executor.py"))
    (HERE / "control_sidechannel.patch").write_bytes(diff.encode("utf-8"))
    metadata = {"status": "NOT_RUNTIME_INTEGRATED", "source": str(SOURCE),
                "candidate": str(HERE / "multiproc_executor.py"),
                "preserved_ast": sorted(preserved), "server_actions": 0, "npu_actions": 0,
                "default_enabled": False, "single_node_only": True}
    (HERE / "build_result.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return metadata


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
