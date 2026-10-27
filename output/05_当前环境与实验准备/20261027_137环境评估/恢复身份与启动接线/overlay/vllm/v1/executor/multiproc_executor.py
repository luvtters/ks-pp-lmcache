# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
import multiprocessing
import os
import pickle
import queue
import signal
import threading
import time
import traceback
import weakref
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future, InvalidStateError
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from multiprocessing.synchronize import Lock as LockType
from threading import Thread
from typing import Any, cast

import cloudpickle
import torch

import vllm.envs as envs
from vllm.config import VllmConfig
from vllm.distributed import destroy_distributed_environment, destroy_model_parallel
from vllm.distributed.device_communicators.shm_broadcast import Handle, MessageQueue
from vllm.distributed.kv_transfer.kv_connector.utils import KVOutputAggregator
from vllm.distributed.parallel_state import (
    get_dcp_group,
    get_dp_group,
    get_ep_group,
    get_inner_dp_world_group,
    get_pcp_group,
    get_pp_group,
    get_tp_group,
    model_parallel_is_initialized,
)
from vllm.envs import enable_envs_cache
from vllm.logger import init_logger
from vllm.platforms import current_platform
from vllm.tracing import instrument, maybe_init_worker_tracer
from vllm.utils import numa_utils
from vllm.utils.network_utils import (
    get_distributed_init_method,
    get_ip,
    get_loopback_ip,
    get_open_port,
)
from vllm.utils.ompmultiprocessing import OMPProcessManager
from vllm.utils.system_utils import (
    _maybe_force_spawn,
    decorate_logs,
    get_mp_context,
    set_process_title,
)
from vllm.v1.core.sched.output import GrammarOutput, SchedulerOutput
from vllm.v1.executor.abstract import Executor, FailureCallback
from vllm.v1.executor.vllm_net_devices import set_worker_net_device
from vllm.v1.outputs import AsyncModelRunnerOutput, DraftTokenIds, ModelRunnerOutput
from vllm.v1.worker.worker_base import WorkerWrapperBase

logger = init_logger(__name__)



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


@dataclass(frozen=True)
class KSRestoreEvent:
    """Per-request restore identity; deliberately has no logical batch ID."""
    event_id: str
    request_id: str
    restore_generation: int
    stage: int
    worker_id: int
    tp_rank: int
    pcp_rank: int
    sequence: int

    def validate(self):
        if (type(self.event_id) is not str or not 0 < len(self.event_id) <= 128
                or type(self.request_id) is not str or not self.request_id
                or any(type(value) is not int or value < 0 for value in (
                    self.restore_generation, self.stage, self.worker_id,
                    self.tp_rank, self.pcp_rank))
                or type(self.sequence) is not int or self.sequence < 1):
            raise ValueError("invalid restore event identity")


class _KSStageEventPublisher:
    """One writer thread per worker; never runs model/HCCL/Future queries.

    Any future host observer must enqueue a previously obtained readiness fact.
    It must NOT query the same LMCache Future as an existing completion path.
    """
    def __init__(self, transport, failed, worker_id, stage, tp_size=None, pcp_size=1):
        self.transport = transport
        self.failed = failed
        self.worker_id = worker_id
        self.stage = stage
        self.tp_size = tp_size
        self.pcp_size = pcp_size
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

    def publish_restore_ready(self, identity):
        # Called only with success drained from the unique device-future owner.
        # This method does not query/synchronize an event or manufacture readiness.
        identity.validate()
        if (self.pcp_size != 1 or type(self.tp_size) is not int or self.tp_size < 1
                or identity.global_worker_rank != self.worker_id
                or identity.stage_id != self.stage
                or self.stage != self.worker_id // self.tp_size
                or identity.tp_rank != self.worker_id % self.tp_size
                or identity.pcp_rank != 0):
            raise ValueError("restore identity does not match publisher PP/TP/PCP rank")
        with self.lock:
            if self.stopped.is_set() or self.failed.is_set():
                raise RuntimeError("stage event channel unavailable")
            sequence = self.sequence + 1
            event = KSRestoreEvent(f"restore:{self.worker_id}:{sequence}",
                identity.request_id, identity.restore_generation, self.stage,
                self.worker_id, identity.tp_rank, identity.pcp_rank, sequence)
            event.validate()
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.failed.set()
                raise RuntimeError("stage event inbox full") from None
            self.sequence = sequence

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


class FutureWrapper(Future):
    def __init__(
        self,
        futures_queue: deque["FutureWrapper"],
        get_response: Callable[[], Any],
        aggregate: Callable = lambda x: x,
    ):
        self.futures_queue = futures_queue
        self.get_response = get_response
        self.aggregate = aggregate
        super().__init__()
        self.futures_queue.appendleft(self)

    def result(self, timeout=None):
        if timeout is not None:
            raise RuntimeError("timeout not implemented")

        # Drain any futures ahead of us in the queue.
        while not self.done():
            future = self.futures_queue.pop()
            future._wait_for_response()
        return super().result()

    def _wait_for_response(self):
        try:
            response = self.aggregate(self.get_response())
            with suppress(InvalidStateError):
                self.set_result(response)
        except Exception as e:
            with suppress(InvalidStateError):
                self.set_exception(e)


class MultiprocExecutor(Executor):
    supports_pp: bool = True

    def __init__(self, vllm_config: VllmConfig, monitor_workers: bool = True):
        self.monitor_workers = monitor_workers
        super().__init__(vllm_config)

    def _init_executor(self) -> None:
        # Call self.shutdown at exit to clean up
        # and ensure workers will be terminated.
        self._finalizer = weakref.finalize(self, self.shutdown)
        self.is_failed = False
        self.failure_callback: FailureCallback | None = None

        tp_size, pp_size, pcp_size = self._get_parallel_sizes()
        assert self.world_size == tp_size * pp_size * pcp_size, (
            f"world_size ({self.world_size}) must be equal to the "
            f"tensor_parallel_size ({tp_size}) x pipeline"
            f"_parallel_size ({pp_size}) x prefill_context"
            f"_parallel_size ({pcp_size}). "
        )

        set_multiprocessing_worker_envs()

        # use the loopback address get_loopback_ip() for communication.
        distributed_init_method = get_distributed_init_method(
            get_loopback_ip(), get_open_port()
        )
        self.rpc_broadcast_mq: MessageQueue | None = None
        scheduler_output_handle: Handle | None = None
        # Initialize worker and set up message queues for SchedulerOutputs
        # and ModelRunnerOutputs
        if self.parallel_config.node_rank_within_dp == 0:
            # For leader node within each dp rank,
            # each dp will have its own leader multiproc executor.
            max_chunk_bytes = envs.VLLM_MQ_MAX_CHUNK_BYTES_MB * 1024 * 1024
            mq_connect_ip = get_ip()
            logger.info(
                "DP group leader: node_rank=%d, node_rank_within_dp=%d, "
                "master_addr=%s, mq_connect_ip=%s (local), "
                "world_size=%d, local_world_size=%d",
                self.parallel_config.node_rank,
                self.parallel_config.node_rank_within_dp,
                self.parallel_config.master_addr,
                mq_connect_ip,
                self.world_size,
                self.local_world_size,
            )
            self.rpc_broadcast_mq = MessageQueue(
                self.world_size,
                self.local_world_size,
                max_chunk_bytes=max_chunk_bytes,
                connect_ip=mq_connect_ip,
            )
            scheduler_output_handle = self.rpc_broadcast_mq.export_handle()
        # Create workers
        context = get_mp_context()
        self._init_stage_event_channels(context, tp_size, pp_size, pcp_size)
        shared_worker_lock = context.Lock()
        unready_workers: list[UnreadyWorkerProcHandle] = []
        success = False
        try:
            global_start_rank = (
                self.local_world_size * self.parallel_config.node_rank_within_dp
            )
            # When using fork, keep track of socket file descriptors that are
            # inherited by the worker, so that we can close them in subsequent
            # workers
            inherited_fds: list[int] | None = (
                [] if context.get_start_method() == "fork" else None
            )

            # For CPU backend only, to setup OpenMP threads affinity
            cpu_omp_manager = OMPProcessManager(self.vllm_config)
            for local_rank in range(self.local_world_size):
                global_rank = global_start_rank + local_rank
                is_driver_worker = self._is_driver_worker(global_rank)
                with cpu_omp_manager.configure_omp_envs(
                    rank=global_rank, local_rank=local_rank
                ):
                    unready_worker_handle = WorkerProc.make_worker_process(
                        vllm_config=self.vllm_config,
                        local_rank=local_rank,
                        rank=global_rank,
                        distributed_init_method=distributed_init_method,
                        input_shm_handle=scheduler_output_handle,
                        shared_worker_lock=shared_worker_lock,
                        is_driver_worker=is_driver_worker,
                        inherited_fds=inherited_fds,
                        stage_event_channel=self._ks_stage_channels.get(global_rank),
                    )
                unready_workers.append(unready_worker_handle)
                if inherited_fds is not None:
                    inherited_fds.append(unready_worker_handle.death_writer.fileno())
                    inherited_fds.append(unready_worker_handle.ready_pipe.fileno())

            # Workers must be created before wait_for_ready to avoid
            # deadlock, since worker.init_device() does a device sync.

            # Wait for all local workers to be ready.
            self.workers = WorkerProc.wait_for_ready(unready_workers)

            # Start background thread to monitor worker health if not in headless mode.
            if self.monitor_workers:
                self.start_worker_monitor()

            self.response_mqs = []
            # Only leader node have remote response mqs
            if self.parallel_config.node_rank_within_dp == 0:
                for rank in range(self.world_size):
                    if rank < self.local_world_size:
                        local_message_queue = self.workers[rank].worker_response_mq
                        assert local_message_queue is not None
                        self.response_mqs.append(local_message_queue)
                    else:
                        remote_message_queue = self.workers[0].peer_worker_response_mqs[
                            rank
                        ]
                        assert remote_message_queue is not None
                        self.response_mqs.append(remote_message_queue)

            # Ensure message queues are ready. Will deadlock if re-ordered
            # Must be kept consistent with the WorkerProc.

            # Wait for all input mqs to be ready.
            if self.rpc_broadcast_mq is not None:
                self.rpc_broadcast_mq.wait_until_ready()
            # Wait for all remote response mqs to be ready.
            for response_mq in self.response_mqs:
                response_mq.wait_until_ready()

            self.futures_queue = deque[FutureWrapper]()

            self._post_init_executor()

            success = True
        finally:
            if not success:
                # Clean up the worker procs if there was a failure.
                # Close death_writers first to signal workers to exit
                for uw in unready_workers:
                    if uw.death_writer is not None:
                        uw.death_writer.close()
                        uw.death_writer = None
                self._ensure_worker_termination([uw.proc for uw in unready_workers])

        self.output_rank = self._get_output_rank()


    def _init_stage_event_channels(self, context, tp_size, pp_size, pcp_size):
        self._ks_stage_channels = {}
        self._ks_stage_sequences = {}
        self._ks_stage_pending = {}
        self._ks_stage_poll_owner = None
        self._ks_stage_rpc_owner = threading.get_ident()
        self._ks_stage_tp_size = tp_size * pcp_size
        self._ks_restore_tp_size = tp_size
        self._ks_restore_pcp_size = pcp_size
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
                if not isinstance(event, (KSStageEvent, KSRestoreEvent)):
                    raise ValueError("unexpected stage channel payload")
                event.validate()
                if (event.worker_id != rank
                        or event.stage != rank // self._ks_stage_tp_size
                        or event.sequence != self._ks_stage_sequences.get(rank, 0) + 1):
                    raise ValueError("stage event worker/sequence mismatch")
                if isinstance(event, KSRestoreEvent):
                    if (self._ks_restore_pcp_size != 1
                            or event.tp_rank != rank % self._ks_restore_tp_size
                            or event.pcp_rank != 0):
                        raise ValueError("restore event TP/PCP identity mismatch")
                elif event.kind in ("CONTROL_RESULT", "CONTROL_ERROR"):
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

    def _get_parallel_sizes(self) -> tuple[int, int, int]:
        self.world_size = self.parallel_config.world_size
        assert self.world_size % self.parallel_config.nnodes_within_dp == 0, (
            f"global world_size ({self.parallel_config.world_size}) must be "
            f"divisible by nnodes_within_dp "
            f"({self.parallel_config.nnodes_within_dp}). "
        )
        self.local_world_size = self.parallel_config.local_world_size
        tp_size = self.parallel_config.tensor_parallel_size
        pp_size = self.parallel_config.pipeline_parallel_size
        pcp_size = self.parallel_config.prefill_context_parallel_size
        return tp_size, pp_size, pcp_size

    def _post_init_executor(self) -> None:
        pass

    def _is_driver_worker(self, rank: int) -> bool:
        return rank % self.parallel_config.tensor_parallel_size == 0

    def start_worker_monitor(self, inline=False) -> None:
        workers = self.workers
        self_ref = weakref.ref(self)

        # Monitors worker process liveness. If any die unexpectedly,
        # logs an error, shuts down the executor and invokes the failure
        # callback to inform the engine.
        def monitor_workers():
            sentinels = [h.proc.sentinel for h in workers]
            died = multiprocessing.connection.wait(sentinels)
            _self = self_ref()
            if not _self or getattr(_self, "shutting_down", False):
                logger.debug("MultiprocWorkerMonitor: shutdown already initiated")
                return
            _self.is_failed = True
            proc_name = next(h.proc.name for h in workers if h.proc.sentinel == died[0])
            logger.error(
                "Worker proc %s died unexpectedly, shutting down executor.", proc_name
            )
            _self.shutdown()
            callback = _self.failure_callback
            if callback is not None:
                _self.failure_callback = None
                callback()

        if not inline:
            Thread(
                target=monitor_workers, daemon=True, name="MultiprocWorkerMonitor"
            ).start()
            return

        monitor_workers()

    def register_failure_callback(self, callback: FailureCallback):
        if self.is_failed:
            callback()
        else:
            self.failure_callback = callback

    def execute_model(  # type: ignore[override]
        self, scheduler_output: SchedulerOutput, non_block: bool = False
    ) -> ModelRunnerOutput | None | Future[ModelRunnerOutput | None]:
        return self.collective_rpc(
            "execute_model",
            args=(scheduler_output,),
            unique_reply_rank=self.output_rank,
            non_block=non_block,
            timeout=envs.VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS,
            kv_output_aggregator=self.kv_output_aggregator,
        )

    def sample_tokens(  # type: ignore[override]
        self, grammar_output: GrammarOutput | None, non_block: bool = False
    ) -> ModelRunnerOutput | Future[ModelRunnerOutput]:
        return self.collective_rpc(
            "sample_tokens",
            args=(grammar_output,),
            unique_reply_rank=self.output_rank,
            non_block=non_block,
            timeout=envs.VLLM_EXECUTE_MODEL_TIMEOUT_SECONDS,
            kv_output_aggregator=self.kv_output_aggregator,
        )

    def execute_dummy_batch(self) -> None:
        self.collective_rpc("execute_dummy_batch", unique_reply_rank=self.output_rank)

    def take_draft_token_ids(self) -> DraftTokenIds | None:
        # OPTIMIZATION: Get output only from a single worker (output_rank)
        return self.collective_rpc(
            "take_draft_token_ids", unique_reply_rank=self.output_rank
        )


    def take_draft_token_ids_async(self) -> Future[DraftTokenIds | None]:
        # Explicit async entry; old take_draft_token_ids remains unchanged.
        return self.collective_rpc(
            "take_draft_token_ids", unique_reply_rank=self.output_rank,
            non_block=True,
        )

    def collective_rpc(  # type: ignore[override]
        self,
        method: str | Callable,
        timeout: float | None = None,
        args: tuple = (),
        kwargs: dict | None = None,
        non_block: bool = False,
        unique_reply_rank: int | None = None,
        kv_output_aggregator: KVOutputAggregator | None = None,
    ) -> Any:
        """Returns single result if unique_reply_rank and/or kv_output_aggregator
        is provided, otherwise list."""
        assert self.rpc_broadcast_mq is not None, (
            "collective_rpc should not be called on follower node"
        )
        if self.is_failed:
            raise RuntimeError("Executor failed.")

        deadline = None if timeout is None else time.monotonic() + timeout
        kwargs = kwargs or {}

        if kv_output_aggregator is not None:
            output_rank = None
            aggregate: Callable[[Any], Any] = partial(
                kv_output_aggregator.aggregate, output_rank=unique_reply_rank or 0
            )
        else:
            output_rank = unique_reply_rank
            aggregate = lambda x: x

        if isinstance(method, str):
            send_method = method
        else:
            send_method = cloudpickle.dumps(method, protocol=pickle.HIGHEST_PROTOCOL)
        self.rpc_broadcast_mq.enqueue((send_method, args, kwargs, output_rank))

        response_mqs: Sequence[MessageQueue] = self.response_mqs
        if output_rank is not None:
            response_mqs = (response_mqs[output_rank],)

        def get_response():
            responses = []
            for mq in response_mqs:
                dequeue_timeout = (
                    None if deadline is None else (deadline - time.monotonic())
                )
                try:
                    status, result = mq.dequeue(timeout=dequeue_timeout)
                except TimeoutError as e:
                    raise TimeoutError(f"RPC call to {method} timed out.") from e
                if status != WorkerProc.ResponseStatus.SUCCESS:
                    raise RuntimeError(
                        f"Worker failed with error '{result}', please check the"
                        " stack trace above for the root cause"
                    )
                responses.append(result)
            return responses[0] if output_rank is not None else responses

        future = FutureWrapper(
            self.futures_queue,
            get_response=get_response,
            aggregate=aggregate,
        )

        return future if non_block else future.result()

    @staticmethod
    def _ensure_worker_termination(worker_procs: list[BaseProcess]):
        """Ensure that all worker processes are terminated. Assumes workers have
        received termination requests. Waits for processing, then sends
        termination and kill signals if needed."""

        def wait_for_termination(procs, timeout):
            if not time:
                # If we are in late stage shutdown, the interpreter may replace
                # `time` with `None`.
                return all(not proc.is_alive() for proc in procs)
            start_time = time.time()
            while time.time() - start_time < timeout:
                if all(not proc.is_alive() for proc in procs):
                    return True
                time.sleep(0.1)
            return False

        active_procs = lambda: [proc for proc in worker_procs if proc.is_alive()]
        initial_count = len(active_procs())

        # Give processes time to clean themselves up properly first
        logger.info(
            "[shutdown] Executor: waiting for worker exit count=%d",
            initial_count,
        )
        if wait_for_termination(active_procs(), 4):
            logger.info_once("[shutdown] Executor: all workers exited gracefully")
            return

        # Send SIGTERM if still running
        remaining = active_procs()
        logger.warning(
            "[shutdown] Executor: workers still running after grace period; "
            "sending SIGTERM count=%d",
            len(remaining),
        )
        for p in remaining:
            p.terminate()
        if not wait_for_termination(active_procs(), 4):
            # Send SIGKILL if still running
            remaining = active_procs()
            logger.warning(
                "[shutdown] Executor: workers still running after SIGTERM; "
                "sending SIGKILL count=%d",
                len(remaining),
            )
            for p in remaining:
                p.kill()

    def shutdown(self):
        """Properly shut down the executor and its workers"""
        if not getattr(self, "shutting_down", False):
            worker_count = len(getattr(self, "workers", None) or [])
            logger.debug(
                "[shutdown] Executor: start worker_count=%d",
                worker_count,
            )
            self.shutting_down = True

            # Make sure all the worker processes are terminated first.
            if workers := getattr(self, "workers", None):
                for w in workers:
                    # Close death_writer to signal child processes to exit
                    if w.death_writer is not None:
                        w.death_writer.close()
                        w.death_writer = None
                self._ensure_worker_termination([w.proc for w in workers])

                for w in workers:
                    # Shutdown response queues
                    if w.worker_response_mq is not None:
                        w.worker_response_mq.shutdown()
                        w.worker_response_mq = None

        if rpc_broadcast_mq := getattr(self, "rpc_broadcast_mq", None):
            rpc_broadcast_mq.shutdown()
            self.rpc_broadcast_mq = None
        if response_mqs := getattr(self, "response_mqs", None):
            for mq in response_mqs:
                mq.shutdown()
            self.response_mqs = []

        self._close_stage_event_channels()
        logger.debug_once("[shutdown] Executor: complete")

    def check_health(self) -> None:
        self.collective_rpc("check_health", timeout=10)
        return

    def _get_output_rank(self) -> int:
        # Only returns ModelRunnerOutput from TP rank=0 and PP rank=-1
        # (the first TP worker of the last PP stage).
        # Example:
        # Assuming TP=8, PP=4, then the world_size=32
        # 0-7, PP rank 0
        # 8-15, PP rank 1
        # 16-23, PP rank 2
        # 24-31, PP rank 3
        # so world_size - tp_size = 32 - 8 = 24 should be PP rank = -1 (i.e. 3)
        return (
            self.world_size
            - self.parallel_config.tensor_parallel_size
            * self.parallel_config.prefill_context_parallel_size
        )

    @classmethod
    def supports_async_scheduling(cls) -> bool:
        return True


@dataclass
class UnreadyWorkerProcHandle:
    """WorkerProcess handle before READY."""

    proc: BaseProcess
    rank: int
    ready_pipe: Connection
    death_writer: Connection | None = None


@dataclass
class WorkerProcHandle:
    proc: BaseProcess
    rank: int
    # The worker process writes to this MQ in single-node mode
    worker_response_mq: MessageQueue | None
    # This is only non empty on driver node,
    # the peer worker process i writes to MQ
    # `peer_worker_response_mqs[i]`
    peer_worker_response_mqs: list[MessageQueue | None]
    death_writer: Connection | None = None

    @classmethod
    def from_unready_handle(
        cls,
        unready_handle: UnreadyWorkerProcHandle,
        worker_response_mq: MessageQueue | None,
        peer_worker_response_mqs: list[MessageQueue | None],
    ) -> "WorkerProcHandle":
        return cls(
            proc=unready_handle.proc,
            rank=unready_handle.rank,
            worker_response_mq=worker_response_mq,
            peer_worker_response_mqs=peer_worker_response_mqs,
            death_writer=unready_handle.death_writer,
        )


class WorkerProc:
    """Wrapper that runs one Worker in a separate process."""

    READY_STR = "READY"
    rpc_broadcast_mq: MessageQueue | None
    worker_response_mq: MessageQueue | None

    def _init_message_queues(
        self, input_shm_handle: Handle, vllm_config: VllmConfig
    ) -> None:
        if vllm_config.parallel_config.nnodes_within_dp == 1:
            # Initialize MessageQueue for receiving SchedulerOutput
            self.rpc_broadcast_mq = MessageQueue.create_from_handle(
                input_shm_handle, self.worker.rank
            )

            # Initializes a message queue for sending the model output
            self.worker_response_mq = MessageQueue(1, 1)
            self.peer_response_handles = []
        else:
            # Initialize remote MessageQueue for receiving SchedulerOutput across nodes
            self.rpc_broadcast_mq = get_inner_dp_world_group().create_mq_broadcaster(
                external_writer_handle=input_shm_handle,
                # Since there is external_writer_handle from executor proc,
                # where the ready signal from actual writer is sent out of the
                # create_mq_broadcaster method and after this setup, we make it
                # non blocking. The handshake will be triggered when
                # worker.rpc_broadcast_mq.wait_until_ready() is called
                blocking=False,
            )
            # Initializes remote message queue for sending the model output to the
            # driver worker, exposing peer_response_handles for driver worker
            # that include handles for all ranks
            self.worker_response_mq, self.peer_response_handles = (
                get_inner_dp_world_group().create_single_reader_mq_broadcasters(
                    reader_rank_in_group=0
                )
            )

    @instrument(span_name="Worker init")
    def __init__(
        self,
        vllm_config: VllmConfig,
        local_rank: int,
        rank: int,
        distributed_init_method: str,
        input_shm_handle: Handle,
        shared_worker_lock: LockType,
        is_driver_worker: bool,
        stage_event_channel=None,
    ):
        self.rank = rank
        wrapper = WorkerWrapperBase(rpc_rank=local_rank, global_rank=rank)
        # TODO: move `init_worker` to executor level as a collective rpc call
        all_kwargs: list[dict] = [
            {} for _ in range(vllm_config.parallel_config.world_size)
        ]
        all_kwargs[local_rank] = {
            "vllm_config": vllm_config,
            "local_rank": local_rank,
            "rank": rank,
            "distributed_init_method": distributed_init_method,
            "is_driver_worker": is_driver_worker,
            "shared_worker_lock": shared_worker_lock,
        }
        wrapper.init_worker(all_kwargs)
        self.worker = wrapper

        self.setup_proc_title_and_log_prefix(
            enable_ep=vllm_config.parallel_config.enable_expert_parallel
        )

        # Load model
        self.worker.init_device()
        # Update process title now that parallel groups are initialized
        self.setup_proc_title_and_log_prefix(
            enable_ep=vllm_config.parallel_config.enable_expert_parallel
        )
        if envs.VLLM_ELASTIC_EP_SCALE_UP_LAUNCH:
            self.worker.elastic_ep_execute("load_model")
        else:
            self.worker.load_model()

        scheduler_config = vllm_config.scheduler_config
        self.use_async_scheduling = scheduler_config.async_scheduling
        if self.use_async_scheduling:
            self.async_output_queue: queue.Queue = queue.Queue()
            self.async_output_copy_thread = Thread(
                target=self.async_output_busy_loop,
                daemon=True,
                name="WorkerAsyncOutputCopy",
            )
            self.async_output_copy_thread.start()

        # Set block size based on the attention backends
        current_platform.update_block_size_for_backend(vllm_config)

        # Initialize message queues after init_device() since multi-node setups
        # (nnodes_within_dp > 1) require distributed groups to be initialized
        self._init_message_queues(input_shm_handle, vllm_config)

        self._init_stage_event_channel(stage_event_channel, vllm_config)

        # Enable environment variable cache (e.g. assume no more
        # environment variable overrides after this point)
        enable_envs_cache()

    @staticmethod
    def make_worker_process(
        vllm_config: VllmConfig,
        local_rank: int,
        rank: int,
        distributed_init_method: str,
        input_shm_handle,  # Receive SchedulerOutput
        shared_worker_lock: LockType,
        is_driver_worker: bool,
        inherited_fds: list[int] | None = None,
        stage_event_channel=None,
    ) -> UnreadyWorkerProcHandle:
        context = get_mp_context()
        # Ready pipe to communicate readiness from child to parent
        ready_reader, ready_writer = context.Pipe(duplex=False)
        # Death pipe to let child detect parent process exit
        death_reader, death_writer = context.Pipe(duplex=False)
        if inherited_fds is not None:
            inherited_fds = inherited_fds.copy()
            inherited_fds.extend((ready_reader.fileno(), death_writer.fileno()))
        process_kwargs = {
            "vllm_config": vllm_config,
            "local_rank": local_rank,
            "rank": rank,
            "distributed_init_method": distributed_init_method,
            "input_shm_handle": input_shm_handle,
            "ready_pipe": ready_writer,
            "death_pipe": death_reader,
            "shared_worker_lock": shared_worker_lock,
            "is_driver_worker": is_driver_worker,
            "stage_event_channel": stage_event_channel,
            # Have the worker close parent end of this worker's pipes too
            "inherited_fds": inherited_fds if inherited_fds is not None else [],
        }
        # Run EngineCore busy loop in background process.
        proc = context.Process(
            target=WorkerProc.worker_main,
            kwargs=process_kwargs,
            name=f"VllmWorker-{rank}",
            daemon=True,
        )

        # Apply NUMA binding if configured
        with numa_utils.configure_subprocess(
            vllm_config, local_rank, process_kind="worker"
        ):
            proc.start()

        # Close child ends of pipes here in the parent
        ready_writer.close()
        death_reader.close()
        # Keep death_writer open in parent - when parent exits,
        # death_reader in child will get EOFError
        return UnreadyWorkerProcHandle(proc, rank, ready_reader, death_writer)

    @staticmethod
    def wait_for_response_handle_ready(
        handles: dict[str, Any], proc_handle: UnreadyWorkerProcHandle
    ) -> WorkerProcHandle:
        response_handle = handles["handle"]
        worker_response_mq: MessageQueue | None = None
        if len(response_handle.local_reader_ranks) > 0:
            worker_response_mq = MessageQueue.create_from_handle(response_handle, 0)
        peer_response_handles = handles["peer_response_handles"]
        peer_worker_response_mqs = [
            MessageQueue.create_from_handle(handle, -1)
            if handle.remote_subscribe_addr is not None
            else None
            for handle in peer_response_handles
        ]
        return WorkerProcHandle.from_unready_handle(
            proc_handle,
            worker_response_mq,
            peer_worker_response_mqs=peer_worker_response_mqs,
        )

    @staticmethod
    def wait_for_ready(
        unready_proc_handles: list[UnreadyWorkerProcHandle],
    ) -> list[WorkerProcHandle]:
        e = Exception(
            "WorkerProc initialization failed due to an exception in a "
            "background process. See stack trace for root cause."
        )

        pipes = {handle.ready_pipe: handle for handle in unready_proc_handles}
        ready_proc_handles: list[WorkerProcHandle | None] = [None] * len(
            unready_proc_handles
        )
        while pipes:
            ready = multiprocessing.connection.wait(pipes.keys())
            for pipe in ready:
                assert isinstance(pipe, Connection)
                try:
                    # Wait until the WorkerProc is ready.
                    unready_proc_handle = pipes.pop(pipe)
                    response: dict[str, Any] = pipe.recv()
                    if response["status"] != "READY":
                        raise e

                    idx = unready_proc_handle.rank % len(ready_proc_handles)
                    ready_proc_handles[idx] = WorkerProc.wait_for_response_handle_ready(
                        response, unready_proc_handle
                    )
                except EOFError:
                    e.__suppress_context__ = True
                    raise e from None

                finally:
                    # Close connection.
                    pipe.close()

        return cast(list[WorkerProcHandle], ready_proc_handles)

    def shutdown(self):
        if publisher := getattr(self, "_ks_stage_publisher", None):
            publisher.close()
            self._ks_stage_publisher = None
        if self.rpc_broadcast_mq is not None:
            self.rpc_broadcast_mq.shutdown()
        if self.worker_response_mq is not None:
            self.worker_response_mq.shutdown()
        self.worker.shutdown()
        self.rpc_broadcast_mq = None
        self.worker_response_mq = None
        destroy_model_parallel()
        destroy_distributed_environment()


    def _init_stage_event_channel(self, stage_event_channel, vllm_config):
        self._ks_stage_publisher = None
        if stage_event_channel is None:
            return
        parallel = vllm_config.parallel_config
        self._ks_stage_index = self.rank // (parallel.tensor_parallel_size
                                            * parallel.prefill_context_parallel_size)
        transport, failed = stage_event_channel
        self._ks_stage_publisher = _KSStageEventPublisher(
            transport, failed, self.rank, self._ks_stage_index,
            tp_size=parallel.tensor_parallel_size,
            pcp_size=parallel.prefill_context_parallel_size)
        # Interface only. No LMCache/stream/event observer is installed here.
        self.worker.ks_publish_stage_event = self._ks_stage_publisher.publish
        self.worker.ks_publish_restore_ready = self._ks_stage_publisher.publish_restore_ready
        if os.environ.get("KS_LMCACHE_STAGE_OBSERVER") == "1":
            from lmcache.integration.vllm.ks_restore_runtime import register_worker_publisher
            register_worker_publisher(self._ks_stage_publisher)

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

    def monitor_death_pipe(self, death_pipe, shutdown_requested: threading.Event):
        if death_pipe is None:
            return

        def death_pipe_monitor(queues_to_shutdown: list[MessageQueue]):
            try:
                # This will block until parent process exits (pipe closes)
                death_pipe.recv()
            except EOFError:
                logger.info_once("Parent process exited, terminating worker queues")
                shutdown_requested.set()
                for mq in queues_to_shutdown:
                    if mq is not None:
                        mq.shutdown()
            except Exception as e:
                logger.warning("Death monitoring error: %s", e)

        # Pass queue references directly to avoid gc issues if passing self
        Thread(
            target=death_pipe_monitor,
            args=([self.rpc_broadcast_mq, self.worker_response_mq],),
            daemon=True,
            name="DeathPipeMonitor",
        ).start()

    @staticmethod
    def worker_main(*args, **kwargs):
        """Worker initialization and execution loops.
        This runs a background process"""

        # Signal handler used for graceful termination.
        # SystemExit exception is only raised once to allow this and worker
        # processes to terminate without error
        shutdown_requested = threading.Event()

        def signal_handler(signum, frame):
            nonlocal shutdown_requested
            if not shutdown_requested.is_set():
                shutdown_requested.set()
                logger.debug(
                    "WorkerProc handling signal %d, raising SystemExit", signum
                )
                raise SystemExit()

        # Either SIGTERM or SIGINT will terminate the worker
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Set net device env vars for the worker if VLLM_GPU_NIC_PCIE_MAPPING is set
        set_worker_net_device(kwargs.get("local_rank", 0), kwargs["vllm_config"])

        worker = None
        ready_writer = kwargs.pop("ready_pipe")
        death_pipe = kwargs.pop("death_pipe", None)

        # Close inherited pipes from parent (incl. other worker pipes)
        # Explicitly passing in existing pipes and closing them makes the pipe
        # behave when using fork. Otherwise, a hidden reference to the pipes
        # exist in the child process and prevents EOF closure.
        for fd in kwargs.pop("inherited_fds", []):
            try:
                os.close(fd)
            except Exception as e:
                logger.warning("Error closing inherited connection: %s: %s", type(e), e)

        try:
            # Initialize tracer
            rank = kwargs.get("rank", 0)
            maybe_init_worker_tracer(
                instrumenting_module_name="vllm.worker",
                process_kind="worker",
                process_name=f"Worker_{rank}",
            )

            worker = WorkerProc(*args, **kwargs)
            assert worker.worker_response_mq is not None
            if kwargs["vllm_config"].parallel_config.numa_bind:
                numa_utils.log_current_affinity_state(f"Worker_{worker.rank}")

            worker.monitor_death_pipe(death_pipe, shutdown_requested)

            # Send READY once we know everything is loaded
            ready_writer.send(
                {
                    "status": WorkerProc.READY_STR,
                    "handle": worker.worker_response_mq.export_handle(),
                    "peer_response_handles": worker.peer_response_handles,
                }
            )

            # Ensure message queues are ready. Will deadlock if re-ordered.
            # Must be kept consistent with the Executor
            if worker.rpc_broadcast_mq is not None:
                worker.rpc_broadcast_mq.wait_until_ready()
            worker.worker_response_mq.wait_until_ready()
            ready_writer.close()
            ready_writer = None

            worker.worker_busy_loop()

        except Exception:
            # NOTE: if an Exception arises in busy_loop, we send
            # a FAILURE message over the MQ RPC to notify the Executor,
            # which triggers system shutdown.
            # TODO(rob): handle case where the MQ itself breaks.

            if ready_writer is not None:
                logger.exception("WorkerProc failed to start.")
            elif shutdown_requested.is_set():
                logger.debug_once(
                    "[shutdown] WorkerProc: exiting after shutdown request"
                )
            else:
                logger.exception("WorkerProc failed.")

            # The parent sends a SIGTERM to all worker processes if
            # any worker dies. Set this value so we don't re-throw
            # SystemExit() to avoid zmq exceptions in __del__.
            shutdown_requested.set()

        except SystemExit as e:
            # SystemExit is raised on SIGTERM or SIGKILL, which usually indicates that
            # the graceful shutdown process did not succeed
            if shutdown_requested.is_set():
                logger.debug_once(
                    "[shutdown] WorkerProc: terminated by shutdown signal"
                )
            else:
                logger.warning("WorkerProc was terminated")
            # SystemExit must never be ignored
            raise e

        finally:
            if ready_writer is not None:
                ready_writer.close()
            if death_pipe is not None:
                death_pipe.close()
            # Clean up once worker exits busy loop
            if worker is not None:
                worker.shutdown()

    class ResponseStatus(Enum):
        SUCCESS = auto()
        FAILURE = auto()

    def enqueue_output(self, output: Any):
        """Prepares output from the worker and enqueues it to the
        worker_response_mq. If the output is an Exception, it is
        converted to a FAILURE response.
        """
        if isinstance(output, AsyncModelRunnerOutput):
            output = output.get_output()

        if isinstance(output, Exception):
            result = (WorkerProc.ResponseStatus.FAILURE, str(output))
        else:
            result = (WorkerProc.ResponseStatus.SUCCESS, output)
        if (response_mq := self.worker_response_mq) is not None:
            response_mq.enqueue(result)

    def handle_output(self, output: Any):
        """Handles output from the worker. If async scheduling is enabled,
        it is passed to the async_output_busy_loop thread. Otherwise, it is
        enqueued directly to the worker_response_mq.
        """
        if self.use_async_scheduling:
            self.async_output_queue.put(output)
        else:
            self.enqueue_output(output)

    def async_output_busy_loop(self):
        """Entrypoint for the thread which handles outputs asynchronously."""

        # set device to the worker device for the thread.
        # a thread will not inherit the context of the main thread.
        # when calling any cuda runtime functions, it will implicitly
        # create a new cuda context on device 0, consuming extra memory.
        # here we set the device to the worker device for the thread,
        # enforcing the context to be the same as the main thread.
        from vllm.platforms import current_platform

        if hasattr(self.worker, "device"):
            current_platform.set_device(self.worker.device)

        while True:
            output = self.async_output_queue.get()
            self.enqueue_output(output)

    def worker_busy_loop(self):
        """Main busy loop for Multiprocessing Workers"""
        assert self.rpc_broadcast_mq is not None
        while True:
            method, args, kwargs, output_rank = self.rpc_broadcast_mq.dequeue(
                indefinite=True
            )
            if isinstance(method, KSStageCommand):
                # Always consume this envelope here: never emit an old RPC reply.
                self._handle_stage_command(method)
                continue
            try:
                if isinstance(method, str):
                    func = getattr(self.worker, method)
                elif isinstance(method, bytes):
                    func = partial(cloudpickle.loads(method), self.worker)

                output = func(*args, **kwargs)
            except Exception as e:
                # Notes have been introduced in python 3.11
                if hasattr(e, "add_note"):
                    e.add_note(traceback.format_exc())
                logger.exception("WorkerProc hit an exception.")
                # exception might not be serializable, so we convert it to
                # string, only for logging purpose.
                if output_rank is None or self.rank == output_rank:
                    self.handle_output(e)
                continue

            if output_rank is None or self.rank == output_rank:
                self.handle_output(output)

    @staticmethod
    def setup_proc_title_and_log_prefix(enable_ep: bool) -> None:
        # Check if parallel groups are initialized first
        if not model_parallel_is_initialized():
            # Parallel groups not yet initialized, use default process name
            set_process_title(name="Worker")
            decorate_logs("Worker")
            return

        dp_size = get_dp_group().world_size
        dp_rank = get_dp_group().rank_in_group
        pp_size = get_pp_group().world_size
        pp_rank = get_pp_group().rank_in_group
        pcp_size = get_pcp_group().world_size
        pcp_rank = get_pcp_group().rank_in_group
        tp_size = get_tp_group().world_size
        tp_rank = get_tp_group().rank_in_group
        dcp_size = get_dcp_group().world_size
        dcp_rank = get_dcp_group().rank_in_group
        process_name = "Worker"
        if dp_size > 1:
            process_name += f"_DP{dp_rank}"
        if pp_size > 1:
            process_name += f"_PP{pp_rank}"
        if pcp_size > 1:
            process_name += f"_PCP{pcp_rank}"
        if tp_size > 1:
            process_name += f"_TP{tp_rank}"
        if dcp_size > 1:
            process_name += f"_DCP{dcp_rank}"
        if enable_ep:
            ep_rank = get_ep_group().rank_in_group
            process_name += f"_EP{ep_rank}"
        set_process_title(name=process_name)
        decorate_logs(process_name)


def set_multiprocessing_worker_envs():
    """Set up environment variables that should be used when there are workers
    in a multiprocessing environment. This should be called by the parent
    process before worker processes are created"""

    _maybe_force_spawn()

    if not current_platform.is_cpu():
        # Configure thread parallelism if OMP_NUM_THREADS isn't set
        #
        # Helps to avoid CPU contention. The default of spawning a thread per
        # core combined with multiprocessing for each GPU can have a negative
        # impact on performance. The contention is amplified when running in a
        # container where CPU limits can cause throttling.
        default_omp_num_threads = 1
        if (
            "OMP_NUM_THREADS" not in os.environ
            and (current_parallelism := torch.get_num_threads())
            > default_omp_num_threads
        ):
            logger.warning_once(
                "Reducing Torch parallelism from %d threads to %d to avoid "
                "unnecessary CPU contention. Set OMP_NUM_THREADS in the "
                "external environment to tune this value as needed.",
                current_parallelism,
                default_omp_num_threads,
            )
            os.environ["OMP_NUM_THREADS"] = str(default_omp_num_threads)
            torch.set_num_threads(default_omp_num_threads)
