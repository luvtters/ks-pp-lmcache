"""Actual candidate AST seams; no vLLM/torch import or framework bootstrap.

Stable module name chain_runtime is required for multiprocessing Queue pickle.
See chain_source_map.json for original code locations and selected boundaries.
"""
from __future__ import annotations
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from dataclasses import dataclass
import os
import queue
import threading
from threading import Thread
import time
import weakref
from typing import Any, Callable, cast

_KS_STAGE_METHODS = {'input_received': 'pp_stage_input_received', 'input_ready': 'pp_stage_input_ready', 'model_ready': 'pp_stage_model_ready', 'release_ready': 'pp_stage_release_ready', 'release': 'release_pp_stage_batch'}
_KS_EVENT_KINDS = frozenset(('CONTROL_RESULT', 'CONTROL_ERROR', 'KV_DEVICE_READY', 'INPUT_DEVICE_READY', 'STAGE_DEVICE_DONE'))

def _ks_identity(event_id, batch_id, generation, stage):
    if not (type(event_id) is str and 0 < len(event_id) <= 128 and (type(batch_id) is str) and (0 < len(batch_id) <= 128) and (type(generation) is int) and (generation >= 0) and (type(stage) is int) and (stage >= 0)):
        raise ValueError('invalid stage event/command identity')

@dataclass(frozen=True)
class KSStageCommand:
    command_id: str
    batch_id: str
    generation: int
    target_stage: int
    operation: str
    final_output_consumed: bool = False

    def validate(self):
        _ks_identity(self.command_id, self.batch_id, self.generation, self.target_stage)
        if self.operation not in _KS_STAGE_METHODS or type(self.final_output_consumed) is not bool:
            raise ValueError('unsupported stage control command')

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
        if type(self.worker_id) is not int or self.worker_id < 0 or type(self.sequence) is not int or (self.sequence < 1) or (self.kind not in _KS_EVENT_KINDS) or (type(self.value) not in (bool, str, type(None))) or (type(self.value) is str and len(self.value) > 512):
            raise ValueError('invalid stage event payload')

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
        if type(self.event_id) is not str or not 0 < len(self.event_id) <= 128 or type(self.request_id) is not str or (not self.request_id) or any((type(value) is not int or value < 0 for value in (self.restore_generation, self.stage, self.worker_id, self.tp_rank, self.pcp_rank))) or (type(self.sequence) is not int) or (self.sequence < 1):
            raise ValueError('invalid restore event identity')

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
        self.thread = Thread(target=self._run, daemon=True, name='KSStageEventWriter')
        self.thread.start()

    def publish(self, event_id, batch_id, generation, kind, value=None):
        with self.lock:
            if self.stopped.is_set() or self.failed.is_set():
                raise RuntimeError('stage event channel unavailable')
            event = KSStageEvent(event_id, batch_id, generation, self.stage, self.worker_id, self.sequence + 1, kind, value)
            event.validate()
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.failed.set()
                raise RuntimeError('stage event inbox full') from None
            self.sequence += 1

    def publish_restore_ready(self, identity):
        identity.validate()
        if self.pcp_size != 1 or type(self.tp_size) is not int or self.tp_size < 1 or (identity.global_worker_rank != self.worker_id) or (identity.stage_id != self.stage) or (self.stage != self.worker_id // self.tp_size) or (identity.tp_rank != self.worker_id % self.tp_size) or (identity.pcp_rank != 0):
            raise ValueError('restore identity does not match publisher PP/TP/PCP rank')
        with self.lock:
            if self.stopped.is_set() or self.failed.is_set():
                raise RuntimeError('stage event channel unavailable')
            sequence = self.sequence + 1
            event = KSRestoreEvent(f'restore:{self.worker_id}:{sequence}', identity.request_id, identity.restore_generation, self.stage, self.worker_id, identity.tp_rank, identity.pcp_rank, sequence)
            event.validate()
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.failed.set()
                raise RuntimeError('stage event inbox full') from None
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
        self.transport.cancel_join_thread()
        self.transport.close()

class FutureWrapper(Future):

    def __init__(self, futures_queue: deque['FutureWrapper'], get_response: Callable[[], Any], aggregate: Callable=lambda x: x):
        self.futures_queue = futures_queue
        self.get_response = get_response
        self.aggregate = aggregate
        super().__init__()
        self.futures_queue.appendleft(self)

    def result(self, timeout=None):
        if timeout is not None:
            raise RuntimeError('timeout not implemented')
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

class MultiprocExecutor:

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
        if os.environ.get('KS_PP_STAGE_EVENTS') != '1':
            return
        if self.parallel_config.nnodes_within_dp != 1 or self.local_world_size != self.world_size:
            raise ValueError('KS stage sidechannel supports single-node only')
        for rank in range(self.world_size):
            self._ks_stage_channels[rank] = (context.Queue(maxsize=64), context.Event())

    def poll_stage_events(self, max_per_worker=8):
        if type(max_per_worker) is not int or max_per_worker < 1:
            raise ValueError('max_per_worker must be positive')
        owner = threading.get_ident()
        if self._ks_stage_poll_owner not in (None, owner):
            raise RuntimeError('only one core reader may consume stage events')
        self._ks_stage_poll_owner = owner
        events = []
        for (rank, (transport, failed)) in self._ks_stage_channels.items():
            if failed.is_set():
                raise RuntimeError(f'stage event channel failed for worker {rank}')
            for _ in range(max_per_worker):
                try:
                    event = transport.get_nowait()
                except queue.Empty:
                    break
                if not isinstance(event, (KSStageEvent, KSRestoreEvent)):
                    raise ValueError('unexpected stage channel payload')
                event.validate()
                if event.worker_id != rank or event.stage != rank // self._ks_stage_tp_size or event.sequence != self._ks_stage_sequences.get(rank, 0) + 1:
                    raise ValueError('stage event worker/sequence mismatch')
                if isinstance(event, KSRestoreEvent):
                    if self._ks_restore_pcp_size != 1 or event.tp_rank != rank % self._ks_restore_tp_size or event.pcp_rank != 0:
                        raise ValueError('restore event TP/PCP identity mismatch')
                elif event.kind in ('CONTROL_RESULT', 'CONTROL_ERROR'):
                    pending = self._ks_stage_pending.get(event.event_id)
                    if pending is None:
                        raise ValueError('unknown or duplicate stage command result')
                    (command, ranks) = pending
                    if event.batch_id != command.batch_id or event.generation != command.generation or event.stage != command.target_stage or (rank not in ranks):
                        raise ValueError('stage command result identity mismatch')
                    ranks.remove(rank)
                    if not ranks:
                        del self._ks_stage_pending[event.event_id]
                self._ks_stage_sequences[rank] = event.sequence
                events.append(event)
        return events

    def _close_stage_event_channels(self):
        for (transport, _) in getattr(self, '_ks_stage_channels', {}).values():
            transport.cancel_join_thread()
            transport.close()
        self._ks_stage_channels = {}
        self._ks_stage_pending = {}

class _KSModelResultReader:

    def __init__(self):
        self.owner = threading.get_ident()
        self.tasks = queue.Queue(maxsize=1)
        self.results = weakref.WeakKeyDictionary()
        self.active = None
        self.closed = False
        self.thread = threading.Thread(target=self._run, daemon=True, name='KSModelResultReader')
        self.thread.start()

    def submit(self, future):
        if threading.get_ident() != self.owner:
            raise RuntimeError('model wait must stay on the configured core thread')
        cached = self.results.get(future)
        if cached is not None:
            return cached
        if self.closed:
            raise RuntimeError('model result reader is closed')
        if self.active is not None and (not self.active.done()):
            raise RuntimeError('another old model future already has the sole reader')
        outcome = Future()
        self.results[future] = outcome
        self.active = outcome
        self.tasks.put_nowait((future, outcome))
        return outcome

    def _run(self):
        while True:
            task = self.tasks.get()
            if task is None:
                return
            (future, outcome) = task
            try:
                outcome.set_result(future.result())
            except BaseException as error:
                outcome.set_exception(error)
            finally:
                del task, future, outcome

    def close(self, timeout=0.2):
        if self.active is not None and (not self.active.done()):
            return False
        if not self.closed:
            self.closed = True
            self.tasks.put_nowait(None)
        self.thread.join(timeout=timeout)
        return not self.thread.is_alive()

class EngineCore:

    def configure_ks_core_event_pump(self, callback, dispatcher=None, poll_interval=0.005):
        if os.environ.get('KS_CORE_STAGE_EVENT_PUMP') != '1':
            raise RuntimeError('core stage event pump is disabled')
        if getattr(self, '_ks_result_reader', None) is not None:
            raise RuntimeError('core event pump is already configured')
        if not callable(callback) or (dispatcher is not None and (not callable(dispatcher))):
            raise ValueError('event callback and optional dispatcher must be callable')
        if not callable(getattr(self.model_executor, 'poll_stage_events', None)):
            raise ValueError('independent executor stage event channel is required')
        if not 0 < poll_interval <= 0.1:
            raise ValueError('poll interval must be between zero and 0.1 seconds')
        self._ks_event_callback = callback
        self._ks_event_dispatcher = dispatcher
        self._ks_event_poll_interval = poll_interval
        self._ks_event_callback_active = False
        self._ks_waiting_model_future = None
        self._ks_result_reader = _KSModelResultReader()

    def ks_model_wait_inflight(self):
        return int(getattr(self, '_ks_waiting_model_future', None) is not None)

    def _ks_assert_no_event_callback_step(self):
        if getattr(self, '_ks_event_callback_active', False):
            raise RuntimeError('event callback cannot recursively schedule a model step')

    def _ks_consume_stage_events(self):
        for event in self.model_executor.poll_stage_events():
            self._ks_event_callback_active = True
            try:
                if self._ks_event_callback(event) is not None:
                    raise ValueError('event callback must return None')
                if self._ks_event_dispatcher is not None:
                    self._ks_event_dispatcher(event)
            finally:
                self._ks_event_callback_active = False

    def _ks_wait_model_future(self, future):
        reader = getattr(self, '_ks_result_reader', None)
        if reader is None:
            return future.result()
        if getattr(self, '_ks_event_callback_active', False):
            raise RuntimeError('event callback cannot consume old model results')
        outcome = reader.submit(future)
        completed = threading.Event()
        outcome.add_done_callback(lambda _: completed.set())
        self._ks_waiting_model_future = future
        try:
            event_error = None
            while True:
                if event_error is None:
                    try:
                        self._ks_consume_stage_events()
                    except BaseException as error:
                        event_error = error
                if completed.is_set():
                    break
                completed.wait(self._ks_event_poll_interval)
            result = outcome.result()
            if event_error is not None:
                raise event_error
            return result
        finally:
            self._ks_waiting_model_future = None

    def _ks_sample_tokens_with_event_pump(self, grammar_output):
        if getattr(self, '_ks_result_reader', None) is None:
            return self.model_executor.sample_tokens(grammar_output)
        return self._ks_wait_model_future(self.model_executor.sample_tokens(grammar_output, non_block=True))

    def _ks_close_core_event_pump(self):
        reader = getattr(self, '_ks_result_reader', None)
        if reader is not None and (not reader.close()):
            raise RuntimeError('old result reader is still active after executor shutdown')

    def step(self) -> tuple[dict[int, EngineCoreOutputs], bool]:
        """Schedule, execute, and make output.

        Returns tuple of outputs and a flag indicating whether the model
        was executed.
        """
        self._ks_assert_no_event_callback_step()
        if not self.scheduler.has_requests():
            return ({}, False)
        scheduler_output = self.scheduler.schedule()
        future = self.model_executor.execute_model(scheduler_output, non_block=True)
        grammar_output = self.scheduler.get_grammar_bitmask(scheduler_output)
        with self.log_error_detail(scheduler_output), self.log_iteration_details(scheduler_output):
            model_output = self._ks_wait_model_future(future)
            if model_output is None:
                model_output = self._ks_sample_tokens_with_event_pump(grammar_output)
        self._process_aborts_queue()
        engine_core_outputs = self.scheduler.update_from_output(scheduler_output, model_output)
        return (engine_core_outputs, scheduler_output.total_num_scheduled_tokens > 0)

    def step_with_batch_queue(self) -> tuple[dict[int, EngineCoreOutputs] | None, bool]:
        """Schedule and execute batches with the batch queue.
        Note that if nothing to output in this step, None is returned.

        The execution flow is as follows:
        1. Try to schedule a new batch if the batch queue is not full.
        If a new batch is scheduled, directly return an empty engine core
        output. In other words, fulfilling the batch queue has a higher priority
        than getting model outputs.
        2. If there is no new scheduled batch, meaning that the batch queue
        is full or no other requests can be scheduled, we block until the first
        batch in the job queue is finished.
        3. Update the scheduler from the output.
        """
        self._ks_assert_no_event_callback_step()
        batch_queue = self.batch_queue
        assert batch_queue is not None
        assert len(batch_queue) < self.batch_queue_size
        model_executed = False
        deferred_scheduler_output = None
        if self.scheduler.has_requests():
            scheduler_output = self.scheduler.schedule()
            with self.log_error_detail(scheduler_output):
                exec_future = self.model_executor.execute_model(scheduler_output, non_block=True)
            if self.is_ec_consumer:
                model_executed = scheduler_output.total_num_scheduled_tokens > 0
            if self.is_pooling_model or not model_executed:
                future = cast(Future[ModelRunnerOutput], exec_future)
            elif not scheduler_output.pending_structured_output_tokens:
                grammar_output = self.scheduler.get_grammar_bitmask(scheduler_output)
                future = self.model_executor.sample_tokens(grammar_output, non_block=True)
            else:
                deferred_scheduler_output = scheduler_output
            if not deferred_scheduler_output:
                batch_queue.appendleft((future, scheduler_output, exec_future))
                if len(batch_queue) < self.batch_queue_size and (model_executed or self.scheduler.has_requests()):
                    return (None, model_executed)
        elif not batch_queue:
            return (None, False)
        (future, scheduler_output, exec_model_fut) = batch_queue.pop()
        with self.log_error_detail(scheduler_output), self.log_iteration_details(scheduler_output):
            model_output = self._ks_wait_model_future(future)
            if model_output is None:
                self._ks_wait_model_future(exec_model_fut)
                raise RuntimeError('unexpected error')
        self._process_aborts_queue()
        engine_core_outputs = self.scheduler.update_from_output(scheduler_output, model_output)
        if deferred_scheduler_output:
            if self.use_spec_decode:
                draft_token_ids = self.model_executor.take_draft_token_ids()
                assert draft_token_ids is not None
                self.scheduler.update_draft_token_ids_in_output(draft_token_ids, deferred_scheduler_output)
            grammar_output = self.scheduler.get_grammar_bitmask(deferred_scheduler_output)
            future = self.model_executor.sample_tokens(grammar_output, non_block=True)
            batch_queue.appendleft((future, deferred_scheduler_output, exec_future))
        return (engine_core_outputs, model_executed)
