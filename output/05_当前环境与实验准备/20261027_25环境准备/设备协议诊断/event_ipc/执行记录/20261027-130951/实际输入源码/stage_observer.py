"""NOT_RUNTIME_INTEGRATED: single-owner LMCache device-future observer.

The actual adapter patch injects the installed MessagingFuture as proxy_factory.
No device library is imported here; context and scheduler identity are mandatory
external inputs, not inferred from the LMCache KV-shard worker_id.
"""
from __future__ import annotations

from dataclasses import dataclass
import threading


@dataclass(frozen=True)
class StageRestoreIdentity:
    request_id: str
    restore_generation: int
    stage_id: int
    tp_rank: int
    pcp_rank: int
    global_worker_rank: int

    def validate(self):
        if not (type(self.request_id) is str and self.request_id):
            raise ValueError("real request identity is required")
        if any(type(value) is not int or value < 0 for value in (
            self.restore_generation, self.stage_id, self.tp_rank,
            self.pcp_rank, self.global_worker_rank)):
            raise ValueError("real restore generation and PP/TP/PCP/global ranks required")


@dataclass
class _Operation:
    identity: StageRestoreIdentity
    signature: tuple
    proxy: object
    producer_event: object
    real_future: object = None
    cancelled: bool = False


class LMCacheStageObserver:
    def __init__(self, proxy_factory, device_context_factory):
        if not callable(proxy_factory) or not callable(device_context_factory):
            raise ValueError("installed MessagingFuture and owner device context required")
        self.proxy_factory = proxy_factory
        self.device_context_factory = device_context_factory
        self.lock = threading.Lock()
        self.entries = {}
        self.latest_generation = {}
        self.success_outbox = []
        self.retained_errors = []
        self.owner = None
        self.thread = None
        self.started = threading.Event()
        self.stopping = threading.Event()
        self.wakeup = threading.Event()
        self.owner_error = None

    def start(self):
        if self.thread is not None:
            raise RuntimeError("observer owner already started")
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="LMCacheStageObserver")
        self.thread.start()
        if not self.started.wait(1) or self.owner_error is not None:
            raise RuntimeError("observer device context did not initialize")

    def reserve(self, identity, signature, producer_event):
        if not isinstance(identity, StageRestoreIdentity):
            raise ValueError("explicit StageRestoreIdentity required")
        identity.validate()
        with self.lock:
            if self.owner is None or self.stopping.is_set() or self.owner_error:
                raise RuntimeError("observer owner/context not ready")
            key = (identity.request_id, identity.restore_generation)
            previous = self.latest_generation.get(identity.request_id, -1)
            if any(operation.identity.request_id == identity.request_id
                   for operation in (*self.entries.values(), *self.retained_errors)):
                raise ValueError("previous restore still owns device lifetime")
            if key in self.entries or identity.restore_generation <= previous:
                raise ValueError("duplicate or stale restore generation")
            # Signature includes exact token span/skip/block IDs supplied by adapter.
            proxy = self.proxy_factory()
            if producer_event is not None:
                proxy.retain_reference(producer_event)
            operation = _Operation(identity, signature, proxy, producer_event)
            self.entries[key] = operation
            self.latest_generation[identity.request_id] = identity.restore_generation
            return operation

    def attach(self, operation, real_future):
        # Ownership transfers before the adapter puts the CPU proxy in its dict.
        with self.lock:
            if operation.real_future is not None:
                raise RuntimeError("real future already attached")
            operation.proxy.retain_reference(real_future)
            operation.real_future = real_future
        self.wakeup.set()
        return operation.proxy

    def submission_failed(self, operation, error):
        with self.lock:
            key = (operation.identity.request_id, operation.identity.restore_generation)
            self.entries.pop(key, None)
            # submit_retrieve raising does not prove that no DMA was submitted.
            # Retain the producer event and operation until a future safe-reap
            # protocol establishes device quiescence; cancellation is insufficient.
            operation.cancelled = True
            self.retained_errors.append(operation)
            operation.proxy.set_exception(error)

    def invalidate_requests(self, request_ids):
        # Invalidation never queries a real future and never releases its refs.
        with self.lock:
            for operation in self.entries.values():
                if operation.identity.request_id in request_ids:
                    operation.cancelled = True
            self.success_outbox = [identity for identity in self.success_outbox
                                   if identity.request_id not in request_ids]

    def drain_success(self):
        with self.lock:
            events, self.success_outbox = self.success_outbox, []
            return events

    def poll_once(self):
        if threading.get_ident() != self.owner:
            raise RuntimeError("only the device-context owner may query real futures")
        with self.lock:
            snapshot = list(self.entries.items())
        for key, operation in snapshot:
            real = operation.real_future
            if real is None:
                continue
            value, error = None, None
            try:
                # No lock is held for event import/query/result or device work.
                if not real.query():
                    continue
                value = real.result(timeout=0)
                if type(value) is not bool:
                    raise TypeError("current RetrieveResult must be bool")
            except Exception as exc:
                error = exc
            with self.lock:
                if self.entries.get(key) is not operation:
                    raise RuntimeError("observer operation identity changed")
                if error is None:
                    if value and not operation.cancelled:
                        self.success_outbox.append(operation.identity)
                    operation.proxy.set_result(value)
                else:
                    # query/import failures do not prove terminal device safety.
                    self.retained_errors.append(operation)
                    operation.proxy.set_exception(error)
                del self.entries[key]

    def _run(self):
        try:
            with self.device_context_factory():
                self.owner = threading.get_ident()
                self.started.set()
                while True:
                    self.poll_once()
                    with self.lock:
                        empty = not self.entries
                    if self.stopping.is_set() and empty:
                        return
                    self.wakeup.wait(0.005)
                    self.wakeup.clear()
        except BaseException as error:
            # Keep all operations referenced; do not fabricate success or free KV.
            self.owner_error = error
            self.started.set()

    def close(self, timeout=0.2):
        with self.lock:
            self.stopping.set()
            self.success_outbox.clear()
            for operation in self.entries.values():
                operation.cancelled = True
        self.wakeup.set()
        if self.thread is not None:
            self.thread.join(timeout=timeout)
        with self.lock:
            return (not self.entries and not self.retained_errors
                    and self.owner_error is None
                    and (self.thread is None or not self.thread.is_alive()))

    def assert_safe_adapter_shutdown(self):
        if not self.close():
            raise RuntimeError(
                "observer not device-quiescent; retain operations and do not close "
                "transfer context without explicit safe unregister/reap protocol")
