"""Explicit Connector startup wiring; no model admission or stage driver."""
from __future__ import annotations

import importlib
import os
import threading

from lmcache.integration.vllm.ks_restore_metadata_bridge import (
    SchedulerRestoreLedger, WorkerRanks, WorkerRestoreBinding,
    configure_constructed_adapter,
)

_worker_publisher = None
_scheduler_begin_restore = None


def enabled():
    return os.environ.get("KS_LMCACHE_STAGE_OBSERVER") == "1"


def register_worker_publisher(publisher):
    """Call from WorkerProc after constructing its actual independent publisher."""
    global _worker_publisher
    if not callable(getattr(publisher, "publish_restore_ready", None)):
        raise ValueError("actual independent worker publisher is required")
    if _worker_publisher is not None and _worker_publisher is not publisher:
        raise RuntimeError("another worker publisher is already registered in this process")
    _worker_publisher = publisher


def bind_scheduler_begin_restore(callback):
    """Call on the core thread after its actual controller/binding is installed."""
    global _scheduler_begin_restore
    if not callable(callback):
        raise ValueError("actual core restore-registration callback is required")
    if _scheduler_begin_restore is not None and _scheduler_begin_restore != callback:
        raise RuntimeError("another core restore callback is already bound")
    _scheduler_begin_restore = callback


class WorkerRestoreRuntime:
    def __init__(self, adapter, config, parallel_strategy, publisher):
        self.publisher = publisher
        self.error = None
        self.unpublished = []
        self.stopped = threading.Event()
        adapter_module = importlib.import_module(type(adapter).__module__)
        ranks = WorkerRanks.from_parallel_groups(config, parallel_strategy)
        if (publisher.worker_id, publisher.stage, publisher.tp_size, publisher.pcp_size) != (
                ranks.global_rank, ranks.stage, parallel_strategy.tp_size, 1):
            raise ValueError("actual publisher and worker groups disagree")
        self.binding = WorkerRestoreBinding(ranks, adapter_module.StageRestoreIdentity)
        import torch
        device = torch.npu.current_device()
        self.observer = configure_constructed_adapter(
            adapter, self.binding, adapter_module.LMCacheStageObserver,
            adapter_module.MessagingFuture, lambda: torch.npu.device(device))
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="KSRestoreOutboxPublisher")
        self.thread.start()

    def _publish_once(self):
        identities = self.observer.drain_success()
        for index, identity in enumerate(identities):
            try:
                self.publisher.publish_restore_ready(identity)
            except BaseException:
                self.unpublished = identities[index:]
                raise

    def _run(self):
        try:
            while not self.stopped.is_set():
                self._publish_once()
                self.stopped.wait(0.005)
        except BaseException as error:
            self.error = error
            self.publisher.failed.set()

    def bind_metadata(self, metadata):
        if self.error is not None or self.publisher.failed.is_set() or self.stopped.is_set():
            raise RuntimeError("independent restore publisher failed or closed") from self.error
        if any(item.direction == "RETRIEVE" for item in metadata.requests):
            self.binding.bind_metadata(metadata)

    def close(self):
        """Guard adapter teardown; enqueueing is not a transport-drained claim."""
        with self.observer.lock:
            if (self.observer.entries or self.observer.retained_errors
                    or self.observer.owner_error is not None):
                raise RuntimeError("restore device lifetime is not safe for adapter shutdown")
        if self.error is not None or self.unpublished:
            raise RuntimeError("unpublished restore identities remain") from self.error
        self.stopped.set()
        self.thread.join(timeout=0.2)
        if self.thread.is_alive():
            raise RuntimeError("restore outbox publisher did not stop")
        self._publish_once()
        self.observer.assert_safe_adapter_shutdown()


def after_connector_init(connector, config, parallel_strategy, role):
    if not enabled():
        return
    if role.name == "SCHEDULER":
        connector._ks_restore_ledger = SchedulerRestoreLedger()
        return
    if role.name != "WORKER":
        raise ValueError("unsupported Connector role")
    if _worker_publisher is None:
        raise RuntimeError("actual worker publisher must be registered before Connector construction")
    connector._ks_restore_worker = WorkerRestoreRuntime(
        connector.worker_adapter, config, parallel_strategy, _worker_publisher)


def require_scheduler_ready(connector):
    if hasattr(connector, "_ks_restore_ledger") and _scheduler_begin_restore is None:
        raise RuntimeError("actual core restore callback must be bound before building metadata")


def stamp_connector_metadata(connector, metadata):
    if hasattr(connector, "_ks_restore_ledger"):
        require_scheduler_ready(connector)
        connector._ks_restore_ledger.stamp(metadata, _scheduler_begin_restore)


def before_start_load(connector, metadata):
    runtime = getattr(connector, "_ks_restore_worker", None)
    if runtime is not None:
        runtime.bind_metadata(metadata)


def before_connector_shutdown(connector):
    runtime = getattr(connector, "_ks_restore_worker", None)
    if runtime is not None:
        runtime.close()
