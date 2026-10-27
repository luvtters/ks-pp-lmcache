"""AST execution of actual LMCache futures and patched adapter methods; CPU only."""
from __future__ import annotations

import ast
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from typing import Any, Generic, Optional, TypeVar, cast
import unittest
from unittest.mock import patch
import weakref

HERE = Path(__file__).resolve().parent
ARCHIVE = HERE.parent.parent / "KV完成通知修改策略与隔离分支_20261022"
BASELINE = ARCHIVE / "lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py"
FUTURES = ARCHIVE / "lmcache_only_followup/sources/lmcache/v1/multiprocess/futures.py"
T = TypeVar("T")
LMCacheTimeoutError = TimeoutError
lmcache_deprecate = lambda text: lambda fn: fn
_lmcache_nvtx_annotate = lambda fn: fn
_ks_rpc_context = lambda **kwargs: nullcontext()
_ks_trace = lambda *args, **kwargs: None
_ks_completion_query = lambda *args: None
_ks_completion_result = lambda *args: nullcontext()
logger = logging.getLogger("cpu_observer_validation")
logger.addHandler(logging.NullHandler())


def execute_selected(tree, names, method_names=None):
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name in names:
            if method_names and node.name == "LMCacheMPWorkerAdapter":
                node.body = [n for n in node.body if isinstance(n, ast.FunctionDef)
                             and n.name in method_names]
            nodes.append(node)
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, "<actual-source-AST>", "exec"), globals())


execute_selected(ast.parse(FUTURES.read_text(encoding="utf-8")),
                 {"MessagingFuture", "DeviceMessagingFuture"})
ADAPTER_METHODS = {"configure_ks_stage_observer", "_ks_reserve_stage_retrieve",
    "_ks_invalidate_stage_retrieves", "submit_retrieve_request", "get_finished",
    "get_finished_with_lazy_offload", "_process_finished_stores",
    "_update_and_get_finished_store", "shutdown"}
execute_selected(ast.parse((HERE / "vllm_multi_process_adapter.py").read_text(encoding="utf-8")),
                 {"StageRestoreIdentity", "_Operation", "LMCacheStageObserver", "LMCacheMPWorkerAdapter"},
                 ADAPTER_METHODS)


def eventually(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.002)
    raise AssertionError("bounded CPU condition did not occur")


class CPUEventBackend:
    def __init__(self):
        self.ready = threading.Event()
        self.import_entered = threading.Event()
        self.import_release = threading.Event()
        self.import_release.set()
        self.threads = []
        self.sync_calls = 0
    def import_event(self, handle, device):
        self.threads.append(threading.get_ident())
        self.import_entered.set()
        if not self.import_release.wait(3):
            raise TimeoutError("CPU import fixture was not released")
        return self.ready
    def query_event(self, event):
        self.threads.append(threading.get_ident())
        return event.is_set()
    def synchronize_event(self, event, device):
        self.threads.append(threading.get_ident())
        self.sync_calls += 1
        if not event.is_set():
            raise AssertionError("observer synchronized before CPU event was ready")


def identity(request_id="R", generation=10):
    return StageRestoreIdentity(request_id, generation, 1, 3, 0, 11)


def make_adapter():
    adapter = LMCacheMPWorkerAdapter.__new__(LMCacheMPWorkerAdapter)
    adapter.is_healthy = True
    adapter.dispatcher = None
    adapter.store_futures, adapter.retrieve_futures = {}, {}
    adapter.store_events, adapter.retrieve_events = {}, {}
    adapter.error_block_ids, adapter._failed_request_ids = set(), set()
    adapter._dropped_retrieves, adapter._failed_store_requests = set(), set()
    adapter._pending_store_kv_events, adapter._completed_store_requests = {}, {}
    adapter.finished_stores, adapter.previously_finished = set(), set()
    adapter._returned_finished = set()
    adapter.lazy_offload = True
    adapter.request_telemetry = SimpleNamespace(on_request_store_finished=lambda **kwargs: None)
    adapter.model_name, adapter.world_size, adapter.worker_id = "CPU-only", 16, 0
    adapter.instance_id = "CPU-instance"
    adapter.parallel_strategy = SimpleNamespace(vllm_worker_id=11)
    adapter.kv_caches, adapter.blocks_in_chunk = [object()], 4
    adapter._ensure_heartbeat_started = lambda: None
    adapter._create_key = lambda *args, **kwargs: object()
    adapter._block_ids_per_group = lambda op: [op.flat_block_ids]
    adapter._publish_store_kv_events = lambda request_id: None
    return adapter


class ObserverActualPaths(unittest.TestCase):
    def setUp(self):
        self.adapter = make_adapter()
        self.observer = LMCacheStageObserver(MessagingFuture, nullcontext)
        self.observer.start()
        with patch.dict(os.environ, {"KS_LMCACHE_STAGE_OBSERVER": "1"}):
            self.adapter.configure_ks_stage_observer(self.observer,
                lambda request_id, op: identity(request_id, getattr(op, "generation", 10)))
        self.backends, self.raws = [], []

    def tearDown(self):
        for backend in self.backends:
            backend.import_release.set(); backend.ready.set()
        for raw in self.raws:
            if not raw.query():
                raw.set_result((b"", False))
        self.observer.close(timeout=2)
        self.assertFalse(self.observer.thread.is_alive())

    def submit(self, result=None, request_id="R", generation=10, block_ids=(5, 6)):
        raw, backend = MessagingFuture(), CPUEventBackend()
        real = DeviceMessagingFuture(raw, device=0, event_backend=backend)
        self.backends.append(backend); self.raws.append(raw)
        self.adapter.transfer_ctx = SimpleNamespace(submit_retrieve=lambda *args, **kwargs: real)
        op = SimpleNamespace(token_ids=[1, 2, 3], start=0, end=3,
            skip_first_n_tokens=0, flat_block_ids=list(block_ids), generation=generation)
        producer_event = object()
        self.adapter.submit_retrieve_request(request_id, op, producer_event)
        proxy = self.adapter.retrieve_futures[request_id][0]
        if result is not None:
            raw.set_result((b"CPU-IPC-event", result))
        return raw, backend, real, proxy, op, producer_event

    def test_01_raw_and_device_must_complete_before_success_proxy(self):
        raw, backend, real, proxy, op, producer = self.submit()
        self.assertIs(type(proxy), MessagingFuture)
        self.assertIn(real, proxy._retained_references)
        self.assertIn(producer, proxy._retained_references)
        self.assertFalse(proxy.query())
        raw.set_result((b"CPU-IPC-event", True))
        self.assertTrue(backend.import_entered.wait(1))
        self.assertFalse(proxy.query())
        self.assertEqual(self.observer.drain_success(), [])
        backend.ready.set()
        eventually(proxy.query)
        self.assertTrue(proxy.result(timeout=0))
        self.assertEqual(self.observer.drain_success(), [identity()])
        self.assertEqual(self.observer.drain_success(), [])
        self.assertEqual(set(backend.threads), {self.observer.owner})
        self.assertEqual(backend.sync_calls, 1)

    def test_02_get_finished_does_not_import_or_wait_device_event(self):
        raw, backend, real, proxy, *_ = self.submit()
        backend.import_release.clear()
        raw.set_result((b"slow-import", True))
        self.assertTrue(backend.import_entered.wait(1))
        self.assertEqual(self.adapter.get_finished(set()), (set(), set()))
        self.assertEqual(self.adapter.get_finished_with_lazy_offload(), (None, set()))
        self.assertFalse(proxy.query())
        self.assertEqual(set(backend.threads), {self.observer.owner})
        backend.import_release.set(); backend.ready.set()

    def test_03_true_result_both_actual_getters_preserve_pop_and_store_reporting(self):
        for lazy, generation in ((False, 10), (True, 11)):
            _, backend, _, proxy, *_ = self.submit(True, generation=generation)
            backend.ready.set(); eventually(proxy.query)
            store = MessagingFuture(); store.set_result(True)
            self.adapter.store_futures["S"] = store
            self.adapter.store_events["S"] = object()
            result = (self.adapter.get_finished_with_lazy_offload() if lazy else
                      self.adapter.get_finished({"S"}))
            self.assertEqual(result, (None if lazy else {"S"}, {"R"}))
            self.assertEqual(self.adapter.retrieve_futures, {})
            self.assertEqual(self.adapter.retrieve_events, {})
            self.assertEqual(self.adapter.store_futures, {})
            self.assertEqual(self.adapter.error_block_ids, set())
            if lazy:
                self.assertEqual(self.adapter._completed_store_requests, {"S": 1})

    def test_04_false_result_both_actual_getters_keep_failure_blocks_and_ids(self):
        for lazy, generation in ((False, 10), (True, 11)):
            _, backend, _, proxy, *_ = self.submit(False, generation=generation)
            backend.ready.set(); eventually(proxy.query)
            result = self.adapter.get_finished_with_lazy_offload() if lazy else self.adapter.get_finished(set())
            self.assertEqual(result[1], {"R"})
            self.assertEqual(self.adapter.error_block_ids, {5, 6})
            self.assertEqual(self.adapter._failed_request_ids, {"R"})
            self.assertNotIn("R", self.adapter.retrieve_futures)
            self.assertEqual(self.observer.drain_success(), [])

    def test_05_exception_preserves_original_getter_raise_and_owner_survives(self):
        raw, _, real, proxy, *_ = self.submit()
        error = RuntimeError("CPU raw retrieve error")
        raw.set_exception(error)
        eventually(proxy.query)
        with self.assertRaisesRegex(RuntimeError, "CPU raw retrieve error"):
            self.adapter.get_finished(set())
        self.assertIs(proxy.exception_, error)
        self.assertTrue(self.observer.thread.is_alive())
        self.assertEqual(len(self.observer.retained_errors), 1)
        self.assertIs(self.observer.retained_errors[0].real_future, real)
        self.assertEqual(self.observer.drain_success(), [])

    def test_06_unhealthy_cancels_both_getters_and_late_success_never_publishes(self):
        for lazy, generation in ((False, 10), (True, 11)):
            raw, backend, real, proxy, *_ = self.submit(generation=generation)
            self.adapter.is_healthy = False
            self.adapter._dropped_retrieves.add("DROPPED")
            result = self.adapter.get_finished_with_lazy_offload() if lazy else self.adapter.get_finished({"R"})
            self.assertEqual(result[1], {"R", "DROPPED"})
            if not lazy:
                self.assertNotIn("R", result[0])
            self.assertEqual(self.adapter.retrieve_futures, {})
            self.assertIn(real, proxy._retained_references)
            raw.set_result((b"late-event", True)); backend.ready.set()
            eventually(proxy.query)
            self.assertEqual(self.observer.drain_success(), [])
            self.adapter.is_healthy = True

    def test_07_missing_generation_and_duplicate_fail_before_real_submit(self):
        _, _, _, proxy, op, event = self.submit()
        with self.assertRaisesRegex(ValueError, "still tracked"):
            self.adapter.submit_retrieve_request("R", op, event)
        self.adapter._ks_stage_identity_resolver = lambda request_id, op: None
        with self.assertRaisesRegex(ValueError, "identity"):
            self.adapter.submit_retrieve_request("NEW", op, event)
        with self.assertRaises(ValueError):
            self.observer.reserve(StageRestoreIdentity("BAD", None, 1, 0, 0, 0), (), None)

    def test_08_only_owner_queries_real_future(self):
        with self.assertRaisesRegex(RuntimeError, "only the device-context owner"):
            self.observer.poll_once()

    def test_09_shutdown_refuses_transfer_close_for_pending_device_work(self):
        raw, backend, real, proxy, *_ = self.submit()
        self.assertFalse(self.observer.close(timeout=.02))
        self.assertTrue(self.observer.thread.is_alive())
        self.assertIn(real, proxy._retained_references)
        with self.assertRaisesRegex(RuntimeError, "not device-quiescent"):
            self.adapter.shutdown()
        self.assertTrue(self.observer.entries)
        raw.set_result((b"event", True)); backend.ready.set()
        eventually(proxy.query)
        eventually(lambda: not self.observer.thread.is_alive())
        self.assertEqual(self.observer.drain_success(), [])

    def test_10_default_disabled_original_future_untouched_and_unhealthy_drop(self):
        other = make_adapter()
        with patch.dict(os.environ, {"KS_LMCACHE_STAGE_OBSERVER": "0"}):
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                other.configure_ks_stage_observer(self.observer, lambda *args: identity())
        raw, backend = MessagingFuture(), CPUEventBackend()
        real = DeviceMessagingFuture(raw, device=0, event_backend=backend)
        other.transfer_ctx = SimpleNamespace(submit_retrieve=lambda *args, **kwargs: real)
        op = SimpleNamespace(token_ids=[1], start=0, end=1, skip_first_n_tokens=0, flat_block_ids=[4])
        other.submit_retrieve_request("OFF", op, None)
        self.assertIs(other.retrieve_futures["OFF"][0], real)
        other.is_healthy = False
        other.submit_retrieve_request("DROP", op, None)
        self.assertEqual(other.get_finished(set())[1], {"OFF", "DROP"})

    def test_11_getter_ast_preserved_except_explicit_unhealthy_invalidation(self):
        baseline = ast.parse(BASELINE.read_text(encoding="utf-8"))
        candidate = ast.parse((HERE / "vllm_multi_process_adapter.py").read_text(encoding="utf-8"))
        names = {"get_finished", "get_finished_with_lazy_offload"}
        def methods(tree):
            return {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name in names}
        before, after = methods(baseline), methods(candidate)
        class RemoveInvalidation(ast.NodeTransformer):
            def visit_Expr(self, node):
                if (isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                        and node.value.func.attr == "_ks_invalidate_stage_retrieves"):
                    return None
                return node
        for name in names:
            self.assertEqual(ast.dump(before[name], include_attributes=False),
                ast.dump(RemoveInvalidation().visit(after[name]), include_attributes=False))

    def test_12_submit_error_does_not_leave_unattached_operation(self):
        def broken(*args, **kwargs):
            raise RuntimeError("CPU submit failed")
        self.adapter.transfer_ctx = SimpleNamespace(submit_retrieve=broken)
        op = SimpleNamespace(token_ids=[1], start=0, end=1, skip_first_n_tokens=0, flat_block_ids=[4])
        with self.assertRaisesRegex(RuntimeError, "CPU submit failed"):
            self.adapter.submit_retrieve_request("R", op, None)
        self.assertEqual(self.observer.entries, {})
        self.assertEqual(self.adapter.retrieve_futures, {})

    def test_13_completed_generation_cannot_be_reused_or_regress(self):
        _, backend, _, proxy, op, event = self.submit(True)
        backend.ready.set(); eventually(proxy.query)
        self.adapter.get_finished(set())
        self.observer.drain_success()
        for stale in (10, 9):
            op.generation = stale
            with self.assertRaisesRegex(ValueError, "duplicate or stale"):
                self.adapter.submit_retrieve_request("R", op, event)
        self.assertEqual(self.adapter.retrieve_futures, {})

    def test_14_unknown_submit_failure_retains_event_and_blocks_shutdown_new_generation(self):
        class ProducerEvent:
            pass
        event = ProducerEvent()
        retained_event = weakref.ref(event)
        calls = []
        def unknown_submission(*args, **kwargs):
            calls.append(args[0])
            raise RuntimeError("submission may have partially issued device work")
        self.adapter.transfer_ctx = SimpleNamespace(submit_retrieve=unknown_submission)
        op = SimpleNamespace(token_ids=[1], start=0, end=1,
                             skip_first_n_tokens=0, flat_block_ids=[4], generation=10)
        with self.assertRaisesRegex(RuntimeError, "partially issued"):
            self.adapter.submit_retrieve_request("R", op, event)
        self.assertEqual(self.observer.entries, {})
        self.assertEqual(len(self.observer.retained_errors), 1)
        retained = self.observer.retained_errors[0]
        self.assertIs(retained.producer_event, event)
        self.assertIn(event, retained.proxy._retained_references)
        self.assertTrue(retained.proxy.query())
        with self.assertRaisesRegex(RuntimeError, "partially issued"):
            retained.proxy.result(timeout=0)
        del event
        self.assertIsNotNone(retained_event())
        self.assertEqual(self.observer.drain_success(), [])
        op.generation = 11
        with self.assertRaisesRegex(ValueError, "still owns device lifetime"):
            self.adapter.submit_retrieve_request("R", op, retained_event())
        self.assertEqual(calls, ["R"])
        self.assertFalse(self.observer.close(timeout=.1))
        with self.assertRaisesRegex(RuntimeError, "not device-quiescent"):
            self.adapter.shutdown()

    def test_15_pending_old_generation_rejects_new_until_real_completion(self):
        raw, backend, _, proxy, op, event = self.submit()
        self.adapter.is_healthy = False
        self.assertEqual(self.adapter.get_finished(set())[1], {"R"})
        self.adapter.is_healthy = True
        op.generation = 11
        with self.assertRaisesRegex(ValueError, "still owns device lifetime"):
            self.adapter.submit_retrieve_request("R", op, event)
        self.assertEqual(self.adapter.retrieve_futures, {})
        self.assertTrue(self.observer.entries)
        raw.set_result((b"old-completion", True)); backend.ready.set()
        eventually(proxy.query)
        self.assertEqual(self.observer.entries, {})
        self.assertEqual(self.observer.drain_success(), [])
        _, newer_backend, _, newer_proxy, *_ = self.submit(True, generation=11)
        newer_backend.ready.set(); eventually(newer_proxy.query)
        self.assertEqual(self.observer.drain_success(), [identity(generation=11)])


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    report = {"status": "CPU_PASS" if result.wasSuccessful() else "CPU_FAIL",
              "tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "runtime_status": "NOT_RUNTIME_INTEGRATED", "actual_adapter": str(BASELINE),
              "actual_futures": str(FUTURES), "server_actions": 0, "npu_actions": 0,
              "AST_executed": ["MessagingFuture", "DeviceMessagingFuture", "submit_retrieve_request",
                "get_finished", "get_finished_with_lazy_offload", "shutdown guard", "LMCacheStageObserver"],
              "NOT_VERIFIED": ["scheduler generation metadata", "PP/TP identity plumbing", "NPU thread context",
                "real event IPC", "runtime safe unregister/reap", "sidechannel/core integration", "performance"]}
    (HERE / "cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
