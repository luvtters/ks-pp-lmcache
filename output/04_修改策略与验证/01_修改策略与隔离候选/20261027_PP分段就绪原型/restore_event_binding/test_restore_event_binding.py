"""CPU-only restore-event binding checks; no framework/device imports."""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from dataclasses import dataclass, replace
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import threading
from threading import Thread
import time
from types import SimpleNamespace
from typing import Any, Callable, cast
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CANDIDATE = HERE / "multiproc_executor.py"


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_actual_executor():
    selected = []
    for node in ast.parse(CANDIDATE.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id.startswith("_KS_")
                for target in node.targets):
            selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "_ks_identity":
            selected.append(node)
        elif isinstance(node, ast.ClassDef):
            if node.name in {"KSStageCommand", "KSStageEvent", "KSRestoreEvent",
                             "_KSStageEventPublisher", "FutureWrapper"}:
                selected.append(node)
            elif node.name == "MultiprocExecutor":
                node.bases = []
                node.body = [method for method in node.body
                             if isinstance(method, ast.FunctionDef) and method.name in {
                                 "_init_stage_event_channels", "_close_stage_event_channels",
                                 "submit_stage_command", "poll_stage_events"}]
                selected.append(node)
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            *selected]
    exec(compile(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])),
                 str(CANDIDATE), "exec"), globals())


READY = CANDIDATE.exists() and (HERE / "restore_binding.py").exists()
if READY:
    load_actual_executor()
    binding = load_file("ks_restore_binding", HERE / "restore_binding.py")
    controller_module = load_file("ks_restore_controller", ROOT / "controller/stage_controller.py")
    observer_module = load_file("ks_restore_observer", ROOT / "lmcache_stage_observer/stage_observer.py")


class RestoreBindingChecks(unittest.TestCase):
    def setUp(self):
        if not READY:
            self.skipTest("binding implementation absent during red check")
        self.ctx = multiprocessing.get_context("spawn")
        self.ex = MultiprocExecutor.__new__(MultiprocExecutor)
        self.ex.parallel_config = SimpleNamespace(nnodes_within_dp=1)
        self.ex.world_size = self.ex.local_world_size = 16
        self.ex.is_failed = False
        self.ex.rpc_broadcast_mq = SimpleNamespace(enqueue=lambda item: None)
        with patch.dict(os.environ, {"KS_PP_STAGE_EVENTS": "1"}):
            self.ex._init_stage_event_channels(self.ctx, 8, 2, 1)
        self.controller = controller_module.StageController((range(8), range(8, 16)))
        self.callback = binding.RestoreEventBinding(self.controller, KSRestoreEvent)
        self.publishers = []

    def tearDown(self):
        if not READY:
            return
        for publisher in self.publishers:
            publisher.close()
            self.assertFalse(publisher.thread.is_alive())
        self.ex._close_stage_event_channels()

    def publisher(self, rank):
        publisher = _KSStageEventPublisher(*self.ex._ks_stage_channels[rank],
                                           rank, rank // 8, tp_size=8, pcp_size=1)
        self.publishers.append(publisher)
        return publisher

    def identity(self, rank=0, request="request-one", generation=3):
        return observer_module.StageRestoreIdentity(request, generation, rank // 8,
                                                    rank % 8, 0, rank)

    def events(self, count=1):
        found = []
        deadline = time.monotonic() + 2
        while len(found) < count and time.monotonic() < deadline:
            found.extend(self.ex.poll_stage_events())
            time.sleep(.001)
        self.assertEqual(len(found), count)
        return found

    def put(self, rank, event):
        self.ex._ks_stage_channels[rank][0].put(event)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                rows = self.ex.poll_stage_events()
            except Exception:
                raise
            if rows:
                return rows
            time.sleep(.001)
        self.fail("multiprocessing transport did not deliver")

    def test_01_shared_sequence_preserves_control_correlation(self):
        publisher = self.publisher(0)
        command = KSStageCommand("control", "logical-batch", 9, 0, "input_ready")
        self.ex._ks_stage_pending["control"] = [command, {0}]
        publisher.publish("control", "logical-batch", 9, "CONTROL_RESULT", True)
        publisher.publish_restore_ready(self.identity())
        first, second = self.events(2)
        self.assertEqual((first.sequence, second.sequence), (1, 2))
        self.assertEqual(first.batch_id, "logical-batch")
        self.assertEqual(second.request_id, "request-one")
        self.assertFalse(hasattr(second, "batch_id"))
        self.assertNotIn("control", self.ex._ks_stage_pending)

    def test_02_publisher_rejects_forged_worker_stage_tp_pcp(self):
        publisher = self.publisher(8)
        good = self.identity(8)
        for field, value in (("global_worker_rank", 0), ("stage_id", 0),
                             ("tp_rank", 7), ("pcp_rank", 1)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                publisher.publish_restore_ready(replace(good, **{field: value}))
        self.assertEqual(publisher.sequence, 0)

    def test_03_poll_rejects_cross_rank_restore_payload(self):
        bad = KSRestoreEvent("bad", "request-one", 3, 0, 0, 1, 0, 1)
        with self.assertRaises(ValueError):
            self.put(0, bad)
        self.assertEqual(self.ex._ks_stage_sequences, {})

    def test_04_unknown_request_does_not_create_restore(self):
        self.publisher(0).publish_restore_ready(self.identity())
        self.callback(self.events()[0])
        self.assertEqual(self.controller._restores, {})
        self.assertEqual(self.controller.inflight_count, 0)

    def test_05_stale_generation_and_duplicate_do_not_release(self):
        self.controller.begin_restore("request-one", 4)
        publisher = self.publisher(0)
        publisher.publish_restore_ready(self.identity(generation=3))
        self.callback(self.events()[0])
        self.assertEqual(self.controller._restores["request-one"].ready, [set(), set()])
        publisher.publish_restore_ready(self.identity(generation=4))
        publisher.publish_restore_ready(self.identity(generation=4))
        for event in self.events(2):
            self.callback(event)
        self.assertEqual(self.controller._restores["request-one"].ready, [{0}, set()])

    def test_06_restore_before_admit_registers_only_stage_workers(self):
        self.controller.begin_restore("request-one", 3)
        for rank in range(8):
            self.publisher(rank).publish_restore_ready(self.identity(rank))
        for event in self.events(8):
            self.callback(event)
        self.assertEqual(self.controller.inflight_count, 0)
        self.controller.admit_batch("logical-batch", 9, [("request-one", 3)], [(4095,)])
        self.assertIsNotNone(self.controller.take_ready(0))
        self.assertIsNone(self.controller.take_ready(1))

    def test_07_drain_bridge_uses_real_observer_success_outbox(self):
        observer_tests = load_file("ks_restore_observer_tests",
            ROOT / "lmcache_stage_observer/test_lmcache_stage_observer.py")
        observer = observer_module.LMCacheStageObserver(
            observer_tests.MessagingFuture, nullcontext)
        observer.start()
        try:
            operation = observer.reserve(self.identity(), ("explicit-cpu-span",), None)
            real = observer_tests.MessagingFuture()
            proxy = observer.attach(operation, real)
            self.assertEqual(binding.publish_observer_success(observer, self.publisher(0)), 0)
            real.set_result(True)  # CPU completion fixture; not an NPU event claim.
            self.assertTrue(proxy.wait(1))
            self.assertEqual(binding.publish_observer_success(observer, self.publishers[0]), 1)
            event = self.events()[0]
            self.assertIsInstance(event, KSRestoreEvent)
            self.assertEqual(observer.drain_success(), [])
        finally:
            self.assertTrue(observer.close(timeout=1))

    def test_08_unsupported_pcp_layout_rejected(self):
        with self.assertRaises(ValueError):
            binding.RestoreEventBinding(self.controller, KSRestoreEvent, pcp_size=2)
        publisher = _KSStageEventPublisher(*self.ex._ks_stage_channels[1], 1, 0,
                                           tp_size=8, pcp_size=2)
        self.publishers.append(publisher)
        with self.assertRaises(ValueError):
            publisher.publish_restore_ready(self.identity(1))

    def test_09_callback_keeps_stage_event_for_existing_handler(self):
        seen = []
        callback = binding.RestoreEventBinding(self.controller, KSRestoreEvent,
                                               on_stage_event=seen.append)
        event = KSStageEvent("known", "logical-batch", 9, 0, 0, 1,
                             "INPUT_DEVICE_READY", True)
        self.assertIsNone(callback(event))
        self.assertEqual(seen, [event])

    def test_10_actual_core_consumes_restore_while_old_model_pending(self):
        core_tests = load_file("ks_restore_core_tests", ROOT / "core_event_pump/test_core_event_pump.py")
        core_tests.ast_classes(ROOT / "core_event_pump/core.py",
                               {"EngineCore", "_KSModelResultReader"},
                               core_tests.METHODS, "RestoreCore")
        core = core_tests.RestoreCore.__new__(core_tests.RestoreCore)
        entered, release, consumed = threading.Event(), threading.Event(), threading.Event()
        old_queue = deque()
        def slow_model():
            entered.set()
            if not release.wait(3):
                raise TimeoutError("model fixture gate not released")
            return "final-model"
        old = FutureWrapper(old_queue, slow_model)
        output = SimpleNamespace(total_num_scheduled_tokens=1)
        core.batch_queue, core.batch_queue_size = deque([(old, output, old)]), 2
        committed = []
        core.scheduler = SimpleNamespace(has_requests=lambda: False,
            update_from_output=lambda sched, result: committed.append(result) or {0: result})
        core.model_executor = self.ex
        core.log_error_detail = core.log_iteration_details = lambda sched: nullcontext()
        core._process_aborts_queue = lambda: None
        self.controller.begin_restore("request-one", 3)
        publisher = self.publisher(0)
        errors = []
        def on_event(event):
            self.callback(event)
            consumed.set()
        def run_core():
            try:
                with patch.dict(os.environ, {"KS_CORE_STAGE_EVENT_PUMP": "1"}):
                    core.configure_ks_core_event_pump(on_event)
                core.step_with_batch_queue()
            except BaseException as error:
                errors.append(repr(error))
        thread = Thread(target=run_core)
        try:
            thread.start()
            self.assertTrue(entered.wait(1))
            publisher.publish_restore_ready(self.identity())
            self.assertTrue(consumed.wait(1), errors)
            self.assertEqual(self.controller._restores["request-one"].ready[0], {0})
            self.assertFalse(old.done())
            self.assertEqual(core.ks_model_wait_inflight(), 1)
            self.assertIs(core._ks_waiting_model_future, old)
            self.assertEqual(committed, [])
        finally:
            release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(committed, ["final-model"])

    def test_11_old_model_result_and_control_handler_unchanged(self):
        before = ast.parse((ROOT / "control_sidechannel/multiproc_executor.py").read_text(encoding="utf-8"))
        after = ast.parse(CANDIDATE.read_text(encoding="utf-8"))
        def selected(tree, class_name, method=None):
            node = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                        and node.name == class_name)
            if method:
                node = next(node for node in node.body if isinstance(node, ast.FunctionDef)
                            and node.name == method)
            return ast.dump(node, include_attributes=False)
        for class_name, method in (("FutureWrapper", None),
                                   ("MultiprocExecutor", "collective_rpc"),
                                   ("WorkerProc", "_handle_stage_command")):
            self.assertEqual(selected(before, class_name, method), selected(after, class_name, method))

    def test_12_long_request_id_through_actual_queue_poll_and_binding(self):
        request_id = "request-" + "x" * 4096
        self.controller.begin_restore(request_id, 3)
        self.publisher(0).publish_restore_ready(self.identity(request=request_id))
        event = self.events()[0]
        self.assertEqual(event.request_id, request_id)
        self.assertLessEqual(len(event.event_id), 128)
        self.assertIsNone(self.callback(event))
        self.assertEqual(self.controller._restores[request_id].ready, [{0}, set()])


class RequiredIncrement(unittest.TestCase):
    def test_required_restore_binding_exists(self):
        self.assertTrue(READY, "new restore envelope/binding is not implemented")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"status": "PASS_CPU_BINDING_ONLY" if result.wasSuccessful() else "FAIL",
              "tests": result.testsRun, "failures": len(result.failures),
              "errors": len(result.errors), "skipped": len(result.skipped),
              "actual_executor": str(CANDIDATE), "not_runtime_integrated": True}
    target = HERE / ("cpu_result.json" if READY else "red_result.json")
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not result.wasSuccessful())
