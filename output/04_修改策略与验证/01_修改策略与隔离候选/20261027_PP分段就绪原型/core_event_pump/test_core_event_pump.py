"""CPU checks execute actual EngineCore and archived FutureWrapper methods."""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from dataclasses import dataclass
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time
from types import SimpleNamespace
from typing import Any, Callable, cast
import unittest
from unittest.mock import patch
import weakref

HERE = Path(__file__).resolve().parent
SNAPSHOT = HERE.parents[3] / "05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1"
BASELINE = SNAPSHOT / "engine/core.py"
EXECUTOR = SNAPSHOT / "executor/multiproc_executor.py"


def ast_classes(path, names, methods=None, alias=None):
    nodes = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name in names:
            if node.name == "EngineCore" and methods:
                node.body = [n for n in node.body if isinstance(n, ast.FunctionDef)
                             and (n.name in methods or n.name.startswith("_ks_"))]
                if alias:
                    node.name = alias
            nodes.append(node)
    module = ast.fix_missing_locations(ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[]))
    exec(compile(module, str(path), "exec"), globals())


METHODS = {"step", "step_with_batch_queue", "configure_ks_core_event_pump", "ks_model_wait_inflight"}
ast_classes(EXECUTOR, {"FutureWrapper"})
ast_classes(BASELINE, {"EngineCore"}, METHODS, "OriginalCore")


def old_path_red_case():
    entered, release, consumed = threading.Event(), threading.Event(), threading.Event()
    wrappers = deque()
    def slow_model():
        entered.set()
        if not release.wait(2):
            raise TimeoutError("CPU red case was not released")
        return "final-model"
    future = FutureWrapper(wrappers, slow_model)
    core = OriginalCore.__new__(OriginalCore)
    output = SimpleNamespace(total_num_scheduled_tokens=1)
    core.batch_queue, core.batch_queue_size = deque([(future, output, future)]), 2
    core.scheduler = SimpleNamespace(has_requests=lambda: False,
        update_from_output=lambda sched, result: {0: result})
    core.model_executor = SimpleNamespace(poll_stage_events=lambda: consumed.set() or ["ready"])
    core.log_error_detail = core.log_iteration_details = lambda sched: nullcontext()
    core._process_aborts_queue = lambda: None
    errors = []
    def run():
        try: core.step_with_batch_queue()
        except BaseException as error: errors.append(str(error))
    thread = threading.Thread(target=run)
    try:
        thread.start()
        assert entered.wait(1)
        observed = consumed.wait(.05)
        return {"status": "RED_CONFIRMED" if not observed else "NOT_CONFIRMED",
                "core_consumed_stage_event_before_old_result": observed,
                "old_model_still_pending": not future.done(),
                "actual_core": str(BASELINE), "actual_future": str(EXECUTOR)}
    finally:
        release.set(); thread.join(2)
        assert not thread.is_alive() and not errors, errors


if __name__ == "__main__" and "--baseline-red" in sys.argv:
    report = old_path_red_case()
    (HERE / "red_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["status"] == "RED_CONFIRMED" else 1)


ast_classes(HERE / "core.py", {"_KSModelResultReader", "EngineCore"}, METHODS)
sys.path.insert(0, str(HERE.parent))
from controller.stage_controller import StageController


class CountedFuture(FutureWrapper):
    def __init__(self, *args, **kwargs):
        self.result_threads = []
        super().__init__(*args, **kwargs)
    def result(self, timeout=None):
        self.result_threads.append(threading.get_ident())
        return super().result(timeout)


def fixture_core(future, exec_future=None, queued=True):
    core = EngineCore.__new__(EngineCore)
    output = SimpleNamespace(total_num_scheduled_tokens=1)
    core.batch_queue, core.batch_queue_size = deque([(future, output, exec_future or future)]), 2
    core.events, core.updates = queue.Queue(), []
    def update(sched, result):
        core.updates.append((sched, result, threading.get_ident()))
        return {0: result}
    core.scheduler = SimpleNamespace(has_requests=lambda: not queued,
        schedule=lambda: output, get_grammar_bitmask=lambda sched: "grammar",
        update_from_output=update)
    def poll():
        events = []
        while True:
            try: events.append(core.events.get_nowait())
            except queue.Empty: return events
    core.model_executor = SimpleNamespace(poll_stage_events=poll,
        execute_model=lambda *args, **kwargs: future)
    core.log_error_detail = core.log_iteration_details = lambda sched: nullcontext()
    core._process_aborts_queue = lambda: None
    return core


class CoreEventPumpPaths(unittest.TestCase):
    def setUp(self):
        self.cores = []

    def tearDown(self):
        for core in self.cores:
            reader = getattr(core, "_ks_result_reader", None)
            if reader is not None:
                self.assertTrue(reader.close(timeout=1))
                self.assertFalse(reader.thread.is_alive())

    def configure(self, core, callback, dispatcher=None):
        with patch.dict(os.environ, {"KS_CORE_STAGE_EVENT_PUMP": "1"}):
            core.configure_ks_core_event_pump(callback, dispatcher, poll_interval=.002)
        self.cores.append(core)

    def test_00_original_real_path_red_record(self):
        report = json.loads((HERE / "red_result.json").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "RED_CONFIRMED")
        self.assertFalse(report["core_consumed_stage_event_before_old_result"])
        self.assertTrue(report["old_model_still_pending"])

    def test_01_real_batch_queue_consumes_event_while_old_result_pending_without_releasing_slot(self):
        entered, release = threading.Event(), threading.Event()
        trace, wrappers = [], deque()
        def slow():
            entered.set()
            if not release.wait(3): raise TimeoutError("CPU model gate not released")
            trace.append("model-finished")
            return "final-model"
        future = CountedFuture(wrappers, slow)
        core = fixture_core(future)
        controller = StageController(((0,), (1,)), max_inflight_batches=1)
        controller.begin_restore("R", 7)
        controller.admit_batch("B", 3, (("R", 7),), ((0,),))
        core_thread = threading.get_ident()
        def callback(event):
            self.assertEqual(event, "CPU-stage-ready")
            self.assertEqual(threading.get_ident(), core_thread)
            self.assertFalse(future.done())
            self.assertEqual(core.updates, [])
            self.assertEqual(len(core.batch_queue), 0)
            self.assertEqual(core.ks_model_wait_inflight(), 1)
            self.assertIs(core._ks_waiting_model_future, future)
            self.assertEqual(controller.inflight_count, 1)
            self.assertIsNone(controller.pop_committable())
            controller.mark_kv_ready("R", 7, 0, 0, device_complete=True)
            self.assertEqual(controller.inflight_count, 1)
            trace.append("core-consumed-stage")
            release.set()
        self.configure(core, callback)
        def feed():
            if entered.wait(1): core.events.put("CPU-stage-ready")
        feeder = threading.Thread(target=feed); feeder.start()
        try:
            result = core.step_with_batch_queue()
        finally:
            release.set(); feeder.join(2)
        self.assertEqual(result, ({0: "final-model"}, False))
        self.assertEqual(trace, ["core-consumed-stage", "model-finished"])
        self.assertEqual(len(core.updates), 1)
        self.assertEqual(core.updates[0][2], core_thread)
        self.assertEqual(core.ks_model_wait_inflight(), 0)
        self.assertEqual(controller.inflight_count, 1)
        self.assertEqual(future.result_threads, [core._ks_result_reader.thread.ident])

    def test_02_real_step_seam_and_sample_wait_both_pump_on_core(self):
        entered, release = threading.Event(), threading.Event()
        wrappers, seen = deque(), []
        def execute():
            entered.set()
            if not release.wait(2): raise TimeoutError("CPU execute gate")
            return None
        execution = CountedFuture(wrappers, execute)
        sample = CountedFuture(wrappers, lambda: "sample-token")
        core = fixture_core(execution, queued=False)
        core.model_executor.sample_tokens = lambda grammar, non_block: sample
        def callback(event):
            seen.append((event, threading.get_ident()))
            self.assertEqual(core.updates, [])
            release.set()
        self.configure(core, callback)
        core.events.put("ready")
        self.assertEqual(core.step(), ({0: "sample-token"}, True))
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][1], threading.get_ident())
        self.assertEqual(execution.result_threads, [core._ks_result_reader.thread.ident])
        self.assertEqual(sample.result_threads, [core._ks_result_reader.thread.ident])

    def test_03_original_future_fifo_and_single_reader_preserved(self):
        wrappers, trace = deque(), []
        older = CountedFuture(wrappers, lambda: trace.append("execute") or "execute-result")
        latest = CountedFuture(wrappers, lambda: trace.append("sample") or "sample-result")
        core = fixture_core(latest, older)
        self.configure(core, lambda event: None)
        self.assertEqual(core.step_with_batch_queue()[0], {0: "sample-result"})
        self.assertEqual(trace, ["execute", "sample"])
        self.assertTrue(older.done() and latest.done())
        self.assertEqual(latest.result_threads, [core._ks_result_reader.thread.ident])
        self.assertEqual(len(wrappers), 0)

    def test_04_same_future_none_error_path_uses_cpu_cache_not_second_wrapper_result(self):
        future = CountedFuture(deque(), lambda: None)
        core = fixture_core(future)
        self.configure(core, lambda event: None)
        with self.assertRaisesRegex(RuntimeError, "unexpected error"):
            core.step_with_batch_queue()
        self.assertEqual(future.result_threads, [core._ks_result_reader.thread.ident])
        self.assertEqual(core.updates, [])

    def test_05_disabled_original_result_thread_and_no_reader(self):
        future = CountedFuture(deque(), lambda: "unchanged")
        core = fixture_core(future)
        with patch.dict(os.environ, {"KS_CORE_STAGE_EVENT_PUMP": "0"}):
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                core.configure_ks_core_event_pump(lambda event: None)
        self.assertEqual(core.step_with_batch_queue()[0], {0: "unchanged"})
        self.assertEqual(future.result_threads, [threading.get_ident()])
        self.assertIsNone(getattr(core, "_ks_result_reader", None))

    def test_06_callback_cannot_reenter_step_or_read_model_future(self):
        release = threading.Event()
        future = CountedFuture(deque(), lambda: release.wait(2) and "done")
        core, rejected = fixture_core(future), []
        def callback(event):
            for action in (core.step, core.step_with_batch_queue,
                           lambda: core._ks_wait_model_future(future)):
                try: action()
                except RuntimeError as error: rejected.append(str(error))
            release.set()
        self.configure(core, callback)
        core.events.put("ready")
        self.assertEqual(core.step_with_batch_queue()[0], {0: "done"})
        self.assertEqual(len(rejected), 3)
        self.assertEqual(len(core.updates), 1)

    def test_07_callback_error_keeps_model_lifetime_and_blocks_scheduler_update(self):
        release = threading.Event()
        future = CountedFuture(deque(), lambda: release.wait(2) and "done")
        core = fixture_core(future)
        def callback(event):
            self.assertEqual(core.ks_model_wait_inflight(), 1)
            release.set()
            raise ValueError("CPU callback failed")
        self.configure(core, callback)
        core.events.put("ready")
        with self.assertRaisesRegex(ValueError, "CPU callback failed"):
            core.step_with_batch_queue()
        self.assertTrue(future.done())
        self.assertEqual(core.updates, [])
        self.assertEqual(core.ks_model_wait_inflight(), 0)

    def test_08_model_exception_stays_exception_and_does_not_update(self):
        def broken(): raise RuntimeError("CPU model error")
        core = fixture_core(CountedFuture(deque(), broken))
        self.configure(core, lambda event: None)
        with self.assertRaisesRegex(RuntimeError, "CPU model error"):
            core.step_with_batch_queue()
        self.assertEqual(core.updates, [])

    def test_09_stage_dispatch_callback_stays_on_core_thread(self):
        core = fixture_core(CountedFuture(deque(), lambda: "model"))
        order = []
        def callback(event): order.append(("register", event, threading.get_ident()))
        def dispatcher(event): order.append(("stage-command", event, threading.get_ident()))
        self.configure(core, callback, dispatcher)
        core.events.put("event-object")
        core.step_with_batch_queue()
        self.assertEqual([row[0] for row in order], ["register", "stage-command"])
        self.assertEqual({row[2] for row in order}, {threading.get_ident()})


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    report = {"status": "CPU_PASS" if result.wasSuccessful() else "CPU_FAIL",
              "tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "runtime_status": "NOT_RUNTIME_INTEGRATED", "actual_core": str(BASELINE),
              "actual_future": str(EXECUTOR), "server_actions": 0, "npu_actions": 0,
              "AST_executed": ["EngineCore.step", "EngineCore.step_with_batch_queue",
                "FutureWrapper", "_KSModelResultReader", "core callback/poll/wait seams"],
              "NOT_VERIFIED": ["real EngineCore bootstrap", "executor aggregation thread affinity",
                "live ModelRunnerOutput", "scheduler stage dispatch integration", "NPU/model/performance"]}
    (HERE / "cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
