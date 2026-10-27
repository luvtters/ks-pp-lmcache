"""Execute real core/draft RPC/FutureWrapper AST on CPU; no device imports."""
from __future__ import annotations

import ast
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from enum import Enum, auto
from functools import partial
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time
from types import SimpleNamespace
from typing import Any, cast
import unittest
from unittest.mock import patch
import weakref

HERE = Path(__file__).resolve().parent
CORE_BEFORE = HERE.parent / "core_event_pump/core.py"
EXECUTOR_BEFORE = HERE.parent / "restore_event_binding/multiproc_executor.py"
CORE_METHODS = {"configure_ks_core_event_pump", "post_step", "step_with_batch_queue",
                "ks_model_wait_inflight", "ks_draft_wait_pending"}


def load_core(path, alias):
    nodes = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name == "_KSModelResultReader":
            nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "EngineCore":
            node.name = alias
            node.body = [n for n in node.body if isinstance(n, ast.FunctionDef)
                         and (n.name in CORE_METHODS or n.name.startswith("_ks_"))]
            nodes.append(node)
    execute(nodes, path)


def load_executor(path, alias, include_future=False):
    nodes = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef):
            if node.name == "FutureWrapper" and include_future:
                nodes.append(node)
            elif node.name == "MultiprocExecutor":
                node.name, node.bases = alias, []
                node.body = [n for n in node.body if isinstance(n, ast.FunctionDef)
                             and n.name in ("take_draft_token_ids", "take_draft_token_ids_async", "collective_rpc")]
                nodes.append(node)
            elif node.name == "WorkerProc":
                node.body = [n for n in node.body if isinstance(n, ast.ClassDef) and n.name == "ResponseStatus"]
                nodes.append(node)
    execute(nodes, path)


def execute(nodes, path):
    module = ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__",
        names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[]))
    exec(compile(module, str(path), "exec"), globals())


load_core(CORE_BEFORE, "OriginalCore")
load_executor(EXECUTOR_BEFORE, "OriginalExecutor", include_future=True)


class Broadcast:
    def __init__(self): self.sent = []
    def enqueue(self, payload): self.sent.append((payload, threading.get_ident()))


def fixture(core_cls, executor_cls):
    core = core_cls.__new__(core_cls)
    ex = executor_cls.__new__(executor_cls)
    ex.is_failed, ex.output_rank, ex.futures_queue = False, 1, deque()
    ex.rpc_broadcast_mq = Broadcast()
    ex.events, ex.reads = queue.Queue(), []
    class WrongRank:
        def dequeue(self, **kwargs): raise AssertionError("wrong draft output rank was read")
    class DraftReply:
        def dequeue(self, **kwargs):
            ex.reads.append(threading.get_ident())
            return WorkerProc.ResponseStatus.SUCCESS, [[7, 8, 9]]
    ex.response_mqs = [WrongRank(), DraftReply()]
    def poll():
        events = []
        while True:
            try: events.append(ex.events.get_nowait())
            except queue.Empty: return events
    ex.poll_stage_events = poll
    core.model_executor = ex
    core.async_scheduling, core.use_spec_decode = False, True
    core.draft_updates, core.model_updates = [], []
    core.scheduler = SimpleNamespace(update_draft_token_ids=lambda value: core.draft_updates.append(value))
    return core, ex


def configure(core, callback):
    with patch.dict(os.environ, {"KS_CORE_STAGE_EVENT_PUMP": "1"}):
        core.configure_ks_core_event_pump(callback, poll_interval=.002)


def baseline_red():
    entered, release, consumed = threading.Event(), threading.Event(), threading.Event()
    core, ex = fixture(OriginalCore, OriginalExecutor)
    def old_model():
        entered.set()
        if not release.wait(2): raise TimeoutError("CPU red model gate")
        return "old-model"
    future = FutureWrapper(ex.futures_queue, old_model)
    errors = []
    def run():
        try:
            configure(core, lambda event: consumed.set())
            core.post_step(True)
        except BaseException as error: errors.append(str(error))
        finally:
            reader = getattr(core, "_ks_result_reader", None)
            if reader: reader.close(timeout=1)
    thread = threading.Thread(target=run)
    try:
        thread.start(); assert entered.wait(1)
        ex.events.put("independent-ready")
        observed = consumed.wait(.05)
        report = {"status": "RED_CONFIRMED" if not observed else "NOT_CONFIRMED",
                  "old_draft_sync_consumed_event": observed,
                  "older_model_still_pending": not future.done(),
                  "draft_updates_before_result": len(core.draft_updates)}
    finally:
        release.set(); thread.join(2)
    assert not thread.is_alive() and not errors, errors
    return report


if __name__ == "__main__" and "--baseline-red" in sys.argv:
    report = baseline_red()
    (HERE / "red_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["status"] == "RED_CONFIRMED" else 1)


load_core(HERE / "core.py", "EngineCore")
load_executor(HERE / "multiproc_executor.py", "MultiprocExecutor")


class DraftWaitPaths(unittest.TestCase):
    def setUp(self): self.cores = []
    def tearDown(self):
        for core in self.cores:
            reader = getattr(core, "_ks_result_reader", None)
            if reader: self.assertTrue(reader.close(timeout=1))

    def configured(self, callback):
        core, ex = fixture(EngineCore, MultiprocExecutor)
        configure(core, lambda event: callback(core, ex, event))
        self.cores.append(core)
        return core, ex

    def test_00_saved_real_post_step_red(self):
        report = json.loads((HERE / "red_result.json").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "RED_CONFIRMED")
        self.assertFalse(report["old_draft_sync_consumed_event"])
        self.assertEqual(report["draft_updates_before_result"], 0)

    def test_01_post_step_pumps_draft_without_model_slot_or_early_draft_update(self):
        entered, release = threading.Event(), threading.Event()
        seen, trace = [], []
        def callback(core, ex, event):
            self.assertEqual(event, "ready")
            self.assertTrue(entered.is_set())
            self.assertFalse(older.done())
            self.assertEqual(core.draft_updates, [])
            self.assertEqual(core.ks_model_wait_inflight(), 0)
            self.assertEqual(core.ks_draft_wait_pending(), 1)
            self.assertIsNone(core._ks_waiting_model_future)
            seen.append(threading.get_ident()); release.set()
        core, ex = self.configured(callback)
        def old_model():
            entered.set()
            if not release.wait(2): raise TimeoutError("CPU green model gate")
            trace.append("old-model-result")
            return "old-model"
        older = FutureWrapper(ex.futures_queue, old_model)
        def feed():
            if entered.wait(1): ex.events.put("ready")
        feeder = threading.Thread(target=feed); feeder.start()
        try: core.post_step(True)
        finally: release.set(); feeder.join(2)
        self.assertEqual(core.draft_updates, [[[7, 8, 9]]])
        self.assertEqual(seen, [threading.get_ident()])
        self.assertEqual(ex.reads, [core._ks_result_reader.thread.ident])
        self.assertEqual(ex.rpc_broadcast_mq.sent[0][0], ("take_draft_token_ids", (), {}, 1))
        self.assertEqual(ex.rpc_broadcast_mq.sent[0][1], threading.get_ident())
        self.assertTrue(older.done())
        self.assertEqual(len(ex.futures_queue), 0)
        self.assertEqual(core.ks_draft_wait_pending(), 0)

    def test_02_deferred_real_branch_does_not_update_draft_before_rpc_result(self):
        entered, release = threading.Event(), threading.Event()
        seen = []
        def callback(core, ex, event):
            self.assertFalse(next_exec.done())
            self.assertEqual(core.draft_updates, [])
            self.assertEqual(len(core.model_updates), 1)  # Prior batch actually completed.
            self.assertEqual(core.ks_draft_wait_pending(), 1)
            self.assertEqual(core.ks_model_wait_inflight(), 0)
            seen.append(event); release.set()
        core, ex = self.configured(callback)
        prior_sched = SimpleNamespace(total_num_scheduled_tokens=1)
        deferred = SimpleNamespace(total_num_scheduled_tokens=1, pending_structured_output_tokens=True)
        prior = FutureWrapper(ex.futures_queue, lambda: "prior-final")
        def old_model():
            entered.set()
            if not release.wait(2): raise TimeoutError("CPU deferred model gate")
            return "new-execute"
        next_exec = FutureWrapper(ex.futures_queue, old_model)
        ex.execute_model = lambda *args, **kwargs: next_exec
        sample = Future(); sample.set_result("new-sample")
        ex.sample_tokens = lambda *args, **kwargs: sample
        core.batch_queue, core.batch_queue_size = deque([(prior, prior_sched, prior)]), 2
        core.is_ec_consumer, core.is_pooling_model = True, False
        core.log_error_detail = core.log_iteration_details = lambda sched: nullcontext()
        core._process_aborts_queue = lambda: None
        core.scheduler = SimpleNamespace(has_requests=lambda: True, schedule=lambda: deferred,
            update_from_output=lambda sched, output: core.model_updates.append((sched, output)) or {0: output},
            update_draft_token_ids_in_output=lambda draft, sched: core.draft_updates.append((draft, sched)),
            get_grammar_bitmask=lambda sched: "grammar")
        def feed():
            if entered.wait(1): ex.events.put("deferred-ready")
        feeder = threading.Thread(target=feed); feeder.start()
        try: result = core.step_with_batch_queue()
        finally: release.set(); feeder.join(2)
        self.assertEqual(result, ({0: "prior-final"}, True))
        self.assertEqual(seen, ["deferred-ready"])
        self.assertEqual(core.draft_updates, [([[7, 8, 9]], deferred)])
        self.assertEqual(len(core.batch_queue), 1)
        self.assertIs(core.batch_queue[0][1], deferred)
        self.assertEqual(ex.rpc_broadcast_mq.sent[0][1], threading.get_ident())

    def test_03_disabled_keeps_original_synchronous_draft_method(self):
        core, ex = fixture(EngineCore, MultiprocExecutor)
        core.post_step(True)
        self.assertEqual(ex.reads, [threading.get_ident()])
        self.assertEqual(core.draft_updates, [[[7, 8, 9]]])
        self.assertIsNone(getattr(core, "_ks_result_reader", None))

    def test_04_unverified_executor_async_interface_rejected_without_sync_fallback(self):
        core, ex = self.configured(lambda *args: None)
        called = []
        core.model_executor = SimpleNamespace(take_draft_token_ids=lambda: called.append("sync"))
        with self.assertRaisesRegex(RuntimeError, "no verified async"):
            core.post_step(True)
        self.assertEqual(called, [])

    def test_05_draft_rpc_failure_preserves_exception_and_no_draft_update(self):
        core, ex = self.configured(lambda *args: None)
        class BadReply:
            def dequeue(self, **kwargs):
                return WorkerProc.ResponseStatus.FAILURE, "CPU draft failed"
        ex.response_mqs[1] = BadReply()
        with self.assertRaisesRegex(RuntimeError, "CPU draft failed"):
            core.post_step(True)
        self.assertEqual(core.draft_updates, [])
        self.assertEqual(core.ks_draft_wait_pending(), 0)
        self.assertEqual(core.ks_model_wait_inflight(), 0)

    def test_06_wrong_thread_rejected_before_broadcast(self):
        core, ex = self.configured(lambda *args: None)
        errors = []
        def wrong_thread():
            try: core.post_step(True)
            except RuntimeError as error: errors.append(str(error))
        thread = threading.Thread(target=wrong_thread); thread.start(); thread.join(1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(ex.rpc_broadcast_mq.sent, [])

    def test_07_old_draft_entry_and_collective_rpc_AST_unchanged(self):
        def methods(path):
            return {n.name: ast.dump(n, include_attributes=False)
                for n in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                if isinstance(n, ast.FunctionDef) and n.name in ("take_draft_token_ids", "collective_rpc")}
        self.assertEqual(methods(EXECUTOR_BEFORE), methods(HERE / "multiproc_executor.py"))


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    report = {"status": "CPU_PASS" if result.wasSuccessful() else "CPU_FAIL",
              "tests_run": result.testsRun, "errors": len(result.errors), "failures": len(result.failures),
              "runtime_status": "NOT_RUNTIME_INTEGRATED", "server_actions": 0, "npu_actions": 0,
              "AST_executed": ["post_step", "step_with_batch_queue deferred branch", "collective_rpc",
                "take_draft_token_ids", "take_draft_token_ids_async", "FutureWrapper", "core sole reader"],
              "NOT_VERIFIED": ["MTP computation", "real draft payload", "NPU/model execution", "performance"]}
    (HERE / "cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
