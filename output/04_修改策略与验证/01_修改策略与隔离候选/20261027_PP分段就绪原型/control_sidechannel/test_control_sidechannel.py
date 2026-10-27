"""CPU-only checks execute AST methods from the actual patched vLLM file."""
from __future__ import annotations

import ast
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future, InvalidStateError
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
import json
import multiprocessing
import os
from pathlib import Path
import pickle
import queue
import sys
import threading
from threading import Thread
import time
import traceback
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent.parent / ".." / ".." / ".."
BASELINE = (HERE.parents[3] / "05_当前环境与实验准备" /
            "20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/executor/multiproc_executor.py")


def load_actual_methods():
    path = HERE / "multiproc_executor.py"
    if not path.exists():
        return
    tree = ast.parse(path.read_text(encoding="utf-8"))
    selected = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            names = [n.id for n in ast.walk(node) if isinstance(n, ast.Name)]
            if any(n in ("_KS_STAGE_METHODS", "_KS_EVENT_KINDS") for n in names):
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "_ks_identity":
            selected.append(node)
        elif isinstance(node, ast.ClassDef):
            if node.name in ("KSStageCommand", "KSStageEvent", "_KSStageEventPublisher", "FutureWrapper"):
                selected.append(node)
            elif node.name == "MultiprocExecutor":
                node.bases = []
                node.body = [n for n in node.body if isinstance(n, ast.FunctionDef) and
                             (n.name.startswith(("_init_stage", "_close_stage", "submit_stage", "poll_stage"))
                              or n.name == "collective_rpc")]
                selected.append(node)
            elif node.name == "WorkerProc":
                node.body = [n for n in node.body if isinstance(n, ast.ClassDef) and n.name == "ResponseStatus"
                             or isinstance(n, ast.FunctionDef) and n.name in
                             ("_init_stage_event_channel", "_handle_stage_command", "worker_busy_loop")]
                selected.append(node)
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *selected]
    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    exec(compile(module, str(path), "exec"), globals())


load_actual_methods()


class BroadcastFixture:
    def __init__(self):
        self.sent = []
    def enqueue(self, item):
        self.sent.append(item)


class StopLoop(BaseException):
    pass


class InputFixture:
    def __init__(self, items):
        self.items = iter(items)
    def dequeue(self, **kwargs):
        try:
            return next(self.items)
        except StopIteration:
            raise StopLoop()


def config():
    return SimpleNamespace(parallel_config=SimpleNamespace(
        nnodes_within_dp=1, tensor_parallel_size=8,
        prefill_context_parallel_size=1))


def make_executor(enabled=True, workers=16):
    ex = MultiprocExecutor.__new__(MultiprocExecutor)
    ex.parallel_config = config().parallel_config
    ex.local_world_size = ex.world_size = workers
    ex.is_failed = False
    ex.futures_queue = deque()
    ex.rpc_broadcast_mq = BroadcastFixture()
    with patch.dict(os.environ, {"KS_PP_STAGE_EVENTS": "1" if enabled else "0"}):
        ex._init_stage_event_channels(multiprocessing.get_context("spawn"), 8, 2, 1)
    return ex


def wait_events(ex, count):
    rows = []
    deadline = time.monotonic() + 3
    while len(rows) < count and time.monotonic() < deadline:
        rows.extend(ex.poll_stage_events())
        if len(rows) < count:
            time.sleep(0.002)
    if len(rows) != count:
        raise AssertionError(f"expected {count} stage events, got {len(rows)}")
    return rows


def publish_from_spawn(channel):
    # AST-extracted candidate class executes in a real independent CPU process.
    pub = _KSStageEventPublisher(*channel, 8, 1)
    pub.publish("device-1", "batch-A", 7, "KV_DEVICE_READY", True)
    deadline = time.monotonic() + 2
    while not pub.pending.empty() and time.monotonic() < deadline:
        time.sleep(0.002)
    # Keep the process alive until parent confirms consumption, avoiding a fake
    # success caused by cancel_join_thread discarding an undrained feeder.
    while not channel[1].is_set() and time.monotonic() < deadline:
        time.sleep(0.002)
    pub.close()


def close_undrained_queue_from_spawn():
    context = multiprocessing.get_context("spawn")
    transport = context.Queue(maxsize=64)
    pub = _KSStageEventPublisher(transport, context.Event(), 0, 0)
    for i in range(64):
        pub.publish(str(i), "undrained", 0, "CONTROL_ERROR", "x" * 512)
    deadline = time.monotonic() + 2
    while not pub.pending.empty() and time.monotonic() < deadline:
        time.sleep(0.002)
    # Deliberately never read this actual multiprocessing.Queue.
    pub.close()
    if pub.thread.is_alive():
        raise AssertionError("sidechannel writer did not exit")


class CandidatePaths(unittest.TestCase):
    def setUp(self):
        self.ex = make_executor()
        self.publishers = []

    def tearDown(self):
        for pub in self.publishers:
            pub.close()
            self.assertFalse(pub.thread.is_alive())
        self.ex._close_stage_event_channels()

    def worker(self, rank, methods=None):
        worker = WorkerProc.__new__(WorkerProc)
        worker.rank = rank
        worker.worker = methods or SimpleNamespace(pp_stage_input_ready=lambda batch: True)
        worker._init_stage_event_channel(self.ex._ks_stage_channels[rank], config())
        self.publishers.append(worker._ks_stage_publisher)
        return worker

    def test_01_spawn_event_consumed_while_real_old_future_blocked(self):
        entered, release = threading.Event(), threading.Event()
        def slow_model():
            entered.set()
            if not release.wait(4):
                raise TimeoutError("CPU model gate was not released")
            return "old-model-result"
        old = FutureWrapper(self.ex.futures_queue, slow_model)
        reader = Thread(target=old.result)
        child = multiprocessing.get_context("spawn").Process(
            target=publish_from_spawn, args=(self.ex._ks_stage_channels[8],))
        try:
            reader.start()
            self.assertTrue(entered.wait(1))
            child.start()
            events = wait_events(self.ex, 1)
            self.assertEqual((events[0].batch_id, events[0].generation, events[0].worker_id),
                             ("batch-A", 7, 8))
            self.assertFalse(old.done())
            self.assertFalse(release.is_set())
        finally:
            self.ex._ks_stage_channels[8][1].set()  # CPU child exit acknowledgement.
            release.set()
            reader.join(3)
            child.join(3)
            if child.is_alive():
                child.terminate()
                child.join(2)
        self.assertFalse(reader.is_alive())
        self.assertEqual(child.exitcode, 0)
        self.assertEqual(old.result(), "old-model-result")

    def test_02_real_busy_loop_targets_only_eight_and_keeps_old_reply_fifo(self):
        command = KSStageCommand("cmd-1", "batch-A", 4, 1, "input_ready")
        self.assertEqual(self.ex.submit_stage_command(command), frozenset(range(8, 16)))
        self.assertEqual(len(self.ex.futures_queue), 0)
        calls, old_replies = [], {}
        for rank in range(16):
            methods = SimpleNamespace(
                ordinary=lambda r=rank: f"ordinary-{r}",
                model_final=lambda r=rank: f"final-{r}",
                pp_stage_input_ready=lambda key, r=rank: calls.append((r, key)) or True)
            worker = self.worker(rank, methods)
            old_replies[rank] = []
            worker.handle_output = old_replies[rank].append
            worker.rpc_broadcast_mq = InputFixture([
                ("ordinary", (), {}, None), self.ex.rpc_broadcast_mq.sent[0],
                ("model_final", (), {}, None)])
            with self.assertRaises(StopLoop):
                worker.worker_busy_loop()
        events = wait_events(self.ex, 8)
        self.assertEqual({r for r, _ in calls}, set(range(8, 16)))
        self.assertEqual({e.worker_id for e in events}, set(range(8, 16)))
        self.assertEqual({e.value for e in events}, {True})
        self.assertEqual(self.ex._ks_stage_pending, {})
        for rank, replies in old_replies.items():
            self.assertEqual(replies, [f"ordinary-{rank}", f"final-{rank}"])

    def test_03_stage_exception_uses_only_sidechannel(self):
        command = KSStageCommand("err-1", "batch-B", 2, 1, "input_ready")
        self.ex.submit_stage_command(command)
        def broken(key):
            raise ValueError("CPU expected stage failure")
        worker = self.worker(8, SimpleNamespace(pp_stage_input_ready=broken))
        worker._handle_stage_command(command)
        event = wait_events(self.ex, 1)[0]
        self.assertEqual(event.kind, "CONTROL_ERROR")
        self.assertIn("CPU expected stage failure", event.value)
        self.assertEqual(len(self.ex.futures_queue), 0)

    def test_04_actual_collective_rpc_keeps_result_order(self):
        trace = []
        class Response:
            def dequeue(self, **kwargs):
                value = ("ordinary", "final-model")[len(trace)]
                trace.append(value)
                return WorkerProc.ResponseStatus.SUCCESS, value
        self.ex.response_mqs = [Response()]
        first = self.ex.collective_rpc("ordinary", non_block=True, unique_reply_rank=0)
        final = self.ex.collective_rpc("execute_model", non_block=True, unique_reply_rank=0)
        pub = self.worker(8)._ks_stage_publisher
        pub.publish("device-B", "batch-B", 1, "KV_DEVICE_READY", True)
        wait_events(self.ex, 1)
        self.assertEqual(len(self.ex.futures_queue), 2)
        self.assertEqual(final.result(), "final-model")
        self.assertEqual(first.result(), "ordinary")
        self.assertEqual(trace, ["ordinary", "final-model"])

    def test_05_disabled_no_queue_thread_or_command(self):
        other = make_executor(enabled=False)
        self.assertEqual(other.poll_stage_events(), [])
        self.assertEqual(other._ks_stage_channels, {})
        with self.assertRaisesRegex(RuntimeError, "disabled"):
            other.submit_stage_command(KSStageCommand("id", "B", 0, 0, "input_ready"))
        worker = WorkerProc.__new__(WorkerProc)
        worker._init_stage_event_channel(None, config())
        self.assertIsNone(worker._ks_stage_publisher)
        other._close_stage_event_channels()

    def test_06_identity_and_duplicate_pending_command_rejected(self):
        command = KSStageCommand("same", "batch", 0, 1, "input_ready")
        self.ex.submit_stage_command(command)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.ex.submit_stage_command(command)
        for invalid in (KSStageCommand("", "b", 0, 0, "input_ready"),
                        KSStageCommand("id", "b", True, 0, "input_ready"),
                        KSStageCommand("id", "b", 0, 2, "input_ready"),
                        KSStageCommand("id", "b", 0, 0, "execute_model")):
            with self.assertRaises(ValueError):
                self.ex.submit_stage_command(invalid)
        self.assertEqual(len(self.ex.rpc_broadcast_mq.sent), 1)

    def test_07_worker_identity_generation_and_sequence_validation(self):
        command = KSStageCommand("id", "batch", 4, 1, "input_ready")
        self.ex.submit_stage_command(command)
        q, _ = self.ex._ks_stage_channels[8]
        q.put_nowait(KSStageEvent("id", "batch", 3, 1, 8, 1, "CONTROL_RESULT", True))
        deadline = time.monotonic()+2
        while time.monotonic() < deadline:
            try:
                self.ex.poll_stage_events()
            except ValueError as error:
                self.assertIn("identity", str(error))
                break
            time.sleep(.002)
        else:
            self.fail("stale command generation was not rejected")
        self.assertEqual(self.ex._ks_stage_sequences, {})

    def test_08_one_writer_thread_no_future_query_or_device_work(self):
        class Transport:
            def __init__(self): self.threads = []
            def put_nowait(self, event): self.threads.append(threading.get_ident())
            def cancel_join_thread(self): pass
            def close(self): pass
        transport = Transport()
        pub = _KSStageEventPublisher(transport, threading.Event(), 0, 0)
        self.publishers.append(pub)
        pub.publish("a", "b", 0, "KV_DEVICE_READY", True)
        pub.publish("b", "b", 0, "INPUT_DEVICE_READY", True)
        deadline = time.monotonic()+2
        while len(transport.threads) < 2 and time.monotonic() < deadline:
            time.sleep(.002)
        self.assertEqual(transport.threads, [pub.thread.ident, pub.thread.ident])
        self.assertNotEqual(pub.thread.ident, threading.get_ident())
        tree = ast.parse((HERE / "multiproc_executor.py").read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "_KSStageEventPublisher")
        forbidden = {"get_output", "result", "query", "synchronize", "execute_model", "get_pp_group", "get_tp_group"}
        self.assertFalse({n.attr for n in ast.walk(cls) if isinstance(n, ast.Attribute)} & forbidden)

    def test_09_full_transport_fails_closed_and_close_does_not_join_feeder(self):
        class FullTransport:
            def __init__(self): self.closed = self.cancelled = False
            def put_nowait(self, event): raise queue.Full()
            def cancel_join_thread(self): self.cancelled = True
            def close(self): self.closed = True
        transport, failed = FullTransport(), threading.Event()
        pub = _KSStageEventPublisher(transport, failed, 0, 0)
        pub.publish("a", "b", 0, "KV_DEVICE_READY", True)
        self.assertTrue(failed.wait(2))
        pub.close()
        self.assertFalse(pub.thread.is_alive())
        self.assertTrue(transport.closed and transport.cancelled)
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            pub.publish("c", "b", 0, "KV_DEVICE_READY", True)

    def test_10_non_owner_core_cannot_send_or_poll(self):
        self.ex.poll_stage_events()
        errors = []
        def wrong_owner():
            for action in (self.ex.poll_stage_events,
                           lambda: self.ex.submit_stage_command(KSStageCommand("id", "b", 0, 0, "input_ready"))):
                try: action()
                except RuntimeError as error: errors.append(str(error))
        thread = Thread(target=wrong_owner)
        thread.start(); thread.join(2)
        self.assertEqual(len(errors), 2)

    def test_11_patch_preserves_legacy_ast_and_has_real_bootstrap_forwarding(self):
        original = ast.parse(BASELINE.read_text(encoding="utf-8"))
        candidate = ast.parse((HERE / "multiproc_executor.py").read_text(encoding="utf-8"))
        names = {"FutureWrapper", "collective_rpc", "handle_output", "enqueue_output",
                 "async_output_busy_loop", "execute_model", "sample_tokens"}
        def extracts(tree):
            return {n.name: ast.dump(n, include_attributes=False) for n in ast.walk(tree)
                    if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in names}
        self.assertEqual(extracts(original), extracts(candidate))
        source = (HERE / "multiproc_executor.py").read_text(encoding="utf-8")
        self.assertIn("stage_event_channel=self._ks_stage_channels.get(global_rank)", source)
        self.assertIn('"stage_event_channel": stage_event_channel', source)
        self.assertIn("self._init_stage_event_channel(stage_event_channel, vllm_config)", source)

    def test_12_stage_event_transport_failure_is_visible_to_core(self):
        self.ex._ks_stage_channels[4][1].set()
        with self.assertRaisesRegex(RuntimeError, "worker 4"):
            self.ex.poll_stage_events()

    def test_13_actual_spawn_exits_with_undrained_feeder(self):
        child = multiprocessing.get_context("spawn").Process(
            target=close_undrained_queue_from_spawn)
        child.start()
        child.join(4)
        still_alive = child.is_alive()
        if still_alive:
            child.terminate()
            child.join(2)
        self.assertFalse(still_alive, "undrained event queue blocked child exit")
        self.assertEqual(child.exitcode, 0)


class RedBoundary(unittest.TestCase):
    def test_real_executor_has_independent_stage_poll(self):
        path = HERE / "multiproc_executor.py"
        tree = ast.parse((path if path.exists() else BASELINE).read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "MultiprocExecutor")
        self.assertIn("poll_stage_events", {n.name for n in cls.body if isinstance(n, ast.FunctionDef)},
                      "real vLLM executor has no independent stage event path")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"status": "CPU_PASS" if result.wasSuccessful() else "CPU_FAIL",
              "tests_run": result.testsRun, "failures": len(result.failures),
              "errors": len(result.errors), "runtime_status": "NOT_RUNTIME_INTEGRATED",
              "actual_source": str(BASELINE), "candidate": str(HERE / "multiproc_executor.py"),
              "actual_AST_paths": ["FutureWrapper", "collective_rpc", "worker_busy_loop",
                                   "_handle_stage_command", "_init_stage_event_channel",
                                   "submit_stage_command", "poll_stage_events", "_KSStageEventPublisher"],
              "structural_only": ["full vLLM bootstrap forwarding", "legacy model AST equality"],
              "server_actions": 0, "npu_actions": 0}
    (HERE / "cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
