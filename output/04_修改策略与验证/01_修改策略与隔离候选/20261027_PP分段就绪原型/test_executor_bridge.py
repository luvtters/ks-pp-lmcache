"""用归档的真实collective_rpc和FutureWrapper验证阶段派发，不加载模型。"""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from enum import Enum
from functools import partial
from pathlib import Path
import pickle
import queue
import sys
import threading
from types import ModuleType, SimpleNamespace
import time
from typing import Any, Callable, Sequence
import unittest
from unittest.mock import patch

try:
    import executor_bridge as bridge
except ModuleNotFoundError:
    bridge = None


HERE = Path(__file__).resolve().parent
ARCHIVE = HERE.parent / "KV完成通知修改策略与隔离分支_20261022"
SOURCE = ARCHIVE / "source_baseline/vllm/v1/executor/multiproc_executor.py"


class Status(Enum):
    SUCCESS = 1
    FAILURE = 2


def real_executor(workers):
    """只抽取实际方法；设备队列和cloudpickle换成明确标注的CPU替身。"""
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    future_node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "FutureWrapper")
    executor_node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "MultiprocExecutor")
    rpc_node = next(n for n in executor_node.body if isinstance(n, ast.FunctionDef) and n.name == "collective_rpc")
    rpc_node.decorator_list = []
    active_group = SimpleNamespace(rank_in_group=None)
    fake_vllm = ModuleType("vllm")
    fake_vllm.__path__ = []
    fake_dist = ModuleType("vllm.distributed")
    fake_dist.get_pp_group = lambda: active_group
    modules = {"vllm": fake_vllm, "vllm.distributed": fake_dist}
    responses = [deque() for _ in workers]
    calls = []

    class Queue:
        def __init__(self, values):
            self.values = values

        def dequeue(self, timeout=None):
            if not self.values:
                raise AssertionError("没有对应worker回复，不能伪造成功")
            return self.values.popleft()

    class Broadcast:
        def enqueue(self, command):
            method, args, kwargs, _ = command
            fn = pickle.loads(method)
            calls.append((fn.operation, fn.target_stage))
            with patch.dict(sys.modules, modules):
                for index, worker in enumerate(workers):
                    active_group.rank_in_group = worker.stage
                    try:
                        output = fn(worker, *args, **kwargs)
                        responses[index].append((Status.SUCCESS, output))
                    except Exception as error:
                        responses[index].append((Status.FAILURE, str(error)))

    namespace = {
        "Future": Future, "InvalidStateError": InvalidStateError,
        "suppress": suppress, "deque": deque, "Callable": Callable,
        "Any": Any, "Sequence": Sequence, "time": time,
        "partial": partial, "pickle": pickle,
        "cloudpickle": pickle,  # 测试只替代序列化库，不声称测过真实cloudpickle。
        "WorkerProc": SimpleNamespace(ResponseStatus=Status),
        "_ks_future_span": lambda *a: nullcontext(),
        "_ks_command_boundary": lambda *a: None,
        "KVOutputAggregator": object,
    }
    selected = ast.Module(body=[future_node, rpc_node], type_ignores=[])
    ast.fix_missing_locations(selected)
    exec(compile(selected, str(SOURCE), "exec"), namespace)
    executor = SimpleNamespace(
        is_failed=False, rpc_broadcast_mq=Broadcast(),
        response_mqs=[Queue(values) for values in responses], futures_queue=deque(),
    )
    executor.collective_rpc = namespace["collective_rpc"].__get__(executor)
    return executor, calls


def workers16():
    result = []
    for rank in range(16):
        worker = SimpleNamespace(rank=rank, stage=rank // 8, calls=[])
        for operation, method in (
            ("prepare_input", "prepare_pp_stage_input"),
            ("input_received", "pp_stage_input_received"),
            ("finalize_input", "finalize_pp_stage_input"),
            ("input_ready", "pp_stage_input_ready"),
            ("execute", "execute_pp_stage_model"),
            ("model_ready", "pp_stage_model_ready"),
            ("release_ready", "pp_stage_release_ready"),
            ("release", "release_pp_stage_batch"),
        ):
            def invoke(*args, _worker=worker, _operation=operation):
                _worker.calls.append((_operation, args))
                return True
            setattr(worker, method, invoke)
        result.append(worker)
    return result


class ExecutorBridgeTests(unittest.TestCase):
    def setUp(self):
        if bridge is None:
            self.fail("阶段派发实现尚不存在，原collective_rpc不能表达这套新控制流程")
        self.workers = workers16()
        self.executor, self.calls = real_executor(self.workers)
        self.driver = bridge.StageRPCBridge(self.executor, (tuple(range(8)), tuple(range(8, 16))))

    def test_only_target_stage_executes_with_real_collective_rpc(self):
        output = SimpleNamespace(total_num_scheduled_tokens=1)
        future = self.driver.submit(0, "execute", ("批次一", 1), output)
        values = self.driver.collect(0, future)
        self.assertEqual(set(values), set(range(8)))
        self.assertTrue(all(w.calls == [("execute", (("批次一", 1), output))] for w in self.workers[:8]))
        self.assertTrue(all(w.calls == [] for w in self.workers[8:]))

    def test_receive_is_enqueued_before_front_compute_without_early_result_wait(self):
        prepare, front = self.driver.prepare_back_then_execute_front(("批次一", 1), "执行安排")
        self.assertEqual(self.calls, [("prepare_input", 1), ("execute", 0)])
        self.assertFalse(prepare.done())
        self.assertFalse(front.done())
        self.driver.collect(0, front)
        self.assertTrue(prepare.done())  # 原FutureWrapper仍按队列顺序取回复。

    def test_truthful_probe_is_not_a_model_result(self):
        result = self.driver.collect(1, self.driver.submit(1, "input_received", ("批次一", 1)))
        self.assertEqual(result, {rank: True for rank in range(8, 16)})
        self.assertTrue(all(not w.calls for w in self.workers[:8]))

    def test_worker_error_is_not_swallowed(self):
        def broken(*args):
            raise ValueError("缺少匹配的已就绪输入")
        self.workers[8].execute_pp_stage_model = broken
        with self.assertRaisesRegex(RuntimeError, "缺少匹配"):
            self.driver.collect(1, self.driver.submit(1, "execute", ("批次一", 1), "执行安排"))

    def test_missing_worker_reply_is_rejected(self):
        values = [bridge.StageReply(0, rank, True, True) for rank in range(7)]
        future = Future()
        future.set_result(values)
        with self.assertRaisesRegex(ValueError, "worker"):
            self.driver.collect(0, future)

    def test_duplicate_worker_cannot_replace_a_missing_worker(self):
        future = Future()
        future.set_result([bridge.StageReply(0, 0, True, True) for _ in range(8)])
        with self.assertRaisesRegex(ValueError, "worker"):
            self.driver.collect(0, future)

    def test_non_target_worker_cannot_claim_execution(self):
        future = Future()
        future.set_result([bridge.StageReply(0, rank, True, True) for rank in range(9)])
        with self.assertRaisesRegex(ValueError, "worker"):
            self.driver.collect(0, future)

    def test_legacy_result_fifo_is_preserved(self):
        first = self.driver.submit(1, "input_received", ("批次一", 1))
        second = self.driver.submit(0, "model_ready", ("批次二", 1))
        self.driver.collect(0, second)
        self.assertTrue(first.done())
        self.assertEqual(len(self.executor.futures_queue), 0)

    def test_rejects_unrecognized_operations_and_stages(self):
        with self.assertRaises(ValueError):
            self.driver.submit(0, "sample_tokens", ("批次一", 1))
        with self.assertRaises(ValueError):
            self.driver.submit(2, "execute", ("批次一", 1), "执行安排")
        self.assertEqual(self.calls, [])

    def test_model_dispatch_requires_an_explicit_scheduler_output(self):
        with self.assertRaises(ValueError):
            self.driver.submit(0, "execute", ("批次一", 1))
        self.assertEqual(self.calls, [])

    def test_release_requires_explicit_final_output_confirmation(self):
        received = []
        for worker in self.workers[8:]:
            def release(key, *, final_output_consumed=False):
                received.append(final_output_consumed)
                return final_output_consumed
            worker.release_pp_stage_batch = release
        self.driver.collect(1, self.driver.submit(1, "release", ("批次一", 1)))
        self.assertEqual(received, [False] * 8)
        received.clear()
        self.driver.collect(1, self.driver.submit(1, "release", ("批次一", 1),
                                                   final_output_consumed=True))
        self.assertEqual(received, [True] * 8)

    def test_real_rpc_with_parallel_worker_queues_can_receive_before_front_is_done(self):
        """模拟各worker独立队列，后段prepare真等待前段发消息；不是串行假回复。"""
        # CPU Event仅代表元数据的到达，不代表HCCL或设备数据传输。
        sent = threading.Event()
        inbound = [queue.Queue() for _ in self.workers]
        outbound = [queue.Queue() for _ in self.workers]
        local_group = threading.local()
        fake_vllm = ModuleType("vllm")
        fake_vllm.__path__ = []
        fake_dist = ModuleType("vllm.distributed")
        fake_dist.get_pp_group = lambda: local_group
        threads = []

        for worker in self.workers[8:]:
            def prepare(*args, _worker=worker):
                _worker.calls.append(("prepare_input", args))
                if not sent.wait(2):
                    raise RuntimeError("后段在等元数据，但前段未被派发")
                return True
            worker.prepare_pp_stage_input = prepare
        for worker in self.workers[:8]:
            def execute(*args, _worker=worker):
                _worker.calls.append(("execute", args))
                sent.set()
                return True
            worker.execute_pp_stage_model = execute

        class ResponseQueue:
            def __init__(self, values):
                self.values = values

            def dequeue(self, timeout=None):
                return self.values.get(timeout=3 if timeout is None else timeout)

        class Broadcast:
            def enqueue(self, command):
                for target in inbound:
                    target.put(command)

        def serve(rank):
            local_group.rank_in_group = self.workers[rank].stage
            while True:
                command = inbound[rank].get()
                if command is None:
                    return
                method, args, kwargs, _ = command
                try:
                    value = pickle.loads(method)(self.workers[rank], *args, **kwargs)
                    outbound[rank].put((Status.SUCCESS, value))
                except Exception as error:
                    outbound[rank].put((Status.FAILURE, str(error)))

        self.executor.rpc_broadcast_mq = Broadcast()
        self.executor.response_mqs = [ResponseQueue(items) for items in outbound]
        with patch.dict(sys.modules, {"vllm": fake_vllm, "vllm.distributed": fake_dist}):
            try:
                for rank in range(16):
                    thread = threading.Thread(target=serve, args=(rank,), daemon=True)
                    thread.start()
                    threads.append(thread)
                prepare, front = self.driver.prepare_back_then_execute_front(("批次一", 1), "执行安排")
                values = self.driver.collect(0, front)
                self.assertEqual(set(values), set(range(8)))
                self.assertTrue(prepare.done())
                self.assertTrue(sent.is_set())
                self.assertTrue(all(w.calls == [("prepare_input", (("批次一", 1), "执行安排"))]
                                    for w in self.workers[8:]))
            finally:
                for target in inbound:
                    target.put(None)
                for thread in threads:
                    thread.join(timeout=3)
                self.assertTrue(all(not thread.is_alive() for thread in threads))


if __name__ == "__main__":
    unittest.main(verbosity=2)
