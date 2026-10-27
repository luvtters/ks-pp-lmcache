"""组合控制器、真实RPC和候选真实worker方法；设备完成仍由CPU替身显式驱动。"""
from __future__ import annotations

from types import SimpleNamespace
import unittest

from controller.stage_controller import StageController
from executor_bridge import StageRPCBridge
from test_executor_bridge import real_executor
import worker_split.test_worker_split as worker_fixture


class ComposedPathTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = []
        for rank in range(16):
            item = worker_fixture.WorkerTests()
            item.setUp()
            item.group.is_first_rank = rank < 8
            item.group.is_last_rank = rank >= 8
            item.group.rank_in_group = rank // 8
            item.worker.rank = rank
            item.worker.stage = rank // 8
            self.fixtures.append(item)
        self.groups = (tuple(range(8)), tuple(range(8, 16)))
        self.control = StageController(self.groups)
        executor, self.calls = real_executor([item.worker for item in self.fixtures])
        self.bridge = StageRPCBridge(executor, self.groups)

    def probe(self, stage, operation, key, **kwargs):
        return self.bridge.collect(stage, self.bridge.submit(stage, operation, key, **kwargs))

    def test_front_compute_does_not_require_back_kv_but_back_compute_does(self):
        request = "缓存命中的请求"
        self.control.begin_restore(request, 1)
        key = self.control.admit_batch("批次一", 1, ((request, 1),), ((4095,),))
        schedule = worker_fixture.output()

        # 尚未提供任何真实完成确认，不得派发。
        self.assertIsNone(self.control.take_ready(0))
        for rank in range(8):
            self.control.mark_kv_ready(request, 1, 0, rank, device_complete=True)
        front_work = self.control.take_ready(0)
        self.assertEqual(front_work.workers, tuple(range(8)))

        # 只准备后段接收，不计算后37层；前段先计算。全部使用相同tuple批身份。
        prepared, computed = self.bridge.prepare_back_then_execute_front(key, schedule)
        self.bridge.collect(0, computed)
        self.assertTrue(prepared.done())
        self.assertTrue(all(len(item.runner_calls) == 1 for item in self.fixtures[:8]))
        self.assertTrue(all(not item.runner_calls for item in self.fixtures[8:]))

        # Python方法返回后，设备事件仍为False，不能把前段当成已完成。
        self.assertTrue(all(value is False for value in self.probe(0, "model_ready", key).values()))
        self.assertIsNone(self.control.take_ready(1))
        for item in self.fixtures[:8]:
            item.events[-1].complete = True
            item.group.handles[0].complete = True
        for rank, value in self.probe(0, "model_ready", key).items():
            self.control.mark_model_complete(key, 0, rank, device_complete=value)

        # 数据接收、TP后处理、设备可读是分开的动作，不能直接用recv返回放行。
        self.assertTrue(all(value is False for value in self.probe(1, "input_received", key).values()))
        for item in self.fixtures[8:]:
            item.group.handles[0].complete = True
        self.assertTrue(all(value is True for value in self.probe(1, "input_received", key).values()))
        self.probe(1, "finalize_input", key)
        self.assertTrue(all(value is False for value in self.probe(1, "input_ready", key).values()))
        for item in self.fixtures[8:]:
            item.events[-1].complete = True
        for rank, value in self.probe(1, "input_ready", key).items():
            self.control.mark_activation_received(key, 1, rank, device_complete=value)

        # 中间结果已经好，但后段KV还没好，后段仍不能计算。
        self.assertIsNone(self.control.take_ready(1))
        self.assertTrue(all(not item.runner_calls for item in self.fixtures[8:]))
        for rank in range(8, 16):
            self.control.mark_kv_ready(request, 1, 1, rank, device_complete=True)
        self.assertEqual(self.control.take_ready(1).workers, tuple(range(8, 16)))
        self.bridge.collect(1, self.bridge.submit(1, "execute", key, schedule))
        self.assertTrue(all(len(item.runner_calls) == 1 for item in self.fixtures))
        self.assertTrue(all(item.group.recv_calls == 1 for item in self.fixtures[8:]))

        for item in self.fixtures[8:]:
            item.events[-1].complete = True
        for rank, value in self.probe(1, "model_ready", key).items():
            self.control.mark_model_complete(key, 1, rank, device_complete=value)
        self.assertIsNone(self.control.pop_committable())
        self.assertEqual(self.control.inflight_count, 1)

        # 此处没有模拟真实采样：只确认缺少最终结果时禁止释放。
        # 新入口不能凭forward事件就声称模型已完成或token正确。
        back_free = self.probe(1, "release_ready", key)
        self.assertTrue(all(value is False for value in back_free.values()))
        self.assertTrue(all(item.worker.model_runner.execute_model_state is not None
                            for item in self.fixtures[8:]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
