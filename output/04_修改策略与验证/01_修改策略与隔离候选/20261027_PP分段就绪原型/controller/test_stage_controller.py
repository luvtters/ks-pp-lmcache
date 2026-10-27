"""CPU控制契约；不加载vLLM、LMCache或NPU运行时。"""
import json
import unittest
from pathlib import Path

try:
    from stage_controller import StageController
except ModuleNotFoundError:
    StageController = None


class StageControllerTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(StageController, "缺少待实现的分段就绪控制器")
        self.c = StageController(((0, 1), (2, 3)),
                                 max_inflight_batches=2,
                                 max_buffered_activations=2)

    def ready(self, req="缓存命中的请求", generation=1, stage=0):
        for worker in self.c.stage_workers[stage]:
            self.c.mark_kv_ready(req, generation, stage, worker,
                                 device_complete=True)

    def add(self, batch="批次一", req="缓存命中的请求", generation=1):
        self.c.begin_restore(req, generation)
        return self.c.admit_batch(batch, generation, ((req, generation),), ((4095,),))

    def complete(self, key, stage):
        for worker in self.c.stage_workers[stage]:
            self.c.mark_model_complete(key, stage, worker, device_complete=True)

    def receive(self, key, stage=1):
        for worker in self.c.stage_workers[stage]:
            self.c.mark_activation_received(key, stage, worker, device_complete=True)

    def finish(self, key):
        self.ready(stage=0)
        self.assertIsNotNone(self.c.take_ready(0))
        self.complete(key, 0)
        self.ready(stage=1)
        self.receive(key)
        self.assertIsNotNone(self.c.take_ready(1))
        self.complete(key, 1)

    def test_front_only_dispatches_after_every_front_worker_kv_ready(self):
        key = self.add()
        self.assertIsNone(self.c.take_ready(0))
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 0, device_complete=True)
        self.assertIsNone(self.c.take_ready(0))
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 1, device_complete=True)
        work = self.c.take_ready(0)
        self.assertEqual((work.key, work.stage, work.workers), (key, 0, (0, 1)))
        self.assertIsNone(self.c.take_ready(1))

    def test_device_incomplete_is_never_counted_as_kv_ready(self):
        self.add()
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 0, device_complete=False)
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 1, device_complete=True)
        self.assertIsNone(self.c.take_ready(0))

    def test_duplicate_and_wrong_generation_do_not_release_front(self):
        self.add()
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 0, device_complete=True)
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 0, device_complete=True)
        self.assertFalse(self.c.mark_kv_ready("缓存命中的请求", 0, 0, 1,
                                              device_complete=True))
        self.assertIsNone(self.c.take_ready(0))

    def test_back_needs_own_kv_and_all_received_intermediates(self):
        key = self.add()
        self.ready(stage=0)
        self.c.take_ready(0)
        self.complete(key, 0)
        self.ready(stage=1)
        self.assertIsNone(self.c.take_ready(1))
        self.c.mark_activation_received(key, 1, 2, device_complete=True)
        self.assertIsNone(self.c.take_ready(1))
        self.c.mark_activation_received(key, 1, 3, device_complete=True)
        self.assertEqual(self.c.take_ready(1).workers, (2, 3))

    def test_back_does_not_run_with_only_received_intermediates(self):
        key = self.add()
        self.ready(stage=0)
        self.c.take_ready(0)
        self.complete(key, 0)
        self.receive(key)
        self.assertIsNone(self.c.take_ready(1))

    def test_acknowledgement_is_not_device_receive_completion(self):
        key = self.add()
        self.ready(stage=0)
        self.c.take_ready(0)
        self.complete(key, 0)
        self.ready(stage=1)
        for worker in (2, 3):
            self.c.mark_activation_received(key, 1, worker, device_complete=False)
        self.assertIsNone(self.c.take_ready(1))

    def test_different_batches_overlap_front_and_back(self):
        one = self.add()
        two = self.add("批次二", "正在计算的请求")
        self.ready()
        self.ready("正在计算的请求")
        self.c.take_ready(0)
        self.complete(one, 0)
        self.ready(stage=1)
        self.receive(one)
        back = self.c.take_ready(1)
        front = self.c.take_ready(0)
        self.assertEqual((back.key, front.key), (one, two))

    def test_same_stage_has_one_active_compute(self):
        self.add()
        self.add("批次二", "正在计算的请求")
        self.ready()
        self.ready("正在计算的请求")
        self.assertIsNotNone(self.c.take_ready(0))
        self.assertIsNone(self.c.take_ready(0))

    def test_first_blocked_batch_cannot_be_bypassed(self):
        self.add()
        self.add("批次二", "正在计算的请求")
        self.ready("正在计算的请求")
        self.assertIsNone(self.c.take_ready(0))

    def test_all_requests_in_logical_batch_must_be_ready(self):
        self.c.begin_restore("缓存命中的请求", 1)
        self.c.begin_restore("正在计算的请求", 1)
        self.c.admit_batch("混合批", 1,
                           (("缓存命中的请求", 1), ("正在计算的请求", 1)),
                           ((4095,), (20, 21)))
        self.ready()
        self.assertIsNone(self.c.take_ready(0))
        self.ready("正在计算的请求")
        work = self.c.take_ready(0)
        self.assertEqual(work.token_positions, ((4095,), (20, 21)))
        self.assertEqual(work.requests, (("缓存命中的请求", 1), ("正在计算的请求", 1)))

    def test_model_completion_requires_all_workers_and_real_completion(self):
        key = self.add()
        self.ready()
        self.c.take_ready(0)
        self.c.mark_model_complete(key, 0, 0, device_complete=True)
        self.c.mark_model_complete(key, 0, 1, device_complete=False)
        self.ready(stage=1)
        self.receive(key)
        self.assertIsNone(self.c.take_ready(1))
        self.c.mark_model_complete(key, 0, 1, device_complete=True)
        self.assertIsNotNone(self.c.take_ready(1))

    def test_front_completion_does_not_free_whole_batch_slot(self):
        key = self.add()
        self.add("批次二", "正在计算的请求")
        self.ready()
        self.c.take_ready(0)
        self.complete(key, 0)
        self.c.begin_restore("第三请求", 1)
        with self.assertRaisesRegex(RuntimeError, "名额"):
            self.c.admit_batch("批次三", 1, (("第三请求", 1),), ((30,),))
        self.assertIsNone(self.c.pop_committable())

    def test_activation_buffer_backpressure(self):
        self.c = StageController(((0, 1), (2, 3)), max_buffered_activations=1)
        one = self.add()
        two = self.add("批次二", "正在计算的请求")
        self.ready()
        self.ready("正在计算的请求")
        self.c.take_ready(0)
        self.complete(one, 0)
        self.assertIsNone(self.c.take_ready(0))
        self.ready(stage=1)
        self.receive(one)
        self.c.take_ready(1)
        self.complete(one, 1)
        self.assertEqual(self.c.take_ready(0).key, two)

    def test_last_stage_done_is_not_final_token_result(self):
        key = self.add()
        self.finish(key)
        self.assertIsNone(self.c.pop_committable())
        self.c.mark_final_result(key, {"token_ids": [42]})
        commit = self.c.pop_committable()
        self.assertEqual((commit.key, commit.result, commit.cancelled),
                         (key, {"token_ids": [42]}, False))

    def test_final_result_cannot_be_published_before_last_stage_done(self):
        key = self.add()
        with self.assertRaisesRegex(RuntimeError, "最后段"):
            self.c.mark_final_result(key, [42])

    def test_results_commit_in_admission_order(self):
        one = self.add()
        two = self.add("批次二", "正在计算的请求")
        self.ready()
        self.ready("正在计算的请求")
        self.c.take_ready(0)
        self.complete(one, 0)
        self.c.take_ready(0)
        self.complete(two, 0)
        self.ready(stage=1)
        self.ready("正在计算的请求", stage=1)
        self.receive(one)
        self.receive(two)
        self.c.take_ready(1)
        self.complete(one, 1)
        # 真正的runner仍保存待sample状态，不能在此执行下一批后段。
        self.assertIsNone(self.c.take_ready(1))
        self.c.mark_final_result(one, [1])
        self.c.take_ready(1)
        self.complete(two, 1)
        self.c.mark_final_result(two, [2])
        self.assertEqual(self.c.pop_committable().key, one)
        self.assertEqual(self.c.pop_committable().key, two)

    def test_last_stage_slot_is_held_until_sampling_result_ready(self):
        one = self.add()
        two = self.add("批次二", "正在计算的请求")
        self.ready()
        self.ready("正在计算的请求")
        self.c.take_ready(0)
        self.complete(one, 0)
        self.c.take_ready(0)
        self.complete(two, 0)
        self.ready(stage=1)
        self.ready("正在计算的请求", stage=1)
        self.receive(one)
        self.receive(two)
        self.c.take_ready(1)
        self.complete(one, 1)
        self.assertIsNone(self.c.take_ready(1))
        self.assertEqual(self.c.inflight_count, 2)
        self.c.mark_final_result(one, [1])
        self.assertEqual(self.c.take_ready(1).key, two)
        self.assertEqual(self.c.inflight_count, 2)

    def test_cancel_does_not_free_unknown_inflight_resources(self):
        key = self.add()
        self.add("批次二", "正在计算的请求")
        self.ready()
        self.c.take_ready(0)
        self.c.cancel(key)
        self.assertIsNone(self.c.pop_committable())
        self.c.begin_restore("第三请求", 1)
        with self.assertRaisesRegex(RuntimeError, "名额"):
            self.c.admit_batch("批次三", 1, (("第三请求", 1),), ((1,),))
        for worker in (0, 1, 2):
            self.c.mark_drained(key, worker, device_complete=True)
        self.c.mark_drained(key, 3, device_complete=False)
        self.assertIsNone(self.c.pop_committable())
        self.c.mark_drained(key, 3, device_complete=True)
        self.assertTrue(self.c.pop_committable().cancelled)

    def test_unknown_worker_cannot_satisfy_stage(self):
        self.add()
        with self.assertRaisesRegex(ValueError, "worker"):
            self.c.mark_kv_ready("缓存命中的请求", 1, 0, 2, device_complete=True)

    def test_batch_key_is_not_reused_after_commit(self):
        key = self.add()
        self.finish(key)
        self.c.mark_final_result(key, [42])
        self.c.pop_committable()
        with self.assertRaisesRegex(ValueError, "批次"):
            self.c.admit_batch("批次一", 1, (("缓存命中的请求", 1),), ((4095,),))

    def test_old_restore_generation_cannot_replace_active_request(self):
        self.add()
        with self.assertRaisesRegex(RuntimeError, "仍有"):
            self.c.begin_restore("缓存命中的请求", 2)

    def test_invalid_topology_and_mismatched_token_members_rejected(self):
        with self.assertRaises(ValueError):
            StageController(((0, 1), (1, 2)))
        self.c.begin_restore("缓存命中的请求", 1)
        with self.assertRaises(ValueError):
            self.c.admit_batch("缺位置", 1, (("缓存命中的请求", 1),), ())

    def test_complete_before_dispatch_rejected(self):
        key = self.add()
        with self.assertRaisesRegex(RuntimeError, "派发"):
            self.c.mark_model_complete(key, 0, 0, device_complete=True)

    def test_tp8_needs_all_eight_specific_workers(self):
        self.c = StageController((tuple(range(8)), tuple(range(8, 16))))
        self.add()
        for worker in range(7):
            self.c.mark_kv_ready("缓存命中的请求", 1, 0, worker, device_complete=True)
        self.assertIsNone(self.c.take_ready(0))
        self.c.mark_kv_ready("缓存命中的请求", 1, 0, 7, device_complete=True)
        self.assertEqual(self.c.take_ready(0).workers, tuple(range(8)))

    def test_cancelled_restore_generation_cannot_be_reused(self):
        key = self.add()
        self.ready()
        self.c.take_ready(0)
        self.c.cancel(key)
        for worker in range(4):
            self.c.mark_drained(key, worker, device_complete=True)
        self.c.pop_committable()
        with self.assertRaisesRegex(ValueError, "失效"):
            self.c.admit_batch("新批", 2, (("缓存命中的请求", 1),), ((4095,),))
        self.c.begin_restore("缓存命中的请求", 2)
        self.c.admit_batch("新批", 2, (("缓存命中的请求", 2),), ((4095,),))
        self.assertFalse(self.c.mark_kv_ready("缓存命中的请求", 1, 0, 0,
                                              device_complete=True))
        self.assertIsNone(self.c.take_ready(0))


if __name__ == "__main__":
    import sys
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StageControllerTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    receipt = {
        "testsRun": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "successful": result.wasSuccessful(),
        "scope": "CPU control contracts only; NOT_RUNTIME_INTEGRATED; no NPU or performance evidence",
    }
    if len(sys.argv) > 1:
        Path(sys.argv[1]).write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
