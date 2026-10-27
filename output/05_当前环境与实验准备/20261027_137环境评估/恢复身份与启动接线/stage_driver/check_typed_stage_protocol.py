"""New CPU cases for span admission, typed replies and real front sample ownership."""
from __future__ import annotations

import ast
import copy
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
PROTO = OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
OLD_RUNNER = OUT / "04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/m8/vllm_ascend/worker/model_runner_v1.py"
SOURCES = []


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ModelOutput:
    def __init__(self, kv_connector_output=None):
        self.kv_connector_output = kv_connector_output


class ConnectorOutput:
    def is_empty(self):
        return False


def extract(path, name, selected, env):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)
    cls.bases = []
    cls.decorator_list = []
    cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in selected]
    for node in cls.body:
        node.decorator_list = []
        SOURCES.append({"path": str(path), "class": name, "method": node.name,
                        "begin": node.lineno, "end": node.end_lineno})
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), env)
    return env[name]


class TypedProtocolChecks(unittest.TestCase):
    def setUp(self):
        self.assertTrue((HERE / "span_stage_controller.py").exists(), "span controller is absent")
        self.assertTrue((HERE / "typed_stage_protocol.py").exists(), "typed stage protocol is absent")
        self.controller_module = load("ks_span_controller_new_cpu", HERE / "span_stage_controller.py")
        self.protocol = load("ks_typed_protocol_new_cpu", HERE / "typed_stage_protocol.py")
        self.groups = (tuple(range(8)), tuple(range(8, 16)))
        self.controller = self.controller_module.StageController(self.groups, 2, 2)
        self.controller.begin_restore("r", 0)
        for stage, workers in enumerate(self.groups):
            for worker in workers:
                self.controller.mark_kv_ready("r", 0, stage, worker, device_complete=True)

    def admit_two(self):
        first = self.controller.admit_batch("b1", 0, [("r", 0)], [(0, 1, 2)])
        second = self.controller.admit_batch("b2", 0, [("r", 0)], [(3, 4, 5)])
        return first, second

    def finish_model(self, key, stage):
        for worker in self.groups[stage]:
            self.controller.mark_model_complete(key, stage, worker, device_complete=True)

    def consume_and_retire(self, key, stage):
        for worker in self.groups[stage]:
            self.controller.mark_sample_consumed(key, stage, worker)
            self.controller.mark_stage_buffer_retired(key, stage, worker, device_complete=True)

    def test_two_same_request_batches_with_disjoint_spans_keep_two_slots(self):
        self.admit_two()
        self.assertEqual(self.controller.inflight_count, 2)
        with self.assertRaises(RuntimeError):
            self.controller.admit_batch("b3", 0, [("r", 0)], [(6,)])

    def test_overlapping_or_out_of_order_token_spans_are_rejected(self):
        self.controller.admit_batch("b1", 0, [("r", 0)], [(3, 4, 5)])
        for positions in ((4, 5, 6), (0, 1, 2)):
            with self.assertRaises(ValueError):
                self.controller.admit_batch("b2", 0, [("r", 0)], [positions])

    def test_front_model_done_does_not_retire_buffer_or_release_whole_slot(self):
        first, second = self.admit_two()
        self.assertEqual(self.controller.take_ready(0).key, first)
        self.finish_model(first, 0)
        self.assertIsNone(self.controller.take_ready(0))
        with self.assertRaises(RuntimeError):
            self.controller.mark_stage_buffer_retired(first, 0, 0, device_complete=True)
        self.consume_and_retire(first, 0)
        self.assertEqual(self.controller.take_ready(0).key, second)
        self.assertEqual(self.controller.inflight_count, 2)

    def test_final_result_and_core_consumption_and_retirement_are_distinct(self):
        first = self.controller.admit_batch("b1", 0, [("r", 0)], [(0,)])
        self.controller.take_ready(0)
        self.finish_model(first, 0)
        self.consume_and_retire(first, 0)
        for worker in self.groups[1]:
            self.controller.mark_activation_received(first, 1, worker, device_complete=True)
        self.controller.take_ready(1)
        self.finish_model(first, 1)
        for worker in self.groups[1]:
            self.controller.mark_sample_consumed(first, 1, worker)
        output = ModelOutput()
        self.controller.mark_final_result(first, output)
        self.assertIs(self.controller.peek_committable().result, output)
        self.assertIsNone(self.controller.pop_committable())
        with self.assertRaises(RuntimeError):
            self.controller.mark_stage_buffer_retired(first, 1, 8, device_complete=True)
        self.controller.mark_core_consumed(first)
        self.consume_and_retire(first, 1)
        self.assertEqual(self.controller.pop_committable().key, first)
        self.assertEqual(self.controller.inflight_count, 0)

    def test_unsupported_cancellation_does_not_clear_state(self):
        first, _ = self.admit_two()
        with self.assertRaises(NotImplementedError):
            self.controller.cancel(first)
        self.assertEqual(self.controller.inflight_count, 2)

    def test_none_model_return_is_not_device_ready_or_final_output(self):
        p = self.protocol
        reply = p.StageReply("c", ("b1", 0), 0, 0, "execute", p.ReplyKind.MODEL_RETURN, None)
        reply.validate((ModelOutput,))
        self.assertFalse(reply.device_complete)
        ledger = p.TypedResultLedger(self.groups, (ModelOutput,), 2)
        ledger.admit(("b1", 0))
        ledger.accept(reply)
        with self.assertRaises(ValueError):
            ledger.record_final(("b1", 0), None)
        self.assertIsNone(ledger.peek_final())

    def test_typed_reply_rejects_model_payload_as_ready_and_bool_as_model(self):
        p = self.protocol
        for kind, payload in ((p.ReplyKind.DEVICE_READY, ModelOutput()), (p.ReplyKind.MODEL_RETURN, True)):
            reply = p.StageReply("c", ("b1", 0), 0, 0, "execute", kind, payload)
            with self.assertRaises(ValueError):
                reply.validate((ModelOutput,))

    def test_result_ledger_holds_payloads_and_final_commit_is_fifo(self):
        p = self.protocol
        ledger = p.TypedResultLedger(self.groups, (ModelOutput,), 2)
        first, second = ("b1", 0), ("b2", 0)
        ledger.admit(first)
        ledger.admit(second)
        stage_payload = ModelOutput(ConnectorOutput())
        ledger.accept(p.StageReply("sample-1", first, 0, 0, "sample", p.ReplyKind.MODEL_RETURN, stage_payload))
        output2, output1 = ModelOutput(), ModelOutput()
        ledger.record_final(second, output2)
        self.assertIsNone(ledger.peek_final())
        ledger.record_final(first, output1)
        self.assertIs(ledger.peek_final()[1], output1)
        self.assertIs(ledger.payloads_for(first)[0], stage_payload)
        with self.assertRaises(RuntimeError):
            ledger.ack_core_consumed(second)
        ledger.ack_core_consumed(first)
        self.assertIs(ledger.peek_final()[1], output2)

    def test_all_rank_sample_aggregation_keeps_front_kv_payload(self):
        p = self.protocol
        key = ("b1", 0)
        ledger = p.TypedResultLedger(self.groups, (ModelOutput,), 2)
        ledger.admit(key)
        samples = [ModelOutput(ConnectorOutput()) for _ in range(16)]
        aggregated = []
        def aggregate(values):
            aggregated.extend(values)
            return values[-1]
        for rank in range(15):
            ledger.accept(p.StageReply("sample", key, rank // 8, rank, "sample", p.ReplyKind.MODEL_RETURN, samples[rank]))
        self.assertFalse(ledger.aggregate_samples(key, aggregate))
        self.assertEqual(aggregated, [])
        ledger.accept(p.StageReply("sample", key, 1, 15, "sample", p.ReplyKind.MODEL_RETURN, samples[15]))
        self.assertTrue(ledger.aggregate_samples(key, aggregate))
        self.assertEqual(aggregated, samples)
        self.assertIs(ledger.peek_final()[1], samples[-1])

    def test_error_reply_holds_batch_and_does_not_become_ready(self):
        p = self.protocol
        ledger = p.TypedResultLedger(self.groups, (ModelOutput,), 2)
        key = ("b1", 0)
        ledger.admit(key)
        failure = p.StageFailure("RuntimeError", "fixture device failure")
        ledger.accept(p.StageReply("execute", key, 0, 0, "execute", p.ReplyKind.ERROR, failure))
        with self.assertRaises(RuntimeError):
            ledger.record_final(key, ModelOutput())
        self.assertEqual(ledger.inflight_count, 1)

    def real_front_worker(self):
        env = {"copy": copy.copy, "EMPTY_MODEL_RUNNER_OUTPUT": ModelOutput(),
               "get_pp_group": lambda: types.SimpleNamespace(world_size=2, is_last_rank=False)}
        runner_type = extract(OLD_RUNNER, "NPUModelRunner", {"sample_tokens"}, env)
        runner = runner_type()
        original_kv = ConnectorOutput()
        runner.kv_connector_output = original_kv
        runner.execute_model_state = None
        runner.is_kv_producer = True
        runner.use_async_scheduling = True
        worker_env = {"get_pp_group": lambda: types.SimpleNamespace(is_last_rank=False)}
        names = {"sample_tokens", "pp_stage_release_ready", "release_pp_stage_batch"}
        worker_type = extract(PROTO / "worker_split/worker.py", "NPUWorker", names, worker_env)
        worker = worker_type()
        key = ("b1", 0)
        record = {"executed": True}
        worker.model_runner = runner
        worker._ks_pp_stage_current = record
        worker._ks_pp_stage_record = lambda received: record if received == key else (_ for _ in ()).throw(KeyError(received))
        worker.pp_stage_model_ready = lambda received: True
        worker._ks_pp_handles_done = lambda handles: True
        worker._pp_send_work = []
        worker._ks_pp_retained_send_tensors = object()
        return worker, original_kv

    def real_execute_worker(self, *, fail=False):
        class DeviceEventFixture:
            def record(self):
                pass
            def query(self):
                return False
        env = {"get_pp_group": lambda: types.SimpleNamespace(is_first_rank=True),
               "torch": types.SimpleNamespace(npu=types.SimpleNamespace(Event=DeviceEventFixture))}
        cls = extract(PROTO / "worker_split/worker.py", "NPUWorker", {"execute_pp_stage_model", "pp_stage_model_ready"}, env)
        worker = cls()
        record = {"executed": False, "model_event": None, "failed": False}
        worker._ks_pp_stage_record = lambda *args, **kwargs: record
        worker.model_runner = types.SimpleNamespace(execute_model_state=None)
        worker._ks_pp_handles_done = lambda handles: True
        worker._pp_send_work = []
        def execute(output):
            if fail:
                raise RuntimeError("CPU model boundary failed")
            return None
        worker.execute_model = execute
        return worker, record

    def test_actual_execute_none_and_real_device_query_remain_distinct(self):
        p = self.protocol
        worker, record = self.real_execute_worker()
        protocol = p.StageWorkerProtocol(worker, 0, 0, (ModelOutput,))
        reply = protocol.execute(("b1", 0), object(), "execute")
        self.assertEqual(reply.kind, p.ReplyKind.MODEL_RETURN)
        self.assertIsNone(reply.payload)
        self.assertTrue(record["executed"])
        ready = protocol.query(("b1", 0), "model_ready", "query")
        self.assertEqual(ready.kind, p.ReplyKind.DEVICE_READY)
        self.assertFalse(ready.device_complete)

    def test_actual_execute_error_latches_failure_without_freeing_worker(self):
        p = self.protocol
        worker, record = self.real_execute_worker(fail=True)
        protocol = p.StageWorkerProtocol(worker, 0, 0, (ModelOutput,))
        reply = protocol.execute(("b1", 0), object(), "execute")
        self.assertEqual(reply.kind, p.ReplyKind.ERROR)
        self.assertTrue(record["failed"])
        worker.execute_model = lambda output: ModelOutput()
        retried = protocol.execute(("b1", 0), object(), "execute-again")
        self.assertEqual(retried.kind, p.ReplyKind.ERROR)

    def test_real_front_sample_consumes_kv_output_and_retire_keeps_typed_payload(self):
        p = self.protocol
        worker, original_kv = self.real_front_worker()
        protocol = p.StageWorkerProtocol(worker, 0, 0, (ModelOutput,))
        key = ("b1", 0)
        self.assertFalse(worker.pp_stage_release_ready(key))
        reply = protocol.sample(key, None, "sample-1")
        self.assertEqual(reply.kind, p.ReplyKind.MODEL_RETURN)
        self.assertIs(reply.payload.kv_connector_output, original_kv)
        self.assertIsNone(worker.model_runner.kv_connector_output)
        retired = protocol.retire_front(key, "retire-1")
        self.assertEqual(retired.kind, p.ReplyKind.CONTROL_ACK)
        self.assertIsNone(worker._ks_pp_stage_current)
        self.assertIs(protocol.held_sample(key).payload.kv_connector_output, original_kv)

    def test_retire_without_real_sample_does_not_clear_worker_state(self):
        p = self.protocol
        worker, original_kv = self.real_front_worker()
        protocol = p.StageWorkerProtocol(worker, 0, 0, (ModelOutput,))
        reply = protocol.retire_front(("b1", 0), "retire-1")
        self.assertEqual(reply.kind, p.ReplyKind.ERROR)
        self.assertIs(worker.model_runner.kv_connector_output, original_kv)
        self.assertIsNotNone(worker._ks_pp_stage_current)

    def test_real_sample_error_retains_pre_sample_connector_reference(self):
        p = self.protocol
        worker, original_kv = self.real_front_worker()
        def fail_empty():
            raise RuntimeError("CPU connector-output inspection failed")
        original_kv.is_empty = fail_empty
        protocol = p.StageWorkerProtocol(worker, 0, 0, (ModelOutput,))
        key = ("b1", 0)
        reply = protocol.sample(key, None, "sample")
        self.assertEqual(reply.kind, p.ReplyKind.ERROR)
        self.assertIs(protocol._protected[key][0], original_kv)
        retry = protocol.sample(key, None, "sample-again")
        self.assertEqual(retry.kind, p.ReplyKind.ERROR)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(TypedProtocolChecks))
    report = {"status": "PASS_TYPED_STAGE_PROTOCOL_CPU_ONLY" if result.wasSuccessful() else "FAIL",
              "tests": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "actual_methods": SOURCES,
              "fixtures": ["model-output class", "front completion/communication probes", "model forward and NPU event", "PP group", "connector output"],
              "runtime_driver_integrated": False, "normal_mtp_computation_executed": False,
              "npu_actions": 0, "installed_source_changes": 0}
    (HERE / "typed_stage_cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not result.wasSuccessful())
