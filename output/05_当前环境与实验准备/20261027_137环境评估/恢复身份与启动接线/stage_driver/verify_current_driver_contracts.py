"""Bounded source-contract evidence, not a runnable stage driver."""
from __future__ import annotations

import ast
import enum
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
PROTO = OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
SCHEDULER = OUT / "05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/core/sched/scheduler.py"
SOURCES = []


def methods(path, class_name, names, env):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in names]
    cls.bases = []
    cls.decorator_list = []
    for node in cls.body:
        node.decorator_list = []
        SOURCES.append({"path": str(path), "class": class_name, "method": node.name,
                        "begin": node.lineno, "end": node.end_lineno})
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), env)
    return env[class_name]


class Status(enum.Enum):
    WAITING_FOR_REMOTE_KVS = 1
    PREEMPTED = 2
    WAITING = 3


class CurrentContractChecks(unittest.TestCase):
    def test_front_ready_is_not_a_legal_whole_request_promotion(self):
        env = {"RequestStatus": Status}
        cls = methods(SCHEDULER, "Scheduler", {"_try_promote_blocked_waiting_request", "_update_waiting_for_remote_kv"}, env)
        scheduler = cls()
        scheduler.finished_recving_kv_req_ids = set()
        scheduler.failed_recving_kv_req_ids = set()
        scheduler.connector = object()
        cached = []
        scheduler.kv_cache_manager = types.SimpleNamespace(cache_blocks=lambda request, tokens: cached.append((request.request_id, tokens)))
        request = types.SimpleNamespace(request_id="fixture", status=Status.WAITING_FOR_REMOTE_KVS,
                                        num_preemptions=0, num_computed_tokens=8, num_tokens=8)
        stage_ready_fixture = [True, False]
        self.assertFalse(scheduler._try_promote_blocked_waiting_request(request))
        self.assertEqual(cached, [])
        # Explicit counterfactual: treating front READY as global finished is the
        # proposed shortcut being rejected, not an observed runtime condition.
        scheduler.finished_recving_kv_req_ids.add(request.request_id)
        self.assertTrue(scheduler._try_promote_blocked_waiting_request(request))
        self.assertEqual(stage_ready_fixture, [True, False])
        self.assertEqual(cached, [("fixture", 8)])
        self.assertEqual(request.num_computed_tokens, 7)

    def test_controller_has_two_slots_but_rejects_same_request_second_batch(self):
        source = PROTO / "controller/stage_controller.py"
        spec = importlib.util.spec_from_file_location("ks_original_controller_driver_contract", source)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        controller = module.StageController((tuple(range(8)), tuple(range(8, 16))), 2, 2)
        controller.begin_restore("request-A", 0)
        controller.admit_batch("batch-1", 0, [("request-A", 0)], [(0,)])
        self.assertEqual(controller.inflight_count, 1)
        with self.assertRaisesRegex(RuntimeError, "同一请求"):
            controller.admit_batch("batch-2", 0, [("request-A", 0)], [(1,)])
        controller.begin_restore("request-B", 1)
        controller.admit_batch("batch-2", 0, [("request-B", 1)], [(0,)])
        self.assertEqual(controller.inflight_count, 2)
        SOURCES.append({"path": str(source), "class": "StageController", "method": "admit_batch", "begin": 118, "end": 148})

    def test_adding_execute_name_alone_rejects_model_output_payload(self):
        emitted = []
        # Explicit counterfactual allowlist addition. Handler body stays exact.
        env = {"_KS_STAGE_METHODS": {"execute": "execute_pp_stage_model"}}
        cls = methods(PROTO / "draft_wait_followup/multiproc_executor.py", "WorkerProc", {"_handle_stage_command"}, env)
        worker = cls()
        worker._ks_stage_index = 0
        worker._ks_stage_publisher = types.SimpleNamespace(publish=lambda *args: emitted.append(args))
        worker.worker = types.SimpleNamespace(execute_pp_stage_model=lambda key: types.SimpleNamespace(kv_connector_output=object()))
        command = types.SimpleNamespace(validate=lambda: None, target_stage=0, operation="execute",
                                        batch_id="batch-1", generation=0, command_id="command-1")
        worker._handle_stage_command(command)
        self.assertEqual(emitted[0][3], "CONTROL_ERROR")
        self.assertIn("only bool or None", emitted[0][4])

    def test_front_buffer_is_held_until_connector_output_is_consumed(self):
        cls = methods(PROTO / "worker_split/worker.py", "NPUWorker", {"pp_stage_release_ready"}, {})
        worker = cls()
        worker._ks_pp_stage_record = lambda key: {"executed": True}
        worker.pp_stage_model_ready = lambda key: True
        worker.model_runner = types.SimpleNamespace(execute_model_state=None, kv_connector_output=object())
        self.assertFalse(worker.pp_stage_release_ready(("batch-1", 0)))


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CurrentContractChecks))
    report = {"status": "CONFIRMED_CURRENT_STAGE_DRIVER_CONTRACTS" if result.wasSuccessful() else "FAILED_CONTRACT_CHECK",
              "cases": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "sources": SOURCES,
              "counterfactuals": ["front-ready inserted into global finished set", "execute inserted into control allowlist"],
              "fixtures": ["request/cache manager", "model output payload", "worker model completion and connector output"],
              "runtime_stage_driver_delivered": False, "model_started": False,
              "npu_actions": 0, "installed_source_changes": 0}
    (HERE / "driver_contract_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not result.wasSuccessful())
