"""Actual schedule -> typed wire -> actual AsyncScheduler final update, CPU only."""
from __future__ import annotations

import ast
from collections import defaultdict
import dataclasses
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest

import check_scheduler_reservation as scheduling
import check_executor_driver_wire_cpu as transport

HERE = Path(__file__).resolve().parent
OLD = scheduling.OLD
SOURCES = []


def add_methods(cls, path, names, namespace):
    source = next(node for node in ast.parse(path.read_text(encoding="utf-8")).body
                  if isinstance(node, ast.ClassDef) and node.name == "Scheduler")
    for node in source.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
            exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), namespace)
            setattr(cls, node.name, namespace[node.name])
            SOURCES.append({"path": str(path), "class": "Scheduler", "method": node.name,
                            "begin": node.lineno, "end": node.end_lineno})


class IntegratedChecks(unittest.TestCase):
    def test_actual_schedule_phase_wire_and_final_async_update_commit_once(self):
        self._run_actual_schedule_wire_update(tp_size=8)

    def test_tp4_actual_schedule_phase_wire_and_final_async_update_commit_once(self):
        self._run_actual_schedule_wire_update(tp_size=4)

    def _run_actual_schedule_wire_update(self, tp_size):
        trace_directory = tempfile.TemporaryDirectory()
        self.addCleanup(trace_directory.cleanup)
        previous_trace = os.environ.get("KS_STAGE_TRACE_DIR")
        os.environ["KS_STAGE_TRACE_DIR"] = trace_directory.name
        def restore_trace():
            if previous_trace is None:
                os.environ.pop("KS_STAGE_TRACE_DIR", None)
            else:
                os.environ["KS_STAGE_TRACE_DIR"] = previous_trace
        self.addCleanup(restore_trace)
        world = transport.World(tp_size=tp_size, blocking_sample=True)
        world.restore("r", 0)
        fixture = scheduling.SchedulerReservationChecks()
        fixture.setUp(controller=world.controller, tp_size=tp_size)
        self.addCleanup(fixture.tearDown)
        base_type = type(fixture.scheduler)
        env = {"RequestStatus": scheduling.Status, "defaultdict": defaultdict,
               "EngineCoreOutput": lambda **fields: types.SimpleNamespace(**fields),
               "EngineCoreOutputs": lambda **fields: types.SimpleNamespace(**fields),
               "remove_all": lambda values, removed: [value for value in values if value not in removed]}
        def stop(request, maximum):
            if request.num_output_tokens >= 1:
                request.status = scheduling.Status.FINISHED_STOPPED
                return True
            return False
        env["check_stop"] = stop
        add_methods(base_type, scheduling.DEST / "scheduler.py",
                    {"update_from_output", "_update_request_with_output", "_handle_stopped_request"},
                    dict(env, _ks_stage_reservation=fixture.helper))
        # Preserve the real AsyncScheduler overrides and their super() calls.
        async_source = OLD / "capacity_followup/sources/vllm/v1/core/sched/async_scheduler.py"
        node = next(node for node in ast.parse(async_source.read_text(encoding="utf-8")).body
                    if isinstance(node, ast.ClassDef) and node.name == "AsyncScheduler")
        node.body = [method for method in node.body if isinstance(method, ast.FunctionDef)
                     and method.name in {"_update_after_schedule", "_update_request_with_output"}]
        async_env = {"Scheduler": base_type, "RequestStatus": scheduling.Status}
        unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
        exec(compile(ast.fix_missing_locations(unit), str(async_source), "exec"), async_env)
        fixture.scheduler.__class__ = async_env["AsyncScheduler"]
        scheduler, request = fixture.scheduler, fixture.request
        scheduler._spec_token_placeholders = [-1, -1, -1]
        scheduler.pp_size = 2
        scheduler.max_model_len = 49152
        scheduler.perf_metrics = None
        scheduler.finished_req_ids_dict = {}
        scheduler.structured_output_manager = types.SimpleNamespace(should_advance=lambda request: False)
        scheduler.make_stats = lambda *args: None
        scheduler.kv_cache_manager.take_events = lambda: None
        scheduler.connector.take_events = lambda: None
        request._output_token_ids = []
        request.async_tokens_to_discard = 0
        request.resumable = False
        request.client_index = 0
        request.stop_reason = None
        request.trace_headers = None
        request.num_nans_in_logits = 0
        request.sampling_params.num_logprobs = None
        request.is_finished = lambda: scheduling.Status.is_finished(request.status)
        request.get_finished_reason = lambda: "length"
        request.take_events = lambda: None
        request.take_prefill_stats = lambda: None
        def append(token):
            request._output_token_ids.append(token)
            request.num_output_tokens = len(request._output_token_ids)
        request.append_output_token_ids = append
        def free(request):
            fixture.freed.append(request.request_id)
            scheduler.requests.pop(request.request_id, None)
            return None
        scheduler._free_request = free

        # Use the archived real ModelRunnerOutput dataclass in the wire fixture.
        module = types.ModuleType("ks_integrated_real_model_output_cpu")
        sys.modules[module.__name__] = module
        module.dataclass, module.field = dataclasses.dataclass, dataclasses.field
        real_output = scheduling.extract_class(OLD / "source_baseline/vllm/v1/outputs.py",
                                               "ModelRunnerOutput", None, vars(module))
        class WireOutput(real_output):
            def __init__(self, kv_connector_output=None, request=None):
                super().__init__(req_ids=[request] if request else [],
                    req_id_to_index={request: 0} if request else {},
                    sampled_token_ids=[[71]] if request else [],
                    kv_connector_output=kv_connector_output)
        # World has already captured its output type; rebuild before admitting.
        previous_output = transport.ModelOutput
        transport.ModelOutput = WireOutput
        self.addCleanup(setattr, transport, "ModelOutput", previous_output)
        world = transport.World(tp_size=tp_size, blocking_sample=True)
        world.restore("r", 0)
        scheduler._ks_stage_controller = world.controller
        scheduler.ks_bind_stage_controller(world.controller)

        output = scheduler.schedule()
        self.assertEqual(output.ks_stage_batch["requests"], (("r", 1),))
        self.assertEqual(output.scheduled_new_reqs[0].num_computed_tokens, 511)
        execute = world.driver.admit(output)
        final = world.driver.request_sample(None)
        for _ in range(10):
            world.tick()
        self.assertTrue(any(command.operation == "execute" and command.target_stage == 0 for command in world.sent))
        self.assertFalse(any(command.operation == "execute" and command.target_stage == 1 for command in world.sent))
        self.assertFalse(final.done())
        self.assertEqual(fixture.cached, [])
        self.assertEqual(scheduler.finished_recving_kv_req_ids, set())
        self.assertEqual(world.controller.max_inflight_batches, 2)

        world.restore("r", 1)
        world.until(final.done)
        model_output = final.result()
        self.assertIsInstance(model_output, real_output)
        self.assertIsNone(execute.result())
        self.assertEqual(model_output.kv_connector_output.finished_recving, {"r"})
        self.assertEqual(fixture.cached, [])
        result = scheduler.update_from_output(output, model_output)
        self.assertEqual(result[0].outputs[0].new_token_ids, [71])
        self.assertEqual(fixture.cached, [("r", 512)])
        self.assertEqual(fixture.freed, ["r"])
        self.assertEqual(request.num_output_placeholders, 0)
        self.assertEqual(scheduler._ks_partial_restore_reservations, {})
        self.assertEqual(scheduler.running, [])
        world.driver.core_consumed(output, model_output)
        world.until(lambda: not world.driver.batches)
        self.assertEqual(world.controller.inflight_count, 0)
        self.assertEqual(len(world.samples), 2 * tp_size)
        self.assertEqual(world.aggregator._recv_remaining_count, {})
        trace_rows = [json.loads(line) for path in Path(trace_directory.name).glob("*.jsonl")
                      for line in path.read_text(encoding="utf-8").splitlines()]
        admission = [row for row in trace_rows if row["phase"] == "partial_admission_decision"]
        self.assertEqual(admission[0]["reason"], "front_ready")
        self.assertEqual(admission[0]["decision"], "reserved")
        self.assertFalse(admission[0]["all_stages_ready"])
        self.result = {"front_started_before_back_kv": True, "cache_before_all_kv": 0,
                       "prefix_cache_commits_after_actual_output": len(fixture.cached),
                       "returned_token": result[0].outputs[0].new_token_ids,
                       "whole_batch_capacity": 2, "sample_calls": len(world.samples),
                       "actual_trace_rows": len(trace_rows),
                       "tensor_parallel_size": tp_size, "pipeline_parallel_size": 2}


if __name__ == "__main__":
    cases = [IntegratedChecks(name) for name in (
        "test_actual_schedule_phase_wire_and_final_async_update_commit_once",
        "test_tp4_actual_schedule_phase_wire_and_final_async_update_commit_once")]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(cases))
    report = {"status": "PASS_ACTUAL_SCHEDULER_WIRE_UPDATE_CPU_ONLY" if result.wasSuccessful() else "FAIL",
              "tests": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
              "evidence": [getattr(case, "result", None) for case in cases], "actual_final_methods": SOURCES,
              "fixtures": ["request/stop boundary", "KV manager", "model forward/PP/device", "model initialization", "EngineCore output containers"],
              "npu_actions": 0, "actual_glm_started": False}
    (HERE / "integrated_first_token_cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not result.wasSuccessful())
