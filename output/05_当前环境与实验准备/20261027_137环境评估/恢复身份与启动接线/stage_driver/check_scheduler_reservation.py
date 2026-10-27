"""Execute actual modified Scheduler.schedule/promote/finish methods on CPU."""
from __future__ import annotations

import ast
from contextlib import nullcontext
import dataclasses
from enum import Enum
from functools import cached_property
import importlib.util
import itertools
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
OLD = OUT / "04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022"
DEST = HERE / "runtime_overlay/vllm/v1/core/sched"
SOURCES = []


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def extract_class(path, class_name, methods, env):
    cls = next(node for node in ast.parse(path.read_text(encoding="utf-8")).body if isinstance(node, ast.ClassDef) and node.name == class_name)
    cls.bases = []
    if methods is not None:
        cls.decorator_list = []
        cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in methods]
    for node in cls.body:
        if isinstance(node, ast.FunctionDef):
            SOURCES.append({"path": str(path), "class": class_name, "method": node.name,
                            "begin": node.lineno, "end": node.end_lineno})
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), env)
    return env[class_name]


class Status(Enum):
    WAITING = 1
    WAITING_FOR_REMOTE_KVS = 2
    PREEMPTED = 3
    RUNNING = 4
    WAITING_FOR_STRUCTURED_OUTPUT_GRAMMAR = 5
    WAITING_FOR_STREAMING_REQ = 6
    FINISHED_STOPPED = 7

    @staticmethod
    def is_finished(status):
        return status is Status.FINISHED_STOPPED


class RequestQueue:
    def __init__(self, values=()):
        self.values = list(values)
    def __bool__(self):
        return bool(self.values)
    def __len__(self):
        return len(self.values)
    def peek_request(self):
        return self.values[0]
    def pop_request(self):
        return self.values.pop(0)
    def prepend_request(self, request):
        self.values.insert(0, request)
    def prepend_requests(self, requests):
        self.values[:0] = requests.values


class Blocks:
    def __init__(self):
        self.blocks = ([],)
    def get_block_ids(self, allow_none=False):
        return ([10, 11, 12, 13],)


class RequestFixture:
    def __init__(self, **fields):
        self.__dict__.update(fields)


class SchedulerReservationChecks(unittest.TestCase):
    def setUp(self, controller=None, tp_size=8):
        self.assertTrue((DEST / "ks_stage_reservation.py").exists(), "actual scheduler reservation overlay is absent")
        self.previous_flag = os.environ.get("KS_PP_STAGE_DRIVER")
        os.environ["KS_PP_STAGE_DRIVER"] = "1"
        self.helper = load("ks_reservation_cpu_module", DEST / "ks_stage_reservation.py")
        output_module = types.ModuleType("ks_real_scheduler_output_cpu")
        sys.modules[output_module.__name__] = output_module
        output_env = vars(output_module)
        output_env.update(dataclass=dataclasses.dataclass, cached_property=cached_property)
        source = OLD / "capacity_followup/sources/vllm/v1/core/sched/output.py"
        for name in ("NewRequestData", "CachedRequestData", "SchedulerOutput"):
            extract_class(source, name, None, output_env)
        self.output_types = output_module
        self.cached = []
        self.allocated = []
        self.freed = []
        env = {"__name__": "ks_scheduler_reserved_cpu", "_ks_stage_reservation": self.helper,
               "RequestStatus": Status, "PauseState": types.SimpleNamespace(UNPAUSED=0, PAUSED_ALL=1),
               "SchedulingPolicy": types.SimpleNamespace(FCFS="fcfs"),
               "create_request_queue": lambda policy: RequestQueue(), "time": time,
               "logger": types.SimpleNamespace(debug=lambda *args: None),
               "record_function_or_nullcontext": lambda name: nullcontext(), "itertools": itertools,
               "NewRequestData": output_module.NewRequestData,
               "CachedRequestData": output_module.CachedRequestData,
               "SchedulerOutput": output_module.SchedulerOutput}
        selected = {"schedule", "_make_cached_request_data", "_update_after_schedule",
                    "_is_blocked_waiting_status", "_select_waiting_queue_for_scheduling",
                    "_try_promote_blocked_waiting_request", "_update_waiting_for_remote_kv",
                    "_update_from_kv_xfer_finished", "_build_kv_connector_meta", "ks_bind_stage_controller"}
        cls = extract_class(DEST / "scheduler.py", "Scheduler", selected, env)
        self.scheduler = cls()
        self.scheduler.parallel_config = types.SimpleNamespace(
            tensor_parallel_size=tp_size, pipeline_parallel_size=2, world_size=2 * tp_size)
        request = RequestFixture(
            request_id="r", status=Status.WAITING_FOR_REMOTE_KVS, num_preemptions=0,
            num_computed_tokens=512, num_tokens=512, num_prompt_tokens=512,
            prompt_token_ids=list(range(512)), _all_token_ids=list(range(512)),
            all_token_ids=list(range(512)), sampling_params=types.SimpleNamespace(max_tokens=1),
            max_tokens=1, num_output_tokens=0, num_output_placeholders=0,
            num_tokens_with_spec=512, spec_token_ids=[], use_structured_output=False,
            has_encoder_inputs=False, lora_request=None, prefill_stats=None,
            mm_features=[], pooling_params=None, prompt_embeds=None, prompt_is_token_ids=None,
            next_decode_eligible_step=0)
        self.request = request
        self.scheduler.requests = {"r": request}
        self.scheduler.current_step = 0
        self.scheduler.running = []
        self.scheduler.waiting = RequestQueue()
        self.scheduler.skipped_waiting = RequestQueue([request])
        self.scheduler.max_num_scheduled_tokens = 16384
        self.scheduler._pause_state = 0
        self.scheduler.max_num_encoder_input_tokens = 0
        self.scheduler.lora_config = None
        self.scheduler.policy = "fcfs"
        self.scheduler.max_num_running_reqs = 128
        self.scheduler.ec_connector = None
        self.scheduler.is_encoder_decoder = False
        self.scheduler.need_mamba_block_aligned_split = False
        self.scheduler.use_eagle = True
        self.scheduler.num_lookahead_tokens = 3
        self.scheduler.scheduler_reserve_full_isl = True
        self.scheduler.connector_prefix_cache_stats = None
        self.scheduler.log_stats = False
        self.scheduler._inflight_prefills = set()
        self.scheduler.use_v2_model_runner = False
        self.scheduler.kv_cache_config = types.SimpleNamespace(kv_cache_groups=[object()])
        self.scheduler.prev_step_scheduled_req_ids = set()
        self.scheduler.needs_kv_cache_zeroing = False
        self.scheduler.finished_req_ids = set()
        self.scheduler.encoder_cache_manager = types.SimpleNamespace(get_freed_mm_hashes=lambda: [])
        self.scheduler.enable_return_routed_experts = False
        self.scheduler.scheduler_config = types.SimpleNamespace(long_prefill_token_threshold=0,
            enable_chunked_prefill=True, async_scheduling=True)
        self.scheduler.finished_recving_kv_req_ids = set()
        self.scheduler.failed_recving_kv_req_ids = set()
        def allocate(*args, **kwargs):
            self.allocated.append(kwargs)
            return Blocks()
        self.scheduler.kv_cache_manager = types.SimpleNamespace(
            new_step_starts=lambda: None, empty_kv_cache_blocks=Blocks(), allocate_slots=allocate,
            get_blocks=lambda request_id: Blocks(),
            get_num_common_prefix_blocks=lambda request_id: [0],
            cache_blocks=lambda request, tokens: self.cached.append((request.request_id, tokens)))
        self.scheduler.connector = types.SimpleNamespace(update_state_after_alloc=lambda *args: None,
            build_connector_meta=lambda output: types.SimpleNamespace(original_metadata=True),
            update_connector_output=lambda output: None)
        self.scheduler._free_blocks = lambda request: self.freed.append(request.request_id)
        self.groups = (tuple(range(tp_size)), tuple(range(tp_size, 2 * tp_size)))
        restore = types.SimpleNamespace(generation=4, ready=[set(range(tp_size)), set()], invalid=False)
        self.controller = controller or types.SimpleNamespace(stage_workers=self.groups, _restores={"r": restore})
        self.scheduler.ks_bind_stage_controller(self.controller)

    def tearDown(self):
        if getattr(self, "previous_flag", None) is None:
            os.environ.pop("KS_PP_STAGE_DRIVER", None)
        else:
            os.environ["KS_PP_STAGE_DRIVER"] = self.previous_flag

    def test_actual_schedule_reserves_front_without_global_ready_or_cache(self):
        output = self.scheduler.schedule()
        self.assertEqual(output.total_num_scheduled_tokens, 1)
        self.assertEqual(output.num_scheduled_tokens, {"r": 1})
        self.assertEqual(output.scheduled_new_reqs[0].num_computed_tokens, 511)
        self.assertEqual(output.ks_stage_batch["requests"], (("r", 4),))
        self.assertEqual(output.ks_stage_batch["token_positions"], ((511,),))
        self.assertEqual(self.request.status, Status.RUNNING)
        self.assertEqual(self.request.num_computed_tokens, 512)
        self.assertEqual(self.scheduler.finished_recving_kv_req_ids, set())
        self.assertTrue(self.allocated[0]["delay_cache_blocks"])
        self.assertEqual(self.allocated[0]["num_lookahead_tokens"], 3)
        self.assertEqual(self.cached, [])
        self.assertTrue(output.kv_connector_metadata.original_metadata)

    def test_missing_one_front_worker_keeps_actual_scheduler_blocked(self):
        self.controller._restores["r"].ready[0].remove(7)
        output = self.scheduler.schedule()
        self.assertEqual(output.total_num_scheduled_tokens, 0)
        self.assertFalse(hasattr(output, "ks_stage_batch"))
        self.assertEqual(self.request.status, Status.WAITING_FOR_REMOTE_KVS)

    def test_true_global_completion_keeps_original_promotion_path(self):
        self.scheduler.finished_recving_kv_req_ids.add("r")
        output = self.scheduler.schedule()
        self.assertFalse(hasattr(output, "ks_stage_batch"))
        self.assertEqual(self.cached, [("r", 512)])

    def test_disabled_flag_keeps_original_remote_gate(self):
        os.environ.pop("KS_PP_STAGE_DRIVER")
        output = self.scheduler.schedule()
        self.assertEqual(output.total_num_scheduled_tokens, 0)
        self.assertEqual(self.cached, [])

    def test_long_output_or_more_than_one_remaining_token_is_rejected(self):
        self.request.sampling_params.max_tokens = 2
        with self.assertRaises(NotImplementedError):
            self.scheduler.schedule()
        self.request.sampling_params.max_tokens = 1
        self.request.num_computed_tokens = 256
        with self.assertRaises(NotImplementedError):
            self.scheduler.schedule()
        self.assertEqual(self.request.status, Status.WAITING_FOR_REMOTE_KVS)

    def test_real_finished_recv_defers_cache_to_actual_async_final_output(self):
        self.scheduler.schedule()
        output = types.SimpleNamespace(finished_recving={"r"}, finished_sending=set())
        self.scheduler._update_from_kv_xfer_finished(output)
        self.assertEqual(self.cached, [])
        self.assertEqual(self.request.status, Status.RUNNING)
        self.assertEqual(self.request.num_computed_tokens, 512)
        self.assertEqual(self.scheduler.finished_recving_kv_req_ids, set())
        self.assertEqual(self.freed, [])
        self.assertTrue(self.scheduler._ks_partial_restore_reservations["r"]["global_complete"])

    def test_bind_rejects_actual_config_or_rank_layout_mismatch(self):
        for field, value in (("tensor_parallel_size", 4), ("pipeline_parallel_size", 1),
                             ("world_size", 8), ("tensor_parallel_size", 0)):
            with self.subTest(field=field, value=value):
                original = getattr(self.scheduler.parallel_config, field)
                setattr(self.scheduler.parallel_config, field, value)
                try:
                    with self.assertRaises(ValueError):
                        self.helper.bind(self.scheduler, self.controller)
                finally:
                    setattr(self.scheduler.parallel_config, field, original)
        for groups in (((0, 1, 2, 3, 4, 5, 6, 6), tuple(range(8, 16))),
                       (tuple(range(8)), tuple(range(7, 15)))):
            with self.subTest(groups=groups):
                with self.assertRaises(ValueError):
                    self.helper.bind(self.scheduler, types.SimpleNamespace(stage_workers=groups))

    def test_trace_reports_existing_branches_without_adding_readiness_gate(self):
        old_trace = os.environ.get("KS_STAGE_TRACE_DIR")
        with tempfile.TemporaryDirectory() as directory:
            os.environ["KS_STAGE_TRACE_DIR"] = directory
            sys.path.insert(0, str(HERE / "runtime_overlay"))
            try:
                restore = self.controller._restores["r"]
                restore.ready[0].remove(7)
                self.assertFalse(self.helper.try_partial_promotion(self.scheduler, self.request, Status.WAITING_FOR_REMOTE_KVS))
                restore.ready[0].add(7)
                self.scheduler.finished_recving_kv_req_ids.add("r")
                self.assertFalse(self.helper.try_partial_promotion(self.scheduler, self.request, Status.WAITING_FOR_REMOTE_KVS))
                self.scheduler.finished_recving_kv_req_ids.clear()
                restore.ready[1].update(self.groups[1])
                self.controller._busy = [("existing", 0), None]
                self.assertTrue(self.helper.try_partial_promotion(self.scheduler, self.request, Status.WAITING_FOR_REMOTE_KVS))
                rows = [json.loads(line) for path in Path(directory).glob("*.jsonl")
                        for line in path.read_text(encoding="utf-8").splitlines()]
                self.assertEqual([row["reason"] for row in rows],
                                 ["front_not_ready", "request_global_finished", "all_stages_ready"])
                self.assertEqual(rows[-1]["decision"], "reserved")
                self.assertTrue(rows[-1]["stage_busy"])
                self.assertTrue(rows[-1]["all_stages_ready"])
            finally:
                sys.path.pop(0)
                if old_trace is None:
                    os.environ.pop("KS_STAGE_TRACE_DIR", None)
                else:
                    os.environ["KS_STAGE_TRACE_DIR"] = old_trace


if __name__ == "__main__":
    outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SchedulerReservationChecks))
    result = {"status": "PASS_ACTUAL_SCHEDULER_FIRST_TOKEN_CPU_ONLY" if outcome.wasSuccessful() else "FAIL",
              "tests": outcome.testsRun, "failures": len(outcome.failures), "errors": len(outcome.errors),
              "actual_methods": SOURCES,
              "fixtures": ["request/config", "KV cache manager", "connector metadata", "parallel controller state", "request queue"],
              "npu_actions": 0, "installed_source_changes": 0}
    (HERE / "scheduler_reservation_cpu_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not outcome.wasSuccessful())
