"""CPU-only protocol checks against methods extracted from the candidate worker.

The fake transport proves call order and ownership only, never HCCL completion.
"""
from __future__ import annotations

import ast
from contextlib import nullcontext
import copy
from pathlib import Path
from types import NoneType, SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / "KV完成通知修改策略与隔离分支_20261022" / "capacity_followup/sources/m8/vllm_ascend/worker/worker.py"
CANDIDATE = ROOT / "worker.py"
METHODS = {
    "execute_model", "_ks_pp_batch_signature", "_ks_pp_stage_record",
    "_ks_pp_handles_done", "prepare_pp_stage_input", "pp_stage_input_received",
    "finalize_pp_stage_input", "pp_stage_input_ready", "execute_pp_stage_model",
    "pp_stage_model_ready", "pp_stage_release_ready", "release_pp_stage_batch",
}


class Event:
    def __init__(self, complete=False):
        self.complete = complete
        self.recorded = False

    def record(self):
        self.recorded = True

    def query(self):
        return self.recorded and self.complete


class Handle:
    def __init__(self, complete=False):
        self.complete = complete
        self.waits = 0

    def is_completed(self):
        return self.complete

    def wait(self):
        if not self.complete:
            raise AssertionError("CPU fake detected a premature blocking wait")
        self.waits += 1


class Intermediate:
    def __init__(self, tensors, comm_handles=None, comm_postprocess=None):
        self.tensors = tensors
        self._comm_handles = comm_handles or []
        self._comm_postprocess = comm_postprocess or []

    def wait_for_comm(self):
        for handle in self._comm_handles:
            handle.wait()
        for callback in self._comm_postprocess:
            callback()


class ModelOutput:
    pass


class Group:
    def __init__(self, first=False):
        self.is_first_rank = first
        self.is_last_rank = not first
        self.rank_in_group = 0 if first else 1
        self.world_size = 2
        self.handles = [Handle()]
        self.recv_calls = 0
        self.send_calls = 0
        self.postprocess_calls = 0

    def irecv_tensor_dict(self, **kwargs):
        self.recv_calls += 1
        return {"hidden_states": object()}, self.handles, [self.postprocess]

    def postprocess(self):
        self.postprocess_calls += 1

    def isend_tensor_dict(self, tensors, **kwargs):
        self.send_calls += 1
        return self.handles


def output(tokens=1):
    return SimpleNamespace(
        total_num_scheduled_tokens=tokens,
        num_scheduled_tokens={"缓存命中的请求": tokens},
        scheduled_new_reqs=[],
        scheduled_cached_reqs=SimpleNamespace(
            req_ids=[], num_computed_tokens=[], new_block_ids=[],
        ),
    )


class WorkerTests(unittest.TestCase):
    def setUp(self):
        path = CANDIDATE if CANDIDATE.exists() else BASE
        tree = ast.parse(path.read_text(encoding="utf-8"))
        worker_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "NPUWorker")
        methods = [n for n in worker_class.body if isinstance(n, ast.FunctionDef) and n.name in METHODS]
        for method in methods:
            method.decorator_list = []
        cls = ast.ClassDef(name="WorkerUnderTest", bases=[], keywords=[], body=methods, decorator_list=[])
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
        ast.fix_missing_locations(module)
        self.group = Group()
        self.events = []

        def new_event():
            event = Event()
            self.events.append(event)
            return event

        namespace = {
            "get_pp_group": lambda: self.group,
            "get_tp_group": lambda: "TP8",
            "enable_sp": lambda: False,
            "get_ascend_config": lambda: SimpleNamespace(msmonitor_use_daemon=False),
            "_ks_span": lambda *a, **kw: nullcontext(),
            "AsyncIntermediateTensors": Intermediate,
            "IntermediateTensors": Intermediate,
            "ModelRunnerOutput": ModelOutput,
            "AsyncModelRunnerOutput": type("AsyncModelOutput", (), {}),
            "NoneType": NoneType,
            "EMPTY_MODEL_RUNNER_OUTPUT": ModelOutput(),
            "copy": copy,
            "torch": SimpleNamespace(npu=SimpleNamespace(Event=new_event)),
        }
        exec(compile(module, str(path), "exec"), namespace)
        self.worker = namespace["WorkerUnderTest"]()
        self.worker._pp_send_work = []
        self.worker.profiler = None
        self.worker.log_memory_stats = lambda: None
        self.worker.vllm_config = SimpleNamespace(parallel_config=SimpleNamespace(distributed_executor_backend="mp"))
        self.runner_calls = []
        self.worker.model_runner = SimpleNamespace(execute_model=self.run_model, execute_model_state=None)

    def run_model(self, scheduler_output, intermediate_tensors):
        self.runner_calls.append((scheduler_output, intermediate_tensors))
        if self.group.is_first_rank:
            result = Intermediate({"hidden_states": object()})
            result.kv_connector_output = None
            return result
        self.worker.model_runner.execute_model_state = object()
        return None

    def prepare(self):
        self.worker.prepare_pp_stage_input(("批次1", 0), output())

    def ready(self):
        self.prepare()
        self.group.handles[0].complete = True
        self.worker.finalize_pp_stage_input(("批次1", 0))
        self.events[-1].complete = True

    def test_candidate_interfaces_exist(self):
        for name in METHODS - {"execute_model"}:
            self.assertTrue(hasattr(self.worker, name), name)

    def test_bridge_batch_key_accepts_id_and_generation_tuple(self):
        self.worker.prepare_pp_stage_input(("批次1", 0), output())
        self.assertEqual(self.group.recv_calls, 1)

    def test_bridge_batch_key_rejects_invalid_id_or_generation(self):
        invalid = ["批次1", (), ("", 0), ("批次1", -1), ("批次1", True),
                   ("批次1", 1.0), (1, 0), ("批次1", 0, 0), ["批次1", 0]]
        for key in invalid:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.worker.prepare_pp_stage_input(key, output())

    def test_prepare_receives_without_forward(self):
        self.prepare()
        self.assertEqual(self.group.recv_calls, 1)
        self.assertEqual(self.runner_calls, [])
        self.assertEqual(self.group.postprocess_calls, 0)

    def test_pending_handles_are_not_ready(self):
        self.prepare()
        self.assertFalse(self.worker.pp_stage_input_received(("批次1", 0)))
        self.assertFalse(self.worker.pp_stage_input_ready(("批次1", 0)))

    def test_unknown_handle_fails_closed(self):
        self.group.handles = [object()]
        self.prepare()
        self.assertFalse(self.worker.pp_stage_input_received(("批次1", 0)))

    def test_duplicate_prepare_does_not_receive_twice(self):
        self.prepare()
        self.worker.prepare_pp_stage_input(("批次1", 0), output())
        self.assertEqual(self.group.recv_calls, 1)

    def test_other_batch_cannot_overwrite_buffer(self):
        self.prepare()
        with self.assertRaises(RuntimeError):
            self.worker.prepare_pp_stage_input(("批次2", 0), output())

    def test_mismatching_members_or_tokens_rejected(self):
        self.prepare()
        with self.assertRaises(ValueError):
            self.worker.prepare_pp_stage_input(("批次1", 0), output(2))

    def test_first_stage_must_not_receive(self):
        self.group.is_first_rank = True
        with self.assertRaises(ValueError):
            self.prepare()
        self.assertEqual(self.group.recv_calls, 0)

    def test_finalize_requires_received_handles(self):
        self.prepare()
        with self.assertRaises(RuntimeError):
            self.worker.finalize_pp_stage_input(("批次1", 0))
        self.assertEqual(self.group.postprocess_calls, 0)

    def test_received_does_not_mean_postprocess_device_complete(self):
        self.prepare()
        self.group.handles[0].complete = True
        self.assertTrue(self.worker.pp_stage_input_received(("批次1", 0)))
        self.assertFalse(self.worker.pp_stage_input_ready(("批次1", 0)))
        self.worker.finalize_pp_stage_input(("批次1", 0))
        self.assertEqual(self.group.postprocess_calls, 1)
        self.assertFalse(self.worker.pp_stage_input_ready(("批次1", 0)))
        self.events[-1].complete = True
        self.assertTrue(self.worker.pp_stage_input_ready(("批次1", 0)))

    def test_finalize_is_idempotent(self):
        self.ready()
        self.worker.finalize_pp_stage_input(("批次1", 0))
        self.assertEqual(self.group.postprocess_calls, 1)

    def test_execute_rejects_unready_input(self):
        self.prepare()
        with self.assertRaises(RuntimeError):
            self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(self.runner_calls, [])

    def test_execute_consumes_prepared_input_without_second_receive(self):
        self.ready()
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(self.group.recv_calls, 1)
        self.assertEqual(len(self.runner_calls), 1)
        self.assertIsNotNone(self.runner_calls[0][1])

    def test_execute_cannot_repeat_same_batch(self):
        self.ready()
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        with self.assertRaises(RuntimeError):
            self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(len(self.runner_calls), 1)

    def test_preserves_execute_then_sample_gate(self):
        self.ready()
        self.worker.model_runner.execute_model_state = object()
        with self.assertRaises(RuntimeError):
            self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(self.runner_calls, [])

    def test_front_previous_send_pending_does_not_enter_blocking_wait(self):
        self.group.is_first_rank = True
        self.group.is_last_rank = False
        self.worker._pp_send_work = [Handle()]
        with self.assertRaises(RuntimeError):
            self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(self.worker._pp_send_work[0].waits, 0)

    def test_front_retains_tensors_until_real_send_completion(self):
        self.group.is_first_rank = True
        self.group.is_last_rank = False
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertIsNotNone(self.worker._ks_pp_retained_send_tensors)
        self.events[-1].complete = True
        with self.assertRaises(RuntimeError):
            self.worker.release_pp_stage_batch(("批次1", 0))
        self.group.handles[0].complete = True
        with self.assertRaises(RuntimeError):
            self.worker.release_pp_stage_batch(("批次1", 0))
        self.events[-1].complete = True
        self.worker.release_pp_stage_batch(("批次1", 0))
        self.assertIsNone(self.worker._ks_pp_retained_send_tensors)

    def test_release_probe_pending_returns_false_without_releasing_tensors(self):
        self.group.is_first_rank = True
        self.group.is_last_rank = False
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        retained = self.worker._ks_pp_retained_send_tensors
        self.assertFalse(self.worker.pp_stage_release_ready(("批次1", 0)))
        self.events[-1].complete = True
        self.assertFalse(self.worker.pp_stage_release_ready(("批次1", 0)))
        self.assertIs(self.worker._ks_pp_retained_send_tensors, retained)
        self.group.handles[0].complete = True
        self.assertFalse(self.worker.pp_stage_release_ready(("批次1", 0)))
        self.assertIs(self.worker._ks_pp_retained_send_tensors, retained)
        self.events[-1].complete = True
        self.assertTrue(self.worker.pp_stage_release_ready(("批次1", 0)))
        self.assertIs(self.worker._ks_pp_retained_send_tensors, retained)
        self.worker.release_pp_stage_batch(("批次1", 0))
        self.assertIsNone(self.worker._ks_pp_retained_send_tensors)

    def test_back_release_probe_requires_final_core_ack(self):
        self.ready()
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.events[-1].complete = True
        self.worker.model_runner.execute_model_state = None
        self.assertFalse(self.worker.pp_stage_release_ready(("批次1", 0)))
        self.assertTrue(self.worker.pp_stage_release_ready(("批次1", 0), final_output_consumed=True))

    def test_front_cannot_release_unconsumed_connector_output(self):
        self.group.is_first_rank = True
        self.group.is_last_rank = False
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.events[-1].complete = True
        self.group.handles[0].complete = True
        self.worker.pp_stage_release_ready(("批次1", 0))
        self.events[-1].complete = True
        self.worker.model_runner.kv_connector_output = object()
        self.assertFalse(self.worker.pp_stage_release_ready(("批次1", 0)))

    def test_release_requires_device_completion_and_sample_consumed(self):
        self.ready()
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        with self.assertRaises(RuntimeError):
            self.worker.release_pp_stage_batch(("批次1", 0))
        self.events[-1].complete = True
        with self.assertRaises(RuntimeError):
            self.worker.release_pp_stage_batch(("批次1", 0))
        self.worker.model_runner.execute_model_state = None
        with self.assertRaises(RuntimeError):
            self.worker.release_pp_stage_batch(("批次1", 0))
        self.worker.release_pp_stage_batch(("批次1", 0), final_output_consumed=True)

    def test_released_batch_key_is_never_reused(self):
        self.ready()
        self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.events[-1].complete = True
        self.worker.model_runner.execute_model_state = None
        self.worker.release_pp_stage_batch(("批次1", 0), final_output_consumed=True)
        with self.assertRaises(RuntimeError):
            self.worker.prepare_pp_stage_input(("批次1", 0), output())

    def test_default_execute_keeps_original_receive_path(self):
        self.group.handles[0].complete = True
        self.worker.execute_model(output())
        self.assertEqual(self.group.recv_calls, 1)
        self.assertEqual(len(self.runner_calls), 1)

    def test_mixing_legacy_execute_into_stage_owned_state_rejected(self):
        self.prepare()
        with self.assertRaises(RuntimeError):
            self.worker.execute_model(output())
        self.assertEqual(self.runner_calls, [])

    def test_unknown_device_event_does_not_imply_ready(self):
        self.prepare()
        self.worker._ks_pp_stage_current["input_event"] = object()
        self.assertFalse(self.worker.pp_stage_input_ready(("批次1", 0)))

    def test_input_postprocess_failure_cannot_be_retried_as_success(self):
        def fail():
            raise RuntimeError("simulated TP all-gather failure")
        self.group.postprocess = fail
        self.prepare()
        self.group.handles[0].complete = True
        with self.assertRaises(RuntimeError):
            self.worker.finalize_pp_stage_input(("批次1", 0))
        with self.assertRaises(RuntimeError):
            self.worker.execute_pp_stage_model(("批次1", 0), output())
        self.assertEqual(self.runner_calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
