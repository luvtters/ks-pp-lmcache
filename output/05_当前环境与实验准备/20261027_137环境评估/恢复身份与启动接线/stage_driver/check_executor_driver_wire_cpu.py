"""Actual worker/executor AST plus typed wire and actual KV aggregator CPU checks."""
from __future__ import annotations
import ast
from collections import deque
from contextlib import nullcontext
from concurrent.futures import Future
import copy
import dataclasses
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time
from types import SimpleNamespace
import unittest
import weakref

HERE = Path(__file__).resolve().parent
OVERLAY = HERE / "runtime_overlay"
sys.path.insert(0, str(OVERLAY))
import ks_stage_executor_driver as wire
from span_stage_controller import StageController


def extract(path, clsname, names, namespace):
    return extract_source(Path(path).read_text(encoding="utf-8"), clsname, names, namespace, str(path))


def extract_source(source, clsname, names, namespace, filename="actual_source"):
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == clsname)
    cls.bases, cls.decorator_list = [], []
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in cls.body} == set(names)
    for node in cls.body: node.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), filename, "exec"), namespace)
    return namespace[clsname]


class KVOutput:
    def __init__(self, **kwargs):
        self.finished_recving = kwargs.get("finished_recving")
        self.finished_sending = kwargs.get("finished_sending")
        self.expected_finished_count = kwargs.get("expected_finished_count", 4)
        self.kv_connector_stats = kwargs.get("kv_connector_stats")
        self.kv_connector_worker_meta = kwargs.get("kv_connector_worker_meta")
        self.kv_cache_events = kwargs.get("kv_cache_events")
        self.invalid_block_ids = kwargs.get("invalid_block_ids", set())
    def is_empty(self): return False


class ModelOutput:
    def __init__(self, kv_connector_output=None, request=None):
        self.kv_connector_output = kv_connector_output
        self.req_ids = [request] if request else []
        self.req_id_to_index = {request: 0} if request else {}
        self.sampled_token_ids = [[71]] if request else []


class AsyncOutput:
    def __init__(self, output, resolutions):
        self.output, self.resolutions = output, resolutions
    def get_output(self):
        self.resolutions.append(self.output.req_ids)
        return self.output


class CPUEvent:
    def __init__(self): self.ready = True
    def record(self): return None
    def query(self): return self.ready


class CPUHandle:
    def is_completed(self): return True
    def wait(self): return None


class CPUIntermediate:
    def __init__(self, tensors, comm_handles, comm_postprocess):
        self.tensors, self.handles = tensors, comm_handles
    def wait_for_comm(self): return None


def fixture_output(request, batch_id):
    return SimpleNamespace(total_num_scheduled_tokens=1, num_scheduled_tokens={request: 1},
        scheduled_new_reqs=[SimpleNamespace(req_id=request, num_computed_tokens=31, block_ids=([1, 2],))],
        scheduled_cached_reqs=SimpleNamespace(req_ids=[], num_computed_tokens=[], new_block_ids=[]),
        scheduled_spec_decode_tokens={}, pending_structured_output_tokens=False,
        ks_stage_batch={"batch_id": batch_id, "generation": 0, "requests": ((request, 1),),
                        "token_positions": ((31,),), "restored_prefix_tokens": {request: 31}})


class World:
    """4-rank CPU transport fixture; all stage methods are original AST."""
    def __init__(self, tp_size=2, blocking_sample=False):
        world_size = 2 * tp_size
        self.blocking_sample = blocking_sample
        self.front_waiting = [threading.Event() for _ in range(tp_size)]
        self.back_broadcast = [threading.Event() for _ in range(tp_size)]
        self.front_receive_finished = []
        gpu_raw = (HERE.parents[1] / "真实KV恢复验证/core_mtp_source_bundle.txt").read_text(encoding="utf-8")
        gpu_source = "\n".join(line.partition(":")[2] for line in gpu_raw.splitlines() if line.partition(":")[0].isdigit())
        self.sent, self.commands, self.samples, self.resolutions = [], deque(), [], []
        self.groups = (tuple(range(tp_size)), tuple(range(tp_size, world_size)))
        self.controller = StageController(self.groups, 2, 2)
        raw = (HERE.parents[1] / "真实KV恢复验证/actual137_kv_aggregator.txt").read_text(encoding="utf-8")
        source = "\n".join(line.partition(":")[2] for line in raw.splitlines() if line.partition(":")[0].isdigit())
        temp = HERE / "_cpu_aggregator_source.py"
        temp.write_text(source, encoding="utf-8")
        env = {"KVConnectorOutput": KVOutput, "logger": SimpleNamespace(debug=lambda *a: None)}
        try:
            aggregate_type = extract(temp, "KVOutputAggregator", {"__init__", "aggregate"}, env)
        finally:
            temp.unlink()
        self.aggregator = aggregate_type(world_size)
        self.executor = SimpleNamespace(output_rank=tp_size, kv_output_aggregator=self.aggregator,
            rpc_broadcast_mq=SimpleNamespace(enqueue=self.enqueue), _ks_stage_channels={},
            _ks_stage_poll_owner=None, _ks_stage_sequences={}, _ks_stage_tp_size=tp_size,
            _ks_stage_pending={}, _ks_restore_pcp_size=1, _ks_restore_tp_size=tp_size)
        self.driver = wire.FirstTokenStageDriver(self.executor, self.controller, (ModelOutput, AsyncOutput), async_scheduling=True)
        self.executor._ks_stage_driver = self.driver
        self.procs = []
        for rank in range(world_size):
            stage = rank // tp_size
            pp = SimpleNamespace(is_first_rank=stage == 0, is_last_rank=stage == 1, world_size=2,
                                 rank=stage, last_rank=1, device_group="CPU broadcast fixture")
            pp.irecv_tensor_dict = lambda **kwargs: ({"activation": "CPU fixture"}, [CPUHandle()], None)
            namespace = {"get_pp_group": lambda pp=pp: pp, "get_tp_group": lambda: object(),
                "enable_sp": lambda: False, "AsyncIntermediateTensors": CPUIntermediate,
                "torch": SimpleNamespace(npu=SimpleNamespace(Event=CPUEvent)), "trace_worker": wire.trace_worker}
            methods = {"_ks_pp_batch_signature", "_ks_pp_stage_record", "_ks_pp_handles_done",
                "prepare_pp_stage_input", "pp_stage_input_received", "finalize_pp_stage_input",
                "pp_stage_input_ready", "execute_pp_stage_model", "pp_stage_model_ready",
                "pp_stage_release_ready", "release_pp_stage_batch", "sample_tokens"}
            worker_type = extract(OVERLAY / "vllm_ascend/worker/worker.py", "NPUWorker", methods, namespace)
            worker = worker_type()
            worker._pp_send_work = []
            runner = SimpleNamespace(execute_model_state=None, kv_connector_output=None,
                                     use_async_scheduling=True, is_kv_producer=True)
            worker.model_runner = runner
            def execute(output, worker=worker, stage=stage):
                worker.model_runner.kv_connector_output = KVOutput(finished_recving={next(iter(output.num_scheduled_tokens))}, expected_finished_count=world_size)
                worker.model_runner.execute_model_state = object() if stage else None
                return None
            worker.execute_model = execute
            if stage == 0:
                runner_env = {"get_pp_group": lambda pp=pp: pp, "EMPTY_MODEL_RUNNER_OUTPUT": ModelOutput(), "copy": copy.copy}
                runner_type = extract(HERE.parents[1] / "完整模型单机P对照/runtime/local-completion/model_runner_local.py",
                                      "NPUModelRunner", {"sample_tokens"}, runner_env)
                # This is the real front sample AST branch; its receive helper
                # is an explicit CPU fixture, not real PP communication.
                runner.__class__ if False else None
                actual_front = runner_type.__new__(runner_type)
                actual_front.__dict__.update(runner.__dict__)
                actual_front.vllm_config = SimpleNamespace(kv_transfer_config=SimpleNamespace(
                    get_from_extra_config=lambda key, default=None: True))
                if blocking_sample:
                    def broadcast_recv(tensor, src, group, lane=rank):
                        self.front_waiting[lane].set()
                        if not self.back_broadcast[lane].wait(3):
                            raise TimeoutError("front actual receiver waited without back sample broadcast")
                        self.front_receive_finished.append(lane)
                    fake_torch = SimpleNamespace(int32="int32", empty=lambda *a, **k: object(),
                        distributed=SimpleNamespace(broadcast=broadcast_recv))
                    receiver = extract_source(gpu_source, "GPUModelRunner",
                        {"_pp_receive_prev_sampled_token_ids_to_input_batch"},
                        {"get_pp_group": lambda pp=pp: pp, "torch": fake_torch,
                         "np": SimpleNamespace(nonzero=lambda values: ([],))})
                    actual_front.device = "CPU fixture"
                    actual_front.input_batch = SimpleNamespace(num_reqs=1, req_ids=["r"],
                        num_tokens_no_spec=[31], is_token_ids={})
                    actual_front.discard_request_mask = SimpleNamespace(np=[False])
                    actual_front.requests = {"r": SimpleNamespace(output_token_ids=[])}
                    actual_front._is_all_reqs_chunked_prefill = lambda: False
                    actual_front._pp_receive_prev_sampled_token_ids_to_input_batch = lambda actual=actual_front, method=receiver._pp_receive_prev_sampled_token_ids_to_input_batch: method(actual)
                else:
                    actual_front._pp_receive_prev_sampled_token_ids_to_input_batch = lambda: None
                worker.model_runner = actual_front
                def front_sample(grammar, worker=worker, actual=actual_front, method=runner_type.sample_tokens, rank=rank):
                    key = worker._ks_pp_stage_current["key"]
                    self.samples.append((key, rank))
                    return method(actual, grammar)
                worker.model_runner.sample_tokens = front_sample
            else:
                if blocking_sample:
                    sender = extract_source(gpu_source, "GPUModelRunner", {"_pp_broadcast_prev_sampled_token_ids"},
                        {"get_pp_group": lambda pp=pp: pp, "torch": SimpleNamespace(distributed=SimpleNamespace(
                            broadcast=lambda tensor, src, group, lane=rank-tp_size: self.back_broadcast[lane].set()))})
                    runner._is_all_reqs_chunked_prefill = lambda: False
                    runner._pp_broadcast_prev_sampled_token_ids = lambda tensor, runner=runner, method=sender._pp_broadcast_prev_sampled_token_ids: method(runner, tensor)
                def back_sample(grammar, worker=worker, rank=rank):
                    key = worker._ks_pp_stage_current["key"]
                    self.samples.append((key, rank))
                    if blocking_sample:
                        worker.model_runner._pp_broadcast_prev_sampled_token_ids(SimpleNamespace(dim=lambda: 2, shape=(1, 1)))
                    request = next(iter(self.driver.batches[key].output.num_scheduled_tokens))
                    output = ModelOutput(worker.model_runner.kv_connector_output, request)
                    worker.model_runner.kv_connector_output = None
                    worker.model_runner.execute_model_state = None
                    return AsyncOutput(output, self.resolutions)
                worker.model_runner.sample_tokens = back_sample
            publisher = SimpleNamespace(lock=threading.Lock(), stopped=threading.Event(), failed=threading.Event(),
                                        sequence=0, pending=queue.Queue(maxsize=64))
            proc = SimpleNamespace(rank=rank, _ks_stage_index=stage, worker=worker, _ks_stage_publisher=publisher)
            self.procs.append(proc)
            self.executor._ks_stage_channels[rank] = (queue.Queue(), publisher.failed)
        env = {"threading": threading, "queue": queue, "KSStageWireReply": wire.KSStageWireReply,
               "ModelRunnerOutput": ModelOutput, "AsyncModelRunnerOutput": AsyncOutput,
               "KSStageEvent": type("UnusedStageEvent", (), {}), "KSRestoreEvent": type("UnusedRestoreEvent", (), {})}
        executor_type = extract(OVERLAY / "vllm/v1/executor/multiproc_executor.py", "MultiprocExecutor", {"poll_stage_events"}, env)
        self.poll = lambda: executor_type.poll_stage_events(self.executor)

    def enqueue(self, value):
        command = value[0]
        self.sent.append(command)
        self.commands.append(command)

    def restore(self, request, stage):
        self.controller.begin_restore(request, 1)
        for rank in self.groups[stage]:
            self.controller.mark_kv_ready(request, 1, stage, rank, device_complete=True)

    def tick(self):
        pending = list(self.commands)
        self.commands.clear()
        threads = []
        for command in pending:
            for proc in self.procs:
                if self.blocking_sample and command.operation == "sample" and proc._ks_stage_index == command.target_stage:
                    thread = threading.Thread(target=wire.handle_worker_command,
                        args=(proc, command, (ModelOutput, AsyncOutput), AsyncOutput), daemon=True)
                    thread.start()
                    threads.append(thread)
                else:
                    wire.handle_worker_command(proc, command, (ModelOutput, AsyncOutput), AsyncOutput)
            if self.blocking_sample and command.operation == "sample" and command.target_stage == 0:
                if not all(event.wait(1) for event in self.front_waiting):
                    raise AssertionError("actual front receiver did not enter its blocking broadcast")
                if self.front_receive_finished:
                    raise AssertionError("front receive completed before any back sample broadcast")
        for thread in threads:
            thread.join(4)
            if thread.is_alive(): raise AssertionError("CPU sample worker remained blocked")
        for proc in self.procs:
            while not proc._ks_stage_publisher.pending.empty():
                self.executor._ks_stage_channels[proc.rank][0].put(proc._ks_stage_publisher.pending.get_nowait())
        self.poll()

    def until(self, predicate, limit=100):
        for _ in range(limit):
            if predicate(): return
            self.tick()
        raise AssertionError("CPU phase wire did not reach its expected state")


class Checks(unittest.TestCase):
    def test_actual_core_batch_queue_wait_pumps_wire_then_updates_and_acks(self):
        world = World()
        for stage in (0, 1): world.restore("r", stage)
        output = fixture_output("r", "actual-core-b1")
        core_path = OVERLAY / "vllm/v1/engine/core.py"
        tree = ast.parse(core_path.read_text(encoding="utf-8"))
        reader = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "_KSModelResultReader")
        namespace = {"threading": threading, "queue": queue, "weakref": weakref, "Future": Future,
                     "os": os, "cast": lambda kind, value: value, "ModelRunnerOutput": ModelOutput}
        exec(compile(ast.Module(body=[reader], type_ignores=[]), str(core_path), "exec"), namespace)
        core_type = extract(core_path, "EngineCore", {"step_with_batch_queue", "configure_ks_core_event_pump",
            "_ks_consume_stage_events", "_ks_wait_model_future", "_ks_wait_executor_future", "_ks_close_core_event_pump"}, namespace)
        core = core_type()
        pending = deque([output])
        updates, acknowledgments, polls = [], [], []
        def update(scheduled, result):
            self.assertIs(scheduled, output)
            self.assertEqual(result.sampled_token_ids, [[71]])
            updates.append(result)
            return {0: "actual core fixture output"}
        def acknowledge(scheduled, result):
            self.assertEqual(updates, [result])
            acknowledgments.append(result)
            world.driver.core_consumed(scheduled, result)
        def poll():
            polls.append(threading.get_ident())
            world.tick()
            return []
        core.model_executor = SimpleNamespace(execute_model=lambda scheduled, non_block: world.driver.admit(scheduled),
            sample_tokens=lambda grammar, non_block: world.driver.request_sample(grammar),
            ks_ack_stage_result=acknowledge, poll_stage_events=poll)
        core.scheduler = SimpleNamespace(has_requests=lambda: bool(pending), schedule=lambda: pending.popleft(),
            get_grammar_bitmask=lambda scheduled: None, update_from_output=update)
        core.batch_queue, core.batch_queue_size = deque(), 2
        core.is_ec_consumer, core.is_pooling_model, core.use_spec_decode = True, False, True
        core.log_error_detail = core.log_iteration_details = lambda scheduled: nullcontext()
        core._process_aborts_queue = lambda: None
        previous = os.environ.get("KS_CORE_STAGE_EVENT_PUMP")
        os.environ["KS_CORE_STAGE_EVENT_PUMP"] = "1"
        try:
            core.configure_ks_core_event_pump(lambda event: None, poll_interval=0.001)
            self.assertEqual(core.step_with_batch_queue(), (None, True))
            self.assertEqual(updates, [])
            self.assertEqual(core.step_with_batch_queue(), ({0: "actual core fixture output"}, False))
            self.assertEqual(len(acknowledgments), 1)
            self.assertTrue(all(owner == threading.get_ident() for owner in polls))
            world.until(lambda: not world.driver.batches)
            core._ks_close_core_event_pump()
        finally:
            if previous is None: os.environ.pop("KS_CORE_STAGE_EVENT_PUMP", None)
            else: os.environ["KS_CORE_STAGE_EVENT_PUMP"] = previous

    def test_device_query_error_is_failure_and_retains_batch(self):
        world = World()
        world.restore("r", 0)
        output = fixture_output("r", "device-error")
        execute = world.driver.admit(output)
        final = world.driver.request_sample(None)
        # Execute records its real phase event, then an actual query exception
        # must reach the core as ERROR without retiring the worker buffer.
        world.tick()
        class BrokenEvent:
            def query(self): raise RuntimeError("actual device query failed")
        for proc in world.procs[:2]:
            proc.worker._ks_pp_stage_current["model_event"] = BrokenEvent()
        with self.assertRaisesRegex(RuntimeError, "retain all batch/device ownership"):
            world.tick()
        self.assertTrue(execute.done())
        self.assertTrue(final.done())
        self.assertIsNotNone(execute.exception())
        self.assertEqual(world.controller.inflight_count, 1)
        self.assertEqual(len(world.driver.batches), 1)
        self.assertTrue(all(proc.worker._ks_pp_stage_current is not None for proc in world.procs))

    def test_actual_receiver_blocks_until_back_sample_without_driver_deadlock(self):
        world = World(tp_size=8, blocking_sample=True)
        for stage in (0, 1): world.restore("r", stage)
        output = fixture_output("r", "blocking-b1")
        world.driver.admit(output)
        final = world.driver.request_sample(None)
        world.until(final.done)
        self.assertEqual(sorted(world.front_receive_finished), list(range(8)))
        self.assertEqual(len(world.resolutions), 8)
        self.assertEqual(len(world.samples), 16)
        self.assertEqual(final.result().sampled_token_ids, [[71]])
        for proc in world.procs[:8]:
            self.assertEqual(proc.worker.model_runner.input_batch.prev_req_id_to_index, {"r": 0})
            self.assertEqual(proc.worker.model_runner.requests["r"].output_token_ids, [-1])
        world.driver.core_consumed(output, final.result())
        world.until(lambda: not world.driver.batches)

    def test_actual_worker_loop_keeps_typed_phase_out_of_old_response_mq(self):
        world = World()
        output = fixture_output("r", "b1")
        command = wire.KSStageRPCCommand("actual-loop", ("b1", 0), 0, "execute", scheduler_output=output)
        messages = deque([(command, (), {}, None)])
        def dequeue(**kwargs):
            if not messages: raise StopIteration()
            return messages.popleft()
        proc = world.procs[0]
        proc.rpc_broadcast_mq = SimpleNamespace(dequeue=dequeue)
        old = []
        proc.handle_output = old.append
        namespace = {"KSStageRPCCommand": wire.KSStageRPCCommand,
            "handle_worker_command": wire.handle_worker_command,
            "ModelRunnerOutput": ModelOutput, "AsyncModelRunnerOutput": AsyncOutput,
            "KSStageCommand": type("UnusedLegacyCommand", (), {})}
        cls = extract(OVERLAY / "vllm/v1/executor/multiproc_executor.py", "WorkerProc", {"worker_busy_loop"}, namespace)
        with self.assertRaises(StopIteration): cls.worker_busy_loop(proc)
        self.assertEqual(old, [])
        reply = proc._ks_stage_publisher.pending.get_nowait()
        self.assertIsInstance(reply, wire.KSStageWireReply)
        self.assertEqual(reply.reply.operation, "execute")
        self.assertIs(reply.reply.kind, wire.ReplyKind.MODEL_RETURN)
        self.assertIsNone(reply.reply.payload)

    def test_front_executes_while_back_kv_missing_and_none_is_not_ready(self):
        world = World()
        world.restore("r", 0)
        output = fixture_output("r", "b1")
        execute = world.driver.admit(output)
        final = world.driver.request_sample(None)
        for _ in range(10): world.tick()
        self.assertTrue(any(c.operation == "execute" and c.target_stage == 0 for c in world.sent))
        self.assertFalse(any(c.operation == "execute" and c.target_stage == 1 for c in world.sent))
        self.assertFalse(execute.done())
        self.assertFalse(final.done())
        self.assertEqual(world.samples, [])

    def test_async_output_resolved_all_rank_kv_once_and_real_final_consumed(self):
        world = World()
        for stage in (0, 1): world.restore("r", stage)
        output = fixture_output("r", "b1")
        execute = world.driver.admit(output)
        final = world.driver.request_sample(None)
        world.until(final.done)
        actual = final.result()
        self.assertIsNone(execute.result())
        self.assertEqual(actual.sampled_token_ids, [[71]])
        self.assertEqual(actual.kv_connector_output.finished_recving, {"r"})
        self.assertEqual(world.aggregator._recv_remaining_count, {})
        self.assertEqual(len(world.resolutions), 2)
        self.assertEqual(sorted(world.samples), [(('b1', 0), rank) for rank in range(4)])
        self.assertEqual(world.driver.ledger.inflight_count, 1)
        world.driver.core_consumed(output, actual)
        world.until(lambda: not world.driver.batches)
        self.assertEqual(world.controller.inflight_count, 0)

    def test_two_whole_slots_fifo_retains_front_payload_and_never_resamples_b1(self):
        world = World()
        for request in ("r1", "r2"):
            for stage in (0, 1): world.restore(request, stage)
        first, second = fixture_output("r1", "b1"), fixture_output("r2", "b2")
        world.driver.admit(first); final1 = world.driver.request_sample(None)
        world.driver.admit(second); final2 = world.driver.request_sample(None)
        self.assertEqual(world.controller.inflight_count, 2)
        world.until(final1.done)
        self.assertFalse(final2.done())
        self.assertEqual(len(world.driver.ledger.payloads_for(("b1", 0))), 8)
        world.driver.core_consumed(first, final1.result())
        world.until(final2.done)
        self.assertEqual(sum(key == ("b1", 0) for key, rank in world.samples), 4)
        self.assertEqual(sum(key == ("b2", 0) for key, rank in world.samples), 4)
        world.driver.core_consumed(second, final2.result())
        world.until(lambda: not world.driver.batches)

    def test_unknown_async_mode_or_scheduled_spec_rejected(self):
        world = World()
        with self.assertRaises(ValueError):
            wire.FirstTokenStageDriver(world.executor, world.controller, (ModelOutput,), async_scheduling=False)
        world.restore("r", 0)
        output = fixture_output("r", "b1")
        output.scheduled_spec_decode_tokens = {"r": [1]}
        with self.assertRaises(ValueError): world.driver.admit(output)

    def test_core_may_not_ack_none_or_wrong_model_output(self):
        world = World()
        for stage in (0, 1): world.restore("r", stage)
        output = fixture_output("r", "b1")
        world.driver.admit(output); final = world.driver.request_sample(None)
        with self.assertRaises(RuntimeError): world.driver.core_consumed(output, None)
        world.until(final.done)
        with self.assertRaises(RuntimeError): world.driver.core_consumed(output, ModelOutput())


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Checks)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"status": "PASS_CPU_ACTUAL_AST_WIRE_ONLY" if result.wasSuccessful() else "FAIL",
              "passed": result.testsRun - len(result.failures) - len(result.errors), "total": result.testsRun,
              "errors": [str(e) for _, e in result.errors], "failures": [str(e) for _, e in result.failures],
              "device_imports": False, "real_forward_or_communication": False,
              "actual_sources": ["private actual137 worker phase methods", "actual NPU front sample branch",
                                  "private actual137 executor.poll_stage_events", "actual137 KVOutputAggregator"]}
    (HERE / "executor_driver_cpu_result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
