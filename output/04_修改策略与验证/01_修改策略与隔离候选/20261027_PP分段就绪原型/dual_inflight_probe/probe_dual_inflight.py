"""One bounded CPU case: actual core schedule/append/pop with two inflight batches."""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
import json
import os
from pathlib import Path
import queue
import threading
import time
from types import SimpleNamespace
from typing import Any, cast
import weakref

HERE = Path(__file__).resolve().parent
CORE_SOURCE = HERE.parent / "draft_wait_followup/core.py"
FUTURE_SOURCE = HERE.parent / "draft_wait_followup/multiproc_executor.py"


def load_actual_methods():
    nodes = []
    for node in ast.parse(FUTURE_SOURCE.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name == "FutureWrapper":
            nodes.append(node)
    for node in ast.parse(CORE_SOURCE.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name == "_KSModelResultReader":
            nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "EngineCore":
            names = {"configure_ks_core_event_pump", "step_with_batch_queue",
                     "ks_model_wait_inflight", "ks_draft_wait_pending"}
            node.body = [n for n in node.body if isinstance(n, ast.FunctionDef)
                         and (n.name in names or n.name.startswith("_ks_"))]
            nodes.append(node)
    module = ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__",
        names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[]))
    exec(compile(module, "<actual-core-and-FutureWrapper-AST>", "exec"), globals())


def run_case():
    load_actual_methods()
    trace, trace_lock = [], threading.Lock()
    def record(action, **values):
        with trace_lock:
            trace.append({"sequence": len(trace)+1, "action": action,
                          "thread_id": threading.get_ident(), **values})

    gate1, gate2 = threading.Event(), threading.Event()
    entered1, entered2 = threading.Event(), threading.Event()
    callback1, callback2 = threading.Event(), threading.Event()
    first_step_checked = threading.Event()
    thread_errors, responses, callbacks, updates = [], [], [], []
    futures_queue, event_queue = deque(), queue.Queue()
    core_thread = threading.get_ident()
    core = EngineCore.__new__(EngineCore)
    scheduled = [SimpleNamespace(batch="B2", total_num_scheduled_tokens=1,
                                 pending_structured_output_tokens=False)]
    schedule1 = SimpleNamespace(batch="B1", total_num_scheduled_tokens=1,
                                pending_structured_output_tokens=False)
    futures = {}

    def get_response(name, gate=None, entered=None):
        def read():
            record("get_response_enter", name=name)
            responses.append((name, threading.get_ident()))
            if entered is not None:
                entered.set()
            if gate is not None and not gate.wait(4):
                raise TimeoutError(f"CPU {name} gate was not released")
            result = {"execute1": "B1-executed", "sample1": "B1-final",
                      "execute2": "B2-executed", "sample2": "B2-final"}[name]
            record("get_response_return", name=name)
            return result
        return read

    futures["execute1"] = FutureWrapper(futures_queue, get_response("execute1", gate1, entered1))
    futures["sample1"] = FutureWrapper(futures_queue, get_response("sample1"))

    class ObservedBatchQueue(deque):
        # Trace the actual core's deque operations while preserving deque behavior.
        def appendleft(self, item):
            super().appendleft(item)
            record("batch_queue_append", batch=item[1].batch, queue_length=len(self))
        def pop(self):
            before = len(self)
            item = super().pop()
            record("batch_queue_pop", batch=item[1].batch,
                   before_length=before, queue_length=len(self))
            return item

    core.batch_queue_size = 2
    core.batch_queue = ObservedBatchQueue(
        [(futures["sample1"], schedule1, futures["execute1"])], maxlen=2)
    core.is_ec_consumer, core.is_pooling_model, core.use_spec_decode = True, False, False
    core.log_error_detail = core.log_iteration_details = lambda output: nullcontext()
    core._process_aborts_queue = lambda: None

    def schedule():
        result = scheduled.pop(0)
        record("scheduler_schedule", batch=result.batch)
        return result
    def update(output, model_result):
        updates.append(output.batch)
        record("scheduler_update", batch=output.batch, model_result=model_result)
        return {0: model_result}
    core.scheduler = SimpleNamespace(has_requests=lambda: bool(scheduled), schedule=schedule,
        get_grammar_bitmask=lambda output: "CPU-grammar", update_from_output=update)
    def execute(output, non_block):
        assert threading.get_ident() == core_thread and non_block and output.batch == "B2"
        record("executor_submit", name="execute2")
        futures["execute2"] = FutureWrapper(futures_queue, get_response("execute2", gate2, entered2))
        return futures["execute2"]
    def sample(grammar, non_block):
        assert threading.get_ident() == core_thread and non_block and grammar == "CPU-grammar"
        record("executor_submit", name="sample2")
        futures["sample2"] = FutureWrapper(futures_queue, get_response("sample2"))
        return futures["sample2"]
    def poll():
        events = []
        while True:
            try: events.append(event_queue.get_nowait())
            except queue.Empty: return events
    core.model_executor = SimpleNamespace(execute_model=execute, sample_tokens=sample,
                                          poll_stage_events=poll)

    def callback(event):
        assert threading.get_ident() == core_thread
        occupancy = len(core.batch_queue) + core.ks_model_wait_inflight()
        if event == "B1-wait-stage-event":
            assert entered1.is_set() and not gate1.is_set() and not gate2.is_set()
            assert not futures["sample1"].done() and not futures["sample2"].done()
            assert not futures["execute2"].done()
            assert len(core.batch_queue) == 1 and core.batch_queue[0][1].batch == "B2"
            assert core._ks_waiting_model_future is futures["sample1"]
            assert core.ks_model_wait_inflight() == 1 and occupancy == 2
            assert updates == []
            callbacks.append({"event": event, "queue_length": 1, "active_model_wait": 1,
                              "occupied_model_slots": 2, "scheduler_updates": []})
            record("core_callback", event=event, occupied_model_slots=occupancy)
            callback1.set()
        elif event == "B2-wait-stage-event":
            assert entered2.is_set() and gate1.is_set() and not gate2.is_set()
            assert futures["sample1"].done() and not futures["sample2"].done()
            assert len(core.batch_queue) == 0 and core.ks_model_wait_inflight() == 1
            assert occupancy == 1 and updates == ["B1"]
            callbacks.append({"event": event, "queue_length": 0, "active_model_wait": 1,
                              "occupied_model_slots": 1, "scheduler_updates": list(updates)})
            record("core_callback", event=event, occupied_model_slots=occupancy)
            callback2.set()
        else:
            raise AssertionError("unexpected CPU stage event")

    def wait(event, label):
        if not event.wait(3):
            raise TimeoutError(label)
    def gate_driver():
        try:
            wait(entered1, "B1 reader did not enter")
            event_queue.put("B1-wait-stage-event")
            wait(callback1, "core did not process B1-wait event")
            record("release_B1_only")
            gate1.set()
            wait(first_step_checked, "first step was not checked")
            wait(entered2, "B2 reader did not enter")
            assert not gate2.is_set()
            event_queue.put("B2-wait-stage-event")
            wait(callback2, "core did not process B2-wait event")
            record("release_B2_only")
            gate2.set()
        except BaseException as error:
            thread_errors.append(f"{type(error).__name__}: {error}")
            gate1.set(); gate2.set()

    before_env = os.environ.get("KS_CORE_STAGE_EVENT_PUMP")
    os.environ["KS_CORE_STAGE_EVENT_PUMP"] = "1"
    try:
        core.configure_ks_core_event_pump(callback, poll_interval=.002)
    finally:
        if before_env is None: os.environ.pop("KS_CORE_STAGE_EVENT_PUMP", None)
        else: os.environ["KS_CORE_STAGE_EVENT_PUMP"] = before_env

    record("first_step_entry", queue_length=len(core.batch_queue), queue_capacity=2)
    driver = threading.Thread(target=gate_driver, name="CPUIndependentBatchGates")
    driver.start()
    try:
        first = core.step_with_batch_queue()
        assert first == ({0: "B1-final"}, True)
        assert updates == ["B1"] and len(core.batch_queue) == 1
        assert core.batch_queue[0][1].batch == "B2"
        assert not futures["execute2"].done() and not futures["sample2"].done()
        assert not gate2.is_set() and core.ks_model_wait_inflight() == 0
        assert [name for name, _ in responses] == ["execute1", "sample1"]
        record("after_first_step", queue_length=1, scheduler_updates=list(updates),
               B2_pending=True, B2_gate_released=False)
        first_step_checked.set()
        second = core.step_with_batch_queue()
        assert second == ({0: "B2-final"}, False)
        assert updates == ["B1", "B2"] and not core.batch_queue
        assert core.ks_model_wait_inflight() == 0 and not futures_queue
        assert [name for name, _ in responses] == ["execute1", "sample1", "execute2", "sample2"]
        assert {thread for _, thread in responses} == {core._ks_result_reader.thread.ident}
        assert core._ks_result_reader.thread.ident != core_thread
        assert len(callbacks) == 2
        assert any(row["action"] == "batch_queue_append" and row["queue_length"] == 2 for row in trace)
        assert all(future.done() for future in futures.values())
    finally:
        gate1.set(); gate2.set(); first_step_checked.set()
        driver.join(4)
        reader_stopped = core._ks_result_reader.close(timeout=1)
    assert not driver.is_alive() and reader_stopped and not thread_errors, thread_errors
    return {"status": "CPU_PASS", "scope": "ONE_REAL_CORE_DUAL_INFLIGHT_CASE",
            "runtime_status": "NOT_RUNTIME_INTEGRATED", "actual_core": str(CORE_SOURCE),
            "actual_FutureWrapper": str(FUTURE_SOURCE), "callback_snapshots": callbacks,
            "model_reply_FIFO": [name for name, _ in responses], "scheduler_update_order": updates,
            "single_model_reply_reader": True, "independent_CPU_batch_gates": 2,
            "threads_left_alive": 0, "server_actions": 0, "npu_actions": 0,
            "production_source_changes": False, "trace": trace,
            "NOT_VERIFIED": ["real PP2 model", "MTP/GLM", "real LMCache restore",
                             "real scheduler data", "performance"]}


if __name__ == "__main__":
    try:
        report = run_case()
    except BaseException as error:
        report = {"status": "CPU_FAIL", "error": f"{type(error).__name__}: {error}",
                  "runtime_status": "NOT_RUNTIME_INTEGRATED", "server_actions": 0, "npu_actions": 0}
    (HERE / "cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["status"] == "CPU_PASS" else 1)
