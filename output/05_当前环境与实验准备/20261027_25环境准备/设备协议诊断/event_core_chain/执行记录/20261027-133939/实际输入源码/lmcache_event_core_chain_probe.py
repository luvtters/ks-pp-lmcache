"""Real NPU event -> observer -> Queue -> actual EngineCore AST wait seam.

Parent is CPU-only. Two spawned children share one explicit NPU. No model,
real LMCache KV, full EngineCore bootstrap, PP2 integration, or speedup claim.
"""
from __future__ import annotations

import argparse
from collections import deque
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
from datetime import datetime, timezone
import json
import multiprocessing as mp
import os
from pathlib import Path, PurePosixPath
import sys
import threading
import time
import traceback
from types import SimpleNamespace


ROOT = Path("/opt/ks-stage-ready/device-probe")
REQUEST = "event-core-chain-component"
IDENTITY = dict(request_id=REQUEST, restore_generation=1, stage_id=0,
                tp_rank=0, pcp_rank=0, global_worker_rank=0)


def cpu_plan(args):
    return {"status": "CPU_PLAN_ONLY", "device_imports_performed": False,
            "device_allocation_performed": False, "planned_device": "npu:" + str(args.device),
            "parent": "CPU-only actual candidate EngineCore AST seam + FutureWrapper",
            "children": ["producer", "consumer"], "timeout_seconds": args.timeout,
            "component_topology": "two logical stages, one worker each; not TP8",
            "synthetic_request_identity": IDENTITY,
            "route": ["real NPU nonempty IPC event", "real DeviceMessagingFuture owner",
                      "LMCacheStageObserver.drain_success", "publish_observer_success",
                      "real _KSStageEventPublisher", "multiprocessing.Queue",
                      "real poll_stage_events", "EngineCore.step_with_batch_queue/_ks_wait_model_future",
                      "RestoreEventBinding", "StageController.mark_kv_ready"],
            "release_rule": "core callback confirms pending Future, updates=0, waiting_inflight=1",
            "publisher_close_rule": "only after parent CORE_CONSUMED ACK",
            "not_verified": ["full EngineCore bootstrap", "GLM", "real LMCache KV",
                             "TP8/PP2 model integration", "performance"]}


def consumer(device_index, timeout, connection, report, directory, transport,
             channel_failed, core_ack):
    import device_component as component
    import chain_runtime as runtime
    import restore_binding
    import stage_observer

    send = component.status_sender("consumer", report, directory)
    keepalive = [connection, transport, channel_failed, core_ack]
    safe = False
    received_event = False
    publisher = observer = None
    core_consumed = False
    deadline = time.monotonic() + timeout
    audit = None
    try:
        torch, device, backend, messaging_class, future_class = component.load_device(device_index, send)
        audit, profile = component.owner_profiler(backend, future_class)

        @contextmanager
        def owner_context():
            with torch.npu.device(device):
                audit["owner"] = {**component.stamp(), "device": str(device),
                                  "current_device": int(torch.npu.current_device())}
                previous = sys.getprofile()
                sys.setprofile(profile)
                try:
                    yield
                finally:
                    sys.setprofile(previous)

        observer = stage_observer.LMCacheStageObserver(messaging_class, owner_context)
        observer.start()
        publisher = runtime._KSStageEventPublisher(transport, channel_failed, 0, 0,
                                                   tp_size=1, pcp_size=1)
        keepalive.extend([torch, device, backend, observer, publisher])
        send("owner_and_publisher_started", owner=audit["owner"],
             publisher_module=type(publisher).__module__)
        connection.send({"kind": "READY"})
        payload = component.receive(connection, deadline, "producer handle timed out")
        if isinstance(payload, dict) and payload.get("kind") == "ABORT_BEFORE_EVENT":
            safe = observer.close(timeout=1)
            publisher.close()  # No success was published, so no notification can be dropped.
            send("final", status="PEER_ABORT", safe_device_completion=safe, peer=payload)
            return
        if (type(payload) is not tuple or len(payload) != 2 or type(payload[0]) is not bytes
                or not payload[0] or payload[1] is not True):
            raise RuntimeError("Refusing invalid or empty IPC handle payload")
        received_event = True
        raw = messaging_class()
        real = future_class(raw, device, backend)
        identity = stage_observer.StageRestoreIdentity(**IDENTITY)
        operation = observer.reserve(identity, ("event-core-chain-component", device_index, 64), None)
        proxy = observer.attach(operation, real)
        raw.retain_reference(backend)
        real.retain_reference(backend)
        keepalive.extend([payload, raw, real, identity, operation, proxy])
        raw.set_result(payload)
        send("raw_future_payload_returned", handle_length=len(payload[0]),
             raw_done=raw.query(), device_completion_not_inferred_from_payload=True)
        if not proxy.wait(max(0, deadline - time.monotonic())):
            raise TimeoutError("real device Future owner did not complete")
        proxy_result = proxy.result(timeout=0)
        safe = True
        send("real_device_completed_and_cpu_proxy_true", proxy_result=proxy_result,
             original_cpu_proxy=type(proxy) is messaging_class,
             original_real_future=type(real) is future_class)
        published = restore_binding.publish_observer_success(observer, publisher)
        second_publish = restore_binding.publish_observer_success(observer, publisher)
        send("observer_success_published", published_count=published,
             duplicate_count=second_publish, identity=asdict(identity),
             event_module=runtime.KSRestoreEvent.__module__)
        if proxy_result is not True or published != 1 or second_publish != 0:
            raise RuntimeError("Device success must publish exactly one identity")
        acknowledgment = component.receive(core_ack, deadline, "CORE_CONSUMED ACK timed out")
        core_consumed = (isinstance(acknowledgment, dict)
                         and acknowledgment.get("kind") == "CORE_CONSUMED"
                         and acknowledgment.get("request_id") == REQUEST)
        if not core_consumed:
            raise RuntimeError("Invalid parent core consumption acknowledgment")
        send("core_consumed_ack_received", ack=acknowledgment)
        publisher.close()
        if publisher.thread.is_alive() or channel_failed.is_set():
            raise RuntimeError("Publisher did not safely close after core consumption")
        if not observer.close(timeout=1):
            safe = False
            raise RuntimeError("Observer did not confirm safe shutdown")
        owner = audit["owner"]["thread_id"]
        methods = audit["methods"]
        owner_ok = all(methods.get(name, {}).get("thread_ids") == [owner]
                       and methods[name]["calls"] > 0 for name in (
                           "backend.import_event", "backend.query_event",
                           "backend.synchronize_event", "real.query", "real.result"))
        validation_ok = owner_ok and methods["backend.query_event"].get("last_bool") is True
        connection.send({"kind": "ACK", "safe_device_completion": True,
                         "validation_ok": validation_ok, "proxy_result": proxy_result,
                         "core_consumed": True})
        send("final", status="PASS" if validation_ok else "FAIL", safe_device_completion=True,
             proxy_result=proxy_result, published_count=published, duplicate_count=second_publish,
             publisher_closed_only_after_core_ack=True, owner_only_import_query_result=owner_ok,
             owner_audit=audit, success_identity=asdict(identity), core_consumed=True)
    except component.NotSupported as error:
        component.send_abort(connection, "NOT_SUPPORTED", error)
        send("final", status="NOT_SUPPORTED", error=str(error), safe_device_completion=True)
    except BaseException as error:
        unsafe = received_event and not safe
        if not received_event:
            component.send_abort(connection, "FAIL", error)
            if observer is not None:
                observer.close(timeout=1)
            if publisher is not None:
                publisher.close()
        send("final", status="FAIL_UNSAFE" if unsafe else "FAIL",
             error=repr(error), traceback=traceback.format_exc(), safe_device_completion=not unsafe,
             references_retained=received_event, core_consumed=core_consumed, owner_audit=audit)
        if received_event:
            # Do not drop a published-but-unacknowledged notification or release
            # uncertain device references. The outer supervisor records failure.
            component.retain_until_supervisor(keepalive)


def run_probe(args):
    import device_component as component
    import chain_runtime as runtime
    from restore_binding import RestoreEventBinding
    from stage_controller import StageController

    directory = Path(args.output_dir)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "plan.json").write_text(json.dumps(cpu_plan(args), indent=2), encoding="utf-8")
    context = mp.get_context("spawn")
    os.environ["KS_PP_STAGE_EVENTS"] = "1"
    os.environ["KS_CORE_STAGE_EVENT_PUMP"] = "1"
    executor = runtime.MultiprocExecutor.__new__(runtime.MultiprocExecutor)
    executor.parallel_config = SimpleNamespace(nnodes_within_dp=1)
    executor.world_size = executor.local_world_size = 2
    executor.is_failed = False
    executor._init_stage_event_channels(context, 1, 2, 1)
    producer_wire, consumer_wire = context.Pipe(duplex=True)
    producer_report, producer_send = context.Pipe(duplex=False)
    consumer_report, consumer_send = context.Pipe(duplex=False)
    core_ack_recv, core_ack_send = context.Pipe(duplex=False)
    transport, failed = executor._ks_stage_channels[0]
    children = [context.Process(name="event-chain-producer", target=component.producer,
                                args=(args.device, args.timeout, producer_wire, producer_send, str(directory))),
                context.Process(name="event-chain-consumer", target=consumer,
                                args=(args.device, args.timeout, consumer_wire, consumer_send,
                                      str(directory), transport, failed, core_ack_recv))]
    for child in children:
        child.start()
    for endpoint in (producer_wire, consumer_wire, producer_send, consumer_send, core_ack_recv):
        endpoint.close()
    core_records, order, updates = [], [], []
    lock = threading.Lock()

    def mark(phase, **values):
        row = {**component.stamp(), "role": "core", "phase": phase, **values}
        with lock:
            core_records.append(row)
            with (directory / "core.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        return row

    model_entered, model_release = threading.Event(), threading.Event()
    core_thread = threading.get_ident()

    def get_response():
        mark("cpu_model_response_gate_entered")
        model_entered.set()
        if not model_release.wait(args.timeout):
            raise TimeoutError("CPU model gate was not released by a valid core callback")
        order.append("cpu-model-result")
        mark("cpu_model_response_gate_released")
        return "synthetic-final-model-result"

    old_future = runtime.FutureWrapper(deque(), get_response)
    core = runtime.EngineCore.__new__(runtime.EngineCore)
    scheduler_output = SimpleNamespace(total_num_scheduled_tokens=1)
    core.batch_queue = deque([(old_future, scheduler_output, old_future)])
    core.batch_queue_size = 2

    def update(scheduled, value):
        order.append("scheduler-update")
        updates.append({"value": value, "thread_id": threading.get_ident()})
        mark("scheduler_update_from_output", value=value)
        return {0: value}

    core.scheduler = SimpleNamespace(has_requests=lambda: False, update_from_output=update)
    core.model_executor = executor
    core.log_error_detail = core.log_iteration_details = lambda scheduled: nullcontext()
    core._process_aborts_queue = lambda: None
    controller = StageController(((0,), (1,)), max_inflight_batches=1)
    controller.begin_restore(REQUEST, 1)
    controller.admit_batch("event-chain-logical-batch", 1, ((REQUEST, 1),), ((31,),))
    binding = RestoreEventBinding(controller, runtime.KSRestoreEvent, tp_size=1)
    callback_records = []

    def on_event(event):
        if not model_entered.wait(1):
            raise RuntimeError("Actual FutureWrapper response reader did not enter its gate")
        before = {"callback_thread": threading.get_ident(), "core_thread": core_thread,
                  "model_future_done": old_future.done(), "scheduler_updates": len(updates),
                  "waiting_inflight": core.ks_model_wait_inflight(),
                  "waiting_future_is_original": core._ks_waiting_model_future is old_future,
                  "batch_queue_length": len(core.batch_queue),
                  "controller_inflight": controller.inflight_count}
        mark("core_restore_callback_entered", event=asdict(event), **before)
        if (before["callback_thread"] != core_thread or before["model_future_done"]
                or before["scheduler_updates"] != 0 or before["waiting_inflight"] != 1
                or not before["waiting_future_is_original"] or before["batch_queue_length"] != 0
                or before["controller_inflight"] != 1 or callback_records):
            raise RuntimeError("Core consumed event outside the pending-model lifetime contract")
        binding(event)
        ready = controller._restores[REQUEST].ready
        work = controller.take_ready(0)
        if ready != [{0}, set()] or work is None or controller.pop_committable() is not None:
            raise RuntimeError("Actual restore binding did not update only the known component worker")
        callback_records.append({"event": asdict(event), "before": before,
                                 "front_ready": sorted(ready[0]), "back_ready": sorted(ready[1]),
                                 "stage_eligibility": asdict(work)})
        order.append("core-event-consumed")
        core_ack_send.send({"kind": "CORE_CONSUMED", "request_id": REQUEST,
                            "model_future_done": False, "scheduler_updates": 0,
                            "waiting_inflight": 1})
        mark("core_consumed_ack_sent_and_model_gate_release", **before)
        model_release.set()  # Only the validated callback opens the CPU model gate.
        return None

    core_error = None
    core_result = None
    deadline = time.monotonic() + args.timeout + 10
    try:
        core.configure_ks_core_event_pump(on_event, poll_interval=0.002)
        mark("actual_core_step_enter", parent_device_modules=[name for name in (
            "torch", "torch_npu", "lmcache") if name in sys.modules])
        core_result = core.step_with_batch_queue()
        mark("actual_core_step_returned", output=core_result,
             waiting_inflight=core.ks_model_wait_inflight())
    except BaseException as error:
        core_error = {"error": repr(error), "traceback": traceback.format_exc()}
        mark("core_failed", **core_error)
    finally:
        model_release.set()
        reader = getattr(core, "_ks_result_reader", None)
        reader_closed = reader.close(timeout=1) if reader is not None else True

    records = []
    readers = [producer_report, consumer_report]
    while time.monotonic() < deadline:
        for reader in list(readers):
            try:
                while reader.poll():
                    records.append(reader.recv())
            except (EOFError, OSError):
                readers.remove(reader)
        if any(row.get("phase") == "final" and row.get("references_retained") for row in records):
            break
        if not any(child.is_alive() for child in children):
            break
        time.sleep(0.02)
    for child in children:
        child.join(timeout=0.5)
    for reader in readers:
        try:
            while reader.poll():
                records.append(reader.recv())
        except (EOFError, OSError):
            pass
    finals = {row["role"]: row for row in records if row.get("phase") == "final"}
    live = [child.pid for child in children if child.is_alive()]
    parent_device_modules = [name for name in ("torch", "torch_npu", "lmcache") if name in sys.modules]
    valid = (not core_error and not live and len(finals) == 2
             and all(row["status"] == "PASS" for row in finals.values())
             and len(callback_records) == 1 and len(updates) == 1
             and order == ["core-event-consumed", "cpu-model-result", "scheduler-update"]
             and updates[0]["thread_id"] == core_thread and reader_closed
             and core.ks_model_wait_inflight() == 0 and not parent_device_modules)
    status = ("PASS_REAL_EVENT_TO_CORE_COMPONENT_ONLY" if valid else "UNSAFE_TIMEOUT" if live
              else "NOT_SUPPORTED" if any(row["status"] == "NOT_SUPPORTED" for row in finals.values()) else "FAIL")
    result = {**component.stamp(), "status": status, "output_directory": str(directory),
              "finals": finals, "core_error": core_error, "core_result": core_result,
              "callback_records": callback_records, "model_order": order, "scheduler_updates": updates,
              "parent_device_modules": parent_device_modules, "model_result_reader_closed": reader_closed,
              "waiting_inflight_after_step": core.ks_model_wait_inflight(),
              "retained_live_pids": live, "forced_termination_performed": False,
              "children": [{"name": child.name, "pid": child.pid, "exitcode": child.exitcode} for child in children],
              "source_map": json.loads((Path(__file__).parent / "chain_source_map.json").read_text(encoding="utf-8")),
              "scope": "real event to core AST component only; no model/real KV/TP8/performance proof"}
    (directory / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False), flush=True)
    if live:
        os._exit(4)  # Preserve child-owned original references; supervisor records unsafe failure.
    executor._close_stage_event_channels()
    return 0 if valid else 3 if status == "NOT_SUPPORTED" else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--cpu-plan", action="store_true")
    modes.add_argument("--run-device-probe", action="store_true")
    parser.add_argument("--device", type=int, required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    output = PurePosixPath(args.output_dir)
    root = PurePosixPath("/opt/ks-stage-ready/device-probe")
    if (args.device < 0 or not 10 <= args.timeout <= 180 or ".." in output.parts
            or not output.is_relative_to(root) or output == root):
        parser.error("Explicit nonnegative device, timeout 10..180 and private output subdirectory required")
    if args.cpu_plan:
        print(json.dumps(cpu_plan(args), indent=2), flush=True)
        return 0
    if os.name != "posix" or not Path(args.output_dir).resolve().is_relative_to(ROOT.resolve()):
        parser.error("Real component probe must run inside the private Linux container")
    return run_probe(args)


if __name__ == "__main__":
    raise SystemExit(main())
