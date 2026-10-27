"""Real Ascend event IPC + installed LMCache futures + single-owner observer.

Only --run-device-probe imports device libraries. --cpu-plan is standard-library
only and does not create processes, import torch/lmcache, or access a device.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import importlib.metadata
import importlib.util
import inspect
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path, PurePosixPath
import sys
import threading
import time
import traceback


OUTPUT_ROOT = Path("/opt/ks-stage-ready/device-probe")
IDENTITY = dict(request_id="event-ipc-component-probe", restore_generation=1,
                stage_id=0, tp_rank=0, pcp_rank=0, global_worker_rank=0)


class NotSupported(RuntimeError):
    pass


def stamp():
    return {"utc": datetime.now(timezone.utc).isoformat(),
            "monotonic": time.monotonic(), "pid": os.getpid(),
            "thread_id": threading.get_ident()}


def status_sender(role, connection, directory):
    lock = threading.Lock()

    def send(phase, **details):
        record = {**stamp(), "role": role, "phase": phase, **details}
        with lock:
            with (Path(directory) / (role + ".jsonl")).open(
                    "a", encoding="utf-8") as output:
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
            try:
                connection.send(record)
            except (BrokenPipeError, EOFError, OSError):
                pass
        return record

    return send


def version(distribution):
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def load_device(device_index, send):
    # Keep these imports out of module scope and out of the CPU-plan path.
    import torch
    import torch_npu
    import lmcache
    from lmcache.v1.multiprocess.futures import (
        DeviceMessagingFuture, MessagingFuture,
    )
    from lmcache.v1.platform.base.event_ipc import get_event_ipc_backend

    # This installed backend's import-failure warning includes a handle prefix.
    # Do not emit raw IPC handle material in this diagnostic's console/logs.
    ipc_logger = "lmcache.v1.platform.devices.npu.event_ipc"
    logging.getLogger(ipc_logger).disabled = True
    device = torch.device("npu", device_index)
    backend = get_event_ipc_backend(device)
    if backend.device_type != "npu":
        raise RuntimeError("Refusing a non-NPU event backend")
    send("dependencies", python=sys.version, torch=str(torch.__version__),
         torch_npu=str(getattr(torch_npu, "__version__", "unknown")),
         lmcache=version("lmcache"), lmcache_ascend=version("lmcache-ascend"),
         lmcache_file=getattr(lmcache, "__file__", None),
         backend_class=type(backend).__module__ + "." + type(backend).__name__,
         backend_source=inspect.getfile(type(backend)),
         future_source=inspect.getfile(DeviceMessagingFuture),
         future_signature=str(inspect.signature(DeviceMessagingFuture)),
         handle_prefix_logger_suppressed=ipc_logger,
         device=str(device))
    try:
        backend.check_event_support(device)
    except Exception as error:
        raise NotSupported(str(error)) from error
    send("check_event_support_returned", supported=True)
    # Warm the thread's real device context before observer.start()'s 1s guard.
    torch.npu.set_device(device)
    return torch, device, backend, MessagingFuture, DeviceMessagingFuture


def receive(connection, deadline, description):
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not connection.poll(remaining):
        raise TimeoutError(description)
    return connection.recv()


def send_abort(connection, status, error):
    try:
        connection.send({"kind": "ABORT_BEFORE_EVENT", "status": status,
                         "error": str(error)})
    except (BrokenPipeError, EOFError, OSError):
        pass


def retain_until_supervisor(keepalive):
    # An import/query/synchronize failure does not prove device quiescence.
    # Keep the original objects and the exporting process alive. The parent
    # records their PIDs and exits without terminating these processes.
    while True:
        threading.Event().wait(1)
        if not keepalive:
            raise RuntimeError("Unsafe retention lost its resource references")


def producer(device_index, timeout, connection, report, directory):
    send = status_sender("producer", report, directory)
    keepalive = [connection]
    submitted = False
    safe_ack = False
    deadline = time.monotonic() + timeout
    try:
        torch, device, backend, _, _ = load_device(device_index, send)
        keepalive.extend([torch, device, backend])
        ready = receive(connection, deadline, "consumer READY timed out")
        if isinstance(ready, dict) and ready.get("kind") == "ABORT_BEFORE_EVENT":
            send("final", status="PEER_ABORT", safe_device_completion=True,
                 peer=ready, submitted_device_work=False)
            return
        if ready != {"kind": "READY"}:
            raise RuntimeError("Invalid consumer READY")
        with torch.npu.device(device):
            event = backend.create_event(device)
            stream = torch.npu.Stream(device=device)
            keepalive.extend([event, stream])
            # Set before the first tensor operation: a raising call can enqueue.
            submitted = True
            with torch.npu.stream(stream):
                tensor = torch.arange(64, dtype=torch.float32, device=device)
                tensor.add_(3)
                backend.record_event(event, stream)
            keepalive.append(tensor)
            recorded_at = stamp()
            handle = backend.export_event(event, device)
            if type(handle) is not bytes or not handle:
                raise RuntimeError("Refusing empty or non-bytes IPC handle")
            keepalive.append(handle)
            send("handle_export_method_returned", handle_length=len(handle),
                 event_record_method_returned=recorded_at,
                 device_completion_not_inferred_from_export=True)
            # This CPU Pipe transports the exact installed future payload.
            connection.send((handle, True))
            send("cpu_pipe_send_returned", handle_length=len(handle),
                 source_event_retained=True)
            ack = receive(connection, deadline, "consumer safe ACK timed out")
            if not isinstance(ack, dict) or ack.get("kind") != "ACK":
                raise RuntimeError("Consumer did not return a safe ACK")
            safe_ack = ack.get("safe_device_completion") is True
            if not safe_ack:
                raise RuntimeError("Consumer ACK did not prove device completion")
            send("consumer_safe_ack_received", ack=ack)
            # Consumer's real result already synchronized the imported event.
            # The producer checks its original event and the small work as well.
            own_event_complete = bool(backend.query_event(event))
            if not own_event_complete:
                safe_ack = False
                raise RuntimeError("Producer event was not complete after safe ACK")
            values = tensor.cpu().tolist()
            device_check = values == [float(i + 3) for i in range(64)]
            if not device_check:
                raise RuntimeError("Producer tensor content validation failed")
            send("final", status="PASS" if ack.get("validation_ok") else "FAIL",
                 safe_device_completion=True, producer_event_query=True,
                 source_retained_until_safe_ack=True, tensor_check=True,
                 tensor_elements=len(values), tensor_first=values[0],
                 tensor_last=values[-1], tensor_sum=sum(values),
                 consumer_validation_ok=ack.get("validation_ok"))
    except NotSupported as error:
        send_abort(connection, "NOT_SUPPORTED", error)
        send("final", status="NOT_SUPPORTED", error=str(error),
             safe_device_completion=True, submitted_device_work=submitted)
    except BaseException as error:
        unsafe = submitted and not safe_ack
        if not submitted:
            send_abort(connection, "FAIL", error)
        send("final", status=("UNSAFE_TIMEOUT" if isinstance(error, TimeoutError)
                              and unsafe else "FAIL_UNSAFE" if unsafe else "FAIL"),
             error=repr(error), traceback=traceback.format_exc(),
             safe_device_completion=not unsafe, submitted_device_work=submitted,
             references_retained=unsafe)
        if unsafe:
            retain_until_supervisor(keepalive)


def load_observer(path):
    module_name = "event_ipc_probe_observer"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load observer from explicit source path")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def owner_profiler(backend, future_class):
    names = {}
    for name, method in (
        ("backend.import_event", backend.import_event),
        ("backend.query_event", backend.query_event),
        ("backend.synchronize_event", backend.synchronize_event),
        ("real.query", future_class.query),
        ("real.result", future_class.result),
    ):
        code = getattr(method, "__code__", None)
        if code is None:
            raise RuntimeError("Cannot audit real method ownership: " + name)
        names[code] = name
    audit = {"owner": None, "methods": {}}

    def profile(frame, event, value):
        name = names.get(frame.f_code)
        if name is None or event not in ("call", "return"):
            return
        item = audit["methods"].setdefault(name, {"calls": 0, "returns": 0,
                                                 "thread_ids": []})
        thread_id = threading.get_ident()
        if thread_id not in item["thread_ids"]:
            item["thread_ids"].append(thread_id)
        if event == "call":
            item["calls"] += 1
            item.setdefault("first_call", stamp())
        else:
            item["returns"] += 1
            item["last_return"] = stamp()
            if type(value) is bool:
                item["last_bool"] = value
                if value:
                    item.setdefault("first_true_return", stamp())
                else:
                    item["false_returns"] = item.get("false_returns", 0) + 1

    return audit, profile


def consumer(device_index, timeout, observer_path, connection, report, directory):
    send = status_sender("consumer", report, directory)
    keepalive = [connection]
    observer = None
    received_event = False
    safe = False
    ack_sent = False
    audit = None
    deadline = time.monotonic() + timeout
    try:
        torch, device, backend, messaging_class, future_class = load_device(
            device_index, send)
        module = load_observer(observer_path)
        audit, profile = owner_profiler(backend, future_class)

        @contextmanager
        def owner_context():
            with torch.npu.device(device):
                audit["owner"] = {**stamp(), "device": str(device),
                                  "current_device": int(torch.npu.current_device())}
                previous = sys.getprofile()
                sys.setprofile(profile)
                try:
                    yield
                finally:
                    sys.setprofile(previous)

        observer = module.LMCacheStageObserver(
            proxy_factory=messaging_class, device_context_factory=owner_context)
        keepalive.extend([torch, device, backend, module, observer, audit])
        observer.start()
        send("owner_started", observer_source=str(observer_path), owner=audit["owner"])
        connection.send({"kind": "READY"})
        payload = receive(connection, deadline, "producer handle timed out")
        if isinstance(payload, dict) and payload.get("kind") == "ABORT_BEFORE_EVENT":
            safe = observer.close(timeout=1)
            send("final", status="PEER_ABORT", safe_device_completion=safe,
                 peer=payload, imported_event=False, owner_audit=audit)
            return
        if (type(payload) is not tuple or len(payload) != 2
                or type(payload[0]) is not bytes or not payload[0]
                or payload[1] is not True):
            raise RuntimeError("Refusing invalid/empty event payload")
        received_event = True
        handle, _ = payload
        send("cpu_pipe_received", handle_length=len(handle),
             device_completion_not_inferred_from_cpu_payload=True)
        raw = messaging_class()
        real = future_class(raw, device, backend)
        raw.retain_reference(backend)
        real.retain_reference(backend)
        identity = module.StageRestoreIdentity(**IDENTITY)
        operation = observer.reserve(identity, ("event-ipc-component", device_index, 64),
                                     producer_event=None)
        proxy = observer.attach(operation, real)
        keepalive.extend([payload, raw, real, identity, operation, proxy])
        raw.set_result(payload)
        send("raw_messaging_future_set_result_returned", raw_done=raw.query(),
             proxy_type=type(proxy).__module__ + "." + type(proxy).__name__,
             real_type=type(real).__module__ + "." + type(real).__name__,
             proxy_is_original_messaging_future=type(proxy) is messaging_class,
             real_is_original_device_messaging_future=type(real) is future_class)
        if not proxy.wait(max(0, deadline - time.monotonic())):
            raise TimeoutError("CPU proxy did not finish within the probe deadline")
        proxy_value = proxy.result(timeout=0)  # CPU-only original MessagingFuture.
        safe = True  # Observer sets proxy only after real.query + real.result.
        first_success = observer.drain_success()
        time.sleep(0.02)
        second_success = observer.drain_success()
        closed_safe = observer.close(timeout=1)
        if not closed_safe:
            safe = False
            raise RuntimeError("Observer did not confirm safe shutdown")
        methods = audit["methods"]
        owner = audit["owner"]["thread_id"]
        required = ("backend.import_event", "backend.query_event",
                    "backend.synchronize_event", "real.query", "real.result")
        owner_ok = all(methods.get(name, {}).get("thread_ids") == [owner]
                       and methods[name]["calls"] >= 1 for name in required)
        success_once = first_success == [identity] and second_success == []
        query_true = methods.get("backend.query_event", {}).get("last_bool") is True
        real_true = methods.get("real.result", {}).get("last_bool") is True
        validation_ok = (proxy_value is True and success_once and owner_ok
                         and query_true and real_true and type(proxy) is messaging_class
                         and type(real) is future_class)
        ack = {"kind": "ACK", "safe_device_completion": True,
               "validation_ok": validation_ok, "proxy_result": proxy_value,
               "success_identity": asdict(identity), "success_count": len(first_success),
               "duplicate_success_count": len(second_success)}
        connection.send(ack)
        ack_sent = True
        send("final", status="PASS" if validation_ok else "FAIL",
             safe_device_completion=True, proxy_result=proxy_value,
             success_events=[asdict(value) for value in first_success],
             duplicate_success_events=[asdict(value) for value in second_success],
             owner_only_import_query_result=owner_ok, owner_audit=audit,
             observed_pending_query=(methods.get("backend.query_event", {})
                                     .get("false_returns", 0) > 0),
             observer_closed_safe=True, safe_ack_sent=True)
    except NotSupported as error:
        send_abort(connection, "NOT_SUPPORTED", error)
        send("final", status="NOT_SUPPORTED", error=str(error),
             safe_device_completion=True, imported_event=False)
    except BaseException as error:
        # A CPU error before receiving a handle has no remote import to retain.
        unsafe = received_event and not safe
        if not received_event:
            send_abort(connection, "FAIL", error)
            if observer is not None:
                observer.close(timeout=1)
        elif safe and not ack_sent:
            connection.send({"kind": "ACK", "safe_device_completion": True,
                             "validation_ok": False, "error": repr(error)})
        send("final", status=("UNSAFE_TIMEOUT" if isinstance(error, TimeoutError)
                              and unsafe else "FAIL_UNSAFE" if unsafe else "FAIL"),
             error=repr(error), traceback=traceback.format_exc(),
             safe_device_completion=not unsafe, references_retained=unsafe,
             owner_audit=audit)
        if unsafe:
            retain_until_supervisor(keepalive)


def cpu_plan(args):
    return {
        "status": "CPU_PLAN_ONLY", "device_imports_performed": False,
        "device_allocation_performed": False, "spawn_performed": False,
        "planned_device": "npu:" + str(args.device), "start_method": "spawn",
        "children": ["producer", "consumer"], "timeout_seconds": args.timeout,
        "observer_source": args.observer_path, "fixed_output_root": OUTPUT_ROOT.as_posix(),
        "requested_output_directory": args.output_dir,
        "payload": "nonempty bytes event handle + True over CPU Pipe",
        "real_components": ["NpuEventIPCBackend", "MessagingFuture",
                            "DeviceMessagingFuture", "LMCacheStageObserver"],
        "synthetic_component_identity": IDENTITY,
        "evidence_scope": "single-NPU cross-process event/future/observer component only",
        "excluded": ["GLM", "LMCache KV restore", "MPserver", "PP2 integration",
                     "performance proof"],
        "unsupported_policy": "NOT_SUPPORTED when real check_event_support raises",
        "unsafe_timeout_policy": "record PIDs; retain child references; parent exits without killing",
    }


def run_device_probe(args):
    run_id = datetime.now(timezone.utc).strftime("event-ipc-%Y%m%dT%H%M%S")
    directory = (Path(args.output_dir) if args.output_dir else
                 OUTPUT_ROOT / (run_id + "-" + str(os.getpid())))
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "plan.json").write_text(json.dumps(cpu_plan(args), indent=2),
                                         encoding="utf-8")
    context = mp.get_context("spawn")
    producer_wire, consumer_wire = context.Pipe(duplex=True)
    producer_report, producer_send = context.Pipe(duplex=False)
    consumer_report, consumer_send = context.Pipe(duplex=False)
    processes = [
        context.Process(name="event-ipc-producer", target=producer,
                        args=(args.device, args.timeout, producer_wire,
                              producer_send, str(directory))),
        context.Process(name="event-ipc-consumer", target=consumer,
                        args=(args.device, args.timeout, args.observer_path,
                              consumer_wire, consumer_send, str(directory))),
    ]
    for process in processes:
        process.start()
    for endpoint in (producer_wire, consumer_wire, producer_send, consumer_send):
        endpoint.close()
    records = []
    readers = [producer_report, consumer_report]
    deadline = time.monotonic() + args.timeout + 10
    while time.monotonic() < deadline:
        for reader in list(readers):
            try:
                while reader.poll():
                    records.append(reader.recv())
            except (EOFError, OSError):
                readers.remove(reader)
        unsafe_seen = any(record.get("phase") == "final" and
                          record.get("safe_device_completion") is False
                          for record in records)
        if unsafe_seen or not any(process.is_alive() for process in processes):
            break
        time.sleep(0.02)
    for process in processes:
        process.join(timeout=0.5)
    for reader in list(readers):
        try:
            while reader.poll():
                records.append(reader.recv())
        except (EOFError, OSError):
            pass
    finals = {record["role"]: record for record in records
              if record.get("phase") == "final"}
    live_pids = [process.pid for process in processes if process.is_alive()]
    statuses = [record["status"] for record in finals.values()]
    if live_pids:
        verdict, code = "UNSAFE_TIMEOUT", 4
    elif len(finals) == 2 and all(value == "PASS" for value in statuses):
        verdict, code = "PASS_COMPONENT_ONLY", 0
    elif "NOT_SUPPORTED" in statuses:
        verdict, code = "NOT_SUPPORTED", 3
    else:
        verdict, code = "FAIL", 2
    summary = {**stamp(), "status": verdict, "output_directory": str(directory),
               "device": "npu:" + str(args.device), "finals": finals,
               "children": [{"name": process.name, "pid": process.pid,
                             "exitcode": process.exitcode} for process in processes],
               "retained_live_pids": live_pids, "forced_termination_performed": False,
               "safe_device_completion": not live_pids and len(finals) == 2 and all(
                   record.get("safe_device_completion") is True for record in finals.values()),
               "evidence_scope": "event IPC/future/owner-thread component only; no PP2/GLM/KV/performance proof"}
    (directory / "result.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    if live_pids:
        # Avoid multiprocessing atexit's unbounded child join. The children
        # deliberately stay alive with original event/backend/future references.
        # Supervisor must handle these recorded PIDs as an unsafe failed probe.
        os._exit(code)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--cpu-plan", action="store_true")
    modes.add_argument("--run-device-probe", action="store_true")
    parser.add_argument("--device", type=int, required=True,
                        help="Explicit single currently-free logical NPU index")
    parser.add_argument("--observer-path", default=str(
        Path(__file__).with_name("stage_observer.py")))
    parser.add_argument("--output-dir", help=(
        "New private output directory below /opt/ks-stage-ready/device-probe"))
    parser.add_argument("--timeout", type=float, default=90,
                        help="Protocol timeout seconds, 10..180; parent adds 10s")
    args = parser.parse_args()
    if args.device < 0 or not 10 <= args.timeout <= 180:
        parser.error("device must be nonnegative and timeout must be 10..180")
    if args.output_dir:
        output = PurePosixPath(args.output_dir)
        root = PurePosixPath("/opt/ks-stage-ready/device-probe")
        if ".." in output.parts or not output.is_relative_to(root) or output == root:
            parser.error("output-dir must be a new directory below " + str(root))
    if args.cpu_plan:
        print(json.dumps(cpu_plan(args), indent=2), flush=True)
        return 0
    if os.name != "posix":
        parser.error("Device probe only runs in the private Linux container")
    if not Path(args.observer_path).is_file():
        parser.error("Explicit observer source file does not exist")
    return run_device_probe(args)


if __name__ == "__main__":
    raise SystemExit(main())
