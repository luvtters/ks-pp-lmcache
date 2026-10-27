"""Real vLLM SHM reply blocking versus independent core event pumping; CPU only."""
from __future__ import annotations

import argparse
from collections import deque
from contextlib import nullcontext
from importlib import metadata
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import threading
import time
from types import SimpleNamespace

import real_reply_ast as actual

HERE = Path(__file__).resolve().parent


def prepare_bundle():
    root = HERE.parent
    paths = {
        "multiproc_executor.py": root / "restore_event_binding/multiproc_executor.py",
        "core.py": root / "core_event_pump/core.py",
        "stage_controller.py": root / "controller/stage_controller.py",
        "restore_binding.py": root / "restore_event_binding/restore_binding.py",
    }
    target = HERE / "source_bundle"
    target.mkdir(exist_ok=True)
    for name, path in paths.items():
        (target / name).write_bytes(path.read_text(encoding="utf-8").encode("utf-8"))
    report = {"status": "PREPARED_NOT_EXECUTED", "sources": {name: str(path) for name, path in paths.items()}}
    (HERE / "source_manifest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def run(args, report):
    # This guard precedes importing torch/vLLM. No bypass flag is provided.
    if not Path("/.dockerenv").exists() or sys.platform != "linux":
        raise RuntimeError("Run only inside the project's Linux CPU container")
    devices = [str(path) for path in Path("/dev").glob("davinci*")]
    if devices:
        raise RuntimeError(f"Refusing a container with NPU device nodes: {devices}")
    report["device_nodes"] = devices
    args.output_dir.mkdir(parents=True, exist_ok=True)
    ipc_root = args.output_dir / "ipc"
    ipc_root.mkdir(exist_ok=True)
    os.environ["VLLM_RPC_BASE_PATH"] = str(ipc_root.resolve())
    os.environ["KS_PP_STAGE_EVENTS"] = "1"
    os.environ["KS_CORE_STAGE_EVENT_PUMP"] = "1"

    import torch.distributed as dist
    from vllm.distributed.device_communicators import shm_broadcast
    MessageQueue = shm_broadcast.MessageQueue  # The installed class; never extracted/reimplemented.
    if dist.is_initialized():
        raise RuntimeError("A pre-existing distributed device/process group is not allowed")
    version = metadata.version("vllm")
    if version != "0.23.0+empty":
        raise RuntimeError(f"Unexpected installed vLLM version: {version}")
    report["vllm"] = version
    report["actual_message_queue"] = inspect.getfile(MessageQueue)
    report["dequeue_signature"] = str(inspect.signature(MessageQueue.dequeue))
    report["spinloop_extension_enabled"] = bool(shm_broadcast.SPINLOOP_EXT_ENABLED)
    controller_module, binding_module = actual.load_candidates(args.source_root)
    controller = controller_module.StageController((range(8), range(8, 16)))
    restore_binding = binding_module.RestoreEventBinding(controller, actual.KSRestoreEvent)
    request_id = "cpu-real-reply-queue-request"
    controller.begin_restore(request_id, 1)

    context = multiprocessing.get_context("spawn")
    executor = actual.MultiprocExecutor.__new__(actual.MultiprocExecutor)
    executor.parallel_config = SimpleNamespace(nnodes_within_dp=1)
    executor.local_world_size = executor.world_size = 16
    executor._init_stage_event_channels(context, 8, 2, 1)
    publisher = actual._KSStageEventPublisher(*executor._ks_stage_channels[0],
                                               0, 0, tp_size=8, pcp_size=1)
    handle_pipe = queue.Queue(maxsize=1)  # Only transports the real MQ's handle.
    permit_model_write, reader_entered = threading.Event(), threading.Event()
    reader_done, writer_wrote = threading.Event(), threading.Event()
    errors, trace, queue_objects = [], [], {}
    updates = []
    bootstrap = {"phase": "ordinary-bootstrap-reply"}
    tail = {"phase": "ordinary-after-model-reply"}
    # Deliberately exceed max_chunk_bytes to exercise real ZMQ overflow receive.
    reply = {"source": "real_vllm_MessageQueue", "payload": "x" * 16384}

    def record(action):
        trace.append({"action": action, "thread": threading.get_ident(), "ns": time.monotonic_ns()})

    def writer():
        try:
            # Creation, handshake and writes stay on this socket's owning thread.
            mq = MessageQueue(n_reader=1, n_local_reader=1, max_chunk_bytes=4096, max_chunks=2)
            queue_objects["writer"] = mq
            handle_pipe.put(mq.export_handle())
            mq.wait_until_ready()
            record("writer_handshake_complete")
            mq.enqueue(bootstrap, timeout=args.timeout)
            if not permit_model_write.wait(args.timeout):
                raise TimeoutError("core did not authorize the model reply")
            record("model_reply_write_begin")
            mq.enqueue(reply, timeout=args.timeout)
            writer_wrote.set()
            record("model_reply_written")
            if not reader_done.wait(args.timeout):
                raise TimeoutError("real reply reader did not finish")
            mq.enqueue(tail, timeout=args.timeout)
        except BaseException as error:
            errors.append({"where": "writer", "error": repr(error)})

    def get_actual_reply():
        # The SAME MQ was created/handshaken/read by main before this transfer.
        # Socket migration is the behavior under test, not a supported-API claim.
        mq = queue_objects["reader"]
        assert threading.get_ident() != core_thread
        record("actual_dequeue_begin")
        reader_entered.set()
        value = mq.dequeue(timeout=args.timeout, indefinite=False)
        record("actual_dequeue_return")
        reader_done.set()
        if value != reply:
            raise AssertionError("real MessageQueue returned an unexpected model reply")
        return value

    old_future = actual.FutureWrapper(deque(), get_actual_reply)
    core = actual.EngineCore.__new__(actual.EngineCore)
    scheduled = SimpleNamespace(total_num_scheduled_tokens=1)
    core.batch_queue, core.batch_queue_size = deque([(old_future, scheduled, old_future)]), 2
    core.scheduler = SimpleNamespace(has_requests=lambda: False,
        update_from_output=lambda sched, result: updates.append(result) or {0: result})
    core.model_executor = executor
    core.log_error_detail = core.log_iteration_details = lambda sched: nullcontext()
    core._process_aborts_queue = lambda: None

    def on_restore(event):
        restore_binding(event)
        assert reader_entered.is_set() and not reader_done.is_set()
        assert not old_future.done() and not writer_wrote.is_set()
        assert not permit_model_write.is_set() and updates == []
        assert core.ks_model_wait_inflight() == 1
        assert core._ks_waiting_model_future is old_future
        assert controller._restores[request_id].ready == [{0}, set()]
        report["before_model_reply"] = {"core_registered_restore": True,
            "actual_dequeue_entered": True, "model_future_done": False,
            "scheduler_update_count": 0, "model_wait_inflight": 1,
            "callback_thread_is_core": threading.get_ident() == core_thread}
        assert report["before_model_reply"]["callback_thread_is_core"]
        record("core_registered_restore_before_model_reply")
        permit_model_write.set()  # Only now can the real reply enter MessageQueue.

    def notify_restore():
        try:
            if not reader_entered.wait(args.timeout):
                raise TimeoutError("real reply reader never entered dequeue")
            # Cover the actual queue's spin period and an idle socket wait too.
            # This imposed silence is not a measured model/performance latency.
            time.sleep(report["empty_model_reply_hold_seconds"])
            identity = SimpleNamespace(request_id=request_id, restore_generation=1,
                stage_id=0, global_worker_rank=0, tp_rank=0, pcp_rank=0,
                validate=lambda: None)
            record("independent_cpu_restore_notification_published")
            publisher.publish_restore_ready(identity)
        except BaseException as error:
            errors.append({"where": "notification", "error": repr(error)})

    core_thread = threading.get_ident()
    core.configure_ks_core_event_pump(on_restore)
    writing = threading.Thread(target=writer, name="ActualReplyMQWriter", daemon=True)
    notifying = threading.Thread(target=notify_restore, name="CPURestoreNotifier", daemon=True)
    try:
        writing.start()
        # Production-shaped seam: init/ordinary RPC reply consumed on core thread.
        mq = MessageQueue.create_from_handle(handle_pipe.get(timeout=args.timeout), rank=0)
        queue_objects["reader"] = mq
        mq.wait_until_ready()
        record("main_reader_handshake_complete")
        assert mq.dequeue(timeout=args.timeout, indefinite=False) == bootstrap
        record("main_bootstrap_reply_consumed")
        report["bootstrap_reader_thread"] = threading.get_ident()
        report["reader_object_id"] = id(mq)
        report["model_reply_payload_bytes"] = len(reply["payload"])
        report["max_chunk_bytes"] = 4096
        report["mq_busy_loop_seconds"] = float(mq._spin_condition.busy_loop_s)
        report["empty_model_reply_hold_seconds"] = max(.15, report["mq_busy_loop_seconds"] + .25)
        if args.timeout <= report["empty_model_reply_hold_seconds"] + 1:
            raise ValueError("timeout is too short for the real queue spin/idle check")
        notifying.start()
        result = core.step_with_batch_queue()
        writing.join(args.timeout)
        notifying.join(args.timeout)
        if errors:
            raise AssertionError(errors)
        assert not writing.is_alive() and not notifying.is_alive()
        assert report.get("before_model_reply", {}).get("core_registered_restore")
        assert old_future.done() and updates == [reply]
        assert result == ({0: reply}, False)
        assert core.ks_model_wait_inflight() == 0
        # get_response returned before old_future became terminal: no concurrent
        # MQ reader remains when main takes back the same object for this check.
        assert queue_objects["reader"] is mq
        assert mq.dequeue(timeout=args.timeout, indefinite=False) == tail
        record("main_tail_reply_consumed_same_reader")
        assert not dist.is_initialized()
        report.update(status="PASS_ACTUAL_REPLY_QUEUE_CPU_ONLY", final_update_count=len(updates),
            final_future_done=old_future.done(), process_group_initialized=False,
            actual_reader_thread=core._ks_result_reader.thread.ident,
            same_reader_main_to_worker_to_main=True,
            thread_migration_supported_in_general="NOT_VERIFIED")
    finally:
        permit_model_write.set()
        if notifying.ident is not None:
            notifying.join(args.timeout)
        writing.join(args.timeout)
        report["trace"] = trace
        report["background_errors"] = errors
        publisher.close()
        executor._close_stage_event_channels()
        if old_future.done():
            core._ks_close_core_event_pump()
        for mq in queue_objects.values():
            mq.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--source-root", type=Path, default=HERE / "source_bundle")
    parser.add_argument("--output-dir", type=Path, default=HERE / "runtime_result")
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    if args.prepare_only:
        prepare_bundle()
        return 0
    report = {"status": "NOT_RUN", "scope": "actual_reply_queue_CPU_component_only",
              "not_model_or_performance_test": True, "source_root": str(args.source_root)}
    try:
        run(args, report)
        code = 0
    except BaseException as error:
        report.update(status="FAIL_OR_DEPENDENCY_BLOCKED", error=repr(error))
        code = 1
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
