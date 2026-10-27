"""Synthetic TP8/PP2 stage dependency probe; no model or real LMCache KV.

Only --run-device-probe imports torch, torch_npu, or vLLM. CPU-plan performs
no framework import, device allocation, or distributed initialization.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import os
from pathlib import Path, PurePosixPath
import time
import traceback
from types import SimpleNamespace


TP, PP, WORLD = 8, 2, 16
SHAPE = (8, 32)
REQUEST, GENERATION, BATCH = "stage-device-component", 1, "stage-batch-0001"
ROOT = Path("/opt/ks-stage-ready/device-probe")


def plan():
    return {
        "scope": "SYNTHETIC_STAGE_DEPENDENCY_DEVICE_PROTOCOL_ONLY",
        "world_size": WORLD, "tp": TP, "pp": PP, "shape": SHAPE,
        "device_backend": "hccl", "control_backend": "existing world.cpu_group Gloo",
        "core": "rank0 original CPU StageController prototype; not vLLM EngineCore",
        "legacy_baseline": "unchanged AST-extracted scheduler predicate lines 2188-2219",
        "controlled_latch": "back-stage H2D is not submitted until rank0 release",
        "rounds": ["front actual H2D+event completion; back not submitted",
                   "legacy predicate rejects while StageController dispatches front",
                   "front actual compute event completion",
                   "real HCCL PP isend/irecv and back TP postprocess completion",
                   "activation-only does not dispatch back; explicit latch release",
                   "back actual H2D+event completion; global legacy gate now opens",
                   "back actual compute, contents, all-rank checks, ordered commit"],
        "group_timeout_seconds": 90, "required_external_task_timeout_seconds": 240,
        "background_device_threads": False,
        "not_verified": ["vLLM core integration", "GLM", "real LMCache KV",
                         "performance or speedup; artificial latch has no timing value"],
    }


def legacy_fixture():
    # Execute the original function body, not a hand-written equivalent gate.
    directory = Path(__file__).parent
    source = (directory / "legacy_scheduler_gate_excerpt.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise RuntimeError("Expected exactly the original promotion function")

    class RequestStatus(Enum):
        WAITING_FOR_REMOTE_KVS = 1
        WAITING = 2
        PREEMPTED = 3
        WAITING_FOR_STRUCTURED_OUTPUT_GRAMMAR = 4
        WAITING_FOR_STREAMING_REQ = 5

    namespace = {"RequestStatus": RequestStatus, "Request": object}
    exec(compile(tree, "legacy_scheduler_gate_excerpt.py", "exec"), namespace)
    updates = []
    scheduler = SimpleNamespace(finished_recving_kv_req_ids=set(),
                                _update_waiting_for_remote_kv=lambda request:
                                updates.append(request.request_id))
    request = SimpleNamespace(status=RequestStatus.WAITING_FOR_REMOTE_KVS,
                              request_id=REQUEST, num_preemptions=0)
    return (namespace[tree.body[0].name], scheduler, request, updates,
            json.loads((directory / "legacy_gate_source.json").read_text(encoding="utf-8")))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--cpu-plan", action="store_true")
    mode.add_argument("--run-device-probe", action="store_true")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output_posix = PurePosixPath(args.output_dir)
    root_posix = PurePosixPath("/opt/ks-stage-ready/device-probe")
    if (".." in output_posix.parts or not output_posix.is_relative_to(root_posix)
            or output_posix == root_posix):
        parser.error("Output must be a private subdirectory below " + str(root_posix))
    if args.cpu_plan:
        print(json.dumps(plan(), indent=2), flush=True)
        return 0

    rank = int(os.environ.get("RANK", "-1"))
    local_rank = int(os.environ.get("LOCAL_RANK", "-1"))
    if int(os.environ.get("WORLD_SIZE", "-1")) != WORLD or local_rank != rank or not 0 <= rank < WORLD:
        raise SystemExit("Requires one-node torchrun, 16 local ranks, all 16 NPUs")
    output_dir = Path(args.output_dir).resolve()
    if not output_dir.is_relative_to(ROOT.resolve()):
        raise SystemExit("Resolved output escaped the fixed private root")
    output_dir.mkdir(parents=True, exist_ok=True)
    events = []
    log = (output_dir / f"rank-{rank:02d}.events.jsonl").open("a", encoding="utf-8", buffering=1)
    result = {"rank": rank, "pid": os.getpid(), "plan": plan(), "status": "STARTING",
              "full_core_integration": "NOT_VERIFIED", "safe_device_completion": False}
    success = False

    def mark(action, **values):
        record = {"rank": rank, "pid": os.getpid(), "index": len(events) + 1,
                  "action": action, "monotonic_ns": time.monotonic_ns(),
                  "utc": datetime.now(timezone.utc).isoformat(), **values}
        events.append(record)
        text = json.dumps(record, ensure_ascii=False)
        log.write(text + "\n")
        log.flush()
        print(text, flush=True)
        return record

    try:
        mark("framework_import_enter")
        import torch
        import torch_npu  # noqa: F401
        import torch.distributed as dist
        from vllm.config import ParallelConfig, VllmConfig, set_current_vllm_config
        from vllm.distributed.parallel_state import (
            destroy_distributed_environment, destroy_model_parallel,
            get_pp_group, get_tp_group, get_world_group,
            init_distributed_environment, initialize_model_parallel,
        )
        from stage_controller import StageController
        mark("framework_import_returned", torch=str(torch.__version__),
             torch_npu=str(getattr(torch_npu, "__version__", "unknown")))
        if not dist.is_backend_available("hccl"):
            raise RuntimeError("Real HCCL required; no device backend fallback")
        device = torch.device("npu", local_rank)
        torch.npu.set_device(device)
        torch.set_num_threads(1)
        config = VllmConfig(parallel_config=ParallelConfig(
            tensor_parallel_size=TP, pipeline_parallel_size=PP,
            distributed_executor_backend="mp"))
        with set_current_vllm_config(config):
            mark("distributed_init_enter")
            init_distributed_environment(
                world_size=WORLD, rank=rank, local_rank=local_rank,
                distributed_init_method="env://", backend="hccl",
                timeout=timedelta(seconds=90))
            initialize_model_parallel(tensor_model_parallel_size=TP,
                                      pipeline_model_parallel_size=PP,
                                      prefill_context_model_parallel_size=1,
                                      decode_context_model_parallel_size=1, backend="hccl")
            pp, tp, world = get_pp_group(), get_tp_group(), get_world_group()
            if (pp.world_size != PP or tp.world_size != TP
                    or str(dist.get_backend(world.device_group)).lower() != "hccl"
                    or str(dist.get_backend(world.cpu_group)).lower() != "gloo"
                    or pp.device.type != "npu" or tp.device.type != "npu"):
                raise RuntimeError("Actual groups differ from TP8/PP2 HCCL + Gloo")
            front = pp.is_first_rank
            if front != (rank < TP):
                raise RuntimeError("Unexpected global stage rank mapping")
            result.update(pp_rank=pp.rank_in_group, tp_rank=tp.rank_in_group,
                          pp_ranks=list(pp.ranks), tp_ranks=list(tp.ranks))
            mark("groups_ready", **{key: result[key] for key in (
                "pp_rank", "tp_rank", "pp_ranks", "tp_ranks")})
            round_index = 0

            def exchange(label, **facts):
                nonlocal round_index
                round_index += 1
                record = mark("cpu_control_report", round=round_index, label=label, **facts)
                reports = [None] * WORLD
                dist.all_gather_object(reports, record, group=world.cpu_group)
                if any(item["round"] != round_index or item["label"] != label
                       or item["rank"] != index for index, item in enumerate(reports)):
                    raise RuntimeError("CPU collective rounds or rank identity diverged")
                return reports

            def command(decision):
                value = [decision if rank == 0 else None]
                dist.broadcast_object_list(value, src=0, group=world.cpu_group)
                mark("cpu_core_command", decision=value[0])
                if not value[0].get("ok"):
                    raise RuntimeError("Core refused command: " + str(value[0]))
                return value[0]

            def complete_event(event, name):
                mark(name + "_record_method_returned", event_query=bool(event.query()))
                event.synchronize()
                complete = bool(event.query())
                mark(name + "_device_complete", event_query=complete)
                if not complete:
                    raise RuntimeError("Event synchronize returned without query=True")
                return complete

            # Retain all H2D sources, device buffers, streams, events and payloads
            # through the final all-rank completion barrier.
            resources = []
            kv_submitted = False

            def restore_kv(stage):
                nonlocal kv_submitted
                if kv_submitted:
                    raise RuntimeError("Local synthetic KV was already submitted")
                cpu_kv = torch.full((32,), float(stage + 2), dtype=torch.float32)
                stream = torch.npu.Stream(device=device)
                event = torch.npu.Event()
                kv_submitted = True
                mark("synthetic_kv_h2d_submit", stage=stage, elements=32)
                with torch.npu.stream(stream):
                    device_kv = cpu_kv.to(device=device, non_blocking=True)
                    event.record(stream)
                resources.extend([cpu_kv, device_kv, stream, event])
                complete_event(event, "synthetic_kv_h2d")
                return device_kv

            controller = None
            if rank == 0:
                controller = StageController((tuple(range(TP)), tuple(range(TP, WORLD))))
                controller.begin_restore(REQUEST, GENERATION)
                key = controller.admit_batch(BATCH, GENERATION,
                                             ((REQUEST, GENERATION),), ((31,),))
                promote, legacy, legacy_request, legacy_updates, legacy_source = legacy_fixture()
                result["legacy_source"] = legacy_source
            kv = restore_kv(0) if front else None
            reports = exchange("front_kv_ready_back_latched", kv_submitted=kv_submitted,
                               kv_complete=front, stage=pp.rank_in_group)
            if rank == 0:
                topology_ok = all(item["kv_complete"] is (index < TP)
                                  and item["kv_submitted"] is (index < TP)
                                  for index, item in enumerate(reports))
                for item in reports[:TP]:
                    controller.mark_kv_ready(REQUEST, GENERATION, 0, item["rank"],
                                             device_complete=item["kv_complete"])
                old_ready = promote(legacy, legacy_request)
                work = controller.take_ready(0)
                decision = {"ok": topology_ok and old_ready is False and work is not None,
                            "front_dispatch": work is not None,
                            "legacy_promoted_front_only": old_ready,
                            "legacy_global_finished_ids": sorted(legacy.finished_recving_kv_req_ids),
                            "new_work": asdict(work) if work else None,
                            "back_restore_latch_open": False}
            else:
                decision = None
            first = command(decision)
            result["front_only_dispatch_contrast"] = first
            if not first["front_dispatch"]:
                raise RuntimeError("Front did not dispatch while back restore was withheld")

            source = payload = None
            if front:
                # This arithmetic consumes the actual H2D KV tensor after its event.
                source = torch.arange(256, dtype=torch.float32, device=device).reshape(SHAPE) + kv[0]
                compute_event = torch.npu.Event()
                compute_event.record()
                resources.extend([source, compute_event])
                complete_event(compute_event, "front_compute")
            reports = exchange("front_compute_complete", front_complete=front,
                               back_compute_started=False, back_kv_submitted=(kv_submitted if not front else False))
            if rank == 0:
                for item in reports[:TP]:
                    controller.mark_model_complete(key, 0, item["rank"],
                                                   device_complete=item["front_complete"])
                blocked = controller.take_ready(1) is None
                decision = {"ok": blocked and all(not item["back_kv_submitted"] for item in reports[TP:]),
                            "activation_transfer_authorized": True,
                            "back_compute_blocked": blocked, "back_restore_latch_open": False}
            else:
                decision = None
            command(decision)

            # Reuse the already exercised real PP Work + TP postprocess path.
            postprocess = []
            if front:
                payload = {"hidden_states": source, "batch_id": BATCH,
                           "lane": tp.rank_in_group}
                mark("isend_enter", back_kv_still_not_submitted=True)
                handles = pp.isend_tensor_dict(payload, all_gather_group=tp)
                tensors = None
                resources.append(payload)
                mark("isend_method_returned", handle_count=len(handles))
            else:
                mark("irecv_enter", kv_submitted=kv_submitted, compute_started=False)
                tensors, handles, postprocess = pp.irecv_tensor_dict(all_gather_group=tp)
                resources.append(tensors)
                mark("irecv_method_returned", handle_count=len(handles),
                     postprocess_count=len(postprocess))
                if tensors is None:
                    raise RuntimeError("No activation received")
            resources.extend(handles)
            if not handles:
                raise RuntimeError("Asynchronous Work path was not exercised")
            mark("work_query_before_wait", values=[bool(handle.is_completed()) for handle in handles])
            mark("work_wait_enter")
            for handle in handles:
                handle.wait()
            mark("work_wait_method_returned", values=[bool(handle.is_completed()) for handle in handles])
            comm_event = torch.npu.Event()
            comm_event.record()
            resources.append(comm_event)
            complete_event(comm_event, "communication_fence")
            tp.barrier()
            activation_ready = False
            if not front:
                if not postprocess:
                    raise RuntimeError("Back TP reconstruction path not exercised")
                mark("tp_postprocess_enter")
                for operation in postprocess:
                    operation()
                reconstructed = torch.npu.Event()
                reconstructed.record()
                resources.extend([postprocess, reconstructed])
                complete_event(reconstructed, "activation_reconstructed")
                expected = torch.arange(256, dtype=torch.float32).reshape(SHAPE) + 2
                if (tensors["batch_id"] != BATCH or tensors["lane"] != tp.rank_in_group
                        or not torch.equal(tensors["hidden_states"].cpu(), expected)):
                    raise RuntimeError("Reconstructed activation content/identity mismatch")
                activation_ready = True
            reports = exchange("activation_complete_back_kv_still_latched",
                               activation_complete=activation_ready,
                               back_kv_submitted=(kv_submitted if not front else False),
                               back_compute_started=False,
                               source_retained=(source is payload["hidden_states"] if front else True))
            if rank == 0:
                for item in reports[TP:]:
                    controller.mark_activation_received(key, 1, item["rank"],
                                                        device_complete=item["activation_complete"])
                blocked = controller.take_ready(1) is None
                still_old_blocked = promote(legacy, legacy_request) is False
                decision = {"ok": blocked and still_old_blocked
                            and all(item["activation_complete"] and not item["back_kv_submitted"]
                                    for item in reports[TP:]),
                            "activation_ready_without_back_kv_rejected": blocked,
                            "legacy_still_rejects": still_old_blocked,
                            "back_restore_latch_open": True, "back_compute_authorized": False}
            else:
                decision = None
            release = command(decision)
            result["activation_only_guard"] = release
            if not front:
                if not release["back_restore_latch_open"]:
                    raise RuntimeError("Back restore latch was not explicitly released")
                kv = restore_kv(1)
            reports = exchange("both_stages_actual_kv_complete", kv_complete=True,
                               activation_complete=activation_ready, kv_submitted=kv_submitted)
            if rank == 0:
                all_kv = all(item["kv_complete"] and item["kv_submitted"] for item in reports)
                for item in reports[TP:]:
                    controller.mark_kv_ready(REQUEST, GENERATION, 1, item["rank"],
                                             device_complete=item["kv_complete"])
                if all_kv:
                    legacy.finished_recving_kv_req_ids.add(REQUEST)
                old_now = promote(legacy, legacy_request)
                work = controller.take_ready(1)
                decision = {"ok": all_kv and old_now is True and work is not None
                            and legacy_updates == [REQUEST],
                            "legacy_promoted_only_after_all_kv": old_now,
                            "back_compute_authorized": work is not None,
                            "new_work": asdict(work) if work else None,
                            "legacy_postgate_helper": "recording fixture, not full scheduler KV bookkeeping"}
            else:
                decision = None
            authorized = command(decision)
            result["all_kv_dispatch_contrast"] = authorized
            content_ok = True
            if not front:
                if not authorized["back_compute_authorized"]:
                    raise RuntimeError("Back computation lacks core authorization")
                output = tensors["hidden_states"] + kv[0]
                output_event = torch.npu.Event()
                output_event.record()
                resources.extend([output, output_event])
                complete_event(output_event, "back_compute")
                expected = torch.arange(256, dtype=torch.float32).reshape(SHAPE) + 5
                content_ok = torch.equal(output.cpu(), expected)
            reports = exchange("back_compute_and_content_complete", back_complete=not front,
                               content_ok=content_ok, all_resources_still_retained=True)
            if rank == 0:
                for item in reports[TP:]:
                    controller.mark_model_complete(key, 1, item["rank"],
                                                   device_complete=item["back_complete"])
                every_content_ok = all(item["content_ok"] for item in reports)
                controller.mark_final_result(key, {"all_rank_content_checks": every_content_ok})
                commit = controller.pop_committable()
                decision = {"ok": every_content_ok and commit is not None and not commit.cancelled,
                            "ordered_commit": asdict(commit) if commit else None,
                            "all_rank_content_checks": every_content_ok,
                            "evidence_scope": "synthetic stage dependency only; no measured speedup"}
            else:
                decision = None
            result["final_commit"] = command(decision)
            world.barrier()
            mark("all_ranks_device_protocol_complete", retained_resource_count=len(resources))
            result["safe_device_completion"] = True
            destroy_model_parallel()
            destroy_distributed_environment()
            result["status"] = "PASS_SYNTHETIC_STAGE_DEPENDENCY_ONLY"
            success = True
            return 0
    except BaseException as error:
        result.update(status="FAIL_STAGE_DEPENDENCY_PROTOCOL", error=repr(error),
                      traceback=traceback.format_exc())
        mark("probe_failed", error=repr(error), safe_device_completion=result["safe_device_completion"])
        raise
    finally:
        result.update(events=events, normal_cleanup=success,
                      completed_utc=datetime.now(timezone.utc).isoformat())
        target = output_dir / f"rank-{rank:02d}.json"
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"rank": rank, "status": result["status"], "result": str(target)}), flush=True)
        log.close()


if __name__ == "__main__":
    raise SystemExit(main())
