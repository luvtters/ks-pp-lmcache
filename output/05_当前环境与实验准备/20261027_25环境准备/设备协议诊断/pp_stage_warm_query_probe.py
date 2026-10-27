"""PP2/TP8真实设备通信诊断；载荷是小张量，不运行GLM或LMCache。

--cpu-plan只输出计划，不导入torch/vLLM；真正占卡必须显式传--run-device-probe。
方法返回、Work完成、stream依赖和设备事件完成分别记录，不能互相替代。
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import time
import traceback


TP = 8
PP = 2
WORLD = TP * PP
PAYLOAD_SHAPE = (8, 32)
QUERY_WINDOW_SECONDS = 2.0


def plan():
    return {
        "scope": "SYNTHETIC_PAYLOAD_DEVICE_PROTOCOL_ONLY",
        "world_size": WORLD, "tensor_parallel_size": TP, "pipeline_parallel_size": PP,
        "backend": "hccl", "backend_fallback_allowed": False,
        "payload_shape": PAYLOAD_SHAPE, "payload_dtype": "float32",
        "full_tensor_bytes_each_front_rank": 8 * 32 * 4,
        "expected_tensor_slice_bytes_per_pp_lane": 32 * 4,
        "kv_payload": "32-float local NPU tensor; not real LMCache restore",
        "front_ranks": list(range(8)), "back_ranks": list(range(8, 16)),
        "device_operations": ["allocate small tensors", "one placeholder add on front",
                              "vLLM isend/irecv_tensor_dict", "TP reconstruction", "NPU event queries"],
        "nonblocking_query_window_seconds": QUERY_WINDOW_SECONDS, "query_probe": "two_round_diagnostic",
        "artificial_sleep": False, "transfer_rounds": 2,
        "model_loaded": False, "lmcache_called": False,
        "not_verified": ["GLM outputs", "real KV restore and completion publication",
                         "stage scheduler integration", "DSA CP/MoE/MTP execution",
                         "Mooncake consumption", "TTFT/TPOT/throughput or acceleration"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--cpu-plan", action="store_true")
    mode.add_argument("--run-device-probe", action="store_true")
    parser.add_argument("--output-dir", default="/opt/ks-stage-ready/device-probe/results")
    args = parser.parse_args()
    if args.cpu_plan:
        print(json.dumps(plan(), ensure_ascii=False, indent=2))
        return 0

    rank = int(os.environ.get("RANK", "-1"))
    local_rank = int(os.environ.get("LOCAL_RANK", "-1"))
    world_size = int(os.environ.get("WORLD_SIZE", "-1"))
    if world_size != WORLD or not 0 <= rank < WORLD or local_rank != rank:
        raise SystemExit("Use one-node torchrun with exactly 16 local processes.")
    output_dir = Path(args.output_dir).resolve()
    allowed = Path("/opt/ks-stage-ready").resolve()
    if not output_dir.is_relative_to(allowed):
        raise SystemExit("Output must remain in the private /opt/ks-stage-ready directory.")
    output_dir.mkdir(parents=True, exist_ok=True)
    events = []
    result = {"rank": rank, "local_rank": local_rank, "plan": plan(), "status": "STARTING"}
    result["candidate_stage_ready_route"] = "NOT_VERIFIED"
    result["full_design_validation"] = "NOT_TESTED"
    event_path = output_dir / f"rank-{rank:02d}.events.jsonl"
    event_log = event_path.open("a", encoding="utf-8", buffering=1)

    transfer_round = -1

    def mark(action, **values):
        # 先逐条落盘，再即时输出；主动中止时无需依赖最后finally保存进度。
        event = {"record_type": "probe_event", "rank": rank, "pid": os.getpid(),
                 "index": len(events) + 1, "action": action, "transfer_round": transfer_round,
                 "monotonic_ns": time.monotonic_ns(),
                 "utc_time": datetime.now(timezone.utc).isoformat(), **values}
        events.append(event)
        line = json.dumps(event, ensure_ascii=False)
        event_log.write(line + "\n")
        event_log.flush()
        print(line, flush=True)

    success = False
    try:
        mark("probe_enter", events_path=str(event_path))
        # 只有设备模式会进入这些导入。CPU计划不加载框架、不初始化设备。
        mark("torch_import_enter")
        import torch
        mark("torch_import_returned")
        mark("torch_npu_import_enter")
        import torch_npu  # noqa: F401 -- registers the NPU/HCCL backend.
        mark("torch_npu_import_returned")
        mark("torch_distributed_import_enter")
        import torch.distributed as dist
        mark("torch_distributed_import_returned")
        mark("vllm_import_enter")
        from vllm.config import ParallelConfig, VllmConfig, set_current_vllm_config
        from vllm.distributed.parallel_state import (
            destroy_distributed_environment,
            destroy_model_parallel,
            get_pp_group,
            get_tp_group,
            get_world_group,
            init_distributed_environment,
            initialize_model_parallel,
        )
        mark("vllm_import_returned")

        mark("frameworks_imported")
        if not dist.is_backend_available("hccl"):
            raise RuntimeError("HCCL backend unavailable; refusing Gloo fallback.")
        device = torch.device(f"npu:{local_rank}")
        mark("set_device_enter")
        torch.npu.set_device(device)
        mark("set_device_returned")
        torch.set_num_threads(1)
        mark("device_selected")

        mark("config_construction_enter")
        config = VllmConfig(
            parallel_config=ParallelConfig(
                tensor_parallel_size=TP,
                pipeline_parallel_size=PP,
                distributed_executor_backend="mp",
            ),
        )
        mark("config_construction_returned")
        with set_current_vllm_config(config):
            mark("init_distributed_environment_enter")
            init_distributed_environment(
                world_size=WORLD, rank=rank, local_rank=local_rank,
                distributed_init_method="env://", backend="hccl", timeout=timedelta(seconds=90),
            )
            mark("init_distributed_environment_returned")
            mark("initialize_model_parallel_enter")
            initialize_model_parallel(
                tensor_model_parallel_size=TP, pipeline_model_parallel_size=PP,
                prefill_context_model_parallel_size=1,
                decode_context_model_parallel_size=1, backend="hccl",
            )
            mark("initialize_model_parallel_returned")
            pp = get_pp_group()
            tp = get_tp_group()
            world = get_world_group()
            if pp.world_size != PP or tp.world_size != TP:
                raise RuntimeError("Actual groups differ from PP2/TP8.")
            if str(dist.get_backend(world.device_group)).lower() != "hccl":
                raise RuntimeError("The real device group did not use HCCL.")
            if pp.device.type != "npu" or tp.device.type != "npu":
                raise RuntimeError("vLLM groups did not select NPU devices.")
            result.update(pp_rank=pp.rank_in_group, tp_rank=tp.rank_in_group,
                          pp_global_ranks=list(pp.ranks), tp_global_ranks=list(tp.ranks))
            mark("groups_ready")

            # 这是本地小张量的就绪事件，不是LMCache恢复完成事件。
            simulated_kv = torch.full((32,), float(rank), device=device)
            kv_event = torch.npu.Event()
            kv_event.record()
            mark("synthetic_local_kv_event_recorded", event_query=bool(kv_event.query()))

            result["round_results"] = []
            for transfer_round in range(2):
                # CPU组对齐，不向被测HCCL stream增加集合通信。
                dist.barrier(group=world.cpu_group)
                mark("transfer_round_start")
                postprocess = []
                if pp.is_first_rank:
                    source = torch.arange(256, dtype=torch.float32, device=device).reshape(PAYLOAD_SHAPE)
                    source = source + 1  # 一次占位加法，不是模型前41层。
                    source_event = torch.npu.Event()
                    source_event.record()
                    source_event.synchronize()
                    mark("front_placeholder_tensor_ready")
                    payload = {"hidden_states": source, "batch_id": f"device-probe-{transfer_round}",
                               "lane": tp.rank_in_group}
                    mark("isend_enter")
                    handles = pp.isend_tensor_dict(payload, all_gather_group=tp)
                    mark("isend_method_returned", handle_count=len(handles))
                    tensors = None
                else:
                    mark("irecv_enter")
                    tensors, handles, postprocess = pp.irecv_tensor_dict(all_gather_group=tp)
                    mark("irecv_method_returned", handle_count=len(handles),
                         postprocess_count=len(postprocess))
                    if tensors is None:
                        raise RuntimeError("No activation received.")

                if not handles:
                    raise RuntimeError("No asynchronous handles: requested Work-query path was not exercised.")

                def query_handles():
                    values = []
                    for handle in handles:
                        query = getattr(handle, "is_completed", None)
                        try:
                            values.append(query() is True if callable(query) else None)
                        except Exception as error:
                            values.append(f"ERROR:{type(error).__name__}")
                    return values

                first_query = query_handles()
                after_return_event = torch.npu.Event()
                after_return_event.record()
                mark("queries_after_method_return", work_query=first_query,
                     local_stream_event_query=bool(after_return_event.query()))
                result["work_query_at_return"] = first_query
                # 仅用于观测Work.query能否独立推进。CPU小集合通信让同段统一结束轮询。
                # 这段诊断轮询不得计为模型或缓存性能，更不能拿它伪造TTFT。
                deadline = time.monotonic() + QUERY_WINDOW_SECONDS
                polls = 0
                while True:
                    values = query_handles()
                    flag = torch.tensor([int(all(value is True for value in values)),
                                         int(time.monotonic() >= deadline)], dtype=torch.int32)
                    dist.all_reduce(flag, op=dist.ReduceOp.SUM, group=tp.cpu_group)
                    polls += 1
                    ready_count, expired_count = flag.tolist()
                    if ready_count == TP or expired_count:
                        result["nonblocking_query_consensus"] = ready_count == TP
                        result["nonblocking_readiness_contract"] = (
                            "OBSERVED_READY_BEFORE_WAIT" if ready_count == TP
                            else "NOT_ESTABLISHED_WITHIN_QUERY_WINDOW"
                        )
                        break
                result["query_poll_rounds"] = polls
                mark("bounded_work_query_finished", **{
                    "consensus_ready": result["nonblocking_query_consensus"], "poll_rounds": polls,
                })

                # Work方法返回和设备当前stream可读仍分开。wait后另记录设备事件确认。
                mark("work_wait_enter")
                for handle in handles:
                    handle.wait()
                mark("work_wait_returned", work_query=query_handles())
                comm_event = torch.npu.Event()
                comm_event.record()
                mark("comm_fence_recorded", event_query=bool(comm_event.query()))
                comm_event.synchronize()
                mark("comm_fence_known_complete", event_query=bool(comm_event.query()))

                # 后处理可能是TP all-gather，先让本段所有rank都完成通信依赖。
                tp.barrier()
                if not pp.is_first_rank:
                    if not postprocess:
                        raise RuntimeError("TP reconstruction path was not exercised.")
                    mark("tp_postprocess_enter")
                    for operation in postprocess:
                        operation()
                    reconstructed_event = torch.npu.Event()
                    reconstructed_event.record()
                    mark("tp_postprocess_method_returned", event_query=bool(reconstructed_event.query()))
                    reconstructed_event.synchronize()
                    mark("activation_device_ready")
                    if tensors["batch_id"] != f"device-probe-{transfer_round}" or tensors["lane"] != tp.rank_in_group:
                        raise RuntimeError("Activation metadata belongs to another batch or TP lane.")
                    expected = torch.arange(256, dtype=torch.float32).reshape(PAYLOAD_SHAPE) + 1
                    if not torch.equal(tensors["hidden_states"].cpu(), expected):
                        raise RuntimeError("TP-reconstructed tensor content mismatch.")
                    result["tensor_content_verified"] = True
                else:
                    # source/payload仍被引用；发送完成前不释放或改写源张量。
                    result["source_retained_until_send_completed"] = source is payload["hidden_states"]

                result["round_results"].append({
                    "round": transfer_round,
                    "kind": "first_transfer" if transfer_round == 0 else "warm_transfer",
                    "nonblocking_query_consensus": result["nonblocking_query_consensus"],
                    "query_poll_rounds": result["query_poll_rounds"],
                    "work_query_at_return": result["work_query_at_return"],
                    "tensor_content_verified": result.get("tensor_content_verified"),
                    "source_retained_until_send_completed": result.get("source_retained_until_send_completed"),
                })
                mark("transfer_round_complete")

            kv_event.synchronize()
            result["synthetic_kv_event_complete"] = bool(kv_event.query())
            result["synthetic_kv_elements"] = simulated_kv.numel()
            world.barrier()
            mark("all_ranks_protocol_complete")
            destroy_model_parallel()
            destroy_distributed_environment()
            mark("communication_groups_destroyed")
            result["status"] = "PASS_DEVICE_TRANSFER_ONLY"
            success = True
        return 0
    except BaseException as error:
        result["status"] = "FAIL_DEVICE_PROTOCOL"
        result["error"] = f"{type(error).__name__}: {error}"
        result["traceback"] = traceback.format_exc()
        raise
    finally:
        result.update(events=events, completed_at_utc=datetime.now(timezone.utc).isoformat(),
                      normal_cleanup=success)
        (output_dir / f"rank-{rank:02d}.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps({"rank": rank, "status": result["status"],
                          "result": str(output_dir / f"rank-{rank:02d}.json")}), flush=True)
        event_log.close()


if __name__ == "__main__":
    raise SystemExit(main())
