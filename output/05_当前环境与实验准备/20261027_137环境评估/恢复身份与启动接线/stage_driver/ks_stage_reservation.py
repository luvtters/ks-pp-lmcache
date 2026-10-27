"""One-token partial-KV reservation for the original Scheduler, not fake global READY."""
from __future__ import annotations

import os
from copy import copy


def enabled():
    return os.environ.get("KS_PP_STAGE_DRIVER") == "1"


def bind(scheduler, controller):
    pc = scheduler.parallel_config
    tp = pc.tensor_parallel_size
    if (type(tp) is not int or tp < 1 or pc.pipeline_parallel_size != 2
            or pc.world_size != 2 * tp
            or controller.stage_workers != (tuple(range(tp)), tuple(range(tp, 2 * tp)))
            or any(type(rank) is not int for group in controller.stage_workers for rank in group)):
        raise ValueError("stage workers must match actual single-node TP/PP2 global ranks")
    if getattr(scheduler, "_ks_stage_controller", controller) is not controller:
        raise RuntimeError("another stage controller is already bound")
    scheduler._ks_stage_controller = controller
    if not hasattr(scheduler, "_ks_partial_restore_reservations"):
        scheduler._ks_partial_restore_reservations = {}
        scheduler._ks_partial_batch_sequence = 0


def _trace_decision(scheduler, request, reason, decision):
    if not os.environ.get("KS_STAGE_TRACE_DIR"):
        return
    controller = getattr(scheduler, "_ks_stage_controller", None)
    restore = controller._restores.get(request.request_id) if controller is not None else None
    front_ready = restore is not None and restore.ready[0] == set(controller.stage_workers[0])
    all_ready = restore is not None and all(
        restore.ready[stage] == set(controller.stage_workers[stage]) for stage in (0, 1))
    busy = getattr(controller, "_busy", ())
    stage_busy = any(value is not None for value in busy)
    generation = restore.generation if restore is not None else None
    global_finished = request.request_id in scheduler.finished_recving_kv_req_ids
    state = (generation, reason, decision, front_ready, all_ready, stage_busy, global_finished)
    previous = getattr(scheduler, "_ks_partial_trace_decisions", None)
    if previous is None:
        previous = scheduler._ks_partial_trace_decisions = {}
    if previous.get(request.request_id) == state:
        return
    previous[request.request_id] = state
    from ks_stage_executor_driver import trace_event
    trace_event("partial_admission_decision", requests=((request.request_id, generation),),
        reason=reason, decision=decision, front_kv_ready=front_ready,
        all_stages_ready=all_ready, stage_busy=stage_busy,
        request_global_finished=global_finished, timestamp_kind="cpu_scheduler_decision")


def try_partial_promotion(scheduler, request, waiting_status):
    if not enabled() or request.status is not waiting_status:
        return False
    if request.request_id in scheduler.finished_recving_kv_req_ids:
        _trace_decision(scheduler, request, "request_global_finished", "deferred_to_original")
        return False
    controller = getattr(scheduler, "_ks_stage_controller", None)
    if controller is None:
        raise RuntimeError("actual core stage controller must be bound before scheduling")
    restore = controller._restores.get(request.request_id)
    if restore is None:
        _trace_decision(scheduler, request, "restore_unregistered", "deferred")
        return False
    if restore.ready[0] != set(controller.stage_workers[0]):
        _trace_decision(scheduler, request, "front_not_ready", "deferred")
        return False
    if restore.invalid:
        _trace_decision(scheduler, request, "restore_invalid", "rejected")
        raise RuntimeError("invalid restore cannot reserve stage computation")
    position = min(request.num_computed_tokens, request.num_tokens - 1)
    if (scheduler.scheduler_config.async_scheduling is not True
            or request.sampling_params is None or request.sampling_params.max_tokens != 1
            or request.max_tokens != 1 or request.num_output_tokens != 0
            or request.num_output_placeholders != 0 or request.spec_token_ids
            or request.use_structured_output or request.has_encoder_inputs
            or request.num_preemptions != 0 or position < 1
            or request.num_tokens - position != 1):
        _trace_decision(scheduler, request, "unsupported_request_shape", "rejected")
        raise NotImplementedError("stage reservation supports only first token, max_tokens=1, no input speculation or chunked remainder")
    reservations = scheduler._ks_partial_restore_reservations
    if request.request_id in reservations:
        raise RuntimeError("duplicate partial restore promotion")
    reservations[request.request_id] = {
        "restore_generation": restore.generation,
        "position": position,
        "restored_prefix_tokens": request.num_computed_tokens,
        "tagged": False,
        "global_complete": False,
        "final_consumed": False,
    }
    # The ordinary Scheduler will allocate the actual budget/blocks and create
    # NewRequestData. Do not call _update_waiting_for_remote_kv/cache_blocks.
    request.num_computed_tokens = position
    reason = ("all_stages_ready" if all(restore.ready[stage] == set(controller.stage_workers[stage])
              for stage in (0, 1)) else "front_ready")
    _trace_decision(scheduler, request, reason, "reserved")
    return True


def delay_cache_blocks(scheduler, request_id):
    return request_id in getattr(scheduler, "_ks_partial_restore_reservations", {})


def attach_output(scheduler, output):
    reservations = getattr(scheduler, "_ks_partial_restore_reservations", {})
    ids = [request_id for request_id in output.num_scheduled_tokens if request_id in reservations]
    if not ids:
        return
    if (len(ids) != 1 or len(output.num_scheduled_tokens) != 1
            or output.num_scheduled_tokens[ids[0]] != 1
            or output.total_num_scheduled_tokens != 1
            or any(output.scheduled_spec_decode_tokens.values())
            or output.scheduled_encoder_inputs
            or output.pending_structured_output_tokens):
        raise NotImplementedError("the partial first-token diagnostic cannot mix requests, chunks or speculative inputs")
    request_id = ids[0]
    reservation = reservations[request_id]
    if reservation["tagged"]:
        raise RuntimeError("partial first-token batch was already emitted")
    if (len(output.scheduled_new_reqs) != 1
            or output.scheduled_new_reqs[0].req_id != request_id
            or output.scheduled_new_reqs[0].num_computed_tokens != reservation["position"]):
        raise RuntimeError("actual NewRequestData does not match the reserved input position")
    sequence = scheduler._ks_partial_batch_sequence
    scheduler._ks_partial_batch_sequence += 1
    output.ks_stage_batch = {
        "batch_id": f"ks-first-token:{sequence}", "generation": 0,
        "requests": ((request_id, reservation["restore_generation"]),),
        "token_positions": ((reservation["position"],),),
        "restored_prefix_tokens": {request_id: reservation["restored_prefix_tokens"]},
    }
    reservation["tagged"] = True


def handle_true_finished_recv(scheduler, request_id):
    reservations = getattr(scheduler, "_ks_partial_restore_reservations", {})
    reservation = reservations.get(request_id)
    if reservation is None:
        return False
    if reservation["global_complete"]:
        raise RuntimeError("duplicate global restore completion for partial request")
    # The original AsyncScheduler caches the valid prefix once while consuming
    # the actual final token, before _free_request. Do not cache or free here.
    reservation["global_complete"] = True
    if reservation["final_consumed"]:
        del reservations[request_id]
    return True


def prepare_update_output(scheduler, kv_output):
    if kv_output is None:
        return None
    consumed = {request_id for request_id in kv_output.finished_recving or ()
                if handle_true_finished_recv(scheduler, request_id)}
    if not consumed:
        return kv_output
    result = copy(kv_output)
    result.finished_recving = set(kv_output.finished_recving or ()) - consumed
    return result


def after_update_output(scheduler, output):
    reservations = getattr(scheduler, "_ks_partial_restore_reservations", {})
    for request_id in output.num_scheduled_tokens:
        reservation = reservations.get(request_id)
        if reservation is None:
            continue
        request = scheduler.requests.get(request_id)
        if request is None or request.is_finished():
            reservation["final_consumed"] = True
            if reservation["global_complete"]:
                del reservations[request_id]
