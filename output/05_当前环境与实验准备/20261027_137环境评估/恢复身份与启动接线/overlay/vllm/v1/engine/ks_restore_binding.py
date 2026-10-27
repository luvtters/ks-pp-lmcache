"""Explicit observer drain and core-thread registration. NOT_RUNTIME_INTEGRATED."""
from __future__ import annotations


def publish_observer_success(observer, publisher):
    """Forward only already-confirmed identities; never query a device Future.

    A publisher failure must fail the channel/caller, not be treated as delivery.
    This explicit drain does not schedule an automatic runtime bridge thread.
    """
    identities = observer.drain_success()
    for identity in identities:
        publisher.publish_restore_ready(identity)
    return len(identities)


class RestoreEventBinding:
    """Core callback records individual workers of an existing restore only."""
    def __init__(self, controller, restore_event_type, *, tp_size=8, pcp_size=1,
                 on_stage_event=None):
        if (type(tp_size) is not int or tp_size < 1 or pcp_size != 1
                or controller.stage_workers != (
                    tuple(range(tp_size)), tuple(range(tp_size, 2 * tp_size)))):
            raise ValueError("restore binding requires explicit single-node PP2/PCP1 layout")
        if on_stage_event is not None and not callable(on_stage_event):
            raise ValueError("existing stage event handler must be callable")
        self.controller = controller
        self.restore_event_type = restore_event_type
        self.tp_size = tp_size
        self.on_stage_event = on_stage_event

    def __call__(self, event):
        if not isinstance(event, self.restore_event_type):
            if self.on_stage_event is not None:
                if self.on_stage_event(event) is not None:
                    raise ValueError("stage event callback must return None")
            return None
        event.validate()
        if (event.stage != event.worker_id // self.tp_size
                or event.tp_rank != event.worker_id % self.tp_size
                or event.pcp_rank != 0):
            raise ValueError("restore callback rank identity mismatch")
        # Deliberately never call begin_restore here: unknown/stale events cannot
        # create state. One worker's readiness cannot stand for all PP/TP workers.
        self.controller.mark_kv_ready(event.request_id, event.restore_generation,
                                      event.stage, event.worker_id, device_complete=True)
        return None
