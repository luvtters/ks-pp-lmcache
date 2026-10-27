"""Build only this directory's disabled restore-event executor candidate."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "control_sidechannel/multiproc_executor.py"

RESTORE_EVENT = '''@dataclass(frozen=True)
class KSRestoreEvent:
    """Per-request restore identity; deliberately has no logical batch ID."""
    event_id: str
    request_id: str
    restore_generation: int
    stage: int
    worker_id: int
    tp_rank: int
    pcp_rank: int
    sequence: int

    def validate(self):
        if (type(self.event_id) is not str or not 0 < len(self.event_id) <= 128
                or type(self.request_id) is not str or not self.request_id
                or any(type(value) is not int or value < 0 for value in (
                    self.restore_generation, self.stage, self.worker_id,
                    self.tp_rank, self.pcp_rank))
                or type(self.sequence) is not int or self.sequence < 1):
            raise ValueError("invalid restore event identity")


'''

RESTORE_PUBLISH = '''    def publish_restore_ready(self, identity):
        # Called only with success drained from the unique device-future owner.
        # This method does not query/synchronize an event or manufacture readiness.
        identity.validate()
        if (self.pcp_size != 1 or type(self.tp_size) is not int or self.tp_size < 1
                or identity.global_worker_rank != self.worker_id
                or identity.stage_id != self.stage
                or self.stage != self.worker_id // self.tp_size
                or identity.tp_rank != self.worker_id % self.tp_size
                or identity.pcp_rank != 0):
            raise ValueError("restore identity does not match publisher PP/TP/PCP rank")
        with self.lock:
            if self.stopped.is_set() or self.failed.is_set():
                raise RuntimeError("stage event channel unavailable")
            sequence = self.sequence + 1
            event = KSRestoreEvent(f"restore:{self.worker_id}:{sequence}",
                identity.request_id, identity.restore_generation, self.stage,
                self.worker_id, identity.tp_rank, identity.pcp_rank, sequence)
            event.validate()
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.failed.set()
                raise RuntimeError("stage event inbox full") from None
            self.sequence = sequence

'''


def replace_once(source, before, after):
    if source.count(before) != 1:
        raise ValueError(f"source anchor is not unique: {before[:90]!r}")
    return source.replace(before, after, 1)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    candidate = replace_once(original, "class _KSStageEventPublisher:",
                             RESTORE_EVENT + "class _KSStageEventPublisher:")
    candidate = replace_once(candidate,
        "def __init__(self, transport, failed, worker_id, stage):",
        "def __init__(self, transport, failed, worker_id, stage, tp_size=None, pcp_size=1):")
    candidate = replace_once(candidate, "        self.stage = stage\n",
        "        self.stage = stage\n        self.tp_size = tp_size\n        self.pcp_size = pcp_size\n")
    candidate = replace_once(candidate, "    def _run(self):\n", RESTORE_PUBLISH + "    def _run(self):\n")
    candidate = replace_once(candidate,
        "        self._ks_stage_tp_size = tp_size * pcp_size\n",
        "        self._ks_stage_tp_size = tp_size * pcp_size\n"
        "        self._ks_restore_tp_size = tp_size\n"
        "        self._ks_restore_pcp_size = pcp_size\n")
    candidate = replace_once(candidate, "if not isinstance(event, KSStageEvent):",
                             "if not isinstance(event, (KSStageEvent, KSRestoreEvent)):")
    candidate = replace_once(candidate,
        '                if event.kind in ("CONTROL_RESULT", "CONTROL_ERROR"):',
        '''                if isinstance(event, KSRestoreEvent):
                    if (self._ks_restore_pcp_size != 1
                            or event.tp_rank != rank % self._ks_restore_tp_size
                            or event.pcp_rank != 0):
                        raise ValueError("restore event TP/PCP identity mismatch")
                elif event.kind in ("CONTROL_RESULT", "CONTROL_ERROR"):''')
    candidate = replace_once(candidate,
        "            transport, failed, self.rank, self._ks_stage_index)",
        "            transport, failed, self.rank, self._ks_stage_index,\n"
        "            tp_size=parallel.tensor_parallel_size,\n"
        "            pcp_size=parallel.prefill_context_parallel_size)")
    candidate = replace_once(candidate,
        "        self.worker.ks_publish_stage_event = self._ks_stage_publisher.publish\n",
        "        self.worker.ks_publish_stage_event = self._ks_stage_publisher.publish\n"
        "        self.worker.ks_publish_restore_ready = self._ks_stage_publisher.publish_restore_ready\n")
    ast.parse(candidate)
    output = HERE / "multiproc_executor.py"
    output.write_bytes(candidate.encode("utf-8"))
    delta = "".join(difflib.unified_diff(original.splitlines(True), candidate.splitlines(True),
        fromfile="a/vllm/v1/executor/multiproc_executor.py",
        tofile="b/vllm/v1/executor/multiproc_executor.py"))
    (HERE / "restore_event_binding.patch").write_bytes(delta.encode("utf-8"))
    report = {"status": "BUILT_NOT_RUNTIME_INTEGRATED", "source": str(SOURCE),
              "candidate": str(output), "diff_is_incremental": True,
              "patch_crlf": delta.count("\r\n"), "patch_lf": delta.count("\n")}
    (HERE / "build_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    build()
