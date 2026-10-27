"""Generate Scheduler-only first-token overlay; never touches executor/core peers."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
SOURCE = OUT / "05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/core/sched/scheduler.py"
DEST = HERE / "runtime_overlay/vllm/v1/core/sched"


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"source anchor is not unique: {old[:100]!r}")
    return text.replace(old, new, 1)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    text = replace_once(original, "import itertools\n", "from vllm.v1.core.sched import ks_stage_reservation as _ks_stage_reservation\nimport itertools\n")
    marker = "    def _try_promote_blocked_waiting_request(self, request: Request) -> bool:\n"
    method = '''    def ks_bind_stage_controller(self, controller):
        _ks_stage_reservation.bind(self, controller)

'''
    text = replace_once(text, marker, method + marker)
    text = replace_once(text,
        "        if request.status == RequestStatus.WAITING_FOR_REMOTE_KVS:\n            # finished_recving_kv_req_ids is populated during\n",
        "        if request.status == RequestStatus.WAITING_FOR_REMOTE_KVS:\n"
        "            if _ks_stage_reservation.try_partial_promotion(self, request, RequestStatus.WAITING_FOR_REMOTE_KVS):\n"
        "                request.status = RequestStatus.WAITING\n"
        "                return True\n"
        "            # finished_recving_kv_req_ids is populated during\n")
    text = replace_once(text, "                    delay_cache_blocks=load_kv_async,\n",
                        "                    delay_cache_blocks=(load_kv_async or _ks_stage_reservation.delay_cache_blocks(self, request_id)),\n")
    text = replace_once(text,
        '        with record_function_or_nullcontext("schedule: update_after_schedule"):\n',
        '        _ks_stage_reservation.attach_output(self, scheduler_output)\n'
        '        with record_function_or_nullcontext("schedule: update_after_schedule"):\n')
    text = replace_once(text,
        '            assert req_id in self.requests\n            req = self.requests[req_id]\n',
        "            if _ks_stage_reservation.handle_true_finished_recv(self, req_id):\n"
        "                continue\n"
        '            assert req_id in self.requests\n            req = self.requests[req_id]\n')
    text = replace_once(text,
        "        kv_connector_output = model_runner_output.kv_connector_output\n",
        "        kv_connector_output = _ks_stage_reservation.prepare_update_output(self, model_runner_output.kv_connector_output)\n")
    text = replace_once(text,
        "        return engine_core_outputs\n",
        "        _ks_stage_reservation.after_update_output(self, scheduler_output)\n        return engine_core_outputs\n")
    ast.parse(text)
    DEST.mkdir(parents=True, exist_ok=True)
    (DEST / "scheduler.py").write_text(text, encoding="utf-8", newline="\n")
    helper = (HERE / "ks_stage_reservation.py").read_text(encoding="utf-8")
    ast.parse(helper)
    (DEST / "ks_stage_reservation.py").write_text(helper, encoding="utf-8", newline="\n")
    delta = "".join(difflib.unified_diff(original.splitlines(True), text.splitlines(True),
        fromfile="a/vllm/v1/core/sched/scheduler.py", tofile="b/vllm/v1/core/sched/scheduler.py"))
    (HERE / "scheduler_reservation.patch").write_text(delta, encoding="utf-8", newline="\n")
    report = {"status": "SCHEDULER_FIRST_TOKEN_OVERLAY_BUILT_NOT_INSTALLED",
              "source": str(SOURCE), "candidate": str(DEST / "scheduler.py"),
              "base_framework_commit": "0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665",
              "feature_flag": "KS_PP_STAGE_DRIVER", "max_tokens_supported": 1,
              "installed_source_changes": 0, "npu_actions": 0}
    (HERE / "scheduler_reservation_build.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    build()
