"""Validate this narrow generated overlay and record portable module mappings."""
from __future__ import annotations

import ast
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OVERLAY = HERE / "runtime_overlay"
MODULES = {
    "vllm/v1/core/sched/scheduler.py": ["vllm.v1.core.sched.scheduler"],
    "vllm/v1/core/sched/ks_stage_reservation.py": ["vllm.v1.core.sched.ks_stage_reservation"],
    "vllm/v1/engine/core.py": ["vllm.v1.engine.core"],
    "vllm/v1/engine/ks_restore_binding.py": ["vllm.v1.engine.ks_restore_binding"],
    "vllm/v1/executor/multiproc_executor.py": ["vllm.v1.executor.multiproc_executor"],
    "vllm_ascend/worker/worker.py": ["vllm_ascend.worker.worker"],
    "lmcache/integration/vllm/lmcache_mp_connector.py": ["lmcache.integration.vllm.lmcache_mp_connector"],
    "lmcache/integration/vllm/lmcache_mp_connector_multi17.py": ["lmcache_mp_connector_multi17", "lmcache.integration.vllm.lmcache_mp_connector_multi17"],
    "lmcache/integration/vllm/vllm_multi_process_adapter.py": ["lmcache.integration.vllm.vllm_multi_process_adapter"],
    "lmcache/integration/vllm/ks_restore_runtime.py": ["lmcache.integration.vllm.ks_restore_runtime"],
    "lmcache/integration/vllm/ks_restore_metadata_bridge.py": ["lmcache.integration.vllm.ks_restore_metadata_bridge"],
    "ks_stage_executor_driver.py": ["ks_stage_executor_driver"],
    "typed_stage_protocol.py": ["typed_stage_protocol"],
    "span_stage_controller.py": ["span_stage_controller"],
}


def main():
    files = []
    for relative, names in MODULES.items():
        path = OVERLAY / relative
        text = path.read_text(encoding="utf-8")
        ast.parse(text, filename=str(path))
        assert "\ufffd" not in text
        files.append({"relative_path": relative, "module_names": names,
                      "bytes": path.stat().st_size})
    evidence = {}
    for filename in ("scheduler_reservation_cpu_result.json", "executor_driver_cpu_result.json", "integrated_first_token_cpu_result.json"):
        result = json.loads((HERE / filename).read_text(encoding="utf-8"))
        assert result["status"].startswith("PASS"), (filename, result["status"])
        evidence[filename] = {"status": result["status"], "tests": result.get("tests", result.get("total"))}
    result = {
        "status": "UNIFIED_FIRST_TOKEN_OVERLAY_CPU_CHECKED_NOT_PRIVATE_IMPORTED",
        "root": str(OVERLAY), "files": files,
        "flags": {"KS_PP_STAGE_DRIVER": "1", "KS_PP_STAGE_EVENTS": "1",
                  "KS_CORE_STAGE_EVENT_PUMP": "1", "KS_LMCACHE_STAGE_OBSERVER": "1"},
        "scope": "single first token; max_tokens=1; full cached prefix or exactly one remaining token; async_scheduling=True; actual-config TP/PP2 (CPU TP8 and TP4); MTP3 unchanged",
        "common_dependencies_to_keep": ["existing local-completion model_runner_local.py mapping and local flags", "existing pin/17-line adaptations"],
        "evidence": evidence, "actual_private_import_verified": False,
        "actual_glm_candidate_started": False, "npu_actions_by_this_builder": 0,
        "baseline_vllm_commit": "0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665",
        "lmcache_adapter_source": "adapter_rebase/actual137_vllm_multi_process_adapter.py",
        "observer_definitions_embedded_in_adapter": ["StageRestoreIdentity", "LMCacheStageObserver"],
        "observer_identity_rule": "runtime uses the same adapter module classes; do not import a second identity copy",
        "lmcache_upstream_commit": "NOT_VERIFIED",
    }
    (HERE / "unified_overlay_manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "files": len(files), "evidence": evidence}, ensure_ascii=False))


if __name__ == "__main__":
    main()
