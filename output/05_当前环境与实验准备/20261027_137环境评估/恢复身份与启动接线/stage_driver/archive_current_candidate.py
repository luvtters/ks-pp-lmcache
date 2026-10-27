"""One local-only explicit-file snapshot; no git commands, hashes or tree walk."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[1]
REPO = Path(r"E:\work\ks\repos\ks-pp-lmcache")
DEST = REPO / "output/05_当前环境与实验准备/20261027_137环境评估"
BRIDGE = "恢复身份与启动接线/"
STAGE = BRIDGE + "stage_driver/"

STAGE_FILES = """build_actual137_observer.py build_common_connector_overlay.py
build_executor_driver_overlay.py build_scheduler_reservation.py build_span_controller.py
build_unified_manifest.py check_executor_driver_wire_cpu.py check_import_dependency_closure.py
check_integrated_first_token.py check_scheduler_reservation.py check_typed_stage_protocol.py
deploy_private_candidate.py prepare_candidate_native.py verify_current_driver_contracts.py
archive_current_candidate.py typed_stage_protocol.py span_stage_controller.py ks_stage_reservation.py
executor_driver_build.json executor_driver_cpu_result.json executor_driver_loader.json
integrated_first_token_cpu_result.json import_dependency_closure.json
unified_overlay_manifest.json scheduler_reservation_build.json scheduler_reservation_cpu_result.json
span_controller_build.json typed_stage_cpu_result.json driver_contract_result.json
private_candidate_deployment.json common17_notification_rebase.json common17_notification_rebase.patch
core.driver.patch multiproc_executor.driver.patch worker.driver.patch
scheduler_reservation.patch span_controller.patch README.md EXECUTOR_WIRE.md
native_candidate/stage-candidate.env.native.sh native_candidate/p-stage-first-token.serve.native.sh
native_candidate/native.diff native_candidate/candidate_native_preparation.json
adapter_rebase/actual137_vllm_multi_process_adapter.py
adapter_rebase/actual137_observer.patch adapter_rebase/actual137_observer_build.json""".split()

BRIDGE_FILES = """build_connector_overlay.py check_connector_overlay.py
check_restore_metadata_bridge.py check_startup_hooks.py restore_metadata_bridge.py
ks_restore_runtime.py README.md connector_overlay_build.json connector_overlay_cpu_result.json
cpu_result.json startup_hooks_cpu_result.json connector_init.patch startup_hooks.patch
required_worker_publisher.patch
overlay/vllm/v1/engine/core.py overlay/vllm/v1/engine/ks_restore_binding.py
overlay/vllm/v1/engine/ks_stage_controller.py
overlay/vllm/v1/executor/multiproc_executor.py
overlay/lmcache/integration/vllm/lmcache_mp_connector.py
overlay/lmcache/integration/vllm/ks_restore_runtime.py
overlay/lmcache/integration/vllm/ks_restore_metadata_bridge.py""".split()

SUPPORT_FILES = """真实KV恢复验证/actual137_worker_source.txt
真实KV恢复验证/actual137_kv_aggregator.txt 真实KV恢复验证/core_mtp_source_bundle.txt
真实KV恢复验证/actual137_kv_mixin.txt 真实KV恢复验证/README.md
完整模型单机P对照/pp_core.py 完整模型单机P对照/pp_executor.py
完整模型单机P对照/runtime/local-completion/model_runner_local.py
完整模型单机P对照/runtime/local-completion/model_runner_local.py.patch
完整模型单机P对照/runtime/local-completion/lmcache_mp_connector_multi17.py
完整模型单机P对照/runtime/local-completion/lmcache_mp_connector_multi17.py.patch
完整模型单机P对照/runtime/local-completion/sitecustomize.py
完整模型单机P对照/runtime/local-completion/cpu_actual_methods.json
完整模型单机P对照/runtime/local-completion/preparation.json
完整模型单机P对照/runtime/common/sitecustomize.py
完整模型单机P对照/runtime/common/lmcache_cache_context_pinned.py
完整模型单机P对照/check_first_token_no_feedback.py
完整模型单机P对照/check_local_completion_methods.py
完整模型单机P对照/first_token_no_feedback_cpu.json
完整模型单机P对照/first_token_no_feedback_staged.json
完整模型单机P对照/warm1_诊断与首token反馈边界.md
完整模型单机P对照/common.env.native.sh
完整模型单机P对照/local-completion.env.native.sh
完整模型单机P对照/p-local-first-token.serve.native.sh
完整模型单机P对照/cache.server.native.sh
完整模型单机P对照/bench-first-token.native.sh""".split()

# These pre-existing repository sources are inputs to the unchanged builders and
# checks. Verify only these known paths; never modify or duplicate old snapshots.
OLD = "output/04_修改策略与验证/01_修改策略与隔离候选/"
EXISTING = [
    OLD + "20261027_PP分段就绪原型/controller/stage_controller.py",
    OLD + "20261027_PP分段就绪原型/worker_split/worker.py",
    OLD + "20261027_PP分段就绪原型/lmcache_stage_observer/stage_observer.py",
    OLD + "20261027_PP分段就绪原型/lmcache_stage_observer/build_lmcache_stage_observer.py",
    OLD + "20261027_PP分段就绪原型/lmcache_stage_observer/vllm_multi_process_adapter.py",
    OLD + "20261027_PP分段就绪原型/draft_wait_followup/multiproc_executor.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/lmcache_only_followup/source_current/lmcache/integration/vllm/lmcache_mp_connector.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/lmcache/integration/vllm/lmcache_mp_metadata.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/vllm/v1/core/sched/output.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/vllm/v1/core/sched/async_scheduler.py",
    OLD + "KV完成通知修改策略与隔离分支_20261022/source_baseline/vllm/v1/outputs.py",
    "output/05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/core/sched/scheduler.py",
]

README = """# 137首token阶段代码的本机归档

这是本次独立新增目录，不替换仓库原299文件、旧报告或旧源码快照。
当前可读入口为`恢复身份与启动接线/stage_driver/EXECUTOR_WIRE.md`，完整复制清单为
`COPY_MANIFEST.json`。所有复制文件按源字节原样保存；绝对路径、历史回执、源码来源和
未验证边界保持原文，不能因归档而变成新的实测。

保存内容包括：统一14模块；构建器和关键9项wire/1项Scheduler组合CPU入口及既有结果；
14模块依赖闭包与137真实7个标准库helper加载回执；metadata/bootstrap接线及其必要输入；
共同17/pin/modelrunner及首tokennative配置。早期bootstrap overlay与最终runtime_overlay
分目录保留，不能盲目叠加。最终runtime以统一manifest为准。

不含客户prompt/JSONL、模型数据、venv、密钥或运行大日志；bench脚本只引用外部原始数据
路径，不提供数据。文档引用的HTTP/cold/warm运行原始回执仍在原工作区，未复制入本归档。
源码bundle是必要CPU读取输入，不是运行日志。既有仓库上游许可证继续适用。

现有仓库未带齐的旧builder/CPU输入，原样放在本目录`dependency_inputs/`下，保留
其原仓库相对层级，清单单列。原builder文本不改，因此如要从源码重建全部中间版本，
须先在独立工作副本恢复这些缺失相对路径；不能据此声称仓库原目录已直接全部可重建。

本次只复制和对比所列文件，不使用hash或全目录扫描，没有git add/commit/push、切分支、
服务操作或NPU任务。旧基线只检查已知依赖路径是否存在，未修改。

原有CPU结果是证据快照，不在复制时重跑。需要复核时从stage_driver目录手动执行：

```powershell
python -X utf8 -B .\\check_executor_driver_wire_cpu.py
python -X utf8 -B .\\check_integrated_first_token.py
```

这些检查仍是CPU/fixture，不是GLM/NPU候选验收。remote-safe-helpers脚本包含只读137入口，
复用时仍须当前授权。部署脚本和native文件仅归档，不代表执行授权。
"""


def main():
    if DEST.exists():
        raise RuntimeError("new archive target already exists; do not overwrite it")
    if not DEST.resolve().is_relative_to(REPO.resolve()):
        raise RuntimeError("archive target leaves repository")
    unified = json.loads((HERE / "unified_overlay_manifest.json").read_text(encoding="utf-8"))
    assert len(unified["files"]) == 14
    selected = [STAGE + "runtime_overlay/" + item["relative_path"] for item in unified["files"]]
    selected += [STAGE + name for name in STAGE_FILES]
    selected += [BRIDGE + name for name in BRIDGE_FILES]
    selected += SUPPORT_FILES
    if len(selected) != len(set(selected)):
        raise RuntimeError("duplicate selected file")
    for relative in selected:
        path = SOURCE / relative
        if not path.is_file(): raise FileNotFoundError(path)
        if not path.resolve().is_relative_to(SOURCE.resolve()): raise RuntimeError(relative)
        if path.suffix == ".log" or any(part in {"data", "venv", ".ssh", "__pycache__"} for part in path.parts):
            raise RuntimeError("excluded archive content: " + relative)
    missing = [relative for relative in EXISTING if not (REPO / relative).is_file()]
    sources = {relative: SOURCE / relative for relative in selected}
    workspace = SOURCE.parents[2]
    for relative in missing:
        source = workspace / relative
        if not source.is_file() or not source.resolve().is_relative_to(workspace.resolve()):
            raise FileNotFoundError(source)
        sources["dependency_inputs/" + relative] = source
    # Take one bounded source snapshot and then compare only the selected copies.
    originals = {relative: source.read_bytes() for relative, source in sources.items()}
    DEST.mkdir(parents=True)
    rows = []
    for relative, content in originals.items():
        target = DEST / relative
        if not target.resolve().is_relative_to(DEST.resolve()): raise RuntimeError(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        rows.append({"relative_path": relative, "source": str(sources[relative]), "bytes": len(content)})
    equal = []
    for relative, content in originals.items():
        if (DEST / relative).read_bytes() != content or sources[relative].read_bytes() != content:
            raise RuntimeError("selected source changed or copy differs: " + relative)
        equal.append(relative)
    manifest = {"status": "LOCAL_CODE_SNAPSHOT_EXACT_SELECTED_FILES_NO_GIT_ACTIONS",
        "created_utc": datetime.now(timezone.utc).isoformat(), "source_root": str(SOURCE),
        "archive_root": str(DEST), "copied_files": rows,
        "existing_repository_inputs_checked_read_only": [name for name in EXISTING if name not in missing],
        "old_inputs_kept_only_under_new_dependency_inputs": missing,
        "excluded": ["customer prompt/data", "model files", "venv", "credentials/keys", "runtime logs"],
        "git_actions": [], "service_actions": [], "npu_actions": 0}
    validation = {"status": "EXACT_SELECTED_BYTES_AND_SOURCE_STABILITY_CONFIRMED",
        "selected_source_files": len(equal), "runtime_overlay_modules": 14,
        "selected_total_bytes": sum(map(len, originals.values())), "hashes_used": False,
        "directory_walk_used": False, "tests_rerun": False, "pre_existing_files_modified": 0}
    (DEST / "ARCHIVE_README.md").write_text(README, encoding="utf-8", newline="\n")
    (DEST / "COPY_MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (DEST / "COPY_VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"directory": str(DEST), **validation}, ensure_ascii=False))


if __name__ == "__main__": main()
