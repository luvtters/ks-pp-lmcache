"""Generate a private source overlay; never overwrite an installed package."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
OLD = OUT / "04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022"
SOURCE = OLD / "lmcache_only_followup/source_current/lmcache/integration/vllm/lmcache_mp_connector.py"
PROTO = OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
DEST = HERE / "overlay/lmcache/integration/vllm"

CORE_STARTUP = '''    def _ks_initialize_restore_notifications(self):
        # This hook registers facts only. No stage driver or model admission.
        if os.environ.get("KS_LMCACHE_STAGE_OBSERVER") != "1":
            return
        if (os.environ.get("KS_PP_STAGE_EVENTS") != "1"
                or os.environ.get("KS_CORE_STAGE_EVENT_PUMP") != "1"):
            raise RuntimeError("restore notification startup requires all three KS flags")
        pc = self.vllm_config.parallel_config
        if (pc.pipeline_parallel_size != 2
                or pc.prefill_context_parallel_size != 1
                or pc.nnodes_within_dp != 1
                or pc.data_parallel_size != 1
                or pc.world_size != 2 * pc.tensor_parallel_size):
            raise ValueError("restore startup supports single-node DP1/PP2/PCP1 only")
        from vllm.v1.engine.ks_stage_controller import StageController
        from vllm.v1.engine.ks_restore_binding import RestoreEventBinding
        from vllm.v1.executor.multiproc_executor import KSRestoreEvent
        from lmcache.integration.vllm.ks_restore_runtime import bind_scheduler_begin_restore
        tp = pc.tensor_parallel_size
        controller = StageController(
            (tuple(range(tp)), tuple(range(tp, 2 * tp))),
            max_inflight_batches=self.batch_queue_size,
            max_buffered_activations=self.batch_queue_size)
        binding = RestoreEventBinding(controller, KSRestoreEvent,
                                      tp_size=tp, pcp_size=pc.prefill_context_parallel_size)
        self.configure_ks_core_event_pump(binding)
        bind_scheduler_begin_restore(controller.begin_restore)
        self._ks_stage_controller = controller
        self._ks_restore_binding = binding
        self._ks_restore_notifications_only = True

'''


def change_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"reviewed source anchor is not unique: {old[:90]!r}")
    return text.replace(old, new, 1)


def change_in_method(text, name, old, new):
    tree = ast.parse(text)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LMCacheMPConnector")
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
    lines = text.splitlines(keepends=True)
    section = "".join(lines[method.lineno - 1:method.end_lineno])
    lines[method.lineno - 1:method.end_lineno] = [change_once(section, old, new)]
    return "".join(lines)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    candidate = change_once(original, "# Standard\n", "# Standard\nfrom lmcache.integration.vllm import ks_restore_runtime as _ks_restore_runtime\n")
    # The whole original constructor still runs, including its validation.
    tree = ast.parse(candidate)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LMCacheMPConnector")
    init = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "__init__")
    lines = candidate.splitlines(keepends=True)
    lines.insert(init.end_lineno, "\n        _ks_restore_runtime.after_connector_init(self, vllm_config, parallel_strategy, role)\n")
    candidate = "".join(lines)
    candidate = change_in_method(candidate, "build_connector_meta",
        "        metadata = LMCacheMPConnectorMetadata()\n",
        "        _ks_restore_runtime.require_scheduler_ready(self)\n        metadata = LMCacheMPConnectorMetadata()\n")
    candidate = change_once(candidate,
        "        return metadata\n\n    def update_connector_output(",
        "        _ks_restore_runtime.stamp_connector_metadata(self, metadata)\n        return metadata\n\n    def update_connector_output(")
    candidate = change_in_method(candidate, "start_load_kv",
        "        assert isinstance(metadata, LMCacheMPConnectorMetadata)\n\n        request_ids = []\n",
        "        assert isinstance(metadata, LMCacheMPConnectorMetadata)\n        _ks_restore_runtime.before_start_load(self, metadata)\n\n        request_ids = []\n")
    candidate = change_once(candidate,
        "        if hasattr(self, \"worker_adapter\"):\n            self.worker_adapter.shutdown()\n",
        "        _ks_restore_runtime.before_connector_shutdown(self)\n        if hasattr(self, \"worker_adapter\"):\n            self.worker_adapter.shutdown()\n")
    ast.parse(candidate)
    DEST.mkdir(parents=True, exist_ok=True)
    target = DEST / "lmcache_mp_connector.py"
    target.write_text(candidate, encoding="utf-8", newline="\n")
    for src, dst in (("restore_metadata_bridge.py", "ks_restore_metadata_bridge.py"),
                     ("ks_restore_runtime.py", "ks_restore_runtime.py")):
        text = (HERE / src).read_text(encoding="utf-8")
        ast.parse(text)
        (DEST / dst).write_text(text, encoding="utf-8", newline="\n")
    patch = "".join(difflib.unified_diff(original.splitlines(True), candidate.splitlines(True),
        fromfile="a/lmcache/integration/vllm/lmcache_mp_connector.py",
        tofile="b/lmcache/integration/vllm/lmcache_mp_connector.py"))
    (HERE / "connector_init.patch").write_text(patch, encoding="utf-8", newline="\n")

    executor = (PROTO / "draft_wait_followup/multiproc_executor.py").read_text(encoding="utf-8")
    hooked = change_once(executor,
        "        self.worker.ks_publish_restore_ready = self._ks_stage_publisher.publish_restore_ready\n",
        "        self.worker.ks_publish_restore_ready = self._ks_stage_publisher.publish_restore_ready\n"
        "        if os.environ.get(\"KS_LMCACHE_STAGE_OBSERVER\") == \"1\":\n"
        "            from lmcache.integration.vllm.ks_restore_runtime import register_worker_publisher\n"
        "            register_worker_publisher(self._ks_stage_publisher)\n")
    worker_patch = "".join(difflib.unified_diff(executor.splitlines(True), hooked.splitlines(True),
        fromfile="a/vllm/v1/executor/multiproc_executor.py", tofile="b/vllm/v1/executor/multiproc_executor.py"))
    (HERE / "required_worker_publisher.patch").write_text(worker_patch, encoding="utf-8", newline="\n")

    executor_dest = HERE / "overlay/vllm/v1/executor"
    executor_dest.mkdir(parents=True, exist_ok=True)
    (executor_dest / "multiproc_executor.py").write_text(hooked, encoding="utf-8", newline="\n")
    core = (PROTO / "draft_wait_followup/core.py").read_text(encoding="utf-8")
    tree = ast.parse(core)
    core_cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "EngineCore")
    core_init = next(node for node in core_cls.body if isinstance(node, ast.FunctionDef) and node.name == "__init__")
    core_lines = core.splitlines(keepends=True)
    core_lines.insert(core_init.end_lineno, "\n        self._ks_initialize_restore_notifications()\n")
    core_hooked = "".join(core_lines)
    core_hooked = change_once(core_hooked,
        '    @instrument(span_name="Prepare model")\n',
        CORE_STARTUP + '    @instrument(span_name="Prepare model")\n')
    ast.parse(core_hooked)
    engine_dest = HERE / "overlay/vllm/v1/engine"
    engine_dest.mkdir(parents=True, exist_ok=True)
    (engine_dest / "core.py").write_text(core_hooked, encoding="utf-8", newline="\n")
    for source, target_name in ((PROTO / "controller/stage_controller.py", "ks_stage_controller.py"),
                                (PROTO / "restore_event_binding/restore_binding.py", "ks_restore_binding.py")):
        text = source.read_text(encoding="utf-8")
        ast.parse(text)
        (engine_dest / target_name).write_text(text, encoding="utf-8", newline="\n")
    core_patch = "".join(difflib.unified_diff(core.splitlines(True), core_hooked.splitlines(True),
        fromfile="a/vllm/v1/engine/core.py", tofile="b/vllm/v1/engine/core.py"))
    (HERE / "startup_hooks.patch").write_text(core_patch + worker_patch, encoding="utf-8", newline="\n")
    report = {"status": "BUILT_PRIVATE_OVERLAY_NOT_INSTALLED", "source": str(SOURCE),
              "candidate": str(target), "archive_commit": "daa4d895b2afa63fc33b6b8182c8d50479ac25e3",
              "lmcache_upstream_commit": "NOT_VERIFIED", "worker_hook_applied": False,
              "core_restore_binding_installed": False,
              "generated_core_restore_binding": True,
              "generated_worker_publisher_hook": True,
              "normal_engine_core_started": False, "server_actions": 0, "npu_actions": 0,
              "changes": ["constructor after adapter initialization", "scheduler metadata registration/stamp", "worker metadata binding before retrieve", "guarded adapter shutdown"]}
    (HERE / "connector_overlay_build.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    build()
