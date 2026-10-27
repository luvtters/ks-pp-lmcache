"""Rebase notification wiring on actual common17; copy only LMCache modules."""
from __future__ import annotations

import ast
import difflib
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
BRIDGE = HERE.parent
SOURCE = BRIDGE.parent / "完整模型单机P对照/runtime/local-completion/lmcache_mp_connector_multi17.py"
DEST = HERE / "runtime_overlay/lmcache/integration/vllm"


def build():
    spec = importlib.util.spec_from_file_location("ks_connector_builder_reuse", BRIDGE / "build_connector_overlay.py")
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    original = SOURCE.read_text(encoding="utf-8")
    candidate = builder.change_once(original, "# Standard\n", "# Standard\nfrom lmcache.integration.vllm import ks_restore_runtime as _ks_restore_runtime\n")
    cls = next(node for node in ast.parse(candidate).body if isinstance(node, ast.ClassDef) and node.name == "LMCacheMPConnector")
    init = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "__init__")
    lines = candidate.splitlines(keepends=True)
    lines.insert(init.end_lineno, "\n        _ks_restore_runtime.after_connector_init(self, vllm_config, parallel_strategy, role)\n")
    candidate = "".join(lines)
    candidate = builder.change_in_method(candidate, "build_connector_meta",
        "        metadata = LMCacheMPConnectorMetadata()\n",
        "        _ks_restore_runtime.require_scheduler_ready(self)\n        metadata = LMCacheMPConnectorMetadata()\n")
    candidate = builder.change_once(candidate,
        "        return metadata\n\n    def update_connector_output(",
        "        _ks_restore_runtime.stamp_connector_metadata(self, metadata)\n        return metadata\n\n    def update_connector_output(")
    candidate = builder.change_in_method(candidate, "start_load_kv",
        "        assert isinstance(metadata, LMCacheMPConnectorMetadata)\n\n        request_ids = []\n",
        "        assert isinstance(metadata, LMCacheMPConnectorMetadata)\n        _ks_restore_runtime.before_start_load(self, metadata)\n\n        request_ids = []\n")
    candidate = builder.change_once(candidate,
        '        if hasattr(self, "worker_adapter"):\n            self.worker_adapter.shutdown()\n',
        '        _ks_restore_runtime.before_connector_shutdown(self)\n        if hasattr(self, "worker_adapter"):\n            self.worker_adapter.shutdown()\n')
    ast.parse(candidate)
    def method_bodies(text):
        cls = next(node for node in ast.parse(text).body if isinstance(node, ast.ClassDef) and node.name == "LMCacheMPConnector")
        return {node.name: ast.dump(node, include_attributes=False) for node in cls.body if isinstance(node, ast.FunctionDef)}
    before, after = method_bodies(original), method_bodies(candidate)
    intentionally_changed = {"__init__", "build_connector_meta", "start_load_kv", "shutdown"}
    unchanged = [name for name in before if name not in intentionally_changed]
    assert all(before[name] == after[name] for name in unchanged), "common17 method was unexpectedly changed"
    DEST.mkdir(parents=True, exist_ok=True)
    (DEST / "lmcache_mp_connector.py").write_text(candidate, encoding="utf-8", newline="\n")
    (DEST / "lmcache_mp_connector_multi17.py").write_text(candidate, encoding="utf-8", newline="\n")
    for source, target in ((BRIDGE / "restore_metadata_bridge.py", "ks_restore_metadata_bridge.py"),
                           (BRIDGE / "ks_restore_runtime.py", "ks_restore_runtime.py")):
        (DEST / target).write_text(source.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    delta = "".join(difflib.unified_diff(original.splitlines(True), candidate.splitlines(True),
        fromfile="a/lmcache/integration/vllm/lmcache_mp_connector_multi17.py",
        tofile="b/lmcache/integration/vllm/lmcache_mp_connector_multi17.py"))
    (HERE / "common17_notification_rebase.patch").write_text(delta, encoding="utf-8", newline="\n")
    report = {"status": "COMMON17_NOTIFICATION_REBASED_NOT_INSTALLED", "source": str(SOURCE),
              "candidate": str(DEST / "lmcache_mp_connector_multi17.py"),
              "common_methods_unchanged": unchanged,
              "source_equal_old_archived_connector_after_newline_normalization": original == builder.SOURCE.read_text(encoding="utf-8"),
              "observer_adapter_dependency": str(builder.PROTO / "lmcache_stage_observer/vllm_multi_process_adapter.py"),
              "installed_source_changes": 0, "npu_actions": 0}
    (HERE / "common17_notification_rebase.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "candidate": report["candidate"], "unchanged_methods": len(unchanged)}, ensure_ascii=False))


if __name__ == "__main__":
    build()
