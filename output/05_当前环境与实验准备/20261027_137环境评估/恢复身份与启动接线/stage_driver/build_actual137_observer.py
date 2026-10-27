"""Apply only observer ownership edits to the captured actual137 adapter."""
from __future__ import annotations

import ast
import difflib
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
PROTO = OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
SOURCE = HERE / "adapter_rebase/actual137_vllm_multi_process_adapter.py"
DEST = HERE / "runtime_overlay/lmcache/integration/vllm/vllm_multi_process_adapter.py"


def once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"actual adapter anchor is not unique: {old[:100]!r}")
    return text.replace(old, new, 1)


def main():
    spec = importlib.util.spec_from_file_location("ks_observer_prior_builder", PROTO / "lmcache_stage_observer/build_lmcache_stage_observer.py")
    prior = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prior)
    original = SOURCE.read_text(encoding="utf-8")
    helper = (PROTO / "lmcache_stage_observer/stage_observer.py").read_text(encoding="utf-8")
    helper = helper[helper.index("@dataclass(frozen=True)"):]
    candidate = once(original, "class LMCacheMPWorkerAdapter:", helper + "\n\nclass LMCacheMPWorkerAdapter:")
    tree = ast.parse(candidate)
    worker = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LMCacheMPWorkerAdapter")
    method = next(node for node in worker.body if isinstance(node, ast.FunctionDef) and node.name == "submit_retrieve_request")
    assignment = next(node for node in ast.walk(method) if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "submit_retrieve")
    lines = candidate.splitlines(keepends=True)
    original_call = lines[assignment.lineno - 1:assignment.end_lineno]
    indent = " " * assignment.col_offset
    replacement = [indent + "_ks_operation = self._ks_reserve_stage_retrieve(request_id, op, event)\n",
                   indent + "try:\n"]
    replacement += ["    " + line for line in original_call]
    replacement += [indent + "except BaseException as _ks_error:\n",
                    indent + "    if _ks_operation is not None:\n",
                    indent + "        self._ks_stage_observer.submission_failed(_ks_operation, _ks_error)\n",
                    indent + "    raise\n",
                    indent + "if _ks_operation is not None:\n",
                    indent + "    future = self._ks_stage_observer.attach(_ks_operation, future)\n"]
    lines[assignment.lineno - 1:assignment.end_lineno] = replacement
    candidate = "".join(lines)
    candidate = once(candidate, "    @_lmcache_nvtx_annotate\n    def submit_retrieve_request(",
                     prior.HELPERS + "    @_lmcache_nvtx_annotate\n    def submit_retrieve_request(")
    clear = "            self.store_futures.clear()\n            self.retrieve_futures.clear()"
    if candidate.count(clear) != 2:
        raise ValueError("actual unhealthy getter boundaries changed")
    candidate = candidate.replace(clear, "            self._ks_invalidate_stage_retrieves()\n" + clear)
    candidate = once(candidate, "        self._kv_event_stop.set()",
        '        if _ks_observer := getattr(self, "_ks_stage_observer", None):\n'
        "            _ks_observer.assert_safe_adapter_shutdown()\n"
        "        self._kv_event_stop.set()")
    ast.parse(candidate)
    def bodies(text):
        return {f"{cls.name}.{node.name}": ast.dump(node, include_attributes=False)
                for cls in ast.parse(text).body if isinstance(cls, ast.ClassDef)
                for node in cls.body if isinstance(node, ast.FunctionDef)}
    before, after = bodies(original), bodies(candidate)
    allowed = {"LMCacheMPWorkerAdapter.submit_retrieve_request", "LMCacheMPWorkerAdapter.get_finished",
               "LMCacheMPWorkerAdapter.get_finished_with_lazy_offload", "LMCacheMPWorkerAdapter.shutdown"}
    assert all(before[name] == after[name] for name in before if name not in allowed)
    DEST.parent.mkdir(parents=True, exist_ok=True)
    DEST.write_text(candidate, encoding="utf-8", newline="\n")
    delta = "".join(difflib.unified_diff(original.splitlines(True), candidate.splitlines(True),
        fromfile="a/lmcache/integration/vllm/vllm_multi_process_adapter.py",
        tofile="b/lmcache/integration/vllm/vllm_multi_process_adapter.py"))
    (HERE / "adapter_rebase/actual137_observer.patch").write_text(delta, encoding="utf-8", newline="\n")
    report = {"status": "ACTUAL137_OBSERVER_REBASED_NOT_INSTALLED", "source": str(SOURCE),
              "candidate": str(DEST), "changed_existing_methods": sorted(allowed),
              "scheduler_lookup_methods_unchanged": True,
              "lmcache_upstream_commit": "NOT_VERIFIED", "installed_source_changes": 0, "npu_actions": 0}
    (HERE / "adapter_rebase/actual137_observer_build.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
