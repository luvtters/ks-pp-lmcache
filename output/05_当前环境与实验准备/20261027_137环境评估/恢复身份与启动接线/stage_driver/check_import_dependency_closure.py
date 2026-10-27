"""Audit actual helper imports; optionally load stdlib-only helpers on 137.

Never import framework packages, install stubs, or initialize devices. The remote
check executes actual helper source with the deployed finder, not full packages.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path
import shlex
import subprocess
import sys

HERE = Path(__file__).resolve().parent
OVERLAY = HERE / "runtime_overlay"
TARGET = "/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate"
SAFE_HELPERS = (
    "typed_stage_protocol",
    "span_stage_controller",
    "ks_stage_executor_driver",
    "vllm.v1.core.sched.ks_stage_reservation",
    "vllm.v1.engine.ks_restore_binding",
    "lmcache.integration.vllm.ks_restore_metadata_bridge",
    "lmcache.integration.vllm.ks_restore_runtime",
)


def static_audit():
    manifest = json.loads((HERE / "unified_overlay_manifest.json").read_text(encoding="utf-8"))
    mapping = {name: entry["relative_path"] for entry in manifest["files"] for name in entry["module_names"]}
    custom = lambda value: any(part.startswith(("ks_", "typed_stage", "span_stage", "lmcache_stage"))
                               for part in value.split("."))
    edges, dynamic, missing = [], [], []
    for entry in manifest["files"]:
        relative = entry["relative_path"]
        tree = ast.parse((OVERLAY / relative).read_text(encoding="utf-8"), filename=relative)
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Import):
                targets.extend(item.name for item in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    raise ValueError("relative imports require explicit resolution: " + relative)
                base = node.module or ""
                if custom(base): targets.append(base)
                targets.extend(base + "." + item.name for item in node.names if custom(item.name))
            for target in targets:
                if custom(target):
                    row = {"from": relative, "line": node.lineno, "module": target,
                           "mapped_to": mapping.get(target)}
                    edges.append(row)
                    if target not in mapping: missing.append(row)
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("import_module", "spec_from_file_location")):
                dynamic.append({"from": relative, "line": node.lineno,
                                "expression": ast.unparse(node)})
    adapter = ast.parse((OVERLAY / "lmcache/integration/vllm/vllm_multi_process_adapter.py").read_text(encoding="utf-8"))
    embedded = {node.name for node in adapter.body if isinstance(node, ast.ClassDef)}
    assert {"StageRestoreIdentity", "LMCacheStageObserver", "_Operation"} <= embedded
    assert not missing, missing
    return {"status": "PASS_AST_CUSTOM_HELPER_CLOSURE_NOT_FULL_PACKAGE_IMPORT",
            "source_files": len(manifest["files"]), "custom_import_edges": edges,
            "missing_custom_modules": missing, "dynamic_import_calls": dynamic,
            "observer_embedded": ["StageRestoreIdentity", "LMCacheStageObserver", "_Operation"],
            "old_controller_or_observer_modules_required": False,
            "safe_helper_load_plan": SAFE_HELPERS,
            "framework_module_imports": "NOT_EXECUTED", "npu_actions": 0}


REMOTE = r'''
import ast, importlib.util, json, pathlib, runpy, sys
root = pathlib.Path(TARGET)
manifest = json.loads((root / "private_candidate_manifest.json").read_text(encoding="utf-8"))
mapping = manifest["module_mappings"]
safe = SAFE_HELPERS
for name in safe:
    path = (root / mapping[name]).resolve()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            imports = [item.name for item in statement.names]
        elif isinstance(statement, ast.ImportFrom):
            imports = [statement.module or ""]
        else:
            imports = []
        for imported in imports:
            if imported not in safe and imported.split(".")[0] not in sys.stdlib_module_names:
                raise RuntimeError("helper is not import-safe without framework initialization: " + name + " -> " + imported)
namespace = runpy.run_path(str(root / "sitecustomize.py"))
finder = namespace["_KSStageCandidateFinder"]()
loaded = []
for name in safe:
    spec = finder.find_spec(name)
    if spec is None:
        raise ImportError("deployed finder has no real helper: " + name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    loaded.append({"module": name, "source": str(pathlib.Path(module.__file__).resolve())})
blocked_roots = ("torch", "torch_npu", "vllm_ascend")
forbidden = [name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in blocked_roots)]
framework_parents = [name for name in ("lmcache", "lmcache.integration", "lmcache.integration.vllm", "vllm", "vllm.v1", "vllm.v1.engine", "vllm.v1.core") if name in sys.modules]
if forbidden or framework_parents:
    raise RuntimeError("unexpected framework initialization: " + repr(forbidden + framework_parents))
print(json.dumps({"status": "PASS_REAL_DEPLOYED_HELPER_SOURCE_IMPORT_ONLY",
 "python_executable": sys.executable, "loaded": loaded,
 "framework_parent_packages_loaded": framework_parents,
 "torch_npu_or_ascend_loaded": forbidden,
 "full_package_imported": False, "device_functions_called": False,
 "method": "actual finder specs and actual source exec, canonical helper names, stdlib-only dependency order; no stubs or function fixtures",
 "npu_actions": 0}))
'''


def main():
    report = static_audit()
    if "--remote-safe-helpers" in sys.argv:
        remote_code = REMOTE.replace("TARGET", repr(TARGET)).replace("SAFE_HELPERS", repr(SAFE_HELPERS))
        command = "docker exec -i ks-pp-m137-priv /opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python -B -S -"
        result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-i",
            str(Path.home() / ".ssh/id_ed25519"), "root@115.120.85.137", command],
            input=remote_code.encode("utf-8"), capture_output=True, timeout=45)
        if result.returncode:
            report["safe_helper_import"] = {"status": "FAIL_REPORTED_NO_FALLBACK", "returncode": result.returncode,
                "stdout": result.stdout.decode("utf-8", errors="replace"), "stderr": result.stderr.decode("utf-8", errors="replace")}
        else:
            report["safe_helper_import"] = json.loads(result.stdout)
    (HERE / "import_dependency_closure.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    if report.get("safe_helper_import", {}).get("status", "").startswith("FAIL"):
        raise SystemExit(1)


if __name__ == "__main__": main()
