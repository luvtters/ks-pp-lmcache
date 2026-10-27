"""Keep partial source trees off import paths; deploy only the stdlib launcher."""
from __future__ import annotations
import base64
import gzip
import json
from pathlib import Path
import shlex
from deploy_private_candidate import LOADER, remote
from prepare_candidate_native import ENV

HERE = Path(__file__).resolve().parent
REMOTE = '''import ast, base64, gzip, importlib.machinery, json, pathlib, sys
root = pathlib.Path("/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate")
launcher = root.parent / "stage-launcher"
payload = json.loads(gzip.decompress(base64.b64decode(sys.stdin.buffer.read())))
old_loader = (root / "sitecustomize.py").read_text(encoding="utf-8")
ast.parse(payload["loader"])
old_vllm = importlib.machinery.PathFinder.find_spec("vllm", [str(root)])
old_v1 = importlib.machinery.PathFinder.find_spec("v1", [str(root / "vllm")])
old_attention = importlib.machinery.PathFinder.find_spec("attention", list(old_v1.submodule_search_locations)) if old_v1 else None
launcher.mkdir(parents=True, exist_ok=True)
(launcher / "sitecustomize.py").write_text(payload["loader"], encoding="utf-8")
(root / "sitecustomize.py").write_text(payload["loader"], encoding="utf-8")
environment = root.parents[1] / "startup/stage-candidate.env.native.sh"
environment.write_text(payload["environment"], encoding="utf-8")
namespace = {"__file__": str(launcher / "sitecustomize.py"), "__name__": "ks_launcher_path_check_only"}
sys.path.insert(0, str(root))
exec(compile(payload["loader"], str(launcher / "sitecustomize.py"), "exec"), namespace)
finder = namespace["_KSStageCandidateFinder"]()
if any(pathlib.Path(value or ".").resolve() == root for value in sys.path):
    raise RuntimeError("candidate source root remains on sys.path")
parents = ["vllm", "vllm.v1", "vllm.v1.attention", "lmcache", "lmcache.integration", "vllm_ascend"]
if any(finder.find_spec(name) is not None for name in parents):
    raise RuntimeError("candidate finder must not fabricate or replace parent packages")
if importlib.machinery.PathFinder.find_spec("vllm", [str(launcher)]) is not None:
    raise RuntimeError("launcher unexpectedly exposes a framework package")
origins = {}
for name, path in finder.modules.items():
    spec = finder.find_spec(name)
    if spec is None or not path.is_file() or pathlib.Path(spec.origin).resolve() != path:
        raise RuntimeError("exact module mapping failed: " + name)
    origins[name] = str(path)
forbidden = [name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in ("vllm", "vllm_ascend", "lmcache", "torch", "torch_npu"))]
if forbidden:
    raise RuntimeError("unexpected framework import: " + repr(forbidden))
report = {"status": "PASS_EXACT_FINDER_LAUNCHER_PARENT_PATH_FIX", "launcher": str(launcher),
          "candidate_root_absent_from_sys_path": True, "exact_mappings": len(origins),
          "parent_packages_untouched": parents,
          "old_partial_root_resolves_vllm_namespace": bool(old_vllm and old_vllm.loader is None),
          "old_partial_v1_attention_found": old_attention is not None,
          "original_framework_import_verification": "DEFERRED_TO_AUTHORIZED_ACTUAL_IMPORT",
          "old_loader_inserted_candidate_root": "sys.path.insert(0, str(_ROOT))" in old_loader,
          "framework_imports": forbidden, "full_package_imported": False,
          "service_actions": 0, "npu_actions": 0}
(root / "loader_parent_path_fix.json").write_text(json.dumps(report, indent=2) + "\\n", encoding="utf-8")
print(json.dumps(report))
'''

def main():
    packed = gzip.compress(json.dumps({"loader": LOADER, "environment": ENV}).encode())
    command = "docker exec -i ks-pp-m137-priv python -S -c " + shlex.quote(REMOTE)
    report = json.loads(remote(command, base64.b64encode(packed)))
    (HERE / "loader_parent_path_fix.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (HERE / "native_candidate/stage-candidate.env.native.sh").write_text(ENV, encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))

if __name__ == "__main__":
    main()
