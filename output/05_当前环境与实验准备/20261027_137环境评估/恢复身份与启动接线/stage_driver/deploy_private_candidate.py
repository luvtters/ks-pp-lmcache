"""Send only small gzip/base64 source payload to the owned container, inactive."""
from __future__ import annotations

import ast
import base64
import gzip
import json
from pathlib import Path
import shlex
import subprocess
import sys
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
OVERLAY = HERE / "runtime_overlay"
TARGET = "/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate"
CONTAINER = "ks-pp-m137-priv"
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-i",
       str(Path.home() / ".ssh/id_ed25519"), "root@115.120.85.137"]

LOADER = '''"""Inactive private candidate finder; no device/framework import here."""
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import sys

_ROOT = Path("/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate")
_MANIFEST = json.loads((_ROOT / "private_candidate_manifest.json").read_text(encoding="utf-8"))

class _KSStageCandidateFinder(importlib.abc.MetaPathFinder):
    root = _ROOT
    modules = {name: (_ROOT / relative).resolve()
               for name, relative in _MANIFEST["module_mappings"].items()}
    _mooncake_override = os.environ.get("KS_N16_MOONCAKE_OVERRIDE")
    if _mooncake_override:
        _mooncake_path = Path(_mooncake_override)
        if not _mooncake_path.is_absolute():
            raise ImportError("KS_N16_MOONCAKE_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector"] = _mooncake_path.resolve()
    _draft_override = os.environ.get("KS_N16_DRAFT_PROPOSER_OVERRIDE")
    if _draft_override:
        _draft_path = Path(_draft_override)
        if not _draft_path.is_absolute():
            raise ImportError("KS_N16_DRAFT_PROPOSER_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.spec_decode.llm_base_proposer"] = _draft_path.resolve()

    def find_spec(self, fullname, path=None, target=None):
        source = self.modules.get(fullname)
        if source is None:
            return None
        if not source.is_file():
            raise ImportError("private stage candidate source is missing: " + str(source))
        return importlib.util.spec_from_file_location(fullname, source)

# The source tree contains partial package directories. Keep it off sys.path;
# only this exact-module finder may expose its files (editable parents stay real).
sys.path[:] = [value for value in sys.path if Path(value or ".").resolve() != _ROOT]
sys.meta_path.insert(0, _KSStageCandidateFinder())
'''

VERIFY = '''"""python -S path/AST validation only; do not import model packages."""
import ast
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
manifest = json.loads((ROOT / "private_candidate_manifest.json").read_text(encoding="utf-8"))
for relative in manifest["source_files"]:
    source = ROOT / relative
    ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
namespace = {"__file__": str(ROOT / "sitecustomize.py"), "__name__": "ks_candidate_finder_verify_only"}
exec(compile((ROOT / "sitecustomize.py").read_text(encoding="utf-8"), str(ROOT / "sitecustomize.py"), "exec"), namespace)
finder = namespace["_KSStageCandidateFinder"]()
origins = {}
for name, relative in manifest["module_mappings"].items():
    expected = (ROOT / relative).resolve()
    if not expected.is_file():
        raise RuntimeError("missing mapped dependency: " + str(expected))
    spec = finder.find_spec(name)
    if spec is None or Path(spec.origin).resolve() != expected:
        raise RuntimeError("finder origin mismatch: " + name)
    origins[name] = str(expected)
forbidden = ("torch", "torch_npu", "vllm", "vllm_ascend", "lmcache")
imported = [name for name in sys.modules if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)]
if imported:
    raise RuntimeError("unexpected framework import: " + repr(imported))
report = {"status": "PASS_INACTIVE_PRIVATE_CANDIDATE_AST_AND_FINDER_ONLY",
          "root": str(ROOT), "source_files": len(manifest["source_files"]),
          "finder_mappings": len(origins), "origins": origins,
          "framework_imports": imported, "full_package_imported": False,
          "candidate_enabled": False, "service_actions": 0, "npu_actions": 0,
          "required_native_extra_config": {"ks_local_p_complete": False}}
(ROOT / "private_candidate_path_check.json").write_text(json.dumps(report, indent=2) + "\\n", encoding="utf-8")
print(json.dumps(report))
'''

REMOTE = '''import ast, base64, gzip, json, pathlib, sys
target = pathlib.Path("/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate")
payload = json.loads(gzip.decompress(base64.b64decode(sys.stdin.buffer.read())))
if payload["target"] != str(target):
    raise RuntimeError("unexpected deployment target")
resolved_root = target.resolve()
for relative, text in payload["files"].items():
    path = target / relative
    if not path.resolve().is_relative_to(resolved_root):
        raise RuntimeError("file would leave private candidate directory")
    if relative.endswith(".py"):
        ast.parse(text, filename=str(path))
for relative, text in payload["files"].items():
    path = target / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".ksluv.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
launcher = target.parent / "stage-launcher"
launcher.mkdir(parents=True, exist_ok=True)
(launcher / "sitecustomize.py").write_text(payload["files"]["sitecustomize.py"], encoding="utf-8")
namespace = {"__file__": str(target / "verify_candidate_paths.py"), "__name__": "__main__"}
exec(compile((target / "verify_candidate_paths.py").read_text(encoding="utf-8"), str(target / "verify_candidate_paths.py"), "exec"), namespace)
'''


def remote(command, payload=None):
    result = subprocess.run(SSH + [command], input=payload, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace") + result.stdout.decode("utf-8", errors="replace"))
    return result.stdout


def main():
    unified = json.loads((HERE / "unified_overlay_manifest.json").read_text(encoding="utf-8"))
    files, mappings = {}, {}
    for item in unified["files"]:
        relative = item["relative_path"]
        text = (OVERLAY / relative).read_text(encoding="utf-8")
        ast.parse(text, filename=relative)
        files[relative] = text
        mappings.update({name: relative for name in item["module_names"]})
    mappings.update({
        "lmcache.v1.platform.base.cache_context": "../common/lmcache_cache_context_pinned.py",
        "vllm_ascend.worker.model_runner_v1": "../local-completion/model_runner_local.py",
    })
    manifest = {"status": "PRIVATE_DEPLOYMENT_PREPARED_NOT_ENABLED", "target": TARGET,
        "pythonpath_launcher": str(Path(TARGET).parent).replace("\\", "/") + "/stage-launcher",
        "partial_source_root_on_sys_path": False,
        "source_files": sorted(files), "module_mappings": mappings,
        "required_native_extra_config": {"ks_local_p_complete": False},
        "candidate_environment_when_later_authorized": unified["flags"],
        "scope": unified["scope"], "observer_identity_rule": unified["observer_identity_rule"],
        "full_package_imported": False, "candidate_enabled": False,
        "prepared_utc": datetime.now(timezone.utc).isoformat()}
    files.update({"sitecustomize.py": LOADER, "verify_candidate_paths.py": VERIFY,
                  "private_candidate_manifest.json": json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"})
    inspected = json.loads(remote("docker inspect " + CONTAINER))[0]
    for mount in inspected.get("Mounts", []):
        destination = mount["Destination"].rstrip("/") or "/"
        if TARGET == destination or TARGET.startswith(destination.rstrip("/") + "/"):
            raise RuntimeError("target resolves into a container mount; do not deploy without ownership verification: " + destination)
    compressed = gzip.compress(json.dumps({"target": TARGET, "files": files}, ensure_ascii=False).encode("utf-8"), compresslevel=9)
    if len(compressed) > 250_000:
        raise RuntimeError("source-only compressed payload is unexpectedly large")
    command = "docker exec -i " + CONTAINER + " python -S -c " + shlex.quote(REMOTE)
    report = json.loads(remote(command, base64.b64encode(compressed)))
    report.update(compressed_bytes=len(compressed), transport="stdin gzip/base64 source JSON", host="115.120.85.137", container=CONTAINER)
    (HERE / "private_candidate_deployment.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("status", "source_files", "finder_mappings", "compressed_bytes", "candidate_enabled", "npu_actions")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
