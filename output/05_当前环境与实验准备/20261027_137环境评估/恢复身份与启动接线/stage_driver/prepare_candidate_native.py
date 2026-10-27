"""Prepare two inactive candidate startup files; keep actual baseline argv exact."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import difflib
import gzip
import json
from pathlib import Path
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
COMMON = HERE.parents[1] / "完整模型单机P对照/p-local-first-token.serve.native.sh"
ROOT = "/opt/ks-stage-ready-luv-20261027/standalone-p"
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-i",
       str(Path.home() / ".ssh/id_ed25519"), "root@115.120.85.137"]
ENV = '''#!/usr/bin/env bash
# Prepared candidate environment only. Source common, local-completion, then this.
# Native JSON keeps ks_local_p_complete=false and ks_local_p_first_token_only=true.
export KS_PP_STAGE_DRIVER=1
export KS_PP_STAGE_EVENTS=1
export KS_CORE_STAGE_EVENT_PUMP=1
export KS_LMCACHE_STAGE_OBSERVER=1
export KS_STAGE_TRACE_DIR=/opt/ks-stage-ready-luv-20261027/standalone-p/results/stage-candidate-trace
export PYTHONPATH="/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher:$PYTHONPATH"
'''

REMOTE = '''import base64, difflib, gzip, json, pathlib, shlex, subprocess, sys
root = pathlib.Path("/opt/ks-stage-ready-luv-20261027/standalone-p")
payload = json.loads(gzip.decompress(base64.b64decode(sys.stdin.buffer.read())))
original_path = root / "startup/p-local-first-token.serve.native.sh"
original = original_path.read_text(encoding="utf-8")
def argv(text):
    return shlex.split(text.replace(chr(92) + chr(10), ""), comments=True)
before = argv(original)
if before != payload["expected_baseline_argv"]:
    raise RuntimeError("actual baseline native argv changed; preserve it and refresh local preparation")
kv = json.loads(before[before.index("--kv-transfer-config") + 1])
extra = kv["kv_connector_extra_config"]
if extra.get("ks_local_p_complete") is not False or extra.get("ks_local_p_first_token_only") is not True:
    raise RuntimeError("actual baseline does not have the required first-token/no-extra-feedback flags")
candidate = original.replace("# Source common.env.native.sh, then local-completion.env.native.sh. Full GLM; original producer compute branches.",
    "# Source common.env.native.sh, local-completion.env.native.sh, then stage-candidate.env.native.sh. Prepared only.")
if argv(candidate) != before:
    raise RuntimeError("candidate must preserve every baseline serve argument")
targets = {"startup/stage-candidate.env.native.sh": payload["candidate_env"],
           "startup/p-stage-first-token.serve.native.sh": candidate}
for relative, text in targets.items():
    target = root / relative
    if not target.resolve().is_relative_to(root.resolve()):
        raise RuntimeError("native target would leave the private project")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    result = subprocess.run(["bash", "-n", str(target)], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr)
report = {"status": "PASS_PRIVATE_CANDIDATE_NATIVE_PREPARED_NOT_STARTED",
          "baseline_native": str(original_path), "candidate_files": [str(root / path) for path in targets],
          "argv_equal": True, "baseline_argv": before, "candidate_argv": argv(candidate),
          "extra_config": {"ks_local_p_complete": False, "ks_local_p_first_token_only": True},
          "baseline_native_content": original, "candidate_native_content": candidate,
          "candidate_environment_content": payload["candidate_env"],
          "shell_syntax_files_passed": 2, "candidate_enabled": False,
          "service_actions": 0, "npu_actions": 0}
(root / "runtime/stage-candidate/candidate_native_preparation.json").write_text(json.dumps(report, indent=2) + "\\n", encoding="utf-8")
print(json.dumps(report))
'''


def main():
    original = COMMON.read_text(encoding="utf-8")
    before = shlex.split(original.replace(chr(92) + chr(10), ""), comments=True)
    packed = gzip.compress(json.dumps({"expected_baseline_argv": before,
        "candidate_env": ENV}).encode("utf-8"), compresslevel=9)
    command = "docker exec -i ks-pp-m137-priv python -S -c " + shlex.quote(REMOTE)
    result = subprocess.run(SSH + [command], input=base64.b64encode(packed), capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace") + result.stdout.decode("utf-8", errors="replace"))
    report = json.loads(result.stdout)
    report.update(prepared_utc=datetime.now(timezone.utc).isoformat(), compressed_payload_bytes=len(packed))
    target = HERE / "native_candidate"
    target.mkdir(exist_ok=True)
    (target / "stage-candidate.env.native.sh").write_text(ENV, encoding="utf-8", newline="\n")
    (target / "p-stage-first-token.serve.native.sh").write_text(report["candidate_native_content"], encoding="utf-8", newline="\n")
    delta = "".join(difflib.unified_diff(report["baseline_native_content"].splitlines(True),
        report["candidate_native_content"].splitlines(True), fromfile="baseline/p-local-first-token.serve.native.sh",
        tofile="candidate/p-stage-first-token.serve.native.sh"))
    (target / "native.diff").write_text(delta, encoding="utf-8")
    (target / "candidate_native_preparation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("status", "candidate_files", "argv_equal", "extra_config", "shell_syntax_files_passed", "candidate_enabled", "service_actions", "npu_actions")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
