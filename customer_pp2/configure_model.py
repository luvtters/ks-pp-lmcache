# SPDX-License-Identifier: Apache-2.0
"""Enable eager lookup in one original model script, preserving other arguments."""
import argparse
import copy
import json
from pathlib import Path
import re
import shutil
import subprocess
import time

HERE = Path(__file__).resolve().parent


def configured_script(text: str) -> str:
    """Change only eager_prefetch in the sole literal LMCache connector config."""
    matches = list(re.finditer(r"--kv-transfer-config\s+'([^']*)'", text, re.S))
    if len(matches) != 1:
        raise ValueError("Expected one single-quoted literal --kv-transfer-config JSON")
    match = matches[0]
    original = match.group(1)
    config = json.loads(original)
    children = config.get("kv_connector_extra_config", {}).get("connectors", [config])
    connectors = [c for c in children if c.get("kv_connector") == "LMCacheMPConnector"]
    if len(connectors) != 1:
        raise ValueError("Expected one LMCacheMPConnector")
    expected = copy.deepcopy(config)
    children = expected.get("kv_connector_extra_config", {}).get("connectors", [expected])
    child = next(c for c in children if c.get("kv_connector") == "LMCacheMPConnector")
    child.setdefault("kv_connector_extra_config", {})["lmcache.mp.eager_prefetch"] = True
    pattern = r'("lmcache\.mp\.eager_prefetch"\s*:\s*)(true|false)'
    if re.search(pattern, original):
        replacement, count = re.subn(pattern, r'\g<1>true', original)
    else:
        pattern = r'("lmcache\.mp\.autostart"\s*:\s*false)'
        replacement, count = re.subn(pattern, r'\g<1>, "lmcache.mp.eager_prefetch": true', original)
    if count != 1 or json.loads(replacement) != expected:
        raise ValueError("Cannot change eager_prefetch alone; no script changed")
    result = text[:match.start(1)] + replacement + text[match.end(1):]
    if "customer_pp2/enable.sh" not in result:
        call = list(re.finditer(r"(?m)^([ \t]*)(?:exec\s+)?vllm\s+serve\b", result))
        if len(call) != 1:
            raise ValueError("Expected one vllm serve line for the startup environment")
        position = call[0].start()
        enable = f'source "{(HERE / "enable.sh").as_posix()}" || exit 1\n'
        result = result[:position] + enable + result[position:]
    return result


def main() -> None:
    """Select one enabled original script, back it up and validate Bash syntax."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("script", type=Path, nargs="?")
    args = parser.parse_args()
    if args.script is None:
        candidates = []
        for path in Path("/opt/ks-luv-pp").glob("*.sh"):
            text = path.read_text(encoding="utf-8")
            if "customer_pp2/enable.sh" in text and "LMCacheMPConnector" in text and "vllm serve" in text:
                candidates.append(path)
        if len(candidates) != 1:
            raise SystemExit("Specify the original model script explicitly: " + str(candidates))
        args.script = candidates[0]
    path = args.script.resolve()
    original = path.read_bytes()
    text = original.decode("utf-8")
    updated = configured_script(text)
    bash = shutil.which("bash")
    if bash is None:
        raise SystemExit("bash is required for checking the original startup script")
    checked = subprocess.run([bash, "-n"], input=updated, capture_output=True, text=True)
    if checked.returncode:
        raise SystemExit(checked.stderr)
    if updated == text:
        print(json.dumps(dict(script=str(path), state="ALREADY_CONFIGURED")))
        return
    backup = path.with_name(path.name + f".pp2-full-backup-{time.time_ns()}")
    with backup.open("xb") as stream:
        stream.write(original)
    stage = path.with_name(path.name + f".pp2-full-{time.time_ns()}.tmp")
    try:
        with stage.open("xb") as stream:
            stream.write(updated.encode("utf-8"))
        stage.chmod(path.stat().st_mode & 0o7777)
        stage.replace(path)
    finally:
        stage.unlink(missing_ok=True)
    print(json.dumps(dict(script=str(path), state="CONFIGURED", backup=str(backup))))


if __name__ == "__main__":
    main()
