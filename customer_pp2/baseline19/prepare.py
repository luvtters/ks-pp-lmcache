"""Generate native LMCache and vLLM launch scripts from this node's originals."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit


def one(pattern, text, what):
    hits = list(re.finditer(pattern, text, re.MULTILINE))
    if len(hits) != 1:
        raise ValueError(f"Expected one {what}, found {len(hits)}")
    return hits[0]


def transform(model, cache, out):
    model = re.sub(r'(?m)^\s*(?:source|\.)\s+[^\n]*?/customer_pp2/enable\.sh[^\n]*\n', '\n', model)
    stage = int(one(r'--node-rank\s+([01])\b', model, "node rank")[1])
    for key, val in (("tensor-parallel-size", "16"), ("pipeline-parallel-size", "2"),
                     ("nnodes", "2"), ("max-model-len", "990000")):
        one(r'--' + key + r'\s+' + val + r'\b', model, f"{key}={val}")
    if bool(re.search(r'--headless\b', model)) != bool(stage):
        raise ValueError("Expected the rank 1 model script to use --headless")
    extra = json.loads(one(r"--additional-config\s+'([^']*)'", model, "model additional config")[1])
    offload = extra.get("bifrost_config", {})
    if offload.get("weight_offload") is not True or offload.get("pin_memory") is not True or offload.get("slot_count") != 2:
        raise ValueError("Expected Bifrost weight_offload=true, pin_memory=true, slot_count=2")
    one(r'--supported-transfer-mode\s+lmcache_driven\b', cache, "LMCache-driven transfer")
    one(r'--http-port\s+\d+\b', cache, "HTTP cache service for native startup ordering")
    if re.search(r"--load-format(?:=|\s+)['\"]?dummy\b", model):
        raise ValueError("Selected a dummy-weight script; select the original customer model script")
    cfg_match = one(r"--kv-transfer-config\s+'([^']*)'", model, "literal connector JSON")
    cfg = json.loads(cfg_match[1])
    children = cfg.get("kv_connector_extra_config", {}).get("connectors", [])
    cores = [x for x in children if x.get("kv_connector") == "LMCacheMPConnector"]
    if len(cores) != 1 or not any(x.get("kv_connector") == "MooncakeConnectorV1" for x in children):
        raise ValueError("Expected the paired MooncakeConnectorV1 and LMCacheMPConnector")
    opts = cores[0]["kv_connector_extra_config"]
    port = int(one(r'(?<!\S)--port\s+(\d+)\b', cache, "cache RPC port")[1])
    urls = [urlsplit(s) for s in opts.get("lmcache.mp.server_urls", [])]
    if len(urls) != 2 or any(u.scheme != "tcp" for u in urls) or urls[stage].port != port:
        raise ValueError("Check the PP-ordered cache URLs against this cache RPC port")
    if (opts.get("lmcache.mp.autostart") is not False or opts.get("lmcache.mp.pp_query_fastpath") is not True
            or opts.get("lmcache.mp.nonblocking_lookup_status") is not True):
        raise ValueError("Use the two-server nonblocking PP fastpath model script with autostart=false")
    opts["lmcache.mp.eager_prefetch"] = True
    model = model[:cfg_match.start(1)] + json.dumps(cfg, ensure_ascii=False, indent=2) + model[cfg_match.end(1):]
    capacity = one(r'(--l1-size-gb\s+)(?:\d+|"?\$(?:LMCACHE_L1_GB|\{LMCACHE_L1_GB\})"?)(?=\s|$)',
                   cache, "L1 capacity")
    cache = cache[:capacity.start()] + capacity[1] + "480" + cache[capacity.end():]
    cache = cache.replace('LMCACHE_L1_GB="$(cat "$KS_LMC/cache-size-gib.txt")"', 'LMCACHE_L1_GB=480')
    if re.search(r'(?m)^\s*(?:exec\s+)?(?:bash\s+)?[^\s#]*gate\.sh\b', model + '\n' + cache):
        raise ValueError("Select the original service scripts without a separate gate launcher")
    common = (
        "unset KS_ORDER_GATE_DIR KS_LMCACHE_LOOKUP_TAIL_20MS LMCACHE_ASCEND_PP_STAGE_LOCAL KS_LMCACHE_L1_RECHECK_ON_STATUS\n"
        f"export LMCACHE_BIFROST_ORDER={port}:{stage * 16}\n"
        "export KS_LMCACHE_MP_READ_POLICY=transfer_borrow KS_LMCACHE_ADMIT_DURING_MODEL_WAIT=1\n"
        "export KS_L1_REGISTERED_MMAP=0 KS_WEIGHT_REGISTERED_MMAP=0 KS_LMCACHE_INLINE_LOOKUP_TRACE=1\n"
        f"export KS_STORE_ASYNC_EXPORT={stage}\n"
        f"export KS_STORE_SUBMIT_DIR={shlex.quote(str(out / 'store-submit'))}\n"
        f"export KS_ADMIT_DIAG_DIR={shlex.quote(str(out / 'admit'))}\n"
    )
    result = {}
    for name, body, cli, old_log in (("model", model, "vllm\\s+serve", "vllm"),
                                     ("cache", cache, "lmcache\\s+server", "lmcache")):
        call = one(r'^(?:[ \t]*)(?:exec[ \t]+)?' + cli + r'\b', body, name + " service command")
        tail = body[call.start():]
        log = re.search(r'[ \t]+(?:>>|>)[ \t]*[^\n;&|]+[ \t]+2>&1[ \t]*(?:\n[ \t]*\n?)*\Z', tail)
        if not log:
            raise ValueError(f"Expected final native {name} command to redirect to its log. Select a service script, not terminal output.")
        command = tail[:log.start()].lstrip()
        if not command.startswith("exec "):
            command = "exec " + command
        install = shlex.quote(str(Path(__file__).resolve().with_name("install.py")))
        checked = f"python -B {install} check --require-applied || exit 1\n"
        body = body[:call.start()] + common + checked + command + " >> " + shlex.quote(str(out / (old_log + ".log"))) + " 2>&1\n"
        subprocess.run([shutil.which("bash") or "bash", "-n"], input=body.encode(), check=True)
        result[name + ".sh"] = body
    return stage, port, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, help="original model script (auto-select when unambiguous)")
    parser.add_argument("--cache", type=Path, default=Path("/opt/ks-luv-pp/cache-foreground.sh"))
    parser.add_argument("--output", type=Path, default=Path("/opt/ks-luv-pp/pp2-lm480-19"))
    args = parser.parse_args()
    cache = args.cache.read_text(encoding="utf-8")
    out = args.output.resolve()
    if args.model:
        model = args.model.resolve()
    else:
        candidates = []
        for path in sorted(args.cache.resolve().parent.glob("*.sh")):
            text = path.read_text(encoding="utf-8", errors="replace")
            if 'LMCacheMPConnector' in text and re.search(r'(?m)^[ \t]*(?:exec\s+)?vllm\s+serve\b', text) and re.search(r'"weight_offload"\s*:\s*true', text):
                candidates.append(path)
        if len(candidates) == 1:
            model = candidates[0]
        elif sys.stdin.isatty():
            for i, path in enumerate(candidates, 1):
                print(f"{i}  {path}")
            choice = input("请输入本机原模型脚本的编号或完整路径: ").strip()
            if choice.isdecimal() and 1 <= int(choice) <= len(candidates):
                model = candidates[int(choice) - 1]
            elif Path(choice).is_absolute() and Path(choice).is_file():
                model = Path(choice).resolve()
            else:
                raise ValueError("Invalid selection or path")
        else:
            raise ValueError(f"Specify --model with the original's full path. Candidates: {candidates}")
    stage, port, scripts = transform(model.read_text(encoding="utf-8"), cache, out)
    if out.exists():
        if not all((out / name).is_file() and (out / name).read_text(encoding="utf-8") == body for name, body in scripts.items()):
            raise ValueError(f"Destination already contains a different setup: {out}. Choose a new --output.")
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=out.parent) as tmp:
            ready = Path(tmp) / "launch"
            ready.mkdir(mode=0o700)
            (ready / "store-submit").mkdir()
            (ready / "admit").mkdir()
            for name, body in scripts.items():
                (ready / name).write_bytes(body.encode())
                (ready / name).chmod(0o700)
            os.rename(ready, out)
    print(f"PREPARED PP{stage} L1=480 GiB ORDER={port}:{stage * 16} ASYNC_EXPORT={stage}")
    print(f"ORIGINALS {model} {args.cache}\nLAUNCH_DIR {out}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        sys.exit(f"STOP {exc}")
