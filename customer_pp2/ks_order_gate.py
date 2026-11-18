# SPDX-License-Identifier: Apache-2.0
"""One startup gate after weight mirrors are complete and before KV registration."""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.request


def process_start(pid):
    stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    if stat[0] == "Z":
        raise RuntimeError(f"Process {pid} has exited")
    return int(stat[19])


def write_once(path, row):
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    with tmp.open("x", encoding="utf8") as f:
        json.dump(row, f, ensure_ascii=False)
    try:
        os.link(tmp, path)
    finally:
        tmp.unlink()


def before_kv_init(worker):
    import torch.distributed as dist
    from vllm_ascend.moe_offload import WeightOffloadManager

    root = Path(os.environ["KS_ORDER_GATE_DIR"])
    rank = dist.get_rank()
    manager = WeightOffloadManager._instance
    if manager is None or not manager.initial_folded_weights_finalized:
        raise RuntimeError(f"rank {rank}: Bifrost initial mirrors and slot prime incomplete")
    mirrored = sorted(manager._parse_layer_idx(k) for k in manager.full_layer_expert_params)
    if len(mirrored) != manager.rotating_moe_layer_count or manager.moe_slot_count != 2:
        raise RuntimeError(f"rank {rank}: unexpected Bifrost mirror/slot count")
    record = dict(rank=rank, local_rank=worker.local_rank, pid=os.getpid(),
                  start_ticks=process_start(os.getpid()), mirror_layers=mirrored,
                  slots=manager.moe_slot_count, timestamp_ns=time.time_ns(),
                  boottime_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME), gate=str(root))
    path = root / f"rank-{rank:02d}.json"
    write_once(path, record)
    release = root / "cache-ready.json"
    # The coordinator owns the experiment window. A lost monitor must leave
    # already running workers intact while it reconnects to this same gate.
    while True:
        if release.is_file():
            receipt = json.loads(release.read_text(encoding="utf8"))
            if receipt["gate"] != str(root) or receipt["worker_pids"].get(str(rank)) != os.getpid():
                raise RuntimeError("Stale or incorrect release receipt")
            return
        time.sleep(0.5)


def rank_records(root, first):
    files = sorted(root.glob("rank-*.json"))
    expected = set(range(first, first + 16))
    rows = [json.loads(p.read_text(encoding="utf8")) for p in files]
    observed = {r["rank"] for r in rows}
    if observed != expected or len(rows) != 16:
        raise RuntimeError(f"markers {sorted(observed)}; expected {sorted(expected)}")
    if len({r["pid"] for r in rows}) != 16:
        raise RuntimeError("Non-unique worker PIDs")
    for row in rows:
        if row["gate"] != str(root) or process_start(row["pid"]) != row["start_ticks"]:
            raise RuntimeError(f"Process identity changed for rank {row['rank']}")
    return rows


def find_cache_pid():
    """Find one CLI cache server in the same PID namespace without signalling it."""
    matches = []
    for entry in Path('/proc').glob('[0-9]*'):
        try:
            argv = (entry / 'cmdline').read_bytes().split(b'\0')
            if (any(Path(os.fsdecode(x)).name == 'lmcache' for x in argv)
                    and b'server' in argv):
                process_start(int(entry.name))
                matches.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    if len(matches) != 1:
        raise RuntimeError(f'Expected one lmcache server, found {matches}; supply --cache-pid')
    return matches[0]


def open_gate(root, rows, cache_pid, pool_gib=480, status_url='http://127.0.0.1:15565/status'):
    start = process_start(cache_pid)
    argv = Path(f"/proc/{cache_pid}/cmdline").read_bytes().split(b"\0")
    if not any(Path(os.fsdecode(x)).name == "lmcache" for x in argv) or b"server" not in argv:
        raise RuntimeError("Expected the new lmcache server PID")
    ticks = os.sysconf("SC_CLK_TCK")
    if (start + 1) / ticks < max(r["boottime_ns"] for r in rows) / 1e9:
        raise RuntimeError("Cache process predates the local mirror completion records")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(status_url, timeout=5) as response:
        status = json.load(response)
    l1 = status["storage_manager"]["l1_manager"]
    size = pool_gib * 1024**3
    if not (status["is_healthy"] and l1["is_healthy"] and
            l1["memory_configured_bytes"] == l1["memory_total_bytes"] == size):
        raise RuntimeError("New native 480 GiB cache is not ready")
    if process_start(cache_pid) != start:
        raise RuntimeError("Cache process changed during status check")
    receipt = dict(gate=str(root), at_ns=time.time_ns(), cache_pid=cache_pid,
                   cache_start_ticks=start, memory_total_bytes=l1["memory_total_bytes"],
                   worker_pids={str(r["rank"]):r["pid"] for r in rows})
    path = root / "cache-ready.json"
    if path.exists():
        old = json.loads(path.read_text(encoding="utf8"))
        if any(old[k] != receipt[k] for k in ("gate", "cache_pid", "cache_start_ticks", "worker_pids")):
            raise RuntimeError("Existing release belongs to a different instance")
        return old
    write_once(path, receipt)
    return receipt


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=("check", "open"))
    ap.add_argument("directory", type=Path)
    ap.add_argument("first_rank", type=int, choices=(0, 16))
    ap.add_argument("--cache-pid", type=int)
    ap.add_argument("--pool-gib", type=int, default=480)
    ap.add_argument("--status-url", default='http://127.0.0.1:15565/status')
    args = ap.parse_args()
    records = rank_records(args.directory, args.first_rank)
    if args.command == "open":
        if args.pool_gib <= 0:
            ap.error('--pool-gib must be positive')
        print(json.dumps(open_gate(args.directory, records, args.cache_pid or find_cache_pid(),
                                   args.pool_gib, args.status_url)))
    else:
        print(json.dumps(records))
