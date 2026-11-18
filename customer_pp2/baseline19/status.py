"""Report this node's mirror/pool startup sequence and PP1 STORE trace."""
import argparse
import json
from pathlib import Path
import re
import sys


def latest_order(folder):
    before = ready = None
    with (folder / "lmcache.log").open(errors="replace") as log:
        for line in log:
            if "BIFROST_POOL_ORDER" not in line:
                continue
            fields = dict(re.findall(r"(\w+)=(\d+)", line))
            if "BIFROST_POOL_ORDER before_pool " in line:
                before, ready = fields, None
            if "BIFROST_POOL_ORDER pool_ready " in line and before and fields["cache_pid"] == before["cache_pid"]:
                ready = fields
    if not before or not ready:
        raise ValueError("WAIT no complete before_pool / pool_ready pair yet")
    pid = int(ready["cache_pid"])
    first = int(before["first"])
    mirror, continued = {}, {}
    with (folder / "vllm.log").open(errors="replace") as log:
        for line in log:
            if "BIFROST_POOL_ORDER" not in line:
                continue
            fields = dict(re.findall(r"(\w+)=(\d+)", line))
            if "BIFROST_POOL_ORDER mirror_ready " in line:
                mirror[(fields["rank"], fields["pid"])] = int(fields["mirror_ns"])
            elif "BIFROST_POOL_ORDER worker_continue " in line and fields["cache_pid"] == str(pid):
                continued[int(fields["rank"])] = fields["pid"]
    expected = set(range(first, first + 16))
    if first not in (0, 16) or set(continued) != expected:
        raise ValueError(f"WAIT cache_pid={pid} first_rank={first} continued={len(continued)}/16")
    ends = [mirror.get((str(rank), wp)) for rank, wp in continued.items()]
    if (None in ends or not max(ends) <= int(before["latest_mirror_ns"]) <= int(before["before_pool_ns"]) < int(ready["pool_ready_ns"])
            or int(before["configured_bytes"]) != int(ready["bytes"]) or int(ready["bytes"]) != 480 * 2**30):
        raise ValueError("CHECK_FAILED mirror/pool ordering or 480 GiB capacity")
    for process in [str(pid), *continued.values()]:
        stat = Path("/proc", process, "stat").read_text().rsplit(") ", 1)[1].split()
        if stat[0] in ("Z", "X"):
            raise ValueError(f"Process {process} has exited")
    enabled = int(first == 16)
    writer = continued[first]
    env = Path("/proc", writer, "environ").read_bytes().split(b"\0")
    if f"KS_STORE_ASYNC_EXPORT={enabled}".encode() not in env:
        raise ValueError(f"CHECK_FAILED PP writer {writer} async flag, expected {enabled}")
    return first, writer, pid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/opt/ks-luv-pp/pp2-lm480-19"))
    args = parser.parse_args()
    first, writer, pid = latest_order(args.output)
    print(f"ORDER_OK PP{first // 16} workers=16/16 pool=480GiB cache_pid={pid} writer_pid={writer}")
    if first == 16:
        exports = args.output / "store-submit" / f"store-export-{writer}.jsonl"
        seen = False
        if exports.is_file():
            with exports.open(errors="replace") as stream:
                for line in stream:
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(row, list) and row and row[0] == "export":
                        seen = True
                        break
        print("PP1_ASYNC_STORE", "OBSERVED" if seen else "AWAITING_FIRST_STORE")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as exc:
        sys.exit(str(exc))
