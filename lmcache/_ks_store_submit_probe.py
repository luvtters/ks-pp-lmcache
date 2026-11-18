"""Optional writer host timings; disabled without KS_STORE_SUBMIT_DIR."""
import functools
import json
import os
import threading
import time

OUT = os.environ.get("KS_STORE_SUBMIT_DIR")
clock = time.perf_counter_ns
local = threading.local()


def batch(fn):
    if not OUT:
        return fn

    @functools.wraps(fn)
    def measured(self, request_ids, *args, **kwargs):
        if not self.is_kv_writer:
            return fn(self, request_ids, *args, **kwargs)
        if not hasattr(local, "log"):
            pid = os.getpid()
            local.log = open(os.path.join(OUT, f"store-submit-{pid}.jsonl"), "a",
                             encoding="utf-8")
            local.seq = 0
        local.seq += 1
        rows = []
        local.rows = rows
        wall = time.time_ns()
        start = clock()
        ok = False
        try:
            result = fn(self, request_ids, *args, **kwargs)
            ok = True
            return result
        finally:
            end = clock()
            local.rows = None
            data = dict(schema=1, pid=os.getpid(), rank=self.parallel_strategy.vllm_worker_id,
                        kv_rank=self.worker_id, seq=local.seq, wall_ns=wall,
                        start_ns=start, end_ns=end, wanted=len(request_ids), ok=ok, rows=rows)
            local.log.write(json.dumps(data, separators=(",", ":")) + "\n")
            local.log.flush()
    return measured


def request(fn):
    if not OUT:
        return fn

    @functools.wraps(fn)
    def measured(self, request_id, op, *args, **kwargs):
        rows = getattr(local, "rows", None)
        if rows is None:
            return fn(self, request_id, op, *args, **kwargs)
        row = [request_id, op.start, op.end, None, [0] * 8, None]
        rows.append(row)
        local.marks = row[4]
        row[3] = clock()
        try:
            return fn(self, request_id, op, *args, **kwargs)
        finally:
            row[5] = clock()
            local.marks = None
    return measured


def mark(i):
    marks = getattr(local, "marks", None)
    if marks is not None:
        marks[i] = clock()
