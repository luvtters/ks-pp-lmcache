"""Opt-in deferred NPU producer-event export for the PP1 writer."""
from concurrent.futures import Future, ThreadPoolExecutor
import threading
import os
import json
import time


def trace(path, record):
    if not path:
        return
    try:
        with open(os.path.join(path, f"store-export-{os.getpid()}.jsonl"), "a") as out:
            out.write(json.dumps(record, separators=(",", ":")) + "\n")
    except OSError:
        pass


class DeferredHandle:
    def __init__(self, future, seq=0, directory=None):
        self.future = future
        self.seq = seq
        self.directory = directory
        self.sent = False

    def first_send(self, request_id, uid, parked, replies):
        if not self.sent:
            self.sent = True
            trace(self.directory, ["send", self.seq, time.monotonic_ns(),
                                   request_id, uid, parked, replies])


class ExportQueue:
    def __init__(self, device_module):
        self.module = device_module
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ks-store-export")
        self.lock = threading.Lock()
        self.last = None
        self.streams = {}
        self.closed = False
        self.seq = 0
        self.trace_dir = os.environ.get("KS_STORE_SUBMIT_DIR")

    def submit(self, event, backend, device):
        with self.lock:
            if self.closed:
                failure = Future()
                failure.set_exception(ConnectionError("STORE export queue is closed"))
                return DeferredHandle(failure)
            if self.last is not None:
                old_event, old_backend, old_device, old_handle = self.last
                if old_event is event and old_backend is backend and old_device == device:
                    return old_handle
            self.seq += 1
            fut = self.pool.submit(self._export, event, backend, device, self.seq, time.monotonic_ns())
            handle = DeferredHandle(fut, self.seq, self.trace_dir)
            self.last = (event, backend, device, handle)
            return handle

    def _export(self, event, backend, device, seq, submitted):
        started = time.monotonic_ns()
        # The NPU wait binding releases the GIL until the original producer
        # record is dequeued and enqueues a wait on our separate stream.
        # No work or waits are added to the producer's forward stream.
        with self.module.device(device):
            if device not in self.streams:
                self.streams[device] = self.module.Stream(device=device)
            event.wait(self.streams[device])
            waited = time.monotonic_ns()
            value = backend.export_event(event, device)
        end = time.monotonic_ns()
        if not isinstance(value, bytes) or not value:
            raise ValueError("NPU event export did not produce a valid IPC handle")
        trace(self.trace_dir, ["export", seq, submitted, started, waited, end])
        return value

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
            self.last = None
        self.pool.shutdown(wait=False, cancel_futures=True)
