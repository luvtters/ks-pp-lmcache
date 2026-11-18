"""Local startup rendezvous for the paired Bifrost TP16 / LMCache deployment."""
from contextlib import ExitStack, contextmanager
import math
import os
import select
import socket
import struct
import time

from lmcache.logging import init_logger

logger = init_logger(__name__)
COUNT = 16
RECORD = struct.Struct("!4sIIIIQQ")


def _config():
    port, first = map(int, os.environ["LMCACHE_BIFROST_ORDER"].split(":"))
    timeout = float(os.environ.get("LMCACHE_BIFROST_WAIT_S", "1800"))
    if not (0 < port <= 65535 and first in (0, 16) and math.isfinite(timeout) and timeout > 0):
        raise ValueError("Expected LMCACHE_BIFROST_ORDER=mq_port:0 or mq_port:16 and positive timeout")
    return port, first, time.monotonic() + timeout


def _addr(port, suffix):
    # Linux abstract addresses disappear with their owning processes.
    return f"\0lmcache.bifrost.v1.{os.geteuid()}.{port}.{suffix}"


def _remaining(deadline):
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("Timed out waiting for the paired local Bifrost workers and cache server")
    return left


def _peer(sock):
    pid, uid, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    if pid <= 0 or uid != os.geteuid():
        raise RuntimeError("Expected the paired service in the same PID namespace and UID")
    return pid


def _recv(sock, size, deadline):
    data = bytearray()
    while len(data) < size:
        sock.settimeout(_remaining(deadline))
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("Paired process exited before startup completed")
        data.extend(chunk)
    return bytes(data)


def _pool_exists(port):
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as probe:
        try:
            probe.connect(_addr(port, "pool"))
        except ConnectionRefusedError:
            return False
        return True


def wait_for_pool(rank, local_rank, layers, slots, instance_id):
    port, first, deadline = _config()
    if not (0 <= local_rank < COUNT and rank == first + local_rank and layers > 0 and slots == 2):
        raise ValueError("Bifrost rank, completed mirrors or slots do not match local TP16")
    stamp = time.monotonic_ns()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(_addr(port, local_rank))
        listener.listen(1)
        if _pool_exists(port):
            raise RuntimeError("An earlier cache already allocated its pool; this new model requires a new cache")
        listener.settimeout(_remaining(deadline))
        logger.info("BIFROST_POOL_ORDER mirror_ready rank=%d pid=%d layers=%d mirror_ns=%d",
                    rank, os.getpid(), layers, stamp)
        conn, _ = listener.accept()
        with conn:
            cache_pid = _peer(conn)
            conn.settimeout(_remaining(deadline))
            # The frozen VllmConfig assigns time_ns() once, before spawning workers.
            conn.sendall(RECORD.pack(b"BFO1", rank, local_rank, layers, slots, int(instance_id), stamp))
            if _recv(conn, 1, deadline) != b"R":
                raise RuntimeError("Paired cache did not confirm pool readiness")
            logger.info("BIFROST_POOL_ORDER worker_continue rank=%d pid=%d cache_pid=%d",
                        rank, os.getpid(), cache_pid)


def _check_connected(connections):
    if select.select(connections, [], [], 0)[0]:
        raise RuntimeError("A mirror worker disconnected or sent unexpected data")


@contextmanager
def paired_workers(mq_port, size):
    port, first, deadline = _config()
    if port != mq_port or size <= 0:
        raise ValueError("Bifrost order must match the local LMCache MQ port and configured eager L1")
    with ExitStack() as lifetime, ExitStack() as pending:
        # One cache per local endpoint; retain pool ownership until this service exits.
        owner = lifetime.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM))
        owner.bind(_addr(port, "owner"))
        logger.info("BIFROST_POOL_ORDER waiting cache_pid=%d mq_port=%d first=%d workers=%d",
                    os.getpid(), port, first, COUNT)
        connections, pids = [], []
        instance_id = None
        latest_mirror = 0
        for index in range(COUNT):
            conn = pending.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM))
            while True:
                conn.settimeout(_remaining(deadline))
                try:
                    conn.connect(_addr(port, index))
                    break
                except ConnectionRefusedError:
                    _check_connected(connections)
                    time.sleep(min(0.1, _remaining(deadline)))
            pid = _peer(conn)
            magic, rank, local, layers, slots, model, stamp = RECORD.unpack(_recv(conn, RECORD.size, deadline))
            if (magic != b"BFO1" or local != index or rank != first + index or not layers
                    or slots != 2 or pid in pids or not model or (instance_id is not None and model != instance_id)):
                raise RuntimeError("Local Bifrost mirror workers do not form one live TP16 cohort")
            connections.append(conn)
            pids.append(pid)
            instance_id = model
            latest_mirror = max(latest_mirror, stamp)
        _check_connected(connections)
        pool = lifetime.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM))
        pool.bind(_addr(port, "pool"))
        logger.info("BIFROST_POOL_ORDER before_pool cache_pid=%d model_instance=%d first=%d workers=%s "
                    "latest_mirror_ns=%d before_pool_ns=%d configured_bytes=%d",
                    os.getpid(), instance_id, first, pids, latest_mirror, time.monotonic_ns(), size)

        def release(status):
            l1 = status["storage_manager"]["l1_manager"]
            if not (status["is_healthy"] and l1["is_healthy"] and
                    l1["memory_configured_bytes"] == l1["memory_total_bytes"] == size):
                raise RuntimeError("Paired cache did not back the complete configured L1 pool")
            _check_connected(connections)
            logger.info("BIFROST_POOL_ORDER pool_ready cache_pid=%d bytes=%d pool_ready_ns=%d",
                        os.getpid(), size, time.monotonic_ns())
            for conn in connections:
                conn.settimeout(_remaining(deadline))
                conn.sendall(b"R")
            pending.close()
        yield release
