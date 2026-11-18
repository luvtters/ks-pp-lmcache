# SPDX-License-Identifier: Apache-2.0
"""CPU-only checks of deployment, refusal, transaction recovery and rollback."""

# Standard
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("customer_deploy", HERE / "deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeployTest(unittest.TestCase):
    """Exercise the shipped installer against real pre-patch source files."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.baseline = {}
        paths = deploy.re.findall(r"^\+\+\+ b/(.+)$", deploy.PATCH.read_text(), deploy.re.M)
        for name in paths:
            cls.baseline[name] = subprocess.check_output(
                ["git", "-C", str(HERE.parent), "show", "e8e2f1d7:" + name])

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.core = self.root / "core"
        self.ascend = self.root / "ascend"
        for name, data in self.baseline.items():
            path = self.core / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        (self.core / "lmcache/__init__.py").write_text("", encoding="utf-8")
        self.worker = self.ascend / "vllm_ascend/worker/worker.py"
        self.worker.parent.mkdir(parents=True)
        self.worker.write_text("class NPUWorker:\n    def initialize_from_config(self):\n"
                               "        ensure_kv_transfer_initialized(self.config)\n", encoding="utf-8")
        (self.ascend / "vllm_ascend/__init__.py").write_text("", encoding="utf-8")
        manager = self.ascend / "vllm_ascend/moe_offload/manager.py"
        manager.parent.mkdir()
        manager.write_text("# initial_folded_weights_finalized full_layer_expert_params\n"
                           "# rotating_moe_layer_count moe_slot_count\n", encoding="utf-8")
        self.state = self.root / "backups"
        self.before = self.contents()

    def contents(self) -> dict[Path, bytes]:
        """Read only the source files covered by this small deployment."""
        return {p: p.read_bytes() for p in
                [*[self.core / name for name in self.baseline], self.worker]}

    def command(self, action: str, *extra: str) -> subprocess.CompletedProcess[str]:
        """Invoke the same standalone command the customer will use."""
        return subprocess.run([sys.executable, "-B", str(HERE / "deploy.py"), action,
                               "--lmcache-root", str(self.core), "--ascend-root", str(self.ascend),
                               "--state-dir", str(self.state), *extra],
                              capture_output=True, text=True, timeout=30)

    def test_check_apply_reapply_and_rollback(self) -> None:
        checked = self.command("check")
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertEqual(self.contents(), self.before)
        applied = self.command("apply")
        self.assertEqual(applied.returncode, 0, applied.stderr)
        self.assertEqual(json.loads(applied.stdout)["status"], "APPLIED")
        self.assertIn("before_kv_init(self)", self.worker.read_text())
        second = self.command("apply")
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(json.loads(second.stdout)["lookup"], "ALREADY_APPLIED")
        restored = self.command("rollback")
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.contents(), self.before)

    def test_mismatched_source_is_not_changed(self) -> None:
        first = self.core / next(iter(self.baseline))
        first.write_text("# unrelated customer version\n", encoding="utf-8")
        before = self.contents()
        result = self.command("apply")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.contents(), before)
        self.assertFalse(self.state.exists())

    def test_rollback_refuses_later_customer_edit(self) -> None:
        result = self.command("apply")
        self.assertEqual(result.returncode, 0, result.stderr)
        with self.worker.open("a", encoding="utf-8") as stream:
            stream.write("\n# later customer edit\n")
        modified = self.contents()
        result = self.command("rollback")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.contents(), modified)

    def test_apply_error_restores_all_changed_files(self) -> None:
        args = deploy.argparse.Namespace(lmcache_root=str(self.core), ascend_root=str(self.ascend),
                                         only_lookup=False, state_dir=str(self.state))
        original_writer = deploy.write_file
        fail_once = True

        def writer(path: Path, data: bytes) -> None:
            nonlocal fail_once
            if path == self.worker and fail_once:
                fail_once = False
                raise OSError("simulated worker write failure")
            original_writer(path, data)

        with mock.patch.object(deploy, "write_file", side_effect=writer):
            with self.assertRaises(OSError):
                deploy.apply(args)
        self.assertEqual(self.contents(), self.before)
        self.assertFalse((self.state / "latest-backup.txt").exists())

    def test_live_service_refuses_source_edit(self) -> None:
        args = deploy.argparse.Namespace(lmcache_root=str(self.core), ascend_root=str(self.ascend),
                                         only_lookup=False, state_dir=str(self.state))
        with mock.patch.object(deploy, "service_pids", return_value=[12345]):
            with self.assertRaisesRegex(RuntimeError, "Stop the model"):
                deploy.apply(args)
        self.assertEqual(self.contents(), self.before)

    def test_lookup_only_leaves_ascend_unchanged(self) -> None:
        result = self.command("apply", "--only-lookup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.worker.read_bytes(), self.before[self.worker])
        result = self.command("rollback")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.contents(), self.before)

    def test_auto_detection_excludes_delivery_checkout(self) -> None:
        paths = [str(HERE.parent), str(self.core), *sys.path]
        with mock.patch.object(sys, "path", paths):
            self.assertEqual(deploy.source_root("lmcache", None), self.core.resolve())

    def test_zombie_worker_is_not_an_active_service(self) -> None:
        proc = self.root / "proc"
        sleeping, zombie = os.getpid() + 10000, os.getpid() + 10001
        for pid, state in ((sleeping, "S"), (zombie, "Z")):
            entry = proc / str(pid)
            entry.mkdir(parents=True)
            (entry / "stat").write_text(f"{pid} (VLLM::Worker) {state} 1\n")
            (entry / "cmdline").write_bytes(b"VLLM::Worker\0")
            (entry / "comm").write_text("VLLM::Worker\n")
        original_path = deploy.Path
        with mock.patch.object(deploy, "Path", side_effect=lambda p: proc if p == "/proc" else original_path(p)):
            self.assertEqual(deploy.service_pids(), [sleeping])


class GateTest(unittest.TestCase):
    """Validate configurable capacity and the PID-bound release receipt."""

    def test_open_checks_capacity_and_preserves_worker_identity(self) -> None:
        spec = importlib.util.spec_from_file_location("customer_gate", HERE / "ks_order_gate.py")
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        rows = [dict(rank=i, pid=1000+i, boottime_ns=0) for i in range(16)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            status = dict(is_healthy=True, storage_manager=dict(l1_manager=dict(
                is_healthy=True, memory_configured_bytes=128 * 1024**3,
                memory_total_bytes=128 * 1024**3)))
            opener = mock.Mock()
            opener.open.return_value = io.StringIO(json.dumps(status))
            with mock.patch.object(gate, "process_start", return_value=100), \
                 mock.patch.object(gate.Path, "read_bytes", return_value=b"/usr/bin/lmcache\0server\0"), \
                 mock.patch.object(gate.os, "sysconf", return_value=100, create=True), \
                 mock.patch.object(gate.urllib.request, "build_opener", return_value=opener):
                receipt = gate.open_gate(root, rows, 42, pool_gib=128)
                self.assertEqual(receipt["worker_pids"]["0"], 1000)
                self.assertEqual(receipt["cache_pid"], 42)
                self.assertEqual(receipt["memory_total_bytes"], 128 * 1024**3)
            (root / "cache-ready.json").unlink()
            opener.open.return_value = io.StringIO(json.dumps(status))
            with mock.patch.object(gate, "process_start", return_value=100), \
                 mock.patch.object(gate.Path, "read_bytes", return_value=b"/usr/bin/lmcache\0server\0"), \
                 mock.patch.object(gate.os, "sysconf", return_value=100, create=True), \
                 mock.patch.object(gate.urllib.request, "build_opener", return_value=opener):
                with self.assertRaisesRegex(RuntimeError, "not ready"):
                    gate.open_gate(root, rows, 42, pool_gib=480)
                self.assertFalse((root / "cache-ready.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
