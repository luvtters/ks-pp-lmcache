"""CPU checks for planning, supported baselines and non-destructive rejection."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from core_files import PATCHES, _entries, core_plan


ROOT = Path(__file__).resolve().parents[2]
NAMES = sorted({name for p in PATCHES for name, _, _ in _entries(p)})


class CorePlanTests(unittest.TestCase):
    def fixture(self, revision):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        root = Path(folder.name)
        for name in ["lmcache/__init__.py", *NAMES]:
            result = subprocess.run(["git", "show", f"{revision}:{name}"],
                                    cwd=ROOT, capture_output=True, check=False)
            if result.returncode == 0:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(result.stdout)
        return root

    def test_previous_full_and_e8_upgrade_without_writing_then_idempotent(self):
        for revision in ("ec90b941", "e8e2f1d7"):
            with self.subTest(revision=revision):
                root = self.fixture(revision)
                before = {p: p.read_bytes() for p in root.rglob("*.py")}
                planned = core_plan(root)
                self.assertTrue(planned)
                self.assertEqual(before, {p: p.read_bytes() for p in root.rglob("*.py")})
                for path, data in planned.items():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
                self.assertEqual(core_plan(root), {})
                for name in NAMES:
                    expected = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
                    self.assertEqual((root / name).read_bytes().replace(b"\r\n", b"\n"), expected)

    def test_unrelated_customer_edit_is_preserved(self):
        root = self.fixture("ec90b941")
        path = root / "lmcache/integration/vllm/vllm_multi_process_adapter.py"
        path.write_bytes(path.read_bytes() + b"\n# Customer-local comment.\n")
        self.assertTrue(core_plan(root)[path].endswith(b"# Customer-local comment.\n"))

    def test_changed_transfer_hunk_is_rejected_without_writing(self):
        root = self.fixture("ec90b941")
        path = root / "lmcache/integration/vllm/lmcache_mp_connector.py"
        source = path.read_bytes().replace(
            b"event = self.worker_adapter.create_recorded_event()",
            b"event = custom_event_factory()")
        path.write_bytes(source)
        with self.assertRaisesRegex(ValueError, "hunk differs"):
            core_plan(root)
        self.assertEqual(path.read_bytes(), source)
        self.assertFalse((root / "lmcache/_ks_store_export.py").exists())

    def test_unrecognized_existing_helper_is_rejected(self):
        root = self.fixture("ec90b941")
        helper = root / "lmcache/_ks_store_export.py"
        helper.write_bytes(b"# Independently modified helper.\n")
        with self.assertRaisesRegex(ValueError, "helper differs"):
            core_plan(root)
        self.assertEqual(helper.read_bytes(), b"# Independently modified helper.\n")

    def test_package_symlink_cannot_escape_target_root(self):
        root = self.fixture("ec90b941")
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "helper.py"
            external.write_bytes(b"# External file.\n")
            helper = root / "lmcache/_ks_store_export.py"
            try:
                helper.symlink_to(external)
            except OSError as error:
                self.skipTest(f"Symlinks unavailable: {error}")
            with self.assertRaisesRegex(ValueError, "escapes"):
                core_plan(root)
            self.assertEqual(external.read_bytes(), b"# External file.\n")

    def test_store_probe_is_identity_without_diagnostic_directory(self):
        body = (ROOT / "lmcache/_ks_store_submit_probe.py").read_bytes()
        namespace = {}
        with patch.dict(os.environ):
            os.environ.pop("KS_STORE_SUBMIT_DIR", None)
            exec(compile(body, "store_probe", "exec"), namespace)
        function = lambda: None
        self.assertIs(namespace["batch"](function), function)
        self.assertIs(namespace["request"](function), function)
        namespace["mark"](0)


if __name__ == "__main__":
    unittest.main()
