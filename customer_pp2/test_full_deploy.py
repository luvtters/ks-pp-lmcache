# SPDX-License-Identifier: Apache-2.0
"""Test full installation against disposable copies of the frozen tested sources."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("full_customer_deploy", HERE / "deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


@unittest.skipUnless(os.getenv("KS_PP2_FULL_TEST_ROOT"), "Frozen source fixture not supplied")
class FullDeployTest(unittest.TestCase):
    """Exercise real patch application, idempotence and rollback across three packages."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        frozen = Path(os.environ["KS_PP2_FULL_TEST_ROOT"])
        for name in ("vllm", "ascend"):
            shutil.copytree(frozen / name, self.root / name)
        self.core = self.root / "core"
        names = deploy.re.findall(r"^\+\+\+ b/(.+)$", deploy.PATCH.read_text(), deploy.re.M)
        for name in names:
            target = self.core / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(subprocess.check_output(
                ["git", "-C", str(HERE.parent), "show", "e8e2f1d7:" + name]))
        (self.core / "lmcache/__init__.py").write_text("", encoding="utf-8")
        self.state = self.root / "backups"
        self.before = self.contents()

    def contents(self) -> dict[str, bytes]:
        """Return only the temporary source files, excluding installation receipts."""
        return {str(p.relative_to(self.root)): p.read_bytes()
                for kind in ("core", "vllm", "ascend")
                for p in (self.root / kind).rglob("*.py")}

    def command(self, action: str, *extra: str) -> subprocess.CompletedProcess[str]:
        """Run the customer's actual full installer entrypoint."""
        return subprocess.run([sys.executable, "-B", str(HERE / "deploy.py"), action,
                               "--full", "--lmcache-root", str(self.core),
                               "--vllm-root", str(self.root / "vllm"),
                               "--ascend-root", str(self.root / "ascend"),
                               "--state-dir", str(self.state), *extra],
                              capture_output=True, text=True, timeout=45)

    def test_full_apply_reapply_require_and_rollback(self) -> None:
        checked = self.command("check")
        self.assertEqual(checked.returncode, 0, checked.stderr)
        row = json.loads(checked.stdout)
        self.assertEqual(len(row["full_patches"]), 4)
        self.assertEqual(row["delivery_scope"], "FULL_13725_2026")
        self.assertEqual(self.contents(), self.before)
        self.assertNotEqual(self.command("check", "--require-applied").returncode, 0)
        applied = self.command("apply")
        self.assertEqual(applied.returncode, 0, applied.stderr)
        installed = self.contents()
        checked = self.command("check", "--require-applied")
        self.assertEqual(checked.returncode, 0, checked.stderr)
        row = json.loads(checked.stdout)
        self.assertEqual(row["lookup"], "ALREADY_APPLIED")
        self.assertTrue(all(p["state"] == "ALREADY_APPLIED" for p in row["full_patches"]))
        self.assertEqual(self.command("apply").returncode, 0)
        self.assertEqual(self.contents(), installed)
        restored = self.command("rollback")
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.contents(), self.before)

    def test_current_partial_install_upgrades_without_reapplying_lookup(self) -> None:
        partial = subprocess.run([sys.executable, "-B", str(HERE / "deploy.py"), "apply",
                                  "--lmcache-root", str(self.core),
                                  "--ascend-root", str(self.root / "ascend"),
                                  "--state-dir", str(self.state)], capture_output=True, text=True)
        self.assertEqual(partial.returncode, 0, partial.stderr)
        before = {p: data for p, data in self.contents().items() if p.startswith("core")}
        full = self.command("apply")
        self.assertEqual(full.returncode, 0, full.stderr)
        after = {p: data for p, data in self.contents().items() if p.startswith("core")}
        self.assertEqual(before, after)

    def test_late_patch_mismatch_changes_no_sources(self) -> None:
        target = self.root / "ascend/vllm_ascend/patch/platform/patch_kv_delivery_preemption.py"
        target.write_text("# different customer source\n", encoding="utf-8")
        before = self.contents()
        checked = self.command("apply")
        self.assertNotEqual(checked.returncode, 0)
        self.assertIn("ascend_tail_20ms", checked.stderr)
        self.assertEqual(self.contents(), before)
        self.assertFalse(self.state.exists())

    def test_partial_full_failure_restores_all_packages(self) -> None:
        args = deploy.argparse.Namespace(lmcache_root=str(self.core),
                                         ascend_root=str(self.root / "ascend"),
                                         vllm_root=str(self.root / "vllm"), full=True,
                                         only_lookup=False, state_dir=str(self.state))
        original = deploy.git_apply

        def fail(root: Path, *options: str, patch: Path = deploy.PATCH):
            if patch.name == "ascend-tail-20ms.patch" and not options:
                return subprocess.CompletedProcess([], 1, "", "simulated late apply failure")
            return original(root, *options, patch=patch)

        with mock.patch.object(deploy, "git_apply", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "simulated late"):
                deploy.apply(args)
        self.assertEqual(self.contents(), self.before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
