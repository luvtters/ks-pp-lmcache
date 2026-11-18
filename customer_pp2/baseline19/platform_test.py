# SPDX-License-Identifier: Apache-2.0
"""Targeted static checks using an external, private platform source fixture.

Usage: python platform_test.py <fixture containing vllm and vllm-ascend roots>
No third-party runtime is imported and no accelerator process is started.
"""

from pathlib import Path
import shutil
import sys
import tempfile

import platform_files as platform


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _snapshot(root: Path) -> dict[Path, bytes]:
    return {p.relative_to(root): p.read_bytes() for p in root.rglob("*.py")}


def _install(root: Path) -> dict[Path, bytes]:
    before = _snapshot(root)
    result = platform.platform_plan(root / "vllm", root / "vllm-ascend")
    _check(before == _snapshot(root), "The planner wrote to target source")
    for path, data in result.items():
        path.write_bytes(data)
    _check(not platform.platform_plan(root / "vllm", root / "vllm-ascend"),
           "The applied plan is not idempotent")
    return result


def _without_patch(root: Path, filename: str) -> None:
    for name, hunks in platform._entries(platform.PATCH_DIR / filename):
        folder = "vllm" if name.startswith("vllm/") else "vllm-ascend"
        path = root / folder / name
        text = path.read_text(encoding="utf-8")
        path.write_text(platform._apply(text, hunks, filename, reverse=True),
                        encoding="utf-8", newline="\n")


def main() -> None:
    """Check actual, original, previous FULL, repeat, and conflict cases."""
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    fixture = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="platform-baseline-") as name:
        base = Path(name)
        actual = base / "actual"
        shutil.copytree(fixture, actual)
        actual_plan = _install(actual)
        _check(len(actual_plan) == 1, "Expected only the old gate fallback removal")
        _check(next(iter(actual_plan)).name == "worker.py", "Unexpected actual-source change")
        expected = _snapshot(actual)

        # Reverse only our exact changes to model the customer's original
        # source; keep the pre-existing Bifrost/executor implementations.
        original = base / "original"
        shutil.copytree(fixture, original)
        for filename in ("ascend-admission-observer.patch", "model-wait-admission.patch",
                         "tail-scheduler-base.patch", "ascend-tail-20ms.patch", "eplb-startup.patch"):
            _without_patch(original, filename)
        worker = original / "vllm-ascend/vllm_ascend/worker/worker.py"
        worker.write_text(worker.read_text(encoding="utf-8").replace(platform._NATIVE_WITH_GATE, ""),
                          encoding="utf-8", newline="\n")
        serve = original / "vllm/vllm/entrypoints/cli/serve.py"
        local = "        from vllm.v1.executor.multiproc_executor import MultiprocExecutor\n\n"
        source = serve.read_text(encoding="utf-8")
        _check(source.count(local) == 1, "Missing measured local headless import")
        serve.write_text(source.replace(local, ""), encoding="utf-8", newline="\n")
        original_plan = _install(original)
        _check(_snapshot(original) == expected, "Original source did not reach measured behavior")

        previous = base / "previous-full"
        shutil.copytree(actual, previous)
        _without_patch(previous, "ascend-admission-observer.patch")
        worker = previous / "vllm-ascend/vllm_ascend/worker/worker.py"
        worker.write_text(worker.read_text(encoding="utf-8").replace(
            platform._NATIVE_ORDER, platform._OLD_GATE), encoding="utf-8", newline="\n")
        previous_plan = _install(previous)
        _check(_snapshot(previous) == expected, "Previous FULL+gate did not reach measured behavior")

        # Unrelated edits survive; changes to our hook are not overwritten.
        worker.write_text(worker.read_text(encoding="utf-8") + "\n# Customer local note.\n",
                          encoding="utf-8", newline="\n")
        _check(not platform.platform_plan(previous / "vllm", previous / "vllm-ascend"),
               "Unrelated customer edit was not preserved")
        worker.write_text(worker.read_text(encoding="utf-8").replace(
            "wait_for_pool(dist.get_rank(), self.local_rank, layers, manager.moe_slot_count,",
            "wait_for_pool(dist.get_rank(), self.local_rank, layers, 99,"),
            encoding="utf-8", newline="\n")
        before = _snapshot(previous)
        try:
            platform.platform_plan(previous / "vllm", previous / "vllm-ascend")
        except platform.PlatformCompatibilityError:
            pass
        else:
            raise RuntimeError("Modified startup hook was accepted")
        _check(before == _snapshot(previous), "Conflict rejection wrote source")

        shared = base / "site-packages"
        shutil.copytree(fixture / "vllm", shared, dirs_exist_ok=True)
        shutil.copytree(fixture / "vllm-ascend", shared, dirs_exist_ok=True)
        before = _snapshot(shared)
        shared_plan = platform.platform_plan(shared, shared)
        _check(before == _snapshot(shared), "Shared-root planning wrote source")
        _check(len(shared_plan) == 1, "Shared site-packages roots were not accepted")
        for path, data in shared_plan.items():
            path.write_bytes(data)
        _check(not platform.platform_plan(shared, shared), "Shared-root repeat was not empty")
        print("PASS actual, original, previous FULL+gate, repeat, unrelated edit, conflict, shared roots")
        print("Planned files:", len(actual_plan), len(original_plan), len(previous_plan))


if __name__ == "__main__":
    main()
