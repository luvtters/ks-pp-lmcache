# SPDX-License-Identifier: Apache-2.0
"""Plan the measured vLLM/Ascend platform changes without writing source files.

Only exact, unique source hunks are adapted. The installed Bifrost manager and
Ascend executor remain customer prerequisites; this module does not redistribute
either implementation. No accelerator package is imported by this planner.
"""

import ast
from pathlib import Path, PurePosixPath
import re


HERE = Path(__file__).resolve().parent
PATCH_DIR = HERE / "platform_patches"
PLATFORM_ENV = {
    "KS_LMCACHE_INLINE_LOOKUP_TRACE": "1",
    "KS_LMCACHE_ADMIT_DURING_MODEL_WAIT": "1",
    "KS_LMCACHE_LOOKUP_TAIL_20MS": "0",
}

_OLD_GATE = '''        from os import environ as _ks_order_environ
        if _ks_order_environ.get("KS_ORDER_GATE_DIR"):
            from ks_order_gate import before_kv_init
            before_kv_init(self)
'''

_NATIVE_ORDER = '''        from os import environ as _ks_order_environ
        if _ks_order_environ.get("LMCACHE_BIFROST_ORDER"):
            from lmcache.v1.multiprocess.bifrost_order import wait_for_pool
            from vllm_ascend.moe_offload import WeightOffloadManager
            import torch.distributed as dist

            manager = WeightOffloadManager._instance
            if manager is None or not manager.initial_folded_weights_finalized:
                raise RuntimeError("Initial Bifrost weight mirrors and slot priming must complete before KV setup")
            layers = len(manager.full_layer_expert_params)
            if layers != manager.rotating_moe_layer_count:
                raise RuntimeError("Incomplete Bifrost mirror group before KV setup")
            wait_for_pool(dist.get_rank(), self.local_rank, layers, manager.moe_slot_count,
                          self.vllm_config.instance_id)
'''

_NATIVE_WITH_GATE = _NATIVE_ORDER + '''        elif _ks_order_environ.get("KS_ORDER_GATE_DIR"):
            from ks_order_gate import before_kv_init
            before_kv_init(self)
'''


class PlatformCompatibilityError(ValueError):
    """The target platform differs at a required integration point."""


def _entries(patch: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    """Parse the bundled small unified patches; reject unexpected targets."""
    lines = patch.read_text(encoding="utf-8").splitlines(keepends=True)
    result = []
    index = 0
    while index < len(lines):
        if not lines[index].startswith("--- "):
            index += 1
            continue
        before = lines[index][4:].strip()
        index += 1
        if index >= len(lines) or not lines[index].startswith("+++ b/"):
            raise PlatformCompatibilityError(f"Invalid patch header: {patch.name}")
        name = lines[index][6:].strip()
        relative = PurePosixPath(name)
        if (relative.is_absolute() or ".." in relative.parts or "\\" in name
                or ":" in name or not name.endswith(".py")
                or not name.startswith(("vllm/", "vllm_ascend/"))
                or before != "a/" + name):
            raise PlatformCompatibilityError(f"Unexpected platform patch path: {name}")
        index += 1
        hunks = []
        while index < len(lines) and not lines[index].startswith(("--- ", "diff --git ")):
            line = lines[index]
            if not line.startswith("@@ "):
                index += 1
                continue
            match = re.match(r"@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@", line)
            if match is None:
                raise PlatformCompatibilityError(f"Invalid platform hunk: {name}")
            expected = tuple(int(v) if v is not None else 1 for v in match.groups())
            old, new = [], []
            index += 1
            while index < len(lines) and lines[index][:1] in (" ", "+", "-"):
                if lines[index].startswith("--- "):
                    break
                mark, content = lines[index][0], lines[index][1:]
                if mark in (" ", "-"):
                    old.append(content)
                if mark in (" ", "+"):
                    new.append(content)
                index += 1
            if (len(old), len(new)) != expected:
                raise PlatformCompatibilityError(f"Invalid platform hunk lengths: {name}")
            hunks.append(("".join(old), "".join(new)))
        if not hunks:
            raise PlatformCompatibilityError(f"Empty platform patch: {name}")
        result.append((name, hunks))
    if not result:
        raise PlatformCompatibilityError(f"Empty platform patch: {patch.name}")
    return result


def _apply(text: str, hunks: list[tuple[str, str]], label: str,
           *, reverse: bool = False) -> str:
    for old, new in hunks:
        if reverse:
            if text.count(new) != 1:
                raise PlatformCompatibilityError(
                    f"Incompatible installed observation at {label}; no files changed"
                )
            text = text.replace(new, old, 1)
            continue
        if text.count(new) == 1:
            continue
        if not old or text.count(old) != 1:
            raise PlatformCompatibilityError(
                f"Incompatible platform source at {label}; no files changed"
            )
        text = text.replace(old, new, 1)
    return text


def _native_order(text: str) -> str:
    tree = ast.parse(text)
    workers = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "NPUWorker"]
    methods = [n for worker in workers for n in worker.body
               if isinstance(n, ast.FunctionDef) and n.name == "initialize_from_config"]
    if len(workers) != 1 or len(methods) != 1:
        raise PlatformCompatibilityError("Expected NPUWorker.initialize_from_config")
    method = methods[0]
    calls = [n for n in method.body if isinstance(n, ast.Expr)
             and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
             and n.value.func.id == "ensure_kv_transfer_initialized"]
    if len(calls) != 1 or calls[0].col_offset != 8:
        raise PlatformCompatibilityError("Expected one top-level worker KV initialization")
    lines = text.splitlines(keepends=True)
    call = calls[0]
    call_text = "".join(lines[call.lineno - 1:call.end_lineno])
    if call_text != "        ensure_kv_transfer_initialized(self.vllm_config, kv_cache_config)\n":
        raise PlatformCompatibilityError("Worker KV initialization signature differs")
    end = call.lineno - 1
    for known in (_NATIVE_WITH_GATE, _OLD_GATE, _NATIVE_ORDER):
        count = len(known.splitlines())
        if "".join(lines[end - count:end]) == known:
            lines[end - count:end] = [_NATIVE_ORDER]
            result = "".join(lines)
            if "KS_ORDER_GATE_DIR" in result or "before_kv_init" in result:
                raise PlatformCompatibilityError("Unknown additional worker gate hook")
            return result
    if any(marker in text for marker in
           ("_ks_order_environ", "KS_ORDER_GATE_DIR", "before_kv_init", "wait_for_pool")):
        raise PlatformCompatibilityError("Unknown worker startup-order hook")
    lines[end:end] = [_NATIVE_ORDER]
    return "".join(lines)


def _headless(text: str) -> str:
    """Use the measured local import after Ascend has patched the executor."""
    tree = ast.parse(text)
    methods = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_headless"]
    if len(methods) != 1:
        raise PlatformCompatibilityError("Expected one native run_headless entry point")
    method = methods[0]
    calls = [n for n in ast.walk(method) if isinstance(n, ast.Assign)
             and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
             and n.value.func.id == "MultiprocExecutor"]
    if len(calls) != 1:
        raise PlatformCompatibilityError("Unknown headless executor construction")
    lines = text.splitlines(keepends=True)
    call = calls[0]
    if "".join(lines[call.lineno - 1:call.end_lineno]) != (
        "        executor = MultiprocExecutor(vllm_config, monitor_workers=False)\n"
    ):
        raise PlatformCompatibilityError("Headless executor call differs")
    imports = [n for n in ast.walk(method) if isinstance(n, ast.ImportFrom)
               and n.module == "vllm.v1.executor.multiproc_executor"
               and any(alias.name == "MultiprocExecutor" for alias in n.names)]
    local = "        from vllm.v1.executor.multiproc_executor import MultiprocExecutor\n"
    if imports:
        if (len(imports) != 1 or imports[0].col_offset != 8
                or imports[0].lineno >= call.lineno
                or lines[call.lineno - 3:call.lineno - 1] != [local, "\n"]):
            raise PlatformCompatibilityError("Unrecognized local headless executor import")
        return text
    top = [n for n in tree.body if isinstance(n, ast.ImportFrom)
           and n.module == "vllm.v1.executor.multiproc_executor"
           and any(alias.name == "MultiprocExecutor" and alias.asname is None for alias in n.names)]
    if len(top) != 1:
        raise PlatformCompatibilityError("Expected the known early-bound headless import")
    lines[call.lineno - 1:call.lineno - 1] = [local, "\n"]
    return "".join(lines)


def platform_plan(vllm_root: Path, ascend_root: Path) -> dict[Path, bytes]:
    """Return changed absolute paths and proposed bytes, or fail before any write.

    Accepts the customer source with the Bifrost/executor prerequisites, the
    previous FULL+gate delivery, and the measured native-order version. Runtime
    configuration must set ``PLATFORM_ENV`` and unset ``KS_ORDER_GATE_DIR``.
    ``LMCACHE_BIFROST_ORDER`` is supplied by the node-specific service entry point.
    The planner does not change environment variables or start services.
    """
    vl, asc = Path(vllm_root).resolve(), Path(ascend_root).resolve()
    originals: dict[Path, bytes] = {}
    texts: dict[Path, str] = {}
    newlines: dict[Path, str] = {}

    def read(root: Path, name: str) -> tuple[Path, str]:
        path = root / name
        package_name = PurePosixPath(name).parts[0]
        if package_name not in ("vllm", "vllm_ascend"):
            raise PlatformCompatibilityError(f"Unexpected platform package: {name}")
        package = root / package_name
        if not path.resolve().is_relative_to(package.resolve()) or not path.is_file():
            raise PlatformCompatibilityError(f"Missing or escaped platform source: {path}")
        if path not in texts:
            raw = path.read_bytes()
            try:
                decoded = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise PlatformCompatibilityError(f"Platform source is not UTF-8: {path}") from exc
            if "\r" in decoded.replace("\r\n", ""):
                raise PlatformCompatibilityError(f"Unsupported source line endings: {path}")
            if "\r\n" in decoded and "\n" in decoded.replace("\r\n", ""):
                raise PlatformCompatibilityError(f"Mixed source line endings: {path}")
            originals[path] = raw
            newlines[path] = "\r\n" if "\r\n" in decoded else "\n"
            texts[path] = decoded.replace("\r\n", "\n")
        return path, texts[path]

    _, manager = read(asc, "vllm_ascend/moe_offload/manager.py")
    for required in ("initial_folded_weights_finalized", "full_layer_expert_params",
                     "rotating_moe_layer_count", "moe_slot_count"):
        if required not in manager:
            raise PlatformCompatibilityError(f"Bifrost manager prerequisite missing: {required}")
    _, executor = read(asc, "vllm_ascend/patch/platform/patch_multiproc_executor.py")
    if ("vllm.v1.executor.multiproc_executor.MultiprocExecutor = AscendMultiprocExecutor" not in executor
            or "daemon=False" not in executor):
        raise PlatformCompatibilityError("The measured Ascend non-daemon executor patch is required")

    sched_name = "vllm_ascend/patch/platform/patch_kv_delivery_preemption.py"
    sched_path, scheduler = read(asc, sched_name)
    observer = _entries(PATCH_DIR / "ascend-admission-observer.patch")[0][1]
    if "_diag_end" in scheduler or "_ks_admit_log" in scheduler or "ks_active" in scheduler:
        texts[sched_path] = _apply(scheduler, observer, sched_name + " admission observer", reverse=True)

    for filename, root in (
        ("model-wait-admission.patch", vl),
        ("tail-scheduler-base.patch", vl),
        ("ascend-tail-20ms.patch", asc),
        ("eplb-startup.patch", asc),
    ):
        for name, hunks in _entries(PATCH_DIR / filename):
            path, source = read(root, name)
            texts[path] = _apply(source, hunks, name)
    texts[sched_path] = _apply(texts[sched_path], observer, sched_name + " admission observer")
    worker_path, worker = read(asc, "vllm_ascend/worker/worker.py")
    texts[worker_path] = _native_order(worker)
    serve_path, serve = read(vl, "vllm/entrypoints/cli/serve.py")
    texts[serve_path] = _headless(serve)

    result = {}
    for path, source in texts.items():
        compile(source, str(path), "exec")
        data = source.replace("\n", newlines[path]).encode("utf-8")
        if data != originals[path]:
            result[path] = data
    return result
