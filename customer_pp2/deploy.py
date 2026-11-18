# SPDX-License-Identifier: Apache-2.0
"""Check, apply and restore the verified Python-only customer patch.

No dependency installation, package import, service launch or process signalling
is performed. Explicit roots may be supplied for exported source trees.
"""

# Standard
import argparse
import ast
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time


HERE = Path(__file__).resolve().parent
PATCH = HERE / "patches" / "lmcache-e8-to-inline.patch"
RELEASE = "customer-pp2-verified-20261118"


def source_root(package: str, supplied: str | None) -> Path:
    """Locate a package source root without importing that package."""
    if supplied:
        root = Path(supplied).resolve()
        if root.name == package and (root / "__init__.py").is_file():
            root = root.parent
    else:
        # A trailing PYTHONPATH colon can expose the delivery checkout itself.
        # Locate the customer's installed package instead of our bundled source.
        previous = sys.path[:]
        try:
            sys.path[:] = [p for p in previous
                           if Path(p or os.getcwd()).resolve() != HERE.parent]
            spec = importlib.util.find_spec(package)
        finally:
            sys.path[:] = previous
        if spec is None or spec.origin is None:
            raise RuntimeError(f"Cannot locate {package}; supply its source root")
        root = Path(spec.origin).resolve().parent.parent
    if not (root / package / "__init__.py").is_file():
        raise RuntimeError(f"Not a {package} source root: {root}")
    return root


def git_apply(root: Path, *options: str) -> subprocess.CompletedProcess[str]:
    """Run patch checks without discovering an unrelated parent repository."""
    env = dict(os.environ, GIT_CEILING_DIRECTORIES=str(root.parent),
               GIT_OPTIONAL_LOCKS="0")
    for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_PREFIX"):
        env.pop(key, None)
    return subprocess.run(["git", "-C", str(root), "apply", *options, str(PATCH)],
                          env=env, capture_output=True, text=True, timeout=30)


def service_pids() -> list[int]:
    """Find model/cache processes in this PID namespace; never stop them."""
    found = []
    for entry in Path("/proc").glob("[0-9]*"):
        if int(entry.name) == os.getpid():
            continue
        try:
            words = (entry / "cmdline").read_bytes().split(b"\0")
            comm = (entry / "comm").read_text().strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        names = [os.fsdecode(word).rsplit("/", 1)[-1] for word in words if word]
        cli = any(name in ("vllm", "lmcache") and i + 1 < len(names)
                  and names[i + 1] in ("serve", "server")
                  for i, name in enumerate(names))
        module = any(word in (b"vllm.entrypoints.openai.api_server",
                              b"lmcache.v1.multiprocess.server") for word in words)
        if cli or module or comm.startswith(("VLLM::", "VllmWorker")):
            found.append(int(entry.name))
    return sorted(found)


def gate_worker(original: bytes) -> bytes:
    """Use the tested worker insertion, or accept that exact insertion once."""
    text = original.decode("utf-8")
    if "before_kv_init(self)" in text:
        block = ('from os import environ as _ks_order_environ',
                 'if _ks_order_environ.get("KS_ORDER_GATE_DIR"):',
                 'from ks_order_gate import before_kv_init')
        if not all(part in text for part in block) or text.count("before_kv_init(self)") != 1:
            raise RuntimeError("A different mirror gate is present; manual alignment required")
        return original
    spec = importlib.util.spec_from_file_location("ks_gate_prepare", HERE / "prepare_gate_worker.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Missing prepare_gate_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.gated_copy(text).encode("utf-8")


def plan(args: argparse.Namespace) -> tuple[dict[str, object], list[tuple[Path, bytes]]]:
    """Validate both changes before returning the files to modify."""
    root = source_root("lmcache", args.lmcache_root)
    paths = re.findall(r"^\+\+\+ b/(.+)$", PATCH.read_text(encoding="utf-8"), re.M)
    if len(paths) != 8 or any(not p.startswith("lmcache/") or ".." in Path(p).parts
                              for p in paths):
        raise RuntimeError("Unexpected delivery patch file list")
    forward = git_apply(root, "--check")
    reverse = git_apply(root, "--reverse", "--check")
    if reverse.returncode == 0:
        state = "ALREADY_APPLIED"
    elif forward.returncode == 0:
        state = "READY_TO_APPLY"
    else:
        raise RuntimeError("LMCache source differs from the verified baseline; no files changed.\n"
                           + forward.stderr + reverse.stderr)
    files = [(root / name, (root / name).read_bytes()) for name in paths]
    out: dict[str, object] = {"release": RELEASE, "python": sys.executable,
                              "lmcache_root": str(root), "lookup": state,
                              "active_service_pids": service_pids()}
    if not args.only_lookup:
        ascend = source_root("vllm_ascend", args.ascend_root)
        manager = ascend / "vllm_ascend/moe_offload/manager.py"
        source = manager.read_text(encoding="utf-8")
        required = ("initial_folded_weights_finalized", "full_layer_expert_params",
                    "rotating_moe_layer_count", "moe_slot_count")
        if not all(name in source for name in required):
            raise RuntimeError("Bifrost manager differs from the tested mirror gate; "
                               "use --only-lookup for the query patch alone")
        worker = ascend / "vllm_ascend/worker/worker.py"
        old = worker.read_bytes()
        gated = gate_worker(old)
        out.update(ascend_root=str(ascend), mirror_gate=(
            "ALREADY_APPLIED" if gated == old else "READY_TO_APPLY"))
        files.append((worker, old))
    return out, files


def write_file(path: Path, data: bytes) -> None:
    """Atomically replace one existing source file, preserving its mode."""
    stage = path.with_name(path.name + f".ks-pp2-{os.getpid()}.tmp")
    try:
        with stage.open("xb") as stream:
            stream.write(data)
        stage.chmod(path.stat().st_mode & 0o7777)
        os.replace(stage, path)
    finally:
        stage.unlink(missing_ok=True)


def apply(args: argparse.Namespace) -> dict[str, object]:
    """Apply checked source changes with a complete before/after backup."""
    info, files = plan(args)
    if info["active_service_pids"]:
        raise RuntimeError(f"Stop the model and cache services first: {info['active_service_pids']}")
    needed = info["lookup"] == "READY_TO_APPLY" or info.get("mirror_gate") == "READY_TO_APPLY"
    if not needed:
        return info
    state = Path(args.state_dir).resolve()
    backup = state / (time.strftime("%Y%m%d-%H%M%S") + f"-{socket.gethostname()}-{os.getpid()}")
    (backup / "before").mkdir(parents=True)
    (backup / "after").mkdir()
    receipt = dict(info, files=[str(path) for path, _ in files], backup=str(backup))
    for i, (_, data) in enumerate(files):
        (backup / "before" / str(i)).write_bytes(data)
    (backup / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    try:
        if info["lookup"] == "READY_TO_APPLY":
            result = git_apply(Path(str(info["lmcache_root"])))
            if result.returncode:
                raise RuntimeError(result.stderr)
        if not args.only_lookup and info["mirror_gate"] == "READY_TO_APPLY":
            worker, original = files[-1]
            write_file(worker, gate_worker(original))
        for i, (path, _) in enumerate(files):
            compile(path.read_bytes(), str(path), "exec")
            (backup / "after" / str(i)).write_bytes(path.read_bytes())
        receipt["status"] = "APPLIED"
    except (OSError, RuntimeError, SyntaxError, subprocess.SubprocessError, KeyboardInterrupt):
        for path, data in files:
            write_file(path, data)
        receipt["status"] = "RESTORED_AFTER_APPLY_ERROR"
        (backup / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
        raise
    (backup / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    (state / "latest-backup.txt").write_text(str(backup) + "\n", encoding="utf-8")
    return receipt


def rollback(args: argparse.Namespace) -> dict[str, object]:
    """Restore only this installation, refusing to overwrite later edits."""
    active = service_pids()
    if active:
        raise RuntimeError(f"Stop the model and cache services first: {active}")
    state = Path(args.state_dir).resolve()
    backup = Path(args.backup or (state / "latest-backup.txt").read_text(encoding="utf-8").strip())
    receipt = json.loads((backup / "receipt.json").read_text(encoding="utf-8"))
    if receipt["status"] != "APPLIED":
        raise RuntimeError(f"Backup is not an active installation: {receipt['status']}")
    files = [Path(name) for name in receipt["files"]]
    for i, path in enumerate(files):
        if path.read_bytes() != (backup / "after" / str(i)).read_bytes():
            raise RuntimeError(f"Source changed after installation; refusing to overwrite: {path}")
    for i, path in enumerate(files):
        write_file(path, (backup / "before" / str(i)).read_bytes())
    receipt["status"] = "ROLLED_BACK"
    (backup / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


def main() -> None:
    """Parse the standalone deployment command and print its JSON receipt."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "apply", "rollback"))
    parser.add_argument("--lmcache-root")
    parser.add_argument("--ascend-root")
    parser.add_argument("--only-lookup", action="store_true")
    parser.add_argument("--state-dir", default="/opt/ks-luv-pp/pp2-update-backups")
    parser.add_argument("--backup")
    args = parser.parse_args()
    try:
        result = rollback(args) if args.command == "rollback" else (
            apply(args) if args.command == "apply" else plan(args)[0])
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"STOP: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
