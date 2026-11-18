"""Install the two-node PP2 baseline into the active Python environment."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from deploy import service_pids, source_root


def replace(path, data, mode):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        tmp = Path(stream.name)
        try:
            stream.write(data)
            stream.close()
            tmp.chmod(mode)
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)


def write_state(folder, meta):
    replace(folder / "receipt.json", json.dumps(meta, indent=2).encode(), 0o600)


def restore(folder, meta):
    rows = meta["files"]
    # Check every file before restoring any. A retry may see partially restored files.
    for i, row in enumerate(rows):
        path = Path(row["path"])
        current = path.read_bytes() if path.exists() else None
        old = (folder / f"{i}.before").read_bytes() if row["exists"] else None
        if current not in (old, (folder / f"{i}.after").read_bytes()):
            raise RuntimeError(f"File changed independently: {path}")
    for i, row in reversed(list(enumerate(rows))):
        path = Path(row["path"])
        if row["exists"]:
            replace(path, (folder / f"{i}.before").read_bytes(), row["mode"])
        else:
            path.unlink(missing_ok=True)
    meta["state"] = "RESTORED"
    write_state(folder, meta)


def require_idle():
    pids = service_pids()
    if pids:
        raise RuntimeError(f"Active model/cache processes in this container: {pids}. Use check while serving.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "apply", "rollback"))
    parser.add_argument("--lmcache-root")
    parser.add_argument("--vllm-root")
    parser.add_argument("--ascend-root")
    parser.add_argument("--state-dir", type=Path, default=Path("/opt/ks-luv-pp/baseline19-backups"))
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--require-applied", action="store_true", help="fail check if a source update is still needed")
    args = parser.parse_args()
    if args.action == "rollback":
        if not args.backup:
            parser.error("rollback requires --backup <directory printed by apply>")
        require_idle()
        folder = args.backup.resolve()
        meta = json.loads((folder / "receipt.json").read_bytes())
        if meta.get("state") not in ("APPLIED", "APPLYING"):
            raise RuntimeError("This backup is not an active installation")
        restore(folder, meta)
        print("RESTORED", folder)
        return

    from core_files import core_plan
    from platform_files import platform_plan
    lm = source_root("lmcache", args.lmcache_root)
    vl = source_root("vllm", args.vllm_root)
    asc = source_root("vllm_ascend", args.ascend_root)
    print("PYTHON", sys.executable, "\nLMCACHE", lm, "\nVLLM", vl, "\nASCEND", asc, flush=True)
    if HERE.parents[1] == lm:
        raise RuntimeError("The delivery checkout was selected as the installed lmcache; use the customer's runtime environment.")
    if not (args.lmcache_root and args.vllm_root and args.ascend_root):
        plugin = source_root("lmcache_ascend", None) / "lmcache_ascend"
        print("LMCACHE_ASCEND", plugin, flush=True)
    def plan():
        changes = {**core_plan(lm), **platform_plan(vl, asc)}
        for path, data in changes.items():
            if not any(path.resolve().is_relative_to(root / package) for root, package in
                       ((lm, "lmcache"), (vl, "vllm"), (asc, "vllm_ascend"))):
                raise RuntimeError(f"Unexpected target path: {path}")
            if path.suffix == ".py":
                compile(data, str(path), "exec")
        return {p: v for p, v in changes.items() if not p.exists() or p.read_bytes() != v}
    changes = plan()
    if not changes:
        print("ALREADY_APPLIED")
        return
    print("READY_TO_APPLY", len(changes), *changes, sep="\n")
    if args.action == "check":
        if args.require_applied:
            raise RuntimeError("This service environment still needs the source update")
        return
    require_idle()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix="pp2-", dir=args.state_dir.resolve()))
    files = []
    for i, (path, data) in enumerate(changes.items()):
        existed = path.is_file()
        if existed:
            (folder / f"{i}.before").write_bytes(path.read_bytes())
        (folder / f"{i}.after").write_bytes(data)
        files.append(dict(path=str(path), exists=existed,
                          mode=path.stat().st_mode & 0o7777 if existed else 0o644))
    meta = {"state": "APPLYING", "files": files}
    write_state(folder, meta)
    print("BACKUP", folder, flush=True)
    try:
        for row, (path, data) in zip(files, changes.items()):
            path.parent.mkdir(parents=True, exist_ok=True)
            replace(path, data, row["mode"])
        if plan():
            raise RuntimeError("Post-install source check still found pending changes")
        meta["state"] = "APPLIED"
        write_state(folder, meta)
    except (Exception, KeyboardInterrupt):
        restore(folder, meta)
        raise
    print("APPLIED", folder)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError) as exc:
        sys.exit(f"STOP {exc}")
