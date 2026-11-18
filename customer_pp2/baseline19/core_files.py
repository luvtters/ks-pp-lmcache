"""Calculate the verified LMCache changes without importing or modifying LMCache.

Both the original e8 customer baseline and the previous FULL delivery are
accepted. Each exact source hunk is either installed already or transformed in
memory. An unfamiliar hunk/helper is rejected rather than overwritten.

Runtime scope is the tested TP16/PP2, stage-local, lmcache_driven deployment.
LMCACHE_BIFROST_ORDER must match in the local model and cache processes.
KS_STORE_ASYNC_EXPORT=1 selects only global rank 16; unset/0 retains normal
event export, as do other ranks and transfer modes. KS_STORE_SUBMIT_DIR is
unset by default: STORE timing decorators then return the original functions
and export tracing writes no files. The device run used diagnostics enabled;
disabling their output is a deployment boundary, not a new device result.
"""
from pathlib import Path, PurePosixPath
import re


HERE = Path(__file__).resolve().parent
PATCHES = (
    HERE.parent / "patches/lmcache-e8-to-inline.patch",
    HERE / "core_patches/native-order-store-export.patch",
)


def _entries(patch: Path):
    """Read our small, text-only unified patches (no external git command)."""
    lines = patch.read_text(encoding="utf-8").splitlines(keepends=True)
    index = 0
    while index < len(lines):
        if not lines[index].startswith("--- "):
            index += 1
            continue
        before = lines[index][4:].strip()
        index += 1
        if index >= len(lines) or not lines[index].startswith("+++ b/"):
            raise ValueError(f"Invalid patch header: {patch.name}")
        name = lines[index][6:].strip()
        relative = PurePosixPath(name)
        if (not name.startswith("lmcache/") or relative.is_absolute()
                or ".." in relative.parts or "\\" in name or ":" in name
                or not name.endswith(".py")
                or before not in ("/dev/null", "a/" + name)):
            raise ValueError(f"Unexpected core patch path: {name}")
        index += 1
        hunks = []
        while index < len(lines) and not lines[index].startswith(("--- ", "diff --git ")):
            line = lines[index]
            if not line.startswith("@@ "):
                index += 1
                continue
            match = re.match(r"@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@", line)
            if match is None:
                raise ValueError(f"Invalid hunk header: {name}")
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
                raise ValueError(f"Invalid hunk lengths: {name}")
            hunks.append(("".join(old), "".join(new)))
        if not hunks:
            raise ValueError(f"Empty core patch: {name}")
        yield name, before == "/dev/null", hunks


def core_plan(lmcache_root: Path) -> dict[Path, bytes]:
    """Return only changed absolute paths and their proposed UTF-8 bytes.

    This function never writes files, loads accelerator packages, or runs git.
    No partially calculated result is returned when any component is unknown.
    """
    root = Path(lmcache_root).resolve()
    package = root / "lmcache"
    if not (package / "__init__.py").is_file():
        raise ValueError(f"Not an LMCache source root: {root}")
    proposed: dict[Path, bytes] = {}
    for patch in PATCHES:
        for name, added, hunks in _entries(patch):
            path = root / name
            if not path.resolve().is_relative_to(package):
                raise ValueError(f"Core path escapes the package: {path}")
            original = proposed.get(path)
            if original is None:
                if not path.is_file() and not added:
                    raise ValueError(f"Missing required LMCache file: {path}")
                original = path.read_bytes() if path.is_file() else b""
            initial = original.decode("utf-8").replace("\r\n", "\n")
            text = initial
            if added:
                if len(hunks) != 1 or hunks[0][0]:
                    raise ValueError(f"Invalid new core file: {name}")
                target = hunks[0][1]
                if path.exists() and text != target:
                    raise ValueError(f"Existing core helper differs: {path}")
                text = target
            else:
                for old, new in hunks:
                    if text.count(new) == 1:
                        continue
                    if not old or text.count(old) != 1:
                        raise ValueError(f"LMCache source hunk differs; no files changed: {path}")
                    text = text.replace(old, new, 1)
            compile(text, str(path), "exec")
            if text != initial:
                proposed[path] = text.encode("utf-8")
    return {path: data for path, data in proposed.items()
            if not path.exists() or path.read_bytes() != data}
