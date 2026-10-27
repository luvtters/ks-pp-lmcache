"""Inactive private candidate finder; no device/framework import here."""
import importlib.abc
import importlib.util
import json
from pathlib import Path
import sys

_ROOT = Path("/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate")
_MANIFEST = json.loads((_ROOT / "private_candidate_manifest.json").read_text(encoding="utf-8"))

class _KSStageCandidateFinder(importlib.abc.MetaPathFinder):
    root = _ROOT
    modules = {name: (_ROOT / relative).resolve()
               for name, relative in _MANIFEST["module_mappings"].items()}

    def find_spec(self, fullname, path=None, target=None):
        source = self.modules.get(fullname)
        if source is None:
            return None
        if not source.is_file():
            raise ImportError("private stage candidate source is missing: " + str(source))
        return importlib.util.spec_from_file_location(fullname, source)

# The source tree contains partial package directories. Keep it off sys.path;
# only this exact-module finder may expose its files (editable parents stay real).
sys.path[:] = [value for value in sys.path if Path(value or ".").resolve() != _ROOT]
sys.meta_path.insert(0, _KSStageCandidateFinder())
