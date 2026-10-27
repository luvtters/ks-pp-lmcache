"""Inactive private candidate finder; no device/framework import here."""
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import sys

_ROOT = Path("/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate")
_MANIFEST = json.loads((_ROOT / "private_candidate_manifest.json").read_text(encoding="utf-8"))

class _KSStageCandidateFinder(importlib.abc.MetaPathFinder):
    root = _ROOT
    modules = {name: (_ROOT / relative).resolve()
               for name, relative in _MANIFEST["module_mappings"].items()}
    _mooncake_override = os.environ.get("KS_N16_MOONCAKE_OVERRIDE")
    if _mooncake_override:
        _mooncake_path = Path(_mooncake_override)
        if not _mooncake_path.is_absolute():
            raise ImportError("KS_N16_MOONCAKE_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector"] = _mooncake_path.resolve()
    _draft_override = os.environ.get("KS_N16_DRAFT_PROPOSER_OVERRIDE")
    if _draft_override:
        _draft_path = Path(_draft_override)
        if not _draft_path.is_absolute():
            raise ImportError("KS_N16_DRAFT_PROPOSER_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.spec_decode.llm_base_proposer"] = _draft_path.resolve()

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
# CPU_ONLY_NOT_DEPLOYED: exact actual-P scheduler hook mapping.
_KSStageCandidateFinder.modules['vllm_ascend.patch.platform.patch_balance_schedule'] = Path('/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/balance-scheduler-fix/patch_balance_schedule_fixed.py')
sys.meta_path.insert(0, _KSStageCandidateFinder())
