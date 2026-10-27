"""Shared explicit local-P adapters. No inference/device packages imported here."""
import importlib.abc
import importlib.util
from pathlib import Path
import sys


class _KSLocalPCompleteFinder(importlib.abc.MetaPathFinder):
    root = Path(__file__).resolve().parent
    modules = {
        "lmcache.v1.platform.base.cache_context": root.parent / "common" / "lmcache_cache_context_pinned.py",
        "lmcache.integration.vllm.lmcache_mp_connector": root / "lmcache_mp_connector_multi17.py",
        "vllm_ascend.worker.model_runner_v1": root / "model_runner_local.py",
    }

    def find_spec(self, fullname, path=None, target=None):
        source = self.modules.get(fullname)
        if source is not None:
            return importlib.util.spec_from_file_location(fullname, source)
        return None


sys.meta_path.insert(0, _KSLocalPCompleteFinder())
