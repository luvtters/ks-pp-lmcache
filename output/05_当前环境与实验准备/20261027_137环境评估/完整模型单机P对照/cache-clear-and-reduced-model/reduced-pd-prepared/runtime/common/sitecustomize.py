"""Map only the approved pinned-index module; import no inference/device package."""
import importlib.abc
import importlib.util
from pathlib import Path
import sys


class _KSPinnedCacheContextFinder(importlib.abc.MetaPathFinder):
    module_name = "lmcache.v1.platform.base.cache_context"
    module_path = Path(__file__).with_name("lmcache_cache_context_pinned.py")

    def find_spec(self, fullname, path=None, target=None):
        if fullname == self.module_name:
            return importlib.util.spec_from_file_location(fullname, self.module_path)
        return None


sys.meta_path.insert(0, _KSPinnedCacheContextFinder())

# Explicit private single-file override; never create a framework package namespace.
import os as _ks_n16_os
_ks_n16_source = _ks_n16_os.environ.get("KS_N16_MOONCAKE_OVERRIDE")
if _ks_n16_source:
    class _KSN16MooncakeFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector":
                return importlib.util.spec_from_file_location(fullname, _ks_n16_source)
            return None
    sys.meta_path.insert(0, _KSN16MooncakeFinder())

# Exact private draft-construction override, alongside the common Mooncake mapping.
_ks_n16_draft_source = _ks_n16_os.environ.get("KS_N16_DRAFT_PROPOSER_OVERRIDE")
if _ks_n16_draft_source:
    class _KSN16DraftProposerFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "vllm_ascend.spec_decode.llm_base_proposer":
                return importlib.util.spec_from_file_location(fullname, _ks_n16_draft_source)
            return None
    sys.meta_path.insert(0, _KSN16DraftProposerFinder())
