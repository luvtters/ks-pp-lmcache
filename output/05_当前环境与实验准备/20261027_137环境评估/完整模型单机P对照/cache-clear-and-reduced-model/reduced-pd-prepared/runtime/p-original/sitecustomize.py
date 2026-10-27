"""Private real-PD common pin + approved Multi17; no model-runner patch."""
import importlib.abc
import importlib.util
from pathlib import Path
import sys
class Finder(importlib.abc.MetaPathFinder):
    root=Path(__file__).resolve().parent
    modules={"lmcache.v1.platform.base.cache_context":root.parent/"common"/"lmcache_cache_context_pinned.py",
             "lmcache.integration.vllm.lmcache_mp_connector":root/"lmcache_mp_connector_multi17.py"}
    def find_spec(self,fullname,path=None,target=None):
        source=self.modules.get(fullname)
        return importlib.util.spec_from_file_location(fullname,source) if source else None
sys.meta_path.insert(0,Finder())

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
