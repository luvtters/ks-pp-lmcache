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
