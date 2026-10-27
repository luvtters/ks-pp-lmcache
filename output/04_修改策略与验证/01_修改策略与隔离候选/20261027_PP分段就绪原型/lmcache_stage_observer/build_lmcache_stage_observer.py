"""Build an adjacent candidate only; never writes a runtime repository."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCHIVE = HERE.parent.parent / "KV完成通知修改策略与隔离分支_20261022"
SOURCE = ARCHIVE / "lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py"
FUTURES = ARCHIVE / "lmcache_only_followup/sources/lmcache/v1/multiprocess/futures.py"

HELPERS = '''
    def configure_ks_stage_observer(self, observer, identity_resolver):
        # Explicit integration hook only; no fabricated generation/rank defaults.
        if os.environ.get("KS_LMCACHE_STAGE_OBSERVER") != "1":
            raise RuntimeError("LMCache stage observer is disabled")
        if getattr(self, "_ks_stage_observer", None) is not None:
            raise RuntimeError("stage observer already configured")
        if not isinstance(observer, LMCacheStageObserver) or not callable(identity_resolver):
            raise ValueError("observer and scheduler identity resolver required")
        self._ks_stage_observer = observer
        self._ks_stage_identity_resolver = identity_resolver

    def _ks_reserve_stage_retrieve(self, request_id, op, event):
        observer = getattr(self, "_ks_stage_observer", None)
        if observer is None:
            return None
        if request_id in self.retrieve_futures:
            raise ValueError("retrieve proxy is still tracked for request")
        identity = self._ks_stage_identity_resolver(request_id, op)
        if not isinstance(identity, StageRestoreIdentity) or identity.request_id != request_id:
            raise ValueError("scheduler restore identity is absent or mismatched")
        signature = (op.start, op.end, op.skip_first_n_tokens, tuple(op.flat_block_ids))
        return observer.reserve(identity, signature, event)

    def _ks_invalidate_stage_retrieves(self):
        observer = getattr(self, "_ks_stage_observer", None)
        if observer is not None:
            observer.invalidate_requests(set(self.retrieve_futures))

'''


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"actual source boundary did not match: {old[:90]!r}")
    return text.replace(old, new, 1)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    helper = (HERE / "stage_observer.py").read_text(encoding="utf-8")
    # Place helper definitions in the installed adapter module; no new import path.
    helper = helper[helper.index("@dataclass(frozen=True)") :]
    text = replace_once(original, "class LMCacheMPWorkerAdapter:", helper + "\n\nclass LMCacheMPWorkerAdapter:")
    text = replace_once(text, "    @_lmcache_nvtx_annotate\n    def submit_retrieve_request(",
                        HELPERS + "    @_lmcache_nvtx_annotate\n    def submit_retrieve_request(")
    start = text.index("    def submit_retrieve_request(")
    end = text.index("    @_lmcache_nvtx_annotate\n    def batched_submit_store_requests", start)
    section = text[start:end]
    anchor = "        with _ks_rpc_context("
    section = replace_once(section, anchor,
        "        _ks_operation = self._ks_reserve_stage_retrieve(request_id, op, event)\n" + anchor)
    old = '''            future = self.transfer_ctx.submit_retrieve(
                request_id,
                key,
                self.kv_caches,
                self._block_ids_per_group(op),
                event,
                self.blocks_in_chunk,
                skip_first_n_tokens=op.skip_first_n_tokens,
            )'''
    new = '''            try:
                future = self.transfer_ctx.submit_retrieve(
                    request_id,
                    key,
                    self.kv_caches,
                    self._block_ids_per_group(op),
                    event,
                    self.blocks_in_chunk,
                    skip_first_n_tokens=op.skip_first_n_tokens,
                )
            except BaseException as _ks_error:
                if _ks_operation is not None:
                    self._ks_stage_observer.submission_failed(_ks_operation, _ks_error)
                raise
            if _ks_operation is not None:
                future = self._ks_stage_observer.attach(_ks_operation, future)'''
    section = replace_once(section, old, new)
    text = text[:start] + section + text[end:]
    old_clear = "            self.store_futures.clear()\n            self.retrieve_futures.clear()"
    if text.count(old_clear) != 2:
        raise ValueError("expected both actual unhealthy getter branches")
    text = text.replace(old_clear,
        "            self._ks_invalidate_stage_retrieves()\n" + old_clear)
    text = replace_once(text, "        self._kv_event_stop.set()",
        "        if _ks_observer := getattr(self, \"_ks_stage_observer\", None):\n"
        "            _ks_observer.assert_safe_adapter_shutdown()\n"
        "        self._kv_event_stop.set()")
    ast.parse(text)
    (HERE / "vllm_multi_process_adapter.py").write_text(text, encoding="utf-8")
    (HERE / "lmcache_stage_observer.patch").write_bytes("".join(difflib.unified_diff(
        original.splitlines(True), text.splitlines(True),
        fromfile="a/lmcache/integration/vllm/vllm_multi_process_adapter.py",
        tofile="b/lmcache/integration/vllm/vllm_multi_process_adapter.py")).encode("utf-8"))
    report = {"status": "NOT_RUNTIME_INTEGRATED", "actual_adapter": str(SOURCE),
              "actual_futures": str(FUTURES), "git_commit": "NOT_VERIFIED",
              "default_enabled": False, "server_actions": 0, "npu_actions": 0}
    (HERE / "build_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
