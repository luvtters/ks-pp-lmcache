"""Build adjacent draft-wait follow-up; never modifies its prerequisite copies."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
CORE = HERE.parent / "core_event_pump/core.py"
EXECUTOR = HERE.parent / "restore_event_binding/multiproc_executor.py"

CORE_HELPERS = '''
    def _ks_wait_model_future(self, future):
        return self._ks_wait_executor_future(future, model_batch=True)

    def ks_draft_wait_pending(self):
        # A draft/control RPC is not a newly admitted model batch.
        return int(getattr(self, "_ks_waiting_draft_future", None) is not None)

    def _ks_take_draft_token_ids(self):
        reader = getattr(self, "_ks_result_reader", None)
        if reader is None:
            return self.model_executor.take_draft_token_ids()
        self._ks_assert_no_event_callback_step()
        if threading.get_ident() != reader.owner:
            raise RuntimeError("draft RPC must be submitted by the configured core writer")
        submit = getattr(self.model_executor, "take_draft_token_ids_async", None)
        if not callable(submit):
            raise RuntimeError("executor has no verified async draft RPC interface")
        # Enqueue on the core thread. The result reader never writes broadcast MQ.
        future = submit()
        return self._ks_wait_executor_future(future, model_batch=False)

'''

ASYNC_DRAFT = '''
    def take_draft_token_ids_async(self) -> Future[DraftTokenIds | None]:
        # Explicit async entry; old take_draft_token_ids remains unchanged.
        return self.collective_rpc(
            "take_draft_token_ids", unique_reply_rank=self.output_rank,
            non_block=True,
        )

'''


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"prerequisite source boundary mismatch: {old[:90]!r}")
    return text.replace(old, new, 1)


def build():
    original_core, original_executor = CORE.read_text(encoding="utf-8"), EXECUTOR.read_text(encoding="utf-8")
    core = replace_once(original_core, "    def _ks_wait_model_future(self, future):\n",
        CORE_HELPERS + "    def _ks_wait_executor_future(self, future, *, model_batch):\n")
    core = replace_once(core, "        self._ks_waiting_model_future = future\n",
        "        if model_batch:\n            self._ks_waiting_model_future = future\n"
        "        else:\n            self._ks_waiting_draft_future = future\n")
    core = replace_once(core, "        finally:\n            self._ks_waiting_model_future = None\n",
        "        finally:\n            if model_batch:\n                self._ks_waiting_model_future = None\n"
        "            else:\n                self._ks_waiting_draft_future = None\n")
    old = "draft_token_ids = self.model_executor.take_draft_token_ids()"
    if core.count(old) != 2:
        raise ValueError("expected post_step and deferred structured-output draft waits")
    core = core.replace(old, "draft_token_ids = self._ks_take_draft_token_ids()")
    executor = replace_once(original_executor, "    def collective_rpc(  # type: ignore[override]\n",
        ASYNC_DRAFT + "    def collective_rpc(  # type: ignore[override]\n")
    ast.parse(core); ast.parse(executor)
    def legacy_methods(text):
        return {n.name: ast.dump(n, include_attributes=False) for n in ast.walk(ast.parse(text))
                if isinstance(n, ast.FunctionDef) and n.name in ("take_draft_token_ids", "collective_rpc")}
    assert legacy_methods(original_executor) == legacy_methods(executor)
    patches = []
    for name, before, after, runtime_path in (
        ("core.py", original_core, core, "vllm/v1/engine/core.py"),
        ("multiproc_executor.py", original_executor, executor, "vllm/v1/executor/multiproc_executor.py")):
        (HERE / name).write_text(after, encoding="utf-8")
        patches.extend(difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                       fromfile="a/"+runtime_path, tofile="b/"+runtime_path))
    (HERE / "draft_wait_followup.patch").write_bytes("".join(patches).encode("utf-8"))
    report = {"status": "NOT_RUNTIME_INTEGRATED", "core_prerequisite": str(CORE),
              "executor_prerequisite": str(EXECUTOR), "old_draft_and_rpc_AST_preserved": True,
              "server_actions": 0, "npu_actions": 0, "default_enabled": False,
              "MTP_computation_changed": False, "scheduler_token_or_block_changes": False}
    (HERE / "build_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
