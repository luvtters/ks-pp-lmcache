"""Build adjacent opt-in EngineCore candidate from the actual 25 snapshot."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[3] / "05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/engine/core.py"

READER = '''
# KS candidate: only one old-result reader may be active for this EngineCore.
# NOT_RUNTIME_INTEGRATED: executor aggregation thread affinity is not verified.
class _KSModelResultReader:
    def __init__(self):
        self.owner = threading.get_ident()
        self.tasks = queue.Queue(maxsize=1)
        self.results = weakref.WeakKeyDictionary()
        self.active = None
        self.closed = False
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="KSModelResultReader")
        self.thread.start()

    def submit(self, future):
        if threading.get_ident() != self.owner:
            raise RuntimeError("model wait must stay on the configured core thread")
        cached = self.results.get(future)
        if cached is not None:
            return cached
        if self.closed:
            raise RuntimeError("model result reader is closed")
        if self.active is not None and not self.active.done():
            raise RuntimeError("another old model future already has the sole reader")
        outcome = Future()
        self.results[future] = outcome
        self.active = outcome
        self.tasks.put_nowait((future, outcome))
        return outcome

    def _run(self):
        while True:
            task = self.tasks.get()
            if task is None:
                return
            future, outcome = task
            try:
                outcome.set_result(future.result())
            except BaseException as error:
                outcome.set_exception(error)
            finally:
                # No permanent strong reference from this thread to old batches.
                del task, future, outcome

    def close(self, timeout=0.2):
        if self.active is not None and not self.active.done():
            return False
        if not self.closed:
            self.closed = True
            self.tasks.put_nowait(None)
        self.thread.join(timeout=timeout)
        return not self.thread.is_alive()


'''

METHODS = '''
    def configure_ks_core_event_pump(self, callback, dispatcher=None,
                                     poll_interval=0.005):
        # Explicit hook only: enabling an env var does not configure a scheduler.
        if os.environ.get("KS_CORE_STAGE_EVENT_PUMP") != "1":
            raise RuntimeError("core stage event pump is disabled")
        if getattr(self, "_ks_result_reader", None) is not None:
            raise RuntimeError("core event pump is already configured")
        if not callable(callback) or (dispatcher is not None and not callable(dispatcher)):
            raise ValueError("event callback and optional dispatcher must be callable")
        if not callable(getattr(self.model_executor, "poll_stage_events", None)):
            raise ValueError("independent executor stage event channel is required")
        if not 0 < poll_interval <= 0.1:
            raise ValueError("poll interval must be between zero and 0.1 seconds")
        self._ks_event_callback = callback
        self._ks_event_dispatcher = dispatcher
        self._ks_event_poll_interval = poll_interval
        self._ks_event_callback_active = False
        self._ks_waiting_model_future = None
        self._ks_result_reader = _KSModelResultReader()

    def ks_model_wait_inflight(self):
        # batch_queue.pop() has happened, but this model batch still owns a slot.
        return int(getattr(self, "_ks_waiting_model_future", None) is not None)

    def _ks_assert_no_event_callback_step(self):
        if getattr(self, "_ks_event_callback_active", False):
            raise RuntimeError("event callback cannot recursively schedule a model step")

    def _ks_consume_stage_events(self):
        # This method only runs on the core thread. No token/block progress here.
        for event in self.model_executor.poll_stage_events():
            self._ks_event_callback_active = True
            try:
                if self._ks_event_callback(event) is not None:
                    raise ValueError("event callback must return None")
                if self._ks_event_dispatcher is not None:
                    self._ks_event_dispatcher(event)
            finally:
                self._ks_event_callback_active = False

    def _ks_wait_model_future(self, future):
        reader = getattr(self, "_ks_result_reader", None)
        if reader is None:
            return future.result()
        if getattr(self, "_ks_event_callback_active", False):
            raise RuntimeError("event callback cannot consume old model results")
        outcome = reader.submit(future)
        completed = threading.Event()
        outcome.add_done_callback(lambda _: completed.set())
        self._ks_waiting_model_future = future
        try:
            event_error = None
            while True:
                if event_error is None:
                    try:
                        self._ks_consume_stage_events()
                    except BaseException as error:
                        # Keep the sole reader and batch alive until the real result
                        # is terminal. Do not update scheduler after callback failure.
                        event_error = error
                if completed.is_set():
                    break
                completed.wait(self._ks_event_poll_interval)
            # CPU relay Future, never the old FutureWrapper a second time.
            result = outcome.result()
            if event_error is not None:
                raise event_error
            return result
        finally:
            self._ks_waiting_model_future = None

    def _ks_sample_tokens_with_event_pump(self, grammar_output):
        if getattr(self, "_ks_result_reader", None) is None:
            return self.model_executor.sample_tokens(grammar_output)
        return self._ks_wait_model_future(
            self.model_executor.sample_tokens(grammar_output, non_block=True))

    def _ks_close_core_event_pump(self):
        reader = getattr(self, "_ks_result_reader", None)
        if reader is not None and not reader.close():
            raise RuntimeError("old result reader is still active after executor shutdown")

'''


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"actual source boundary mismatch: {old[:100]!r}")
    return text.replace(old, new, 1)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    text = replace_once(original, "import time\n", "import time\nimport weakref\n")
    text = replace_once(text, "class EngineCore:\n", READER + "class EngineCore:\n")
    text = replace_once(text, "    def step(self) ->", METHODS + "    def step(self) ->")
    text = replace_once(text, "        # Check for any requests remaining in the scheduler",
                        "        self._ks_assert_no_event_callback_step()\n        # Check for any requests remaining in the scheduler")
    text = replace_once(text, "        batch_queue = self.batch_queue\n",
                        "        self._ks_assert_no_event_callback_step()\n        batch_queue = self.batch_queue\n")
    old = "            model_output = future.result()"
    if text.count(old) != 2:
        raise ValueError("expected step and step_with_batch_queue result seams")
    text = text.replace(old, "            model_output = self._ks_wait_model_future(future)")
    text = replace_once(text, "                model_output = self.model_executor.sample_tokens(grammar_output)",
                        "                model_output = self._ks_sample_tokens_with_event_pump(grammar_output)")
    text = replace_once(text, "                exec_model_fut.result()",
                        "                self._ks_wait_model_future(exec_model_fut)")
    text = replace_once(text, "            self.model_executor.shutdown()\n        if self.scheduler:",
                        "            self.model_executor.shutdown()\n        self._ks_close_core_event_pump()\n        if self.scheduler:")
    ast.parse(text)
    (HERE / "core.py").write_text(text, encoding="utf-8")
    patch = "".join(difflib.unified_diff(original.splitlines(True), text.splitlines(True),
                    fromfile="a/vllm/v1/engine/core.py", tofile="b/vllm/v1/engine/core.py"))
    (HERE / "core_event_pump.patch").write_bytes(patch.encode("utf-8"))
    report = {"status": "NOT_RUNTIME_INTEGRATED", "actual_core": str(SOURCE),
              "default_enabled": False, "new_dependencies": [], "server_actions": 0, "npu_actions": 0,
              "changed_real_methods": ["step", "step_with_batch_queue", "shutdown"],
              "scheduler_token_or_block_changes": False, "old_future_wrapper_changed": False}
    (HERE / "build_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
