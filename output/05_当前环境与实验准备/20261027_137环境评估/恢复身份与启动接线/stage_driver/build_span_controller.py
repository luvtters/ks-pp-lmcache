"""Build a separate span-aware controller; leave the original candidate intact."""
from __future__ import annotations

import ast
import difflib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = next(path for path in HERE.parents if path.name == "output")
SOURCE = OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/controller/stage_controller.py"

ADMIT = '''    def admit_batch(self, batch_id, generation, requests, token_positions):
        """Reserve an original whole-batch slot; same-request spans may queue."""
        key = (batch_id, generation)
        if (not isinstance(batch_id, str) or not batch_id
                or type(generation) is not int or generation < 0):
            raise ValueError("invalid batch identity")
        if key in self._batches or key in self._retired:
            raise ValueError("duplicate batch identity")
        if self.inflight_count >= self.max_inflight_batches:
            raise RuntimeError("whole-batch slots are full")
        members = tuple(tuple(req) for req in requests)
        positions = tuple(tuple(row) for row in token_positions)
        if (not members or len(members) != len(positions)
                or any(len(req) != 2 for req in members)
                or len({req[0] for req in members}) != len(members)
                or any(not row or any(type(pos) is not int or pos < 0 for pos in row)
                       or row != tuple(range(row[0], row[-1] + 1)) for row in positions)):
            raise ValueError("each request requires one nonempty contiguous token span")
        for (request_id, restore_generation), row in zip(members, positions):
            restore = self._restores.get(request_id)
            if not restore or restore.generation != restore_generation or restore.invalid:
                raise ValueError("unknown, stale or invalid request restore")
            for active in self._batches.values():
                for (old_request, old_generation), old_row in zip(active.requests, active.token_positions):
                    if old_request != request_id:
                        continue
                    if old_generation != restore_generation or row[0] <= old_row[-1]:
                        raise ValueError("overlapping or out-of-order active token spans")
        self._batches[key] = _Batch(key, members, positions)
        return key
'''

MODEL_DONE = '''    def mark_model_complete(self, key, stage, worker, *, device_complete):
        """Device completion does not consume sample output or retire buffers."""
        self._worker(stage, worker)
        batch = self._batches.get(key)
        if not batch or device_complete is not True:
            return False
        if batch.state[stage] == "waiting":
            raise RuntimeError("stage was not dispatched")
        if batch.state[stage] == "done":
            return False
        batch.completed[stage].add(worker)
        if batch.completed[stage] != set(self.stage_workers[stage]):
            return False
        batch.state[stage] = "done"
        return True
'''

FINAL_AND_OWNERSHIP = '''    def mark_sample_consumed(self, key, stage, worker):
        self._worker(stage, worker)
        batch = self._batches[key]
        if batch.state[stage] == "waiting":
            raise RuntimeError("sample cannot precede stage dispatch")
        previous = len(batch.sample_consumed[stage])
        batch.sample_consumed[stage].add(worker)
        return previous != len(batch.sample_consumed[stage])

    def mark_stage_buffer_retired(self, key, stage, worker, *, device_complete):
        self._worker(stage, worker)
        batch = self._batches[key]
        if (device_complete is not True or batch.state[stage] != "done"
                or worker not in batch.sample_consumed[stage]):
            raise RuntimeError("device/model/sample ownership is not retired")
        if stage == 1 and not batch.core_consumed:
            raise RuntimeError("final-stage output has not been consumed by the core")
        previous = len(batch.buffer_retired[stage])
        batch.buffer_retired[stage].add(worker)
        if batch.buffer_retired[stage] == set(self.stage_workers[stage]):
            if self._busy[stage] == key:
                self._busy[stage] = None
            if stage == 1:
                batch.buffer_reserved = False
        return previous != len(batch.buffer_retired[stage])

    def mark_final_result(self, key, result):
        batch = self._batches[key]
        if (batch.state[1] != "done" or result is None
                or batch.sample_consumed[1] != set(self.stage_workers[1])):
            raise RuntimeError("final model/sample result is not complete")
        if batch.result is not _NO_RESULT:
            raise RuntimeError("duplicate final result")
        batch.result = result

    def peek_committable(self):
        if not self._batches:
            return None
        batch = next(iter(self._batches.values()))
        if (batch.result is _NO_RESULT or batch.core_consumed
                or batch.state != ["done", "done"]
                or any(batch.sample_consumed[s] != set(self.stage_workers[s]) for s in (0, 1))):
            return None
        return Commit(batch.key, batch.result, False)

    def mark_core_consumed(self, key):
        commit = self.peek_committable()
        if commit is None or commit.key != key:
            raise RuntimeError("core consumption must follow ready final-result FIFO")
        self._batches[key].core_consumed = True
'''

POP = '''    def pop_committable(self):
        """Release a whole-batch slot only after real core consumption/retirement."""
        if not self._batches:
            return None
        key = next(iter(self._batches))
        batch = self._batches[key]
        if (not batch.core_consumed or batch.result is _NO_RESULT
                or any(batch.buffer_retired[s] != set(self.stage_workers[s]) for s in (0, 1))):
            return None
        del self._batches[key]
        self._retired.add(key)
        return Commit(key, batch.result, False)
'''


def replace_method(text, name, replacement):
    tree = ast.parse(text)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "StageController")
    node = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
    lines = text.splitlines(keepends=True)
    lines[node.lineno - 1:node.end_lineno] = [replacement]
    return "".join(lines)


def build():
    original = SOURCE.read_text(encoding="utf-8")
    marker = "    drained: set[int] = field(default_factory=set)\n"
    if original.count(marker) != 1:
        raise ValueError("reviewed batch fields changed")
    candidate = original.replace(marker, marker +
        "    sample_consumed: list[set[int]] = field(default_factory=lambda: [set(), set()])\n"
        "    buffer_retired: list[set[int]] = field(default_factory=lambda: [set(), set()])\n"
        "    core_consumed: bool = False\n", 1)
    for name, body in (("admit_batch", ADMIT), ("mark_model_complete", MODEL_DONE),
                       ("mark_final_result", FINAL_AND_OWNERSHIP),
                       ("cancel", '    def cancel(self, key):\n        raise NotImplementedError("device-safe multi-batch cancellation is not integrated; retain all ownership")\n'),
                       ("pop_committable", POP)):
        candidate = replace_method(candidate, name, body)
    ast.parse(candidate)
    (HERE / "span_stage_controller.py").write_text(candidate, encoding="utf-8", newline="\n")
    patch = "".join(difflib.unified_diff(original.splitlines(True), candidate.splitlines(True),
        fromfile="a/controller/stage_controller.py", tofile="b/controller/span_stage_controller.py"))
    (HERE / "span_controller.patch").write_text(patch, encoding="utf-8", newline="\n")
    report = {"status": "SPAN_CONTROLLER_BUILT_NOT_RUNTIME_INTEGRATED", "source": str(SOURCE),
              "candidate": str(HERE / "span_stage_controller.py"), "whole_batch_default": 2,
              "same_request_active_spans_supported": True,
              "installed_source_changes": 0, "npu_actions": 0}
    (HERE / "span_controller_build.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    build()
