"""PP2分段就绪控制器：只实现CPU可检验的控制规则。

NOT_RUNTIME_INTEGRATED：尚未接入真实vLLM核心、Connector通知或设备通信。
调用者必须从真实设备完成条件产生device_complete，不能从RPC接受ACK推导。
本类由一个核心线程调用；不自行创建线程、不搬运KV、不执行模型。
"""
from dataclasses import dataclass, field
from typing import Any

BatchKey = tuple[str, int]
RequestKey = tuple[str, int]
_NO_RESULT = object()


@dataclass(frozen=True)
class StageWork:
    """一份不可变阶段安排；前后段必须沿用相同成员与token位置。"""
    key: BatchKey
    stage: int
    workers: tuple[int, ...]
    requests: tuple[RequestKey, ...]
    token_positions: tuple[tuple[int, ...], ...]


@dataclass(frozen=True)
class Commit:
    key: BatchKey
    result: Any
    cancelled: bool


@dataclass
class _Restore:
    generation: int
    ready: list[set[int]] = field(default_factory=lambda: [set(), set()])
    invalid: bool = False


@dataclass
class _Batch:
    key: BatchKey
    requests: tuple[RequestKey, ...]
    token_positions: tuple[tuple[int, ...], ...]
    state: list[str] = field(default_factory=lambda: ["waiting", "waiting"])
    completed: list[set[int]] = field(default_factory=lambda: [set(), set()])
    received: set[int] = field(default_factory=set)
    buffer_reserved: bool = False
    result: Any = _NO_RESULT
    cancelled: bool = False
    drained: set[int] = field(default_factory=set)
    sample_consumed: list[set[int]] = field(default_factory=lambda: [set(), set()])
    buffer_retired: list[set[int]] = field(default_factory=lambda: [set(), set()])
    core_consumed: bool = False


class StageController:
    """两段、每段一个计算名额；整批名额和中间结果缓冲分别记账。"""

    def __init__(self, stage_workers, max_inflight_batches=2,
                 max_buffered_activations=2):
        groups = tuple(tuple(group) for group in stage_workers)
        flat = [worker for group in groups for worker in group]
        if (len(groups) != 2 or any(not group for group in groups)
                or len(flat) != len(set(flat))
                or any(type(worker) is not int or worker < 0 for worker in flat)):
            raise ValueError("PP2需要两组非空且不重复的worker")
        if (type(max_inflight_batches) is not int or max_inflight_batches < 1
                or type(max_buffered_activations) is not int
                or max_buffered_activations < 1):
            raise ValueError("批次名额和缓冲名额必须为正整数")
        self.stage_workers = groups
        self.max_inflight_batches = max_inflight_batches
        self.max_buffered_activations = max_buffered_activations
        self._restores: dict[str, _Restore] = {}
        self._batches: dict[BatchKey, _Batch] = {}
        self._retired: set[BatchKey] = set()
        self._busy: list[BatchKey | None] = [None, None]

    @property
    def inflight_count(self):
        return len(self._batches)

    @property
    def buffered_activations(self):
        return sum(batch.buffer_reserved for batch in self._batches.values())

    def _worker(self, stage, worker):
        if type(stage) is not int or stage not in (0, 1):
            raise ValueError("PP阶段只能是0或1")
        if worker not in self.stage_workers[stage]:
            raise ValueError("worker不属于报告的PP阶段")

    def begin_restore(self, request_id, generation):
        """登记本次恢复；旧恢复不能覆盖仍被模型使用的请求。"""
        if (not isinstance(request_id, str) or not request_id
                or type(generation) is not int or generation < 0):
            raise ValueError("请求ID和恢复代次无效")
        previous = self._restores.get(request_id)
        if previous and previous.generation == generation:
            if previous.invalid:
                raise ValueError("该恢复代次已失效，必须重新恢复并增加代次")
            return
        if previous and generation < previous.generation:
            raise ValueError("不能重新登记旧恢复代次")
        if any(any(req == request_id for req, _ in batch.requests)
               for batch in self._batches.values()):
            raise RuntimeError("请求仍有在途批次，不能替换恢复代次")
        self._restores[request_id] = _Restore(generation)

    def mark_kv_ready(self, request_id, generation, stage, worker, *, device_complete):
        """只计入本段本代次、真实设备就绪的worker确认。"""
        self._worker(stage, worker)
        restore = self._restores.get(request_id)
        if (not restore or restore.generation != generation
                or restore.invalid or device_complete is not True):
            return False
        old_size = len(restore.ready[stage])
        restore.ready[stage].add(worker)
        return len(restore.ready[stage]) != old_size

    def admit_batch(self, batch_id, generation, requests, token_positions):
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

    def take_ready(self, stage):
        """只在本段依赖齐全时预留名额并给出一次执行安排。"""
        if type(stage) is not int or stage not in (0, 1):
            raise ValueError("PP阶段只能是0或1")
        if self._busy[stage] is not None:
            return None
        for batch in self._batches.values():
            if batch.cancelled:
                # 未确认设备停止前，不跨过失败批去复用其通信顺序。
                if not self._all_drained(batch):
                    return None
                continue
            if batch.state[stage] == "done":
                continue
            if batch.state[stage] != "waiting":
                return None
            for request_id, generation in batch.requests:
                restore = self._restores[request_id]
                if (restore.generation != generation
                        or restore.ready[stage] != set(self.stage_workers[stage])):
                    return None
            if stage == 0:
                if self.buffered_activations >= self.max_buffered_activations:
                    return None
                batch.buffer_reserved = True
            elif (batch.state[0] != "done"
                  or batch.received != set(self.stage_workers[1])):
                return None
            batch.state[stage] = "dispatched"
            self._busy[stage] = batch.key
            return StageWork(batch.key, stage, self.stage_workers[stage],
                             batch.requests, batch.token_positions)
        return None

    def mark_model_complete(self, key, stage, worker, *, device_complete):
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

    def mark_activation_received(self, key, stage, worker, *, device_complete):
        """收到的是哪一批的中间结果，必须和后段计算安排严格匹配。"""
        self._worker(stage, worker)
        if stage != 1:
            raise ValueError("只有后段需要接收前段中间结果")
        batch = self._batches.get(key)
        if not batch or batch.cancelled or device_complete is not True:
            return False
        if batch.state[0] == "waiting":
            raise RuntimeError("前段尚未派发，不能确认中间结果已到达")
        old_size = len(batch.received)
        batch.received.add(worker)
        return len(batch.received) != old_size

    def mark_sample_consumed(self, key, stage, worker):
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

    def cancel(self, key):
        raise NotImplementedError("device-safe multi-batch cancellation is not integrated; retain all ownership")

    def mark_drained(self, key, worker, *, device_complete):
        batch = self._batches.get(key)
        if worker not in self.stage_workers[0] + self.stage_workers[1]:
            raise ValueError("未知worker不能确认停止")
        if not batch or not batch.cancelled or device_complete is not True:
            return False
        old_size = len(batch.drained)
        batch.drained.add(worker)
        return len(batch.drained) != old_size

    def _all_drained(self, batch):
        return batch.drained == set(self.stage_workers[0] + self.stage_workers[1])

    def pop_committable(self):
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
