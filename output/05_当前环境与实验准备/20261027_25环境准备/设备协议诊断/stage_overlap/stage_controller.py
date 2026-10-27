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
        """冻结逻辑批成员；入批不代表两段模型均可执行。"""
        key = (batch_id, generation)
        if (not isinstance(batch_id, str) or not batch_id
                or type(generation) is not int or generation < 0):
            raise ValueError("批次ID或代次无效")
        if key in self._batches or key in self._retired:
            raise ValueError("批次ID与代次不可重复使用")
        if self.inflight_count >= self.max_inflight_batches:
            raise RuntimeError("整批在途名额已满")
        members = tuple(tuple(req) for req in requests)
        positions = tuple(tuple(row) for row in token_positions)
        if (not members or len(members) != len(positions)
                or any(len(req) != 2 for req in members)
                or len({req[0] for req in members}) != len(members)
                or any(not row or any(type(pos) is not int or pos < 0 for pos in row)
                       or tuple(sorted(set(row))) != row for row in positions)):
            raise ValueError("批次成员与各请求token位置必须一一对应且有序")
        active_requests = {req for batch in self._batches.values()
                           for req, _ in batch.requests}
        for request_id, restore_generation in members:
            restore = self._restores.get(request_id)
            if not restore or restore.generation != restore_generation:
                raise ValueError("批次包含未登记的请求恢复代次")
            if restore.invalid:
                raise ValueError("批次包含已失效的恢复代次")
            # 此最小原型不重排同一请求的多个在途token/分块。
            if request_id in active_requests:
                raise RuntimeError("同一请求仍有在途批次")
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
        """worker方法返回不够；只接纳真实设备完成后的确认。"""
        self._worker(stage, worker)
        batch = self._batches.get(key)
        if not batch or batch.cancelled or device_complete is not True:
            return False
        if batch.state[stage] == "waiting":
            raise RuntimeError("本段尚未派发，不能报告模型完成")
        if batch.state[stage] == "done":
            return False
        batch.completed[stage].add(worker)
        if batch.completed[stage] != set(self.stage_workers[stage]):
            return False
        batch.state[stage] = "done"
        if stage == 0:
            self._busy[stage] = None
        else:
            # 后段真实执行结束后，才允许复用本批输入中间结果缓冲。
            batch.buffer_reserved = False
            # 末段runner仍保留execute_model_state等待sample，计算名额暂不释放。
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

    def mark_final_result(self, key, result):
        """采样等最终工作完成后再登记结果；这与后段forward完成分开。"""
        batch = self._batches[key]
        if batch.cancelled or batch.state[1] != "done":
            raise RuntimeError("最后段尚未完成，不能登记最终结果")
        if batch.result is not _NO_RESULT:
            raise RuntimeError("最终结果不可重复登记")
        batch.result = result
        # 调用方确认sample及必要输出已真实完成，末段才可接下一批execute。
        self._busy[1] = None

    def cancel(self, key):
        """停止新派发；不凭取消指令就释放未知是否仍在使用的设备资源。"""
        batch = self._batches.get(key)
        if batch:
            batch.cancelled = True
            for request_id, _ in batch.requests:
                # 取消/失败后的设备内容与未结束恢复可能未知，禁止复用旧就绪位。
                self._restores[request_id].invalid = True

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
        """按原逻辑批顺序提交；前段完成不会提前释放整批名额。"""
        if not self._batches:
            return None
        key = next(iter(self._batches))
        batch = self._batches[key]
        if batch.cancelled:
            if not self._all_drained(batch):
                return None
            result = None
        else:
            if batch.state[1] != "done" or batch.result is _NO_RESULT:
                return None
            result = batch.result
        for stage in (0, 1):
            if self._busy[stage] == key:
                self._busy[stage] = None
        batch.buffer_reserved = False
        del self._batches[key]
        self._retired.add(key)
        return Commit(key, result, batch.cancelled)
