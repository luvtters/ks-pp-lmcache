"""隔离候选：复用真实collective_rpc，把动作只交给目标PP阶段。

此模块没有接入EngineCore，不能独立用于vllm serve。
原RPC的广播和Future读取顺序保留；这不是解决通知阻塞的独立消息通道。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


METHODS = {
    "prepare_input": "prepare_pp_stage_input",
    "input_received": "pp_stage_input_received",
    "finalize_input": "finalize_pp_stage_input",
    "input_ready": "pp_stage_input_ready",
    "execute": "execute_pp_stage_model",
    "model_ready": "pp_stage_model_ready",
    "release_ready": "pp_stage_release_ready",
    "release": "release_pp_stage_batch",
}
WITH_SCHEDULE = frozenset(("prepare_input", "execute"))
WITH_FINAL_ACK = frozenset(("release_ready", "release"))


@dataclass(frozen=True)
class StageReply:
    stage: int
    worker_rank: int
    handled: bool
    value: Any = None


@dataclass(frozen=True)
class StageCommand:
    """所有worker接收广播，只有目标阶段进入该动作。"""

    target_stage: int
    operation: str
    batch_key: tuple[str, int]
    scheduler_output: Any = None
    final_output_consumed: bool = False

    def __call__(self, worker):
        from vllm.distributed import get_pp_group

        stage = get_pp_group().rank_in_group
        rank = worker.rank
        if stage != self.target_stage:
            return StageReply(stage, rank, False)
        method = getattr(worker, METHODS[self.operation])
        if self.operation in WITH_SCHEDULE:
            value = method(self.batch_key, self.scheduler_output)
        elif self.operation in WITH_FINAL_ACK:
            value = method(self.batch_key, final_output_consumed=self.final_output_consumed)
        else:
            value = method(self.batch_key)
        return StageReply(stage, rank, True, value)


class StageRPCBridge:
    """只实现派发桥接；阶段资格由StageController检查，不由worker猜测。"""

    def __init__(self, executor, stage_workers):
        self.executor = executor
        self.stage_workers = tuple(frozenset(ranks) for ranks in stage_workers)
        if not self.stage_workers or any(not ranks for ranks in self.stage_workers):
            raise ValueError("每个stage必须有明确的worker集合")
        flattened = [rank for ranks in self.stage_workers for rank in ranks]
        if len(set(flattened)) != len(flattened):
            raise ValueError("worker不能同时属于两个stage")

    def submit(self, stage, operation, batch_key, scheduler_output=None, *,
               final_output_consumed=False):
        if type(stage) is not int or not 0 <= stage < len(self.stage_workers):
            raise ValueError("目标stage不存在")
        if operation not in METHODS:
            raise ValueError("未知阶段操作；采样仍需保持原execute/sample生命周期")
        if operation in WITH_SCHEDULE and scheduler_output is None:
            raise ValueError("计算和接收必须带明确的SchedulerOutput")
        if not (isinstance(batch_key, tuple) and len(batch_key) == 2
                and isinstance(batch_key[0], str) and batch_key[0]
                and type(batch_key[1]) is int and batch_key[1] >= 0):
            raise ValueError("batch_key必须含批次ID和代次")
        if type(final_output_consumed) is not bool:
            raise ValueError("最终输出确认必须显式为bool")
        command = StageCommand(stage, operation, batch_key, scheduler_output,
                               final_output_consumed)
        # 不用unique_reply_rank掩盖缺少的TP确认；由collect校验完整目标集合。
        return self.executor.collective_rpc(command, non_block=True)

    def collect(self, stage, future):
        """读取按原RPC顺序返回的回复，不提前宣布整批模型完成。"""
        expected = self.stage_workers[stage]
        seen = set()
        selected = {}
        all_workers = frozenset().union(*self.stage_workers)
        replies = future.result()
        for reply in replies:
            if not isinstance(reply, StageReply):
                raise ValueError("不是分段worker回复，不能把旧模型输出混入")
            rank = reply.worker_rank
            if rank not in all_workers or rank in seen:
                raise ValueError("worker回复身份非法或重复")
            if (type(reply.stage) is not int or not 0 <= reply.stage < len(self.stage_workers)
                    or rank not in self.stage_workers[reply.stage]):
                raise ValueError("worker回复所属stage不匹配")
            seen.add(rank)
            if rank in expected:
                if reply.stage != stage or not reply.handled:
                    raise ValueError("目标worker未处理阶段任务")
                selected[rank] = reply.value
            elif reply.handled:
                raise ValueError("非目标worker不应执行模型或控制动作")
        if seen != all_workers or set(selected) != set(expected):
            raise ValueError("缺少必要worker回复，不允许继续调度")
        return selected

    def prepare_back_then_execute_front(self, batch_key, scheduler_output):
        """先排接收，再排前段；两者入队前绝不能同步等待接收future。

        真实PP接收可能先等前段发送的元数据。若先prepare.result()，
        此时前段计算还没派出，就会互相等。此方法只排任务、不等待结果。
        调用者必须已经确认前段KV就绪并预留批与缓冲名额。
        """
        if len(self.stage_workers) != 2:
            raise ValueError("当前原型仅对应已确认的PP2")
        prepare = self.submit(1, "prepare_input", batch_key, scheduler_output)
        front = self.submit(0, "execute", batch_key, scheduler_output)
        return prepare, front
