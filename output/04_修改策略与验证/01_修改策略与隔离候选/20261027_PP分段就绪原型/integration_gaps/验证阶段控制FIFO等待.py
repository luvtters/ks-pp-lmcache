"""只验证归档FutureWrapper的读取顺序，不执行模型、通信或新调度实现。"""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
import json
from pathlib import Path
import sys
import threading
from typing import Any, Callable


HERE = Path(__file__).resolve().parent
PROTOTYPE = HERE.parent
ARCHIVE = PROTOTYPE.parent / "KV完成通知修改策略与隔离分支_20261022"
SOURCE = ARCHIVE / "source_baseline/vllm/v1/executor/multiproc_executor.py"
BRIDGE_SOURCE = PROTOTYPE / "executor_bridge.py"
sys.path.insert(0, str(PROTOTYPE))
from executor_bridge import StageReply, StageRPCBridge


def source_future_wrapper():
    """只抽取真实类，移除的仅是观测span依赖，没有改FIFO逻辑。"""
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "FutureWrapper")
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls]
    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {
        "Future": Future, "InvalidStateError": InvalidStateError,
        "deque": deque, "Callable": Callable, "Any": Any,
        "suppress": suppress, "_ks_future_span": lambda *args: nullcontext(),
    }
    exec(compile(module, str(SOURCE), "exec"), namespace)
    methods = {node.name: {"start": node.lineno, "end": node.end_lineno}
               for node in cls.body if isinstance(node, ast.FunctionDef)}
    return namespace["FutureWrapper"], methods


def run_case():
    wrapper, source_lines = source_future_wrapper()
    queue = deque()
    front_can_finish = threading.Event()
    front_reader_entered = threading.Event()
    control_consumed = threading.Event()
    trace = []
    lock = threading.Lock()
    thread_result = {}

    def mark(action):
        with lock:
            trace.append({"sequence": len(trace) + 1, "action": action})

    def replies(stage):
        # 这是已准备好的CPU回复载荷，不代表真实worker曾执行这些动作。
        return [StageReply(rank // 8, rank, rank // 8 == stage,
                           True if rank // 8 == stage else None)
                for rank in range(16)]

    front_reply = replies(0)
    ready_control_reply = replies(1)

    def wait_front():
        mark("读取旧的B2前段回复：进入等待")
        front_reader_entered.set()
        if not front_can_finish.wait(timeout=2):
            raise TimeoutError("CPU案例未被释放；不是设备通信超时")
        mark("旧B2前段等待已被外部测试条件放行")
        return front_reply

    def read_control():
        mark("现在才读取已准备好的B1后段控制回复")
        control_consumed.set()
        return ready_control_reply

    class ReplyFixtureExecutor:
        """只提供回复函数；不模拟真正的worker或MQ，也不执行StageCommand。"""
        def collective_rpc(self, command, non_block):
            if command.target_stage == 0 and command.operation == "execute":
                return wrapper(queue, get_response=wait_front)
            if command.target_stage == 1 and command.operation == "release_ready":
                return wrapper(queue, get_response=read_control)
            raise AssertionError("CPU案例出现了范围外操作")

    bridge = StageRPCBridge(ReplyFixtureExecutor(), (tuple(range(8)), tuple(range(8, 16))))
    older = bridge.submit(0, "execute", ("B2前段", 1), scheduler_output=object())
    control = bridge.submit(1, "release_ready", ("B1后段", 1))
    mark("后段控制回复载荷已在CPU中准备好；尚未调用collect")

    def collect_later_control():
        try:
            thread_result["values"] = bridge.collect(1, control)
            mark("Bridge.collect返回后段控制结果")
        except BaseException as error:
            thread_result["error"] = f"{type(error).__name__}: {error}"

    thread = threading.Thread(target=collect_later_control, name="CPUStageControlFIFO")
    observed_wait = False
    try:
        thread.start()
        assert front_reader_entered.wait(timeout=1), "没有进入真实FutureWrapper的旧回复读取"
        # 不是用sleep推断等待：旧读取函数已进入，门闩未放行，控制读取未发生。
        observed_wait = (not front_can_finish.is_set() and not control_consumed.is_set()
                         and thread.is_alive() and not control.done())
        assert observed_wait, "未能确认本案例中的后续控制读取受旧future限制"
        mark("已观察：collect控制结果时先等旧future，控制载荷仍未读取")
    finally:
        # 所有路径都必须清理线程；不能为展示阻塞制造永久挂起。
        mark("外部CPU测试释放旧前段等待条件")
        front_can_finish.set()
        thread.join(timeout=3)

    assert not thread.is_alive(), "CPU案例线程未结束"
    assert "error" not in thread_result, thread_result.get("error")
    assert control_consumed.is_set() and older.done() and control.done()
    assert older.exception() is None and control.exception() is None
    assert thread_result["values"] == {rank: True for rank in range(8, 16)}
    assert len(queue) == 0
    actions = [row["action"] for row in trace]
    assert actions.index("外部CPU测试释放旧前段等待条件") < actions.index("现在才读取已准备好的B1后段控制回复")

    bridge_tree = ast.parse(BRIDGE_SOURCE.read_text(encoding="utf-8"))
    bridge_class = next(n for n in bridge_tree.body if isinstance(n, ast.ClassDef) and n.name == "StageRPCBridge")
    collect_node = next(n for n in bridge_class.body if isinstance(n, ast.FunctionDef) and n.name == "collect")
    return {
        "status": "KNOWN_LIMITATION_CONFIRMED",
        "scope": "ONE_BOUNDED_CPU_CALL_ORDER_CASE",
        "observed_control_waited_behind_old_future": observed_wait,
        "threads_left_alive": 0,
        "true_source": {"future_wrapper": str(SOURCE), "methods": source_lines,
                        "stage_bridge": str(BRIDGE_SOURCE),
                        "collect": {"start": collect_node.lineno, "end": collect_node.end_lineno}},
        "trace": trace,
        "fixture_conditions": [
            "后续控制RPC的全部16份CPU回复载荷预先可用，排除本例的worker回复缺失",
            "旧前段get_response由CPU Event暂时挡住，随后由主线程显式放行",
            "直接执行真实StageRPCBridge.collect及归档FutureWrapper，没有改其排队逻辑",
        ],
        "not_proven": [
            "真实HCCL发生死锁或设备等待时长",
            "本轮GPU任务为什么未完成",
            "新阶段调度完整接入或性能收益",
            "PP2相对PP1新增等待量",
        ],
        "server_actions": 0, "npu_actions": 0, "framework_source_modified": False,
    }


if __name__ == "__main__":
    try:
        report = run_case()
    except BaseException as error:
        report = {"status": "NOT_CONFIRMED", "error": f"{type(error).__name__}: {error}",
                  "scope": "CPU_CASE_ONLY", "server_actions": 0, "npu_actions": 0}
    (HERE / "验证结果.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["status"] == "KNOWN_LIMITATION_CONFIRMED" else 1)
