"""本机CPU验收入口：不连接服务器、不加载模型、不调用NPU。"""
from __future__ import annotations

from datetime import datetime, timezone
import importlib
import io
import json
from pathlib import Path
import sys
import unittest


HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "controller")]


def main():
    groups = (
        ("阶段资格和资源", "controller.test_stage_controller"),
        ("真实worker拆分方法", "worker_split.test_worker_split"),
        ("真实RPC与并发worker队列", "test_executor_bridge"),
        ("三部分组合路径", "test_composed_path"),
    )
    rows = []
    full_log = io.StringIO()
    for label, name in groups:
        module = importlib.import_module(name)
        suite = unittest.defaultTestLoader.loadTestsFromModule(module)
        result = unittest.TextTestRunner(stream=full_log, verbosity=2).run(suite)
        row = {
            "component": label, "module": name,
            "tests": result.testsRun,
            "failures": len(result.failures), "errors": len(result.errors),
            "skipped": len(result.skipped),
            "pass": result.wasSuccessful() and not result.skipped,
        }
        rows.append(row)
        print(f"{label}: {'PASS' if row['pass'] else 'FAIL'} ({row['tests']}项)")
    passed = all(row["pass"] for row in rows)
    receipt = {
        "status": "PASS" if passed else "FAIL",
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "date_for_user": "2026-10-27 Asia/Shanghai",
        "tests": sum(row["tests"] for row in rows),
        "groups": rows,
        "scope": "ISOLATED_CPU_PROTOTYPE",
        "runtime_integration": "NOT_RUNTIME_INTEGRATED",
        "npu_correctness": "NOT_VERIFIED",
        "hccl_completion_semantics": "NOT_VERIFIED",
        "model_token_correctness": "NOT_VERIFIED",
        "performance_gain": "NOT_VERIFIED",
        "server_connections": 0, "services_started_or_stopped": 0,
        "npu_allocations": 0, "runtime_source_files_deployed": 0,
        "real_source_methods_exercised": [
            "archived MultiprocExecutor.collective_rpc",
            "archived FutureWrapper",
            "candidate NPUWorker.execute_model and split methods",
        ],
        "cpu_substitutes": [
            "HCCL queues/tensors/handles and NPU events",
            "model runner without real compute or sampling",
            "stdlib pickle instead of cloudpickle in extracted RPC tests",
        ],
        "not_exercised": [
            "full installed vLLM imports and actual EngineCore scheduling",
            "real LMCache stage-completion event publication",
            "NewRequestData/chunked prefill/MTP lifecycle integration",
            "device and original 1P1D/customer benchmark",
        ],
    }
    (HERE / "CPU验证结果.json").write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (HERE / "CPU验证日志.txt").write_text(full_log.getvalue(), encoding="utf-8")
    print(f"总计：{receipt['tests']}项；{receipt['status']}")
    print("完整框架接入、NPU正确性和性能收益：尚未验证")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
