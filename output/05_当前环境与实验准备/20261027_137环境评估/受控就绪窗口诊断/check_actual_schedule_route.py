"""Read-only source routing assertions; no framework imports or device work."""
from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT.parent / "减层PD运行证据"
ACTUAL = ROOT / "actual137_patch_balance_schedule.py"
BASE = ROOT.parent / "恢复身份与启动接线/stage_driver/runtime_overlay/vllm/v1/core/sched/scheduler.py"
tree = ast.parse(ACTUAL.read_text(encoding="utf-8"))
cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "BalanceScheduler")
schedule = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "schedule")
disable = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_disable_preemption_on_prefill_node")
env = {"vllm_version_is": lambda version: version == "0.23.0"}
exec(compile(ast.Module(body=[disable], type_ignores=[]), str(ACTUAL), "exec"), env)
config = SimpleNamespace(kv_transfer_config=SimpleNamespace(kv_role="kv_producer"))
disabled = env[disable.name](config)
guard = compile(ast.Expression(schedule.body[0].test), str(ACTUAL), "eval")
uses_super_fast_path = eval(guard, {"self": SimpleNamespace(_balance_enabled=False, _disable_preemption=disabled)})
names = {n.func.attr for n in ast.walk(schedule) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
allocations = [n for n in ast.walk(schedule) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "allocate_slots"]
delay_values = [ast.unparse(k.value) for n in allocations for k in n.keywords if k.arg == "delay_cache_blocks"]
base_tree = ast.parse(BASE.read_text(encoding="utf-8"))
base_cls = next(n for n in base_tree.body if isinstance(n, ast.ClassDef) and n.name == "Scheduler")
base_schedule = next(n for n in base_cls.body if isinstance(n, ast.FunctionDef) and n.name == "schedule")
base_names = {n.func.attr for n in ast.walk(base_schedule) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
checks = {
    "actual_023_producer_disables_preemption": disabled is True,
    "balance_false_producer_bypasses_base_schedule": uses_super_fast_path is False,
    "actual_schedule_calls_inherited_promotion": "_try_promote_blocked_waiting_request" in names,
    "actual_schedule_lacks_attach_output": "attach_output" not in names,
    "actual_waiting_delay_is_only_load_kv_async": delay_values == ["load_kv_async"],
    "candidate_base_has_both_hooks": {"attach_output", "delay_cache_blocks"} <= base_names,
}
assert all(checks.values()), checks
trace_path = EVIDENCE / "stage-diagnostic-trace/stage-1512244.jsonl"
events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
benchmark_path = EVIDENCE / "stage-diagnostic-warm/benchmark.json"
benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
reserved = next(e for e in events if e.get("decision") == "reserved")
front = next(e for e in events if e["phase"] == "stage_kv_ready_observed" and e["stage"] == 0)
back = next(e for e in events if e["phase"] == "stage_kv_ready_observed" and e["stage"] == 1)
assert len(events) == 4 and not reserved["request_global_finished"]
assert not any(e["phase"] == "stage_batch_admitted" for e in events)
summary = {
    "scope": "DIAGNOSTIC_ONLY_REAL_BACK_COMPLETION_THEN_CPU_PUBLICATION_DELAY",
    "artificial_back_publication_delay_ms": 500,
    "performance_comparison_eligible": False,
    "source_routing_assertions": checks,
    "cpu_scope": "Actual predicate AST plus source-call assertions only; not a full effective scheduler execution.",
    "actual_source": ACTUAL.name,
    "source_lines": {"producer_condition": [49, 53], "constructor_warning": [79, 81], "super_gate": [107, 111], "inherited_promotion": [336, 338], "allocate_slots": [476, 485], "return_without_attach": [637, 639], "canonical_replacement": 745},
    "client": {"successful_requests": benchmark["completed"], "failed_requests": benchmark["failed"], "input_tokens": benchmark["input_lens"][0], "output_tokens": benchmark["output_lens"][0], "ttft_s": benchmark["mean_ttft_ms"] / 1000, "tpot_ms": benchmark["mean_tpot_ms"], "e2e_s": benchmark["mean_e2el_ms"] / 1000},
    "trace_evidence": trace_path.relative_to(ROOT.parent).as_posix(),
    "trace_line_count": len(events),
    "trace": events,
    "front_to_reservation_cpu_ms": (reserved["monotonic_ns"] - front["monotonic_ns"]) / 1e6,
    "reservation_to_back_observed_cpu_ms": (back["monotonic_ns"] - reserved["monotonic_ns"]) / 1e6,
    "stage_batch_admission_observed": False,
    "stage_worker_execution_verified": False,
    "new_path_device_compute_or_overlap_verified": False,
    "token_or_kv_content_correctness": "NOT_VERIFIED",
    "after_gauges": {},
    "required_followup": ["Preserve official PD no-preemption semantics in the actual effective schedule.", "Apply partial delay_cache_blocks and attach_output to that schedule, then verify the effective class route.", "Verify admission, actual worker execution, parsed final output, genuine global completion and normal producer lifecycle on device."],
}
for role in ("p", "d0", "d1"):
    values = {}
    for line in (EVIDENCE / f"stage-diagnostic-warm/{role}-after.prom").read_text(encoding="utf-8").splitlines():
        if not line.startswith("#") and any(line.startswith("vllm:" + key + "{") for key in ("num_requests_running", "num_requests_waiting", "kv_cache_usage_perc")):
            key, val = line.rsplit(" ", 1)
            values[key.split("{", 1)[0].removeprefix("vllm:")] = float(val)
    summary["after_gauges"][role] = values
assert all(v == 0 for role in summary["after_gauges"].values() for v in role.values())
out = ROOT / "结果与接线缺口.json"
out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"source_assertions": checks, "trace_lines": len(events), "stage_admission": False, "client_success": benchmark["completed"], "output_tokens": benchmark["output_lens"][0], "output": out.name}, ensure_ascii=True))
