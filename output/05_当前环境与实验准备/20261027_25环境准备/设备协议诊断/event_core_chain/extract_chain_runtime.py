"""Extract unchanged candidate method ASTs into a stable spawn-importable module."""
import ast
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROTOTYPE = HERE.parents[3] / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
EXECUTOR = PROTOTYPE / "restore_event_binding/multiproc_executor.py"
CORE = PROTOTYPE / "core_event_pump/core.py"
selected, provenance = [], []
for path in (EXECUTOR, CORE):
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        include = False
        methods = None
        if path == EXECUTOR:
            if isinstance(node, ast.Assign):
                include = any(isinstance(target, ast.Name) and target.id.startswith("_KS_")
                              for target in node.targets)
            elif isinstance(node, ast.FunctionDef):
                include = node.name == "_ks_identity"
            elif isinstance(node, ast.ClassDef):
                include = node.name in {"KSStageCommand", "KSStageEvent", "KSRestoreEvent",
                                        "_KSStageEventPublisher", "FutureWrapper", "MultiprocExecutor"}
                if node.name == "MultiprocExecutor":
                    methods = {"_init_stage_event_channels", "_close_stage_event_channels", "poll_stage_events"}
                    node.bases = []  # Full executor bootstrap is deliberately outside this seam.
        elif isinstance(node, ast.ClassDef):
            include = node.name in {"_KSModelResultReader", "EngineCore"}
            if node.name == "EngineCore":
                methods = {"step", "step_with_batch_queue", "configure_ks_core_event_pump",
                           "ks_model_wait_inflight"}
                methods.update(item.name for item in node.body
                               if isinstance(item, ast.FunctionDef) and item.name.startswith("_ks_"))
        if include:
            record = {"source_filepath": str(path), "name": getattr(node, "name", "constants"),
                      "source_line_start": node.lineno, "source_line_end": node.end_lineno,
                      "selected_methods": sorted(methods) if methods else None}
            if methods is not None:
                node.body = [item for item in node.body
                             if isinstance(item, ast.FunctionDef) and item.name in methods]
            selected.append(node)
            provenance.append(record)
prefix = '''"""Actual candidate AST seams; no vLLM/torch import or framework bootstrap.

Stable module name chain_runtime is required for multiprocessing Queue pickle.
See chain_source_map.json for original code locations and selected boundaries.
"""
from __future__ import annotations
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from dataclasses import dataclass
import os
import queue
import threading
from threading import Thread
import time
import weakref
from typing import Any, Callable, cast

'''
body = ast.unparse(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])))
(HERE / "chain_runtime.py").write_text(prefix + body + "\n", encoding="utf-8")
(HERE / "chain_source_map.json").write_text(json.dumps({
    "extraction": "selected original AST bodies; ast.unparse; module stable chain_runtime",
    "excluded": "framework imports, EngineCore bootstrap and unselected executor methods",
    "sources": provenance,
}, ensure_ascii=False, indent=2), encoding="utf-8")
print("Created chain_runtime.py and chain_source_map.json; no framework imports")
