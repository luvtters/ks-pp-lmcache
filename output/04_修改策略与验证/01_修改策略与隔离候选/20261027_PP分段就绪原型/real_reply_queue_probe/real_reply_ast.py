"""Standard-library AST loader for unchanged candidate methods, not a mock MQ."""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future, InvalidStateError
from contextlib import nullcontext, suppress
from dataclasses import dataclass
import importlib.util
import os
from pathlib import Path
import queue
import sys
import threading
from threading import Thread
from typing import Any, Callable, cast
import weakref


def load_plain(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_candidates(source_root):
    source_root = Path(source_root)
    nodes = []
    for node in ast.parse((source_root / "multiproc_executor.py").read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                and target.id.startswith("_KS_") for target in node.targets):
            nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "_ks_identity":
            nodes.append(node)
        elif isinstance(node, ast.ClassDef):
            if node.name in {"KSStageCommand", "KSStageEvent", "KSRestoreEvent",
                             "_KSStageEventPublisher", "FutureWrapper"}:
                nodes.append(node)
            elif node.name == "MultiprocExecutor":
                node.bases = []
                node.body = [method for method in node.body if isinstance(method, ast.FunctionDef)
                             and method.name in {"_init_stage_event_channels",
                                "_close_stage_event_channels", "poll_stage_events"}]
                nodes.append(node)
    for node in ast.parse((source_root / "core.py").read_text(encoding="utf-8")).body:
        if not isinstance(node, ast.ClassDef):
            continue
        if node.name == "_KSModelResultReader":
            nodes.append(node)
        elif node.name == "EngineCore":
            node.body = [method for method in node.body if isinstance(method, ast.FunctionDef)
                         and (method.name.startswith("_ks_") or method.name in {
                             "configure_ks_core_event_pump", "ks_model_wait_inflight",
                             "step", "step_with_batch_queue"})]
            nodes.append(node)
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes]
    exec(compile(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])),
                 "<actual-candidate-source-AST>", "exec"), globals())
    controller = load_plain("ks_real_reply_controller", source_root / "stage_controller.py")
    binding = load_plain("ks_real_reply_binding", source_root / "restore_binding.py")
    return controller, binding
