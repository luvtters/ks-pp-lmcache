"""New normal-method CPU cases for generated core/executor startup hooks."""
from __future__ import annotations

import ast
from collections import deque
from concurrent.futures import Future
import dataclasses
import json
import os
from pathlib import Path
import queue
import sys
import threading
import types
import unittest
import weakref

from check_connector_overlay import OverlayChecks, HERE, SOURCES, load_file
import check_restore_metadata_bridge as original_cases

CORE = HERE / "overlay/vllm/v1/engine/core.py"
EXECUTOR = HERE / "overlay/vllm/v1/executor/multiproc_executor.py"


def source_methods(path, name, methods, env):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in methods]
    cls.bases = []
    cls.decorator_list = []
    for node in cls.body:
        node.decorator_list = []
        SOURCES.append({"path": str(path), "class": name, "method": node.name,
                       "begin": node.lineno, "end": node.end_lineno})
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), env)
    return env[name]


class StartupChecks(OverlayChecks):
    def setUp(self):
        super().setUp()
        self.cores = []

    def tearDown(self):
        for core in getattr(self, "cores", []):
            reader = getattr(core, "_ks_result_reader", None)
            if reader is not None:
                reader.close()
        super().tearDown()

    def enable_flags(self):
        os.environ["KS_PP_STAGE_EVENTS"] = "1"
        os.environ["KS_CORE_STAGE_EVENT_PUMP"] = "1"

    def register_via_worker_hook(self):
        self.assertTrue(EXECUTOR.exists(), "generated actual worker startup hook is absent")
        source = EXECUTOR
        module = types.ModuleType("vllm.v1.executor.multiproc_executor")
        self.put_module(module.__name__, module)
        env = vars(module)
        env.update(dataclass=dataclasses.dataclass, queue=queue, threading=threading,
                   Thread=threading.Thread, os=os)
        original_cases.source_class(source, "KSRestoreEvent", env)
        original_cases.source_class(source, "_KSStageEventPublisher", env)
        worker_type = source_methods(source, "WorkerProc", {"_init_stage_event_channel"}, env)
        worker = worker_type.__new__(worker_type)
        worker.rank = 10
        worker.worker = types.SimpleNamespace()
        import multiprocessing
        self.transport = multiprocessing.get_context("spawn").Queue(maxsize=64)
        config, _ = self.config()
        worker._init_stage_event_channel((self.transport, threading.Event()), config)
        self.publisher = worker._ks_stage_publisher
        return worker

    def new_core(self):
        self.assertTrue(CORE.exists(), "generated normal core startup hook is absent")
        self.put_module("vllm.plugins", types.SimpleNamespace(load_general_plugins=lambda: None))
        for name in ("vllm.v1", "vllm.v1.engine", "vllm.v1.executor"):
            self.put_module(name, types.ModuleType(name))
        load_file("vllm.v1.engine.ks_stage_controller", HERE / "overlay/vllm/v1/engine/ks_stage_controller.py")
        load_file("vllm.v1.engine.ks_restore_binding", HERE / "overlay/vllm/v1/engine/ks_restore_binding.py")
        if "vllm.v1.executor.multiproc_executor" not in sys.modules:
            self.register_via_worker_hook()
        env = {"__name__": "ks_normal_core_cpu", "os": os, "threading": threading,
               "queue": queue, "weakref": weakref, "Future": Future, "deque": deque,
               "logger": types.SimpleNamespace(info=lambda *a: None, debug=lambda *a: None),
               "VLLM_VERSION": "CPU_SOURCE_FIXTURE", "envs": types.SimpleNamespace(VLLM_ELASTIC_EP_SCALE_UP_LAUNCH=False),
               "StructuredOutputManager": lambda config: object(),
               "resolve_kv_cache_block_sizes": lambda *a: (128, 128),
               "MULTIMODAL_REGISTRY": types.SimpleNamespace(engine_receiver_cache_from_config=lambda config: None),
               "get_hash_fn_by_name": lambda name: None, "init_none_hash": lambda f: None,
               "get_request_block_hasher": lambda *a: None, "freeze_gc_heap": lambda: None,
               "maybe_attach_gc_debug_callback": lambda: None, "enable_envs_cache": lambda: None}
        original_cases.source_class(CORE, "_KSModelResultReader", env)
        methods = {"__init__", "_ks_initialize_restore_notifications", "configure_ks_core_event_pump", "_ks_consume_stage_events"}
        core_type = source_methods(CORE, "EngineCore", methods, env)
        core_type._initialize_kv_caches = lambda self, config: types.SimpleNamespace(kv_cache_groups=[object()])
        core_type.step = lambda self: None
        core_type.step_with_batch_queue = lambda self: None

        class ExecutorBoundary:
            def __init__(executor, config):
                executor.events = []
                executor.model_updates = 0

            def init_kv_output_aggregator(executor, connector):
                executor.aggregator_connector = connector

            def get_kv_connector_handshake_metadata(executor):
                return None

            def poll_stage_events(executor):
                events, executor.events = executor.events, []
                return events

        config, _ = self.config()
        config.parallel_config.data_parallel_rank_local = 0
        config.parallel_config.nnodes_within_dp = 1
        config.max_concurrent_batches = 2
        config.cache_config.prefix_caching_hash_algo = "CPU_FIXTURE"
        config.model_config = types.SimpleNamespace(runner_type="generate")
        config.ec_transfer_config = None
        config.speculative_config = types.SimpleNamespace(num_speculative_tokens=3)

        def scheduler_boundary(**kwargs):
            connector = self.scheduler()
            return types.SimpleNamespace(connector=connector, get_kv_connector=lambda: connector)

        config.scheduler_config = types.SimpleNamespace(get_scheduler_cls=lambda: scheduler_boundary, async_scheduling=True)
        core = core_type(config, ExecutorBoundary, False)
        self.cores.append(core)
        return core

    def test_normal_worker_method_registers_actual_publisher(self):
        worker = self.register_via_worker_hook()
        self.assertIs(self.runtime._worker_publisher, worker._ks_stage_publisher)
        self.assertIs(worker.worker.ks_publish_restore_ready.__self__, worker._ks_stage_publisher)

    def test_normal_core_constructor_creates_binding_without_changing_capacity(self):
        self.enable_flags()
        self.register_via_worker_hook()
        core = self.new_core()
        self.assertEqual((core.batch_queue_size, core.batch_queue.maxlen), (2, 2))
        self.assertEqual(core._ks_stage_controller.max_inflight_batches, 2)
        self.assertEqual(core._ks_stage_controller.max_buffered_activations, 2)
        self.assertIs(core._ks_event_callback, core._ks_restore_binding)
        self.assertIs(self.runtime._scheduler_begin_restore.__self__, core._ks_stage_controller)
        self.assertEqual((core.vllm_config.parallel_config.tensor_parallel_size,
                          core.vllm_config.parallel_config.pipeline_parallel_size,
                          core.vllm_config.speculative_config.num_speculative_tokens), (8, 2, 3))

    def test_startup_to_metadata_to_core_only_registers_ready_fact(self):
        self.enable_flags()
        self.register_via_worker_hook()
        core = self.new_core()
        metadata = core.scheduler.connector.build_connector_meta(object())
        import pickle
        received = pickle.loads(pickle.dumps(metadata))
        worker = self.worker()
        worker._get_connector_metadata = lambda: received
        worker.start_load_kv(None)
        event = self.transport.get(timeout=2)
        core.model_executor.events.append(event)
        core._ks_consume_stage_events()
        restore = core._ks_stage_controller._restores["cpu-metadata-fixture"]
        self.assertEqual(restore.ready, [set(), {10}])
        self.assertEqual(core._ks_stage_controller.inflight_count, 0)
        self.assertEqual(core.model_executor.model_updates, 0)
        self.assertIsNone(core._ks_stage_controller.take_ready(0))
        self.assertIsNone(core._ks_stage_controller.take_ready(1))

    def test_normal_core_rejects_partially_enabled_flags(self):
        self.register_via_worker_hook()
        with self.assertRaisesRegex(RuntimeError, "flags"):
            self.new_core()

    def test_disabled_normal_core_and_worker_keep_original_paths(self):
        os.environ.pop("KS_LMCACHE_STAGE_OBSERVER")
        self.register_via_worker_hook()
        self.assertIsNone(self.runtime._worker_publisher)
        core = self.new_core()
        self.assertFalse(hasattr(core, "_ks_stage_controller"))
        self.assertFalse(hasattr(core, "_ks_result_reader"))


if __name__ == "__main__":
    names = ["test_normal_worker_method_registers_actual_publisher",
             "test_normal_core_constructor_creates_binding_without_changing_capacity",
             "test_startup_to_metadata_to_core_only_registers_ready_fact",
             "test_normal_core_rejects_partially_enabled_flags",
             "test_disabled_normal_core_and_worker_keep_original_paths"]
    outcome = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(StartupChecks(name) for name in names))
    result = {"status": "PASS_NORMAL_STARTUP_METHODS_CPU_ONLY" if outcome.wasSuccessful() else "FAIL",
              "tests": outcome.testsRun, "failures": len(outcome.failures), "errors": len(outcome.errors),
              "actual_methods": SOURCES,
              "fixtures": ["model executor/initialization", "adapter network transfer and CPU retrieve Future", "NPU context", "request tracker and block slicing", "parallel groups/config"],
              "normal_core_constructor_body_executed": True,
              "actual_model_loaded": False, "npu_actions": 0,
              "stage_driver_integrated": False, "installed_source_modified": False}
    (HERE / "startup_hooks_cpu_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not outcome.wasSuccessful())
