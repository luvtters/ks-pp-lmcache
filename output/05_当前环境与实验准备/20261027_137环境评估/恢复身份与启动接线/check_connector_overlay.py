"""Exercise generated Connector methods with explicit CPU boundary fixtures."""
from __future__ import annotations

import ast
from contextlib import nullcontext
import dataclasses
import enum
import importlib.util
import json
import math
import multiprocessing
import os
from pathlib import Path
import pickle
import queue
import sys
import threading
import types
import unittest

import check_restore_metadata_bridge as original_cases

HERE = Path(__file__).resolve().parent
PROTO = original_cases.OUT / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
OVERLAY = HERE / "overlay/lmcache/integration/vllm"
SOURCES = []


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def selected_class(path, name, methods, env):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    chosen = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in methods]
    for method in chosen:
        SOURCES.append({"path": str(path), "class": name, "method": method.name,
                        "begin": method.lineno, "end": method.end_lineno})
    cls.body = chosen
    cls.decorator_list = []
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), env)
    return env[name]


class BaseConnector:
    def __init__(self, config, role, cache_config):
        self._vllm_config, self._role, self._kv_cache_config = config, role, cache_config


class Role(enum.Enum):
    SCHEDULER = 1
    WORKER = 2


class CpuProxy:
    def __init__(self):
        self.done = threading.Event()
        self.value = None

    def retain_reference(self, value):
        self.retained = value

    def set_result(self, value):
        self.value = value
        self.done.set()

    def set_exception(self, error):
        self.error = error
        self.done.set()


class CpuDeviceFuture:
    def query(self):
        return True

    def result(self, timeout=0):
        return True


class OverlayChecks(unittest.TestCase):
    def setUp(self):
        self.assertTrue((OVERLAY / "ks_restore_runtime.py").exists(), "normal Connector runtime overlay is absent")
        self.saved_env = dict(os.environ)
        os.environ["KS_LMCACHE_STAGE_OBSERVER"] = "1"
        self.saved_modules = {}
        self.publisher = None
        self.workers = []
        self.calls = []
        for name in ("lmcache", "lmcache.integration", "lmcache.integration.vllm", "vllm", "vllm.distributed"):
            self.put_module(name, types.ModuleType(name))
        self.bridge = load_file("lmcache.integration.vllm.ks_restore_metadata_bridge", OVERLAY / "ks_restore_metadata_bridge.py")
        self.runtime = load_file("lmcache.integration.vllm.ks_restore_runtime", OVERLAY / "ks_restore_runtime.py")
        self.observer_module = load_file("ks_original_observer_for_connector_cpu", PROTO / "lmcache_stage_observer/stage_observer.py")
        self.install_adapter_fixture()
        self.install_groups_fixture()
        self.Connector = self.connector_class()

    def put_module(self, name, value):
        if name not in self.saved_modules:
            self.saved_modules[name] = sys.modules.get(name)
        sys.modules[name] = value

    def tearDown(self):
        for worker in self.workers:
            runtime = getattr(worker, "_ks_restore_worker", None)
            if runtime is not None:
                runtime.close()
        if self.publisher is not None:
            self.publisher.close()
        for name, previous in self.saved_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
        os.environ.clear()
        os.environ.update(self.saved_env)

    def install_adapter_fixture(self):
        module = types.ModuleType("ks_adapter_constructor_cpu_fixture")
        self.put_module(module.__name__, module)
        module.LMCacheStageObserver = self.observer_module.LMCacheStageObserver
        module.StageRestoreIdentity = self.observer_module.StageRestoreIdentity
        module.MessagingFuture = CpuProxy
        calls = self.calls
        env = dict(vars(module), os=os)
        source = PROTO / "lmcache_stage_observer/vllm_multi_process_adapter.py"
        raw = selected_class(source, "LMCacheMPWorkerAdapter", {"configure_ks_stage_observer"}, env)

        def constructor(adapter, **kwargs):
            calls.append("adapter-constructor")
            adapter.parallel_strategy = kwargs["parallel_strategy"]
            adapter.submitted = []

        def submit(adapter, request_ids, ops, event, **kwargs):
            for request_id, op in zip(request_ids, ops):
                identity = adapter._ks_stage_identity_resolver(request_id, op)
                adapter.submitted.append((request_id, op, identity))
                operation = adapter._ks_stage_observer.reserve(identity, (op.start, op.end, op.skip_first_n_tokens, tuple(op.flat_block_ids)), event)
                adapter._ks_stage_observer.attach(operation, CpuDeviceFuture())

        raw.__module__ = module.__name__
        raw.__init__ = constructor
        raw.batched_submit_retrieve_requests = submit
        raw.create_recorded_event = lambda adapter: None
        module.LMCacheMPWorkerAdapter = raw
        self.Adapter = raw

    def install_groups_fixture(self):
        _, _, groups = original_cases.parallel_inputs()
        self.put_module("vllm.distributed.parallel_state", groups)
        sys.modules["vllm.distributed"].parallel_state = groups
        torch = types.ModuleType("torch")
        torch.npu = types.SimpleNamespace(current_device=lambda: 10, device=lambda device: nullcontext())
        self.put_module("torch", torch)

    def config(self):
        config, strategy, _ = original_cases.parallel_inputs()
        config.parallel_config.data_parallel_size = 1
        strategy.dcp_size = 1
        config.cache_config = types.SimpleNamespace(enable_prefix_caching=True)
        config.kv_transfer_config = types.SimpleNamespace(
            is_kv_producer=True, kv_connector_extra_config={},
            get_from_extra_config=lambda key, default=None: default)
        config.kv_events_config = None
        return config, strategy

    def connector_class(self):
        config, strategy = self.config()
        env = {
            "__name__": "ks_connector_overlay_cpu", "KVConnectorBase_V1": BaseConnector,
            "SupportsHMA": object, "KVConnectorRole": Role, "math": math, "sys": sys,
            "LMCacheMPWorkerAdapter": self.Adapter,
            "LMCacheMPSchedulerAdapter": lambda **kwargs: types.SimpleNamespace(lmcache_tokens_per_chunk=512),
            "LMCacheMPConnectorStats": lambda: None, "KVConnectorTransferResults": None,
            "validate_mamba_step_alignment": lambda *args: None, "validate_kv_cache_groups": lambda *args: None,
            "get_group_tokens_per_block": lambda *args: [128],
            "get_vllm_scheduler_block_size": lambda *args: 128,
            "get_dcp_decorated_model_name": lambda *args: "CPU_MODEL_FIXTURE",
            "_ensure_transport_scheme": lambda value: value,
            "is_mp_server_autostart_enabled": lambda config: False,
            "validate_dcp_support": lambda *args: None,
            "logger": types.SimpleNamespace(info=lambda *args: None, debug=lambda *args: None),
            "zmq": types.SimpleNamespace(Context=types.SimpleNamespace(instance=lambda: object())),
            "build_parallel_strategy_from_vllm_config": lambda *args: strategy,
            "print_banner_once": lambda *args: None,
            "_has_preemption_reqs": lambda output: False,
            "LMCacheMPConnectorMetadata": original_cases.ORIGINAL.LMCacheMPConnectorMetadata,
            "LMCacheMPRequestMetadata": original_cases.ORIGINAL.LMCacheMPRequestMetadata,
            "LMCacheMPRequestState": types.SimpleNamespace(WAITING_FOR_LOAD=1, READY=2),
            "_ks_restore_runtime": self.runtime,
        }
        methods = {"__init__", "role", "transfer_intermediate_tensors", "start_load_kv", "build_connector_meta", "_process_retrieve_requests"}
        tree = ast.parse((OVERLAY / "lmcache_mp_connector.py").read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "LMCacheMPConnector")
        cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in methods]
        SOURCES.extend({"path": str(OVERLAY / "lmcache_mp_connector.py"), "class": cls.name,
                        "method": n.name, "begin": n.lineno, "end": n.end_lineno} for n in cls.body)
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(OVERLAY / "lmcache_mp_connector.py"), "exec"), env)
        return env["LMCacheMPConnector"]

    def register_publisher(self):
        source = PROTO / "draft_wait_followup/multiproc_executor.py"
        module = types.ModuleType("ks_real_publisher_connector_cpu")
        self.put_module(module.__name__, module)
        env = vars(module)
        env.update(dataclass=dataclasses.dataclass, queue=queue, threading=threading, Thread=threading.Thread)
        original_cases.source_class(source, "KSRestoreEvent", env)
        original_cases.source_class(source, "_KSStageEventPublisher", env)
        self.transport = multiprocessing.get_context("spawn").Queue(maxsize=64)
        self.publisher = module._KSStageEventPublisher(self.transport, threading.Event(), 10, 1, tp_size=8)
        self.runtime.register_worker_publisher(self.publisher)

    def worker(self):
        config, _ = self.config()
        result = self.Connector(config, Role.WORKER)
        self.workers.append(result)
        return result

    def scheduler(self):
        config, _ = self.config()
        result = self.Connector(config, Role.SCHEDULER)
        result._process_new_requests = lambda *args: None
        result._process_cached_requests = lambda *args: None
        result._report_block_allocation_deltas = lambda *args: None
        # The actual source GetRetrieveMetadata computes real op fields.
        result.request_trackers = {"fixture": types.SimpleNamespace(
            request_id="cpu-metadata-fixture", state=1, num_vllm_hit_tokens=128,
            num_lmcache_hit_tokens=1024, all_token_ids=list(range(1024)),
            allocated_block_ids={0: list(range(10, 18))}, cache_salt="fixture",
            request_configs=None, is_ready_for_retrieving=lambda: True,
            get_token_ids=lambda: list(range(1024)))}
        return result

    def test_real_constructor_installs_observer_after_publisher_exists(self):
        self.register_publisher()
        worker = self.worker()
        self.assertEqual(self.calls, ["adapter-constructor"])
        self.assertIsNotNone(worker.worker_adapter._ks_stage_observer.owner)
        self.assertIs(worker.worker_adapter._ks_stage_identity_resolver, worker._ks_restore_worker.binding)

    def test_missing_publisher_fails_enabled_worker_constructor(self):
        with self.assertRaisesRegex(RuntimeError, "publisher"):
            self.worker()

    def test_disabled_constructor_preserves_original_path(self):
        os.environ.pop("KS_LMCACHE_STAGE_OBSERVER")
        worker = self.worker()
        self.assertFalse(hasattr(worker, "_ks_restore_worker"))
        self.assertFalse(hasattr(worker.worker_adapter, "_ks_stage_observer"))

    def test_real_build_metadata_requires_core_binding(self):
        scheduler = self.scheduler()
        with self.assertRaisesRegex(RuntimeError, "core"):
            scheduler.build_connector_meta(object())

    def test_real_build_then_start_load_publishes_independent_restore_event(self):
        registered = []
        self.runtime.bind_scheduler_begin_restore(lambda request, generation: registered.append((request, generation)))
        scheduler = self.scheduler()
        metadata = scheduler.build_connector_meta(object())
        self.assertEqual(registered, [("cpu-metadata-fixture", 0)])
        received = pickle.loads(pickle.dumps(metadata))
        self.register_publisher()
        worker = self.worker()
        worker._get_connector_metadata = lambda: received
        worker.start_load_kv(None)
        event = self.transport.get(timeout=2)
        self.assertEqual((event.request_id, event.restore_generation, event.worker_id, event.stage), ("cpu-metadata-fixture", 0, 10, 1))
        self.assertIs(worker.worker_adapter.submitted[0][1], received.requests[0].op)


if __name__ == "__main__":
    outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(OverlayChecks))
    report = {"status": "PASS_CONNECTOR_OVERLAY_CPU_ONLY" if outcome.wasSuccessful() else "FAIL",
              "tests": outcome.testsRun, "failures": len(outcome.failures), "errors": len(outcome.errors),
              "actual_methods": SOURCES,
              "fixtures": ["adapter constructor/network transfer", "CPU completed retrieve Future", "request tracker and single-group block slicing", "NPU device context", "parallel groups/config", "scheduler core callback"],
              "normal_connector_methods_executed": True,
              "full_engine_core_started": False, "actual_lmcache_transfer": False,
              "npu_verified": False, "installed_source_modified": False}
    (HERE / "connector_overlay_cpu_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not outcome.wasSuccessful())
