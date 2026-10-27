"""New CPU cases only; original metadata bodies are extracted, not rewritten."""
from __future__ import annotations

import ast
import dataclasses
import importlib.util
import json
from pathlib import Path
import pickle
import sys
import types
import unittest

HERE = Path(__file__).resolve().parent
OUT = next(p for p in HERE.parents if p.name == "output")
OLD = OUT / "04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022"
ADAPTER = OLD / "lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py"
METADATA = OLD / "capacity_followup/sources/lmcache/integration/vllm/lmcache_mp_metadata.py"
SELECTED = []


def source_class(path, name, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    SELECTED.append({"path": str(path), "class": name,
                     "begin": node.lineno, "end": node.end_lineno})
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), namespace)


def metadata_types():
    module = types.ModuleType("ks_restore_original_metadata_cpu")
    sys.modules[module.__name__] = module
    env = vars(module)
    env.update(dataclass=dataclasses.dataclass, KVConnectorMetadata=object)
    source_class(ADAPTER, "LoadStoreOp", env)
    source_class(METADATA, "LMCacheMPRequestMetadata", env)
    source_class(METADATA, "LMCacheMPConnectorMetadata", env)

    # Explicit boundary fixture: no installed group_view.py is available locally.
    # Real GetRetrieveMetadata still computes its original span and APC skip.
    def slice_fixture(allocated, sizes, start, end):
        if sizes != [128] or start != 0 or end != 1024:
            raise AssertionError("outside the single-group fixture")
        return [list(allocated[0])]

    env["slice_block_ids_per_group"] = slice_fixture
    return module


ORIGINAL = metadata_types()
try:
    import restore_metadata_bridge as bridge
except ModuleNotFoundError:
    bridge = None


def metadata():
    tracker = types.SimpleNamespace(
        request_id="cpu-metadata-fixture", num_vllm_hit_tokens=128,
        num_lmcache_hit_tokens=1024, all_token_ids=list(range(1024)),
        allocated_block_ids={0: list(range(10, 18))}, cache_salt="fixture",
        request_configs=None, is_ready_for_retrieving=lambda: True,
        get_token_ids=lambda: list(range(1024)),
    )
    request = ORIGINAL.LMCacheMPRequestMetadata.GetRetrieveMetadata(tracker, 512, [128])
    result = ORIGINAL.LMCacheMPConnectorMetadata()
    result.add_request_metadata(request)
    return result


def parallel_inputs():
    config = types.SimpleNamespace(parallel_config=types.SimpleNamespace(
        rank=10, world_size=16, tensor_parallel_size=8,
        pipeline_parallel_size=2, prefill_context_parallel_size=1))
    strategy = types.SimpleNamespace(vllm_worker_id=10, kv_worker_id=0,
                                     tp_size=8, pp_size=2, vllm_world_size=16)
    groups = types.SimpleNamespace(
        get_world_group=lambda: types.SimpleNamespace(rank=10, world_size=16),
        get_pp_group=lambda: types.SimpleNamespace(rank_in_group=1, world_size=2),
        get_tp_group=lambda: types.SimpleNamespace(rank_in_group=2, world_size=8),
    )
    return config, strategy, groups


class RestoreMetadataChecks(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(bridge, "isolated restore metadata bridge is absent")
        self.meta = metadata()
        self.ledger = bridge.SchedulerRestoreLedger()
        self.registrations = []

    def stamp(self, value=None):
        return self.ledger.stamp(self.meta if value is None else value,
                                 lambda request, generation: self.registrations.append((request, generation)))

    def binding(self):
        config, strategy, groups = parallel_inputs()
        ranks = bridge.WorkerRanks.from_parallel_groups(config, strategy, groups)
        return bridge.WorkerRestoreBinding(ranks, lambda **fields: types.SimpleNamespace(**fields))

    def test_real_get_retrieve_metadata_preserves_span_skip_and_blocks(self):
        request = self.meta.requests[0]
        self.assertIsInstance(request.op, ORIGINAL.LoadStoreOp)
        self.assertEqual((request.op.start, request.op.end, request.op.skip_first_n_tokens), (0, 1024, 128))
        self.assertEqual(request.op.flat_block_ids, list(range(10, 18)))
        before = dataclasses.asdict(request.op)
        self.stamp()
        self.assertEqual(before, dataclasses.asdict(request.op))
        self.assertIsNone(request.request_configs)

    def test_same_metadata_reuses_generation_without_re_registering(self):
        first = self.stamp()
        self.assertEqual(first, self.stamp())
        self.assertEqual(len(self.registrations), 1)

    def test_python_pickle_preserves_original_metadata_sidecar(self):
        self.stamp()
        received = pickle.loads(pickle.dumps(self.meta))
        self.assertEqual(self.stamp(received), self.stamp())
        binding = self.binding()
        binding.bind_metadata(received)
        identity = binding(received.requests[0].request_id, received.requests[0].op)
        self.assertEqual(identity.global_worker_rank, 10)

    def test_cancel_retry_increases_generation_and_rejects_old_metadata(self):
        generation = self.stamp()["cpu-metadata-fixture"]
        self.assertTrue(self.ledger.cancel("cpu-metadata-fixture", generation))
        retry = metadata()
        self.assertGreater(self.stamp(retry)["cpu-metadata-fixture"], generation)
        with self.assertRaises(ValueError):
            self.stamp()

    def test_worker_rejects_missing_sidecar_and_mutated_operation(self):
        binding = self.binding()
        with self.assertRaises(ValueError):
            binding.bind_metadata(self.meta)
        self.stamp()
        binding.bind_metadata(self.meta)
        request = self.meta.requests[0]
        request.op.block_ids[0][0] += 1
        with self.assertRaises(ValueError):
            binding(request.request_id, request.op)

    def test_worker_cancel_retry_rejects_old_generation(self):
        old = self.stamp()["cpu-metadata-fixture"]
        binding = self.binding()
        binding.bind_metadata(self.meta)
        binding.cancel("cpu-metadata-fixture", old)
        with self.assertRaises(ValueError):
            binding("cpu-metadata-fixture", self.meta.requests[0].op)
        self.ledger.cancel("cpu-metadata-fixture", old)
        retry = metadata()
        new = self.stamp(retry)["cpu-metadata-fixture"]
        binding.bind_metadata(retry)
        self.assertEqual(binding("cpu-metadata-fixture", retry.requests[0].op).restore_generation, new)
        with self.assertRaises(ValueError):
            binding.bind_metadata(self.meta)

    def test_identical_retry_does_not_relabel_old_operation_as_new_generation(self):
        old = self.stamp()["cpu-metadata-fixture"]
        old_op = self.meta.requests[0].op
        binding = self.binding()
        binding.bind_metadata(self.meta)
        binding.cancel("cpu-metadata-fixture", old)
        self.ledger.cancel("cpu-metadata-fixture", old)
        retry = metadata()
        self.stamp(retry)
        binding.bind_metadata(retry)
        with self.assertRaises(ValueError):
            binding("cpu-metadata-fixture", old_op)

    def test_actual_group_interfaces_bind_global_rank_not_kv_shard(self):
        self.stamp()
        binding = self.binding()
        binding.bind_metadata(self.meta)
        identity = binding("cpu-metadata-fixture", self.meta.requests[0].op)
        self.assertEqual((identity.global_worker_rank, identity.stage_id, identity.tp_rank, identity.pcp_rank), (10, 1, 2, 0))

    def test_group_config_mismatch_and_pcp_are_rejected(self):
        config, strategy, groups = parallel_inputs()
        config.parallel_config.rank = 9
        with self.assertRaises(ValueError):
            bridge.WorkerRanks.from_parallel_groups(config, strategy, groups)
        config.parallel_config.rank = 10
        config.parallel_config.prefill_context_parallel_size = 2
        with self.assertRaises(ValueError):
            bridge.WorkerRanks.from_parallel_groups(config, strategy, groups)

    def test_same_metadata_cannot_hide_token_or_skip_changes(self):
        self.stamp()
        self.meta.requests[0].op.skip_first_n_tokens += 1
        with self.assertRaises(ValueError):
            self.stamp()


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RestoreMetadataChecks)
    outcome = unittest.TextTestRunner(verbosity=2).run(suite)
    result = {"status": "PASS_NEW_CPU_METADATA_BRIDGE_ONLY" if outcome.wasSuccessful() else "FAIL",
              "tests": outcome.testsRun, "failures": len(outcome.failures), "errors": len(outcome.errors),
              "original_classes": SELECTED,
              "fixtures": ["request tracker values", "single-group block slicing", "parallel group/config objects", "identity constructor"],
              "normal_bootstrap_integrated": False, "npu_verified": False,
              "original_tests_rerun": 0, "server_actions": 0}
    (HERE / ("cpu_result.json" if bridge is not None else "red_result.json")).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(not outcome.wasSuccessful())
