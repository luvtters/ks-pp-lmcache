"""Check exact optional N16 finder paths without importing frameworks."""
from __future__ import annotations
import ast
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import unittest
from deploy_private_candidate import LOADER

HERE=Path(__file__).resolve().parent
CANONICAL='vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector'
OVERRIDE=HERE.parents[1]/'完整模型单机P对照/cache-clear-and-reduced-model/pd_actual_mooncake_connector.py'
DRAFT_CANONICAL='vllm_ascend.spec_decode.llm_base_proposer'
DRAFT_OVERRIDE=HERE.parents[1]/'完整模型单机P对照/cache-clear-and-reduced-model/reduced-pd-prepared/runtime/draft-n16/llm_base_proposer_n16.py'


def finder():
    manifest=json.loads((HERE/'unified_overlay_manifest.json').read_text(encoding='utf-8'))
    mappings={name:item['relative_path'] for item in manifest['files'] for name in item['module_names']}
    mappings.update({'lmcache.v1.platform.base.cache_context':'../common/lmcache_cache_context_pinned.py',
                     'vllm_ascend.worker.model_runner_v1':'../local-completion/model_runner_local.py'})
    node=next(node for node in ast.parse(LOADER).body if isinstance(node,ast.ClassDef))
    namespace={'importlib':importlib,'Path':Path,'os':os,'_ROOT':HERE/'runtime_overlay',
               '_MANIFEST':{'module_mappings':mappings}}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),
                 'actual_candidate_finder_class','exec'),namespace)
    return namespace[node.name](),mappings


class FinderChecks(unittest.TestCase):
    def setUp(self):
        self.previous=os.environ.pop('KS_N16_MOONCAKE_OVERRIDE',None)
        self.previous_draft=os.environ.pop('KS_N16_DRAFT_PROPOSER_OVERRIDE',None)

    def tearDown(self):
        if self.previous is None:
            os.environ.pop('KS_N16_MOONCAKE_OVERRIDE',None)
        else:
            os.environ['KS_N16_MOONCAKE_OVERRIDE']=self.previous
        if self.previous_draft is None:
            os.environ.pop('KS_N16_DRAFT_PROPOSER_OVERRIDE',None)
        else:
            os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']=self.previous_draft

    def test_without_override_original_seventeen_are_unchanged(self):
        actual,mappings=finder()
        self.assertEqual(len(actual.modules),17)
        self.assertEqual(actual.modules,{name:(HERE/'runtime_overlay'/relative).resolve()
                                         for name,relative in mappings.items()})
        self.assertIsNone(actual.find_spec(CANONICAL))
        self.assertIsNone(actual.find_spec(DRAFT_CANONICAL))

    def test_absolute_override_maps_only_the_exact_mooncake_module(self):
        self.assertTrue(OVERRIDE.is_file())
        os.environ['KS_N16_MOONCAKE_OVERRIDE']=str(OVERRIDE)
        actual,mappings=finder()
        self.assertEqual(len(actual.modules),18)
        self.assertEqual(Path(actual.find_spec(CANONICAL).origin),OVERRIDE.resolve())
        self.assertEqual({name:path for name,path in actual.modules.items() if name!=CANONICAL},
                         {name:(HERE/'runtime_overlay'/relative).resolve() for name,relative in mappings.items()})
        for parent in ('vllm','vllm_ascend','vllm_ascend.distributed',
                       'vllm_ascend.distributed.kv_transfer.kv_p2p',CANONICAL+'_other'):
            self.assertIsNone(actual.find_spec(parent))

    def test_relative_override_is_rejected(self):
        os.environ['KS_N16_MOONCAKE_OVERRIDE']='relative/mooncake.py'
        with self.assertRaises(ImportError):
            finder()

    def test_draft_alone_and_both_overrides_preserve_original_mappings(self):
        self.assertTrue(DRAFT_OVERRIDE.is_file())
        os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']=str(DRAFT_OVERRIDE)
        actual,mappings=finder()
        self.assertEqual(len(actual.modules),18)
        self.assertEqual(Path(actual.find_spec(DRAFT_CANONICAL).origin),DRAFT_OVERRIDE.resolve())
        self.assertIsNone(actual.find_spec(CANONICAL))
        os.environ['KS_N16_MOONCAKE_OVERRIDE']=str(OVERRIDE)
        both,mappings=finder()
        self.assertEqual(len(both.modules),19)
        self.assertEqual(Path(both.find_spec(DRAFT_CANONICAL).origin),DRAFT_OVERRIDE.resolve())
        self.assertEqual(Path(both.find_spec(CANONICAL).origin),OVERRIDE.resolve())
        self.assertEqual({name:path for name,path in both.modules.items()
                          if name not in (CANONICAL,DRAFT_CANONICAL)},
                         {name:(HERE/'runtime_overlay'/relative).resolve() for name,relative in mappings.items()})
        for parent in ('vllm_ascend','vllm_ascend.spec_decode',DRAFT_CANONICAL+'_other'):
            self.assertIsNone(both.find_spec(parent))

    def test_relative_draft_override_is_rejected(self):
        os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']='relative/proposer.py'
        with self.assertRaises(ImportError):
            finder()


if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(FinderChecks))
    report={'status':'PASS_EXACT_OPTIONAL_N16_FINDER_CPU_ONLY' if result.wasSuccessful() else 'FAIL',
        'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
        'canonical':CANONICAL,'no_override_mappings':17,'with_override_mappings':18,
        'draft_canonical':DRAFT_CANONICAL,'both_override_mappings':19,
        'framework_imports':[],'model_or_operator_calls':0,'service_actions':0}
    (HERE/'mooncake_optional_finder_cpu_result.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    raise SystemExit(not result.wasSuccessful())
