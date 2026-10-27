"""Local exact-module Balance mapping proposal; no server/loader deployment."""
from __future__ import annotations
import ast
import difflib
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath

HERE=Path(__file__).resolve().parent
BASE=HERE.parent/'受控就绪窗口诊断/current_stage_loader_snapshot.py'
RECEIPT=HERE.parent/'恢复身份与启动接线/stage_driver/private_candidate_deployment.json'
CANONICAL='vllm_ascend.patch.platform.patch_balance_schedule'
PROPOSED='/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/balance-scheduler-fix/patch_balance_schedule_fixed.py'

class PosixSourcePath(PurePosixPath):
    """CPU-only POSIX path semantics for a remote mapping; no file existence claim."""
    def resolve(self):return self

def main():
    old=BASE.read_text(encoding='utf-8')
    anchor='sys.meta_path.insert(0, _KSStageCandidateFinder())'
    assert old.count(anchor)==1
    line=f'_KSStageCandidateFinder.modules[{CANONICAL!r}] = Path({PROPOSED!r})\n'
    new=old.replace(anchor,'# CPU_ONLY_NOT_DEPLOYED: exact actual-P scheduler hook mapping.\n'+line+anchor)
    ast.parse(new)
    (HERE/'sitecustomize_balance_CANDIDATE.py').write_text(new,encoding='utf-8',newline='\n')
    (HERE/'loader_exact_map_candidate.patch').write_text(''.join(difflib.unified_diff(old.splitlines(True),new.splitlines(True),
        fromfile=str(BASE),tofile='CANDIDATE/sitecustomize.py')),encoding='utf-8')
    receipt=json.loads(RECEIPT.read_text(encoding='utf-8'));origins=receipt['origins']
    assert receipt['finder_mappings']==len(origins)==17 and CANONICAL not in origins
    (HERE/'actual17_origins_snapshot.json').write_text(json.dumps({'source_receipt':str(RECEIPT),
        'origins':origins},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    def mapping(text,moon,draft):
        tree=ast.parse(text)
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='_KSStageCandidateFinder')
        additions=[n for n in tree.body if isinstance(n,ast.Assign) and any(
            isinstance(t,ast.Subscript) and isinstance(t.value,ast.Attribute)
            and t.value.attr=='modules' for t in n.targets)]
        env={'importlib':importlib,'os':os,'Path':PosixSourcePath,
             '_ROOT':PosixSourcePath('/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate'),
             '_MANIFEST':{'module_mappings':origins}}
        keys=['KS_N16_MOONCAKE_OVERRIDE','KS_N16_DRAFT_PROPOSER_OVERRIDE'];saved={k:os.environ.get(k) for k in keys}
        try:
            for key,value in zip(keys,[moon,draft]):
                if value:os.environ[key]=value
                else:os.environ.pop(key,None)
            exec(compile(ast.fix_missing_locations(ast.Module(body=[cls]+additions,type_ignores=[])),
                         'actual_loader_class_exact_map_AST','exec'),env)
            finder=env['_KSStageCandidateFinder']()
            for parent in ['vllm','vllm.v1','lmcache','vllm_ascend','vllm_ascend.patch.platform']:
                assert finder.find_spec(parent) is None
            return finder.modules
        finally:
            for key,value in saved.items():
                if value is None:os.environ.pop(key,None)
                else:os.environ[key]=value
    cases=[]
    for label,moon,draft,expected in [('no_optional',None,None,18),
        ('mooncake_only','/opt/private_mooncake.py',None,19),
        ('draft_only',None,'/opt/private_draft.py',19),
        ('both_n16_optional','/opt/private_mooncake.py','/opt/private_draft.py',20)]:
        before=mapping(old,moon,draft);after=mapping(new,moon,draft)
        assert len(after)==expected and set(after)-set(before)=={CANONICAL}
        assert all(after[name]==path for name,path in before.items())
        assert str(after[CANONICAL])==PROPOSED
        cases.append({'case':label,'before':len(before),'after':len(after),'only_added':CANONICAL})
    result={'status':'PASS_CPU_ONLY_NOT_DEPLOYED','actual_base_origins':17,'cases':cases,
            'package_parent_namespaces_created':False,'filesystem_existence_checked':False,
            'framework_imported':False,'private_source_path_is_proposal':True,
            'NPU_actions':0,'server_runtime_modified':False,'deployed':False}
    (HERE/'loader_cpu_result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
