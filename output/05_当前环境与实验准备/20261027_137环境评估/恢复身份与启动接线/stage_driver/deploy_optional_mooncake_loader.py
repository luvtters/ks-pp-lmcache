"""Upload only the isolated launcher optional exact N16 mappings."""
from __future__ import annotations
import base64
import difflib
import gzip
import json
from pathlib import Path
import shlex
from deploy_private_candidate import LOADER,remote

HERE=Path(__file__).resolve().parent
SCRIPT=r'''import ast,base64,gzip,json,os,pathlib,sys
root=pathlib.Path('/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate')
launcher=root.parent/'stage-launcher/sitecustomize.py'
canonical='vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector'
override=pathlib.Path('/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/mooncake-n16/mooncake_connector_n16.py')
draft_canonical='vllm_ascend.spec_decode.llm_base_proposer'
draft_override=pathlib.Path('/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/draft-n16/llm_base_proposer_n16.py')
source=gzip.decompress(base64.b64decode(sys.stdin.buffer.read())).decode('utf-8')
ast.parse(source,filename=str(launcher))
old=launcher.read_text(encoding='utf-8')
previous=os.environ.pop('KS_N16_MOONCAKE_OVERRIDE',None)
previous_draft=os.environ.pop('KS_N16_DRAFT_PROPOSER_OVERRIDE',None)
saved_meta,saved_path=list(sys.meta_path),list(sys.path)
try:
    namespace={'__file__':str(launcher),'__name__':'ks_optional_exact_finder_check_only'}
    exec(compile(source,str(launcher),'exec'),namespace)
    baseline=namespace['_KSStageCandidateFinder']()
    manifest=json.loads((root/'private_candidate_manifest.json').read_text(encoding='utf-8'))
    expected={name:(root/relative).resolve() for name,relative in manifest['module_mappings'].items()}
    if baseline.modules != expected or len(baseline.modules)!=17 or baseline.find_spec(canonical) is not None:
        raise RuntimeError('no-override mappings changed')
    for name,path in expected.items():
        if pathlib.Path(baseline.find_spec(name).origin).resolve()!=path:
            raise RuntimeError('original exact source path changed: '+name)
    if not override.is_file():
        raise RuntimeError('prepare-owned private Mooncake source is not ready')
    os.environ['KS_N16_MOONCAKE_OVERRIDE']=str(override)
    overridden={'__file__':str(launcher),'__name__':'ks_optional_exact_finder_check_only'}
    exec(compile(source,str(launcher),'exec'),overridden)
    candidate=overridden['_KSStageCandidateFinder']()
    if len(candidate.modules)!=18 or pathlib.Path(candidate.find_spec(canonical).origin).resolve()!=override:
        raise RuntimeError('optional exact Mooncake source mismatch')
    if {name:path for name,path in candidate.modules.items() if name!=canonical}!=expected:
        raise RuntimeError('override changed an original mapping')
    if not draft_override.is_file():
        raise RuntimeError('prepare-owned private draft proposer source is not ready')
    os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']=str(draft_override)
    both_namespace={'__file__':str(launcher),'__name__':'ks_optional_exact_finder_check_only'}
    exec(compile(source,str(launcher),'exec'),both_namespace)
    both=both_namespace['_KSStageCandidateFinder']()
    if len(both.modules)!=19 or pathlib.Path(both.find_spec(draft_canonical).origin).resolve()!=draft_override:
        raise RuntimeError('optional exact draft source mismatch')
    if pathlib.Path(both.find_spec(canonical).origin).resolve()!=override:
        raise RuntimeError('draft override changed the existing Mooncake mapping')
    if {name:path for name,path in both.modules.items() if name not in (canonical,draft_canonical)}!=expected:
        raise RuntimeError('draft override changed an original mapping')
    parents=('vllm','vllm_ascend','vllm_ascend.distributed','vllm_ascend.distributed.kv_transfer',
             'vllm_ascend.distributed.kv_transfer.kv_p2p',canonical+'_other')
    if any(both.find_spec(name) is not None for name in (*parents,'vllm_ascend.spec_decode',draft_canonical+'_other')):
        raise RuntimeError('finder must only map exact modules')
    if any(pathlib.Path(value or '.').resolve()==root for value in sys.path):
        raise RuntimeError('candidate partial source root leaked onto sys.path')
    frameworks=[name for name in sys.modules if any(name==prefix or name.startswith(prefix+'.')
        for prefix in ('vllm','vllm_ascend','lmcache','torch','torch_npu'))]
    if frameworks:
        raise RuntimeError('unexpected framework import: '+repr(frameworks))
finally:
    sys.meta_path[:]=saved_meta
    sys.path[:]=saved_path
    if previous is None:
        os.environ.pop('KS_N16_MOONCAKE_OVERRIDE',None)
    else:
        os.environ['KS_N16_MOONCAKE_OVERRIDE']=previous
    if previous_draft is None:
        os.environ.pop('KS_N16_DRAFT_PROPOSER_OVERRIDE',None)
    else:
        os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']=previous_draft
temporary=launcher.with_name('sitecustomize.py.ksluv.tmp')
temporary.write_text(source,encoding='utf-8')
temporary.replace(launcher)
print(json.dumps({'status':'PASS_PRIVATE_LAUNCHER_N16_OPTIONALS_DEPLOYED_NOT_ACTIVATED',
    'target':str(launcher),'canonical':canonical,'optional_source':str(override),
    'without_override_exact_mappings':17,'with_override_exact_mappings':18,
    'draft_canonical':draft_canonical,'optional_draft_source':str(draft_override),
    'both_override_exact_mappings':19,
    'original_mappings_unchanged':True,'parent_packages_untouched':True,
    'candidate_root_off_sys_path':True,'source_files_written':1,'framework_imports':frameworks,
    'service_actions':0,'model_or_operator_calls':0,'old_source':old}))
'''


def main():
    packed=gzip.compress(LOADER.encode('utf-8'))
    report=json.loads(remote('docker exec -i ks-pp-m137-priv python -S -c '+shlex.quote(SCRIPT),base64.b64encode(packed)))
    old=report.pop('old_source')
    (HERE/'optional_n16_loader.patch').write_text(''.join(difflib.unified_diff(
        old.splitlines(True),LOADER.splitlines(True),fromfile='stage-launcher-before/sitecustomize.py',
        tofile='stage-launcher-after/sitecustomize.py')),encoding='utf-8')
    report['compressed_payload_bytes']=len(packed)
    (HERE/'optional_n16_loader_deployment.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report))


if __name__=='__main__':
    main()
