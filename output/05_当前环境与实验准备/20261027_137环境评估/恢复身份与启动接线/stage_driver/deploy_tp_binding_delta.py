"""Upload only the CPU-checked TP binding/trace helper, for next startup."""
from __future__ import annotations
import base64
import difflib
import gzip
import json
from pathlib import Path
import shlex
from deploy_private_candidate import remote

HERE = Path(__file__).resolve().parent
RELATIVE = 'vllm/v1/core/sched/ks_stage_reservation.py'
SCRIPT = r'''import ast,base64,gzip,json,pathlib,sys
root=pathlib.Path('/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate')
payload=json.loads(gzip.decompress(base64.b64decode(sys.stdin.buffer.read())))
relative='vllm/v1/core/sched/ks_stage_reservation.py'
path=root/relative
old=path.read_text(encoding='utf-8')
if 'this diagnostic requires unchanged TP8/PP2 workers' not in old:
    raise RuntimeError('private helper changed since reviewed TP8 source; do not overwrite')
tree=ast.parse(payload['source'],filename=str(path))
names={node.name for node in tree.body if isinstance(node,ast.FunctionDef)}
if not {'bind','_trace_decision','try_partial_promotion'} <= names:
    raise RuntimeError('binding/trace delta is incomplete')
temporary=path.with_name(path.name+'.ksluv.tmp')
temporary.write_text(payload['source'],encoding='utf-8')
temporary.replace(path)
manifest_path=root/'private_candidate_manifest.json'
manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
manifest['scope']=payload['scope']
manifest['latest_private_delta']={'name':'actual-config TP/PP2 binding and partial admission observations',
    'source_files_changed':[relative],'activation':'next module import only; running process is not reloaded',
    'cpu_evidence':payload['evidence'],'max_tokens_supported':1,'scheduling_return_logic_changed':False,
    'service_actions':0,'npu_actions':0}
manifest_path.write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'status':'PASS_PRIVATE_TP_BINDING_DELTA_DEPLOYED_NOT_RELOADED',
    'target':str(path),'source_files_changed':1,'ast_valid':True,'old_source':old,
    'module_mappings':len(manifest['module_mappings']),'cpu_evidence':payload['evidence'],
    'max_tokens_supported':1,'service_actions':0,'npu_actions':0}))
'''


def main():
    unified=json.loads((HERE/'unified_overlay_manifest.json').read_text(encoding='utf-8'))
    source=(HERE/'runtime_overlay'/RELATIVE).read_text(encoding='utf-8')
    packed=gzip.compress(json.dumps({'source':source,'scope':unified['scope'],
        'evidence':unified['evidence']}).encode('utf-8'))
    report=json.loads(remote('docker exec -i ks-pp-m137-priv python -S -c '+shlex.quote(SCRIPT),base64.b64encode(packed)))
    old=report.pop('old_source')
    delta=''.join(difflib.unified_diff(old.splitlines(True),source.splitlines(True),
        fromfile='private-before/'+RELATIVE,tofile='private-after/'+RELATIVE))
    (HERE/'tp_binding_delta.patch').write_text(delta,encoding='utf-8')
    report['compressed_payload_bytes']=len(packed)
    (HERE/'tp_binding_private_deployment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    main()
