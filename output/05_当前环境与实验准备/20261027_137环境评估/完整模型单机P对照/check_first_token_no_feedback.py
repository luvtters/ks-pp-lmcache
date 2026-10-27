"""Focused real-method CPU proof for max1 P mode; not an HCCL reproduction."""
from pathlib import Path
from types import SimpleNamespace as NS
from contextlib import nullcontext
import ast
import copy
import json
import shlex

HERE=Path(__file__).resolve().parent
TARGET=HERE/'runtime/local-completion'
source=ast.parse((HERE/'check_local_completion_methods.py').read_text(encoding='utf-8'))
body=[node for node in source.body if isinstance(node,ast.FunctionDef) and node.name in ['methods','sample_case']]
ns={'Path':Path,'NS':NS,'nullcontext':nullcontext,'ast':ast,'copy':copy,
    'runner_path':TARGET/'model_runner_local.py'}
exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])),str(HERE/'check_local_completion_methods.py'),'exec'),ns)
# Execute the actual private sample_tokens for each PP stage in actual async mode.
# The helper asserts no receive/send and that final sampled/draft values and
# finalize_kv_connector remain intact on the last stage.
ns['sample_case'](False,False,True)
ns['sample_case'](False,True,True)
changes=[]
for name in ['p-local-first-token.serve.native.sh','p-local-complete.serve.native.sh']:
 p=HERE/name
 old=p.read_text(encoding='utf-8')
 argv=shlex.split('\n'.join(v for v in old.split('\n') if not v.startswith('#')).replace('\\\n',' '))
 original=argv[argv.index('--kv-transfer-config')+1]
 config=json.loads(original)
 extra=config['kv_connector_extra_config']
 assert config['kv_role']=='kv_producer' and extra['connectors'][0]['kv_role']=='kv_both'
 extra['ks_local_p_complete']=False
 extra['ks_local_p_first_token_only']=True
 new=json.dumps(config,separators=(',',':'))
 changed=old.replace(original,new,1)
 changed=changed.replace('Shared local-P return adapter.','First-token only; preserve original producer PP sample-feedback skip.')
 p.write_text(changed,encoding='utf-8',newline='\n')
 changes.append({'file':name,'config':config})
receipt={'status':'FOCUSED_ACTUAL_METHODS_CPU_PASS_NOT_HCCL_CAUSAL_PROOF',
 'passed':2,'async_scheduling':True,'request_scope':'max_tokens=1 only',
 'config_change':{'ks_local_p_complete':[True,False],'ks_local_p_first_token_only':[None,True]},
 'sample_feedback':'original producer PP skip retained; no new HCCL sampled-token broadcast',
 'sampled_and_draft_values':'unchanged CPU fixtures; last stage still proposes MTP and finalizes KV',
 'LMCache17_pin_roles':'unchanged','runtime_HCCL_root_cause':'NOT_VERIFIED',
 'service_actions':[],'changes':changes}
(HERE/'first_token_no_feedback_cpu.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:v for k,v in receipt.items() if k!='changes'},ensure_ascii=False,indent=2))
