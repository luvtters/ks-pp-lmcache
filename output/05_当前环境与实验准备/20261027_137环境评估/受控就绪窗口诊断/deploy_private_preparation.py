"""Prepare only new diagnostic files; never modifies active runtime or services."""
from pathlib import Path
import base64
import json
import shlex
import subprocess

HERE=Path(__file__).resolve().parent
BASE=HERE.parent/'完整模型单机P对照/cache-clear-and-reduced-model/reduced-pd-prepared'
REMOTE='/opt/ks-stage-ready-luv-20261027/reduced-pd'
DIAG=REMOTE+'/runtime/diagnostic-back-ready'
CANONICAL='lmcache.integration.vllm.vllm_multi_process_adapter'
source_name='vllm_multi_process_adapter_DIAGNOSTIC_ONLY.py'
loader=(HERE/'current_stage_loader_snapshot.py').read_text(encoding='utf-8')
anchor='sys.meta_path.insert(0, _KSStageCandidateFinder())'
assert loader.count(anchor)==1
loader=loader.replace(anchor,
    '# DIAGNOSTIC_ONLY: only this existing exact module mapping is replaced.\n'+
    f'_KSStageCandidateFinder.modules[{CANONICAL!r}] = Path({(DIAG+"/"+source_name)!r})\n'+anchor)
(HERE/'sitecustomize_DIAGNOSTIC_ONLY.py').write_text(loader,encoding='utf-8',newline='\n')
env=(BASE/'startup/p-stage-candidate.env.native.sh').read_text(encoding='utf-8')
env=env.replace('stage-candidate-trace','stage-diagnostic-trace')
assert env.count('export PYTHONPATH=')==1
env=env.replace('export PYTHONPATH=',f'export PYTHONPATH={DIAG}:')
env+='export KS_LMCACHE_DIAGNOSTIC_BACK_READY_DELAY_MS=500\n'
(HERE/'p-stage-diagnostic.env.native.sh').write_text(env,encoding='utf-8',newline='\n')
serve=(BASE/'startup/p-stage-candidate.serve.native.sh').read_text(encoding='utf-8')
serve=serve.replace('p-stage-candidate.env','p-stage-diagnostic.env')
(HERE/'p-stage-diagnostic.serve.native.sh').write_text(serve,encoding='utf-8',newline='\n')
bench=(BASE/'startup/bench-stage-candidate-warm.native.sh').read_text(encoding='utf-8')
assert bench.count('/results/stage-candidate-warm ')==1
bench=bench.replace('/results/stage-candidate-warm ','/results/stage-diagnostic-warm ')
(HERE/'bench-stage-diagnostic-warm.native.sh').write_text(bench,encoding='utf-8',newline='\n')
assets={
 'runtime/diagnostic-back-ready/'+source_name:(HERE/source_name).read_bytes(),
 'runtime/diagnostic-back-ready/sitecustomize.py':loader.encode(),
 'startup/p-stage-diagnostic.env.native.sh':env.encode(),
 'startup/p-stage-diagnostic.serve.native.sh':serve.encode(),
 'startup/bench-stage-diagnostic-warm.native.sh':bench.encode(),
}
payload={'assets':{name:base64.b64encode(blob).decode() for name,blob in assets.items()},
         'expected_base_loader':base64.b64encode((HERE/'current_stage_loader_snapshot.py').read_bytes()).decode()}
code=r'''
import ast,base64,json,os,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path('/opt/ks-stage-ready-luv-20261027/reduced-pd')
base=Path('/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher/sitecustomize.py')
if root.resolve()!=root:raise ValueError('private root mismatch')
if base.read_bytes()!=base64.b64decode(p['expected_base_loader']):raise ValueError('base19 loader changed; review required')
for name,blob in p['assets'].items():
 rel=Path(name);target=root/rel
 if rel.is_absolute() or '..' in rel.parts or target.is_symlink():raise ValueError('unsafe private destination')
 if not (name.startswith('runtime/diagnostic-back-ready/') or name in {
  'startup/p-stage-diagnostic.env.native.sh','startup/p-stage-diagnostic.serve.native.sh','startup/bench-stage-diagnostic-warm.native.sh'}):raise ValueError('outside new diagnostic ownership')
 target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(base64.b64decode(blob))
 if name.endswith('.py'):ast.parse(target.read_text())
 if name.endswith('.sh'):subprocess.run(['bash','-n',str(target)],check=True)
os.environ['KS_N16_MOONCAKE_OVERRIDE']=str(root/'runtime/mooncake-n16/mooncake_connector_n16.py')
os.environ['KS_N16_DRAFT_PROPOSER_OVERRIDE']=str(root/'runtime/draft-n16/llm_base_proposer_n16.py')
def read_map(path):
 old_meta=list(sys.meta_path);old_path=list(sys.path)
 try:
  ns={'__file__':str(path)};exec(compile(path.read_text(),str(path),'exec'),ns)
  finder=ns['_KSStageCandidateFinder']()
  sources=dict(finder.modules)
  for name,source in sources.items():
   spec=finder.find_spec(name)
   if spec is None or Path(spec.origin)!=source:raise ValueError('finder origin mismatch '+name)
  return sources
 finally:sys.meta_path[:]=old_meta;sys.path[:]=old_path
before=read_map(base);after=read_map(root/'runtime/diagnostic-back-ready/sitecustomize.py')
canonical='lmcache.integration.vllm.vllm_multi_process_adapter'
changed=[name for name in before if before[name]!=after[name]]
if len(before)!=19 or len(after)!=19 or set(before)!=set(after) or changed!=[canonical]:raise ValueError('not an exact one-map diagnostic override')
print(json.dumps({'root':str(root),'files':list(p['assets']),'map_count_before':len(before),'map_count_after':len(after),
 'changed_modules':changed,'optional_mooncake_and_proposer_preserved':True,'finder_cpu':'PASS','bash_syntax':'PASS',
 'source':'runtime/diagnostic-back-ready/vllm_multi_process_adapter_DIAGNOSTIC_ONLY.py',
 'services_changed':False,'requests_sent':0,'active_runtime_files_changed':False,'framework_imported':False}))
'''
encoded=base64.b64encode(code.encode()).decode()
command=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
         shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                   'root@115.120.85.137',command],input=json.dumps(payload).encode(),capture_output=True,timeout=75)
if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
receipt=json.loads(r.stdout)
(HERE/'private_preparation_deployment.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
