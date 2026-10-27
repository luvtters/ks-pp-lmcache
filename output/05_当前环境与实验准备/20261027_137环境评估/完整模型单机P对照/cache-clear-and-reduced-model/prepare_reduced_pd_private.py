"""Generate private PD material; deploy only with explicit --materialize.

Deployment builds metadata/symlink model view only. It never starts a service,
imports a device framework, clears caches, or edits the original checkpoint.
"""
from pathlib import Path
import argparse
import ast
import base64
import json
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
SINGLE = HERE.parent
PROJECT = next(p for p in HERE.parents if p.name == 'ks')
OUT = HERE/'reduced-pd-prepared'
REMOTE = '/opt/ks-stage-ready-luv-20261027/reduced-pd'
ORIGINAL = '/opt/ks-models/GLM-5.1-w8a8c8-MTP'
VIEW = REMOTE+'/model-N16'
HISTORICAL = PROJECT/'output/02_部署基线与接入打通/03_成功ON与复现配置/原形态P侧接入打通_20261016/d'
CUSTOMER = PROJECT/'output/05_当前环境与实验准备/20261024_客户数据压测准备/实际验证配置'


def compact(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False)


def native(arguments, note):
    lines = ['#!/usr/bin/env bash', '# PREPARED ONLY. '+note]
    chunks = [arguments[:3]]
    for item in arguments[3:]:
        if item.startswith('--'):
            chunks.append([item])
        else:
            chunks[-1].append(item)
    lines.extend(('' if i == 0 else '  ') + ' '.join(shlex.quote(v) for v in chunk)
                 + (' \\' if i < len(chunks)-1 else '')
                 for i,chunk in enumerate(chunks))
    return '\n'.join(lines)+'\n'


def set_option(arguments, name, value):
    if name in arguments:
        arguments[arguments.index(name)+1] = str(value)
    else:
        arguments.extend([name, str(value)])


def load_native(path):
    text = path.read_text(encoding='utf-8')
    return shlex.split(text[text.index('vllm serve '):].replace('\\\n', ' '))


def write(relative, text):
    path = OUT/relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8', newline='\n')


def generate():
    OUT.mkdir(exist_ok=True)
    topology = {'prefill':{'dp_size':1,'tp_size':4,'pp_size':2,'pp_layer_partition':'8,8'},
                'decode':{'dp_size':2,'tp_size':4,'pp_size':1}}
    mooncake = {'kv_connector':'MooncakeConnectorV1','kv_role':'kv_producer',
                'kv_port':28000,'kv_connector_extra_config':{'use_ascend_direct':True,**topology}}
    lmcache = {'kv_connector':'LMCacheMPConnector','kv_role':'kv_both',
               'kv_connector_module_path':'lmcache.integration.vllm.lmcache_mp_connector',
               'kv_connector_extra_config':{'lmcache.mp.host':'tcp://127.0.0.1',
                                           'lmcache.mp.port':5565,'lmcache.mp.autostart':False,
                                           'lmcache.mp.nonblocking_lookup_status':False}}
    kv = {'kv_role':'kv_producer','kv_port':28000,'engine_id':'ks-luv-N16-p',
          'kv_connector':'MultiConnector','kv_connector_extra_config':{
              'ks_local_p_complete':False,'ks_local_p_first_token_only':False,
              'connectors':[mooncake,lmcache]}}
    spec = {'num_speculative_tokens':3,'method':'deepseek_mtp','enforce_eager':True,'quantization':'ascend',
            'model':ORIGINAL}
    p = load_native(SINGLE/'p-local-complete.serve.native.sh')
    p[2] = VIEW
    for key,value in {'--tensor-parallel-size':4,'--speculative-config':compact(spec),
                      '--kv-transfer-config':compact(kv)}.items():
        set_option(p,key,value)
    for group in ['original-async','stage-candidate']:
        write(f'startup/p-{group}.serve.native.sh',native(p,
              f'N16 real PD P; source common.env and p-{group}.env; no local feedback.'))
    historical = json.loads((HISTORICAL/'baseline-d-launch.json').read_text(encoding='utf-8'))
    history = []
    for item in historical:
        argv = item['argv']
        history.append({'port':item['port'],'argv':argv,
                        'visible_devices':item.get('env',{}).get('ASCEND_RT_VISIBLE_DEVICES')})
    write('historical137_d_actual_argv.json',json.dumps(history,ensure_ascii=False,indent=2)+'\n')
    for rank,devices in [(0,'8,9,10,11'),(1,'12,13,14,15')]:
        d = load_native(CUSTOMER/f'd/d{rank}.serve.native.sh')
        d[2] = VIEW
        dkv = {'kv_connector':'MooncakeConnectorV1','kv_role':'kv_consumer','kv_port':29000,
               'engine_id':f'ks-luv-N16-d{rank}',
               'kv_connector_extra_config':{'use_ascend_direct':True,**topology}}
        for key,value in {'--data-parallel-size':2,'--data-parallel-rank':rank,
                          '--data-parallel-address':'192.168.0.223',
                          '--data-parallel-rpc-port':18300,
                          '--speculative-config':compact(spec),
                          '--kv-transfer-config':compact(dkv)}.items():
            set_option(d,key,value)
        write(f'startup/d{rank}.serve.native.sh',native(d,
              f'N16 real PD D rank{rank}/DP2; source common.env and d{rank}.env.'))
        write(f'startup/d{rank}.env.native.sh',f'''#!/usr/bin/env bash
export ASCEND_RT_VISIBLE_DEVICES={devices}
unset VLLM_PP_LAYER_PARTITION
export KS_SERVICE=d{rank}
export HOME={REMOTE}/home/d{rank} TMPDIR={REMOTE}/tmp/d{rank}
export XDG_CACHE_HOME={REMOTE}/cache/d{rank}
export HF_HOME="$XDG_CACHE_HOME/huggingface" VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub" HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
export PYTHONPATH={REMOTE}/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH
mkdir -p "$HOME" "$TMPDIR" "$XDG_CACHE_HOME"
''')
    common = (SINGLE/'common.env.native.sh').read_text(encoding='utf-8')
    common = common.replace('/opt/ks-stage-ready-luv-20261027/standalone-p',REMOTE)
    common = common.replace('export VLLM_PP_LAYER_PARTITION=41,37','unset VLLM_PP_LAYER_PARTITION')
    common = common.replace('export KS_P_COMMON_RUNTIME="$KS_P_SINGLE/runtime/common"',
                            'export KS_P_COMMON_RUNTIME="$KS_P_SINGLE/runtime/common"')
    common += '''export KS_ASCEND_PYTHONPATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:/usr/local/Ascend/ascend-toolkit/latest/opp/built-in/op_impl/ai_core/tbe:/usr/local/Ascend/cann-9.1.0/python/site-packages:/usr/local/Ascend/cann-9.1.0/opp/built-in/op_impl/ai_core/tbe
unset KS_PP_STAGE_DRIVER KS_PP_STAGE_EVENTS KS_CORE_STAGE_EVENT_PUMP KS_LMCACHE_STAGE_OBSERVER KS_STAGE_TRACE_DIR
'''
    write('startup/common.env.native.sh',common)
    p_env = f'''#!/usr/bin/env bash
export ASCEND_RT_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
export VLLM_PP_LAYER_PARTITION=8,8
export VLLM_ASCEND_ENABLE_FUSED_MC2=0
export HCCL_BUFFSIZE=512
export HOME={REMOTE}/home/p TMPDIR={REMOTE}/tmp/p
export XDG_CACHE_HOME={REMOTE}/cache/p
export HF_HOME="$XDG_CACHE_HOME/huggingface" VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub" HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
mkdir -p "$HOME" "$TMPDIR" "$XDG_CACHE_HOME"
'''
    write('startup/p-original-async.env.native.sh',p_env+
          f'export PYTHONPATH={REMOTE}/runtime/p-original:{REMOTE}/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH\n')
    write('startup/p-stage-candidate.env.native.sh',p_env+f'''export KS_PP_STAGE_DRIVER=1 KS_PP_STAGE_EVENTS=1 KS_CORE_STAGE_EVENT_PUMP=1 KS_LMCACHE_STAGE_OBSERVER=1
export KS_STAGE_TRACE_DIR={REMOTE}/results/stage-candidate-trace
export PYTHONPATH=/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher:{REMOTE}/runtime/p-original:{REMOTE}/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH
''')
    write('runtime/common/sitecustomize.py',(SINGLE/'runtime/common/sitecustomize.py').read_text(encoding='utf-8'))
    write('runtime/common/lmcache_cache_context_pinned.py',
          (SINGLE/'runtime/common/lmcache_cache_context_pinned.py').read_text(encoding='utf-8'))
    write('runtime/p-original/lmcache_mp_connector_multi17.py',
          (SINGLE/'runtime/local-completion/lmcache_mp_connector_multi17.py').read_text(encoding='utf-8'))
    write('runtime/p-original/sitecustomize.py','''"""Private real-PD common pin + approved Multi17; no model-runner patch."""
import importlib.abc
import importlib.util
from pathlib import Path
import sys
class Finder(importlib.abc.MetaPathFinder):
    root=Path(__file__).resolve().parent
    modules={"lmcache.v1.platform.base.cache_context":root.parent/"common"/"lmcache_cache_context_pinned.py",
             "lmcache.integration.vllm.lmcache_mp_connector":root/"lmcache_mp_connector_multi17.py"}
    def find_spec(self,fullname,path=None,target=None):
        source=self.modules.get(fullname)
        return importlib.util.spec_from_file_location(fullname,source) if source else None
sys.meta_path.insert(0,Finder())
''')
    cache = (SINGLE/'cache.server.native.sh').read_text(encoding='utf-8')
    cache = cache.replace('ks-luv-standalone-p-20261027','ks-luv-reduced-pd-N16-20261027')
    write('startup/cache-server.native.sh',cache)
    write('startup/proxy.native.sh',f'''#!/usr/bin/env bash
# Source common.env. Real remote-prefill/decode semantics; never discards Mooncake.
python {REMOTE}/proxy_v023.py --host 127.0.0.1 --port 18083 --workers 1 --max-retries 1 --prefiller-hosts 192.168.0.223 --prefiller-ports 18100 --decoder-hosts 192.168.0.223 192.168.0.223 --decoder-ports 18200 18201
''')
    proxy = HISTORICAL/'proxy_v023.py'
    write('proxy_v023.py',proxy.read_text(encoding='utf-8'))
    write('data/original-row36.jsonl',(SINGLE/'data/first-token-original.jsonl').read_text(encoding='utf-8'))
    manifest_files = list((SINGLE/'data').glob('*first*manifest*.json*'))
    if manifest_files:
        write('data/source-first-token-manifest.json',manifest_files[0].read_text(encoding='utf-8'))
    write('data/diagnostic_scope.json',json.dumps({'original_row':36,'original_index':35,
          'original_request_id':'req-0353','recorded_glm_input_tokens':29696,
          'output_tokens':300,'text':'unchanged original','N':1,'C':1,'warmup':0,
          'scope':'N16 real PD mechanism only; one cold and one warm per ON group'},indent=2)+'\n')
    for group in ['original-async','stage-candidate']:
        for phase in ['cold','warm']:
            write(f'startup/bench-{group}-{phase}.native.sh',f'''#!/usr/bin/env bash
# Original row36 text/output300; one request; no synthetic data or first-token truncation.
vllm bench serve --backend openai --base-url http://127.0.0.1:18083 --endpoint /v1/completions --model glm-5 --tokenizer {ORIGINAL} --dataset-name custom --dataset-path {REMOTE}/data/original-row36.jsonl --custom-output-len -1 --disable-shuffle --skip-chat-template --no-oversample --num-prompts 1 --max-concurrency 1 --request-rate inf --num-warmups 0 --ready-check-timeout-sec 0 --temperature 0 --ignore-eos --seed 0 --percentile-metrics ttft,tpot,itl,e2el --metric-percentiles 50,90,95,99 --save-result --save-detailed --result-dir {REMOTE}/results/{group}-{phase} --result-filename benchmark.json
''')
    write('build_model_view.py',(HERE/'build_model_view.py').read_text(encoding='utf-8'))
    ports = {'P_API':18100,'P_master':18190,'D_APIs':[18200,18201],'DP_RPC':18300,
             'proxy':18083,'cache_rpc':5565,'cache_http':15565,
             'P_Mooncake_handshake':list(range(28000,28008)),
             'D0_Mooncake_handshake':list(range(29000,29004)),
             'D1_Mooncake_handshake':list(range(29004,29008)),
             'automatic':['Mooncake transfer-engine RPC ports','vLLM/HCCL internal auxiliary ports'],
             'runtime_listener_availability_checked':False}
    flat=[]
    for value in ports.values():
        if type(value) is int: flat.append(value)
        elif isinstance(value,list) and all(type(v) is int for v in value): flat.extend(value)
    if len(flat)!=len(set(flat)):
        raise ValueError('configured fixed-port collision')
    write('topology-and-port-plan.json',json.dumps({'topology':topology,'ports':ports,
        'P_devices':list(range(8)),'D0_devices':list(range(8,12)),'D1_devices':list(range(12,16)),
        'D_global_EP_world':8,'DP_external_hybrid_mode':'explicit rank per API; original argv pattern',
        'candidate_overlay_dependency':'standalone-p/runtime/stage-launcher (root maintained)',
        'not_runtime_validated':True},indent=2)+'\n')
    if (OUT/'runtime/mooncake-n16/mooncake_connector_n16.py').is_file():
        # Preserve the approved common single-file override on later native revisions.
        from prepare_mooncake_n16_overlay import update_loaders
        update_loaders()
    for path in OUT.rglob('*.py'):
        ast.parse(path.read_text(encoding='utf-8'))
    return ports


def deploy(materialize, native_only=False):
    assets={str(p.relative_to(OUT)).replace('\\','/'):base64.b64encode(p.read_bytes()).decode()
            for p in OUT.rglob('*') if p.is_file()}
    if native_only:
        chosen={'README.md','startup/p-original-async.serve.native.sh','startup/p-stage-candidate.serve.native.sh'}
        assets={name:blob for name,blob in assets.items() if name in chosen}
    payload={'root':REMOTE,'assets':assets,'materialize':materialize,'native_only':native_only}
    program=r'''
import ast,base64,json,os,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root'])
if str(root)!='/opt/ks-stage-ready-luv-20261027/reduced-pd':raise ValueError('private root mismatch')
root.mkdir(parents=True,exist_ok=True)
if root.resolve()!=root:raise ValueError('private root symlink forbidden')
for name,blob in p['assets'].items():
 rel=Path(name)
 if rel.is_absolute() or '..' in rel.parts:raise ValueError('unsafe asset path')
 target=root/rel
 target.parent.mkdir(parents=True,exist_ok=True)
 if target.is_symlink():raise ValueError('refusing symlink asset overwrite')
 target.write_bytes(base64.b64decode(blob))
for part in ['home','tmp','cache','work','results']:(root/part).mkdir(exist_ok=True)
env=dict(os.environ,HOME=str(root/'home'),TMPDIR=str(root/'tmp'),PYTHONDONTWRITEBYTECODE='1')
for path in [root/name for name in p['assets'] if name.endswith('.sh')]:
 subprocess.run(['bash','-n',str(path)],env=env,check=True)
for path in [root/name for name in p['assets'] if name.endswith('.py')]:ast.parse(path.read_text())
args=['python','-S',str(root/'build_model_view.py'),'--source','/opt/ks-models/GLM-5.1-w8a8c8-MTP',
      '--destination',str(root/'model-N16'),'--layers','16']
if p['materialize']:args.append('--materialize')
view={'untouched':True}
if not p['native_only']:
 r=subprocess.run(args,env=env,capture_output=True,text=True,check=True)
 view=json.loads(r.stdout)
print(json.dumps({'root':str(root),'asset_count':len(p['assets']),'view':view,
                  'services_started':False,'device_framework_imported':False,'bash_syntax':'PASS'}))
'''
    encoded = base64.b64encode(program.encode()).decode()
    remote_cmd = ("docker exec -i -e HOME="+REMOTE+"/home -e TMPDIR="+REMOTE+
                  "/tmp ks-pp-m137-priv python -S -c ") + shlex.quote(
        "import base64;exec(base64.b64decode("+repr(encoded)+"))")
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',
                 str(Path.home()/'.ssh/id_ed25519'),'root@115.120.85.137',remote_cmd],
                 input=json.dumps(payload).encode(),capture_output=True,timeout=100)
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors='replace')+'\n'+result.stdout.decode(errors='replace'))
    receipt=json.loads(result.stdout)
    receipt_name='reduced_pd_native_revision.json' if native_only else 'reduced_pd_private_deployment.json'
    (HERE/receipt_name).write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(receipt,ensure_ascii=False,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--materialize',action='store_true',help='upload private files and create model view; no service start')
    parser.add_argument('--native-only',action='store_true',help='upload only two P native commands and README; keep existing model view untouched')
    args=parser.parse_args()
    ports=generate()
    if args.native_only:
        deploy(False, native_only=True)
    elif args.materialize:
        deploy(True)
    else:
        print(json.dumps({'local_preparation':str(OUT),'remote_written':False,'ports':ports},indent=2))


if __name__=='__main__':
    main()
