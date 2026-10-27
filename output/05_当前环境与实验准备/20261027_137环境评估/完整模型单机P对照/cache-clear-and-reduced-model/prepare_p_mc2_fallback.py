"""Authorized P-only official env fallback and benchmark output-path correction.

Only private startup text is deployed. Never starts/stops a service or touches D.
"""
from pathlib import Path
import base64
import json
import shlex
import subprocess

HERE=Path(__file__).resolve().parent
OUT=HERE/'reduced-pd-prepared'
REMOTE='/opt/ks-stage-ready-luv-20261027/reduced-pd'
names=[]
for group in ['original-async','stage-candidate']:
    name=f'startup/p-{group}.env.native.sh';path=OUT/name
    text=path.read_text(encoding='utf-8')
    if 'export VLLM_ASCEND_ENABLE_FUSED_MC2=0' not in text:
        text+='export VLLM_ASCEND_ENABLE_FUSED_MC2=0\n'
        path.write_text(text,encoding='utf-8',newline='\n')
    names.append(name)
    for phase in ['cold','warm']:
        name=f'startup/bench-{group}-{phase}.native.sh';path=OUT/name
        text=path.read_text(encoding='utf-8').replace(f'/results/{group}/{phase}',f'/results/{group}-{phase}')
        path.write_text(text,encoding='utf-8',newline='\n');names.append(name)
generator=HERE/'prepare_reduced_pd_private.py';text=generator.read_text(encoding='utf-8')
text=text.replace('/results/{group}/{phase}','/results/{group}-{phase}')
if 'export VLLM_PP_LAYER_PARTITION=8,8\nexport VLLM_ASCEND_ENABLE_FUSED_MC2=0' not in text:
    text=text.replace('export VLLM_PP_LAYER_PARTITION=8,8\n','export VLLM_PP_LAYER_PARTITION=8,8\nexport VLLM_ASCEND_ENABLE_FUSED_MC2=0\n')
generator.write_text(text,encoding='utf-8',newline='\n')
readme=OUT/'README.md';text=readme.read_text(encoding='utf-8')
marker='## P 预热融合通信路径调整'
if marker not in text:
    text+='''

## P 预热融合通信路径调整

第三次N16加载已越过target/draft权重构造，末段实际模型加载约24.17GB；P在13:33:06 UTC的profile dummy-run中，于target MoE调用 `DispatchFFNCombine` 发生EZ1008 tiling失败。两D已ready。实际137 selector按官方 `VLLM_ASCEND_ENABLE_FUSED_MC2`：值0时小token选MC2、大token选ALLTOALL，避开该融合算子；没有找到EP>=8最小rank硬限制。C++另有HCCL窗口容量检查，TP4每rank token变多可能相关，因未记录实参M不能确认为本次唯一原因。

用户已批准P原异步与候选共同在各自P env追加 `export VLLM_ASCEND_ENABLE_FUSED_MC2=0`，覆盖common原值1；D env/common及已运行D/cache/proxy均不改。TP4/PP2、8,8、CP、MTP3、模型和客户输入/输出均不改。这是减层PD两ON组共同必要启动差异，不当作候选独享收益。官方配置在FUSED_MC2=1时会自动禁用多流共享专家重叠；改0后原native的multistream_overlap_shared_expert=true不再受该条件禁用，此官方连带行为应与性能边界一起记录。

四份benchmark只修正结果目录为管理器约定的 `results/original-async-cold`、`original-async-warm`、`stage-candidate-cold`、`stage-candidate-warm`，数据、原输出300、N1/C1、预热0及其余参数不改。proxy仍18083。此准备没有启停任何服务或执行benchmark，非融合路径能否在本形态实际完成仍待本轮运行验收。
'''
    readme.write_text(text,encoding='utf-8',newline='\n')
names.append('README.md')
payload={name:base64.b64encode((OUT/name).read_bytes()).decode() for name in names}
code=r'''
import base64,json,subprocess,sys
from pathlib import Path
root=Path('/opt/ks-stage-ready-luv-20261027/reduced-pd');data=json.load(sys.stdin)
if root.resolve()!=root:raise ValueError('unsafe private root')
for name,blob in data.items():
 path=root/name
 if path.is_symlink() or '..' in Path(name).parts:raise ValueError('unsafe destination')
 path.write_bytes(base64.b64decode(blob))
 if name.endswith('.sh'):subprocess.run(['bash','-n',str(path)],check=True)
print(json.dumps({'files':list(data),'syntax':'PASS','P_env':'VLLM_ASCEND_ENABLE_FUSED_MC2=0',
                  'D_files_changed':False,'service_action':False}))
'''
encoded=base64.b64encode(code.encode()).decode()
cmd=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
     shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                   'root@115.120.85.137',cmd],input=json.dumps(payload).encode(),capture_output=True,timeout=60)
if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
result=json.loads(r.stdout)
(HERE/'P_mc2_fallback_deployment.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,indent=2))
