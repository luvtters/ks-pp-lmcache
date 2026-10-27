"""Apply authorized P-only HCCL window size from the actual 413MB tiling error."""
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
    if 'export HCCL_BUFFSIZE=512' not in text:
        text+='export HCCL_BUFFSIZE=512\n'
        path.write_text(text,encoding='utf-8',newline='\n')
    names.append(name)
generator=HERE/'prepare_reduced_pd_private.py';text=generator.read_text(encoding='utf-8')
anchor='export VLLM_PP_LAYER_PARTITION=8,8\nexport VLLM_ASCEND_ENABLE_FUSED_MC2=0\n'
if anchor+'export HCCL_BUFFSIZE=512\n' not in text:
    text=text.replace(anchor,anchor+'export HCCL_BUFFSIZE=512\n')
    generator.write_text(text,encoding='utf-8',newline='\n')
readme=OUT/'README.md';text=readme.read_text(encoding='utf-8')
marker='## P 普通MC2窗口容量修正'
if marker not in text:
    text+='''

## P 普通MC2窗口容量修正

env0后13:45:11 UTC的新首错已明确：profile_run为mc2_tokens_capacity做预热，在普通 `npu_moe_distribute_dispatch_v2`／`aclnnMoeDistributeDispatchV4` 的tiling检查报 `HCCL_BUFFSIZE_EP is too SMALL`。日志实际参数为maxBs64、h6144、epWorldSize4、localMoeExpertNum64、k8；算子要求413MB，实际HCCL/CCL窗口为400MB。这是已确认窗口不足，不据此宣称EP4不受支持。

已按用户批准，仅P原异步／候选两份env追加 `export HCCL_BUFFSIZE=512` 覆盖common400，保留 `VLLM_ASCEND_ENABLE_FUSED_MC2=0`。D/common文件和已就绪D/cache/proxy均不改，P TP/PP/CP/MTP3、batch/seq、模型、客户数据、输出及压测口径均不变。512超过实测所需413MB；这只完成必要配置准备，是否通过设备预热及PD请求仍由本轮实际运行记录验收。本脚本不启动、停止服务或发请求。
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
print(json.dumps({'files':list(data),'syntax':'PASS','P_env':{'HCCL_BUFFSIZE':512,'VLLM_ASCEND_ENABLE_FUSED_MC2':0},
                  'D_files_changed':False,'service_action':False}))
'''
encoded=base64.b64encode(code.encode()).decode()
cmd=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
     shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                   'root@115.120.85.137',cmd],input=json.dumps(payload).encode(),capture_output=True,timeout=60)
if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
receipt=json.loads(r.stdout)
(HERE/'P_hccl512_deployment.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
