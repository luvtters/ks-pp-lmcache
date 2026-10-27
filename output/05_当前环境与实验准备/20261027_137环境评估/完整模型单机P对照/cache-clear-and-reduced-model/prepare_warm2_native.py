"""Duplicate approved warm commands with independent warm2 result directories."""
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
    source=OUT/f'startup/bench-{group}-warm.native.sh'
    text=source.read_text(encoding='utf-8')
    old=f'/results/{group}-warm '
    new=f'/results/{group}-warm2 '
    if text.count(old)!=1:raise ValueError(f'original warm result path not unique: {group}')
    name=f'startup/bench-{group}-warm2.native.sh'
    (OUT/name).write_text(text.replace(old,new),encoding='utf-8',newline='\n')
    names.append(name)
readme=OUT/'README.md';text=readme.read_text(encoding='utf-8')
marker='## 追加第二次同文热请求'
if marker not in text:
    text+='''

## 追加第二次同文热请求

主任务已确认N16原异步组cold/warm各1次均返回原300输出；首次warm TTFT为9.195s，存在首热化混杂的可能。用户已批准两ON组各追加一次相同warm2复核，不替换原cold/warm结果。两份 `bench-original-async-warm2.native.sh`／`bench-stage-candidate-warm2.native.sh`逐字复制既有warm命令，只将结果目录改成 `results/original-async-warm2`／`results/stage-candidate-warm2`。客户原文、原输出300、N1/C1、预热0、proxy18083及全部采样参数保持；本准备仅上传启动文件并做bash语法检查，没有发送请求或启停服务。
'''
    readme.write_text(text,encoding='utf-8',newline='\n')
names.append('README.md')
data={name:base64.b64encode((OUT/name).read_bytes()).decode() for name in names}
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
print(json.dumps({'files':list(data),'syntax':'PASS','difference':'warm2 result directories only',
                  'requests_sent':0,'service_action':False}))
'''
encoded=base64.b64encode(code.encode()).decode()
cmd=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
     shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                   'root@115.120.85.137',cmd],input=json.dumps(data).encode(),capture_output=True,timeout=60)
if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
receipt=json.loads(r.stdout)
(HERE/'warm2_native_deployment.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
