"""Update only prepared proxy/bench material; read-only listener check, no start."""
from pathlib import Path
import base64
import json
import shlex
import subprocess

HERE=Path(__file__).resolve().parent
OUT=HERE/'reduced-pd-prepared'
REMOTE='/opt/ks-stage-ready-luv-20261027/reduced-pd'
names=['startup/proxy.native.sh','README.md','topology-and-port-plan.json']
names.extend(str(p.relative_to(OUT)).replace('\\','/') for p in (OUT/'startup').glob('bench-*.native.sh'))
for name in names:
    p=OUT/name
    p.write_text(p.read_text(encoding='utf-8').replace('18082','18083'),encoding='utf-8',newline='\n')
generator=HERE/'prepare_reduced_pd_private.py'
generator.write_text(generator.read_text(encoding='utf-8').replace('18082','18083'),encoding='utf-8',newline='\n')
payload={'files':{name:base64.b64encode((OUT/name).read_bytes()).decode() for name in names}}
code=r'''
import base64,json,subprocess,sys
from pathlib import Path
root=Path('/opt/ks-stage-ready-luv-20261027/reduced-pd');p=json.load(sys.stdin)
if root.resolve()!=root:raise ValueError('private root symlink')
listeners=[]
for name in ['/proc/net/tcp','/proc/net/tcp6']:
 for row in Path(name).read_text().splitlines()[1:]:
  cols=row.split()
  if cols[3]=='0A' and int(cols[1].split(':')[1],16)==18083:listeners.append(row.strip())
listener='\n'.join(listeners)
for name,data in p['files'].items():
 path=root/name
 if path.is_symlink() or '..' in Path(name).parts:raise ValueError('unsafe private file')
 path.write_bytes(base64.b64decode(data))
 if name.endswith('.sh'):subprocess.run(['bash','-n',str(path)],check=True)
print(json.dumps({'proxy_port':18083,'port_free_at_check':not bool(listener),'listener':listener,
                  'files':list(p['files']),'syntax':'PASS','service_action':False}))
'''
encoded=base64.b64encode(code.encode()).decode()
command=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
          shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                   'root@115.120.85.137',command],input=json.dumps(payload).encode(),capture_output=True,timeout=60)
if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
receipt=json.loads(r.stdout)
(HERE/'reduced_pd_proxy_port_revision.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
