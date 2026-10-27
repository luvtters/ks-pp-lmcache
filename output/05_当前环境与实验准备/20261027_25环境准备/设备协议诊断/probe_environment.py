"""在自己的诊断容器中只读检查工具和地址解析，不导入推理框架。"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess


if os.environ.get('KS_CONTAINER') != 'ks-luv-m25-stage-probe':
    raise SystemExit('Run only inside ks-luv-m25-stage-probe.')
report = {
    'status': 'PRECHECK_ENVIRONMENT', 'hostname': socket.gethostname(),
    'frameworks_imported': False, 'npu_tensors_allocated': False,
    'path': os.environ.get('PATH'), 'ld_library_path': os.environ.get('LD_LIBRARY_PATH'),
    'device_files': sorted(str(p) for p in Path('/dev').glob('davinci*')
                           if p.name[len('davinci'):].isdigit()),
    'name_resolution': {},
}
for host in dict.fromkeys(('liteserver-hps-805f-00003.novalocal', socket.gethostname(), 'localhost')):
    families = {}
    for name, family in (('IPv4', socket.AF_INET), ('IPv6', socket.AF_INET6)):
        try:
            values = socket.getaddrinfo(host, None, family, socket.SOCK_STREAM)
            families[name] = {'ok': True, 'addresses': sorted({value[4][0] for value in values})}
        except socket.gaierror as error:
            families[name] = {'ok': False, 'error': str(error)}
    report['name_resolution'][host] = families
tools = [shutil.which('npu-smi'), '/usr/local/Ascend/driver/tools/npu-smi',
         '/usr/local/Ascend/driver/bin/npu-smi']
report['npu_smi_on_path'] = shutil.which('npu-smi')
report['npu_smi_candidates'] = [path for path in dict.fromkeys(tools) if path and Path(path).is_file()]
if report['npu_smi_candidates']:
    path = report['npu_smi_candidates'][0]
    try:
        checked = subprocess.run([path, 'info'], capture_output=True, text=True, timeout=20)
        report['npu_smi_check'] = {
            'path': path, 'exit_code': checked.returncode,
            'stderr': checked.stderr[:2000], 'output_header': checked.stdout.splitlines()[:4],
        }
    except subprocess.TimeoutExpired:
        report['npu_smi_check'] = {'path': path, 'status': 'QUERY_TIMEOUT'}
print(json.dumps(report, ensure_ascii=False))
