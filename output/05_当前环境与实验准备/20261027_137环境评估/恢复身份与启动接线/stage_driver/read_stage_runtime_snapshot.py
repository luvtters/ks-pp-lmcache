"""One read-only runtime trace/log snapshot; never import a framework."""
from __future__ import annotations
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import shlex
from deploy_private_candidate import remote

HERE = Path(__file__).resolve().parent
SCRIPT = r'''from pathlib import Path
from datetime import datetime,timezone
import json
root=Path('/opt/ks-stage-ready-luv-20261027/standalone-p/results')
paths=sorted((root/'stage-candidate-trace').glob('*.jsonl'))
paths += [root/'p-stage-candidate.log',root/'first-token/stage-first-token-warm1/console.log']
files=[]
for path in paths:
    if not path.is_file():
        files.append({'path':str(path),'exists':False})
        continue
    with path.open('r',encoding='utf-8',errors='replace') as stream:
        content=stream.read()
    files.append({'path':str(path),'exists':True,'content':content,'characters':len(content)})
print(json.dumps({'utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':files}))
'''


def main():
    snapshot=json.loads(remote('docker exec -i ks-pp-m137-priv python -S -c '+shlex.quote(SCRIPT)))
    target=HERE/'runtime_snapshots'/datetime.now().strftime('%Y%m%d-%H%M%S')
    target.mkdir(parents=True,exist_ok=False)
    (target/'raw.json').write_text(json.dumps(snapshot,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    rows=[]
    parse_errors=[]
    for item in snapshot['files']:
        if item['path'].endswith('.jsonl'):
            for number,line in enumerate(item.get('content','').splitlines(),1):
                try:
                    row=json.loads(line)
                    row['source_file']=item['path']
                    row['source_line']=number
                    rows.append(row)
                except json.JSONDecodeError:
                    parse_errors.append({'file':item['path'],'line':number,'text':line})
    rows.sort(key=lambda row:row.get('monotonic_ns',0))
    log=next((item.get('content','') for item in snapshot['files'] if item['path'].endswith('/p-stage-candidate.log')),'')
    loglines=log.splitlines()
    findings=[{'line':index,'text':line} for index,line in enumerate(loglines,1)
              if re.search(r'Traceback|Error|ERROR|Exception|Assertion|KS|bootstrap|stage|observer|restore',line)]
    summary={'snapshot':str(target/'raw.json'),'captured_utc':snapshot['utc'],
        'trace_files':len([item for item in snapshot['files'] if item['path'].endswith('.jsonl')]),
        'trace_rows':len(rows),'phase_counts':dict(Counter(row.get('phase') for row in rows)),
        'phases_by_stage':dict(Counter(str((row.get('phase'),row.get('stage'))) for row in rows)),
        'worker_ids':sorted({row['worker_id'] for row in rows if type(row.get('worker_id')) is int}),
        'parse_errors':parse_errors,'log_findings':findings,'trace_rows_sorted':rows,
        'warm_console':next((item.get('content','') for item in snapshot['files'] if item['path'].endswith('/console.log')),'')}
    (target/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({key:value for key,value in summary.items() if key not in ('trace_rows_sorted','log_findings')},ensure_ascii=False))
    print(json.dumps({'log_findings_last_35':findings[-35:],'trace_rows_first_12':rows[:12],'trace_rows_last_20':rows[-20:]},ensure_ascii=False))


if __name__=='__main__':
    main()
