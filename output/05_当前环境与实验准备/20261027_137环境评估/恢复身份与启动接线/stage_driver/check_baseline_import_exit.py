"""One common+local framework-import exit control; no model or operators."""
from __future__ import annotations

import json
from pathlib import Path
import shlex

from deploy_private_candidate import remote

HERE = Path(__file__).resolve().parent
CHECK = "import importlib,json; names=['vllm.v1.engine.core','vllm.v1.executor.multiproc_executor','vllm_ascend.worker.worker','lmcache.integration.vllm.vllm_multi_process_adapter']; print(json.dumps({name:importlib.import_module(name).__file__ for name in names}))"
SCRIPT = r'''from pathlib import Path
import json,shlex,subprocess
root=Path('/opt/ks-stage-ready-luv-20261027/standalone-p')
target=root/'results/baseline-private-import-exit.json'
if target.exists():
    raise RuntimeError('one baseline import was already recorded; no automatic repetition')
check=CHECK_LITERAL
command='source '+str(root/'startup/common.env.native.sh')+' && source '+str(root/'startup/local-completion.env.native.sh')+' && unset KS_PP_STAGE_DRIVER KS_PP_STAGE_EVENTS KS_CORE_STAGE_EVENT_PUMP KS_LMCACHE_STAGE_OBSERVER KS_STAGE_TRACE_DIR && exec python -B -c '+shlex.quote(check)
try:
    result=subprocess.run(['bash','--noprofile','--norc','-c',command],capture_output=True,text=True,timeout=40)
    out={'exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr,'timed_out':False}
except subprocess.TimeoutExpired as error:
    def decode(value): return value.decode('utf-8',errors='replace') if isinstance(value,bytes) else (value or '')
    out={'exit':None,'stdout':decode(error.stdout),'stderr':decode(error.stderr),'timed_out':True}
out.update(scope='ONE_COMMON_LOCAL_FRAMEWORK_IMPORT_EXIT_CONTROL_NO_MODEL_OR_OPERATORS',
    imported_names=['vllm.v1.engine.core','vllm.v1.executor.multiproc_executor','vllm_ascend.worker.worker','lmcache.integration.vllm.vllm_multi_process_adapter'],
    candidate_environment_sourced=False,stage_flags_unset=True,trace_unset=True,
    command=command,service_actions=0,model_loads=0,operator_tests=0)
target.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps(out))
'''.replace('CHECK_LITERAL', repr(CHECK))


def main():
    target = HERE / 'import_exit_baseline_comparison.json'
    if target.exists():
        raise RuntimeError('one baseline import was already recorded locally; do not repeat')
    candidate_path = HERE.parents[1] / '完整Pimport-stage回执.json'
    candidate = json.loads(candidate_path.read_text(encoding='utf-8'))
    (HERE / 'candidate_import_exit_snapshot.json').write_text(
        json.dumps(candidate,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    baseline = json.loads(remote('docker exec -i ks-pp-m137-priv python -S -c '+shlex.quote(SCRIPT)))
    both_abort = candidate.get('exit') == -6 and baseline.get('exit') == -6
    result = {'scope':'ONE_MATCHED_IMPORT_EXIT_CONTROL', 'candidate_source':str(candidate_path),
        'candidate':candidate,'baseline':baseline,
        'candidate_module_paths_printed_before_exit':'stage-candidate/' in candidate.get('stdout',''),
        'baseline_has_candidate_source_paths':'stage-candidate/' in baseline.get('stdout',''),
        'both_sigabrt':both_abort,
        'interpretation':('Both import controls abort during interpreter teardown after printing module origins; '
            'candidate-only regression is not established. Native extension/environment/import teardown interaction remains unlocalized.'
            if both_abort else 'Different import exit results; candidate-specific import/teardown interaction remains possible. No root cause is established.'),
        'full_package_import_pass':False,'model_correctness_proven':False,'service_actions':0,'operator_tests':0}
    target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'candidate_exit':candidate.get('exit'),'baseline_exit':baseline.get('exit'),
        'both_sigabrt':both_abort,'baseline_has_candidate_source_paths':result['baseline_has_candidate_source_paths'],
        'baseline_stdout_tail':baseline.get('stdout','').splitlines()[-1:],
        'baseline_stderr_tail':baseline.get('stderr','').splitlines()[-3:],
        'interpretation':result['interpretation'],'output':str(target)},ensure_ascii=False))


if __name__ == '__main__':
    main()
