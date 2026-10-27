"""Local-only actual137 BalanceScheduler wiring candidate; no deployment."""
from pathlib import Path
import ast
import difflib
import json

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
SOURCE=ROOT/'受控就绪窗口诊断/actual137_patch_balance_schedule.py'
HELPER=ROOT/'恢复身份与启动接线/stage_driver/runtime_overlay/vllm/v1/core/sched/ks_stage_reservation.py'

def main():
    before=SOURCE.read_text(encoding='utf-8')
    (HERE/'actual137_patch_balance_schedule.py').write_text(before,encoding='utf-8',newline='\n')
    (HERE/'ks_stage_reservation_snapshot.py').write_text(HELPER.read_text(encoding='utf-8'),encoding='utf-8',newline='\n')
    after=before.replace('from vllm.v1.core.sched.scheduler import Scheduler\n',
        'from vllm.v1.core.sched.scheduler import Scheduler\nfrom vllm.v1.core.sched import ks_stage_reservation as _ks_stage_reservation\n',1)
    assert after.count('delay_cache_blocks=load_kv_async,')==1
    after=after.replace('delay_cache_blocks=load_kv_async,',
        'delay_cache_blocks=(load_kv_async or _ks_stage_reservation.delay_cache_blocks(self, request_id)),',1)
    anchor='''        with record_function_or_nullcontext("schedule: update_after_schedule"):
            self._update_after_schedule(scheduler_output)'''
    assert after.count(anchor)==1
    after=after.replace(anchor,'        _ks_stage_reservation.attach_output(self, scheduler_output)\n\n'+anchor,1)
    ast.parse(after)
    (HERE/'patch_balance_schedule_fixed.py').write_text(after,encoding='utf-8',newline='\n')
    patch=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),
        fromfile='actual137/patch_balance_schedule.py',tofile='private/patch_balance_schedule_fixed.py'))
    (HERE/'balance_scheduler_wiring.patch').write_text(patch,encoding='utf-8')
    old_tree,new_tree=ast.parse(before),ast.parse(after)
    def methods(tree):
        return {(c.name,m.name):ast.dump(m,include_attributes=False) for c in tree.body if isinstance(c,ast.ClassDef)
                for m in c.body if isinstance(m,ast.FunctionDef)}
    old,new=methods(old_tree),methods(new_tree)
    assert {key for key in old if old[key]!=new[key]}=={('BalanceScheduler','schedule')}
    info={'status':'CPU_ONLY_NOT_DEPLOYED','actual_source':str(SOURCE),'helper_source':str(HELPER),
          'canonical':'vllm_ascend.patch.platform.patch_balance_schedule',
          'only_method_changed':'BalanceScheduler.schedule','diff_lines':len(patch.splitlines()),
          'NPU_actions':0,'server_runtime_modified':False,'deployed':False}
    (HERE/'candidate_manifest.json').write_text(json.dumps(info,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(info,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
