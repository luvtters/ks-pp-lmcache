"""Run actual Balance.schedule with existing actual Scheduler/helper fixtures.

Only fixture setup is reused; the previous test suites are never run.
"""
from __future__ import annotations
import ast
import dataclasses
import importlib.util
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
FIXTURE=HERE.parent/'恢复身份与启动接线/stage_driver/check_scheduler_reservation.py'
spec=importlib.util.spec_from_file_location('actual_balance_fixture_setup_only',FIXTURE)
fixture=importlib.util.module_from_spec(spec);sys.modules[spec.name]=fixture;spec.loader.exec_module(fixture)
RECORDS=[]

def make(patched=True,enabled=True):
    case=fixture.SchedulerReservationChecks('test_actual_schedule_reserves_front_without_global_ready_or_cache')
    case.setUp(tp_size=4)
    os.environ.pop('KS_STAGE_TRACE_DIR',None)
    if enabled:os.environ['KS_PP_STAGE_DRIVER']='1'
    else:os.environ.pop('KS_PP_STAGE_DRIVER',None)
    base=type(case.scheduler)
    env=dict(case.scheduler.schedule.__globals__)
    env.update(BaseScheduler=base,vllm_version_is=lambda version:version=='0.23.0')
    source=HERE/('patch_balance_schedule_fixed.py' if patched else 'actual137_patch_balance_schedule.py')
    tree=ast.parse(source.read_text(encoding='utf-8'))
    cls=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='BalanceScheduler')
    schedule=next(node for node in cls.body if isinstance(node,ast.FunctionDef) and node.name=='schedule')
    cls.body=[schedule];cls.bases=[ast.Name(id='BaseScheduler',ctx=ast.Load())];cls.decorator_list=[]
    disable=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='_disable_preemption_on_prefill_node')
    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),disable,cls],type_ignores=[])
    exec(compile(ast.fix_missing_locations(module),str(source),'exec'),env)
    scheduler=env['BalanceScheduler']();scheduler.__dict__.update(case.scheduler.__dict__)
    scheduler._balance_enabled=False
    scheduler._disable_preemption=env['_disable_preemption_on_prefill_node'](
        SimpleNamespace(kv_transfer_config=SimpleNamespace(kv_role='kv_producer')))
    scheduler.max_model_len=49152
    case.scheduler=scheduler
    RECORDS.append({'source':str(source),'class':'BalanceScheduler','method':'schedule',
                    'begin':schedule.lineno,'end':schedule.end_lineno,'fixture_init_only':str(FIXTURE)})
    return case

def summarize(case,output):
    allocations=[{key:({'fixture':'KVCacheBlocks','block_ids':value.get_block_ids()}
                        if isinstance(value,fixture.Blocks) else value)
                  for key,value in item.items()} for item in case.allocated]
    return {'tokens':output.num_scheduled_tokens,'total':output.total_num_scheduled_tokens,
            'tag':getattr(output,'ks_stage_batch',None),'request_status':case.request.status.name,
            'computed':case.request.num_computed_tokens,'allocated':allocations,
            'cached':case.cached,'free':case.freed,
            'new_request_positions':[item.num_computed_tokens for item in output.scheduled_new_reqs]}

def main():
    checks=[];observations={}
    previous_flag=os.environ.get('KS_PP_STAGE_DRIVER');previous_trace=os.environ.get('KS_STAGE_TRACE_DIR')
    try:
        # First execute the unmodified actual P schedule, then require the desired
        # wiring assertion to FAIL. No pretagged output or reservation is injected.
        old=make(False);out=old.scheduler.schedule();observations['old_actual_partial']=summarize(old,out)
        assert 'r' in old.scheduler._ks_partial_restore_reservations
        assert out.num_scheduled_tokens=={'r':1} and out.scheduled_new_reqs[0].num_computed_tokens==511
        try:
            assert old.allocated[0]['delay_cache_blocks'] is True and hasattr(out,'ks_stage_batch')
        except AssertionError:
            checks.append('RED: unmodified actual Balance schedule creates real reservation but misses both hooks')
        else:raise AssertionError('old P scheduler unexpectedly passed the missing-wiring assertion')
        assert old.allocated[0]['delay_cache_blocks'] is False and not hasattr(out,'ks_stage_batch')
        case=make(True);out=case.scheduler.schedule();observations['fixed_actual_partial']=summarize(case,out)
        assert case.allocated[0]['delay_cache_blocks'] is True
        assert out.ks_stage_batch['requests']==(('r',4),) and out.ks_stage_batch['token_positions']==((511,),)
        assert out.num_scheduled_tokens=={'r':1} and out.scheduled_new_reqs[0].num_computed_tokens==511
        assert case.cached==[] and case.scheduler.finished_recving_kv_req_ids==set()
        checks.append('GREEN: actual inherited promotion/helper -> real Balance allocation/output -> tag and delayed cache blocks')
        snapshots=[]
        for patched in [False,True]:
            case=make(patched,False);out=case.scheduler.schedule();snapshots.append(summarize(case,out))
        assert snapshots[0]==snapshots[1] and snapshots[1]['total']==0 and snapshots[1]['tag'] is None
        checks.append('default OFF: old/fixed actual producer Balance output/state equal')
        case=make(True);case.scheduler.finished_recving_kv_req_ids.add('r');out=case.scheduler.schedule()
        assert not hasattr(out,'ks_stage_batch') and not case.scheduler._ks_partial_restore_reservations
        assert case.cached==[('r',512)] and case.allocated[0]['delay_cache_blocks'] is False
        observations['true_global_completion']=summarize(case,out)
        checks.append('true global completion uses original promotion/cache path without partial tag')
        for patched in [False,True]:
            case=make(patched,False);s=case.scheduler;r=case.request
            s.skipped_waiting=fixture.RequestQueue();s.running=[r];r.status=fixture.Status.RUNNING;r.num_computed_tokens=0
            calls=[]
            s.kv_cache_manager.allocate_slots=lambda *args,**kwargs:(calls.append(kwargs),None)[1]
            s._preempt_request=lambda *args:(_ for _ in ()).throw(AssertionError('official disabled preemption was bypassed'))
            out=s.schedule()
            assert s._disable_preemption is True and s.running==[r] and r.status is fixture.Status.RUNNING
            assert len(calls)==1 and not out.preempted_req_ids and out.total_num_scheduled_tokens==0
        checks.append('old/fixed disable-preemption allocation-failure branch retains running request and never preempts')
        for patched in [False,True]:
            case=make(patched);s=case.scheduler;s._balance_enabled=True
            s.balance_queue=[SimpleNamespace(item=lambda:s.max_num_running_reqs)]
            out=s.schedule();assert out.total_num_scheduled_tokens==0 and not case.allocated
        checks.append('official balance-queue throttle remains active in old/fixed actual schedule')
        result={'status':'PASS_CPU_ONLY_NOT_DEPLOYED','test_count':len(checks),'checks':checks,
                'order':'old missing-wiring assertion failed first; fixed actual function passed next',
                'actual_methods':RECORDS,'observations':observations,
                'helper_route':'real _try_promote_blocked_waiting_request -> real try_partial_promotion -> actual Balance.schedule -> real attach_output',
                'manual_output_tag_injected':False,'prior_suites_run':False,'NPU_actions':0,'deployed':False,
                'fixtures':['request/config','KV allocator recording','connector metadata','controller readiness','queue'],
                'boundary':'actual function/helper/output AST with CPU metadata fixtures; no KV bytes/model/NPU/runtime proof'}
        (HERE/'cpu_result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({k:v for k,v in result.items() if k not in {'actual_methods','observations'}},ensure_ascii=False,indent=2))
    finally:
        if previous_flag is None:os.environ.pop('KS_PP_STAGE_DRIVER',None)
        else:os.environ['KS_PP_STAGE_DRIVER']=previous_flag
        if previous_trace is None:os.environ.pop('KS_STAGE_TRACE_DIR',None)
        else:os.environ['KS_STAGE_TRACE_DIR']=previous_trace

if __name__=='__main__':main()
