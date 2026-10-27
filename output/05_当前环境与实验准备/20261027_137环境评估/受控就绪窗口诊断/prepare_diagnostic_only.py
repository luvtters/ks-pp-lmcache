"""DIAGNOSTIC_ONLY local source/patch and same-source CPU checks. No deployment."""
from __future__ import annotations
import ast
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import difflib
import json
import os
from pathlib import Path
import threading

HERE=Path(__file__).resolve().parent
SOURCE=HERE.parent/'恢复身份与启动接线/stage_driver/runtime_overlay/lmcache/integration/vllm/vllm_multi_process_adapter.py'
ENV='KS_LMCACHE_DIAGNOSTIC_BACK_READY_DELAY_MS'


def build():
    original=SOURCE.read_text(encoding='utf-8')
    text=original
    old='''    real_future: object = None
    cancelled: bool = False'''
    new=old+'''
    real_value: bool | None = None
    real_done_at: float | None = None
    release_at: float | None = None'''
    assert text.count(old)==1;text=text.replace(old,new)
    anchor='''        self.owner_error = None

    def start(self):'''
    replacement='''        self.owner_error = None
        # DIAGNOSTIC_ONLY: delay publication after successful device completion.
        # Default0 preserves the normal observer path; no real Future is modified.
        self._diagnostic_back_delay_s = float(os.environ.get(
            "KS_LMCACHE_DIAGNOSTIC_BACK_READY_DELAY_MS", "0")) / 1000.0

    def start(self):'''
    assert text.count(anchor)==1;text=text.replace(anchor,replacement)
    old='''                if not real.query():
                    continue
                value = real.result(timeout=0)
                if type(value) is not bool:
                    raise TypeError("current RetrieveResult must be bool")'''
    new='''                if operation.real_value is None:
                    if not real.query():
                        continue
                    value = real.result(timeout=0)
                    if type(value) is not bool:
                        raise TypeError("current RetrieveResult must be bool")
                else:
                    value = operation.real_value'''
    assert text.count(old)==1;text=text.replace(old,new)
    old='''                if error is None:
                    if value and not operation.cancelled:
                        self.success_outbox.append(operation.identity)
                    operation.proxy.set_result(value)'''
    new='''                if error is None:
                    if operation.real_value is None:
                        operation.real_value = value
                        operation.real_done_at = time.monotonic()
                        if (value and operation.identity.stage_id == 1
                                and not operation.cancelled and not self.stopping.is_set()
                                and self._diagnostic_back_delay_s > 0):
                            operation.release_at = operation.real_done_at + self._diagnostic_back_delay_s
                    # Never block the owner. Cancellation/shutdown bypass only
                    # this CPU publication delay, after real device completion.
                    if (value and not operation.cancelled and not self.stopping.is_set()
                            and operation.release_at is not None
                            and time.monotonic() < operation.release_at):
                        continue
                    if value and not operation.cancelled:
                        self.success_outbox.append(operation.identity)
                    operation.proxy.set_result(value)'''
    assert text.count(old)==1;text=text.replace(old,new)
    ast.parse(text)
    (HERE/'vllm_multi_process_adapter_DIAGNOSTIC_ONLY.py').write_text(text,encoding='utf-8',newline='\n')
    (HERE/'observer_back_ready_DIAGNOSTIC_ONLY.patch').write_text(''.join(difflib.unified_diff(
        original.splitlines(True),text.splitlines(True),fromfile=str(SOURCE),
        tofile='DIAGNOSTIC_ONLY/vllm_multi_process_adapter.py')),encoding='utf-8')
    (HERE/'source_provenance.json').write_text(json.dumps({'source':str(SOURCE),
        'diagnostic_copy':'vllm_multi_process_adapter_DIAGNOSTIC_ONLY.py','default_enabled':False,
        'environment':ENV,'suggested_value_ms':500,'deployed':False,'device_action':False},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return text


class Clock:
    def __init__(self):self.now=100.0
    def monotonic(self):return self.now


class Proxy:
    def __init__(self):self.refs=[];self.done=False;self.value=None;self.error=None;self.sets=[]
    def retain_reference(self,obj):self.refs.append(obj)
    def query(self):return self.done
    def set_result(self,value):
        assert not self.done;self.done=True;self.value=value;self.sets.append(('result',value))
    def set_exception(self,error):
        assert not self.done;self.done=True;self.error=error;self.sets.append(('error',type(error).__name__))
    def result(self,timeout=0):
        if not self.done:raise TimeoutError('pending CPU proxy')
        if self.error:raise self.error
        return self.value


class Real:
    def __init__(self,ready=True,value=True,error=None):
        self.ready=ready;self.value=value;self.error=error;self.queries=0;self.getters=0;self.owner=threading.get_ident()
    def query(self):
        assert threading.get_ident()==self.owner;self.queries+=1
        return self.ready
    def result(self,timeout=0):
        assert threading.get_ident()==self.owner;assert self.ready;self.getters+=1
        if self.error:raise self.error
        return self.value


def checks(text):
    clock=Clock();ns={'dataclass':dataclass,'threading':threading,'os':os,'time':clock}
    tree=ast.parse(text)
    classes=[node for node in tree.body if isinstance(node,ast.ClassDef) and node.name in
             {'StageRestoreIdentity','_Operation','LMCacheStageObserver'}]
    module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+classes,type_ignores=[])
    exec(compile(ast.fix_missing_locations(module),'actual_DIAGNOSTIC_ONLY_observer_AST','exec'),ns)
    Observer=ns['LMCacheStageObserver'];Identity=ns['StageRestoreIdentity']
    @contextmanager
    def configured(ms):
        previous=os.environ.get(ENV)
        if ms is None:os.environ.pop(ENV,None)
        else:os.environ[ENV]=str(ms)
        try:yield
        finally:
            if previous is None:os.environ.pop(ENV,None)
            else:os.environ[ENV]=previous
    def observer(ms=500):
        with configured(ms):obj=Observer(Proxy,lambda:nullcontext())
        obj.owner=threading.get_ident();return obj
    counter=0
    def add(obj,stage=1,real=None):
        nonlocal counter;counter+=1
        identity=Identity(f'CPU_FIXTURE_{counter}',counter,stage,0,0,stage*4)
        producer=object();operation=obj.reserve(identity,(0,512,0,(1,)),producer)
        future=real or Real();proxy=obj.attach(operation,future)
        return operation,future,proxy,producer
    passed=[]
    obj=observer(None);op,real,proxy,_=add(obj);obj.poll_once()
    assert proxy.result() is True and obj.drain_success()==[op.identity];passed.append('default OFF publishes immediately')
    obj=observer();op,real,proxy,_=add(obj,0);obj.poll_once()
    assert proxy.result() is True and obj.drain_success()==[op.identity];passed.append('front immediate with diagnostic ON')
    obj=observer();real=Real(False);op,real,proxy,producer=add(obj,real=real);obj.poll_once()
    assert not proxy.query() and obj.drain_success()==[] and real.getters==0 and op.real_done_at is None
    clock.now+=1;real.ready=True;obj.poll_once();deadline=op.release_at
    assert not proxy.query() and not proxy.sets and deadline==clock.now+0.5
    assert producer in proxy.refs and real in proxy.refs and list(obj.entries.values())==[op]
    for _ in range(4):clock.now+=0.1;obj.poll_once();assert not proxy.query() and obj.drain_success()==[]
    assert real.getters==1 and real.queries==2 and op.release_at==deadline
    clock.now=deadline;obj.poll_once()
    assert proxy.result() is True and proxy.sets==[('result',True)] and obj.drain_success()==[op.identity]
    obj.poll_once();assert not obj.drain_success() and real.getters==1 and not obj.entries
    passed.append('back only after real success+window; deadline fixed; getter once; references retained; one publication')
    obj=observer();back,_,bp,_=add(obj);obj.poll_once();front,_,fp,_=add(obj,0);obj.poll_once()
    assert fp.query() and not bp.query() and obj.drain_success()==[front.identity]
    passed.append('held back entry does not block later front entry')
    obj=observer();op,real,proxy,_=add(obj,real=Real(True,False));obj.poll_once()
    assert proxy.result() is False and not obj.drain_success() and real.getters==1;passed.append('real False immediate; no success')
    obj=observer();op,real,proxy,_=add(obj,real=Real(True,error=ValueError('real error')));obj.poll_once()
    assert isinstance(proxy.error,ValueError) and obj.retained_errors==[op] and not obj.drain_success()
    passed.append('real exception immediate; retained ownership; no success')
    obj=observer();op,real,proxy,_=add(obj);obj.poll_once();obj.invalidate_requests({op.identity.request_id});obj.poll_once()
    assert proxy.result() is True and not obj.drain_success() and not obj.entries and real.getters==1
    clock.now+=1;obj.poll_once();assert not obj.drain_success();passed.append('cancel after real success bypasses delay; no late success')
    obj=observer();op,real,proxy,_=add(obj,real=Real(False));obj.invalidate_requests({op.identity.request_id});obj.poll_once()
    assert not proxy.query() and real.getters==0
    real.ready=True;obj.poll_once();assert proxy.result() is True and not obj.drain_success()
    passed.append('cancel before device completion retains until real terminal state')
    obj=observer();op,real,proxy,_=add(obj);obj.poll_once();assert obj.close(timeout=0) is False
    obj.poll_once();assert proxy.result() is True and not obj.drain_success() and obj.close(timeout=0)
    passed.append('close bypasses only completed publication delay; no late success')
    obj=observer();op,real,proxy,_=add(obj,real=Real(False));assert not obj.close(timeout=0);obj.poll_once()
    assert not proxy.query() and not obj.close(timeout=0)
    real.ready=True;obj.poll_once();assert obj.close(timeout=0) and not obj.drain_success()
    passed.append('close never fabricates completion for an unfinished real Future')
    poll=next(n for c in classes if c.name=='LMCacheStageObserver' for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='poll_once')
    assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr in {'sleep','wait'} for n in ast.walk(poll))
    passed.append('owner poll contains no sleep/wait')
    report={'status':'PASS','scope':'DIAGNOSTIC_ONLY same generated observer AST, scalar Future/proxy/clock fixtures',
            'cases':passed,'case_count':len(passed),'source_imported':False,'NPU_test':False,
            'runtime_modified':False,'deployed':False,'requests_sent':0,
            'boundary':'No KV bytes or real NPU Future executed; verifies publication/lifetime state machine only.'}
    (HERE/'cpu_result.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__=='__main__':checks(build())
