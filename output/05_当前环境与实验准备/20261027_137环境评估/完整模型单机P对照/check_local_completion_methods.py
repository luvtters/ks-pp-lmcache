"""Run extracted real methods with CPU fixtures; no device/framework imports.

These checks cover control flow, explicit-mode scope, cold bookkeeping and
ownership. They do not validate NPU communication, GLM, MTP numerics or core.
"""
from pathlib import Path
from types import MethodType, SimpleNamespace as NS
from contextlib import nullcontext
import ast
import copy
import json
import sys
import time

HERE=Path(__file__).resolve().parent
TARGET=(HERE.parent if HERE.name=='startup' else HERE)/'runtime/local-completion'


def methods(path, class_name, names, globals_=None):
 tree=ast.parse(path.read_text(encoding='utf-8'))
 cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==class_name)
 found=[]
 for node in cls.body:
  if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in names:
   node=copy.deepcopy(node);node.decorator_list=[];found.append(node)
 assert {n.name for n in found}==set(names)
 module=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),*found],type_ignores=[])
 ns=dict(globals_ or {})
 exec(compile(ast.fix_missing_locations(module),str(path),'exec'),ns)
 return {name:ns[name] for name in names}


checks=[]
runner_path=TARGET/'model_runner_local.py'


def sample_case(local_mode,last_stage,async_scheduling):
 calls=[]
 pp=NS(world_size=2,is_last_rank=last_stage)
 class AsyncResult:
  def __init__(self,**kwargs):
   self.sampled_token_ids_cpu=kwargs['sampled_token_ids']
   self.async_copy_ready_event=object()
   self.original=kwargs['model_runner_output']
 fn=methods(runner_path,'NPUModelRunner',['sample_tokens'],{
  'get_pp_group':lambda:pp,'record_function_or_nullcontext':lambda _:nullcontext(),
  'copy':copy.copy,'ModelRunnerOutput':lambda **kw:NS(**kw),
  'AsyncGPUModelRunnerOutput':AsyncResult,'EMPTY_MODEL_RUNNER_OUTPUT':NS(),
 })['sample_tokens']
 cfg=NS(get_from_extra_config=lambda key,default:local_mode if key=='ks_local_p_complete' else default)
 owner=NS(is_kv_producer=True,vllm_config=NS(kv_transfer_config=cfg),
  use_async_scheduling=async_scheduling,kv_connector_output=None,execute_model_state=None,
  _pp_receive_prev_sampled_token_ids_to_input_batch=lambda:calls.append('receive'))
 if not last_stage:
  assert fn(owner,None) is None
  assert calls==(['receive'] if local_mode and async_scheduling else [])
  return
 schedule=NS(total_num_scheduled_tokens=4)
 owner.execute_model_state=(schedule,None,None,object(),None,None,None,None,None,None,None,None)
 owner.need_accepted_tokens=False
 owner.supports_mm_inputs=False
 owner._sample=lambda logits,metadata:NS(sampled_token_ids=[[11]],logprobs_tensors=None)
 owner._bookkeeping_sync=lambda *a:(None,[[11]],{},['R'],{'R':0},[])
 owner.speculative_config=NS(use_eagle=lambda:True,uses_draft_model=lambda:False,
  uses_extract_hidden_states=lambda:False,use_ngram_gpu=lambda:False,disable_padded_drafter_batch=False)
 owner.input_batch=NS(sampling_metadata=object(),vocab_size=100,
  set_async_sampled_token_ids=lambda *a:calls.append('sample-state'))
 owner.propose_draft_token_ids=lambda *a:(calls.append('real-method-draft-call') or [[7,8,9]])
 owner._copy_draft_token_ids_to_cpu=lambda output:calls.append('copy-draft')
 owner.finalize_kv_connector=lambda:calls.append('finalize-kv')
 owner.ascend_config=NS(profiling_chunk_config=NS(need_timing=False))
 owner.dynamic_eplb=False
 owner._finalize_dump_data=lambda:None
 owner.routed_experts_initialized=False
 owner.async_output_copy_stream=None
 owner._pp_broadcast_prev_sampled_token_ids=lambda ids:calls.append(('send',ids))
 result=fn(owner,None)
 assert owner._draft_token_ids==[[7,8,9]]
 assert calls[:3]==['real-method-draft-call','copy-draft','finalize-kv']
 assert any(isinstance(v,tuple) and v[0]=='send' for v in calls)==(local_mode and async_scheduling)
 assert (result.original if async_scheduling else result).sampled_token_ids==[[11]]


for mode in [False,True]:
 for last in [False,True]:
  for async_ in [False,True]:
   sample_case(mode,last,async_)
   checks.append(f'actual_sample_mode={mode}_last={last}_async={async_}')

events=[]
class Tracker:
 def __init__(self):
  self.allocated_block_ids={};self.lookup_started_at=None;self.state='PREFETCHING'
  self.num_vllm_hit_tokens=0;self.num_lmcache_hit_tokens=0
  self.cache_salt=None;self.request_configs={}
 def append_block_ids(self,ids,window): self.allocated_block_ids={0:list(ids[0])}
 def increase_num_scheduled_tokens(self,n): pass
 def get_token_ids(self): return list(range(1024))

states=NS(BYPASS_LMCACHE='BYPASS',READY='READY',PREFETCHING='PREFETCHING')
g={'time':time,'LMCacheMPRequestState':states,'RequestStatus':NS(PREEMPTED='PREEMPTED'),
 'logger':NS(debug=lambda *a:None,warning=lambda *a:None),
 'LMCacheMPRequestMetadata':NS(GetStoreMetadata=lambda tracker,*a,**kw:
  list(tracker.allocated_block_ids.get(0,[])) or None)}
lm=methods(TARGET/'lmcache_mp_connector_multi17.py','LMCacheMPConnector',[
 'get_num_new_matched_tokens','_process_new_requests','request_finished',
 'set_xfer_handshake_metadata_pp_aware'],g)
tracker=Tracker()
adapter=NS(lmcache_tokens_per_chunk=512,maybe_submit_lookup_request=lambda *a,**kw:events.append('lookup'),
 check_lookup_result=lambda req:512,cleanup_lookup_result=lambda req:events.append('lookup-cleanup'),
 end_session=lambda req:events.append('end-session'))
child=NS(_get_or_create_request_tracker=lambda req:tracker,_get_request_tracker=lambda req:tracker,
 scheduler_adapter=adapter,_connector_stats=NS(record_lookup=lambda v:None),_hit_alignment_tokens=16,
 _reserve_last_token_for_lookup=False,_can_store=True,_mamba_relocation_window=0,
 _group_tokens_per_block=[16],_save_decode_cache=False,lazy_offload=False,
 _cleanup_request_tracker=lambda req:events.append('tracker-cleanup'))
for name,fn in lm.items(): setattr(child,name,MethodType(fn,child))
request=NS(request_id='R',num_computed_tokens=0,status='RUNNING',all_token_ids=list(range(1024)),kv_transfer_params=None)
assert child.get_num_new_matched_tokens(request,0)==(512,True)
checks.append('actual_MP_lookup_returns_real_matched_count_not_store_only')
child.set_xfer_handshake_metadata_pp_aware({(1,0):None})
checks.append('actual_approved_PP_handshake_setter_accepts_stage1')

new=NS(req_id='R',block_ids=([3,4],))
schedule=NS(scheduled_new_reqs=[new],num_scheduled_tokens={'R':32})
tracker.allocated_block_ids={}
collected=[]
metadata=NS(add_request_metadata=lambda value:collected.append(value))
unpatched=methods(HERE/'installed_lmcache_mp_connector.py','LMCacheMPConnector',['_process_new_requests'],g)['_process_new_requests']
unpatched(child,schedule,metadata)
assert collected==[] and tracker.allocated_block_ids=={}
child._process_new_requests(schedule,metadata)
assert collected==[[3,4]] and tracker.allocated_block_ids=={0:[3,4]}
checks.append('actual_cold_STORE_requires_and_preserves_approved7_block_ids')

multi=methods(HERE/'pp_multi_connector.py','MultiConnector',[
 'get_num_new_matched_tokens','update_state_after_alloc','_aggregate_request_finished',
 'request_finished','get_finished'])
owner=NS(_connectors=[child],_requests_to_connector={},_extra_async_saves={})
for name,fn in multi.items(): setattr(owner,name,MethodType(fn,owner))
assert owner.get_num_new_matched_tokens(request,0)==(512,True)
assert owner._requests_to_connector=={'R':0}
checks.append('actual_Multi_single_child_hot_lookup_selects_LMCache')
received=[]
child.update_state_after_alloc=lambda req,blocks,n:received.append((blocks,n))
empty=object();blocks=NS(new_empty=lambda:empty)
owner._requests_to_connector.clear()
owner.update_state_after_alloc(request,blocks,0)
assert received==[(empty,0)]
checks.append('actual_Multi_cold_miss_supplies_empty_blocks_even_single_child')
assert owner.request_finished(request,[3,4])==(True,None)
assert events[-3:]==['tracker-cleanup','lookup-cleanup','end-session']
assert owner._extra_async_saves=={}
child.get_finished=lambda ids:(None,None)
assert owner.get_finished({'R'})==(None,None)
child.get_finished=lambda ids:({'R'},None)
assert owner.get_finished({'R'})==({'R'},None)
checks.append('actual_Multi_finish_waits_for_single_LMCache_owner_completion')
assert not any(name in sys.modules for name in ['torch','torch_npu','vllm','lmcache'])
receipt={'status':'CPU_ACTUAL_METHODS_PASS_NOT_NPU_OR_FULL_LIFECYCLE',
 'passed':len(checks),'checks':checks,'inference_or_device_imports':False,
 'sample_and_draft_ids':'CPU fixtures only; no generated model tokens',
 'core_draft_batch_binding':'NOT_COVERED_SEPARATE_AUDIT',
 'full_GLM_local_completion':'NOT_VERIFIED','services_started':[]}
(TARGET/'cpu_actual_methods.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,ensure_ascii=False,indent=2))
