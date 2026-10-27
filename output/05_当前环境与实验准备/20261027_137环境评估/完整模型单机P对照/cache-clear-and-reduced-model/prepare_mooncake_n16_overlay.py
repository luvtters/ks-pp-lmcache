"""Build/check the authorized private N16 Mooncake mapping; optional file deploy.

No inference/device import, installed source edit, checkpoint edit, or service start.
"""
from __future__ import annotations
import argparse
import ast
import base64
from collections import OrderedDict
import difflib
import json
import logging
import math
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time
from types import MethodType, ModuleType, SimpleNamespace

HERE=Path(__file__).resolve().parent
OUT=HERE/'reduced-pd-prepared'
REMOTE='/opt/ks-stage-ready-luv-20261027/reduced-pd'
CANONICAL='vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector'
RELATIVE='runtime/mooncake-n16/mooncake_connector_n16.py'

HELPER='''def _ks_n16_original_mtp_mapping(vllm_config) -> bool:
    """Only the approved target16 / original draft78 / one MTP KV layer."""
    model = vllm_config.model_config
    target = getattr(model, "hf_text_config", None) or getattr(model, "hf_config", None)
    speculative = vllm_config.speculative_config
    draft_model = getattr(speculative, "draft_model_config", None)
    draft = getattr(draft_model, "hf_text_config", None) or getattr(draft_model, "hf_config", None)
    return (
        getattr(target, "model_type", None) == "glm_moe_dsa"
        and getattr(target, "num_hidden_layers", None) == 16
        and getattr(speculative, "method", None) == "mtp"
        and getattr(draft, "num_hidden_layers", None) == 78
        and getattr(draft, "num_nextn_predict_layers", None) == 1
    )


'''


def generate_source():
    before=(HERE/'pd_actual_mooncake_connector.py').read_text(encoding='utf-8')
    old='''                    if assigned_indices and layer_idx < min(assigned_indices) or layer_idx in assigned_indices:
                        layer_idx = next_mtp_layer_idx
                        next_mtp_layer_idx += 1'''
    new='''                    if _ks_n16_original_mtp_mapping(self.vllm_config):
                        # Preserve physical checkpoint/tensor names; transfer MTP as logical layer16.
                        if layer_idx == 78:
                            layer_idx = self.total_layers
                    elif assigned_indices and layer_idx < min(assigned_indices) or layer_idx in assigned_indices:
                        layer_idx = next_mtp_layer_idx
                        next_mtp_layer_idx += 1'''
    if before.count(old)!=1:raise ValueError('actual build mapping anchor changed')
    after=before.replace(old,new)
    receiver='''    def _get_group_kv_caches(self, group_idx: int, layer_indices: list[int] | None = None) -> dict[str, Any]:
'''
    branch='''        if _ks_n16_original_mtp_mapping(self.vllm_config):
            group_spec, full_layer_indices = self.kv_group2layeridx[group_idx]
            requested = set(full_layer_indices if layer_indices is None else layer_indices)
            # Zip complete metadata first; filtering must not shift name/index pairs.
            return {
                name: self.kv_caches[name]
                for name, logical_idx in zip(group_spec["layer_names"], full_layer_indices, strict=True)
                if logical_idx in requested
            }
'''
    if after.count(receiver)!=1:raise ValueError('actual receiver anchor changed')
    after=after.replace(receiver,receiver+branch)
    after=after.replace('class MooncakeConnectorWorker:',HELPER+'class MooncakeConnectorWorker:',1)
    ast.parse(after)
    path=OUT/RELATIVE;path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(after,encoding='utf-8',newline='\n')
    patch=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),
                  fromfile='actual137/mooncake_connector.py',tofile='private/mooncake_connector_n16.py'))
    (HERE/'mooncake_n16_review.patch').write_text(patch,encoding='utf-8')
    def class_other_methods(text):
        tree=ast.parse(text)
        result={}
        for node in tree.body:
            if isinstance(node,ast.ClassDef):
                for method in node.body:
                    if isinstance(method,ast.FunctionDef):result[(node.name,method.name)]=ast.dump(method,include_attributes=False)
        return result
    previous,current=class_other_methods(before),class_other_methods(after)
    changed={key for key in previous if previous[key]!=current[key]}
    assert changed=={('MooncakeConnectorWorker','_build_kv_group2layeridx'),
                     ('KVCacheRecvingThread','_get_group_kv_caches')}
    return after,patch


def function(text,name,owner=None):
    tree=ast.parse(text)
    body=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==owner).body if owner else tree.body
    node=next(n for n in body if isinstance(n,ast.FunctionDef) and n.name==name)
    node.decorator_list=[]
    return node


def cpu_check(after):
    original=(HERE/'pd_actual_mooncake_connector.py').read_text(encoding='utf-8')
    util=(HERE/'extra_models_utils.py').read_text(encoding='utf-8')
    def namespace(source):
        nodes=[function(util,'extract_layer_index')]
        if source==after:nodes.append(function(source,'_ks_n16_original_mtp_mapping'))
        nodes.extend([function(source,'_build_kv_group2layeridx','MooncakeConnectorWorker'),
                      function(source,'_get_group_kv_caches','KVCacheRecvingThread'),
                      function(source,'register_kv_caches','MooncakeConnectorWorker'),
                      function(source,'get_prefill_pp_indices')])
        tree=ast.parse(source)
        recv=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='KVCacheRecvingThread')
        nodes.append(next(n for n in ast.walk(recv) if isinstance(n,ast.FunctionDef) and n.name=='pp_layer_indices'))
        ns={'OrderedDict':OrderedDict,'UniformTypeKVCacheSpecs':type('UniformTypeKVCacheSpecs',(),{}),
            'math':math,'threading':threading,'time':time,'logging':logging,
            'logger':logging.getLogger('n16-cpu'),
            'enable_sfa_dcp_replicated_indexer':lambda config:False,
            'collect_storage_merged_register_regions':lambda caches:SimpleNamespace(ptrs=[1],lengths=[1]),
            'validate_register_region_count':lambda regions:None,
            'global_te':SimpleNamespace(register_buffer=lambda ptrs,lengths:None),
            'MooncakeAgentMetadata':lambda **kw:SimpleNamespace(**kw),'get_ip':lambda:'127.0.0.1'}
        class ThreadFixture:
            def __init__(self,*args):self.args=args
            def start(self):
                for arg in self.args:
                    if isinstance(arg,threading.Event):arg.set()
            def is_alive(self):return True
        ns['KVCacheSendingThread']=ns['KVCacheRecvingThread']=ThreadFixture
        mod=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]+nodes,type_ignores=[])
        exec(compile(ast.fix_missing_locations(mod),'actual137_private_n16_AST','exec'),ns)
        fake=ModuleType('vllm.v1.worker.utils');fake.extract_layer_index=ns['extract_layer_index']
        sys.modules['vllm.v1.worker.utils']=fake
        return ns
    old,new=namespace(original),namespace(after)
    def config(target=16,draft=78,nextn=1):
        return SimpleNamespace(model_config=SimpleNamespace(is_deepseek_mla=True,
            hf_text_config=SimpleNamespace(model_type='glm_moe_dsa',num_hidden_layers=target,index_topk=2048)),
            speculative_config=SimpleNamespace(method='mtp',draft_model_config=SimpleNamespace(
                hf_config=SimpleNamespace(num_hidden_layers=draft,num_nextn_predict_layers=nextn))))
    class Tensor:
        def __init__(self,pointer):self.pointer=pointer;self.shape=(4,2,1,8)
        def element_size(self):return 2
        def stride(self,dimension):assert dimension==0;return 16
        def data_ptr(self):return self.pointer
    def worker(ns,names,target=16,groups=None,role='kv_producer'):
        spec=object()
        group_specs=[SimpleNamespace(layer_names=group,kv_cache_spec=spec) for group in (groups or [names])]
        obj=SimpleNamespace(total_layers=target,num_layers=target,vllm_config=config(target),
            kv_cache_config=SimpleNamespace(kv_cache_groups=group_specs,num_blocks=4),
            _get_spec_total_num_kv_heads=lambda spec,idx:1,
            _get_kv_transfer_spec_key=lambda spec,heads:('MLA',1,1),
            _serialize_kv_group_spec=lambda group,**kw:{'layer_names':kw['layer_names'],'kv_cache_spec_type':'MLA'},
            _is_hma_required=False,_has_mamba_group=lambda:False,
            _requires_group_aware_attention_transfer=lambda:False,
            use_hybrid=False,_as_kv_cache_tuple=lambda tensors:tensors,kv_role=role,
            engine_id='cpu-metadata',te_rpc_port=0,block_size=2,side_channel_host='127.0.0.1',
            handshake_port=0,side_channel_port=0,tp_rank=0,tp_size=4,_prefill_tp_size=4,
            _prefill_pp_size=2,_prefill_pp_layer_partition='8,8',pcp_rank=0,engine=None)
        obj._build_kv_group2layeridx=MethodType(ns['_build_kv_group2layeridx'],obj)
        obj.kv_caches={name:(Tensor(1000+i*10),Tensor(1001+i*10)) for i,name in enumerate(reversed(names))}
        return obj
    name=lambda i:f'model.layers.{i}.self_attn.attn'
    checks=[]
    for label,indices,role in [('P-stage0',list(range(8)),'kv_producer'),
                               ('P-stage1',list(range(8,16))+[78],'kv_producer'),
                               ('D',list(range(16))+[78],'kv_consumer')]:
        names=[name(i) for i in indices]
        obj=worker(new,names,role=role)
        new['register_kv_caches'](obj,obj.kv_caches)
        mapping={n:i for spec,idxs in obj.kv_group2layeridx.values() for n,i in zip(spec['layer_names'],idxs)}
        for physical in indices:
            logical=16 if physical==78 else physical
            assert mapping[name(physical)]==logical
            assert obj.kv_caches_base_addr[logical]==[t.data_ptr() for t in obj.kv_caches[name(physical)]]
        assert len(obj.kv_caches_base_addr)==(8 if label=='P-stage0' else 17)
        if label!='P-stage0':
            selected=new['_get_group_kv_caches'](obj,0,[16])
            assert list(selected)==[name(78)] and selected[name(78)] is obj.kv_caches[name(78)]
        checks.append(label+' actual register address slots/name selection')
    # Exact D PP filter now sees logical16 and selects the correct original tensor78.
    obj=worker(new,[name(i) for i in range(16)]+[name(78)],role='kv_consumer')
    obj.kv_group2layeridx=obj._build_kv_group2layeridx()
    obj.pp_layer_indices={0:(0,8),1:(8,16)};obj.num_draft_layers=1
    new['self']=obj
    filtered=new['pp_layer_indices'](list(range(17)),1)
    selected=new['_get_group_kv_caches'](obj,0,filtered)
    assert filtered==list(range(8,17))
    assert list(selected)==[name(i) for i in range(8,16)]+[name(78)]
    checks.append('D PP last-stage8..16 selects target8..15 and actual MTP78 tensor')
    # Multiple groups and noncontiguous selected subset: full metadata zip before filter.
    names=[name(i) for i in range(16)]+[name(78)]
    groups=[[name(78),name(8),name(12)],[name(2),name(9),name(15)]]
    obj=worker(new,names,groups=groups);obj.kv_group2layeridx=obj._build_kv_group2layeridx()
    selected=new['_get_group_kv_caches'](obj,0,[12,16])
    assert list(selected)==[name(78),name(12)]
    assert all(selected[n] is obj.kv_caches[n] for n in selected)
    assert list(new['_get_group_kv_caches'](obj,1,[9]))==[name(9)]
    checks.append('group order/reversed cache dict/noncontiguous subset retain exact name-index pairing')
    # Outside the explicitly guarded configuration every original name/index remains unchanged.
    for target,indices in [(78,list(range(41,78))+[78]),(16,list(range(8,16))+[78])]:
        names=[name(i) for i in indices]
        for ns in [old,new]:
            obj=worker(ns,names,target=target)
            if target==16:obj.vllm_config=config(16,77)
            mapping=obj._build_kv_group2layeridx()
            if ns is old:expected=mapping
            else:assert mapping==expected
        checks.append(f'original mapping unchanged outside guard target{target}')
    assert new['_ks_n16_original_mtp_mapping'](config())
    assert not new['_ks_n16_original_mtp_mapping'](config(nextn=2))
    checks.append('exact target16/draft78/MTP1 guard')
    report={'status':'PASS','cases':checks,'case_count':len(checks),
            'actual_AST_methods':['_build_kv_group2layeridx','register_kv_caches','_get_group_kv_caches','pp_layer_indices'],
            'fixture_scope':'KV specs/tensor metadata/transport-thread event only; no inference or transfer',
            'device_imported':False,'installed_source_modified':False,'NPU_test':False}
    (HERE/'mooncake_n16_cpu_result.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    return report


def update_loaders():
    for relative in ['runtime/common/sitecustomize.py','runtime/p-original/sitecustomize.py']:
        path=OUT/relative;text=path.read_text(encoding='utf-8')
        if 'KS_N16_MOONCAKE_OVERRIDE' in text:continue
        text+='''
# Explicit private single-file override; never create a framework package namespace.
import os as _ks_n16_os
_ks_n16_source = _ks_n16_os.environ.get("KS_N16_MOONCAKE_OVERRIDE")
if _ks_n16_source:
    class _KSN16MooncakeFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector":
                return importlib.util.spec_from_file_location(fullname, _ks_n16_source)
            return None
    sys.meta_path.insert(0, _KSN16MooncakeFinder())
'''
        path.write_text(text,encoding='utf-8',newline='\n')
    path=OUT/'startup/common.env.native.sh';text=path.read_text(encoding='utf-8')
    if 'KS_N16_MOONCAKE_OVERRIDE' not in text:
        text+=f'export KS_N16_MOONCAKE_OVERRIDE={REMOTE}/{RELATIVE}\n'
        path.write_text(text,encoding='utf-8',newline='\n')
    for relative in ['runtime/common/sitecustomize.py','runtime/p-original/sitecustomize.py']:
        path=OUT/relative
        ns={'__file__':str(path.resolve())}
        import os
        previous=os.environ.get('KS_N16_MOONCAKE_OVERRIDE')
        try:
            os.environ['KS_N16_MOONCAKE_OVERRIDE']=str((OUT/RELATIVE).resolve())
            before=list(sys.meta_path)
            exec(compile(path.read_text(encoding='utf-8'),str(path),'exec'),ns)
            spec=sys.meta_path[0].find_spec(CANONICAL)
            assert Path(spec.origin).resolve()==(OUT/RELATIVE).resolve()
        finally:
            sys.meta_path[:]=before
            if previous is None:os.environ.pop('KS_N16_MOONCAKE_OVERRIDE',None)
            else:os.environ['KS_N16_MOONCAKE_OVERRIDE']=previous


def deploy():
    selected=[RELATIVE,'runtime/common/sitecustomize.py','runtime/p-original/sitecustomize.py',
              'startup/common.env.native.sh','README.md']
    payload={'root':REMOTE,'files':{name:base64.b64encode((OUT/name).read_bytes()).decode() for name in selected}}
    program=r'''
import ast,base64,json,os,subprocess,sys
from pathlib import Path
p=json.load(sys.stdin);root=Path(p['root'])
if str(root)!='/opt/ks-stage-ready-luv-20261027/reduced-pd' or root.resolve()!=root:raise ValueError('private root mismatch')
for name,blob in p['files'].items():
 rel=Path(name)
 if rel.is_absolute() or '..' in rel.parts:raise ValueError('unsafe path')
 path=root/rel
 if path.is_symlink():raise ValueError('symlink overwrite forbidden')
 path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(base64.b64decode(blob))
 if path.suffix=='.py':ast.parse(path.read_text())
 if path.suffix=='.sh':subprocess.run(['bash','-n',str(path)],check=True)
print(json.dumps({'root':str(root),'files':list(p['files']),'syntax':'PASS',
                  'services_started':False,'installed_source_modified':False,'model_view_modified':False}))
'''
    encoded=base64.b64encode(program.encode()).decode()
    command=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+
             '/tmp ks-pp-m137-priv python -S -c '+shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
    r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                       'root@115.120.85.137',command],input=json.dumps(payload).encode(),capture_output=True,timeout=60)
    if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
    result=json.loads(r.stdout)
    (HERE/'mooncake_n16_deployment.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--deploy',action='store_true')
    args=parser.parse_args()
    after,patch=generate_source();result=cpu_check(after);update_loaders()
    print(json.dumps({'CPU':result,'diff_lines':len(patch.splitlines()),'private_source':REMOTE+'/'+RELATIVE},indent=2))
    if args.deploy:print(json.dumps({'deployment':deploy()},indent=2))


if __name__=='__main__':main()
