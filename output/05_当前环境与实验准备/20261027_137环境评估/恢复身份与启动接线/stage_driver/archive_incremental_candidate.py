"""Explicit local archive delta; no git, hashes, directory walk, tests or SSH."""
from __future__ import annotations
from datetime import datetime, timezone
import difflib
import json
from pathlib import Path
import re
import sys

from deploy_private_candidate import LOADER

HERE=Path(__file__).resolve().parent
SOURCE=HERE.parents[1]
REPO=Path(r'E:\work\ks\repos\ks-pp-lmcache')
DEST=REPO/'output/05_当前环境与实验准备/20261027_137环境评估'
STAGE='恢复身份与启动接线/stage_driver/'
N16='完整模型单机P对照/cache-clear-and-reduced-model/'
STAGE_FILES='''ks_stage_reservation.py check_scheduler_reservation.py check_integrated_first_token.py
build_scheduler_reservation.py build_unified_manifest.py unified_overlay_manifest.json
scheduler_reservation_build.json scheduler_reservation_cpu_result.json integrated_first_token_cpu_result.json
tp_binding_delta.patch tp_binding_private_deployment.json TP4适配与观测.md deploy_tp_binding_delta.py
deploy_private_candidate.py prepare_candidate_native.py repair_private_loader.py loader_parent_path_fix.json
check_mooncake_optional_finder.py mooncake_optional_finder_cpu_result.json
deploy_optional_mooncake_loader.py optional_mooncake_loader.patch optional_mooncake_loader_deployment.json
optional_n16_loader.patch optional_n16_loader_deployment.json check_baseline_import_exit.py
read_stage_runtime_snapshot.py archive_incremental_candidate.py
native_candidate/stage-candidate.env.native.sh native_candidate/p-stage-first-token.serve.native.sh
native_candidate/native.diff native_candidate/candidate_native_preparation.json
runtime_snapshots/20261027-202422/事实判读.md
launcher_snapshot/sitecustomize.py launcher_snapshot/deployment_contract.json
version_snapshots/full_p_first_token/ks_stage_reservation.py
version_snapshots/full_p_first_token/sitecustomize.py
version_snapshots/source_version_boundaries.json import_exit_summary.json
runtime_snapshots/20261027-202422/stage_ready_events.json'''.split()
STAGE_FILES += ['前段先就绪_机制诊断边界.md']


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


def materialize_small_evidence():
    """Preserve source versions using already-recorded exact text deltas."""
    relative='vllm/v1/core/sched/ks_stage_reservation.py'
    frozen=HERE/'version_snapshots/full_p_first_token/ks_stage_reservation.py'
    original=(frozen if frozen.is_file() else DEST/STAGE/'runtime_overlay'/relative).read_text(encoding='utf-8')
    current=(HERE/'runtime_overlay'/relative).read_text(encoding='utf-8')
    patch=''.join(difflib.unified_diff(original.splitlines(True),current.splitlines(True),
        fromfile='private-before/'+relative,tofile='private-after/'+relative))
    if patch!=(HERE/'tp_binding_delta.patch').read_text(encoding='utf-8'):
        raise RuntimeError('full-P helper does not match recorded pre-TP4 private source')
    frozen.parent.mkdir(parents=True,exist_ok=True)
    frozen.write_text(original,encoding='utf-8',newline='\n')
    draft='''    _draft_override = os.environ.get("KS_N16_DRAFT_PROPOSER_OVERRIDE")
    if _draft_override:
        _draft_path = Path(_draft_override)
        if not _draft_path.is_absolute():
            raise ImportError("KS_N16_DRAFT_PROPOSER_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.spec_decode.llm_base_proposer"] = _draft_path.resolve()
'''
    moon='''    _mooncake_override = os.environ.get("KS_N16_MOONCAKE_OVERRIDE")
    if _mooncake_override:
        _mooncake_path = Path(_mooncake_override)
        if not _mooncake_path.is_absolute():
            raise ImportError("KS_N16_MOONCAKE_OVERRIDE requires an absolute private source path")
        modules["vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector"] = _mooncake_path.resolve()
'''
    if LOADER.count(draft)!=1 or LOADER.count(moon)!=1:
        raise RuntimeError('reviewed optional loader blocks changed')
    loader18=LOADER.replace(draft,'')
    loader17=loader18.replace(moon,'').replace('import os\n','')
    for before,after,name in ((loader17,loader18,'optional_mooncake_loader.patch'),
                              (loader18,LOADER,'optional_n16_loader.patch')):
        delta=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),
            fromfile='stage-launcher-before/sitecustomize.py',tofile='stage-launcher-after/sitecustomize.py'))
        if delta!=(HERE/name).read_text(encoding='utf-8'):
            raise RuntimeError('loader version is not the recorded actual deployment source: '+name)
    (frozen.parent/'sitecustomize.py').write_text(loader17,encoding='utf-8',newline='\n')
    final=HERE/'launcher_snapshot/sitecustomize.py'
    final.parent.mkdir(parents=True,exist_ok=True)
    final.write_text(LOADER,encoding='utf-8',newline='\n')
    write_json(final.parent/'deployment_contract.json',{
        'source':'exact deployed LOADER literal; both private deployment patches match this version chain',
        'remote':'/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher/sitecustomize.py',
        'no_optional_environment_exact_mappings':17,'mooncake_only':18,'draft_only':18,'both':19,
        'partial_package_source_root_on_sys_path':False,'full_package_exit_pass':False,
        'loader_cpu_only':True,'service_actions_by_archive':0})
    write_json(HERE/'version_snapshots/source_version_boundaries.json',{
        'full_p_first_token':{'helper':'full_p_first_token/ks_stage_reservation.py',
            'loader':'full_p_first_token/sitecustomize.py','source_confirmed_by':'recorded exact private deployment diffs',
            'runtime_evidence':'full P cold/warm HTTP success and two full-stage KV readiness gates; no stage batch admission/execute trace'},
        'latest_private_candidate':{'runtime':'../runtime_overlay','loader':'../launcher_snapshot/sitecustomize.py',
            'TP4_binding_and_trace':'CPU TP8+TP4 Scheduler/wire/update pass, uploaded for next startup; not hot-reloaded into earlier full P',
            'N16_optionals':'same Mooncake/draft private sources selected by common P/D and candidate env',
            'limits':'max_tokens=1 first-token diagnostic only; no proof of stage GLM compute, speedup, output300 or full PD'},
        'archive_only':True,'no_new_tests_or_runtime_actions':True})
    comparison=json.loads((HERE/'import_exit_baseline_comparison.json').read_text(encoding='utf-8'))
    exits={}
    for name in ('candidate','baseline'):
        row=comparison[name]
        modules={}
        for line in row.get('stdout','').splitlines():
            try:
                value=json.loads(line)
                if isinstance(value,dict) and 'vllm.v1.engine.core' in value:modules=value
            except json.JSONDecodeError:pass
        exits[name]={'exit':row.get('exit'),'module_origins_printed':modules,
                     'stderr_last_line':row.get('stderr','').splitlines()[-1:]}
    write_json(HERE/'import_exit_summary.json',{'source':'local import_exit_baseline_comparison.json; raw import logs intentionally excluded',
        'rows':exits,'interpretation':comparison['interpretation'],'full_package_pass':False})
    snapshot=HERE/'runtime_snapshots/20261027-202422/raw.json'
    raw=json.loads(snapshot.read_text(encoding='utf-8'))
    events=[json.loads(line) for item in raw['files'] if item['path'].endswith('.jsonl')
            for line in item.get('content','').splitlines()]
    write_json(snapshot.parent/'stage_ready_events.json',{'captured_utc':raw['utc'],
        'source':'one read-only stage-candidate-trace snapshot; full runtime log intentionally excluded',
        'events':events,'stage_compute_entered_proven':False,'DMA_completion_order_proven':False})


def main():
    if not DEST.is_dir() or not DEST.resolve().is_relative_to(REPO.resolve()):
        raise RuntimeError('only the pre-existing new 137 archive area may be updated')
    n16_selection=json.loads((SOURCE/N16/'archive_n16_selection.json').read_text(encoding='utf-8'))
    if n16_selection.get('frozen') is not True:
        raise RuntimeError('N16 owner has not frozen the explicitly selected sources')
    materialize_small_evidence()
    unified=json.loads((HERE/'unified_overlay_manifest.json').read_text(encoding='utf-8'))
    selected=[STAGE+'runtime_overlay/'+item['relative_path'] for item in unified['files']]
    selected += [STAGE+name for name in STAGE_FILES]
    selected += [N16+name for name in n16_selection['relative_files']]
    selected += [N16+'archive_n16_selection.json']
    if len(selected)!=len(set(selected)):raise RuntimeError('duplicate selected path')
    originals={}
    for relative in selected:
        source=SOURCE/relative
        target=DEST/relative
        if (not source.is_file() or source.is_symlink() or not source.resolve().is_relative_to(SOURCE.resolve())
                or not target.resolve().is_relative_to(DEST.resolve())):
            raise RuntimeError('unsafe or missing selected source: '+relative)
        if (source.suffix in ('.log','.jsonl','.safetensors','.bin','.pem','.key')
                or any(part.lower() in {'data','venv','.ssh','__pycache__','model-n16','model','weights'} for part in source.parts)):
            raise RuntimeError('excluded content: '+relative)
        content=source.read_bytes()
        if len(content)>2_000_000:raise RuntimeError('selected source unexpectedly large: '+relative)
        text=content.decode('utf-8')
        private_key_marker='-----BEGIN '+'OPENSSH PRIVATE KEY'+'-----'
        if (private_key_marker in text
                or re.search(r'https?://[^/\s:@]+:[^/\s@]+@',text)):
            raise RuntimeError('credential material in selected file: '+relative)
        originals[relative]=content
    rows=[]
    for relative,content in originals.items():
        target=DEST/relative
        before=target.read_bytes() if target.is_file() else None
        action='unchanged' if before==content else ('updated_new_archive_file' if before is not None else 'added')
        if before!=content:
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(content)
        if target.read_bytes()!=content or (SOURCE/relative).read_bytes()!=content:
            raise RuntimeError('selected copy differs or source changed: '+relative)
        rows.append({'relative_path':relative,'source':str(SOURCE/relative),'bytes':len(content),
                     'action':action,'bytes_equal':True,'utf8_text_equal':True})
    report={'status':'EXACT_EXPLICIT_INCREMENTAL_ARCHIVE_SYNC_LOCAL_ONLY',
        'updated_utc':datetime.now(timezone.utc).isoformat(),'source_root':str(SOURCE),'archive_root':str(DEST),
        'selected_files':rows,'selected_count':len(rows),'selected_bytes':sum(row['bytes'] for row in rows),
        'actions':{kind:sum(row['action']==kind for row in rows) for kind in ('added','updated_new_archive_file','unchanged')},
        'original_299_files_written':0,'allowed_write_root':str(DEST),'hashes_used':False,
        'directory_walk_used':False,'git_actions':[],'tests_rerun':False,'remote_actions':0,
        'excluded':['customer prompt/data','model/weight files','venv','credentials','large runtime/import logs'],
        'unverified_NPU_scope':['actual stage GLM compute and earlier compute','stage candidate speedup',
            'N16 two private fixes full-model/PD correctness','output300 stage driver','full 1P1D comparison']}
    write_json(DEST/'COPY_INCREMENTAL_MANIFEST.json',report)
    (DEST/'COPY_INCREMENTAL_FILES.txt').write_text('\n'.join(row['relative_path'] for row in rows)+'\n',encoding='utf-8')
    write_json(DEST/'COPY_INCREMENTAL_VALIDATION.json',{key:report[key] for key in (
        'status','updated_utc','selected_count','selected_bytes','actions','original_299_files_written',
        'hashes_used','directory_walk_used','git_actions','tests_rerun','remote_actions','unverified_NPU_scope')})
    readme=DEST/'ARCHIVE_README.md'
    previous=readme.read_text(encoding='utf-8')
    marker='## 初次112文件归档记录（历史说明）'
    initial=previous.split(marker,1)[-1].lstrip() if marker in previous else previous.split('\n',1)[-1].lstrip()
    latest=f'''# 137本机源码归档

最新增量精确清单为 `COPY_INCREMENTAL_FILES.txt`，含逐文件来源、字节数及文本/字节一致结果的记录为
`COPY_INCREMENTAL_MANIFEST.json`。本次显式核对 {len(rows)} 个文件；对于与初次 `COPY_MANIFEST.json`
重名的路径，以增量清单为当前文件版本。原299文件及已放置的5个旧依赖输入未改动。

新增内容包括 TP4绑定/partial观测、父包路径修复、17→18→19可选精确加载器、私有部署与CPU回执，
以及 N16 两份共同私有修复的原始基线、最终源码、最小diff、必要CPU输入和 native/env/view构建入口。
不含客户 prompt/data、权重、venv、secret 或运行大日志。小型导入退出摘要及两行就绪事件不提升证据等级。

`恢复身份与启动接线/stage_driver/version_snapshots/` 保留完整P实际用过的TP8 helper与修复后17项loader，
并通过已记录私有部署diff逐字确认。`runtime_overlay/` 与 `launcher_snapshot/` 为最后私有部署版本；
TP4增量未热重载到早先完整P。最新 CPU 是 reservation 8项、wire 9项、TP8/TP4组合2项、finder5项；
本次仅复制已有结果，没有重跑。

完整P HTTP成功及全段KV事件登记不能证明阶段计算提前。实际阶段GLM计算、提速、输出300的阶段driver、
N16共同修复的完整PD正确性和正式1P1D对照仍未在本增量中验收。N16共同修复同时供P/D两组选择，
不可当作候选独占收益。详情见源版本边界JSON和N16 README/CPU proof。

本轮只有本机归档，没有git add/commit/push、远端动作、模型/算子或服务操作；没有hash和全仓扫描。

{marker}

'''
    readme.write_text(latest+initial,encoding='utf-8',newline='\n')
    print(json.dumps({key:report[key] for key in ('status','selected_count','selected_bytes','actions',
        'original_299_files_written','tests_rerun','remote_actions')},ensure_ascii=False))


def recheck_final_n16():
    """Recheck the frozen list and existing CPU receipt; never rerun its cases."""
    selection=json.loads((SOURCE/N16/'archive_n16_selection.json').read_text(encoding='utf-8'))
    if selection.get('frozen') is not True:
        raise RuntimeError('final explicit N16 source list is not frozen')
    frozen_count=len(selection['relative_files'])
    n16=SOURCE/N16
    before=(n16/'ascend_llm_base_proposer.py').read_text(encoding='utf-8')
    after=(n16/'reduced-pd-prepared/runtime/draft-n16/llm_base_proposer_n16.py').read_text(encoding='utf-8')
    patch=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),
        fromfile='actual137/llm_base_proposer.py',tofile='private/llm_base_proposer_n16.py'))
    if patch!=(n16/'draft_config_n16_review.patch').read_text(encoding='utf-8') or len(patch.splitlines())!=32:
        raise RuntimeError('final proposer source does not match its reviewed 32-line diff')
    proof=json.loads((n16/'draft_n16_family_quant_cpu.json').read_text(encoding='utf-8'))
    fixed=next(case for case in proof['cases'] if case['case']=='private_copy_fix_strict_all_seven_written')
    if (proof.get('status')!='PASS' or proof.get('special_weights_written')!=7
            or len(fixed['writes'])!=7 or fixed['not_written'] or fixed['strict_failure'] is not None
            or fixed.get('construction_model_type')!='glm_moe_dsa' or fixed.get('construction_num_layers')!=78
            or not any(case.get('status')=='W8A8_DISPATCH_SUCCESS_REAL_TWO_KEYS' for case in proof['quant_dispatch'])
            or proof.get('real_checkpoint_bytes_loaded') is not False or proof.get('npu_actions')!=0):
        raise RuntimeError('existing final CPU proof does not establish the stated limited evidence')
    for group in ('p-original-async','p-stage-candidate'):
        text=(n16/f'reduced-pd-prepared/startup/{group}.env.native.sh').read_text(encoding='utf-8')
        if text.count('export VLLM_ASCEND_ENABLE_FUSED_MC2=0')!=1:
            raise RuntimeError('both P groups must share the approved official MC2 fallback')
        if text.count('export HCCL_BUFFSIZE=512')!=1:
            raise RuntimeError('both P groups must share the approved HCCL window setting')
    for group in ('original-async','stage-candidate'):
        for phase in ('cold','warm'):
            text=(n16/f'reduced-pd-prepared/startup/bench-{group}-{phase}.native.sh').read_text(encoding='utf-8')
            if f'/results/{group}-{phase}' not in text or f'/results/{group}/{phase}' in text:
                raise RuntimeError('benchmark result directory does not match its manager')
    manifest_path=DEST/'COPY_INCREMENTAL_MANIFEST.json'
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    rows={row['relative_path']:row for row in manifest['selected_files']}
    selected=[N16+name for name in selection['relative_files']]
    selected += [N16+'archive_n16_selection.json',STAGE+'archive_incremental_candidate.py',
                 STAGE+'前段先就绪_机制诊断边界.md']
    now=datetime.now(timezone.utc).isoformat()
    changed=[]
    for relative in selected:
        source,target=SOURCE/relative,DEST/relative
        if not source.resolve().is_relative_to(SOURCE.resolve()) or not target.resolve().is_relative_to(DEST.resolve()):
            raise RuntimeError('selected recheck path leaves its allowed source/archive root')
        if (source.suffix in ('.log','.jsonl','.safetensors','.bin','.pem','.key')
                or any(part.lower() in {'data','venv','.ssh','__pycache__','model-n16','model','weights'} for part in source.parts)):
            raise RuntimeError('excluded content in frozen selection: '+relative)
        content=source.read_bytes()
        text=content.decode('utf-8')
        private_key_marker='-----BEGIN '+'OPENSSH PRIVATE KEY'+'-----'
        if private_key_marker in text or re.search(r'https?://[^/\s:@]+:[^/\s@]+@',text):
            raise RuntimeError('credential material in frozen selection: '+relative)
        existed=target.is_file()
        if not existed or target.read_bytes()!=content:
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(content)
            changed.append(relative)
        if target.read_bytes()!=content or source.read_bytes()!=content:
            raise RuntimeError('final N16 source changed or archive copy differs: '+relative)
        rows.setdefault(relative,{'relative_path':relative,'source':str(source),'action':'added_in_current_archive_revision'})
        rows[relative].update(bytes=len(content),bytes_equal=True,utf8_text_equal=True,
            last_recheck_utc=now,last_recheck_action='updated' if relative in changed else 'unchanged')
    summary={'status':'FROZEN_N16_FINAL_SOURCE_AND_EXISTING_CPU_RECEIPT_MATCHED',
        'updated_utc':now,'frozen_N16_files':frozen_count,'selected_with_selection_and_archive_entry':len(selected),
        'N16_source_files_changed':sum(name.startswith(N16) for name in changed),
        'changed_archive_paths':changed,'final_proposer_review_diff_lines':32,
        'existing_CPU_receipt':{'file':'draft_n16_family_quant_cpu.json','status':'PASS',
            'special_weight_writes':7,'actual_quant_metadata_dispatch':'W8A8_DISPATCH_SUCCESS_REAL_TWO_KEYS',
            'evidence_class':'ACTUAL_AST_CPU_ONLY','rerun':False,'real_checkpoint_bytes_loaded':False},
        'runtime_boundary':{'source':'root report; not remotely inspected by this archive action',
            'D0':'ready','D1':'ready','P_previous_attempt':'target warmup DispatchFFNCombine operator error',
            'P_env0_attempt':'MC2 reported required HCCL window 413MB exceeds actual 400MB',
            'P_current_attempt':'original group restarting with common P-only FUSED_MC2=0 and HCCL_BUFFSIZE=512; not verified',
            'PD_success':False,'candidate_stage_compute_success':False},
        'common_P_fallback':{'VLLM_ASCEND_ENABLE_FUSED_MC2':'0','HCCL_BUFFSIZE':'512','applied_to':['original-async','stage-candidate'],
            'D_files_changed_by_fallback':False,'benchmark_result_directories':'manager-compatible hyphenated paths'},
        'selected_content_review':'explicit whitelist excludes prompt/data/weights/venv/keys/logs; new additions are preparation/deployment and official source snapshots; native text references external data only',
        'git_actions':[],'tests_rerun':False,'remote_actions':0,'original_299_files_written':0,'hashes_used':False}
    manifest['selected_files']=list(rows.values())
    manifest['selected_count']=len(rows)
    manifest['selected_bytes']=sum(row['bytes'] for row in rows.values())
    manifest['updated_utc']=now
    manifest['actions_scope']='initial snapshot sync; final N16 recheck action counts are separate'
    manifest['final_N16_source_recheck']=summary
    write_json(manifest_path,manifest)
    validation_path=DEST/'COPY_INCREMENTAL_VALIDATION.json'
    validation=json.loads(validation_path.read_text(encoding='utf-8'))
    validation.update(updated_utc=now,selected_count=len(rows),selected_bytes=manifest['selected_bytes'],final_N16_source_recheck=summary)
    write_json(validation_path,validation)
    write_json(DEST/'N16_FINAL_SOURCE_RECHECK.json',summary)
    (DEST/'COPY_INCREMENTAL_FILES.txt').write_text('\n'.join(rows)+'\n',encoding='utf-8')
    readme=DEST/'ARCHIVE_README.md'
    text=readme.read_text(encoding='utf-8')
    marker='## N16最终源码复核'
    text=text.split(marker,1)[0].rstrip()
    text+='\n\n'+marker+f'\n\n{frozen_count}项冻结清单与最终32行draft GLM-family/HF78源码、diff及既有7权重/真实quant CPU回执已逐文件复核。'
    text+='CPU用例未重跑，未加载真实checkpoint。root报告两D已ready，P在target预热DispatchFFNCombine遇算子错误；'
    text+='P两组随后共同采用官方FUSED_MC2=0及HCCL_BUFFSIZE=512并重启原组，尚未验收；四个benchmark结果目录已匹配管理器。'
    text+='不能标为PD成功或候选阶段计算成功。复核细节见`N16_FINAL_SOURCE_RECHECK.json`。\n'
    readme.write_text(text,encoding='utf-8',newline='\n')
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':
    recheck_final_n16() if sys.argv[1:]==['--n16-final'] else main()
