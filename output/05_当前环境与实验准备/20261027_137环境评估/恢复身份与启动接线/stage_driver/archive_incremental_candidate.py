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


def materialize_status_docs():
    """Create archival status snapshots without copying generated response text."""
    target=SOURCE/'docs'
    target.mkdir(parents=True,exist_ok=True)
    readme_source=SOURCE/'README.md'
    state_source=SOURCE/'本轮执行状态.json'
    agents_source=SOURCE.parents[2]/'AGENTS.md'
    captured={path:path.read_bytes() for path in (readme_source,state_source,agents_source)}
    (target/'README_本轮运行状态快照.md').write_bytes(captured[readme_source])
    state=json.loads(captured[state_source].decode('utf-8'))
    omitted=[]
    def strip(value,prefix=''):
        if isinstance(value,dict):
            for key in list(value):
                path=prefix+'.'+key if prefix else key
                if key in {'prompt','messages','generated_text','generated_texts','all_generated_text','all_generated_texts','completion_text','output_text'}:
                    del value[key]
                    omitted.append(path)
                else:strip(value[key],path)
        elif isinstance(value,list):
            for index,child in enumerate(value):strip(child,prefix+f'[{index}]')
    strip(state)
    write_json(target/'本轮执行状态_去生成文本.json',state)
    lines=captured[agents_source].decode('utf-8').splitlines(keepends=True)
    begin=next(index for index,line in enumerate(lines) if line.startswith('### 当前评估窗口（2026-10-27，137环境）'))
    end=next((index for index in range(begin+1,len(lines)) if lines[index].startswith('### ')),len(lines))
    excerpt=''.join(lines[begin:end])
    prefix='# AGENTS 137运行状态摘录（归档快照）\n\n仅摘录本轮运行状态，不是仓库规则入口；未覆盖仓库根AGENTS。源段落中的旧PID/待验描述以段尾最新状态为准。\n\n'
    (target/'AGENTS_137运行状态摘录.md').write_text(prefix+excerpt,encoding='utf-8',newline='\n')
    write_json(target/'来源与删减说明.json',{
        'captured_utc':datetime.now(timezone.utc).isoformat(),
        'README':{'source':str(readme_source),'copy':'README_本轮运行状态快照.md','mode':'exact source bytes'},
        'state':{'source':str(state_source),'copy':'本轮执行状态_去生成文本.json','mode':'numeric/status snapshot; generated/customer payload keys removed',
            'omitted_field_paths':omitted,'omitted_values_retained':False},
        'AGENTS':{'source':str(agents_source),'copy':'AGENTS_137运行状态摘录.md','source_lines':[begin+1,end],
            'mode':'exact current137 section plus archival label; repository root rules untouched'},
        'git_remote_status_independently_verified':False,'git_actions':[],'remote_actions':0})
    if any(path.read_bytes()!=content for path,content in captured.items()):
        raise RuntimeError('status source changed during snapshot creation')


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
    if (HERE/'archive_closing_selection.json').is_file():
        closing=json.loads((HERE/'archive_closing_selection.json').read_text(encoding='utf-8'))
        if closing.get('frozen') is not True:raise RuntimeError('closing source list is not frozen')
        selected += closing['relative_files']+[STAGE+'archive_closing_selection.json']
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
    materialize_status_docs()
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
    closing=json.loads((HERE/'archive_closing_selection.json').read_text(encoding='utf-8'))
    if closing.get('frozen') is not True:raise RuntimeError('closing source list is not frozen')
    selected = closing['relative_files']+[STAGE+'archive_closing_selection.json',STAGE+'archive_incremental_candidate.py']
    natural=json.loads((SOURCE/'减层PD运行证据/结果摘要.json').read_text(encoding='utf-8'))
    if (len(natural['runs'])!=6 or natural['performance_gain_verified'] is not False
            or natural['stage_dispatch_npu_verified'] is not False
            or any(run['client']['successful_requests']!=1 or run['client']['failed_requests']!=0
                   or run['client']['output_tokens']!=300 for run in natural['runs'])):
        raise RuntimeError('natural summary does not match the frozen six-run evidence boundary')
    def reject_payload(value):
        if isinstance(value,dict):
            if any(key in {'prompt','messages','generated_texts','generated_text','completion_text','output_text'} for key in value):
                raise RuntimeError('raw customer/generated payload key in compact natural summary')
            for child in value.values():reject_payload(child)
        elif isinstance(value,list):
            for child in value:reject_payload(child)
    reject_payload(natural)
    diagnostic_cpu=json.loads((SOURCE/'受控就绪窗口诊断/cpu_result.json').read_text(encoding='utf-8'))
    diagnostic_deployment=json.loads((SOURCE/'受控就绪窗口诊断/private_preparation_deployment.json').read_text(encoding='utf-8'))
    if (diagnostic_cpu.get('status')!='PASS' or diagnostic_cpu.get('case_count')!=11
            or diagnostic_cpu.get('NPU_test') is not False
            or diagnostic_deployment.get('changed_modules')!=['lmcache.integration.vllm.vllm_multi_process_adapter']
            or diagnostic_deployment.get('map_count_before')!=19 or diagnostic_deployment.get('map_count_after')!=19):
        raise RuntimeError('diagnostic source/preparation evidence boundary changed')
    balance=json.loads((SOURCE/'实际P调度接线修复/archive_selection.json').read_text(encoding='utf-8'))
    balance_cpu=json.loads((SOURCE/'实际P调度接线修复/cpu_result.json').read_text(encoding='utf-8'))
    if (balance.get('frozen') is not True or len(balance['relative_files'])!=15
            or balance_cpu.get('status')!='PASS_CPU_ONLY_NOT_DEPLOYED' or balance_cpu.get('test_count')!=6
            or balance_cpu.get('deployed') is not False):
        raise RuntimeError('actual Balance CPU-only candidate is not the frozen expected source')
    now=datetime.now(timezone.utc).isoformat()
    changed=[]
    changes=[]
    transformations=[]
    planned={(DEST/name).resolve() for name in selected}
    transformed_reports={'减层PD运行证据/结果分析.md','受控就绪窗口诊断/结果与接线缺口.md'}
    for relative in selected:
        source,target=SOURCE/relative,DEST/relative
        if not source.resolve().is_relative_to(SOURCE.resolve()) or not target.resolve().is_relative_to(DEST.resolve()):
            raise RuntimeError('selected recheck path leaves its allowed source/archive root')
        if (source.suffix in ('.log','.jsonl','.safetensors','.bin','.pem','.key')
                or any(part.lower() in {'data','venv','.ssh','__pycache__','model-n16','model','weights'} for part in source.parts)):
            raise RuntimeError('excluded content in frozen selection: '+relative)
        raw_content=source.read_bytes()
        content=raw_content
        text=content.decode('utf-8')
        private_key_marker='-----BEGIN '+'OPENSSH PRIVATE KEY'+'-----'
        if private_key_marker in text or re.search(r'https?://[^/\s:@]+:[^/\s@]+@',text):
            raise RuntimeError('credential material in frozen selection: '+relative)
        converted=[]
        if relative in transformed_reports:
            def local_link(match):
                label,reference=match.group(1),match.group(2)
                if reference.startswith(('http://','https://','#')):return match.group(0)
                referenced=(target.parent/reference.split('#',1)[0]).resolve()
                if referenced in planned or referenced.is_file():return match.group(0)
                if not (reference.endswith('.jsonl') or reference.endswith('/benchmark.json') or reference.endswith('.launch.json')):
                    raise RuntimeError('unclassified local report link: '+reference)
                converted.append(reference)
                return label+'（`'+reference+'`）'
            text=re.sub(r'\[([^\]]+)\]\(([^)]+)\)',local_link,text)
            note='原始证据仅保留本机/容器，仓库以本页精简JSON为数值凭据。'
            if note not in text:
                first,separator,rest=text.partition('\n')
                text=first+'\n\n'+note+'\n'+rest
            content=text.encode('utf-8')
            transformations.append({'relative_path':relative,'mode':'archive-copy-only raw-link conversion',
                'plain_path_references':converted,'source_report_modified':False})
        existed=target.is_file()
        if not existed or target.read_bytes()!=content:
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(content)
            changed.append(relative)
            changes.append({'relative_path':relative,'action':'updated' if existed else 'added'})
        if target.read_bytes()!=content or source.read_bytes()!=raw_content:
            raise RuntimeError('final N16 source changed or archive copy differs: '+relative)
        rows.setdefault(relative,{'relative_path':relative,'source':str(source),'action':'added_in_current_archive_revision'})
        rows[relative].update(bytes=len(content),bytes_equal=content==raw_content,utf8_text_equal=content==raw_content,
            source_bytes=len(raw_content),expected_archive_bytes_equal=True,source_unchanged=True,
            copy_mode='documented raw-link transform' if relative in transformed_reports else 'exact source bytes',
            last_recheck_utc=now,last_recheck_action='updated' if relative in changed else 'unchanged')
    dependency_paths=[(DEST/name) for name in balance['external_source_references']]
    dependency_paths += [DEST/'实际P调度接线修复/actual137_patch_balance_schedule.py',
        DEST/'实际P调度接线修复/patch_balance_schedule_fixed.py',
        REPO/'output/04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/vllm/v1/core/sched/output.py']
    if any(not path.is_file() for path in dependency_paths):
        raise RuntimeError('an explicitly referenced migrated CPU input is missing: '+repr([str(path) for path in dependency_paths if not path.is_file()]))
    summary={'status':'FROZEN_N16_FINAL_SOURCE_AND_EXISTING_CPU_RECEIPT_MATCHED',
        'updated_utc':now,'frozen_N16_files':frozen_count,'selected_with_selection_and_archive_entry':len(selected),
        'verification_scope':'closing files only; prior frozen N16 files not recopied',
        'N16_source_files_changed':sum(name.startswith(N16) for name in changed),
        'changed_archive_paths':changed,'final_proposer_review_diff_lines':32,
        'existing_CPU_receipt':{'file':'draft_n16_family_quant_cpu.json','status':'PASS',
            'special_weight_writes':7,'actual_quant_metadata_dispatch':'W8A8_DISPATCH_SUCCESS_REAL_TWO_KEYS',
            'evidence_class':'ACTUAL_AST_CPU_ONLY','rerun':False,'real_checkpoint_bytes_loaded':False},
        'runtime_boundary':{'source':'root report; not remotely inspected by this archive action',
            'D0':'ready','D1':'ready','P_previous_attempt':'target warmup DispatchFFNCombine operator error',
            'P_env0_attempt':'MC2 reported required HCCL window 413MB exceeds actual 400MB',
            'natural_PD_requests':'six HTTP/benchmark successes, original/candidate cold+warm+warm2',
            'natural_stage_admissions':0,'performance_gain_verified':False,
            'token_or_KV_content_correctness':'NOT_VERIFIED',
            'P_current_attempt':'DIAGNOSTIC_ONLY request output300 succeeded and partial reserved, but effective Balance schedule omitted two hooks; no stage worker compute',
            'full_formal_PD_acceptance':False,'candidate_stage_compute_success':False},
        'closing_archive':{'diagnostic_source_preparation_files':13,'diagnostic_result_source_checker_files':4,'natural_sanitized_files':2,
            'diagnostic_default_delay':'OFF; only separate diagnostic env enables 500ms',
            'diagnostic_CPU_cases':11,'diagnostic_stage_NPU_verified':False,'diagnostic_request_success':True,
            'diagnostic_stage_admission_observed':False,'actual_Balance_two_hook_gap_source_confirmed':True,
            'natural_result_source':'减层PD运行证据/结果摘要.json','natural_runs':6,
            'raw_customer_or_generated_payloads_archived':False},
        'actual_Balance_fix':{'selected_files':15,'existing_CPU_cases':6,'CPU_cases_rerun':False,
            'deployed':False,'NPU_verified':False,'loader_map_counts':[18,19,19,20],
            'explicit_migrated_dependencies_present':[path.relative_to(REPO).as_posix() for path in dependency_paths]},
        'copy_transformations':transformations,
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
    text+='CPU用例未重跑，未加载真实checkpoint。早期P曾在target预热遇DispatchFFNCombine错误；'
    text+='P两组随后共同采用官方FUSED_MC2=0及HCCL_BUFFSIZE=512，benchmark结果目录已匹配管理器。'
    text+='自然请求功能结果见下段；这不等于token/KV内容或候选阶段计算验收。复核细节见`N16_FINAL_SOURCE_RECHECK.json`。\n'
    text+='\n自然6次结果现已单独归档到`减层PD运行证据/`：6次HTTP/benchmark成功，未出现stage admission，不能证明提速，token/KV内容仍NOT_VERIFIED。'
    text+='该目录只带结果分析和精简JSON；文档中原始benchmark/metrics/log链接仍指原工作区，本仓库按数据边界没有复制那些文件。'
    text+='独立`受控就绪窗口诊断/`带13份源码/准备材料及4份精简结果/实际0.23调度源码/检查器；默认关闭，500ms只由独立env启用。'
    text+='真实单请求成功并出现partial reserved，但实际Balance schedule遗漏两钩子、未进入阶段worker；不能并入自然6次或标为模型阶段PASS。'
    text+='本轮README/状态及AGENTS的137运行段落另存docs，状态中历史生成文本字段已删除并记录，根仓库AGENTS未改。\n'
    text+='实际P调度接线修复的15项隔离候选与既有6项CPU结果已保存，loader为18/20 exact候选；仅核对明确引用依赖存在，未部署或复跑。\n'
    readme.write_text(text,encoding='utf-8',newline='\n')
    final_paths={'FINAL_ARCHIVE_CHANGED_PATHS.txt','FINAL_ARCHIVE_CHANGES.json'}
    metadata_changed={'ARCHIVE_README.md','COPY_INCREMENTAL_MANIFEST.json','COPY_INCREMENTAL_VALIDATION.json',
        'COPY_INCREMENTAL_FILES.txt','N16_FINAL_SOURCE_RECHECK.json','GIT_STAGE_CANDIDATES.txt',
        'GIT_STAGE_CANDIDATES.nul','GIT_STAGE_CANDIDATES_VALIDATION.json'}|final_paths
    archive_relative=DEST.relative_to(REPO)
    precise=sorted({(archive_relative/name).as_posix() for name in changed}|{(archive_relative/name).as_posix() for name in metadata_changed})
    (DEST/'FINAL_ARCHIVE_CHANGED_PATHS.txt').write_text('\n'.join(precise)+'\n',encoding='utf-8')
    write_json(DEST/'FINAL_ARCHIVE_CHANGES.json',{'updated_utc':now,'source_changes':changes,
        'updated_archive_metadata':sorted(metadata_changed),'precise_repository_paths':precise,
        'documented_copy_transformations':transformations,'source_reports_modified':False,
        'root_AGENTS_modified':False,'git_actions':[],'tests_rerun':False,'remote_actions':0})
    write_git_candidates()
    print(json.dumps(summary,ensure_ascii=False))


def write_git_candidates():
    """Derive exact paths from archive manifests; do not invoke Git or scan."""
    archive_relative=DEST.relative_to(REPO)
    initial=json.loads((DEST/'COPY_MANIFEST.json').read_text(encoding='utf-8'))
    latest=json.loads((DEST/'COPY_INCREMENTAL_MANIFEST.json').read_text(encoding='utf-8'))
    placement=json.loads((DEST/'INPUT_PLACEMENT.json').read_text(encoding='utf-8'))
    copied={row['relative_path'] for row in initial['copied_files']}
    copied.update(row['relative_path'] for row in latest['selected_files'])
    metadata={'ARCHIVE_README.md','COPY_MANIFEST.json','COPY_VALIDATION.json','INPUT_PLACEMENT.json',
        'COPY_INCREMENTAL_MANIFEST.json','COPY_INCREMENTAL_VALIDATION.json','COPY_INCREMENTAL_FILES.txt',
        'N16_FINAL_SOURCE_RECHECK.json','GIT_STAGE_CANDIDATES.txt','GIT_STAGE_CANDIDATES.nul',
        'GIT_STAGE_CANDIDATES_VALIDATION.json','FINAL_ARCHIVE_CHANGED_PATHS.txt','FINAL_ARCHIVE_CHANGES.json'}
    if len(placement['entries'])!=5 or any(row['status']!='NEW_INPUT_CREATED_EXACT_BYTES' for row in placement['entries']):
        raise RuntimeError('only the five recorded new inputs may leave the new archive area')
    old_inputs={row['path'] for row in placement['entries']}
    paths=sorted({(archive_relative/name).as_posix() for name in copied|metadata}|old_inputs)
    (DEST/'GIT_STAGE_CANDIDATES.txt').write_text('\n'.join(paths)+'\n',encoding='utf-8')
    (DEST/'GIT_STAGE_CANDIDATES.nul').write_bytes(b'\0'.join((':(literal)'+name).encode('utf-8') for name in paths)+b'\0')
    write_json(DEST/'GIT_STAGE_CANDIDATES_VALIDATION.json',{
        'status':'EXPLICIT_PROSPECTIVE_PATHS_ONLY_NO_GIT_ACTIONS','created_utc':datetime.now(timezone.utc).isoformat(),
        'candidate_paths':len(paths),'new_archive_paths':len(paths)-5,'necessary_new_old_input_paths':sorted(old_inputs),
        'source_manifests':['COPY_MANIFEST.json','COPY_INCREMENTAL_MANIFEST.json','INPUT_PLACEMENT.json'],
        'excluded_initial_read_only_dependencies':initial['existing_repository_inputs_checked_read_only'],
        'git_actions':[],'directory_scan_used':False,'hashes_used':False,'tests_rerun':False,
        'customer_data_or_model_files_included':False})
    readme=DEST/'ARCHIVE_README.md'
    text=readme.read_text(encoding='utf-8')
    text=re.sub(r'(?:本次显式核对|当前增量清单包含) \d+ 个文件；',
                f'当前增量清单包含 {latest["selected_count"]} 个文件；',text,count=1)
    marker='## 暂存候选路径（未执行Git）'
    text=text.split(marker,1)[0].rstrip()
    text+='\n\n'+marker+f'\n\n`GIT_STAGE_CANDIDATES.txt` 为本轮 {len(paths)} 个精确仓库相对路径，另有NUL分隔的`GIT_STAGE_CANDIDATES.nul`。'
    text+='来源仅为两份复制manifest、本目录索引和5个新增旧输入；没有Git、暂存、提交或推送动作。\n'
    readme.write_text(text,encoding='utf-8',newline='\n')


if __name__=='__main__':
    recheck_final_n16() if sys.argv[1:]==['--n16-final'] else main()
