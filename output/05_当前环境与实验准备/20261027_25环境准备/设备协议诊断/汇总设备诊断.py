"""只分析已取回的16进程JSON；不连接服务器、不导入设备框架。"""
import json
from pathlib import Path

HERE=Path(__file__).resolve().parent
summary=[]
for run in ('20261027-124320','20261027-124721'):
    folder=HERE/'执行记录'/run/'逐进程结果'
    rows=[json.loads(path.read_text(encoding='utf-8')) for path in sorted(folder.glob('rank-??.json'))]
    if {row['rank'] for row in rows} != set(range(16)):
        raise ValueError('Missing or repeated rank: '+run)
    groups=[]
    rounds=(0,1) if 'round_results' in rows[0] else (-1,)
    for iteration in rounds:
        selected=[row if iteration==-1 else row['round_results'][iteration] for row in rows]
        ranges={}
        for stage, ranks in (('front',range(8)),('back',range(8,16))):
            elapsed=[]
            for row in rows:
                if row['rank'] not in ranks:
                    continue
                events={event['action']:event for event in row['events']
                        if event.get('transfer_round',-1)==iteration}
                begin=events['queries_after_method_return']['monotonic_ns']
                end=events['bounded_work_query_finished']['monotonic_ns']
                elapsed.append((end-begin)/1e6)
            ranges[stage]={'min_ms':round(min(elapsed),3),'max_ms':round(max(elapsed),3)}
        groups.append({'round':iteration,
            'all_16_query_consensus_before_wait':all(item['nonblocking_query_consensus'] for item in selected),
            'query_consensus_interval_after_method_return':ranges,
            'interpretation':'CPU query and TP consensus interval, NOT pure HCCL/device or model duration'})
    summary.append({'run_id':run,'rank_count':len(rows),
        'all_16_transfer_pass':all(row['status']=='PASS_DEVICE_TRANSFER_ONLY' for row in rows),
        'all_8_back_payload_verified':all(row.get('tensor_content_verified') is True for row in rows if row['rank']>=8),
        'all_8_front_source_retained':all(row.get('source_retained_until_send_completed') is True for row in rows if row['rank']<8),
        'groups':groups,'new_scheduler_integrated':False,'real_lmcache_called':False,'model_tested':False})
folder=HERE/'执行记录/20261027-133354/逐进程结果'
rows=[json.loads(path.read_text(encoding='utf-8')) for path in sorted(folder.glob('rank-??.json'))]
if {row['rank'] for row in rows} != set(range(16)):
    raise ValueError('Missing stage-dependency ranks')
commands=[event['decision'] for event in rows[0]['events'] if event['action']=='cpu_core_command']
stage={'run_id':'20261027-133354','rank_count':len(rows),
 'all_ranks_pass':all(row['status']=='PASS_SYNTHETIC_STAGE_DEPENDENCY_ONLY' for row in rows),
 'legacy_denied_front_only':any(item.get('legacy_promoted_front_only') is False for item in commands),
 'front_dispatched_with_back_restore_latched':any(item.get('front_dispatch') and item.get('back_restore_latch_open') is False for item in commands),
 'activation_without_back_kv_denied':any(item.get('activation_ready_without_back_kv_rejected') for item in commands),
 'all_rank_content_checks':any(item.get('all_rank_content_checks') for item in commands),
 'scope':'synthetic tensors, actual H2D/HCCL/events, prototype controller and real old scheduler predicate; no model/performance'}
chain=json.loads((HERE/'event_core_chain/执行记录/20261027-133939/result.json').read_text(encoding='utf-8'))
before=chain['callback_records'][0]['before']
event_chain={'run_id':'20261027-133939','status':chain['status'],
 'model_future_not_done_at_callback':before['model_future_done'] is False,
 'scheduler_updates_at_callback':before['scheduler_updates'],
 'waiting_inflight_at_callback':before['waiting_inflight'],
 'model_order':chain['model_order'],'retained_live_pids':chain['retained_live_pids'],
 'scope':'real NPU IPC event to actual core AST seam; synthetic request identity and CPU model-result fixture'}
prep=HERE.parent
mq=json.loads((prep/'真实回复队列CPU纯导入结果.json').read_text(encoding='utf-8'))
spin=json.loads((prep/'真实回复队列CPU扩展导入结果.json').read_text(encoding='utf-8'))
dual=json.loads((prep/'dual_inflight_probe_25CPU结果.json').read_text(encoding='utf-8'))
result={'scope':'RECORDED_COMPONENT_AND_STAGE_PROTOCOL_ONLY','runs':summary,
        'stage_dependency':stage,'real_event_to_core':event_chain,
        'real_reply_queue':{'status':mq['status'],'actual_source':mq['actual_message_queue'],
             'same_reader_main_to_worker_to_main':mq['same_reader_main_to_worker_to_main'],
             'before_model_reply':mq['before_model_reply'],
             'spinloop_extension_enabled':mq['spinloop_extension_enabled']},
        'spinloop_requested_case':{'actual_extension_enabled':spin['spinloop_extension_enabled'],
             'coverage':'NOT_VERIFIED_EXTENSION_UNAVAILABLE_OR_FALLBACK'},
        'dual_inflight':{'status':dual['status'],'snapshots':dual['callback_snapshots'],
             'model_reply_FIFO':dual['model_reply_FIFO'],'scheduler_update_order':dual['scheduler_update_order'],
             'single_reader':dual['single_model_reply_reader'],'threads_left_alive':dual['threads_left_alive']}}
(HERE/'设备诊断汇总.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(result,ensure_ascii=False,indent=2))
