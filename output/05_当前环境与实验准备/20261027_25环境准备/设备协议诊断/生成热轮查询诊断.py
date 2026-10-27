"""从已通过的单轮脚本生成双轮诊断；不改框架源码，不运行设备。"""
import ast
from pathlib import Path

HERE=Path(__file__).resolve().parent
source=(HERE/'pp_stage_device_probe.py').read_text(encoding='utf-8')
source=source.replace('QUERY_WINDOW_SECONDS = 0.25','QUERY_WINDOW_SECONDS = 2.0',1)
source=source.replace('"artificial_sleep": False,','"artificial_sleep": False, "transfer_rounds": 2,',1)
source=source.replace('    def mark(action, **values):','    transfer_round = -1\n\n    def mark(action, **values):',1)
source=source.replace('"index": len(events) + 1, "action": action,','"index": len(events) + 1, "action": action, "transfer_round": transfer_round,',1)
start=source.index('            postprocess = []\n')
end=source.index('            kv_event.synchronize()\n',start)
body=source[start:end]
body=body.replace('"device-probe-0001"','f"device-probe-{transfer_round}"')
body=''.join('    '+line if line.strip() else line for line in body.splitlines(keepends=True))
prefix='''            result["round_results"] = []
            for transfer_round in range(2):
                # CPU组对齐，不向被测HCCL stream增加集合通信。
                dist.barrier(group=world.cpu_group)
                mark("transfer_round_start")
'''
suffix='''                result["round_results"].append({
                    "round": transfer_round,
                    "kind": "first_transfer" if transfer_round == 0 else "warm_transfer",
                    "nonblocking_query_consensus": result["nonblocking_query_consensus"],
                    "query_poll_rounds": result["query_poll_rounds"],
                    "work_query_at_return": result["work_query_at_return"],
                    "tensor_content_verified": result.get("tensor_content_verified"),
                    "source_retained_until_send_completed": result.get("source_retained_until_send_completed"),
                })
                mark("transfer_round_complete")

'''
source=source[:start]+prefix+body+suffix+source[end:]
source=source.replace('"nonblocking_query_window_seconds": QUERY_WINDOW_SECONDS,','"nonblocking_query_window_seconds": QUERY_WINDOW_SECONDS, "query_probe": "two_round_diagnostic",',1)
ast.parse(source)
target=HERE/'pp_stage_warm_query_probe.py'
target.write_text(source,encoding='utf-8')
print('Generated:',target)
