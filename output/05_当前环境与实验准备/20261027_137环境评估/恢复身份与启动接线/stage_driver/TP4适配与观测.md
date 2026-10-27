# TP4/PP2 绑定增量

此增量只替换私有候选的 `vllm/v1/core/sched/ks_stage_reservation.py`，安装源码、core/executor/worker 和服务均未修改。远端路径为 `/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-candidate/vllm/v1/core/sched/ks_stage_reservation.py`。已运行完整 P 不热重载，下次进程导入才采用。

`bind` 从真实 Scheduler 初始化保存的 `scheduler.parallel_config` 读取 TP/PP/world，要求 TP 为正整数、PP=2、world=2*TP，并要求 controller 两段精确等于 `[0,TP)` 和 `[TP,2*TP)` 的全局整数 rank。原 core 的 DP1、PCP1、单节点约束不变。没有调整计算配置、MTP3 或两份 whole-batch 名额。

`partial_admission_decision` 是新增主机调度观察。`reason` 记录实际已有分支：`request_global_finished`、`restore_unregistered`、`front_not_ready`、`restore_invalid`、`unsupported_request_shape`，成功预留记录 `front_ready` 或 `all_stages_ready`；`decision` 区分 deferred、deferred_to_original、rejected、reserved。附带 `front_kv_ready`、`all_stages_ready`、`stage_busy`、`request_global_finished`。相同请求代次的相同状态不重复写入。`all_stages_ready` 和 `stage_busy` 只是观察，没有新增返回或拒绝条件；CPU 用例覆盖两者为 True 时保留原预留行为。

本机命令：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\stage_driver\check_scheduler_reservation.py"
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\stage_driver\check_integrated_first_token.py"
```

结果是 reservation 8/8，组合 2/2（TP8/PP2 和 TP4/PP2，各自启用实际 trace）。组合实际执行 Scheduler.schedule、typed wire、真实 worker 方法 AST、真实 KVOutputAggregator、真实 ModelRunnerOutput/AsyncScheduler.update_from_output。模型计算、设备、PP 通信、KV manager 和请求边界仍为明确 CPU fixture。两组均保持前段先执行、后段等待恢复及同批 activation、最终 KV prefix 入账一次、FIFO 消费后退休；sample 调用分别 16 次和 8 次，whole-batch 容量均为 2。不是 NPU 计算或性能证据。

先行失败记录确认 TP4 被旧固定 `(8,8)` 门拒绝；修改后通过。启用实际 trace 的组合还拦住过新日志代码误用 `_busy.values()` 的本地问题，已按真实 controller 的 list 结构纠正后通过，错误版本从未上传。

私有上传仅 2591 字节 gzip 源载荷，17 个精确映射保持；回执为 `tp_binding_private_deployment.json`，最小差异为 `tp_binding_delta.patch`。没有框架导入、算子、模型加载、服务启停或缓存操作。

安全运行范围仍为首 token、`max_tokens=1`、无已安排 speculative 输入，完整缓存前缀或仅余一个 token；不支持原输出 300 的阶段运行路径。TP4 绑定通过不能代替 N16 模型加载、Mooncake 双 Connector 释放或完整 PD 正确性验收。
