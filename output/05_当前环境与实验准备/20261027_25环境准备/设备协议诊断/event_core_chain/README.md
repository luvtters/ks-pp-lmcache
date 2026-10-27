# 真实 NPU 事件到核心等待方法的组件链路

本诊断将已通过的两进程 IPC 事件组件继续连接到当前候选的真实核心等待方法：

`真实 Ascend IPC event → DeviceMessagingFuture → LMCacheStageObserver.drain_success → publish_observer_success → _KSStageEventPublisher → multiprocessing.Queue → MultiprocExecutor.poll_stage_events → EngineCore.step_with_batch_queue / _ks_wait_model_future → RestoreEventBinding → StageController.mark_kv_ready`

producer 和 consumer 两个 spawn 子进程共用显式指定的 NPU 15。producer 做 64 float 加法、导出非空事件 handle，并保留事件、backend、stream、张量直到 consumer 安全 ACK。consumer 的真实事件导入、查询、取结果只由 observer 唯一 owner 线程在 `torch.npu.device(device)` 中执行。父进程全程只有 CPU，不导入 torch、torch_npu 或 lmcache。

## 核心证明条件

父进程调用候选的真实 `EngineCore.step_with_batch_queue` 和 `_ks_wait_model_future` 方法；旧模型结果使用真实 `FutureWrapper`，其 `get_response` 是受控 CPU gate。事件 callback 先核对：旧 Future 未 done、scheduler 更新数为 0、`ks_model_wait_inflight()==1`、当前等待 Future 身份未变、逻辑批仍在途。随后通过真实 `RestoreEventBinding` 更新已登记请求的 worker 就绪位，核对只更新前段组件 worker，再发送 `CORE_CONSUMED` 并放开 CPU 模型 gate。

consumer 发送通知后必须等父进程 `CORE_CONSUMED` ACK 才关闭 publisher；已消费的通知不会被其丢弃式 close 丢掉。旧模型结果随后返回，scheduler 更新恰好一次；回执要求顺序为 `core-event-consumed → cpu-model-result → scheduler-update`。回调、scheduler 更新都必须在父核心线程，最终等待中的模型数回到 0。

这是两个逻辑阶段、每阶段一个 worker 的组件布局，身份为明确的合成诊断请求；物理 NPU 15 不能解释为真实模型 rank 元数据。它没有运行 GLM、真实 LMCache KV 恢复或完整 EngineCore bootstrap，没有验证 TP8/PP2 整体集成，也没有性能结论。候选 scheduler 和模型输出的周边对象仍是明确 CPU fixture；实际执行的 wait、FutureWrapper、publisher、Queue、poll 和 binding 代码保持当前候选 AST。

## 文件

部署以下七个文件到同一容器私有目录 `/opt/ks-stage-ready/device-probe/event_core_chain/`：

- `lmcache_event_core_chain_probe.py`：主入口及 consumer／CPU core 编排。
- `device_component.py`：此前事件 IPC 诊断原样复制，复用 producer、真实设备依赖加载与 owner 方法审计。
- `chain_runtime.py`：当前 executor/core AST 抽取的可导入模块；事件类模块名固定为 `chain_runtime`，保证 spawn 与 Queue pickle 一致。
- `chain_source_map.json`：原文件、类、行号、所选方法与 bootstrap 排除边界。
- `stage_observer.py`、`restore_binding.py`、`stage_controller.py`：现有候选原样复制。

`extract_chain_runtime.py` 是本机抽取入口；使用现有候选源码生成 runtime 和来源表，不安装依赖。完整 executor/core 的框架 import 和 bootstrap 不进入 runtime，所选方法体经 AST 原样保留，使用 `ast.unparse` 输出。`cpu_check.json` 是本机语法与导入拦截的 CPU-plan 检查，不是设备结果。

## 最简原生命令

机器25；本项目私有容器 `ks-luv-m25-stage-privcheck`；工作目录 `/opt/ks-stage-ready/device-probe/event_core_chain`。执行前主任务实时核对单卡 15 空闲及授权窗口。没有服务启动或参数切换。

无设备计划：

```bash
/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready/device-probe/event_core_chain/lmcache_event_core_chain_probe.py --cpu-plan --device 15 --output-dir /opt/ks-stage-ready/device-probe/event-core-chain-20261027 --timeout 90
```

真实设备组件；沿用已经验证的私有插件路径，不安装或改写环境：

```bash
export PYTHONPATH=/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/LMCache-Ascend-official-v0.5.6rc5:$PYTHONPATH
/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready/device-probe/event_core_chain/lmcache_event_core_chain_probe.py --run-device-probe --device 15 --output-dir /opt/ks-stage-ready/device-probe/event-core-chain-20261027 --timeout 90
```

输出目录必须是 `/opt/ks-stage-ready/device-probe/` 下尚不存在的新子目录。回执包括 `result.json`、`plan.json` 和 `producer.jsonl`、`consumer.jsonl`、`core.jsonl`。成功状态只允许 `PASS_REAL_EVENT_TO_CORE_COMPONENT_ONLY`。handle 只记长度，不输出内容；producer 安全 ACK、真实方法 owner 审计、核心 callback 条件、最终模型顺序与父进程没有设备库均纳入判断。

真实设备不支持事件时报告 `NOT_SUPPORTED`。错误或超时不能当设备完成；存活子进程及真实引用按此前 IPC 组件策略保留，`result.json` 记录 `retained_live_pids`，父诊断退出而不强杀。外层监督任务必须将其记录为失败，不能把清退当作安全事件完成。

本子任务只运行本机语法和 CPU-plan，未 SSH、未用卡。真实事件到核心通路为 **NOT_VERIFIED**，以主任务后续取回的设备回执为准。
