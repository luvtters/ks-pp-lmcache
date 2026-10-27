# 阶段协议：token span、typed 结果与输出所有权

2026-10-27。当前状态：`FIRST_TOKEN_DRIVER_CPU_INTEGRATED_NOT_NPU_VALIDATED`。首 token 窄路径的真实 scheduler 预约、typed 阶段 RPC/Future 和最终 AsyncScheduler 更新已经生成并组合通过；完整包私有导入和真实 GLM 候选仍待现场验证。通用长输出／任意 chunk／取消路径未实现。

上级目录最初的通知 overlay 仍只登记事实；本目录的 `runtime_overlay/` 已进一步接入阶段计算安排，不能混用两份目录。最新交付范围与模块映射以 `unified_overlay_manifest.json` 为准，RPC/Future 细节见 `EXECUTOR_WIRE.md`。

## 当前首 token 完整 CPU 路径

- 原 scheduler 只有在前段全部 8 个必要 worker 真实恢复就绪时才预约；依旧由原 `Scheduler.schedule()` 分配预算／blocks、生成 NewRequestData，保留延迟 KV 缓存，不伪造 global finished。
- 完整 SchedulerOutput 带 `ks_stage_batch` 交给 typed driver。后段先准备接收，前段先执行；后段自身全 TP KV 和同批 activation 均齐才计算。执行和采样分别兑现原核心期待的 Future，front/back 各采样一次。
- 真实全 rank KV 聚合信号在真实 final update 前处理，避免请求因 `max_tokens=1` 停止／free 后再查找失败；prefix 只由原 AsyncScheduler 在最终模型输出消费时缓存一次。
- 更新结果后，核心按 FIFO 确认消费，再退休后段和整批名额，原两个名额保持。

`scheduler_reservation_cpu_result.json` 为 6 项通过；`executor_driver_cpu_result.json` 为 9 项通过，包含实际 Core step／reader／wait-pump 闭环；`integrated_first_token_cpu_result.json` 是完整组合检查通过：真实 schedule → 16 rank CPU wire → 真实 ModelRunnerOutput → 实际 AsyncScheduler 更新。前段已执行但后段 KV 未好时 cache 调用 0；最终 prefix cache 调用 1、返回 token 71、16 rank 各采样 1 次、placeholder 归零、两名额最终全部退休。模型 forward、NPU event、通信和 cache manager 为明确 CPU fixture；这不是 GLM/NPU 或性能验收。

实际范围限定为已确认 `async_scheduling=True`、单请求的首 token、`max_tokens=1`、无已调度 spec token／structured output、缓存恢复后仅剩 1 个输入 token。MTP3 配置和原两个名额不改；冷／未命中请求走原完整模型路径。客户原文可用，但输出改 1 必须记录为机制诊断，不能当逐条原输出的正式客户用例。

共同 17 行 Connector 与实际137 adapter 已分别重基：前者 36 个非目标方法 AST 保持，后者保留实际 scheduler lookup 行为，仅增加唯一观察者的必要 worker 增量。observer 和 identity 定义内嵌在同一个 adapter 模块；私有 loader 不应再导入第二份同名类型。

构建、文件别名和依赖边界见 `unified_overlay_manifest.json`；该清单明确尚未通过私有完整 package 导入，不能仅靠 Python 源码语法／AST 方法检查断言可部署。

在保留原 TP8/PP2、DSA CP、MTP、长输出请求和两个 whole-batch 名额的要求下，现已针对两个耦合点写出独立可执行实现，不再只记录拒绝：同请求多批按 token span 管理；模型载荷、设备状态、控制确认分型并保留结果所有权。未修改这些模型／请求参数；本次 CPU 检查不执行真实 MTP 计算。

## 已交付的最小实现

| 文件 | 实际实现 |
|---|---|
| `span_stage_controller.py`、`span_controller.patch` | 基于原控制器生成独立副本；同一请求的两个不重叠、顺序 token span 可占满两个整批名额，重叠／倒序 span 拒绝。每阶段仍只有一个实际 buffer owner，前驱计算完成不会直接放开这个 owner |
| `typed_stage_protocol.py` 的 `StageReply` | `MODEL_RETURN` 可承载明确提供的 ModelRunnerOutput／AsyncModelRunnerOutput 类型或 None；`DEVICE_READY` 只接受真实 probe 的 bool；`CONTROL_ACK` 仅确认控制方法返回；`ERROR` 是显式失败。None、ACK、错误均不等于设备完成或最终输出 |
| 同文件 `TypedResultLedger` | 两个整批结果名额；按 batch/stage/worker/command 关联并拒绝重复。全 rank sample 未齐不聚合，前段 KVConnectorOutput 样本保留并按原 rank 顺序交给传入的真实聚合器；最终结果只允许核心按 FIFO 确认消费 |
| 同文件 `StageWorkerProtocol` | 调用实际 worker 的 prepare/finalize/execute/sample/query/release 方法，不制造 READY。前段必须真实 sample 返回、runner 的 ExecuteModelState/KVConnectorOutput 已被原方法消费、实际 release probe 通过，才调用原 release；退休后仍持有 typed sample，等核心最终消费才可放开 |

控制器明确区分：模型设备完成 → 本 rank sample 已消费 → 本阶段 buffer 退休 → 最终结果可按 FIFO 交给核心 → 核心真实消费 → 后段 buffer 退休 → 整批名额释放。不能把其中任何一个 ACK 当作所有步骤完成。取消／设备故障的协调回收尚未实现，因此新控制器 `cancel` 明确拒绝并保留状态，不伪造 drained。

新结果 `typed_stage_cpu_result.json` 为 **15 项 CPU 通过**。实际 AST 方法包括 `NPUWorker.execute_pp_stage_model`、`pp_stage_model_ready`、`sample_tokens`、`pp_stage_release_ready`、`release_pp_stage_batch`，以及真实 NPUModelRunner 的前段 `sample_tokens` 分支。模型 forward、NPU event、PP group、通信完成条件及模型输出类是明确 CPU fixture。

检查发现并修复了一个实际所有权问题：真实 sample 可能先清 runner 的 Connector 字段，再在后续检查抛异常。wrapper 现保留调用前的 Connector/state 引用并锁住失败批；不能重试一次得到 None 就把原失败伪装成成功。

新控制器与协议尚未替换上级目录通知 overlay 内的旧 controller，也没有进入安装源码。调用者须明确迁移新生命周期接口，不能只换 import 后继续按原 `pop_committable` 行为运行。

## 四项有限 CPU 源码检查

`verify_current_driver_contracts.py` 原样 AST 执行已有方法，4 项通过，结果见 `driver_contract_result.json`。它只提供阻塞证据，不是新的 runtime driver 验收。

| 真实源码／方法 | 检查结果 | 为什么不能用快捷改法 |
|---|---|---|
| 25 快照 `vllm/v1/core/sched/scheduler.py:2188-2203` 与 `2154-2186` | 只有前段就绪时原 promote 返回 False。显式反事实把请求放入 global finished 后，实际代码会调用整请求 `cache_blocks`，全命中时把 computed token 回退 1 | 不能用前段 READY 冒充全请求恢复完成；会把后段尚未完成的 KV 按全局前缀缓存处理 |
| `controller/stage_controller.py:118-148` | 两个名额确实存在：不同请求可各占一批；但同一请求第二个在途批在名额未满时仍被拒绝 | 原 chunked prefill、异步调度、MTP 可能让同请求下一批在前批模型结果 commit 前产生。现有 controller 不能直接承接这一协议；不能把隐式串行或 maxbatch1 当同条件结果 |
| `draft_wait_followup/multiproc_executor.py:1078-1101` | 显式反事实仅给 execute 加白名单，实际 handler 收到模型输出对象后发 CONTROL_ERROR，因为它只接受 bool/None | prepare/finalize 的控制确认与 execute/sample 的模型结果不是同一类载荷；必须定义模型结果的归属和聚合路径，不能丢掉 ModelRunnerOutput/KVConnectorOutput |
| `worker_split/worker.py:770-802` | 即使前段计算事件已完成，runner 仍持有 KVConnectorOutput 时，实际 release-ready 返回 False | “前段算完”不能直接释放／覆盖它的输出状态。要保留原采样、MTP、Connector 完成信息和发送源生命周期 |

request/cache-manager、模型输出对象及计算完成条件为 CPU fixture。第一项的 global finished 注入和第三项的白名单添加是明确反事实，目的是验证所提快捷改法为何不足；不是报告现场系统已经这样错误运行。

## 通用 driver 仍须覆盖的三个协议

1. **真实阶段调度记账**。异步恢复时，现有 scheduler 在 `allocate_slots(..., delay_cache_blocks=True)` 后把请求留在 WAITING_FOR_REMOTE_KVS，并不加入本轮 NewRequestData／计算安排（`scheduler.py:761-824`）。新 driver 需要从已分配的真实 block、真实 token 位置构造合法阶段批，预留原 token/请求预算，防止后续普通 scheduler 再次安排同一份工作；全局 KV 前缀缓存必须继续等各段真实恢复完成。不能临时把全局 finished 置真来借用原分支。
2. **阶段执行与最终模型回复分开，但最终回复保持原序**。原 `execute_model`／`sample_tokens` 经 `collective_rpc` 和 KVOutputAggregator（`multiproc_executor.py:570-592,632-675`）；原 core 每批保存 `(sample_future, scheduler_output, exec_future)` 并在真实结果返回后更新 scheduler（`core.py:679-740`）。新增阶段 prepare/finalize/execute 不能直接走 bool-only 控制回复，也不能把额外的全 rank 模型回复写入旧 MQ 而不提供匹配 reader。需保持每批 execute/sample/draft 的可关联身份与 exactly-once 最终更新。
3. **每批输出所有权**。真实 NPUModelRunner 前段在 `model_runner_v1.py:2353-2361` 把 Connector 输出放入 IntermediateTensors，并保留 `self.kv_connector_output`；`sample_tokens:2423-2444` 才取走并清空。末段还持 ExecuteModelState（`2395-2416`），MTP 后才 finalizes Connector（`2543-2548`）。若希望前段跨到下一逻辑批，必须证明本 rank 对前批 sample／Connector 信息已消费，且旧全批结果仍正确留在 FIFO；不能手工清字段或凭设备完成事件宣布整批完成。

另外，当前 worker 每阶段只有一个 `_ks_pp_stage_current` 记录。两个 whole-batch 名额不等于每阶段能同时覆盖两份输入／输出；新控制器保持该阶段 owner 到实际退休，允许两个批按前后段交错。新控制器已支持同请求多个不重叠在途 span，但**span 不重叠不证明下一个自回归／MTP token 输入已经可用**；真实 driver 仍须复用原 draft/采样 FIFO 和合法 SchedulerOutput，不能跳过 token 数据依赖。

尤须避免重复采样：若新 driver 已分段消费 front.sample(B1)，随后再原样广播全 rank sample(B1)，前段可能误取当前 B2 的 Connector 输出。正确 wire 必须复用保留的 front sample(B1)，再取得 back sample(B1)，按全 rank 原顺序聚合成 B1 的同一个最终 Future。新结果 ledger 提供这个聚合 seam，尚未替换原全 rank RPC 路径。

## 能继续复用的最窄边界

- 上级目录的真实 metadata generation sidecar、worker rank 绑定、唯一 observer、独立发布器和正常 core notification hook 可继续复用。
- StageController 的全 TP KV／activation 依赖规则可继续作为资格规则；其 admission 生命周期需要适配真实 scheduler，不能当已经完成的 driver。
- `prepare_pp_stage_input`、`finalize_pp_stage_input`、`execute_pp_stage_model` 与 release probe 是可复用的 worker 方法，但需要合法异步派发与模型输出协议。
- 首 token 的 scheduler 预约与 RPC/Future 路由已经生成并组合通过；通用长输出、多 chunk、MTP 输入依赖和取消回收仍不能从这项窄诊断外推。

机器：本地 Windows；无容器；工作目录 `E:\work\ks`。本次不 SSH、不占卡、不改安装源码，不运行旧 126 项：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\stage_driver\verify_current_driver_contracts.py"
```

构建独立 span 控制器及验证新协议（不运行旧 126 项）：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\stage_driver\build_span_controller.py"
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\stage_driver\check_typed_stage_protocol.py"
```

主任务可在私有完整导入和版本核对后验证原 P 基线及首 token 窄候选的真实模型正确性；提前计算与性能仍需实际设备时序和模型结果证据。不能将窄诊断扩称为通用长输出 driver 或正式客户性能收益。
