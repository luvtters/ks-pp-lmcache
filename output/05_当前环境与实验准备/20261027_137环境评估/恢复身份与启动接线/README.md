# 恢复身份与启动接线：隔离 Connector overlay

日期：2026-10-27。状态：`ISOLATED_CPU_HELPER_NOT_RUNTIME_INSTALLED`。

只新增本目录材料，没有覆盖原候选或安装源码，没有 SSH、占卡、启停服务或运行旧 126 项检查。现已生成完整隔离 Connector 文件、最小增量与初始化 runtime；该 overlay 不执行 GLM，不替代真实 scheduler 阶段 driver，也没有被宣称为已能正常启动模型。

## 已实现的最小桥接

- `SchedulerRestoreLedger.stamp(metadata, begin_restore)` 只读取真实 Connector metadata 内的 `RETRIEVE` 条目，为每次实际恢复分配单调代次，并在返回 metadata 前调用显式提供的 `begin_restore(request_id, generation)`。同一 metadata 重用代次，不重复登记。
- 代次与原始操作签名放在 Connector metadata 的 `_ks_restore_tickets` 独立侧车字段；不改 `LoadStoreOp`、token、block、APC skip、缓存 key 或 `request_configs`。签名保留 token、分组 block、start/end/skip。单组 block 列表被 IPC 展平时，签名按同一单组结构归一化。
- `WorkerRestoreBinding.bind_metadata(metadata)` 在提交前读取同一侧车，并强持有该次实际 `meta.op`。其 callable 接口符合候选 adapter 的 `identity_resolver(request_id, op)`，同时核对对象身份与完整操作签名，返回调用者提供的真实候选 `StageRestoreIdentity` 类型。重试内容完全相同时，迟到的旧 op 也不能借新代次发布；当前 `start_load_kv → batched_submit_retrieve_requests` 原样传递同一 `meta.op`，若未来改为复制 op，必须同步改为显式操作 ID，不能去掉身份检查。
- `WorkerRanks.from_parallel_groups(vllm_config, parallel_strategy, groups=None)` 默认读取 `vllm.distributed.parallel_state` 的 world、PP 和 TP 组，交叉核对配置和 adapter strategy；不使用 KV shard 的 `worker_id`。目前只接受 PP2/PCP1 和连续全局 rank 布局。
- scheduler 与 worker 均提供显式 `cancel(request_id, generation)`，拒绝旧 metadata、旧代重绑定和操作内容变更。**取消只撤销身份，不取消设备操作、不证明 DMA 结束、不释放 KV block。**真实 observer 对 pending/异常持有操作的保护继续适用。
- `configure_constructed_adapter(...)` 构造观察者、调用候选 `configure_ks_stage_observer`、启动唯一 owner。新 overlay 已在原 Connector 构造完成后调用该初始化链。CPU 案例执行原构造函数与实际 configure 方法，但 adapter 的网络构造和 NPU context 为明确 fixture；不能称现场 adapter 已构造成功。

每个 scheduler 生命周期只创建一个 ledger。不能在每个批次重新创建并从零计数，也不能在 worker 本地分配代次。当前 helper 没有完整接入请求结束、故障、重试和设备安全回收；调用者必须在旧恢复生命周期得到确认后决定是否开始下一次恢复。

## 真实源码插入点

下列行号来自本地未修改的旧归档，不是将来 overlay 的新行号。完整源码基线由私有归档提交 `daa4d895b2afa63fc33b6b8182c8d50479ac25e3` 固定；LMCache upstream commit 仍为 `NOT_VERIFIED`。

| 真实相对路径与行号 | 当前行为 | 最窄接线位置 |
|---|---|---|
| `lmcache/integration/vllm/lmcache_mp_metadata.py:335-405` | `GetRetrieveMetadata()` 从 tracker 的真实命中 token、allocated blocks、APC 命中生成 `LoadStoreOp` | 保留函数原逻辑；取它实际返回的 metadata，不另外构造一套伪造操作 |
| `lmcache/integration/vllm/lmcache_mp_connector.py:1682-1698` | `_process_retrieve_requests()` 为 WAITING_FOR_LOAD 的 tracker 生成 RETRIEVE metadata，随后将 tracker 置 READY | 在真实操作产生、metadata 离开 scheduler 前 stamp，并登记同代恢复；不能在 worker 猜代次 |
| 同文件 `1475-1507` | `build_connector_meta()` 汇总本轮 metadata | 此函数返回前也可做一次统一 stamp；应只选择一个登记点，避免重复分配 |
| 同文件 `773-781` | 正常 worker 分支构造 `LMCacheMPWorkerAdapter` | 其后显式配置 `WorkerRestoreBinding` 与唯一观察者；真实 publisher 的可用时机还须接线 |
| 同文件 `491-515` | `build_parallel_strategy_from_vllm_config()` 读取 `parallel_config.rank/TP/PP` | 用实际并行组交叉核验这些字段；不能拿 `kv_worker_id` 当全局 rank |
| 同文件 `921-947` | `start_load_kv()` 读取真实 metadata 的 request/op 并批量提交 | 在提交任何 retrieve 前 `binding.bind_metadata(metadata)`，再使用原 request/op 提交 |
| 候选 `lmcache_stage_observer/vllm_multi_process_adapter.py:2069-2096` | 配置观察者，并在 reserve 时调用身份解析器 | helper 的 worker binding 适配此接口；身份类型必须来自同一份候选 adapter 模块 |

完整旧路径均以以下两个本地目录为根：

- Connector：`E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\KV完成通知修改策略与隔离分支_20261022\lmcache_only_followup\source_current\lmcache\integration\vllm\lmcache_mp_connector.py`
- Metadata：`E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\KV完成通知修改策略与隔离分支_20261022\capacity_followup\sources\lmcache\integration\vllm\lmcache_mp_metadata.py`

## 新 CPU 检查的实际边界

`check_restore_metadata_bridge.py` AST 原样执行归档 `LoadStoreOp`、`LMCacheMPRequestMetadata.GetRetrieveMetadata()` 与 `LMCacheMPConnectorMetadata`，保留源文件路径和原始类行号。输入请求 tracker、单组 block 切片函数、并行组对象和 identity constructor 是明确 CPU fixture。

检查覆盖：真实 metadata 方法计算的 span/APC skip、原 op 不被改写、同 metadata 代次稳定、Python pickle 后侧车可用、取消后重试换代、旧代拒绝、内容相同的重试仍拒绝旧 op、缺失侧车拒绝、操作变更拒绝、全局 rank 与 KV shard 的区别、组与配置不一致／PCP>1 拒绝。

Python pickle 通过只证明该传递方式；**尚未验证 vLLM 现场 MessageQueue/序列化器保留新增侧车**。CPU fixture 读取真实组接口的方式，也不等于已从现场 worker 提取真实 rank。

本机 Windows，无容器，工作目录 `E:\work\ks`；输出位置为本目录 `cpu_result.json`，首次 helper 缺失的记录为 `red_result.json`：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\check_restore_metadata_bridge.py"
```

## 已生成的可应用文件

`build_connector_overlay.py` 只写本目录，生成：

- `overlay/lmcache/integration/vllm/lmcache_mp_connector.py`：完整 Connector 隔离副本。改动仅为原构造末尾初始化、build metadata 前检查／返回前 stamp、start-load 提交前绑定，以及关闭前的 observer/outbox 保护。
- 同目录 `ks_restore_runtime.py`、`ks_restore_metadata_bridge.py`：两个必须随 Connector 一起进入个人完整 LMCache 源码副本的新模块。
- `connector_init.patch`：相对上述旧 Connector 精确基线的最小增量，新增 6 行有效语句和 1 个空行，未改原计算／请求参数。
- `required_worker_publisher.patch`：相对 `draft_wait_followup/multiproc_executor.py` 的 3 行增量；把现场 `_KSStageEventPublisher` 注册到同进程 runtime。已进入下方生成副本，**没有应用到原候选或安装源码**。
- `overlay/vllm/v1/executor/multiproc_executor.py`：正常 WorkerProc 的发布器注册已加入生成副本。
- `overlay/vllm/v1/engine/core.py`：正常 EngineCore 构造末尾调用 `_ks_initialize_restore_notifications`，仅在观察者 flag 开启时创建真实 StageController／RestoreEventBinding、配置既有核心 pump，并绑定 scheduler 的 begin_restore。另两个 `ks_stage_controller.py`、`ks_restore_binding.py` 是现有原型的原样模块副本。
- `startup_hooks.patch`：相对最终 `draft_wait_followup/core.py` 和 `multiproc_executor.py` 的启动接线增量。
- `connector_overlay_build.json`、`connector_overlay_cpu_result.json`：构建及 5 项新 CPU 组合回执。

这三个 overlay 文件是源文件覆盖集合，**不能只把 `overlay/` 加入 PYTHONPATH 就认定生效**；真实 LMCache 是普通 Python package，必须在确认的个人完整源码副本内应用 Connector 增量并放入两个新模块，同时使用现有候选 adapter。安装包和公共源码不在此构建器写入范围内。应用前必须核对真实源文件与记录基线，不能因版本号同为 0.5.6rc5 就跳过。

机器：本机 Windows；容器：无；工作目录 `E:\work\ks`。构建与新增 CPU 检查入口（各检查负责自己的范围；不需要因此重复旧套件）：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\build_connector_overlay.py"
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\check_connector_overlay.py"
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\恢复身份与启动接线\check_startup_hooks.py"
```

## 发布器何时才真实可用

已归档 executor 的 `WorkerProc.__init__` 在 `draft_wait_followup/multiproc_executor.py:927` 调用 `_init_stage_event_channel`，创建实际 publisher；真正 LMCache Connector 更晚由 Ascend worker 的 `initialize_from_config`（`worker_split/worker.py:1126-1128`）进入 `ensure_kv_transfer_initialized` 时构造。原代码只把发布方法挂在 `WorkerWrapperBase` 上，Connector 不能从自己的 adapter 自动取到它。

因此 `required_worker_publisher.patch` 的明确调用点是 `_init_stage_event_channel` 现有 `ks_publish_restore_ready` 赋值之后，以原 `_ks_stage_publisher` 调用 `register_worker_publisher()`。该注册在本 worker 进程中，不能在父进程代做后假定 spawn 继承。

overlay 的原 Connector 构造末尾随后检查该注册，核对 publisher 与真实并行组的 rank，配置并启动唯一 observer，并启动只 drain CPU success outbox 的 `KSRestoreOutboxPublisher`。该线程不查询设备 Future；实际设备 query/result 仍只在 observer owner。发布异常会保留未交付身份并设置现有 publisher 的 shared failed 事件，不能默默丢弃后继续报告 READY。

现有侧信道、核心 pump、观察者分别仍需要已有的 `KS_PP_STAGE_EVENTS=1`、`KS_CORE_STAGE_EVENT_PUMP=1`、`KS_LMCACHE_STAGE_OBSERVER=1` 条件。它们只是必要条件，不构成完整启动入口。观察者启用时，缺真实 worker publisher 会使 Connector 构造报错；缺核心登记回调会在 build metadata 前报错。

## 新 CPU 组合实际验证

新增 5 项检查执行完整原 Connector 构造体，以及实际 `configure_ks_stage_observer`、`build_connector_meta`、`_process_retrieve_requests`、`GetRetrieveMetadata` 和 `start_load_kv` 方法。实际生成的 metadata 经 Python pickle 后再交给 worker；提交仍使用收到的同一个真实 `meta.op`，由原观察者代码经 outbox、真实 `_KSStageEventPublisher` 与真实 multiprocessing.Queue 交付带 request/generation/rank 的独立消息。

网络 adapter 构造、传输、CPU 已完成 Future、NPU context、并行组和核心 callback 是 fixture；没有运行真实 KV 搬运或完整 EngineCore。检查同时确认缺 publisher、缺 core callback 时明确拒绝，默认关闭时继续原构造路径。关闭代码保留 pending／异常设备引用存在时的拒绝保护；本次未运行真实设备安全回收。这里没有把 fixture 换成模型推理的说法。

随后新增的 5 项正常启动方法检查已通过，回执 `startup_hooks_cpu_result.json`。实际执行生成副本的 `WorkerProc._init_stage_event_channel` 和完整 `EngineCore.__init__` 方法体；模型初始化／executor 为明确 CPU 边界 fixture。检查从实际发布器注册、核心 controller/pump 构造、真实 metadata 生成及传递，一直走到 worker 提交与核心 consume 登记。回调后只有后段 worker 10 的就绪事实，模型更新数 0、逻辑在途批 0、前后段均无可派发批；证明的正是“通知已经正常接线，**计算仍未被阶段 driver 安排**”。原容量 2、TP8／PP2、MTP3 fixture 值保持。

## 仍需分工接通的精确位置

1. **生成副本的正常启动尚需现场应用验证**：core／worker hook 现已实际生成并通过 CPU 方法检查，但没有进入服务器私有完整源码树，也没有运行真实模型初始化。core hook 明确标记 `_ks_restore_notifications_only=True`，不安装阶段 dispatcher。完整 package 导入、线程亲和性、NPU context 与真实模型输出仍需现场验证。
2. **worker hook 应用**：将包含 `startup_hooks.patch` 的生成副本只用于版本相符的个人 executor/core 工作区；必须在同一 worker 进程且先于 Connector 构造注册 publisher。单放 Connector overlay 而不接此 hook 会明确失败。
3. **真实 wire**：Python pickle 通过不等于现场 vLLM MessageQueue 编码／解码已验证，须确认 `_ks_restore_tickets` 随实际 SchedulerOutput 的 Connector metadata 到达 worker。
4. **真实阶段 driver**：仍需把真实 SchedulerOutput 的成员、token 位置、KV blocks 接到 `admit_batch/take_ready` 及前后段动作。当前独立通道只含 input/model-ready 查询与释放；prepare/finalize/execute 不在其命令表。没有改变原名额、拓扑或 MTP 来绕过这个缺口。
5. **请求生命周期**：取消／重试身份撤销、完成后的 ledger 清理和 stage 资源回收需由真实 driver 接线。当前 observer 对 pending／异常仍保留设备引用，safe reap 未实现。关闭保护只保证本地 observer/outbox 不被直接丢弃，**不宣称 publisher 队列已被核心消费或传输已 drained**；旧 publisher 的丢弃式 close 边界仍存在。

以上文件足以审查和应用此段窄增量，但不能称完整正常启动链已接通。

本轮另一项真实 MP STORE→RETRIEVE 诊断直接使用 TransferContext，不经过本目录的 scheduler/adapter 初始化。因此该 NPU 结果不能替本目录证明真实业务 metadata 传播或正常启动；本目录也不阻塞该诊断先用卡。
