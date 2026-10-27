# LMCache阶段观察器：唯一设备Future所有者与CPU代理

2026-10-27。**状态：15项定向CPU通过；NOT_RUNTIME_INTEGRATED。** 只在本目录生成候选，没有SSH、用卡、部署、服务启停或运行源码覆盖。另按审阅要求，仅在已交付`control_sidechannel/README.md`补充了“丢弃式退出不等于drained”的一句说明。

本候选补的是一个具体接口：原adapter的`get_finished`两套实现不能与新增观察线程并发查询同一个`DeviceMessagingFuture`。现在将真实设备Future交给唯一观察器，原`retrieve_futures`只保存当前版本真正的`MessagingFuture[bool]`作为CPU代理。原getter查询的是CPU Event，不再导入设备事件或同步设备。

## 源码与修改位置

构建输入：

- `../../KV完成通知修改策略与隔离分支_20261022/lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py`，实际归档`submit_retrieve_request`在1885行；两套getter在2075与2221行。上面路径中的`../..`以本README所在目录为起点；精确绝对路径保存在`build_result.json`。
- 同归档`lmcache_only_followup/sources/lmcache/v1/multiprocess/futures.py`中的真实`MessagingFuture`与`DeviceMessagingFuture`。Git提交未核验，记录为`NOT_VERIFIED`。

`build_lmcache_stage_observer.py`将`stage_observer.py`的定义注入adapter隔离副本，输出`vllm_multi_process_adapter.py`与`lmcache_stage_observer.patch`；没有改原输入或Future实现，没有新增运行依赖。

真实接线改动：

1. 新增`configure_ks_stage_observer`显式接口，默认关闭。只有候选源码环境变量`KS_LMCACHE_STAGE_OBSERVER=1`且显式注入观察器与身份解析器才允许配置。**这个开关没有部署，也不会自动产生观察线程。**
2. `submit_retrieve_request`在真正提交前预留带真实身份的操作，防止缺少元数据时已经发出无法归属的传输。`transfer_ctx.submit_retrieve`返回后，`attach`把真实Future交唯一观察器；`retrieve_futures`仍保存原二元组结构，但其中Future换为CPU代理。
3. 唯一owner先`real_future.query()`；只有返回True才调用真实`result(timeout=0)`。当结果严格为True且代次仍有效时，把成功身份放入本地outbox，再完成代理。False只完成False代理；异常只对代理`set_exception`，不会发成功，也不会使单次Future错误杀掉观察线程。
4. 原`get_finished`与lazy getter保持成功、False错误block/request、finished_sending、store处理及pop行为。唯一新增语句在unhealthy清表前撤销本批阶段发布资格，防止晚到成功变成错误放行。
5. 不pop adapter表、不释放vLLM块、不修改store Future。代理强持有真实Future和提交依赖event，观察器也持有未终结操作；观察器不会拿producer event冒充restore完成事件。

## 必须显式提供的输入

`StageRestoreIdentity`要求：`request_id / restore_generation / stage_id / tp_rank / pcp_rank / global_worker_rank`。没有默认代次，不本地自增，也不把adapter的KV shard `worker_id`当PP/TP身份。身份解析器接口为`identity_resolver(request_id, op)`；操作另记录`start / end / skip_first_n_tokens / flat_block_ids`签名。

真实generation应由scheduler／connector一次恢复操作的元数据分配并传播；真实PP、TP、PCP及全局rank应来自本worker的并行组。**本目录没有修改这些上游字段或完成广播。当前直接启用缺少这些输入，必须拒绝。**

观察器构造必须提供`device_context_factory`；只有显式调用`start()`才创建线程。该工厂在owner线程中进入，所有真实Future的查询和结果读取都在该线程内；锁只保护CPU表、状态与outbox，设备导入／query／result均在锁外。CPU测试使用`nullcontext`及CPU事件后端，**不能将其作为真实NPU线程context绑定已验证的依据**。真实Ascend设备绑定仍待接线和验收。

## 本地CPU入口

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。本轮没有模型启动、配置切换或推理执行。

构建隔离副本与diff：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\lmcache_stage_observer\build_lmcache_stage_observer.py"
```

执行定向CPU验收：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\lmcache_stage_observer\test_lmcache_stage_observer.py"
```

本轮实际**15项通过**，回执为`cpu_result.json`。没有重跑原66项或侧信道14项。

测试直接AST执行归档的真实`MessagingFuture / DeviceMessagingFuture`、候选adapter真实`submit_retrieve_request / get_finished / get_finished_with_lazy_offload`、store完成合并及新增shutdown保护。CPU替身仅提供事件后端、传输入口与无关服务依赖；没有重写一套假Future或假getter来证明自己。

- 原始回复未到、设备事件未完成时，代理未完成，不发成功；True仅发一次成功。
- owner卡在CPU模拟慢event import时，两套原getter仍立即返回，不触碰真实Future；全部事件后端调用记录只来自owner线程。
- 两套getter的True、False、error_block_ids、failed_request_ids、store完成、finished_sending及pop路径均检查。异常仍由原getter传播，不伪装成False或成功。
- unhealthy的两套路径清表前撤销发布资格；后续真实Future晚到True也不发成功，原drop一次性报告仍保留。
- 身份缺失、重复／旧代次、非owner查询及提交异常均有明确拒绝或回收路径。原getter AST在去掉新增invalidation语句后与归档完全一致。
- 真实提交入口抛异常时，不能证明没有部分设备操作已发出：操作、producer event与CPU代理被保留到异常持有表；代理抛原异常，不发成功，不宣告shutdown安全，也不允许该request注册新代次。
- unhealthy撤销旧代发布并清adapter表后，只要旧代仍在观察表或异常持有表，仍拒绝该request新代次。旧代真实完成且没有异常持有项后，新代次才可注册；单纯cancel不会解除设备生命周期约束。

## 生命周期与未验证边界

`close(timeout)`撤销发布资格并作有界线程等待。**超时不代表设备操作被取消或终结**：未完成操作继续由owner保留并观察；`close`返回False，不清除真实Future、producer event或代理引用。异常的真实Future也单独保留，因为event import/query异常不能证明设备已安全结束。

`submit_retrieve`抛异常但没有返回Future时，同样按“设备操作状态未知”保留操作和producer event，不把预留操作直接丢弃。同request旧代在pending或异常持有表时，禁止任何新代次；只有真实完成并从pending退出且没有异常持有项，才允许新恢复。异常持有项目前没有安全reap接口，因此会继续拒绝该request新代和adapter安全shutdown。跨request复用同一KV block的生命周期保护仍依赖尚未接入的上游所有权协议，本目录没有证明其安全。

adapter新shutdown保护在原unregister和`transfer_ctx.close()`前检查观察器是否安全终结；有pending、异常保留或仍运行owner时，会拒绝继续原shutdown。**这是故意暴露的真实集成缺口，不是已解决的NPU退出协议。** 后续必须给出安全unregister/reap路径，才能处理永不完成操作、设备错误及其资源释放；本目录没有伪造这个保证。

还未接通的部分：

- 成功outbox的`drain_success()`还没接到`control_sidechannel`、核心与scheduler；它返回带真实恢复身份的成功事实，不自动完成整请求。也没有建立batch ID映射或阶段TP全员聚合。
- False／异常仍依赖原完成与失败回报；新阶段控制面的FAILED/RETRY消息协议未接。submit时unhealthy直接drop的原路径没有真实Future，不能制造成功代理或READY；阶段控制面如何停止等待仍须上游失败事件接线。
- 现场真实restore generation、PP/TP/PCP身份、NPU线程context、事件IPC、设备生命周期及safe reap均为`NOT_VERIFIED`。
- 未运行真实模型、HCCL、采样/MTP、Mooncake、OFF/ON压测；不说明KV已在真实设备可用，也不说明有性能收益。

这是一份可审查的CPU协议候选，尚不足以部署模型或正式benchmark。
