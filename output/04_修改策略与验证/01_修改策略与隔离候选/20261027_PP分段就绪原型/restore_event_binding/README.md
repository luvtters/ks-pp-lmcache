# 恢复完成身份接入控制队列：CPU隔离候选

2026-10-27。状态：**NOT_RUNTIME_INTEGRATED**。本目录只新增隔离文件，未连接服务器、未占卡、未改安装源码，也未修改其他原型目录。

这一增量把已经由唯一观察者确认的恢复成功身份，接入现有独立控制队列，再由核心线程登记到StageController。登记一个worker完成，不等于整个请求就绪，更不等于模型批完成。

## 实际接线

1. `publish_observer_success(observer, publisher)`显式调用真实观察者的`drain_success()`，不查询或同步设备Future，不新建桥接线程。
2. `publisher.publish_restore_ready(identity)`核对global worker rank、PP阶段、TP rank、PCP rank，然后产生独立的`KSRestoreEvent`。它与旧`KSStageEvent`共用发布锁、worker队列和连续sequence。
3. `KSRestoreEvent`字段为`event_id/request_id/restore_generation/stage/worker_id/tp_rank/pcp_rank/sequence`，没有batch_id。请求身份没有冒充逻辑批身份；request_id只要求非空字符串，不增加上游没有的长度限制。独立event_id不包含请求ID。
4. 真实`poll_stage_events()`同时接受两类事件；旧CONTROL_RESULT/CONTROL_ERROR仍按command ID、batch ID、generation及目标worker集合关联。
5. 新核心的`configure_ks_core_event_pump(callback)`收到事件后，在核心线程调用`RestoreEventBinding`；它只调用`StageController.mark_kv_ready()`。必须提前由独立调用者执行`begin_restore()`，未知、旧代或已失效代次均不会新建恢复记录或放行。
6. 恢复事件可以先登记，稍后才`admit_batch()`；后段仍须自己的KV及前段中间结果就绪。绑定不会自动提交模型结果，不释放整批名额。

当前明确支持单机PP2、PCP1、连续全局worker布局；默认每段TP8。PCP>1直接拒绝，不能猜测真实rank映射。旧阶段事件可交给`on_stage_event`回调继续处理。

## 最简CPU指令

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。本轮没有服务启动或参数切换。

构建执行器新副本和相对control_sidechannel的增量LF diff。补丁目标为vLLM工作树内的`vllm/v1/executor/multiproc_executor.py`，必须基于已套用`control_sidechannel`的隔离运行源码应用；不能直接套到未含该候选的原版文件：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\restore_event_binding\build_restore_event_binding.py"
```

只运行本增量CPU验收，不重跑已有14/15/66项：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\restore_event_binding\test_restore_event_binding.py"
```

构建器仅写当前目录的`multiproc_executor.py`、`restore_event_binding.patch`和`build_result.json`；测试写`cpu_result.json`。首次实现前的缺失检查保存在`red_result.json`。

## CPU证据范围

- 真实候选执行器文件中AST抽取的publisher和poll，消息经过真实`multiprocessing.Queue`及真实转发线程；没有另造一套poll实现。
- 实际观察者收到真实归档MessagingFuture的CPU完成输入后产生success_outbox，经显式桥接进入队列。该输入是CPU fixture，不是实际NPU/KV恢复。
- 与`core_event_pump/core.py`中的实际EngineCore方法及真实FutureWrapper组合：旧模型结果仍未完成时，核心已经消费恢复事件、登记单worker就绪；`ks_model_wait_inflight()==1`，旧Future仍被核心强持有，最终模型结果未commit。释放CPU模型门闩后才提交最终结果。
- 校验共享sequence及旧CONTROL关联、伪造rank/stage/TP/PCP拒绝、未知请求不创建记录、旧代不登记、重复不重复计数、恢复先于逻辑批、旧模型返回及控制处理方法不变。

## 未接通的部分

真实restore generation和PP/TP/PCP/global rank仍未从运行metadata注入；测试使用显式合成身份。真实LMCache设备Future→本观察者→本桥接→现场核心尚未整条连接。此次没有GLM、实际KV恢复、完整阶段调度、Mooncake消费、安全错误回收或性能对照，不能把CPU通过称为runtime集成或加速。

桥接一旦发布失败必须向上报错，不能当作成功交付。观察者已drain的身份不会由本函数自动重试；关闭队列仍沿用旧丢弃式关闭边界，不能据此宣称消息已drained。取消、重试代次和设备资源安全回收继续由原有独立规则负责。
