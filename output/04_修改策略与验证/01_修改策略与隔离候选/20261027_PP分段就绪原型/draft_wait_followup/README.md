# Draft token等待：核心事件消费的窄后续候选

2026-10-27。**状态：真实AST红→绿，8项定向CPU检查通过；NOT_RUNTIME_INTEGRATED。** 仅新增本目录文件，没有SSH、用卡、部署或修改已稳定的core／restore候选。

这是已有核心事件pump的一个遗漏等待点：原`post_step`及structured-output deferred分支仍同步调用`take_draft_token_ids()`。该RPC虽然只取末段output rank的draft数据，仍走旧全局Future FIFO；如果前面有在途模型结果，核心会停在同步RPC里，期间不消费独立事件。

## 基础与实际改动

本候选以`../core_event_pump/core.py`及`../restore_event_binding/multiproc_executor.py`为输入，不以原始未修改框架为直接输入。`draft_wait_followup.patch`应应用在这两个先决候选已经存在的隔离源码上；构建回执记录精确输入路径。

只有以下改动：

1. `MultiprocExecutor`新增`take_draft_token_ids_async()`，调用真实`collective_rpc("take_draft_token_ids", unique_reply_rank=self.output_rank, non_block=True)`。原`take_draft_token_ids()`及`collective_rpc`的AST完全保持。
2. 核心的两个真实draft调用处改走`_ks_take_draft_token_ids()`。配置pump后，先在**核心线程**调用异步入口完成RPC广播，再把返回Future交给已有唯一结果读取线程；没有把collective_rpc／广播MQ写入搬到结果线程。
3. 将已有等待循环窄提取为`_ks_wait_executor_future(..., model_batch=...)`，模型等待继续走原包装入口。draft等待使用独立`_ks_waiting_draft_future`和`ks_draft_wait_pending()`，不设置或清除model等待标记、不新增整批admission、不释放模型批名额。
4. 独立事件callback／dispatcher仍只在核心线程运行。draft没有真实返回前，原`update_draft_token_ids`或`update_draft_token_ids_in_output`不会执行；deferred分支中已经真正完成的前一批模型输出仍按原先顺序更新。

未配置核心pump时仍调用原同步draft方法。配置pump后，未知执行器如果没有明确的异步draft接口，会在提交前拒绝，不能声称已覆盖；错误核心线程也在广播前拒绝。

没有改变MTP计算、MTP3参数、scheduler token／block数据、正式用例、输出长度或模型拓扑。模型批所有权仍由原批次／StageController生命周期维护，draft RPC本身不会创造或释放模型名额。

## 本机CPU复现

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。本轮没有服务启动或参数切换。

构建隔离副本及LF补丁：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\draft_wait_followup\build_draft_wait_followup.py"
```

原同步draft等待的有限时间红案例：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\draft_wait_followup\test_draft_wait_followup.py" --baseline-red
```

定向绿色检查：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\draft_wait_followup\test_draft_wait_followup.py"
```

实际结果：**8项通过**，包括红记录核对及7项候选检查。回执为`red_result.json / cpu_result.json / build_result.json`。没有重跑其他旧套件。

红案例直接执行已有核心候选的真实`post_step`与真实同步draft／collective_rpc／FutureWrapper：旧模型Future仍被CPU门闩挡住时，独立事件没有被处理，draft更新次数为0；随后主动放行并回收线程。

绿色检查直接AST执行新`post_step`、真实`step_with_batch_queue`的deferred分支、原／新draft入口、collective_rpc和FutureWrapper。确认：

- draft未返回时，核心已经处理独立事件，draft信息尚未更新；draft_wait=1、model_wait=0。
- RPC广播发生于核心线程，旧结果由唯一reader读取，原FIFO及output rank保留；没有抢读第二份回复队列。
- deferred分支只在draft真正返回后更新新批draft信息并保留该批队列项；前一批实际完成后的模型更新顺序不变。
- 默认关闭、未知异步接口拒绝、错线程提交前拒绝及RPC异常传播均覆盖；原draft与RPC方法AST保持。

测试的CPU回复／门闩是明确替身，没有执行MTP或真实设备。真实draft payload、现场聚合线程亲和性、MTP正确性、完整阶段调度和性能收益均为`NOT_VERIFIED`。该候选可用于审查遗漏等待点，不能作为模型已集成或加速的证据。
