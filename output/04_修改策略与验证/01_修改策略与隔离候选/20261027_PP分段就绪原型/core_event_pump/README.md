# 核心等待模型结果时消费独立阶段事件

2026-10-27。**状态：真实方法CPU红→绿，10项定向检查通过；NOT_RUNTIME_INTEGRATED。** 只新增本目录文件，没有SSH、用卡、部署或修改其他原型目录。

本候选补一个等待接缝：独立阶段队列即使有消息，原`EngineCore.step_with_batch_queue`停在`future.result()`时也不会主动消费。新版本把这一处旧结果读取交给唯一读取线程，核心线程继续处理独立事件；真正模型结果终结后，才沿原路径更新scheduler。

## 针对的真实代码

输入为25环境已归档源码：

- `output/05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/engine/core.py`，原`step`在443行、`step_with_batch_queue`在484行，等待分别位于461与560行。
- 同快照`executor/multiproc_executor.py`的真实`FutureWrapper`；没有修改它的FIFO逻辑。

`build_core_event_pump.py`根据唯一边界生成本目录`core.py`与LF格式`core_event_pump.patch`。输入文件与运行仓库均不覆盖。

真实改动限于`EngineCore`的两个step方法等待处、普通step中的采样等待，以及shutdown中已有执行器shutdown之后的结果读取线程回收。**没有改scheduler的token、block、NewRequestData或finished_recv处理。**

## 消费和读取各由谁执行

显式调用`configure_ks_core_event_pump(callback, dispatcher=None, poll_interval=0.005)`后：

1. `_KSModelResultReader`为该核心持有唯一结果读取线程；同一时间只允许一份未完成旧Future提交给它。原`FutureWrapper.result()`仍按原FIFO取得更早的execute、sample及普通回复，不抢读第二份旧MQ。
2. 核心的`_ks_wait_model_future`非阻塞轮询独立`model_executor.poll_stage_events()`，在**核心线程**调用`callback(event) -> None`；可选dispatcher也在这个线程运行。事件字段不在pump中解释，恢复代次不能在这里伪造成逻辑batch身份。
3. callback仅可登记StageController状态／时序；dispatcher仅可提交独立阶段控制命令。禁止新增整批admission、直接修改scheduler、普通同步RPC或读取旧模型Future。代码会拒绝从callback递归调用step或模型等待入口。
4. callback异常不会宣布模型batch结束。候选保留真实模型等待，待唯一读取者获得终态后再传播异常，不执行`update_from_output`。真实模型异常仍按原异常传播。
5. 相同Future再次经过原错误检查入口时，读取CPU中继结果，不第二次调用该旧`FutureWrapper.result()`；完成结果采用弱键缓存，避免结果读取线程永久持有历史批次。

默认不开启：只有候选源码中`KS_CORE_STAGE_EVENT_PUMP=1`并且显式配置callback，才创建结果线程；单独设置环境变量不会自动接线。未配置时仍直接执行原`future.result()`及同步采样。**这不是已经部署的vLLM启动开关。**

## 模型批的名额没有因pop而释放

原方法在等待前已经`batch_queue.pop()`。新callback不能用此时队列长度判断“整批有空位”。等待期间：

- `core._ks_waiting_model_future`强持有这份模型Future。
- `core.ks_model_wait_inflight()`明确返回1，直到真正模型结果结束或异常传播完成。
- 原局部`scheduler_output`和真实逻辑批所有权继续保持，`update_from_output`尚未调用。
- StageController的全局inflight名额由其原完成／提交协议维护，pump不取消、提交或释放它。

该标记是显式接线接口，不会自动修改已有scheduler或替代完整批次生命周期。新阶段提前forward仍未接入。

## 本机CPU复现

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。本轮没有服务启动、参数切换或模型推理。

原真实路径的有限时间红案例：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\core_event_pump\test_core_event_pump.py" --baseline-red
```

先执行归档真实`EngineCore.step_with_batch_queue`与`FutureWrapper`，确认旧结果仍pending时核心没有消费独立消息；测试随后主动放行门闩并回收线程。记录为`red_result.json`。它证明等待接缝，不证明真实设备死锁。

构建隔离候选：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\core_event_pump\build_core_event_pump.py"
```

执行绿色定向检查：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\core_event_pump\test_core_event_pump.py"
```

本轮10项通过，包括红记录核对与9项候选执行检查；回执为`cpu_result.json`。没有重跑control14项、observer15项或原66项。

测试直接AST执行真实原／新`EngineCore.step / step_with_batch_queue`及归档`FutureWrapper`。候选case证明：旧Future仍被CPU门闩挡住时，真实新核心方法已经调用poll与callback；callback仍在核心线程、scheduler update次数为0、waiting inflight为1，真实StageController原型整批名额仍为1。模型放行后才出现最终更新。还检查了原FIFO、采样、同Future错误入口去重、默认关闭、递归拒绝、callback／模型异常及dispatcher线程归属。不是另外实现一套独立CPU scheduler来证明自己。

## 还未验证

- 该目录自身的事件输入是CPU队列，尚未在本目录完成真实侧信道及restore envelope的组合；异构事件身份绑定由独立接线候选处理，pump不会把request_id当batch_id。
- 真实`FutureWrapper.get_response/aggregate`换到结果线程后的线程亲和性、connector聚合、模型输出及异常shutdown仍需真实版本验证。CPU通过不能替代这一项。
- 没有自动在真实EngineCore启动中配置callback，没有将真实LMCache完成事件绑定到controller，也没有接入新阶段forward派发、scheduler资格、TP全员完成或完整采样／Mooncake资源生命周期。
- poll间隔会影响控制消息处理延迟；未测量开销或性能，不声称等待时间减少，更不声称TTFT／TPOT改善。
- 未运行GLM、NPU/HCCL、正式OFF/ON benchmark或模型正确性验收。

因此该候选只把“核心阻塞在旧结果时仍可处理独立事件”做成可审查、可CPU复现的真实方法修改，不代表新阶段调度已runtime集成。
