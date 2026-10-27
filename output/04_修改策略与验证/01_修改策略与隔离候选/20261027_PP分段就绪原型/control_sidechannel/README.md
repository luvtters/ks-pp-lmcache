# PP阶段控制侧信道：真实multiproc源码的隔离候选

2026-10-27。状态：**CPU候选通过；NOT_RUNTIME_INTEGRATED**。本目录仅新增隔离文件，没有连接服务器、占卡、启停服务、覆盖运行仓库或修改现有controller、executor_bridge、worker_split。

这版针对一个已确认缺口：旧`FutureWrapper.result()`必须先读全局FIFO中更早的模型结果，不能拿来承载独立阶段通知。这里为每个本地worker增加独立上行队列；事件不会进入旧`response_mqs`或`futures_queue`。**核心等待旧模型结果时，侧信道本身已经可以被另一个明确的消费入口读取；现有核心尚未改为调用这个入口。**

## 源码依据与实际补丁

输入为25环境已归档vLLM 0.23源码：

`output/05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/executor/multiproc_executor.py`。

本目录`build_control_sidechannel.py`按唯一文本边界构建完整隔离副本`multiproc_executor.py`与`control_sidechannel.patch`。任何边界不唯一即拒绝构建，不修改输入文件。候选以现有单机TP8/PP2为目标，拒绝跨节点模式；默认关闭，只有候选源码中的`KS_PP_STAGE_EVENTS=1`会创建队列和发布线程。**这个环境变量不是已部署的服务开关。**

真实修改路径如下：

1. `MultiprocExecutor._init_executor`为每worker创建一对有界`multiprocessing.Queue`与失败标记，并沿真实`make_worker_process → process_kwargs → worker_main → WorkerProc.__init__`传入。没有新增依赖。
2. 每worker的`_KSStageEventPublisher`用一条专用线程写自己的上行队列。调用方只向本进程有界队列放置已知事实；该线程只做队列转发，**没有模型、TP/HCCL、设备事件或LMCache Future查询**。不抢读旧模型回复。
3. `submit_stage_command`在现有单写者广播通路中提交`KSStageCommand`，不创建旧`FutureWrapper`。真实`worker_busy_loop`识别新envelope，只让目标PP阶段执行控制方法，结果或异常只发到侧信道；非目标worker直接继续，不形成空回复门槛。
4. `poll_stage_events`直接非阻塞读取独立事件，检查worker、阶段、连续序号、事件ID、批次与代次。控制命令仅等待目标阶段真正需要的worker集合；拒绝未知或重复控制结果、错代次及跨worker回复。原普通RPC、execute/sample和最终模型结果仍用原通路。
5. 队列满或转发失败会设置父进程可见失败标记，消费入口报错，不能凭缺失通知放行。关闭只做有界线程等待及`cancel_join_thread()/close()`，不等待无人读取的multiprocessing feeder。这是丢弃式退出，不保证已publish的事件全部送达；CPU关闭检查只证明不被feeder拖住，不等于已drained。

每个事件带`event_id / batch_id / generation / stage / worker_id / sequence / kind / value`。`CONTROL_RESULT`只是控制方法结果；`CONTROL_ERROR`带异常文本。`KV_DEVICE_READY / INPUT_DEVICE_READY / STAGE_DEVICE_DONE`只是未来事件生产接口，**没有在此处制造真实设备完成事实**。传输层返回的独立设备事件仍需未来控制器按当前恢复代次判定有效性，不能直接释放KV或提交整批。

控制命令只开放现有worker原型中的`input_received / input_ready / model_ready / release_ready / release`，返回类型限`bool/None`。**没有将execute、sample、prepare或TP后处理搬到新线程；没有把最终模型输出混入阶段控制结果。** 原worker副本的这些控制方法也没有在现场自动启用。

## 本机CPU复现

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。本轮没有服务启动或参数切换。

构建隔离副本与diff：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\control_sidechannel\build_control_sidechannel.py"
```

执行本目录定向CPU验收，不重跑原66项：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\control_sidechannel\test_control_sidechannel.py"
```

结果位于`build_result.json`和`cpu_result.json`。本轮定向验收**14项通过**：

- 直接从候选真实文件AST执行`FutureWrapper / collective_rpc / worker_busy_loop / 新command处理 / 新publish与poll`。并非另外实现一个自证的CPU执行器模型。
- 真实spawn子进程发布CPU就绪事实；旧模型Future仍被CPU门闩挡住时，父进程已经通过真实新poll消费事件。该事件不是NPU或LMCache产生。
- 真实worker主循环处理16份输入，只有目标后段8worker调用控制方法并回包；普通RPC和最终模型输出没有控制回复插入，原顺序不变。
- 控制异常只发侧信道；关闭、默认禁用、错代次、非法ID、重复命令、单消费方及队列故障均作定向检查。专用写线程不查询Future或调用设备接口。
- 真实spawn进程对从未读取的multiprocessing队列发布64份消息后退出，确认关闭不会等待未消费feeder。测试进程与线程均退出。
- 构建器与测试用AST相等性检查，确认原`FutureWrapper / collective_rpc / enqueue_output / handle_output / async_output_busy_loop / execute_model / sample_tokens`逻辑未改变。

完整vLLM启动沿途的参数转发只做AST结构检查，**没有在CPU上初始化真实vLLM、torch、设备组或模型**。本机测试使用标准库队列承载CPU输入与回复；独立上行有真实`multiprocessing.Queue`和spawn进程检查。

## 没有闭合的链路

1. **核心还没接新poll。** 原`FutureWrapper.result()`保持不变；若未来核心还是同步停在该函数内而无人消费侧信道，仍不会自动前进。需要明确接入核心事件循环或一个只收集事件、由核心处理状态的通道，不能开线程抢读旧回复MQ。
2. **没有真实LMCache阶段生产者。** 后续必须从现有唯一完成消费路径获得带恢复代次的本段设备完成事实，再向发布接口转发；不能另开线程同时查询同一个Future，不能拿RPC ACK或Python返回当设备完成。
3. **stage-command仍走原worker串行主循环。** 它能使已完成的控制回复独立消费，不能让排在阻塞模型调用之后的控制方法提前执行。真实设备事实由未来独立生产接口发布后，才可能在模型RPC未结束时到达。CPU子进程案例明确注入了这样的已知事实。
4. 未接真实scheduler、批次名额、采样/MTP、Mooncake远端消费及最终提交；传输侧信道不等于完成新阶段调度。也未改变已有整批资源所有权。
5. 未验证实际容器副本应用、MessageQueue广播握手、真实TP8/PP2进程、LMCache、NPU/HCCL、图执行、模型正确性或性能；没有新的OFF/ON结果与提速结论。

因此本目录是**可审查的最小传输候选与CPU证明**，不能单独替换运行文件后用于正式benchmark。
