# 两个模型批在途时，核心仍能处理事件

2026-10-27。**一个定向CPU案例通过；NOT_RUNTIME_INTEGRATED。** 本目录仅新增验证脚本、回执及说明，没有生产功能改动，没有SSH、用卡或重跑其他测试套件。

本案例直接AST执行当前`draft_wait_followup/core.py`中的真实`EngineCore.step_with_batch_queue`与唯一结果读取者，以及同目录`multiproc_executor.py`中的真实`FutureWrapper`。scheduler、grammar、模型结果及独立事件是明确的CPU替身，不代表真实PP2模型。

第一次进入真实step时，队列已有B1，长度1、容量2，满足原`assert len(batch_queue) < batch_queue_size`。随后真实方法走生成式、非pooling分支：

1. scheduler生成B2，execute2／sample2分别创建真实FutureWrapper。
2. 真实append将队列填到2，再pop B1进入等待。
3. B1旧结果仍被CPU门闩挡住时，核心处理独立事件；此时queue中仍有B2，`1个排队批 + 1个active model wait = 2`，没有scheduler结果更新，B2也未完成。
4. 只放行B1，第一次step返回并更新B1；检查B2仍pending、仍在队列，B2独立门闩尚未放行。
5. 下一次step等待B2，处理第二个事件后才单独放行B2，并更新B2。

所有真实get_response调用均来自同一个结果读取线程，FIFO严格为`execute1 → sample1 → execute2 → sample2`；scheduler输出更新顺序为`B1 → B2`。事件callback和scheduler更新均在核心线程。两条CPU门闩彼此独立，未把B1完成当作B2完成。

这证明当前真实方法的双在途等待接缝可以消费事件，同时在callback处保留满额计数。没有证明scheduler已具备新的阶段admission规则，也没有运行LMCache、GLM、MTP或任何NPU任务；没有性能结论。

机器：本地Windows；容器：无；工作目录：`E:\work\ks`。单命令复现：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\dual_inflight_probe\probe_dual_inflight.py"
```

回执为`cpu_result.json`，含两次callback的名额快照、完整22步顺序、各真实get_response调用线程及FIFO顺序。测试线程全部退出。本轮没有服务启动或参数切换，也没有发现需要修改旧候选的生产问题。
