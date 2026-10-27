# 真实vLLM回复队列的CPU阻塞诊断

状态：**仅完成本机语法及源码API核查，尚未现场执行**。此组件不运行GLM、LMCache缓存服务或NPU任务，不是性能测试。

## 验证的具体接缝

之前CPU检查中的旧模型回复被CPU Event挡住，不能证明真实vLLM MessageQueue阻塞时是否仍允许核心线程前进。这份组件直接导入现场安装的`vllm.distributed.device_communicators.shm_broadcast.MessageQueue`，不重写或AST替代它。

执行顺序如下：

1. writer线程创建真实MessageQueue并导出handle；主线程创建reader。两边并行执行真实`wait_until_ready()`，主线程先消费一份bootstrap普通回复。
2. 主线程把同一个、已经使用过的reader对象明确移交给新核心的唯一结果reader。真实归档FutureWrapper的`get_response`直接调用该对象的`dequeue()`，模型回复此时尚未写入。
3. 等待真实队列的初始spin阶段过去，并进入一小段idle等待后，CPU通知线程向独立实际multiprocessing.Queue发布恢复事件。
4. 新核心的实际方法消费事件，真实RestoreEventBinding登记单worker就绪；callback必须同时证明旧模型Future未完成、scheduler update为0、model wait inflight为1、原Future仍被强持有。
5. 只有callback断言成立，writer才获准把16KiB的CPU模型回复fixture写入真实MQ；单chunk为4KiB，故模型回复经过真实ZMQ overflow路径。唯一reader读到回复后，核心才提交一次最终结果。
6. 唯一reader返回且核心不再持有在途等待后，主线程从同一个reader读取第三份普通回复，检查再次移交没有破坏此例的回复顺序。

ZMQ socket在不同线程间同步移交是否具有通用官方保证仍是**NOT_VERIFIED**；此处专门验证当前现场版本的具体用法，失败须如实保留，不能换一个新reader回避接缝。

## 文件清单

- `probe_real_reply_queue.py`：唯一任务入口。
- `real_reply_ast.py`：只依赖标准库，抽取真实候选FutureWrapper、独立publisher/poll、核心方法。
- `source_bundle/multiproc_executor.py`：最新restore_event_binding候选完整源副本。
- `source_bundle/core.py`：最新core_event_pump候选源副本。
- `source_bundle/stage_controller.py`、`source_bundle/restore_binding.py`：实际CPU控制器与绑定源副本。
- `source_manifest.json`：上述副本的本机来源路径；`README.md`说明边界。

脚本与源副本一起复制到本项目**无NPU设备的私有CPU容器**即可，不需要整个output或测试运行器，不安装新依赖。入口必须先确认Linux容器且不存在`/dev/davinci*`，不提供绕过开关。

## 原生CPU入口

建议容器私有路径：`/opt/ks-stage-ready/real_reply_queue_probe`。机器：25；容器：本项目无NPU的CPU容器；本任务没有服务启动或参数切换。

```bash
cd /opt/ks-stage-ready/real_reply_queue_probe
python -B probe_real_reply_queue.py --output-dir /opt/ks-stage-ready/real_reply_queue_probe/runtime_result
```

使用现有基础Python中的vLLM `0.23.0+empty`、torch、pyzmq及共享内存支持。若导入或版本不匹配，输出`FAIL_OR_DEPENDENCY_BLOCKED`，不安装或替换包。ZMQ IPC文件指向上述私有输出目录；没有挂载公共目录或改宿主配置。

本机只准备源副本：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\real_reply_queue_probe\probe_real_reply_queue.py" --prepare-only
```

现场结果写入`runtime_result/result.json`。只有实际队列传输、核心先处理事件和最终回复顺序全部成立才输出`PASS_ACTUAL_REPLY_QUEUE_CPU_ONLY`；实际MQ路径、版本、spinloop开关、线程与动作时间线一并保存。人为延迟、合成恢复身份和CPU模型回复均不是客户用例或设备恢复证据。

## 不能由结果证明的事项

即使通过，也仅证明这一个现场版本、一个local reader、一次bootstrap／overflow模型回复／普通尾回复的组件例子不会让核心被真实回复队列等待饿死。不证明所有vLLM聚合器都可换线程，不证明多worker或跨节点覆盖，不证明真实generation/rank传播、NPU设备context、GLM正确性或性能收益。
