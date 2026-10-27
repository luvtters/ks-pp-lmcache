# Ascend IPC 事件与唯一 owner 线程组件诊断

此目录只准备一个小组件诊断：同一台机器上两个 `spawn` 子进程，共用显式指定的一张当前空闲 NPU。producer 做 64 元素加法、记录真实 IPC 事件，将非空 `(handle_bytes, True)` 通过 CPU Pipe 送到 consumer；consumer 使用已安装 LMCache 的原始 `MessagingFuture`、`DeviceMessagingFuture` 与原型 `LMCacheStageObserver`，在唯一 owner 线程的 `torch.npu.device(device)` 上下文内导入、查询并取得结果。另一个原始 `MessagingFuture` 作为 CPU proxy，诊断核对一次成功身份，随后回 ACK。producer 收到安全 ACK 后才校验张量内容并退出。

这不是 GLM、MPserver、LMCache KV 恢复、PP2 整体集成或性能测试。identity 是组件诊断专用的合成身份，不是模型请求的真实身份。首次 query 若已经为 True，仍能验证真实 IPC 通路，但不证明观测到了 pending 到 complete 的过程。

## 文件与依赖

- `lmcache_event_ipc_probe.py`：单入口，只使用现有环境，无安装步骤。
- `cpu_check.json`：2026-10-27 本机语法和带导入拦截的 CPU-plan 检查结果。未导入 torch、torch_npu 或 lmcache，未起进程或用卡。
- `stage_observer.py`：部署时将现有原型原样放入脚本同目录；也可用 `--observer-path` 指定原型文件。原型来源：`output/04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/lmcache_stage_observer/stage_observer.py`。

真实设备路径依赖已有 `torch`、`torch_npu`、LMCache 0.5.6rc5 及原环境中的 Ascend 适配。脚本使用 `lmcache.v1.platform.base.event_ipc.get_event_ipc_backend`、`lmcache.v1.multiprocess.futures.MessagingFuture` 和 `DeviceMessagingFuture`；不启用服务、不修改安装包。实际依赖版本、来源路径、Future 构造签名会写入回执。

## 最简命令

目标机器：25；容器：本项目私有 `ks-luv-m25-stage-privcheck`；工作目录：`/opt/ks-stage-ready/device-probe`。设备 15 只是本轮明确指定的组件设备，主任务执行前须实时核对空闲状态。此文档没有运行设备诊断。

```bash
/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready/device-probe/lmcache_event_ipc_probe.py --cpu-plan --device 15 --output-dir /opt/ks-stage-ready/device-probe/event-ipc-component-20261027
```

在已授权的时间内运行真实组件；使用一个尚不存在的私有输出子目录：

```bash
/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready/device-probe/lmcache_event_ipc_probe.py --run-device-probe --device 15 --output-dir /opt/ks-stage-ready/device-probe/event-ipc-component-20261027 --timeout 90
```

`--cpu-plan` 只输出计划，不导入设备库、不访问设备、不分配 NPU、不创建子进程、不创建设备输出目录。`--run-device-probe` 的输出位置限制在 `/opt/ks-stage-ready/device-probe/` 下，不接受其根目录、上级路径或覆盖已有目录。

## 回执与安全边界

输出目录包含 `result.json`、`plan.json`、`producer.jsonl`、`consumer.jsonl`。记录两个 PID、owner 线程与当前设备、handle 长度、CPU payload 返回时刻、真实 import/query/result/synchronize 方法调用和返回时刻、proxy 结果、成功身份数量、张量校验与错误。不会输出 handle 原文；安装版 NPU backend 已知会在 import 失败的 warning 中打印 handle 前缀，故仅抑制该模块的日志，不替换 backend 或 Future。

- `PASS_COMPONENT_ONLY`：两进程均 PASS、真实 owner 方法审计通过、proxy 为 True、成功身份恰好一次、无重复事件、consumer 安全退出并 ACK、producer 事件和张量校验通过。只证明上述组件。
- `NOT_SUPPORTED`（退出码 3）：真实 `backend.check_event_support` 拒绝支持；不替换为 dummy/stub，也不制造成功。
- `FAIL`（退出码 2）：其他错误；回执单列是否确认设备安全完成。
- `UNSAFE_TIMEOUT`（退出码 4）：真实设备调用不返回、ACK 超时或发生无法证明安全完成的错误。回执列出 `retained_live_pids`；脚本不强杀子进程。原事件、backend、真实 Future 等引用继续留在相应子进程，父诊断进程在记录后退出，避免 Python 自动 join 无限等待。外层任务必须将这些 PID 作为未安全完成的失败诊断处理，不能把强杀记为设备完成或正常验收。

正常协议有 90 秒期限，父进程另留 10 秒并对两个子进程有界 join。设备驱动内部的同步调用不能靠 Python 超时保证返回，因此保留上述明确的 unsafe 边界；没有实现额外的设备恢复框架。

本目录本机检查截至交付时仅覆盖 Python 语法和 CPU-plan。**未在本子任务运行真实 NPU，设备通路为 NOT_VERIFIED。** 主任务实际运行结果须以其取回的 `result.json` 为准。
