# TP8/PP2 阶段设备依赖诊断

这是一轮合成小张量协议与策略验证：rank0 执行已有 CPU `StageController` 原型，通过原有 world Gloo 组收齐真实 NPU 事件报告；16 个 rank 的主线程执行 HCCL PP 发送／接收和后段 TP 重构。它没有接入 vLLM EngineCore，没有运行 GLM、真实 LMCache KV 恢复或正式 benchmark，不输出提速结论。

旧策略对照使用现场归档 `scheduler.py:2188—2219` 的 `_try_promote_blocked_waiting_request` 完整函数，按 AST 原样提取到 `legacy_scheduler_gate_excerpt.py`。没有重写其 `WAITING_FOR_REMOTE_KVS` / `finished_recving_kv_req_ids` 判断。CPU fixture 只提供 RequestStatus、请求字段和越过 gate 后的记录回调，不能将该回调解释为真实 scheduler KV 记账执行；来源路径、行号见 `legacy_gate_source.json`。

## 一轮实际验证的顺序

1. 前段 8 rank 各做 32 float 的 CPU→NPU 小张量传输并确认事件完成；后段 KV 恢复尚未提交，受明确的 CPU 门闩控制。
2. rank0 对同一条件断言：真实旧 scheduler 函数返回 False，全局 finished 集合仍空；新 StageController 可以派前段。若两者无法形成这个区别，本轮直接失败。
3. 前段计算实际消费 H2D KV 值，完成后通过真实 `isend_tensor_dict` 发送；后段真实 `irecv_tensor_dict` 接收、等待 Work、另确认设备事件，再完成 TP 后处理及内容校验。Work 方法返回与设备事件完成单独记录，发送源保持引用。
4. activation 已真实完整到达，但后段 KV 仍未提交。rank0 断言后段仍无法派发，之后才广播开闩命令。
5. 后段实际 H2D 和事件完成后，rank0 才填入旧 scheduler 的全局 finished 标记，验证原函数此时 True；新控制器在 activation 与本段 KV 均真实完成后派后段。
6. 后段计算真实消费 activation 和 KV，核对输出 `arange(256).reshape(8,32)+5`。全体 rank 内容检查和设备完成后，rank0 进行一次有序 commit。

前段／后段 KV 分别为值 2／3 的合成张量。人工门闩只用来构造可证伪的依赖顺序，它的持续时间没有性能含义。新控制器只在 rank0 单线程调用；所有 rank 按相同编号的 Gloo collective 轮次推进，没有 Python 后台线程发 TP 或 HCCL。

## 部署和原生命令

将以下四个文件放到容器同一私有目录 `/opt/ks-stage-ready/device-probe/stage_overlap/`：

- `pp_stage_overlap_probe.py`
- `stage_controller.py`（现有原型原样复制）
- `legacy_scheduler_gate_excerpt.py`（真实 scheduler 函数原样提取）
- `legacy_gate_source.json`

机器25；本项目容器 `ks-luv-m25-stage-privcheck`；工作目录 `/opt/ks-stage-ready/device-probe/stage_overlap`。沿用此前已通过真实设备协议的 torchrun 与环境。没有模型服务启动或参数切换；这里的 TP8/PP2 只属于小张量诊断通信组。

不占卡的计划入口：

```bash
python /opt/ks-stage-ready/device-probe/stage_overlap/pp_stage_overlap_probe.py --cpu-plan --output-dir /opt/ks-stage-ready/device-probe/stage-overlap-20261027
```

真实设备入口必须由主任务在有效授权窗口内实时核实 16 卡归属后执行。环境与此前已通过的设备协议保持一致；只换本轮诊断入口与独立输出目录，保留 static IPv4 rendezvous：

```bash
export ASCEND_RT_VISIBLE_DEVICES=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
export HCCL_IF_IP=192.168.0.122
export GLOO_SOCKET_IFNAME=enp23s0f3
export TP_SOCKET_IFNAME=enp23s0f3
export HCCL_SOCKET_IFNAME=enp23s0f3
export HCCL_NPU_SOCKET_PORT_RANGE=auto
export OMP_NUM_THREADS=1
timeout --signal=TERM --kill-after=10s 240s torchrun --nnodes=1 --node-rank=0 --nproc-per-node=16 --rdzv-backend=static --master-addr=127.0.0.1 --master-port=29627 --max-restarts=0 /opt/ks-stage-ready/device-probe/stage_overlap/pp_stage_overlap_probe.py --run-device-probe --output-dir /opt/ks-stage-ready/device-probe/stage-overlap-20261027
```

通信组 timeout 为 90 秒，外层任务 deadline 为 240 秒。每 rank 即时写 `rank-XX.events.jsonl`，最终写 `rank-XX.json`。正常完成状态是 `PASS_SYNTHETIC_STAGE_DEPENDENCY_ONLY`，要核对 16 份结果全部通过。失败／外层中止不代表安全设备完成，也不算验收成功；不得把停止进程记为设备事件完成。

本子任务只做了本机语法与带导入拦截的 CPU-plan 检查，结果见 `cpu_check.json`；未导入 torch、torch_npu、lmcache、vllm，未 SSH 或用卡。真实运行结果为 **NOT_VERIFIED**，以主任务后续取回的 16 rank 回执为准。
