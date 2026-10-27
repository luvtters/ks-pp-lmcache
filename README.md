# PP＋LMCache 分段就绪：隔离候选归档

个人私有仓库，分支 `ks/pp-stage-ready-luv-20261027`。提交的是当前修改、关联候选、测试入口、精确源码基线与验证记录；**没有启用这些修改，没有运行 GLM 模型或新 benchmark**。

## 从哪里开始看

- [统一修改讲解（Markdown）](output/04_修改策略与验证/01_修改策略与隔离候选/20261026_PP分段就绪设计/PP分段就绪调度_源码依据与修改设计.md)
- [统一修改讲解（独立 HTML，下载后离线打开）](output/04_修改策略与验证/01_修改策略与隔离候选/20261026_PP分段就绪设计/PP分段就绪调度_源码依据与修改设计.html)
- [完整候选目录](output/04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/)
- [设备验证汇总](output/05_当前环境与实验准备/20261027_25环境准备/设备协议诊断/设备诊断汇总.json)

## 哪些是当前候选，哪些是先前阶段

| 目录／文件 | 作用及状态 |
|---|---|
| `draft_wait_followup/core.py`、`multiproc_executor.py` | 当前累计的核心／执行器候选，包含独立恢复事件、等待期间事件消费、异步草稿等待；已核对与25隔离工作区一致，仅归一化换行格式作比较，逐文件结果见 source_status.json |
| `lmcache_stage_observer/` | LMCache 真实设备 Future 的唯一观察者、CPU 代理、adapter 对接；隔离候选 |
| `restore_event_binding/` | 恢复身份事件与控制器绑定；其中 executor 是 draft 累计版本之前的阶段，不应倒序覆盖最终候选 |
| `controller/`、`worker_split/` | 阶段资格与 Ascend 接收／计算拆分原型；真实 scheduler／GLM 全链未集成 |
| `control_sidechannel/`、`core_event_pump/` | 已保留的先决增量及测试，用来审查每一步差异 |
| `integration_gaps/`、`executor_bridge.py` | 早期原型与已发现的旧 FIFO 限制，保留作来源记录；不是最终通道实现 |
| `dual_inflight_probe/`、`real_reply_queue_probe/` | 双在途及实际回复队列诊断，后者需要已有 Linux 无NPU容器环境 |
| `recorded_environment/vllm_actual_worktree.patch` | 从25实际隔离分支只读取得的累计补丁，相对真实基础提交；没有反向修改服务器 |
| `recorded_environment/actual_vllm/` | 从25只读导出的两份实际文件，按原始字节保存，便于区分Windows换行与服务器换行 |

**不能把所有子目录的补丁依次盲目应用。**`draft_wait_followup` 是累计候选文件，但其 patch 是相对先决候选的增量。需要从原始 vLLM 基线查看累计变化时，用 `recorded_environment/vllm_actual_worktree.patch`；LMCache／worker 相关改动分别在各候选目录，未自动组成可启动整链。

## 验证结论

- 原型66项及独立通道14、观察器15、核心10、恢复绑定13、草稿8项定向CPU检查已有归档。
- 两个在途批的CPU场景验证原顺序与等待名额保留；不是多负载压力结论。
- 16卡小张量 H2D／HCCL／事件与阶段依赖通过；真实NPU IPC事件可在模型结果前到达核心方法并登记就绪。
- 真实恢复代次／rank元数据、正常EngineCore启动、真实scheduler阶段派发、GLM／MTP／Mooncake完整生命周期仍未接通。
- **完整模型正确性、TTFT／TPOT／吞吐与提速均为 NOT_VERIFIED。**已有诊断回执是历史记录，不因提交代码变成新实测。

## 本地CPU复核

机器：本地 Windows 或普通CPU环境；无需服务器、模型、torch或NPU。先进入候选目录：

```powershell
cd "output/04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"
$env:PYTHONPATH = Join-Path (Get-Location) "controller"
python -X utf8 -B -m unittest controller.test_stage_controller worker_split.test_worker_split test_executor_bridge test_composed_path
python -X utf8 -B -m unittest control_sidechannel.test_control_sidechannel lmcache_stage_observer.test_lmcache_stage_observer core_event_pump.test_core_event_pump restore_event_binding.test_restore_event_binding draft_wait_followup.test_draft_wait_followup
python -X utf8 -B dual_inflight_probe/probe_dual_inflight.py
```

这些命令只复核候选逻辑。最后一条会刷新该候选的回执；不等于执行模型或性能测试。`real_reply_queue_probe` 不适合普通Windows：它要求既有Linux无NPU容器、vLLM0.23.0+empty及已安装依赖。设备诊断代码只为完整归档，不授权任何人按旧命令用卡。

## 保留的来源

vLLM基础：`0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`；vLLM-Ascend基础：`5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`。LMCache adapter／future来自项目既有归档，构建记录未确认Git提交，不能冒称所有文件来自同一份快照。

`output/` 原相对层级保留，以满足构建器／CPU测试的来源定位。只带必要的旧源码文件，不包含完整历史工作区、客户数据、模型、venv、凭证、聊天原图或清退进程脚本。源码原有版权／SPDX头保持，附带可获得的上游许可证。

本仓库是候选归档，**没有已验证的 `vllm serve` 新功能开关或一键部署入口**。提交和推送不会启用服务器修改。
