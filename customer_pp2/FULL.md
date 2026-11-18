# 客户对齐今天蓝区完整实测版本

对应唯一蓝区运行：`LM480_13725_EPfix_inline_eager_admit_tail20_20261118_2026_r1`。
2000条预热、4000条约90%共享前缀正式用例均成功、0失败，8192入、1出。
这是补丁与开关的对齐；客户原模型、权重、TP/PP/MTP、990000上下文、IP、端口及正常STORE均保留。
蓝区本轮使用dummy权重、480GiB、20RPS；客户当前800GiB、真实权重、10RPS的成绩单列，不承诺相同TTFT。

## 这次完整安装包含什么

| 项目 | 安装内容 |
|---|---|
| A线 | 已实测的权重镜像先于大池分配，沿用当前gate启动方式 |
| 首次LOOKUP回复 | 原交付的8文件增量，已安装时不重复应用 |
| 提前查询 | 自动核对并设置原脚本中的`lmcache.mp.eager_prefetch=true` |
| 模型等待期间接纳 | 实测的vLLM core、executor、共享内存队列三文件补丁 |
| 步尾依赖 | 实测的vLLM Scheduler初始化和等待方法 |
| 实际Ascend步尾 | `KVDeliveryScheduler.schedule`的真实20ms路径，包含生效标记 |
| 共同EPLB启动修正 | 各对端独立接收缓冲、预热结束同步；保留动态EPLB |

这里直接交付已测的原生vLLM接纳补丁，不把尚未完成设备验收的Ascend移植替代品混进来。
自动双server启动方案仍未通过完整NPU冷启动验收，不包含在本轮完整包。

## 已下载旧包：两端容器各执行

保留当前服务和日志。第一段只更新交付目录、检查源码，不修改运行中的服务源码。

```bash
source /opt/ks-luv-pp/env.sh
cd /opt/ks-pp2-verified-20261118
git pull --ff-only
python -B customer_pp2/deploy.py check --full
```

源码不适用会明确指出组件并停止；回传该输出即可，不强行覆盖整棵框架。
若自动定位不正确，沿用检查给出的实际包根，显式指定`--lmcache-root`、`--ascend-root`、`--vllm-root`。

停止两端本轮vLLM和LMCache，保留容器。然后两端分别执行：

```bash
cd /opt/ks-pp2-verified-20261118
python -B customer_pp2/deploy.py apply --full &&
python -B customer_pp2/deploy.py check --full --require-applied &&
python -B customer_pp2/configure_model.py
```

最后一条只自动选择唯一一份已经接入`customer_pp2/enable.sh`的原模型脚本。
如果候选不唯一，按打印的原脚本路径显式执行，例如711已确认的脚本：

```bash
python -B customer_pp2/configure_model.py /opt/ks-luv-pp/startpptest.sh
```

该操作备份原脚本，只设置LMCache子连接器的提前查询以及必要的enable行，核对其余JSON字段语义不变，并检查Bash语法。
不修改模型、其他启动参数或缓存容量。

安装检查必须包含`delivery_scope=FULL_13725_2026`，以及以下全部`ALREADY_APPLIED`：

```text
lookup
mirror_gate
model_wait_admission
tail_scheduler_base
ascend_tail_20ms
eplb_startup
```

僵尸进程不再计作活服务；实际存活的模型和缓存仍会阻止应用。安装失败会恢复本次涉及的全部源码。

## 启动与生效核验

原模型脚本中的`source .../enable.sh`会自动开启与本轮相同的接纳、20ms步尾、查询观测开关，并在启动前检查完整补丁。
两台同时启动原模型脚本；各16份gate记录齐全后，启动原LMCache，再分别放行。
800GiB用法保持：711 `gate.sh open 0 --pool-gib 800`，695 `gate.sh open 16 --pool-gib 800`。
API健康后按原负载预热。

在711检查本轮新EngineCore的日志：

```bash
grep -E 'LMCache ADD during model wait|PP_LOOKUP_TAIL_ACTIVE|PP_LOOKUP_TAIL step=' /opt/ks-luv-pp/logs/vllm.log | tail -n 12
```

必须看到当前新PID的`PP_LOOKUP_TAIL_ACTIVE ...KVDeliveryScheduler.schedule`，后续摘要`impl=KVDeliveryScheduler`。
持续负载还应出现`LMCache ADD during model wait`；无新请求时不能据此判定该回调失效。
只有源码齐全、原脚本配置齐全以及本轮实际入口生效，才将客户成绩标为完整版本。

## 回退

停止本轮模型和缓存后，在同一交付目录执行`python -B customer_pp2/deploy.py rollback`。
它恢复最近一次安装前源码并拒绝覆盖之后的源码改动。
原模型脚本单独恢复`configure_model.py`打印的`.pp2-full-backup-*`原件。
同一完整版本的新增代码依赖新的enable开关；回退源码后需同时恢复对应旧交付目录版本和原启动配置，不能保留新enable继续起旧源码。
