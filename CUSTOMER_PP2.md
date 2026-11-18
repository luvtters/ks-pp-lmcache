# 客户 PP2 验证入口

固定交付分支：`delivery/customer-pp2-verified-20261118`。

请从 [客户环境应用与回退指导](customer_pp2/README.md) 开始。

本分支的 LMCache 源码终点来自 `91de3c8517699e11a4d1f04b732333b09867c6d9`。
默认部署只改现场已安装 LMCache 的八个 Python 文件，以及 Ascend worker 的启动门。
不安装依赖、不重新编译，不更换客户模型和服务参数。

设备执行间隙提前接纳的**已测原生 vLLM 版**放在
`customer_pp2/reference/verified-native-vllm-admission.patch`，仅供审查，默认部署不会应用。
它迁到 Ascend 后的版本尚未设备验收。

步尾 20ms 及 APC 覆盖上界等未完成设备验证的候选不在本次安装范围。
