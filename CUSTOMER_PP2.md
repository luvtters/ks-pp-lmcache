# 客户 PP2 服务部署

当前 480 GiB、开启权重卸载的双机验证基线使用 [两端安装、启动与检查](customer_pp2/baseline19/README.md)。交付分支为 `delivery/customer-pp2-lm480-20261119`。

## 11 月 18 日历史版本

固定交付分支：`delivery/customer-pp2-verified-20261118`。

与今天137＋25正式测试完整对齐的入口：[完整安装和生效核验](customer_pp2/FULL.md)。
统一执行`python -B customer_pp2/deploy.py check --full`和`apply --full`。

包括LMCache首次回包、提前查询、原生vLLM设备等待期间接纳、实际Ascend步尾20ms、共同EPLB启动修正，以及已测的镜像先分配启动门。
不安装依赖、不重新编译，保留客户原模型和服务参数。

最初不带`--full`的安装仅是子集，不能当成今天的完整实测版本。
尚未完成设备验收的自动双server启动方案及Ascend接纳移植版未混入。
