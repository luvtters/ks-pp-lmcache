# PP2 / LMCache 代码 Review

2026-11-12。本仓库归档跨机PP2接入、可靠性、查询与消息队列优化，以及独立CPU候选。保留实验使用的原commit号；这次只发布代码和说明，没有修改已测代码或启动服务。

## 从哪里开始

**最近查询、MQ和lookup统计修正在LMCache；LMCache-Ascend是前期PP/NPU适配。** 建议先看下面的查询/MQ增量，再按需查看可靠性和Ascend适配。未验证的候选分别保留，没有合成一个“全部通过”的版本。

| Review范围 | 可直接打开的链接 | 说明 |
|---|---|---|
| **查询 / MQ主要增量** | [02ffb69 → e8e2f1d](https://github.com/luvtters/ks-pp-lmcache/compare/02ffb6943924d1ebd78e31dc3a608dd4af3de271...e8e2f1d7295e56b55f880f51b4609c46b46fdc0a) | 夜间主要设备版本；单段最终miss短路、各端ACK后独立STATUS、有限连续发送、配置校验与默认关闭观测 |
| 可靠性 | [89d93a2 → 02ffb69](https://github.com/luvtters/ks-pp-lmcache/compare/89d93a2...02ffb6943924d1ebd78e31dc3a608dd4af3de271) | 实际搬运期KV借用保护、失源安全重算、迟到控制清理、PREEMPTED后重查 |
| 前期Core PP接入 | [3e11e10 → 89d93a2](https://github.com/luvtters/ks-pp-lmcache/compare/3e11e10812b448142c6cdb177a3058652bd826fa...89d93a2) | stage-local几何、原生PP注册、初始化拒绝原因传播 |
| MQ轻量观测 | [e8e2f1d → 4ebc47e](https://github.com/luvtters/ks-pp-lmcache/compare/e8e2f1d7295e56b55f880f51b4609c46b46fdc0a...4ebc47edbcfe332a06271f56e15e472bff046998) | mq.py增加18行聚合观测；今天新128模型组实际加载 |
| lookup统计去重 | [e8e2f1d → a46b927](https://github.com/luvtters/ks-pp-lmcache/compare/e8e2f1d7295e56b55f880f51b4609c46b46fdc0a...a46b9277bd258aab451868a7b31e31cf61fd10e0) | CPU 9项通过，未部署；只修样本计数，不是TTFT提速 |
| 独立STATUS预算 | [02ffb69 → 1362d90](https://github.com/luvtters/ks-pp-lmcache/compare/02ffb6943924d1ebd78e31dc3a608dd4af3de271...1362d907a5e474b7359fe3636955ab0f5852ae92) | CPU 32项通过，未部署，不包含在e8 / 4eb / a46中 |
| **Ascend PP生产适配** | [fbabeec → ac1e4c7](https://github.com/luvtters/ks-pp-lmcache/compare/fbabeecc79ba79663db6c7bb8555b9dcc649a125...ac1e4c73bc3115d3b6b2179da31d9ccb4ebed28e) | 分段NPU缓存、pinning与按Core注册元数据选择缓存上下文 |
| Ascend启动材料 | [ac1e4c7 → b06a829](https://github.com/luvtters/ks-pp-lmcache/compare/ac1e4c73bc3115d3b6b2179da31d9ccb4ebed28e...b06a829b8d2d0601e722fcd1b11578a2372ad665) | 历史启动patch打包、切换官方headless executor修正，单独审查 |

本仓库用不同分支保留两个上游仓库的历史；不是把两套代码合入同一源码树。上游入口：[LMCache](https://github.com/LMCache/LMCache)、[LMCache-Ascend](https://github.com/LMCache/LMCache-Ascend)。完整hash、全部提交及变更文件见[commit-manifest.json](review/commit-manifest.json)。

## 可直接浏览的源码分支

| 内容 | 分支 | 固定终点 |
|---|---|---|
| LMCache查询 / MQ | [ks/pp-control-fastpath-luv-20261111](https://github.com/luvtters/ks-pp-lmcache/tree/ks/pp-control-fastpath-luv-20261111) | [e8e2f1d](https://github.com/luvtters/ks-pp-lmcache/commit/e8e2f1d7295e56b55f880f51b4609c46b46fdc0a) |
| LMCache MQ观测 | [ks/mq-observe-luv-20261112](https://github.com/luvtters/ks-pp-lmcache/tree/ks/mq-observe-luv-20261112) | [4ebc47e](https://github.com/luvtters/ks-pp-lmcache/commit/4ebc47edbcfe332a06271f56e15e472bff046998) |
| LMCache统计候选 | [ks/lookup-metric-luv-20261112](https://github.com/luvtters/ks-pp-lmcache/tree/ks/lookup-metric-luv-20261112) | [a46b927](https://github.com/luvtters/ks-pp-lmcache/commit/a46b9277bd258aab451868a7b31e31cf61fd10e0) |
| LMCache预算候选 | [ks/pp-status-budget-luv-20261111](https://github.com/luvtters/ks-pp-lmcache/tree/ks/pp-status-budget-luv-20261111) | [1362d90](https://github.com/luvtters/ks-pp-lmcache/commit/1362d907a5e474b7359fe3636955ab0f5852ae92) |
| LMCache-Ascend | [ks/pp-framework-patches-luv-20261110](https://github.com/luvtters/ks-pp-lmcache/tree/ks/pp-framework-patches-luv-20261110) | [b06a829](https://github.com/luvtters/ks-pp-lmcache/commit/b06a829b8d2d0601e722fcd1b11578a2372ad665) |

## 关键commit简述

### LMCache：接入与可靠性

| commit | 简介 |
|---|---|
| [8cd2d5b](https://github.com/luvtters/ks-pp-lmcache/commit/8cd2d5b) | 保留既有PP本地缓存握手、STORE块索引和pin修正 |
| [c02faca](https://github.com/luvtters/ks-pp-lmcache/commit/c02faca) | 为Ascend分段缓存提供MP几何扩展点 |
| [3f018d1](https://github.com/luvtters/ks-pp-lmcache/commit/3f018d1) | 将完整stage的PP缓存注册纳入原生MP协议 |
| [89d93a2](https://github.com/luvtters/ks-pp-lmcache/commit/89d93a2) | 经ZMQ返回PP初始化拒绝原因 |
| [eececb6](https://github.com/luvtters/ks-pp-lmcache/commit/eececb6) | 保留延迟STORE完成基线，作为安全读取前提 |
| [fe3d38d](https://github.com/luvtters/ks-pp-lmcache/commit/fe3d38d) | 按实际对象身份建立和释放read borrow，隔离旧回调与同key新对象 |
| [86135ad](https://github.com/luvtters/ks-pp-lmcache/commit/86135ad) | 控制Future增加竞争安全完成回调 |
| [858619f](https://github.com/luvtters/ks-pp-lmcache/commit/858619f) | borrow保护持续到设备完成；失源、部分提交和失败负载安全收尾 |
| [1b574d2](https://github.com/luvtters/ks-pp-lmcache/commit/1b574d2) | 控制交付状态不确定时保留generation |
| [9a155a5](https://github.com/luvtters/ks-pp-lmcache/commit/9a155a5) | 部分APC恢复失败后丢弃旧提示，约束STORE覆盖范围 |
| [944f0c5](https://github.com/luvtters/ks-pp-lmcache/commit/944f0c5) | 本地迟回访但未发STATUS时不误判服务端不健康 |
| [02ffb69](https://github.com/luvtters/ks-pp-lmcache/commit/02ffb69) | PREEMPTED后重建查询会话，防止复用旧命中 |

### LMCache：本轮查询 / MQ

| commit | 简介 |
|---|---|
| [4e2e3de](https://github.com/luvtters/ks-pp-lmcache/commit/4e2e3de) | 单段最终整数STATUS=0即可提前返回miss，保留迟到控制清理 |
| [f6a5176](https://github.com/luvtters/ks-pp-lmcache/commit/f6a5176) | 每端自身ACK完成后推进自身STATUS，不要求双端ACK齐备 |
| [fe11bf0](https://github.com/luvtters/ks-pp-lmcache/commit/fe11bf0) | 有限连续发送、预算耗尽后重新唤醒；注销仍完整排空 |
| [e56fb68](https://github.com/luvtters/ks-pp-lmcache/commit/e56fb68) | 配置传递、非法组合前置校验、CPU排队 / STORE等观测 |
| [d713144](https://github.com/luvtters/ks-pp-lmcache/commit/d713144) | 实际CPU TCP / ZMQ控制链测试 |
| [e8e2f1d](https://github.com/luvtters/ks-pp-lmcache/commit/e8e2f1d) | 观测限定支持模式和生命周期，补原路径ACK记录 |
| [4ebc47e](https://github.com/luvtters/ks-pp-lmcache/commit/4ebc47e) | 可选MQ轮次耗时、预算用满和残余队列聚合 |
| [a46b927](https://github.com/luvtters/ks-pp-lmcache/commit/a46b927) | 同一次lookup结果只记首次最终样本，避免短重复样本稀释均值 |
| [1362d90](https://github.com/luvtters/ks-pp-lmcache/commit/1362d90) | 独立的首次STATUS固定预算，后续不续期 |

### LMCache-Ascend

| commit | 简介 |
|---|---|
| [8c49454](https://github.com/luvtters/ks-pp-lmcache/commit/8c49454) | 显式启用TP16 / PP2分段缓存、NPU pinning和相关CPU契约 |
| [ac1e4c7](https://github.com/luvtters/ks-pp-lmcache/commit/ac1e4c7) | 根据Core注册元数据选择本段缓存上下文 |
| [9183a46](https://github.com/luvtters/ks-pp-lmcache/commit/9183a46) | 归档跨机PP启动patch及应用脚本 |
| [b06a829](https://github.com/luvtters/ks-pp-lmcache/commit/b06a829) | 修订为官方headless executor补丁；旧patch保留审查和回退用途 |

Ascend自有增量应比较`fbabeec..b06a829`。不要用更早的`19c13d8`作为自有变更起点，否则会混入后续上游MP / 内核迁移。

## 分支关系

```text
Core上游 3e11e10
  └─ PP接入 89d93a2
      └─ 可靠性 02ffb69
          ├─ 查询/MQ e8e2f1d
          │   ├─ MQ观测 4ebc47e    [新128已运行]
          │   └─ 统计修正 a46b927  [独立CPU-only]
          └─ STATUS预算 1362d90    [独立CPU-only]

Ascend上游 fbabeec
  └─ PP适配 ac1e4c7
      └─ 启动材料 b06a829
```

## 已验证范围与限制

- e8定向CPU 81项通过，实际TCP / ZMQ控制与模型benchmark已执行；不等于每项设备故障或严格数值全部验收。
- 夜间两类3000请求均3000 / 0，平均TTFT和吞吐优于固定APC参考，但参考缺P90，总差值尚未归因到查询或MQ。
- 同600G查询开 / 关的一对结果未证明明显净收益；今天MQ0 / 32两组各128 / 0，因cache详细观测开关不同，不能作为纯MQ的P90收益或回归证明。
- STORE只有观测，没有关闭STORE、删除同步、放宽设备安全条件或提前释放KV。新模型采样确已记录原生free queue归还，客户P90先因仍未定。
- a46 CPU 9项通过，136 CPU 32项通过，二者未部署。统计修正可能让lookup均值升高，只代表不再被短重复样本稀释。
- Ascend目录内的历史启动patch会作用于vLLM / vLLM-Ascend；与本轮LMCache查询 / MQ逻辑分开。实验私有loader、观测wrapper、容器现有工作树和native二进制不在这次提交中。

## 本地查看

```bash
git clone https://github.com/luvtters/ks-pp-lmcache.git
cd ks-pp-lmcache

git diff 02ffb69..e8e2f1d -- lmcache
git diff 89d93a2..02ffb69 -- lmcache
git diff fbabeec..ac1e4c7 -- lmcache_ascend
git show a46b927
git show 1362d90

# 阅读固定版本时按需切换；不会安装依赖或启动模型
git switch --detach e8e2f1d
```

建议重点review：迟到控制与取消清理、borrow设备终态安全、部分APC失败后的有效STORE范围、MQ重新唤醒 / 保序 / 注销排空，以及统计去重是否正确保留真实重查样本。
