# 137本机源码归档

最新增量精确清单为 `COPY_INCREMENTAL_FILES.txt`，含逐文件来源、字节数及文本/字节一致结果的记录为
`COPY_INCREMENTAL_MANIFEST.json`。当前增量清单包含 162 个文件；对于与初次 `COPY_MANIFEST.json`
重名的路径，以增量清单为当前文件版本。原299文件及已放置的5个旧依赖输入未改动。

新增内容包括 TP4绑定/partial观测、父包路径修复、17→18→19可选精确加载器、私有部署与CPU回执，
以及 N16 两份共同私有修复的原始基线、最终源码、最小diff、必要CPU输入和 native/env/view构建入口。
不含客户 prompt/data、权重、venv、secret 或运行大日志。小型导入退出摘要及两行就绪事件不提升证据等级。

`恢复身份与启动接线/stage_driver/version_snapshots/` 保留完整P实际用过的TP8 helper与修复后17项loader，
并通过已记录私有部署diff逐字确认。`runtime_overlay/` 与 `launcher_snapshot/` 为最后私有部署版本；
TP4增量未热重载到早先完整P。最新 CPU 是 reservation 8项、wire 9项、TP8/TP4组合2项、finder5项；
本次仅复制已有结果，没有重跑。

完整P HTTP成功及全段KV事件登记不能证明阶段计算提前。实际阶段GLM计算、提速、输出300的阶段driver、
N16共同修复的完整PD正确性和正式1P1D对照仍未在本增量中验收。N16共同修复同时供P/D两组选择，
不可当作候选独占收益。详情见源版本边界JSON和N16 README/CPU proof。

本轮只有本机归档，没有git add/commit/push、远端动作、模型/算子或服务操作；没有hash和全仓扫描。

## 初次112文件归档记录（历史说明）

这是本次独立新增目录，不替换仓库原299文件、旧报告或旧源码快照。
当前可读入口为`恢复身份与启动接线/stage_driver/EXECUTOR_WIRE.md`，完整复制清单为
`COPY_MANIFEST.json`。所有复制文件按源字节原样保存；绝对路径、历史回执、源码来源和
未验证边界保持原文，不能因归档而变成新的实测。

保存内容包括：统一14模块；构建器和关键9项wire/1项Scheduler组合CPU入口及既有结果；
14模块依赖闭包与137真实7个标准库helper加载回执；metadata/bootstrap接线及其必要输入；
共同17/pin/modelrunner及首tokennative配置。早期bootstrap overlay与最终runtime_overlay
分目录保留，不能盲目叠加。最终runtime以统一manifest为准。

不含客户prompt/JSONL、模型数据、venv、密钥或运行大日志；bench脚本只引用外部原始数据
路径，不提供数据。文档引用的HTTP/cold/warm运行原始回执仍在原工作区，未复制入本归档。
源码bundle是必要CPU读取输入，不是运行日志。既有仓库上游许可证继续适用。

原仓库缺少的5份旧builder/CPU输入已获授权在原相对位置独占新建，未覆盖既有内容；
本目录`dependency_inputs/`同时保留其原样备份。详见`INPUT_PLACEMENT.json`。因此无需
修改builder路径或恢复wrapper。这里只核对输入存在与复制一致，没有为归档重跑构建/测试。

本次只复制和对比所列文件，不使用hash或全目录扫描，没有git add/commit/push、切分支、
服务操作或NPU任务。旧基线只检查已知依赖路径是否存在，未修改。

原有CPU结果是证据快照，不在复制时重跑。需要复核时从stage_driver目录手动执行：

```powershell
python -X utf8 -B .\check_executor_driver_wire_cpu.py
python -X utf8 -B .\check_integrated_first_token.py
```

这些检查仍是CPU/fixture，不是GLM/NPU候选验收。remote-safe-helpers脚本包含只读137入口，
复用时仍须当前授权。部署脚本和native文件仅归档，不代表执行授权。

## N16最终源码复核

68项冻结清单与最终32行draft GLM-family/HF78源码、diff及既有7权重/真实quant CPU回执已逐文件复核。CPU用例未重跑，未加载真实checkpoint。早期P曾在target预热遇DispatchFFNCombine错误；P两组随后共同采用官方FUSED_MC2=0及HCCL_BUFFSIZE=512，benchmark结果目录已匹配管理器。自然请求功能结果见下段；这不等于token/KV内容或候选阶段计算验收。复核细节见`N16_FINAL_SOURCE_RECHECK.json`。

自然6次结果现已单独归档到`减层PD运行证据/`：6次HTTP/benchmark成功，未出现stage admission，不能证明提速，token/KV内容仍NOT_VERIFIED。该目录只带结果分析和精简JSON；文档中原始benchmark/metrics/log链接仍指原工作区，本仓库按数据边界没有复制那些文件。独立`受控就绪窗口诊断/`带13份源码/准备材料及4份精简结果/实际0.23调度源码/检查器；默认关闭，500ms只由独立env启用。真实单请求成功并出现partial reserved，但实际Balance schedule遗漏两钩子、未进入阶段worker；不能并入自然6次或标为模型阶段PASS。本轮README/状态及AGENTS的137运行段落另存docs，状态中历史生成文本字段已删除并记录，根仓库AGENTS未改。
实际P调度接线修复的15项隔离候选与既有6项CPU结果已保存，loader为18/20 exact候选；仅核对明确引用依赖存在，未部署或复跑。

## 暂存候选路径（未执行Git）

`GIT_STAGE_CANDIDATES.txt` 为本轮 263 个精确仓库相对路径，另有NUL分隔的`GIT_STAGE_CANDIDATES.nul`。来源仅为两份复制manifest、本目录索引和5个新增旧输入；没有Git、暂存、提交或推送动作。
