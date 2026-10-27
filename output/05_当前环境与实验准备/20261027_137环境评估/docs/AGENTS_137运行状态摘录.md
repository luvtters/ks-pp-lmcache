# AGENTS 137运行状态摘录（归档快照）

仅摘录本轮运行状态，不是仓库规则入口；未覆盖仓库根AGENTS。源段落中的旧PID/待验描述以段尾最新状态为准。

### 当前评估窗口（2026-10-27，137环境）

- 用户新放行137两小时，后多次延长，最新明确最晚可使用至北京时间22:30，继续验证减层单机PD；能提前完成就告知用户。8／12没有本轮占卡授权，不能自行使用。22:30后不新发占卡任务，不因到点自动停止自有服务。
- 用户已授权清退，重新核实后已终止`vllm-ascend-dsa-cta`的16个占卡worker及2个直接模型父进程，保留容器和文件；不得再用旧PID重复清退。两张设备仍报Alarm，小矩阵正确不等于硬件健康恢复。
- 不把全局Python未安装lmcache误认为私有venv缺失；不把旧D runtime当新P候选。新文件只放自有容器私有`/opt`，不写现有宿主挂载`/root/ks-work`或公共模型。当前剩余约49GiB，不复制大权重／venv。
- 最新用户确定顺序：先调通新机制，再以完整模型单机P对比原异步ON和分段候选ON，最后才尝试减层单机PD。两组均保留TP8／PP2、DSA CP、MTP、分层41／37和相同请求／缓存条件；共同的本地返回适配单列。不得把通知接通冒称实际前段提前计算，不得用小模型替代完整模型性能结论。
- 真实小张量STORE／RETRIEVE与内容校验、真实Future经独立通知到core CPU等待方法的功能链已执行；NPU子进程退出发生native heap abort，整体验收未通过，不得只取内部PASS。临时小缓存已在确认终态、设备同步和UNREGISTER后结束，未停止他人服务。
- 完整P缓存PID1417770、端口5565／15565已启动并保留。最初单一LMCache `kv_both`在PP＋MTP校验中失败、未加载模型；后续共同适配保留外层producer、单LMCache子both、原17行与pin，并显式恢复本地sample返回。当前仅允许客户原文max_tokens=1的首token机制诊断，连续MTP生成尚未支持，不能跑原输出长度正式benchmark。新P PID1418075已启动，是否就绪以实时日志为准，不能将进程存在当启动成功。
- 本轮具体证据与命令在`output/05_当前环境与实验准备/20261027_137环境评估/`。客户原行36的prompt不改、原输出300保留在数据文件，首token诊断显式覆盖为1并单独记录，不与389条正式结果混用。不重跑已通过的126项CPU／16rank合成通信／人工IPC组件。
- 原P PID1418075已自行故障退出：cold首token成功20.136s且STORE完成，但随后cold清理批暴露HcclBroadcast超时；warm无已观察的正常READ，不能把失败直接归因恢复内容错误。KV-only零token“必然发sample”与“后段必然收activation”两种简单猜测已排除，不按猜测改分支。
- 已准备并启动共同首token缩窄版P PID1430473：`ks_local_p_complete=false`、`ks_local_p_first_token_only=true`，保留原producer PP采样反馈skip，去掉先前为完整本地连续生成加入的非必要反馈。原17行、pin、TP／PP／CP／MTP均保留；能否解决尚待真实请求验收。缓存已nonforce验空，缓存／容器未停。
- 最新完整P结果覆盖上述旧启动状态：原组PID1430473与候选PID1442833各完成1冷＋2热共6次成功，已按授权先后切组并转入减层PD。原组TTFT为9.536／9.752／0.658s，候选8.677／0.736／0.653s；首次热路径热化混杂，第二热接近，不能宣称提速。候选真实恢复通知已到core，但两次均后段先就绪、约26—28ms后前段就绪；没有阶段admission／execute／设备完成事件，核心分段执行尚未验收。
- 新分段候选14模块部署于私有`runtime/stage-candidate`，实际通过独立`stage-launcher`加载并保留common runner／pin。scheduler、wire、真实Core组合CPU通过，7个stdlib-only helper真实导入通过；完整框架导入退出native heap错误在原组同样出现，不视为候选特有或整体导入通过。
- 用户批准N16单机PD：P为TP4／PP2、8＋8层、前8卡，D为DP2／TP4／PP1、后8卡，保留DSA CP、MTP3；同一客户原文输入29696、原输出300、N1/C1冷／热，不跑389条。公共模型不改，仅私有N16视图、原MTP78层及必要的Mooncake逻辑层号／draft配置适配。21:37实查两D已ready，P在融合专家通信预热中失败；下一轮对两组P共同关闭官方FUSED_MC2选项，不重启D。减层PD尚无benchmark成功结果。

- 22:27前卡上请求验证结束：N16原异步与候选各1冷＋2热共6次均输出300成功，D每次外部覆盖29696、新算前缀0；第二热TTFT548／532ms，候选自然两热均后段先就绪、随后global_finished回原路径，无stage admission，不证明提速。受控500ms后段消息公开延迟诊断也成功300，明确前段先就绪并partial reserved，但仍无真实stage计算证据。
- 已读取137实际`patch_balance_schedule.py`：Ascend在PD producer启用独立BalanceScheduler.schedule，继承候选promotion却覆盖基础schedule，遗漏partial delay_cache_blocks与attach_output两钩子。这是本轮分段候选尚未真正派发的接线缺口，不是LMCache原性能劣化的根因。后续仅在本机隔离副本补实际调度器钩子并CPU验证，不部署或启停服务。
- 当前P1512007保留在DIAGNOSTIC_ONLY配置（后段真实完成消息延迟500ms），不能直接当正式性能配置；D1471631／1471664、缓存1417770和代理／容器保留。22:30后不新发用卡任务。所有N16输出全文差异与token／KV数值正确性仍记NOT_VERIFIED，不把HTTP成功等同完整正确性验收。
- 本机`实际P调度接线修复`已基于137真实BalanceScheduler.schedule补两钩子（partial缓存保护＋分段批标记），6项实际函数CPU验证通过：旧版先复现预约但无tag/保护，修复后生成tag/保护，并保留默认关闭、全局完成、禁抢占和balance节流。未部署或启用；真实阶段worker执行、设备重叠及收益仍未验收，不能拿CPU通过代替。

