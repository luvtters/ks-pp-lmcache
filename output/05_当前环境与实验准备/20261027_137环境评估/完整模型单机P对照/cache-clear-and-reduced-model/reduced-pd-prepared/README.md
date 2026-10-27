# N16 同机 PD：已批准机制诊断的启动准备

用户已确认：P 使用前8卡、TP4/PP2、分段8,8；D 使用后8卡、DP2/TP4/PP1；保留原 DSA CP 和 MTP3。每个 ON 组使用客户原行36/index35/req-0353 原文，既有 GLM 输入长度29696，原输出300，N1/C1、预热0，一冷一热。减层结果只用于机制核对，不能替代完整78层模型或原389条正式对照。本目录的命令尚未启动；当前完整 P 的运行由主任务管理。

## 模型视图与权重覆盖

原模型为 `/opt/ks-models/GLM-5.1-w8a8c8-MTP`。私有 target 视图为 `/opt/ks-stage-ready-luv-20261027/reduced-pd/model-N16`。仅新写 `config.json`（业务层数78→16）、小索引和清单；34个原始大分片和 tokenizer／量化描述均用原文件链接，不复制或重命名 tensor，不改原模型。索引包含所有实际可见分片的31328个名称，其中前16层及全局权重必需30670个；多出的658个为边界16层，由实际官方 target loader 的 spec-layer 判断跳过，没有layer>16溢出。构建器只读 safetensors 头部验证索引名称覆盖并重算视图 `total_size`，不读取 tensor payload。

P、D 的 `--speculative-config` 均显式 `model=/opt/ks-models/GLM-5.1-w8a8c8-MTP`，draft 从原配置78定位 `model.layers.78` 的 MTP；不让 draft 从减层配置误读普通业务层16。原 `num_nextn_predict_layers=1` 保留，使 target 跳过混入的边界16层。

私有写入的本机入口：

```powershell
python -X utf8 "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\完整模型单机P对照\cache-clear-and-reduced-model\prepare_reduced_pd_private.py" --materialize
```

该入口上传本目录的小材料，并在自有容器内运行：

```bash
python -S /opt/ks-stage-ready-luv-20261027/reduced-pd/build_model_view.py --source /opt/ks-models/GLM-5.1-w8a8c8-MTP --destination /opt/ks-stage-ready-luv-20261027/reduced-pd/model-N16 --layers 16 --materialize
```

去掉 builder 的 `--materialize` 只读检查，且要求目标视图尚不存在；创建后用清单核对，不能反复创建或覆盖既有视图。本次不自动删除／重建任何目录。

## 拓扑、DP 与端口

历史137四D的实际argv提取在 `historical137_d_actual_argv.json`，来源为2026-10-16实际采集，不冒充当前进程：四D各显式DP rank0—3、共DP4、TP4、PP1、启用专家并行，共用192.168.0.223:18300。候选使用同样的联合DP形式，改为rank0/1共DP2；全局专家并行组为DP2×TP4=8，不把两个D当作两个独立DP1服务。

| 服务 | NPU | HTTP／控制端口 | Mooncake握手端口 |
|---|---|---|---|
| P TP4/PP2 | 0—7 | 18100；mp master18190 | 28000—28007 |
| D0，DP rank0 | 8—11 | 18200；共同DP RPC18300 | 29000—29003 |
| D1，DP rank1 | 12—15 | 18201；共同DP RPC18300 | 29004—29007 |
| proxy | 无 | 127.0.0.1:18083 | 无 |
| LMCache | 独立缓存设备上下文，主任务管理 | RPC5565；HTTP15565 | 无 |

P/D双方 Mooncake 都明确 prefill(dp1,tp4,pp2,partition8,8)／decode(dp2,tp4,pp1)。实际137源码按 `kv_port + DP_rank*TP*PP*PCP + PP/TP内索引` 得到上述握手端口。固定端口集合的静态交集为空；未据此声称实时端口空闲。Mooncake transfer-engine及框架辅助通信仍使用官方自动端口。proxy的P/D地址均为137实际内网192.168.0.223，只列两个D。

## 启动材料与两组差异

完整原生命令分别保存在 `startup/p-original-async.serve.native.sh`、`p-stage-candidate.serve.native.sh`、`d0.serve.native.sh`、`d1.serve.native.sh`、`cache-server.native.sh`、`proxy.native.sh` 和四份 `bench-*.native.sh`。这些文件直接以 `vllm serve`／`lmcache server`／`vllm bench serve` 为入口，没有管理器、后台轮询或隐藏必要参数。人工执行时先在新 shell source `startup/common.env.native.sh`，再 source 相应服务的 `*.env.native.sh`，最后复制对应完整原生命令。

两组 P 的 serve 参数完全相同，仅环境选择原异步与通知／分段候选 loader。共同使用已批准17行MP适配与pin，不增加local sampled-token反馈；两local flags均false，保留原P skip，真正经Mooncake和D返回原输出300并释放。P保留DSA CP、batch16384、seq64、maxlen49152、util0.94、eager；D沿用最新已实测客户共同D的maxlen65536、4.5GiB显式KV池、batch164、seq32、util0.92、FULL_DECODE_ONLY图、async与MTP3。历史137 D 的maxlen32768未被隐瞒，参数差异单列在此；本次客户输入加原输出小于32768。

原异步 P 的私有loader只映射共同pin和原17，不替换model runner。候选 P 依赖主任务维护的 `/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher`，其中已有统一manifest、共同runner与pin映射；此目录不由本准备脚本改动。候选core按实际TP计算每段4个rank，无TP8硬编码。候选只允许P侧max_tokens1／无输入spec，原proxy将P请求设1，D仍请求原300。**Multi的Mooncake+LMCache两子组合、远端消费释放、实际量化加载与设备容量仍须运行验收，不能把文件准备或CPU/AST检查称为已通过。**

基准数据 `data/original-row36.jsonl` 的内容与原首token诊断数据相同，文件中原output300保留。本次benchmark显式 `--custom-output-len -1`，恢复逐条原输出；不沿用前阶段 `--custom-output-len 1`。原OFF不重跑。两ON组之间只由主任务按已授权流程切换P／缓存并用官方非force clear验空；本脚本不清缓存、不重启D／proxy或任何服务。

原生参数补记：D `--max-num-batched-tokens 164` 原样来自 `output/05_当前环境与实验准备/20261024_客户数据压测准备/实际验证配置/d/d0.serve.native.sh`（d1相同），本次不改成16384。两组N16共同沿用已验证完整P的LMCache子 `kv_both`；外Multi和Mooncake子仍为 `kv_producer`。此前生成草稿的LMCache子producer已修正，不保留无必要角色差异。reservation的TP8诊断门由源码负责人完成TP4最小适配并补CPU实际Scheduler+wire+Async更新组合TP4/TP8两项通过；这不构成NPU或长输出验收。

**N16 原MTP层名78的共同Mooncake私有适配已准备，设备验证待运行。** 137原代码会把 `model.layers.78.self_attn.attn`映射78，D的N16 PP末段8—16会漏掉它；单改metadata还会在接收方二次parse时漏选tensor。已授权的私有副本只改 `_build_kv_group2layeridx` 与 `KVCacheRecvingThread._get_group_kv_caches`，明确guard为target16/draft78/MTP1：保持checkpoint／tensor原名78，传输逻辑映射16；接收方先从完整group名称／完整逻辑index建立对应，再筛选子集，不用过滤后的indices与完整名称zip。register地址和PP过滤本体沿用原实现。actual AST CPU8项覆盖P前／末段、D、17个逻辑槽、PP末段8—16选到真正MTP78 tensor、组顺序／反序cache字典／非连续子集、原N78及guard外行为不变。diff为上级 `mooncake_n16_review.patch`，结果为 `mooncake_n16_cpu_result.json`；入口为 `python -X utf8 prepare_mooncake_n16_overlay.py --deploy`。仅私有文件变更，安装源与公共权重未改，不把CPU结果称为PD正确性或提速。

共同env显式设置 `KS_N16_MOONCAKE_OVERRIDE=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/mooncake-n16/mooncake_connector_n16.py`；canonical为 `vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector`。原异步P普通finder和D/common finder读取该变量；候选独立stage-launcher由其负责人加入相同可选映射，无变量时原17项严格不变。不把框架包目录加入PYTHONPATH，也不影响仍运行的完整P进程。

首次N16初始化已实际失败，未发benchmark：P 12:52:37 UTC、D1 12:52:47、D0 12:52:48均在draft加载时报11项 `model.layers.16.*` 特殊参数未初始化。根因不是Mooncake注册：父类 `_create_draft_vllm_config`只替kernel/attention，未替model_config；即使draft路径指向原模型，new-style MTP构造仍读target16。共同私有修正只在target16/draft78/MTP1条件下 `copy.copy(draft_vllm_config)` 后换为真实draft ModelConfig，保留target输入和parallel/quant/compilation对象；原N78路径不改。官方spec显式增加 `quantization="ascend"`，补齐显式draft路径不会自动继承的量化字段；不通过关闭missing检查或伪造初始化绕过错误。部署入口 `python -X utf8 prepare_draft_config_n16.py --deploy`要求先有实际AST构造与原78权重写入的CPU PASS回执，再传私有源及P/D共同env；候选finder使用相同可选单file映射。CPU参数写入fixture与实际NPU加载验收分开记录。

最终版本补记：上述仅替换draft ModelConfig的中间版本在13:09 UTC再次初始化失败，首错为原MTP78的 `fused_qkv_a_proj.weight` KeyError。实际Ascend量化代码按构造上下文的HF `model_type`选择packed map；通用`deepseek_mtp`表缺该融合映射，GLM表则正确拆成原描述里真实存在的两项W8A8，未发现借用target最后业务层权重的依据。最终已部署32行私有改动：真实draft ModelConfig/HF独立副本保留层数78与派生信息，仅构造上下文HF family恢复target `glm_moe_dsa`；另传原draft配置仍选择DeepSeekMTP类和原checkpoint，target/specdraft对象均不改。新CPU回执 `draft_n16_family_quant_cpu.json`同时证明真实quant dispatch成功、通用deepseek_mtp反例重现KeyError、7项权重实际写入路径及强制strict通过；Scheme/Adapter/参数为CPU fixture，未加载真实checkpoint bytes或NPU。`prepare_draft_config_n16.py --deploy --source-only`只更新已接线的proposer单file，已执行完成；安装源／原权重未改。最终设备初始化与PD结果由本轮后续原始日志记录，不能以此CPU结果认定已跑通。

所有HOME/TMPDIR/cache/trace均在私有 `/opt/ks-stage-ready-luv-20261027/reduced-pd`；不写宿主root、公共挂载或模型目录。阶段准备只做元数据／文件／bash语法检查，不导入torch、vllm、lmcache或任何NPU模块。当前未有模型正确性、PD连通或性能结论。


## P 预热融合通信路径调整

第三次N16加载已越过target/draft权重构造，末段实际模型加载约24.17GB；P在13:33:06 UTC的profile dummy-run中，于target MoE调用 `DispatchFFNCombine` 发生EZ1008 tiling失败。两D已ready。实际137 selector按官方 `VLLM_ASCEND_ENABLE_FUSED_MC2`：值0时小token选MC2、大token选ALLTOALL，避开该融合算子；没有找到EP>=8最小rank硬限制。C++另有HCCL窗口容量检查，TP4每rank token变多可能相关，因未记录实参M不能确认为本次唯一原因。

用户已批准P原异步与候选共同在各自P env追加 `export VLLM_ASCEND_ENABLE_FUSED_MC2=0`，覆盖common原值1；D env/common及已运行D/cache/proxy均不改。TP4/PP2、8,8、CP、MTP3、模型和客户输入/输出均不改。这是减层PD两ON组共同必要启动差异，不当作候选独享收益。官方配置在FUSED_MC2=1时会自动禁用多流共享专家重叠；改0后原native的multistream_overlap_shared_expert=true不再受该条件禁用，此官方连带行为应与性能边界一起记录。

四份benchmark只修正结果目录为管理器约定的 `results/original-async-cold`、`original-async-warm`、`stage-candidate-cold`、`stage-candidate-warm`，数据、原输出300、N1/C1、预热0及其余参数不改。proxy仍18083。此准备没有启停任何服务或执行benchmark，非融合路径能否在本形态实际完成仍待本轮运行验收。


## P 普通MC2窗口容量修正

env0后13:45:11 UTC的新首错已明确：profile_run为mc2_tokens_capacity做预热，在普通 `npu_moe_distribute_dispatch_v2`／`aclnnMoeDistributeDispatchV4` 的tiling检查报 `HCCL_BUFFSIZE_EP is too SMALL`。日志实际参数为maxBs64、h6144、epWorldSize4、localMoeExpertNum64、k8；算子要求413MB，实际HCCL/CCL窗口为400MB。这是已确认窗口不足，不据此宣称EP4不受支持。

已按用户批准，仅P原异步／候选两份env追加 `export HCCL_BUFFSIZE=512` 覆盖common400，保留 `VLLM_ASCEND_ENABLE_FUSED_MC2=0`。D/common文件和已就绪D/cache/proxy均不改，P TP/PP/CP/MTP3、batch/seq、模型、客户数据、输出及压测口径均不变。512超过实测所需413MB；这只完成必要配置准备，是否通过设备预热及PD请求仍由本轮实际运行记录验收。本脚本不启动、停止服务或发请求。


## 追加第二次同文热请求

主任务已确认N16原异步组cold/warm各1次均返回原300输出；首次warm TTFT为9.195s，存在首热化混杂的可能。用户已批准两ON组各追加一次相同warm2复核，不替换原cold/warm结果。两份 `bench-original-async-warm2.native.sh`／`bench-stage-candidate-warm2.native.sh`逐字复制既有warm命令，只将结果目录改成 `results/original-async-warm2`／`results/stage-candidate-warm2`。客户原文、原输出300、N1/C1、预热0、proxy18083及全部采样参数保持；本准备仅上传启动文件并做bash语法检查，没有发送请求或启停服务。
