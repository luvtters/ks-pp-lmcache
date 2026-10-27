# 当前首 token 阶段调度：代码、证据与运行边界

当前已完成实际Scheduler→阶段RPC/Future wire→实际Core消费的CPU闭环，并按137实际源码重基完整隔离overlay。**尚未完成统一包在真实环境的完整import验收，也未启动NPU阶段候选。** 单机P共同接法已有真实冷请求返回首token，但随后在该请求清理阶段出现HCCL广播超时，不能算完整生命周期成功。warm请求未正常进入可验收的READ过程。目前没有新的性能收益结论。

本说明对应当前版本。服务操作由主任务统一执行；本文整理和CPU检查不占NPU、不启停服务、不清缓存。

## 范围和版本

机器`115.120.85.137`，自有容器`ks-pp-m137-priv`，私有根目录`/opt/ks-stage-ready-luv-20261027/`。实际vLLM提交`0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`，Ascend提交`5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`，私有LMCache0.5.6rc5。overlay以实际137源码为基线，不用旧同版本完整模块替代，不修改安装源码。

单机P保留完整78业务层GLM、TP8/PP2、41/37分段、DP1/PCP1、DSA CP、MTP3、16K计算批、49152上下文上限、util0.94。真实启动日志确认actual async=True。请求为客户原行36/index35/req-0353的完整原文，GLM输入29696token，N1/C1、预热0、max_tokens=1。原输出长度300仍在manifest；本次1token是独立机制诊断，不能称原输出、389条正式对照或1P1D结果。未重跑OFF。

共同Connector为外MultiConnector/kv_producer、唯一LMCacheMP子kv_both，保留原P计算分支和真实缓存读写；共同17行及pin适配不变。

## 证据分层

| 层次 | 已确认 | 证据边界 |
|---|---|---|
| 真实小载荷缓存 | 官方MP真实STORE/LOOKUP/RETRIEVE；改写源tensor后两层各4096元素恢复正确、其他块不动；真实Future查询False→True，唯一observer查询/result并独立发事件；UNREGISTER ACK=True。 | 最终出现heap退出错误，不是正常退出PASS，不是GLM/PP模型整链。见`../../真实KV恢复验证/README.md`。 |
| 实际方法wire CPU | `executor_driver_cpu_result.json`为9/9：WorkerProc命令分流、worker阶段API、实际KVOutputAggregator、实际Core等待/消费方法。 | forward/设备/通信仍有CPU fixture；没有GLM/HCCL或性能证明。 |
| Scheduler组合CPU | `integrated_first_token_cpu_result.json`为1/1：实际schedule在front全READY时合法预约剩余1token，TP8/16rank wire→实际AsyncScheduler更新；前缀仅cache一次、16次sample，无重复free/assert。 | 正常模型初始化、统一包完整import及真实NPU阶段候选尚未验收。 |
| 统一包helper依赖 | 14模块AST的12条自定义导入全部在manifest中；137私有venv实际加载部署的7个标准库helper源模块成功，无缺失helper。见`import_dependency_closure.json`。 | 加载未执行vLLM/LMCache父包、torch或Ascend导入，不等于完整框架包import验收。 |
| 真实单机P cold | HTTP成功；29696输入，输出1token文本`5`；TTFT20.136239960s，bench总时长约20.136984s。 | 只证明该次HTTP输出；不能声称清理/后续请求正常、输出语义验收或缓存热收益。 |
| cold清理与warm尝试 | fatal dump仍是cold request finished清理；HcclBroadcast超时后服务退出。warm1没有有效输出，现有缓存日志没有warm Retrieved完成。 | 不能写成warm恢复内容错误、热请求成功耗时、硬件Alarm已致因或阶段候选失败。 |

cold/warm原始结果在`../../首tokenstatus-original-async-cold.json`、`../../首tokenstatus-original-async-warm1.json`；故障摘录在`../../完整模型单机P对照/warm1_p-original-async.log`和`warm1_诊断与首token反馈边界.md`。真实小载荷heap错误与模型HCCL超时是独立现象，尚未证明同源。最小import/退出检查未复现heap错误，不能据此排除完整运行时析构问题。

## 阶段实现与已验证控制流

1. 实际Connector metadata携带请求恢复generation和真实rank；唯一observer确认真实Future设备完成后，经独立通道发布。core每次schedule前及等待Future时登记事实，不伪造全局完成。
2. Scheduler仅在front TP8全READY、剩余1计算token、max_tokens=1且无scheduled spec/结构化输出时partial预约，保留原真实组批和记账，不提前cache未全局恢复的前缀；冷/未命中或已全局完成走旧路径。
3. driver复用原广播writer；worker主线程执行阶段API。后段先接activation，前段可先执行；后段本段KV与activation均就绪后才执行。设备event完成与CPU提交分别记录。
4. 每rank只sample一次。driver不等待front sample reply才派back sample。AsyncModelRunnerOutput先在worker调用真实get_output，再传CPU结果，不把NPU event/tensor直接pickle进独立通道。
5. 全rank样本按globalrank原序调用原KVOutputAggregator一次；最终Future由原Core FIFO消费。scheduler真实更新后才ack；设备/通信依赖和采样状态均退休后释放引用。控制ACK和execute的None都不冒充最终模型输出。

9项CPU中，实际Core用例直接运行step_with_batch_queue、_KSModelResultReader和等待泵：先排队早返，等待时在core线程推进wire，真实最终输出后才update/ack。TP8阻塞门直接运行实际GPU sender/receiver AST，让8个front先等待CPU broadcast门，再派back sample解锁，并核对prev-request映射/placeholder。它证明双方参与时driver无派发死锁，不证明现场mask和广播次数一致。另有设备query异常用例，异常转typed ERROR并保留batch/device引用，不吞成False无限轮询。

本版保留两个whole-batch名额与FIFO，但每段只有一个原始activation/runner buffer owner；B1输出消费及buffer退休后才启动B2，**没有实现B2前段与B1后段同时计算**。连续输出大于1token仍缺新draft、变长接受、前段输入和生命周期适配。保留MTP3配置不等于连续decode支持。

## 当前共同首 token 配置候选

已运行cold组为`ks_local_p_complete=true`，恢复普通PP sampled-token反馈。更窄共同候选保留原P producer skip，取消max1不需要的反馈：

```diff
- "ks_local_p_complete": true
+ "ks_local_p_complete": false
+ "ks_local_p_first_token_only": true
```

严格max1没有下一轮front decode输入；末段真实sample、MTP3 proposal和KV finalize保留，17/pin/外producer/子both/activation不变。first_token_only标明范围，专用客户端仍须明确max1，不是通用请求拦截器。

实际sample方法CPU前/后段2项通过，见`../../完整模型单机P对照/first_token_no_feedback_cpu.json`。它证明新增反馈路径被移除，**尚未NPU运行，也不证明反馈是现场超时唯一根因**。服务器仅额外保存未启用`startup/p-local-first-token.no-feedback.candidate.native.sh`，未据此重选或重启服务。wire9项与共同配置2项是不同证据，不能合成“新共同候选已整链通过”。

## 原生入口和参数

以下是复现材料，不代表现在执行。机器/容器如上，工作目录`/opt/ks-stage-ready-luv-20261027/standalone-p/work`。共同环境准确入口：

```bash
source /opt/ks-stage-ready-luv-20261027/standalone-p/startup/common.env.native.sh
source /opt/ks-stage-ready-luv-20261027/standalone-p/startup/local-completion.env.native.sh
```

环境原文在`../../完整模型单机P对照/common.env.native.sh`，包括私有venv/Ascend插件、容器私有缓存、设备0—15、192.168.0.223/enp23s0f3、`PYTORCH_NPU_ALLOC_CONF=expandable_segments:False`、`VLLM_PP_LAYER_PARTITION=41,37`、`VLLM_KV_CACHE_LAYOUT=NHD`。共同finder在`runtime/local-completion/sitecustomize.py`。

完整模型缓存命令使用5565；小载荷诊断曾用15577，两者不能混同：

```bash
lmcache server --host 127.0.0.1 --port 5565 --http-host 127.0.0.1 --http-port 15565 --chunk-size 512 --supported-transfer-mode lmcache_driven --l1-size-gb 12 --eviction-policy LRU --l1-align-bytes 65536 --disable-metrics --instance-id ks-luv-standalone-p-20261027 --max-gpu-workers 1
```

保留默认lazy，不加--no-l1-use-lazy；disable-metrics避开默认Prometheus9090冲突。小载荷eager107002不能泛化为默认lazy失败。

下面是**未运行的新共同首token候选**完整命令。实际cold组仅KV配置为ks_local_p_complete=true且没有first_token_only；准确旧命令保存在`../../完整Pstart-p-local-first-token回执.json`。

```bash
vllm serve /opt/ks-models/GLM-5.1-w8a8c8-MTP \
  --host 0.0.0.0 \
  --port 18100 \
  --tensor-parallel-size 8 \
  --pipeline-parallel-size 2 \
  --enable-expert-parallel \
  --distributed-executor-backend mp \
  --master-addr 127.0.0.1 \
  --master-port 18190 \
  --nnodes 1 \
  --node-rank 0 \
  --hf-overrides '{"use_index_cache":true,"index_topk_freq":4}' \
  --speculative-config '{"num_speculative_tokens":3,"method":"deepseek_mtp","enforce_eager":true}' \
  --seed 1024 \
  --served-model-name glm-5 \
  --max-model-len 49152 \
  --additional-config '{"fuse_muls_add":true,"recompute_scheduler_enable":false,"multistream_overlap_shared_expert":true,"enable_dsa_cp":true,"enable_sparse_sfa_c8":true,"enable_sparse_li_c8":true,"c8_enable_reshape_optim":true}' \
  --max-num-batched-tokens 16384 \
  --max-num-seqs 64 \
  --trust-remote-code \
  --no-enable-prefix-caching \
  --quantization ascend \
  --gpu-memory-utilization 0.94 \
  --enforce-eager \
  --enable-auto-tool-choice \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --kv-transfer-config '{"kv_connector":"MultiConnector","kv_role":"kv_producer","kv_connector_extra_config":{"ks_local_p_complete":false,"connectors":[{"kv_connector":"LMCacheMPConnector","kv_role":"kv_both","kv_connector_module_path":"lmcache.integration.vllm.lmcache_mp_connector","kv_connector_extra_config":{"lmcache.mp.host":"tcp://127.0.0.1","lmcache.mp.port":5565,"lmcache.mp.autostart":false,"lmcache.mp.nonblocking_lookup_status":false}}],"ks_local_p_first_token_only":true}}'
```

实际cold的bench完整入口：

```bash
vllm bench serve --backend openai --base-url http://127.0.0.1:18100 --endpoint /v1/completions --model glm-5 --tokenizer /opt/ks-models/GLM-5.1-w8a8c8-MTP --dataset-name custom --dataset-path /opt/ks-stage-ready-luv-20261027/standalone-p/data/first-token-original.jsonl --custom-output-len 1 --disable-shuffle --skip-chat-template --no-oversample --num-prompts 1 --max-concurrency 1 --request-rate inf --num-warmups 0 --ready-check-timeout-sec 0 --temperature 0 --ignore-eos --seed 0 --percentile-metrics ttft,tpot,itl,e2el --metric-percentiles 50,90,95,99 --save-result --save-detailed --result-dir /opt/ks-stage-ready-luv-20261027/standalone-p/results/first-token/original-async-cold --result-filename benchmark.json
```

阶段候选另需组合统一loader并加以下flag；已运行原异步cold组没有这些flag：

```bash
export KS_PP_STAGE_DRIVER=1
export KS_PP_STAGE_EVENTS=1
export KS_CORE_STAGE_EVENT_PUMP=1
export KS_LMCACHE_STAGE_OBSERVER=1
```

加载映射见`executor_driver_loader.json`，总体清单见`unified_overlay_manifest.json`。runtime_overlay须在sys.path；canonical映射core/executor/scheduler/Ascend worker及LMCache。共同model_runner_local和pin保持原文件；Connector映射重基common17的通知版。observer及身份类内联于actual137 adapter，保持唯一类型。不能混入旧完整adapter或用多个sitecustomize互相遮盖。**统一包import尚未验收，此处不虚构已可运行的最终阶段服务入口。**

14模块依赖闭包已核对：12条自定义导入全部有映射，core使用`span_stage_controller`，binding没有未打包helper；`ks_stage_controller`或`ks_stage_observer`旧模块均不再是运行依赖。137上用私有venv的`python -B -S`执行已部署finder的真实spec/source，依次加载typed protocol、span controller、executor driver、reservation、binding、metadata bridge与restore runtime，共7个真实helper成功。没有注入stub或fixture，也没有导入vLLM/LMCache父包、torch/torch_npu/Ascend或调用设备函数。这补足了finder路径检查，仍不替代框架父包及完整14模块正常import；不能保证后者无插件设备初始化，因此本次未执行。

可选KS_STAGE_TRACE_DIR写每进程JSONL，含request/generation、batch/stage、front/all KV ready、提交、worker开始、设备完成观测、sample解析、Core消费。CPU submit/host observed明确标注，不冒称纯设备耗时。性能比较须两组等价观测或单列开销。

## CPU复现与未验证缺口

本机PowerShell，在本文件所在目录：

```powershell
python -X utf8 .\build_executor_driver_overlay.py
python -X utf8 .\check_executor_driver_wire_cpu.py
python -X utf8 .\check_integrated_first_token.py
python -X utf8 .\check_import_dependency_closure.py --remote-safe-helpers
```

builder只生成本任务core/executor/worker和直接helper，不覆盖其他worker的scheduler/LMCache输出、不改安装文件、不加载模型。实际Scheduler/Connector/observer重基有各自构建器及统一manifest记录。已取得9项wire/1项组合，不为报告重跑旧126项。

剩余必要验证：

- 统一包完整import与canonical来源核对。
- 新共同skip配置和阶段候选各自的真实首token、异步STORE结束、请求清理、后续请求及正常退出。
- 真实restore事件与前后段设备执行先后，未知DMA错误的保留路径。
- HCCL超时两端helper当场rank/num_reqs/discard mask/skip flag/广播序号，不能按异步报错栈猜发起点。
- 连续MTP与跨batch流水仍未支持，不在本次max1机制验收范围。

已排除：actual Core零token批只取execute Future、不sample；actual137 Worker原有forward_pass=total>0保护后段activation接收；kv_connector_no_forward/finalize无PP采样广播。单独调用sample(None)的反事实夹具不是现场复现。缺口补齐前，不宣布候选整链通过、warm数据恢复错误、连续MTP支持或加速。

