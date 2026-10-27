# N16 target配原78层MTP：实际构造与权重路由CPU核对

已确认配置传递错误，不是原权重文件缺少MTP：实际父类`vllm/v1/spec_decode/llm_base_proposer.py:1157`的`_create_draft_vllm_config`只从target的VllmConfig复制kernel/attention字段，未替换model_config。Ascend `_get_model`另传draft_model_config到loader，但`model_loader/utils.py:40`的`initialize_model`对新式构造函数直接传完整vllm_config；模型类别可按draft选择，构造参数仍是target16。

`deepseek_mtp.py:129`的Predictor和207的MTP均直接读`vllm_config.model_config.hf_config`，所以构造层16；`BaseModelLoader.load_model:55`不会替换该字段。实际源快照为上级目录`actual_parent_proposer_and_loader.txt`、`extra_model_loader_utils.py`、`model_sources_deepseek_mtp.py`及`pd_actual_patch_deepseek_mtp.py`。

最小私有修改已由prepare worker放在`reduced-pd-prepared/runtime/draft-n16/llm_base_proposer_n16.py`：只在method=mtp、target类型glm_moe_dsa、target16、draft78、nextn1时，取得原draft_vllm_config后执行：

```python
draft_vllm_config = copy.copy(draft_vllm_config)
draft_vllm_config.model_config = self.speculative_config.draft_model_config
```

使用浅copy保留parallel/compilation/quant_config及注册对象，不触发dataclasses.replace可能重跑的配置post-init；target对象不变。原78场景guard不触发，构造配置选择保持原行为。spec.quantization=ascend是另一个必要配置项，不能代替以上构造修复，也不能靠关闭权重追踪绕过未加载。

## CPU结果

执行实际父类方法、prepare最终私有_get_model、BaseLoader.load_model、initialize_model、Predictor/MTP构造、Ascend名称重写、MTP.load_weights和DefaultLoader严格检查。4个场景通过，回执为上级`draft_actual_ctor_weight_cpu.json`：

| 场景 | 实际构造层 | CPU参数写入 | 严格检查 |
|---|---:|---|---|
| target16/draft78，原方法、quant=None | 16 | 仅rot写入16，其余6项未写 | 实际检查报缺失 |
| target16/draft78，只补quant=ascend | 16 | 同样只写rot，其余6项未写 | 默认不检查，可能不报错，不能当成功 |
| target16/draft78，私有copy修复 | 78 | 7项全部写入正确目标 | 即使强制True也通过 |
| 原target78/draft78，私有guard保留 | 78 | 7项全部写入正确目标 | 强制True通过，model_config仍原对象 |

输入名称来自已经只读提取的`shard_closure_metadata.json`中6个真实特殊权重名称，加原始`rot.weight`。实际load_weights路由得到：

| 原输入名称 | 实际写入参数 |
|---|---|
| model.layers.78.enorm.weight | model.layers.78.enorm.weight |
| model.layers.78.hnorm.weight | model.layers.78.hnorm.weight |
| model.layers.78.shared_head.norm.weight | model.layers.78.shared_head.norm.weight |
| model.layers.78.shared_head.head.weight | model.layers.78.shared_head.head.weight |
| model.layers.78.eh_proj.weight | model.layers.78.eh_proj.weight |
| model.layers.78.embed_tokens.weight | model.embed_tokens.weight |
| rot.weight | model.layers.78.rot.weight |

这不是仅检查“missing异常消失”：测试记录每次实际weight_loader写入的源标记与目标参数，并检查没有未写的7项参数。错误构造时rot特殊规则仍能重写到16，足以让MTP按“该层至少有一项loaded”检查通过；因此仅quant关闭默认严格追踪尤其可能掩盖问题。

本次仍是CPU：配置容器、nn.Module/Parameter、权重值、设备上下文和量化后处理使用fixture；权重值为源名称标记，没有读取checkpoint tensor字节，也未验证MoE/expert、FP8、真实数值、NPU构造或完整模型加载。原始权重与安装源码均未修改，无GPU或服务操作。

本机PowerShell入口：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\完整模型单机P对照\cache-clear-and-reduced-model\mtp_config_construction\check_actual_draft_ctor_weights.py"
```
