# 受控就绪窗口：DIAGNOSTIC_ONLY

用途是在真实KV恢复基础上创造一个可观测的前段先准入窗口，补查真实stage worker是否会执行。主任务自然热请求中后段先ready约29.2ms，随后全局恢复完成并走回原路径；该事实不能证明分段执行失败，也尚未覆盖front-only admission。本诊断只延迟后段成功状态的CPU公开时间，不改变KV、真实设备Future或业务输入，不用于性能比较。

初始本机阶段仅生成隔离源副本、patch和同源AST CPU回执。主任务随后授权私有部署准备，已新增独立诊断源／launcher与三份诊断启动材料；没有改原runtime、安装源、当前P1505582、D、cache或proxy，没有启停或发请求。原源文件的绝对路径见 `source_provenance.json`，部署准备回执见 `private_preparation_deployment.json`。

## 拦截位置与行为

当前owner源码 `vllm_multi_process_adapter.py` 的 `_Operation:1333`、`LMCacheStageObserver.poll_once:1427`。真实 `real.result(timeout=0):1441` 与bool类型检查之后，先保存真实结果／真实完成观测时刻／公开截止时刻；在原 `success_outbox.append:1451`、`proxy.set_result:1452`、删除entry:1457之前拦截。

默认 `KS_LMCACHE_DIAGNOSTIC_BACK_READY_DELAY_MS=0`。仅stage_id=1、真实结果True、未取消、非关闭时设一次截止时刻；建议500ms。窗口内保持proxy pending、outbox为空、entry及producer event／real Future引用仍在，不向真实Future赋值，不先向proxy写False再改True。owner继续处理其他操作，无sleep/wait；真实getter只执行一次。到期在原锁内共同公开proxy与outbox。前段、False、异常走即时原语义；cancel/close只绕过已经设备完成的人工公开延迟，不制造未完成请求的完成，不产生迟到成功通知。

legacy `get_finished` 在原2352／2484先query本CPU proxy，pending时不会进入result；因此本改动不加入阻塞式等待。独立publisher原62—66行从outbox排出身份并发布，仍保留真实rank/generation。公开后两个消费线程的先后顺序并无硬保证；应验收front实际准入是否发生在back公开之前，而非假定独立通知总比legacy先到。

## CPU复现

```powershell
python -X utf8 "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\受控就绪窗口诊断\prepare_diagnostic_only.py"
```

该命令从当前owner源生成本目录副本和review patch，再提取同一副本的 `StageRestoreIdentity`、`_Operation`、`LMCacheStageObserver` 实际AST执行；没有重实现observer。Future、CPU proxy和单调时钟是明确fixture，不加载torch／lmcache／CANN，不访问NPU。11项通过见 `cpu_result.json`：默认关闭、front即时、back真完成前不公开、完成后固定窗口／到期、getter一次与引用持有、后段等待不阻塞新front、False／异常即时、取消与关闭不延迟终态／不发迟到成功，以及owner poll没有sleep/wait。

## 验收与边界

如主任务后续决定启用，应使用单独诊断tag／结果目录并显式记录500ms设置，不替换任何自然cold/warm/warm2。至少关联同一request/rank/generation的真实完成、截止、front admission、实际stage0 worker execute、stage1公开及实际stage1 execute；确认stage1执行没有早于其公开就绪。0.5s未必覆盖调度线程的全部阻塞，若窗口内仍无front admission应记录未触发，不把它称为模型错误或性能结论。

back设备恢复在计时起点已经真实完成，所以本诊断可验证真实stage计算与通知／准入链路，不能证明前段计算与后段DMA重叠，更不能拿增加的人工等待计算收益。CPU结果只验证状态机和生命周期，真实Future、零token RPC、完整GLM/PD及性能均未由本目录运行验收。

## 已完成的私有部署准备

入口为 `python -X utf8 deploy_private_preparation.py`。仅新增远端 `/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/diagnostic-back-ready` 内的诊断adapter和独立sitecustomize，以及startup中的 `p-stage-diagnostic.env.native.sh`、`p-stage-diagnostic.serve.native.sh`、`bench-stage-diagnostic-warm.native.sh`。从实际当前loader快照生成，远端逐项spec检查19→19仅改变 `lmcache.integration.vllm.vllm_multi_process_adapter` 一项；Mooncake／draft两个optional映射与所有其他路径保持。新增三份shell均bash-n通过，不导入框架／设备包。

env仅相对candidate增加500ms标记、`stage-diagnostic-trace`和PYTHONPATH首诊断launcher；serve参数完全沿用candidate，benchmark仅改结果tag／目录为 `stage-diagnostic-warm`。私有文件准备成功不表示已启用或已完成设备诊断；后续P切换、请求与实际trace由主任务执行。
