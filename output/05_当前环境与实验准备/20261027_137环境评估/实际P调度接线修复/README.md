# 实际P调度接线修复：CPU_ONLY_NOT_DEPLOYED

本目录只在本地生成候选和CPU证据。没有SSH、部署、修改当前server runtime、启停服务、发请求或使用NPU。当前服务器仍是此前诊断旧版本，不能把本目录结果写成已启用或设备验收通过。

## 根因与最小diff

基线是主任务从137实际取回的 `受控就绪窗口诊断/actual137_patch_balance_schedule.py`，本目录保存逐字源码副本。该0.23版本的 `BalanceScheduler.schedule:107` 在P producer禁抢占条件为True时使用自己的完整schedule，不调用基础 `Scheduler.schedule`。它会继承候选的真实partial promotion，却遗漏基础schedule已有的两个接线点：原483行slot allocation仍只用 `delay_cache_blocks=load_kv_async`；原637行更新/返回输出前没有 `attach_output`。

候选 `patch_balance_schedule_fixed.py`仅三处改动：导入现有helper；slot参数改为 `load_kv_async or helper.delay_cache_blocks(self, request_id)`；在实际创建connector metadata之后、`_update_after_schedule`之前调用 `helper.attach_output(self,scheduler_output)`。原禁抢占分支、运行／等待队列、负载均衡、connector metadata及其余方法均保持；AST检查只有 `BalanceScheduler.schedule` 方法发生变化。review见 `balance_scheduler_wiring.patch`。

## 实际函数CPU验证

```powershell
python -X utf8 "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\实际P调度接线修复\prepare_balance_candidate.py"
python -X utf8 "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\实际P调度接线修复\check_actual_balance_schedule.py"
```

只复用已有 `check_scheduler_reservation.py` 的fixture初始化，不运行旧8/11套件。提取实际Balance.schedule作为真实Scheduler方法子类，走继承的实际 `_try_promote_blocked_waiting_request`、真实helper `try_partial_promotion`，由实际schedule创建真实 `NewRequestData`／`SchedulerOutput`，再由真实helper标记；没有手工创建预约或预标记output。request、KV allocator、controller readiness和connector为明确CPU metadata fixture，完整服务初始化／实际KV容量与NPU不在本次范围。

`cpu_result.json`六项通过。先执行未修改实际函数，真实预约r/gen4及pos511/1token已产生，但allocator.delay=False且没有tag，目标断言按预期失败；再执行修复实际函数，allocator.delay=True、真实output含正确request/gen/position tag，KV不被当作全局恢复完成。另覆盖默认关闭旧／新输出状态一致、真正global completion走原cache且不加tag、分配失败时官方禁抢占分支保留运行请求，以及balance queue节流仍有效。首次写JSON回执遇到fixture Blocks不能序列化，只修正回执序列化后重跑本新检查通过，没有修改调度fixture或构造成功结果。

## 私有exact loader候选，尚未部署

```powershell
python -X utf8 "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\实际P调度接线修复\prepare_loader_candidate.py"
```

`sitecustomize_balance_CANDIDATE.py`／`loader_exact_map_candidate.patch`只在原exact finder里增加canonical `vllm_ascend.patch.platform.patch_balance_schedule`。所指 `/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/balance-scheduler-fix/patch_balance_schedule_fixed.py`是将来私有准备的建议路径，本次没有创建或上传。其余module映射、父包正常搜索及原有两个N16 optional行为保留，不将框架包目录放入PYTHONPATH。若后续以受控诊断19-map为基线，应保留其observer诊断映射，再同样增加Balance映射。

基准17个模块名来自实际部署回执 `stage_driver/private_candidate_deployment.json` 的真实 `origins`，不是合成名字；本目录保存其17项快照。CPU只执行实际loader class AST与新增assignment，使用POSIX路径语义fixture，不声称远端文件已存在或真实导入框架。`loader_cpu_result.json`验证：没有两个N16 optional时17→18；只一个optional时18→19；两个optional同时存在时19→20。所有原映射逐项不变，只有新增Balance exact map，父包find_spec保持None。

本次只补实际P调度接线。新的20-map候选尚未完整导入、未进入真实EngineCore／Worker／NPU；不能据CPU通过推导真实分段执行成功、KV内容正确或性能收益。
