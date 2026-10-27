# warm1失败：已有证据与共同首token修正候选

只读日志确认服务内部退出，不能归类为“正常成功但特殊token没有文本”。最早记录的worker错误是HcclBroadcast连接超时，前段device1／3／5／7分别涉及对应后段9／11／13／15；API随后关闭。异步错误在后续`execute_model → synchronize_input_prep → prepare_inputs_event.record`处暴露，栈位置不是通信发起点证明。

特别是fatal dump的zero-token输出仍携带cold请求`cmpl-bench-2a3cdae4-0-0-b8cc7828`的finished_req_ids，而不是warm的bench-b4f6efa1。日志在warm前已出现60秒无可用shared-memory广播块；缓存日志仅有cold STORE，没有warm Retrieved完成。因此当前不能说“warm KV恢复本身报错”，也不能用设备Alarm解释该广播超时。

已核除两条猜测：实际Core的zero-token批使用execute Future，不发送sample；137原Worker本来就用`forward_pass = total_num_scheduled_tokens > 0`保护PP activation接收。KV-only与finalize路径也没有PP采样广播。不能拿单独调用sample(None)的反事实夹具当作现场复现。

当前共同runner为了早期“完整本地decode”方案恢复了普通PP sampled-token反馈。这条新反馈在严格max_tokens=1机制诊断中没有必要：末段向Core/API返回真实首token后请求结束，前段不会执行下一轮依赖新sample/draft的decode。原PD P节点正是保留producer skip。

因此准备了更窄的共同配置候选，两组都相同：

```diff
- "ks_local_p_complete": true
+ "ks_local_p_complete": false
+ "ks_local_p_first_token_only": true
```

末段仍执行真实sample、MTP3 proposal和KV finalize；LMCache17、pin、外producer／子both、PP计算和activation传输均不变。新标记用于说明执行范围；专用客户端仍须明确max_tokens=1。没有添加HTTP middleware或通用请求拦截器。

`check_first_token_no_feedback.py`提取实际private `sample_tokens`，只验证actual async=True下前段、后段两个控制流：不调用新增采样broadcast/receive，末段sample/draft fixture值及KV finalize保持。两项CPU通过，见`first_token_no_feedback_cpu.json`。这证明该候选消除了新增采样反馈路径，不证明它就是现场超时的唯一原因，也不是NPU运行成功证据。

本机两份native已反映该候选；服务器此前生效的旧启动记录保留，是否采用和重启由主任务统一执行。本子任务没有启动、停止或重启服务。连续输出超过1token仍未支持。
