# PP分段worker接口：隔离候选

状态：**CPU协议验证通过，NOT_RUNTIME_INTEGRATED。** 未部署到服务器，未导入torch／vLLM／torch-npu，未占卡、未启停任何服务。这里的完整worker文件基于项目归档的真实Ascend worker生成，不能将通过CPU测试称为已能启动原模型或已有性能收益。

## 改了什么

原入口`execute_model`仍保留原来的行为。新入口把后段的接收动作从模型计算中单独拿出来：后段可以先接收并保存前段的中间结果，等后段自身KV也准备好后，再实际执行后37层。前段仍计算前41层，不改变层数、TP／PP／DP、MTP或模型内容。

| 接口 | 做什么 | 必须由上层保证什么 |
|---|---|---|
| `prepare_pp_stage_input(batch_key, scheduler_output)` | 后段只接收和保存中间结果，不执行模型 | 必须以非阻塞RPC排入；元数据接收可能等待前段，不能先同步等待本接口返回才下发前段 |
| `pp_stage_input_received(batch_key)` | 只查询本worker通信句柄完成状态 | 要收齐整个TP段的确认，不能只看其中一个rank |
| `finalize_pp_stage_input(batch_key)` | 建立通信到设备stream的依赖，执行可能包含TP all-gather的后处理，记录设备事件 | 整个TP段都已received后，一起调用；不可从某一个rank的轮询中擅自调用 |
| `pp_stage_input_ready(batch_key)` | 只查询后处理设备事件，未知状态返回False | 整个TP段都ready才允许后段计算 |
| `execute_pp_stage_model(batch_key, scheduler_output)` | 只执行当前PP段一次，后段复用已准备的输入，避免再次接收 | 上层已证明本段每个必要worker的LMCache恢复真正完成；本接口不替代LMCache完成事件 |
| `pp_stage_model_ready(batch_key)` | 查询模型设备事件 | 方法已返回不等于该事件已完成，更不等于整个模型批已完成 |
| `pp_stage_release_ready(batch_key, final_output_consumed=False)` | 非破坏性查询释放条件，普通未完成返回False；需要时建立发送stream依赖并记录设备事件 | 轮询收齐整个TP段的True，期间仍保留全部tensor；不能把普通未完成当RPC异常 |
| `release_pp_stage_batch(batch_key, final_output_consumed=False)` | 安全释放本段缓冲所有权 | 前段发送的句柄与stream事件完成；后段必须等采样状态已消费且核心处理最终结果后传True |

`batch_key`统一为调用方生成的`tuple[str, int]`，例如`("逻辑批1", 0)`：第一项为非空批次ID，第二项为非负整数代次，拒绝bool、负数和其他类型。每个逻辑批唯一，不能用可复用的请求ID替代。候选同时只允许每stage拥有一个缓冲，避免在尚未实现并行资源管理时覆盖另一个批次。这个限制是正确性原型的边界，不能据此声称最终吞吐会提高。

## 为何接收完成还要分两步

现有`irecv_tensor_dict`除了返回异步通信句柄，还返回后处理函数；后处理可能调用TP all-gather。如果某个worker先完成就直接做all-gather，而同段其他worker还没走到这里，可能把自己的控制通路阻塞住。因此先收齐通信完成，再一起后处理，最后检查设备事件。

源码依据（均为既有归档，非实时服务器版本）：

- `capacity_followup/sources/m8/vllm_ascend/worker/worker.py`：原613—679行，接收、runner执行和发送原本集中在同一个入口。
- `source_snapshots_extra/current-callchain/vllm-workspace/vllm/vllm/distributed/parallel_state.py`：原73—78行的Handle协议提供`is_completed`和`wait`；原964—1026行先同步发送元数据再发异步tensor；原1059—1164行先同步接收元数据，再返回异步句柄及TP后处理。
- 同一快照的`v1/worker/gpu_worker.py`：原85—117行，`AsyncIntermediateTensors.wait_for_comm`先等待句柄，再执行后处理。
- `capacity_followup/sources/m8/vllm_ascend/worker/model_runner_v1.py`：原1973—1974行保留execute→sample规则；原2462行在采样工作前清空暂存状态，因此仅看到状态为空不足以证明采样输出已交给核心。

## CPU验证

在本机PowerShell执行；不会连接服务器或调用NPU：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\worker_split\test_worker_split.py"
```

2026-10-27实际结果：28项通过。测试从生成的真实worker文件中用AST提取候选方法与原`execute_model`，替换设备与通信对象后执行。先在原worker上观察到新增接口缺失的失败，再生成候选并验证；组合对接后先新增并观察到tuple批身份被拒、释放探测接口缺失、前段尚未消费Connector输出却被允许释放的失败，再修正接口后重跑。

验证覆盖：只接收不forward；未知完成状态不放行；同批重入不重复接收；另一批不能覆盖缓冲；请求成员/token/块信息不匹配时拒绝；tuple身份与代次类型严格校验；后处理前不能消费输入；设备事件未完成不能forward或释放；保持execute→sample规则；发送未完成不进入原来的阻塞wait；发送源tensor在通信结束前被保留；普通释放等待不抛异常、不释放tensor；末段最终结果未被核心处理不能释放；老入口仍执行原接收路径；候选路径和老路径不能混用；后处理异常不能当成功重试。

生成隔离候选：

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\worker_split\build_worker_split.py"
```

## 还没有验证什么

- **NOT_VERIFIED**：当前服务器版本的HCCL Work对象完成查询语义，`wait`建立的stream依赖，以及NPU event在实际通信后处理上的顺序。
- **NOT_VERIFIED**：TP8／EP／DSA CP真实算子、图回放、MTP3与Mooncake在新派发顺序下的正确性和性能。
- **NOT_RUNTIME_INTEGRATED**：调度核心、调度器和LMCache真实阶段完成事件尚未接到这里。仅替换worker文件不会启用分段调度，也不会自动缩短通知等待。
- 需要上层补齐阶段资源、请求取消／KV读取失败的全段协调退出、逻辑批顺序和最终输出处理。候选对失败记录保持拒绝重试；没有擅自清理设备缓冲或继续计算。
- 本目录没有模型启动命令，因为候选还不具备独立`vllm serve`启用路径。不能把任意自创参数写成已经可用。

## 文件

- `worker.py`：隔离完整worker候选。
- `worker_split.patch`：相对明确归档基线的diff，不自动应用。
- `build_worker_split.py`：可复现生成器，检查已评审基线结构后才生成。
- `test_worker_split.py`：28项CPU控制流测试入口。

未修改项目现有运行源码、依赖、启动参数、容器或服务。
