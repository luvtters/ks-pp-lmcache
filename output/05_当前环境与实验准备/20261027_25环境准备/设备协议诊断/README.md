# PP2／TP8设备协议诊断

状态：**本地实时日志补充完成，这一版尚未部署或上卡。** 主任务回报旧任务`20261027-115832`按暂停要求退出143，不能据此判新设计失败。本次修改没有SSH访问25、占卡、清退、服务启停或部署动作；未修改旧执行记录及原始日志。

## 本轮只补即时进度，不改设备控制

原来`mark`只把事件暂存在Python列表，等最终`finally`才写结果；若启动阶段收到SIGTERM，可能没有留下阶段边界。现在每次`mark`都会：

1. 向`rank-xx.events.jsonl`追加一条JSON并立即flush。
2. 向标准输出打印同一条JSON并立即flush。
3. 继续保留原来的内存事件列表与最终结果JSON。

每条事件含rank、PID、序号、UTC时间与单调时钟。即使最终结果JSON没有生成，也可以读取SIGTERM前已经写入的事件。若重复使用同一个结果目录，事件文件会继续追加，需结合PID和时间区分任务；主任务可沿用独立任务目录。

新增启动观测点：

| 阶段 | 进入事件 | 返回事件 |
|---|---|---|
| 脚本进入设备路径 | `probe_enter` | 记录事件文件位置 |
| torch导入 | `torch_import_enter` | `torch_import_returned` |
| torch_npu导入 | `torch_npu_import_enter` | `torch_npu_import_returned` |
| torch.distributed导入 | `torch_distributed_import_enter` | `torch_distributed_import_returned` |
| vLLM配置与并行模块导入 | `vllm_import_enter` | `vllm_import_returned` |
| 选择本进程设备 | `set_device_enter` | `set_device_returned` |
| 配置构造（包含ParallelConfig和VllmConfig） | `config_construction_enter` | `config_construction_returned` |
| 分布式环境初始化 | `init_distributed_environment_enter` | `init_distributed_environment_returned` |
| TP／PP分组初始化 | `initialize_model_parallel_enter` | `initialize_model_parallel_returned` |

原有发送、接收、Work查询、等待、设备围栏、TP后处理等关键动作也改为即时输出。只有进入而没有返回，说明尚未观察到这个区间结束；不能仅凭这点把原因定为rendezvous、HCCL或某个内部函数。torch导入还可能包含后端自动加载，两个导入区间不能简单当作互不重叠的纯CPU模块耗时。

**日志文件写入、flush与标准输出都是新增诊断开销，不能当作原业务或设备性能。** 本轮没有改变HCCL后端、TP／PP、载荷、rendezvous地址、超时或原来的执行控制。

CPU静态验收：语法通过；`--cpu-plan`未新增任何torch／torch_npu／vLLM导入，输出计划不变；26个配置／设备／通信调用的AST实参保持不变。回执见`cpu_check.json`。这不是设备执行或新设计正确性验收。

## 这次检查什么

使用16个真实NPU进程和vLLM的TP8／PP2分组。前8个进程仅做一次小张量加法作为占位计算，通过真实`isend_tensor_dict`发送；后8个进程只接收、做原接口要求的TP重构，再验证内容。

每个前段进程的完整载荷只有1KiB，另有32个float的本地KV占位张量。**这些不是GLM中间结果，不是LMCache实际恢复出来的KV。** 不运行模型、MoE、DSA CP、MTP、Mooncake或benchmark，不能从本检查推导TTFT／TPOT、端到端性能或提速。

1KiB只是应用载荷大小，**不是总显存开销**。NPU上下文、框架和HCCL通信缓冲仍会另外分配显存；实际总量必须在设备执行时观察，本地CPU检查不估算该数字。

按本次TP重构路径，每条PP通道实际发送的是完整张量的1/8切片，预期张量数据128字节；1KiB不应写成每条通道的线上传输字节数，CPU元数据与通信协议开销另计。

记录四种不同的时刻：

1. 发送／接收的Python方法返回。
2. 通信Work对象的`is_completed()`查询及整段TP共同确认。
3. `Work.wait()`返回后，当前设备stream上另记事件并确认完成。
4. 后段TP重构方法返回后，另一个设备事件真正完成。

探测Work能否在`wait`之前独立返回完成状态时，最多进行250ms的有界CPU共识轮询，不使用sleep。**这段轮询是诊断额外开销，不能当作业务耗时或人为制造的劣化。** 若查询未在窗口内完成，脚本仍调用原语义要求的`wait`来校验最终传输；仅如实记录`nonblocking_query_consensus=false`，不能宣称该接口永远不能完成。

接收后处理可能包含TP all-gather，因此只在本段所有进程完成通信依赖后共同执行。记录和判断不会把方法返回当作设备完成。前段源张量在发送完成之前持续保留。

## 已核对的源码接口

- `parallel_state.py:1516`：`init_distributed_environment(world_size, rank, distributed_init_method, local_rank, backend, timeout)`。
- `parallel_state.py:1674`：`initialize_model_parallel(tensor_model_parallel_size, pipeline_model_parallel_size, prefill_context_model_parallel_size, decode_context_model_parallel_size, backend)`。
- `parallel_state.py:964`、1059：真实发送、接收、通信句柄和后处理返回。
- 25当前`vllm/config/vllm.py:295—310`：VllmConfig的model_config默认None、parallel_config与scheduler_config均有构造入口。
- 25当前`vllm/config/parallel.py:109`起：TP、PP和`distributed_executor_backend`配置。

当前两个config文件由主任务从本项目25 CPU容器取回，位于本目录外的`../25镜像源码与配置快照/sources/vllm/vllm/config/`。parallel_state完整归档位于：

`E:\work\ks\output\03_性能劣化原因分析\03_真机观测与原始日志\首轮TTFT观测与原始证据_20261019\source_snapshots_extra\current-callchain\vllm-workspace\vllm\vllm\distributed\parallel_state.py`

初始化前显式建立`set_current_vllm_config`上下文；预检HCCL可用，初始化后再确认实际设备组后端仍是HCCL、分组设备确实为NPU。拒绝静默回退到Gloo设备后端。CPU元数据和同段共识通信仍按原vLLM接口走Gloo，这是原机制的一部分。

`source_reference/`只用于本地接口审查，不是运行依赖，不必上传。它来自本项目已归档的旧baseline私有运行包；与25当前config接口已作直接源码核对，没有把旧快照冒充运行时执行结果。

## 本机CPU检查

下面只输出计划，不导入torch／vLLM，不占卡：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_25环境准备\设备协议诊断\pp_stage_device_probe.py" --cpu-plan
```

## 设备任务原生命令

机器：25；容器：由主任务建立的本项目NPU诊断容器；私有目录：`/opt/ks-stage-ready/device-probe`。以下是**待执行命令**，必须先完成真实驱动和设备接入，并核实占卡窗口，不可在CPU准备容器中尝试占卡。

```bash
export ASCEND_RT_VISIBLE_DEVICES=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
export HCCL_IF_IP=192.168.0.122
export GLOO_SOCKET_IFNAME=enp23s0f3
export TP_SOCKET_IFNAME=enp23s0f3
export HCCL_SOCKET_IFNAME=enp23s0f3
export OMP_NUM_THREADS=1

torchrun --nnodes=1 --node-rank=0 --nproc-per-node=16 \
  --rdzv-backend=static --master-addr=127.0.0.1 --master-port=29627 --max-restarts=0 \
  /opt/ks-stage-ready/device-probe/pp_stage_device_probe.py \
  --run-device-probe \
  --output-dir /opt/ks-stage-ready/device-probe/results
```

这不是vLLM模型服务，所以没有伪造`vllm serve`入口。脚本本身要求显式`--run-device-probe`才导入框架；仅支持16个单机进程，不会遇到缺卡就悄悄改TP或PP。

每rank写一个小JSON，记录事件、真实组成员、Work查询和数据校验。正常结束销毁通信组；失败写入异常和调用栈，由torchrun的失败处理终止本次诊断worker，最大重启次数为0。不因此停止任何已存在的模型、缓存、代理或容器。

## 验收边界

- `PASS_DEVICE_TRANSFER_ONLY`只证明这组小载荷的真实传输、TP重构和设备围栏检查通过，不代表新阶段调度方案通过。
- `nonblocking_query_consensus=false`本身不是传输失败；需要结合Work.wait后的结果判断查询路径如何使用。
- Work未在wait前取得全段完成共识时，`nonblocking_readiness_contract`明确记录`NOT_ESTABLISHED_WITHIN_QUERY_WINDOW`，不能把后续wait成功改写为新候选的非阻塞就绪路径已通过。
- 不能从这次诊断宣称真实LMCache已分段报告完成，也不能宣称核心／scheduler已接入新设计。
- 仍需完整框架和真实请求验证，才能判断前段先开始是否缩短最终首token，以及是否影响吞吐或采样顺序。
- 初稿时仅作CPU检查；主任务后续已执行两份设备诊断，实际结果单独保存于`本轮设备验证结论.md`及`执行记录/`，不回填为本地CPU检查结果。
