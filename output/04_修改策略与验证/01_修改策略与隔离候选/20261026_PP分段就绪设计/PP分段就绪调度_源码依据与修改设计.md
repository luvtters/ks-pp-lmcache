# PP分段就绪调度：修改讲解与验证结果

## 先把结论讲清楚

我们想改的，不是“把还没恢复好的 KV 当作好了”，而是**不让已经具备条件的前段，继续等待后段的 KV 或另一模型批的结果**。

原程序有两个限制：**KV 完成信息与模型批结果的返回耦合；调度器按整条请求放行，没有“只安排已就绪前段”的执行安排。**这版分别处理完成通知和分段执行资格。

**最新结果：真实设备完成事件可以独立进入核心，并在旧模型结果尚未返回时登记阶段就绪；前段先执行、后段按真实依赖再执行的小张量设备协议也已通过。完整 GLM 推理链还没接通，没有新的 TTFT、TPOT 或提速结果。**

| 现在可以准确说什么 | 依据 |
|---|---|
| 原调度器要求整请求恢复完成，不能只凭前段 KV 就绪放行 | 真实源码确认 |
| 新核心在等待模型结果时能处理独立事件，不提前更新模型结果 | CPU 验证＋真实 NPU 事件到核心方法的组件验证 |
| 前段先具备条件可以执行；后段仍等自己的 KV 与中间结果 | 16 卡真实通信、小张量与原型控制器验证 |
| 使用这版候选的 GLM 已运行，原客户用例已有提速 | **NOT_VERIFIED**，不能这样汇报；不否定历史旧方案的实测 |

一句话汇报：**已经验证了“完成通知不必等模型批结束”和“前后段分别按真实依赖放行”两个关键机制；仍需接入真实请求、调度器和模型 worker，才能确认整体性能收益。**

## 从两条请求看：究竟少等哪一步

固定两条请求：**正在计算的请求**已经被安排到模型批；**缓存命中的请求**正在恢复历史 KV，恢复以后仍要计算剩余 token。前者也可能命中过缓存，这里只代表尚未结束的模型工作。

核心调用调度器形成安排，执行器把安排交给 worker。前段负责前 41 层，后段负责后 37 层，各段使用自己的 KV。LMCache 恢复的是大块历史 KV；调度核心只接收状态，不接收这些大块数据。

### 原程序怎样走一圈

1. **核心调用调度器。**缓存命中的请求若需异步恢复，就进入“等远端 KV”状态，本轮不安排其剩余 token 计算。正在计算的请求照常获得执行安排。
2. **执行器下发安排。**前段算前 41 层，将中间计算结果交给后段。这个传递属于同一执行安排，不必每过一段重新找调度器批准。
3. **各 worker 分别恢复自己的 KV。**恢复完成信息经模型输出路径汇总。一个前段 worker 返回了确认，不代表整个前后段都恢复完成。
4. **核心等待本批真实结果。**缓存请求可能已在设备侧具备条件，但核心尚未处理到足够的完成确认。后段的计算、采样及原结果顺序，都可能影响这条返回路径。
5. **结果返回后更新调度器。**确认齐全的请求变回可调度，下一轮才有机会安排它的剩余 token 计算，依次通过前后段。

### 修改后的目标怎样走

设备真实完成 → 独立通知核心 → 登记哪一段就绪 → 有合法批次和资源时只派发该段。后段尚不能计算时，先接收并保留前段中间结果；等自己的 KV 与中间结果均齐全，再计算后 37 层。

**恢复和计算属于同一请求的两个动作，不是两个请求，也不固定占两个模型批。**异步恢复后何时进入计算，取决于通知、调度与资源。前后段可以处理不同在途批；同一批的后段仍依赖前段结果。“完成消息在等”也不等于前段始终空闲或整机都在空等。

### 四种交接内容要分开

| 内容 | 从哪里到哪里 | 对方拿到后能做什么 |
|---|---|---|
| 历史 KV 数据 | LMCache → 对应 worker 的 KV 区域 | 本段 attention 读取历史 |
| KV 完成标记 | 恢复观察器 → 独立事件队列 → 核心 | 登记本请求、本次恢复、本段某 worker 就绪 |
| 模型中间结果 | 前段 worker → 后段 worker | 后段在自身 KV 可用后继续计算 |
| 最终模型／采样结果 | 末段结果路径 → 核心 → 调度器 | 更新请求，输出 token，推进后续工作 |

## 这版实际改了哪些地方

这不是原来 17 行 MultiConnector 兼容补丁的另一种叫法。17 行处理握手与冷 STORE；现在的候选涉及 **LMCache、vLLM、vLLM-Ascend 和独立阶段控制器**。计算形态和测试数据不在此修改范围内。

| 改动 | 原来为什么等 | 现在谁做什么 | 哪些等待保留 |
|---|---|---|---|
| 独立观察 KV 完成 | 完成状态的发现依赖旧 getter 被调用时机 | LMCache 专用线程在正确设备上下文查询真实 Future；普通路径读取 CPU 代理 | 真实搬运、设备同步和本段必要成员确认 |
| 完成消息走独立队列 | 事件可能排在旧模型结果后面 | 每 worker 发布线程发送带请求、恢复代次、阶段身份的小事件；核心核验后登记 | 队列传递、核心轮询；不自动产生计算安排 |
| 核心等模型时仍处理事件 | 核心停在等结果的调用里 | 唯一结果读取线程按原顺序收模型回复；核心线程继续处理阶段事件 | 真模型结果、输出顺序、原模型批名额 |
| 补齐 MTP 草稿等待 | 取草稿 token 的同步 RPC 也会卡住核心 | 核心异步提交，唯一 reader 等结果，核心继续消费事件 | MTP 计算、真实草稿返回及有序更新 |
| 分别判断前后段资格 | 旧调度器只有整请求就绪状态 | 原型控制器分别记录本段 KV、计算和中间结果状态 | 真 scheduler 的组批、token、block 管理仍未接入 |
| 后段接收与模型计算拆开 | 旧接收动作放在模型执行方法里面 | Ascend worker 候选先接收并保留缓冲，条件齐全再计算 | 全 TP 接收一致性、设备依赖、采样和最终回收 |

### 两个执行名额占满时，不能凭通知多开第三个批

**完成通知只登记事实，不释放模型批名额。**一个批从队列取出、核心正在等它时，仍占名额。新核心通过 `ks_model_wait_inflight()` 暴露这份等待记录；上层必须把“队列里的批＋正在等待结果的批”一起计数，不能误判成“队列只剩一个，所以还能再开一个”。这份接口不会自动阻止真实 scheduler 组批，实际 admission 尚需接线。

提前知道某段就绪后，也要等该段资源、合法批次、输入和顺序满足，才能推进。谁能得到新模型批名额仍由真实调度器决定；当前尚未接通完整逻辑。

### 为什么不能只改 LMCache，或调一个参数

LMCache 能早报告数据已好，却不能替 vLLM 组批、管理 token 位置或改变模型结果返回次序。原 scheduler 也没有一个参数能把整请求门禁变成分段派发。

仅减少完成确认人数，会在后段没好时错误放行整请求；把控制消息仍塞回旧回复队列，也可能继续排在旧模型结果后。**独立通知与分段执行接口都需要代码支持。**

## 对应代码：哪里拦住，哪里拆开

以下是原代码和当前隔离候选的连续原文摘录。先读中文解释，再对应函数；这些代码不代表完整框架已经运行。

### 原调度器：整请求恢复完成才放行

请求 ID 不在完成集合就返回 False。最新候选尚未改此函数，因此不能说收到前段事件后，原 scheduler 已自动安排计算。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        if request.status == RequestStatus.WAITING_FOR_REMOTE_KVS:
            # finished_recving_kv_req_ids is populated during
            # update_from_output(), based on worker-side connector signals
            # in KVConnectorOutput.finished_recving
            if request.request_id not in self.finished_recving_kv_req_ids:
                return False
            self._update_waiting_for_remote_kv(request)
            if request.num_preemptions:
                request.status = RequestStatus.PREEMPTED
            else:
                request.status = RequestStatus.WAITING
            return True
```

来源：`05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/core/sched/scheduler.py`，第 2192—2203 行。

### 原核心：直接等待模型结果

Future 是尚未取得的结果凭据。核心进入 future.result() 后，返回前不能继续后面的正常结果处理。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        # Block until the next result is available.
        future, scheduler_output, exec_model_fut = batch_queue.pop()
        with (
            self.log_error_detail(scheduler_output),
            self.log_iteration_details(scheduler_output),
        ):
            model_output = future.result()
            if model_output is None:
                # None from sample_tokens() implies that the original execute_model()
                # call failed - raise that exception.
                exec_model_fut.result()
                raise RuntimeError("unexpected error")
```

来源：`05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/engine/core.py`，第 554—565 行。

### 旧模型回复 FIFO 保留

模型结果仍按提交次序读取。新方案没有让第二个线程乱序抢读旧队列，而是给恢复事件另开通道。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
    def result(self, timeout=None):
        if timeout is not None:
            raise RuntimeError("timeout not implemented")

        # Drain any futures ahead of us in the queue.
        while not self.done():
            future = self.futures_queue.pop()
            future._wait_for_response()
        return super().result()
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/multiproc_executor.py`，第 246—254 行。

### LMCache：真实设备状态由唯一线程查询

owner 指唯一可接触真实设备 Future 的线程。query 确认完成后才取结果；后面还要求成功值严格为 True，False 或异常不发布 READY。旧 getter 读取 CPU 代理。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
    def poll_once(self):
        if threading.get_ident() != self.owner:
            raise RuntimeError("only the device-context owner may query real futures")
        with self.lock:
            snapshot = list(self.entries.items())
        for key, operation in snapshot:
            real = operation.real_future
            if real is None:
                continue
            value, error = None, None
            try:
                # No lock is held for event import/query/result or device work.
                if not real.query():
                    continue
                value = real.result(timeout=0)
                if type(value) is not bool:
                    raise TypeError("current RetrieveResult must be bool")
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/lmcache_stage_observer/stage_observer.py`，第 126—142 行。

### 核心：按身份登记单 worker 就绪

恢复代次表示本请求第几次恢复。旧代次或未知事件不能创建恢复记录；一个 worker 的确认也不能代表全段。当前显式限定 PCP=1，TP 身份取自消息核验。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        event.validate()
        if (event.stage != event.worker_id // self.tp_size
                or event.tp_rank != event.worker_id % self.tp_size
                or event.pcp_rank != 0):
            raise ValueError("restore callback rank identity mismatch")
        # Deliberately never call begin_restore here: unknown/stale events cannot
        # create state. One worker's readiness cannot stand for all PP/TP workers.
        self.controller.mark_kv_ready(event.request_id, event.restore_generation,
                                      event.stage, event.worker_id, device_complete=True)
        return None
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/restore_event_binding/restore_binding.py`，第 38—47 行。

### 新核心：等待期间仍处理事件

唯一结果线程等模型，核心主线程运行此循环。回调出错也保留模型等待与资源，等真实结果结束后再报错。正常回调只处理阶段事实，不提前提交模型结果。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
            event_error = None
            while True:
                if event_error is None:
                    try:
                        self._ks_consume_stage_events()
                    except BaseException as error:
                        # Keep the sole reader and batch alive until the real result
                        # is terminal. Do not update scheduler after callback failure.
                        event_error = error
                if completed.is_set():
                    break
                completed.wait(self._ks_event_poll_interval)
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/core.py`，第 575—586 行。

### 调度器模型更新仍等真实结果

直到真实模型结果返回才调用 update_from_output。提前处理 KV 通知，与提前宣布整个模型批结束是两回事。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
            model_output = self._ks_wait_model_future(future)
            if model_output is None:
                # None from sample_tokens() implies that the original execute_model()
                # call failed - raise that exception.
                self._ks_wait_model_future(exec_model_fut)
                raise RuntimeError("unexpected error")

        # Before processing the model output, process any aborts that happened
        # during the model execution.
        self._process_aborts_queue()
        engine_core_outputs = self.scheduler.update_from_output(
            scheduler_output, model_output
        )
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/core.py`，第 728—740 行。

### 阶段控制器：只检查本段的完整依赖

前段检查本批所有请求、本段全部 TP 成员的 KV 确认，并预留缓冲。后段还要求前段计算完成和本段全 TP 接收完成。这是原型资格判断，尚不是实际 scheduler 的分段派发。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
            for request_id, generation in batch.requests:
                restore = self._restores[request_id]
                if (restore.generation != generation
                        or restore.ready[stage] != set(self.stage_workers[stage])):
                    return None
            if stage == 0:
                if self.buffered_activations >= self.max_buffered_activations:
                    return None
                batch.buffer_reserved = True
            elif (batch.state[0] != "done"
                  or batch.received != set(self.stage_workers[1])):
                return None
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/controller/stage_controller.py`，第 166—177 行。

### Ascend worker：后段先准备接收

后段即便还不能算，也要有人接住前段张量。此方法独立建立接收记录。CPU 元信息接收可能阻塞，实际调用必须非阻塞排入，不能先同步等接收返回再启动前段。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        if get_pp_group().is_first_rank:
            raise ValueError("first PP stage has no upstream activation")
        record = self._ks_pp_stage_record(batch_key, scheduler_output, create=True)
        if record["input"] is not None:
            return {"batch_key": batch_key, "prepared": True}
        try:
            tensor_dict, handles, postprocess = get_pp_group().irecv_tensor_dict(
                all_gather_group=None if enable_sp() else get_tp_group()
            )
            if tensor_dict is None:
                raise RuntimeError("PP receive returned no activation buffer")
            record["handles"] = handles
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/worker_split/worker.py`，第 675—686 行。

### 通信完成仍要建立设备使用依赖

query 和 Python 方法返回不是允许模型直接读取张量的同义词。wait 建立计算流依赖，再记录设备事件；可能涉及 TP all-gather 的后处理要由本段全 TP 一致推进。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        record = self._ks_pp_stage_record(batch_key)
        if record["input_event"] is not None:
            return
        if not self.pp_stage_input_received(batch_key):
            raise RuntimeError("communication has not completed")
        try:
            # wait() also establishes the device-stream dependency; query alone
            # must not be mistaken for permission to read the received tensor.
            record["input"].wait_for_comm()
            event = torch.npu.Event()
            event.record()
            record["input_event"] = event
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/worker_split/worker.py`，第 705—716 行。

### MTP：保留同步接口，新增异步取结果

取草稿仍使用原 output rank，MTP3 与实际计算不变。先返回 Future，让核心在等待时处理独立事件。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
    def take_draft_token_ids_async(self) -> Future[DraftTokenIds | None]:
        # Explicit async entry; old take_draft_token_ids remains unchanged.
        return self.collective_rpc(
            "take_draft_token_ids", unique_reply_rank=self.output_rank,
            non_block=True,
        )
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/multiproc_executor.py`，第 604—609 行。

### 草稿广播仍由核心提交

核心线程写广播，唯一 reader 等回复，不让结果线程接管命令写入。model_batch=False 表示草稿等待不会新增或释放模型批名额。

```python
# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。
        reader = getattr(self, "_ks_result_reader", None)
        if reader is None:
            return self.model_executor.take_draft_token_ids()
        self._ks_assert_no_event_callback_step()
        if threading.get_ident() != reader.owner:
            raise RuntimeError("draft RPC must be submitted by the configured core writer")
        submit = getattr(self.model_executor, "take_draft_token_ids_async", None)
        if not callable(submit):
            raise RuntimeError("executor has no verified async draft RPC interface")
        # Enqueue on the core thread. The result reader never writes broadcast MQ.
        future = submit()
        return self._ks_wait_executor_future(future, model_batch=False)
```

来源：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/core.py`，第 548—559 行。

## 验证结果：已经走到哪一步

验证按“CPU 规则 → 真实设备协议 → 真实事件与核心组合”推进。**各层通过不能互相替代，测试数量不是性能收益。**此处复用已归档回执，不重跑测试。

| 验证对象 | 记录结果 | 已经说明 | 尚未说明 |
|---|---|---|---|
| 最初阶段原型 | 66 项 CPU 通过 | 阶段资格、worker 拆分、原型 RPC 与组合规则 | 旧 bridge 仍存在 FIFO 阻塞，之后才补独立通道；66 项不是最终整链验收 |
| 独立控制通道 | 14 项 CPU 通过 | 模型路径保留，控制事件分流 | 未覆盖完整模型阶段派发 |
| LMCache 观察器 | 15 项 CPU 通过 | 真／假／异常结果、代理、唯一线程和关闭检查 | 不等于真实 LMCache KV 已接入模型 |
| 核心事件消费 | 10 项 CPU 通过 | 等结果时可处理事件，模型按序更新 | 不是完整 EngineCore 正常启动 |
| 恢复身份绑定 | 13 项 CPU 通过 | 请求／代次／阶段／worker 核验 | 真实元数据产生与传递尚未接上 |
| MTP 草稿等待 | 8 项 CPU 通过 | post_step 与 deferred 分支可处理独立事件 | 未执行真实 MTP 算子与草稿负载 |
| 两个在途批 | 1 个 CPU 场景通过 | 等第一批时仍占两名额；B1→B2 更新顺序正确 | 非压力测试，不代表真实 worker 已支持任意双批重叠 |
| 实际 vLLM 回复队列 | 默认路径 CPU 通过，包含大消息溢出路径 | 受控的单 reader 交接、原 FIFO、事件先处理成立 | 请求 spinloop 扩展后回退；扩展启用分支 NOT_VERIFIED，不是通用跨线程安全证明 |
| TP8／PP2 阶段依赖 | 16／16 rank 通过 | 真实 H2D／HCCL／事件；前段先执行，后段没 KV 不执行，内容校验正确 | 合成小张量、原型控制器，不是 GLM／真实 LMCache |
| 真实事件到核心 | 组件链通过 | 真 NPU IPC event 可在模型结果前登记到核心 | 单 worker／阶段，CPU 门闩模拟模型结果；非完整 TP8／PP2 runtime |

### 最关键的两条实际记录

**通知可以先处理，模型批仍没结束。**真实 NPU IPC event 经观察器、发布器、独立 Queue 到核心方法。回调当时模型 Future 未完成、调度器模型更新数 0、在途等待数 1。最终顺序为“核心处理事件 → 模拟模型结果返回 → 调度器更新”。这就是要保留的安全边界。

**前段先执行，不等于后段也被提前放行。**16 卡场景刻意拦住后段 KV 恢复：原 scheduler 的真实判断函数拒绝整请求放行，原型控制器允许前段执行。中间结果到了、后段 KV 还没好时，后段继续被拒绝；自身 KV 就绪后才计算，全部内容校验正确。

后段延迟是人为设定的依赖检查，不是性能样本。真实事件到核心测试第一次 query 已是 True，未覆盖真实事件从未完成变完成的全过程。**上述结果都不能换算成节省了多少毫秒。**

### HCCL 等待语义已经核实

第一轮短超时未轮询到完成，不能说明“必须 wait 才会推进”。后续同通信器的两轮，在 wait 之前均轮询到完成。默认 HCCL wait 主要为计算流建立通信依赖，CPU 返回并不等于设备全完成；阻塞等待配置与 barrier 另有语义。

小张量结果不能替代 GLM 的真实恢复耗时，不能用来解释历史 218—240ms 的全部来源，也不能把该数值说成相对 PP1 固定新增。

### 原始汇总字段

```json
{
  "stage_dependency": {
    "run_id": "20261027-133354",
    "rank_count": 16,
    "all_ranks_pass": true,
    "legacy_denied_front_only": true,
    "front_dispatched_with_back_restore_latched": true,
    "activation_without_back_kv_denied": true,
    "all_rank_content_checks": true,
    "scope": "synthetic tensors, actual H2D/HCCL/events, prototype controller and real old scheduler predicate; no model/performance"
  },
  "real_event_to_core": {
    "run_id": "20261027-133939",
    "status": "PASS_REAL_EVENT_TO_CORE_COMPONENT_ONLY",
    "model_future_not_done_at_callback": true,
    "scheduler_updates_at_callback": 0,
    "waiting_inflight_at_callback": 1,
    "model_order": [
      "core-event-consumed",
      "cpu-model-result",
      "scheduler-update"
    ],
    "retained_live_pids": [],
    "scope": "real NPU IPC event to actual core AST seam; synthetic request identity and CPU model-result fixture"
  },
  "dual_inflight": {
    "status": "CPU_PASS",
    "snapshots": [
      {
        "event": "B1-wait-stage-event",
        "queue_length": 1,
        "active_model_wait": 1,
        "occupied_model_slots": 2,
        "scheduler_updates": []
      },
      {
        "event": "B2-wait-stage-event",
        "queue_length": 0,
        "active_model_wait": 1,
        "occupied_model_slots": 1,
        "scheduler_updates": [
          "B1"
        ]
      }
    ],
    "model_reply_FIFO": [
      "execute1",
      "sample1",
      "execute2",
      "sample2"
    ],
    "scheduler_update_order": [
      "B1",
      "B2"
    ],
    "single_reader": true,
    "threads_left_alive": 0
  }
}
```

## 哪些等待保留，怎样才算真的改好

### 本次瞄准两类可避免的等待

1. **事实发生了，核心还没处理到。**设备已经恢复好 KV，完成信息仍随模型结果返回。这版独立通知，并让核心在等待模型时消费事件；真实事件组件链证明通知不必依赖模型完成。
2. **本段就绪了，仍被整请求门禁拦住。**前段 KV 齐、后段未齐，原调度器不允许前段先算。控制器与小张量设备验证支持拆开此依赖；实际模型调度还需接线。

### 不是所有等待都能删

| 等待 | 为什么保留 |
|---|---|
| 本段必要 TP 成员 KV 真完成 | 本段计算需要它们的数据，不能只看一个 worker |
| 后段自己的 KV 与同批中间结果 | 是后段计算的真实输入依赖 |
| 剩余 token 的模型算子、TP／EP 通信和采样 | 这版未优化这些算子与通信，命中后仍须经过完整模型 |
| 名额、缓冲与合法执行顺序 | 事件不产生第三个名额；不能复用仍在使用的 KV 或 tensor |

所以本方案**不直接解决“完整命中后最后一个 token 仍要几百毫秒”的全部问题**。它瞄准通知与阶段放行。如果剩余 token 模型链仍很重，整体收益可能仍小。当前没有新的性能结果证明它能抵消多少恢复成本。

### 完整模型接入前还缺五件事

| 缺口 | 还要做什么 | 验收要求 |
|---|---|---|
| 真实恢复身份 | scheduler／Connector 产生并共享恢复代次；从真实组取得 PP／TP／PCP／global rank | 旧恢复不放行新恢复，不能把 KV 分片 worker_id 当全局 rank |
| 正常启动接线 | 启动时建立观察器、通道、回调、控制器，并显式提交 outbox 事件 | 不靠测试脚本拼对象；默认关闭、有完整启用入口 |
| 真阶段调度 | 接上 begin_restore、合法组批、admit_batch、take_ready 和阶段派发 | 成员、token 位置、KV block、chunked prefill、取消／重试与名额都正确 |
| 模型与输出生命周期 | 接上前后段模型、MTP、采样、Connector 输出与 Mooncake 消费 | 前段发送结束前保留源；最终结果未消费不释放整批；内容正确 |
| 错误与退出 | 失败事件、协调中止、发布失败与设备操作安全回收 | 未完 DMA 引用不丢失；异常持有项尚无安全 reap，关闭超时不能假装回收 |

当前独立控制通道只接了指定的阶段状态／释放方法，没有完整派发 execute／sample。worker 每阶段仅保留一份当前缓冲记录，控制器“两批上限”不等于真实 worker 已支持任意双在途。发布器关闭仍采用丢弃式退出，不能称消息已排空。

控制器里“中间结果已到达”的登记，也需要上层等本段全 TP 接收、统一后处理以及输入设备事件全部 ready 后再填写。不能把单个 `pp_stage_input_received()` 返回 True，就当成模型已经可以安全读取。这个映射目前尚未自动接线。

### 怎样才算优化有效

先验真实模型内容、原输出顺序和资源回收，再在同计算形态、同客户数据下比较。应逐请求记录“真实 KV 完成 → 核心登记 → 阶段派发 → 设备计算开始”，并检查 TTFT／TPOT／端到端时延。**只有计算确实提前、整体指标也改善，才能说消除等待转化成了性能收益。**

PP1 也存在按批返回与通知等待；PP2 多了前后段依赖与阶段返回路径。不能仅凭 PP2 一段长等待，或 PP1 整体收益较大，就断言通知等待是 PP2 独有，或将全部时差归因 PP。

## 文件、复现入口与启用状态

本次报告整理只在本地 Windows `E:\work\ks` 读文件、生成文档，**未 SSH、未占卡、未改推理源码、未启停服务，也未重跑候选 CPU 测试**。

已归档隔离环境为 25 上无 NPU 挂载的自有容器 `ks-luv-m25-prep`，工作区 `/opt/ks-stage-ready/framework/vllm`，分支 `ks/pp-stage-ready-luv-20261027`。核心／执行器侧候选应用在隔离 worktree，默认未启用；安装目录 `/vllm-workspace/vllm` 未改。LMCache 观察器和 Ascend worker 拆分为隔离候选材料。

核心／执行器累计 2 文件、501 行新增／6 行删除；**这是两个文件的范围，不是整套方案总代码量**。完整运行接线尚未完成，因此不提供虚构的“已能运行”`vllm serve` 命令或开关。

### CPU 复核命令（本报告未重新执行）

机器：本地 Windows；容器：无；工作目录：`E:\work\ks`。以下入口不加载模型、不调用 NPU，但会刷新对应测试回执；如需保留历史原件，应先复制候选目录到个人工作目录再运行。

```powershell
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\control_sidechannel\test_control_sidechannel.py"
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\lmcache_stage_observer\test_lmcache_stage_observer.py"
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\core_event_pump\test_core_event_pump.py"
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\restore_event_binding\test_restore_event_binding.py"
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\draft_wait_followup\test_draft_wait_followup.py"
python -X utf8 -B "E:\work\ks\output\04_修改策略与验证\01_修改策略与隔离候选\20261027_PP分段就绪原型\dual_inflight_probe\probe_dual_inflight.py"
```

最初 66 项原型入口是原型根目录的 `run_cpu_validation.py`，不代替后续独立通道和真实设备验证。

### 证据归档位置

以下均为相对 `output` 的位置。HTML 已内嵌关键代码、差异与结果，不依赖本地路径才看得懂。

- 隔离候选：`04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/`
- 结果汇总：`05_当前环境与实验准备/20261027_25环境准备/设备协议诊断/设备诊断汇总.json`
- 16 卡依赖原始记录：`05_当前环境与实验准备/20261027_25环境准备/设备协议诊断/执行记录/20261027-133354/`
- 真实事件到核心记录：`05_当前环境与实验准备/20261027_25环境准备/设备协议诊断/event_core_chain/执行记录/20261027-133939/`
- 真实回复队列：`05_当前环境与实验准备/20261027_25环境准备/真实回复队列CPU纯导入结果.json`
- 双在途记录：`05_当前环境与实验准备/20261027_25环境准备/dual_inflight_probe_25CPU结果.json`

来源版本：25 归档 vLLM `0.23.0+empty`，提交 `0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665`；vLLM-Ascend `0.23.0`，提交 `5cb98caaadeff42b5b62b996e34bb2aaa29d20fd`；LMCache `0.5.6rc5`。worker 拆分输入来自先前 8 侧归档；LMCache adapter／future 为既有项目归档，构建回执 Git 提交为 NOT_VERIFIED，不能说所有文件都直接来自同一份 25 快照。

本版依据现存源码与 2026-10-27 归档回执整合。文档浏览器检查只验证呈现，不增加模型或性能证据。

## 完整代码差异

HTML 按文件折叠并左右高亮展示；这里保留相同原文差异。

### vLLM 核心：等待与事件消费

相对 25 原始核心的累计候选；隔离 worktree 已应用、默认关闭，包含 MTP 草稿等待。

```diff
--- 05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/engine/core.py
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/core.py
@@ -6,6 +6,7 @@
 import signal
 import threading
 import time
+import weakref
 from collections import defaultdict, deque
 from collections.abc import Callable, Generator
 from concurrent.futures import Future
@@ -92,6 +93,60 @@
 _R = TypeVar("_R")  # Return type for collective_rpc
 
 
+
+# KS candidate: only one old-result reader may be active for this EngineCore.
+# NOT_RUNTIME_INTEGRATED: executor aggregation thread affinity is not verified.
+class _KSModelResultReader:
+    def __init__(self):
+        self.owner = threading.get_ident()
+        self.tasks = queue.Queue(maxsize=1)
+        self.results = weakref.WeakKeyDictionary()
+        self.active = None
+        self.closed = False
+        self.thread = threading.Thread(target=self._run, daemon=True,
+                                       name="KSModelResultReader")
+        self.thread.start()
+
+    def submit(self, future):
+        if threading.get_ident() != self.owner:
+            raise RuntimeError("model wait must stay on the configured core thread")
+        cached = self.results.get(future)
+        if cached is not None:
+            return cached
+        if self.closed:
+            raise RuntimeError("model result reader is closed")
+        if self.active is not None and not self.active.done():
+            raise RuntimeError("another old model future already has the sole reader")
+        outcome = Future()
+        self.results[future] = outcome
+        self.active = outcome
+        self.tasks.put_nowait((future, outcome))
+        return outcome
+
+    def _run(self):
+        while True:
+            task = self.tasks.get()
+            if task is None:
+                return
+            future, outcome = task
+            try:
+                outcome.set_result(future.result())
+            except BaseException as error:
+                outcome.set_exception(error)
+            finally:
+                # No permanent strong reference from this thread to old batches.
+                del task, future, outcome
+
+    def close(self, timeout=0.2):
+        if self.active is not None and not self.active.done():
+            return False
+        if not self.closed:
+            self.closed = True
+            self.tasks.put_nowait(None)
+        self.thread.join(timeout=timeout)
+        return not self.thread.is_alive()
+
+
 class EngineCore:
     """Inner loop of vLLM's Engine."""
 
@@ -440,6 +495,117 @@
         )
         self._iteration_index += 1
 
+
+    def configure_ks_core_event_pump(self, callback, dispatcher=None,
+                                     poll_interval=0.005):
+        # Explicit hook only: enabling an env var does not configure a scheduler.
+        if os.environ.get("KS_CORE_STAGE_EVENT_PUMP") != "1":
+            raise RuntimeError("core stage event pump is disabled")
+        if getattr(self, "_ks_result_reader", None) is not None:
+            raise RuntimeError("core event pump is already configured")
+        if not callable(callback) or (dispatcher is not None and not callable(dispatcher)):
+            raise ValueError("event callback and optional dispatcher must be callable")
+        if not callable(getattr(self.model_executor, "poll_stage_events", None)):
+            raise ValueError("independent executor stage event channel is required")
+        if not 0 < poll_interval <= 0.1:
+            raise ValueError("poll interval must be between zero and 0.1 seconds")
+        self._ks_event_callback = callback
+        self._ks_event_dispatcher = dispatcher
+        self._ks_event_poll_interval = poll_interval
+        self._ks_event_callback_active = False
+        self._ks_waiting_model_future = None
+        self._ks_result_reader = _KSModelResultReader()
+
+    def ks_model_wait_inflight(self):
+        # batch_queue.pop() has happened, but this model batch still owns a slot.
+        return int(getattr(self, "_ks_waiting_model_future", None) is not None)
+
+    def _ks_assert_no_event_callback_step(self):
+        if getattr(self, "_ks_event_callback_active", False):
+            raise RuntimeError("event callback cannot recursively schedule a model step")
+
+    def _ks_consume_stage_events(self):
+        # This method only runs on the core thread. No token/block progress here.
+        for event in self.model_executor.poll_stage_events():
+            self._ks_event_callback_active = True
+            try:
+                if self._ks_event_callback(event) is not None:
+                    raise ValueError("event callback must return None")
+                if self._ks_event_dispatcher is not None:
+                    self._ks_event_dispatcher(event)
+            finally:
+                self._ks_event_callback_active = False
+
+
+    def _ks_wait_model_future(self, future):
+        return self._ks_wait_executor_future(future, model_batch=True)
+
+    def ks_draft_wait_pending(self):
+        # A draft/control RPC is not a newly admitted model batch.
+        return int(getattr(self, "_ks_waiting_draft_future", None) is not None)
+
+    def _ks_take_draft_token_ids(self):
+        reader = getattr(self, "_ks_result_reader", None)
+        if reader is None:
+            return self.model_executor.take_draft_token_ids()
+        self._ks_assert_no_event_callback_step()
+        if threading.get_ident() != reader.owner:
+            raise RuntimeError("draft RPC must be submitted by the configured core writer")
+        submit = getattr(self.model_executor, "take_draft_token_ids_async", None)
+        if not callable(submit):
+            raise RuntimeError("executor has no verified async draft RPC interface")
+        # Enqueue on the core thread. The result reader never writes broadcast MQ.
+        future = submit()
+        return self._ks_wait_executor_future(future, model_batch=False)
+
+    def _ks_wait_executor_future(self, future, *, model_batch):
+        reader = getattr(self, "_ks_result_reader", None)
+        if reader is None:
+            return future.result()
+        if getattr(self, "_ks_event_callback_active", False):
+            raise RuntimeError("event callback cannot consume old model results")
+        outcome = reader.submit(future)
+        completed = threading.Event()
+        outcome.add_done_callback(lambda _: completed.set())
+        if model_batch:
+            self._ks_waiting_model_future = future
+        else:
+            self._ks_waiting_draft_future = future
+        try:
+            event_error = None
+            while True:
+                if event_error is None:
+                    try:
+                        self._ks_consume_stage_events()
+                    except BaseException as error:
+                        # Keep the sole reader and batch alive until the real result
+                        # is terminal. Do not update scheduler after callback failure.
+                        event_error = error
+                if completed.is_set():
+                    break
+                completed.wait(self._ks_event_poll_interval)
+            # CPU relay Future, never the old FutureWrapper a second time.
+            result = outcome.result()
+            if event_error is not None:
+                raise event_error
+            return result
+        finally:
+            if model_batch:
+                self._ks_waiting_model_future = None
+            else:
+                self._ks_waiting_draft_future = None
+
+    def _ks_sample_tokens_with_event_pump(self, grammar_output):
+        if getattr(self, "_ks_result_reader", None) is None:
+            return self.model_executor.sample_tokens(grammar_output)
+        return self._ks_wait_model_future(
+            self.model_executor.sample_tokens(grammar_output, non_block=True))
+
+    def _ks_close_core_event_pump(self):
+        reader = getattr(self, "_ks_result_reader", None)
+        if reader is not None and not reader.close():
+            raise RuntimeError("old result reader is still active after executor shutdown")
+
     def step(self) -> tuple[dict[int, EngineCoreOutputs], bool]:
         """Schedule, execute, and make output.
 
@@ -447,6 +613,7 @@
         was executed.
         """
 
+        self._ks_assert_no_event_callback_step()
         # Check for any requests remaining in the scheduler - unfinished,
         # or finished and not yet removed from the batch.
         if not self.scheduler.has_requests():
@@ -458,9 +625,9 @@
             self.log_error_detail(scheduler_output),
             self.log_iteration_details(scheduler_output),
         ):
-            model_output = future.result()
+            model_output = self._ks_wait_model_future(future)
             if model_output is None:
-                model_output = self.model_executor.sample_tokens(grammar_output)
+                model_output = self._ks_sample_tokens_with_event_pump(grammar_output)
 
         # Before processing the model output, process any aborts that happened
         # during the model execution.
@@ -477,7 +644,7 @@
         # need to update draft token ids here.
         if not self.async_scheduling and self.use_spec_decode and model_executed:
             # Take the draft token ids.
-            draft_token_ids = self.model_executor.take_draft_token_ids()
+            draft_token_ids = self._ks_take_draft_token_ids()
             if draft_token_ids is not None:
                 self.scheduler.update_draft_token_ids(draft_token_ids)
 
@@ -498,6 +665,7 @@
         3. Update the scheduler from the output.
         """
 
+        self._ks_assert_no_event_callback_step()
         batch_queue = self.batch_queue
         assert batch_queue is not None
 
@@ -557,11 +725,11 @@
             self.log_error_detail(scheduler_output),
             self.log_iteration_details(scheduler_output),
         ):
-            model_output = future.result()
+            model_output = self._ks_wait_model_future(future)
             if model_output is None:
                 # None from sample_tokens() implies that the original execute_model()
                 # call failed - raise that exception.
-                exec_model_fut.result()
+                self._ks_wait_model_future(exec_model_fut)
                 raise RuntimeError("unexpected error")
 
         # Before processing the model output, process any aborts that happened
@@ -579,7 +747,7 @@
             # we need to get the draft token ids from the prior step before
             # we can compute the grammar bitmask for the deferred request.
             if self.use_spec_decode:
-                draft_token_ids = self.model_executor.take_draft_token_ids()
+                draft_token_ids = self._ks_take_draft_token_ids()
                 assert draft_token_ids is not None
                 # Update the draft token ids in the scheduler output to
                 # filter out the invalid spec tokens, which will be padded
@@ -612,6 +780,7 @@
         self.structured_output_manager.clear_backend()
         if self.model_executor:
             self.model_executor.shutdown()
+        self._ks_close_core_event_pump()
         if self.scheduler:
             self.scheduler.shutdown()
 
```


### vLLM 执行器：独立通道与草稿接口

隔离 worktree 已应用、默认关闭。保留旧模型 FIFO，增加独立事件及恢复身份。

```diff
--- 05_当前环境与实验准备/20261027_25环境准备/25镜像源码与配置快照/sources/vllm/vllm/v1/executor/multiproc_executor.py
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/draft_wait_followup/multiproc_executor.py
@@ -67,6 +67,169 @@
 logger = init_logger(__name__)
 
 
+
+# KS candidate: NOT_RUNTIME_INTEGRATED. No device/LMCache producer is installed.
+_KS_STAGE_METHODS = {
+    "input_received": "pp_stage_input_received",
+    "input_ready": "pp_stage_input_ready",
+    "model_ready": "pp_stage_model_ready",
+    "release_ready": "pp_stage_release_ready",
+    "release": "release_pp_stage_batch",
+}
+_KS_EVENT_KINDS = frozenset(("CONTROL_RESULT", "CONTROL_ERROR",
+                           "KV_DEVICE_READY", "INPUT_DEVICE_READY",
+                           "STAGE_DEVICE_DONE"))
+
+
+def _ks_identity(event_id, batch_id, generation, stage):
+    if not (type(event_id) is str and 0 < len(event_id) <= 128
+            and type(batch_id) is str and 0 < len(batch_id) <= 128
+            and type(generation) is int and generation >= 0
+            and type(stage) is int and stage >= 0):
+        raise ValueError("invalid stage event/command identity")
+
+
+@dataclass(frozen=True)
+class KSStageCommand:
+    command_id: str
+    batch_id: str
+    generation: int
+    target_stage: int
+    operation: str
+    final_output_consumed: bool = False
+
+    def validate(self):
+        _ks_identity(self.command_id, self.batch_id, self.generation,
+                     self.target_stage)
+        if (self.operation not in _KS_STAGE_METHODS
+                or type(self.final_output_consumed) is not bool):
+            raise ValueError("unsupported stage control command")
+
+
+@dataclass(frozen=True)
+class KSStageEvent:
+    event_id: str
+    batch_id: str
+    generation: int
+    stage: int
+    worker_id: int
+    sequence: int
+    kind: str
+    value: bool | str | None = None
+
+    def validate(self):
+        _ks_identity(self.event_id, self.batch_id, self.generation, self.stage)
+        if (type(self.worker_id) is not int or self.worker_id < 0
+                or type(self.sequence) is not int or self.sequence < 1
+                or self.kind not in _KS_EVENT_KINDS
+                or type(self.value) not in (bool, str, type(None))
+                or (type(self.value) is str and len(self.value) > 512)):
+            raise ValueError("invalid stage event payload")
+
+
+@dataclass(frozen=True)
+class KSRestoreEvent:
+    """Per-request restore identity; deliberately has no logical batch ID."""
+    event_id: str
+    request_id: str
+    restore_generation: int
+    stage: int
+    worker_id: int
+    tp_rank: int
+    pcp_rank: int
+    sequence: int
+
+    def validate(self):
+        if (type(self.event_id) is not str or not 0 < len(self.event_id) <= 128
+                or type(self.request_id) is not str or not self.request_id
+                or any(type(value) is not int or value < 0 for value in (
+                    self.restore_generation, self.stage, self.worker_id,
+                    self.tp_rank, self.pcp_rank))
+                or type(self.sequence) is not int or self.sequence < 1):
+            raise ValueError("invalid restore event identity")
+
+
+class _KSStageEventPublisher:
+    """One writer thread per worker; never runs model/HCCL/Future queries.
+
+    Any future host observer must enqueue a previously obtained readiness fact.
+    It must NOT query the same LMCache Future as an existing completion path.
+    """
+    def __init__(self, transport, failed, worker_id, stage, tp_size=None, pcp_size=1):
+        self.transport = transport
+        self.failed = failed
+        self.worker_id = worker_id
+        self.stage = stage
+        self.tp_size = tp_size
+        self.pcp_size = pcp_size
+        self.pending = queue.Queue(maxsize=64)
+        self.stopped = threading.Event()
+        self.lock = threading.Lock()
+        self.sequence = 0
+        self.thread = Thread(target=self._run, daemon=True,
+                             name="KSStageEventWriter")
+        self.thread.start()
+
+    def publish(self, event_id, batch_id, generation, kind, value=None):
+        with self.lock:
+            if self.stopped.is_set() or self.failed.is_set():
+                raise RuntimeError("stage event channel unavailable")
+            event = KSStageEvent(event_id, batch_id, generation, self.stage,
+                                 self.worker_id, self.sequence + 1, kind, value)
+            event.validate()
+            try:
+                self.pending.put_nowait(event)
+            except queue.Full:
+                self.failed.set()
+                raise RuntimeError("stage event inbox full") from None
+            self.sequence += 1
+
+    def publish_restore_ready(self, identity):
+        # Called only with success drained from the unique device-future owner.
+        # This method does not query/synchronize an event or manufacture readiness.
+        identity.validate()
+        if (self.pcp_size != 1 or type(self.tp_size) is not int or self.tp_size < 1
+                or identity.global_worker_rank != self.worker_id
+                or identity.stage_id != self.stage
+                or self.stage != self.worker_id // self.tp_size
+                or identity.tp_rank != self.worker_id % self.tp_size
+                or identity.pcp_rank != 0):
+            raise ValueError("restore identity does not match publisher PP/TP/PCP rank")
+        with self.lock:
+            if self.stopped.is_set() or self.failed.is_set():
+                raise RuntimeError("stage event channel unavailable")
+            sequence = self.sequence + 1
+            event = KSRestoreEvent(f"restore:{self.worker_id}:{sequence}",
+                identity.request_id, identity.restore_generation, self.stage,
+                self.worker_id, identity.tp_rank, identity.pcp_rank, sequence)
+            event.validate()
+            try:
+                self.pending.put_nowait(event)
+            except queue.Full:
+                self.failed.set()
+                raise RuntimeError("stage event inbox full") from None
+            self.sequence = sequence
+
+    def _run(self):
+        while not self.stopped.is_set():
+            try:
+                event = self.pending.get(timeout=0.05)
+            except queue.Empty:
+                continue
+            try:
+                self.transport.put_nowait(event)
+            except (queue.Full, OSError, ValueError):
+                self.failed.set()
+                return
+
+    def close(self):
+        self.stopped.set()
+        self.thread.join(timeout=0.2)
+        # Never join a multiprocessing feeder whose reader may have exited.
+        self.transport.cancel_join_thread()
+        self.transport.close()
+
+
 class FutureWrapper(Future):
     def __init__(
         self,
@@ -157,6 +320,7 @@
             scheduler_output_handle = self.rpc_broadcast_mq.export_handle()
         # Create workers
         context = get_mp_context()
+        self._init_stage_event_channels(context, tp_size, pp_size, pcp_size)
         shared_worker_lock = context.Lock()
         unready_workers: list[UnreadyWorkerProcHandle] = []
         success = False
@@ -188,6 +352,7 @@
                         shared_worker_lock=shared_worker_lock,
                         is_driver_worker=is_driver_worker,
                         inherited_fds=inherited_fds,
+                        stage_event_channel=self._ks_stage_channels.get(global_rank),
                     )
                 unready_workers.append(unready_worker_handle)
                 if inherited_fds is not None:
@@ -246,6 +411,104 @@
 
         self.output_rank = self._get_output_rank()
 
+
+    def _init_stage_event_channels(self, context, tp_size, pp_size, pcp_size):
+        self._ks_stage_channels = {}
+        self._ks_stage_sequences = {}
+        self._ks_stage_pending = {}
+        self._ks_stage_poll_owner = None
+        self._ks_stage_rpc_owner = threading.get_ident()
+        self._ks_stage_tp_size = tp_size * pcp_size
+        self._ks_restore_tp_size = tp_size
+        self._ks_restore_pcp_size = pcp_size
+        self._ks_stage_count = pp_size
+        if os.environ.get("KS_PP_STAGE_EVENTS") != "1":
+            return
+        if (self.parallel_config.nnodes_within_dp != 1
+                or self.local_world_size != self.world_size):
+            raise ValueError("KS stage sidechannel supports single-node only")
+        for rank in range(self.world_size):
+            self._ks_stage_channels[rank] = (context.Queue(maxsize=64),
+                                              context.Event())
+
+    def submit_stage_command(self, command):
+        if not self._ks_stage_channels:
+            raise RuntimeError("KS stage sidechannel is disabled")
+        if threading.get_ident() != self._ks_stage_rpc_owner:
+            raise RuntimeError("stage and ordinary RPC must share the core writer")
+        if self.is_failed:
+            raise RuntimeError("Executor failed.")
+        if not isinstance(command, KSStageCommand):
+            raise ValueError("expected KSStageCommand")
+        command.validate()
+        if command.target_stage >= self._ks_stage_count:
+            raise ValueError("unknown PP stage")
+        if command.command_id in self._ks_stage_pending:
+            raise ValueError("duplicate pending stage command id")
+        ranks = frozenset(rank for rank in self._ks_stage_channels
+                          if rank // self._ks_stage_tp_size == command.target_stage)
+        if not ranks:
+            raise ValueError("target stage has no workers")
+        self._ks_stage_pending[command.command_id] = [command, set(ranks)]
+        try:
+            # Same single broadcast writer; no FutureWrapper/model FIFO entry.
+            self.rpc_broadcast_mq.enqueue((command, (), {}, None))
+        except Exception:
+            del self._ks_stage_pending[command.command_id]
+            raise
+        return ranks
+
+    def poll_stage_events(self, max_per_worker=8):
+        if type(max_per_worker) is not int or max_per_worker < 1:
+            raise ValueError("max_per_worker must be positive")
+        owner = threading.get_ident()
+        if self._ks_stage_poll_owner not in (None, owner):
+            raise RuntimeError("only one core reader may consume stage events")
+        self._ks_stage_poll_owner = owner
+        events = []
+        for rank, (transport, failed) in self._ks_stage_channels.items():
+            if failed.is_set():
+                raise RuntimeError(f"stage event channel failed for worker {rank}")
+            for _ in range(max_per_worker):
+                try:
+                    event = transport.get_nowait()
+                except queue.Empty:
+                    break
+                if not isinstance(event, (KSStageEvent, KSRestoreEvent)):
+                    raise ValueError("unexpected stage channel payload")
+                event.validate()
+                if (event.worker_id != rank
+                        or event.stage != rank // self._ks_stage_tp_size
+                        or event.sequence != self._ks_stage_sequences.get(rank, 0) + 1):
+                    raise ValueError("stage event worker/sequence mismatch")
+                if isinstance(event, KSRestoreEvent):
+                    if (self._ks_restore_pcp_size != 1
+                            or event.tp_rank != rank % self._ks_restore_tp_size
+                            or event.pcp_rank != 0):
+                        raise ValueError("restore event TP/PCP identity mismatch")
+                elif event.kind in ("CONTROL_RESULT", "CONTROL_ERROR"):
+                    pending = self._ks_stage_pending.get(event.event_id)
+                    if pending is None:
+                        raise ValueError("unknown or duplicate stage command result")
+                    command, ranks = pending
+                    if (event.batch_id != command.batch_id
+                            or event.generation != command.generation
+                            or event.stage != command.target_stage or rank not in ranks):
+                        raise ValueError("stage command result identity mismatch")
+                    ranks.remove(rank)
+                    if not ranks:
+                        del self._ks_stage_pending[event.event_id]
+                self._ks_stage_sequences[rank] = event.sequence
+                events.append(event)
+        return events
+
+    def _close_stage_event_channels(self):
+        for transport, _ in getattr(self, "_ks_stage_channels", {}).values():
+            transport.cancel_join_thread()
+            transport.close()
+        self._ks_stage_channels = {}
+        self._ks_stage_pending = {}
+
     def _get_parallel_sizes(self) -> tuple[int, int, int]:
         self.world_size = self.parallel_config.world_size
         assert self.world_size % self.parallel_config.nnodes_within_dp == 0, (
@@ -335,6 +598,14 @@
         # OPTIMIZATION: Get output only from a single worker (output_rank)
         return self.collective_rpc(
             "take_draft_token_ids", unique_reply_rank=self.output_rank
+        )
+
+
+    def take_draft_token_ids_async(self) -> Future[DraftTokenIds | None]:
+        # Explicit async entry; old take_draft_token_ids remains unchanged.
+        return self.collective_rpc(
+            "take_draft_token_ids", unique_reply_rank=self.output_rank,
+            non_block=True,
         )
 
     def collective_rpc(  # type: ignore[override]
@@ -486,6 +757,7 @@
                 mq.shutdown()
             self.response_mqs = []
 
+        self._close_stage_event_channels()
         logger.debug_once("[shutdown] Executor: complete")
 
     def check_health(self) -> None:
@@ -600,6 +872,7 @@
         input_shm_handle: Handle,
         shared_worker_lock: LockType,
         is_driver_worker: bool,
+        stage_event_channel=None,
     ):
         self.rank = rank
         wrapper = WorkerWrapperBase(rpc_rank=local_rank, global_rank=rank)
@@ -651,6 +924,8 @@
         # (nnodes_within_dp > 1) require distributed groups to be initialized
         self._init_message_queues(input_shm_handle, vllm_config)
 
+        self._init_stage_event_channel(stage_event_channel, vllm_config)
+
         # Enable environment variable cache (e.g. assume no more
         # environment variable overrides after this point)
         enable_envs_cache()
@@ -665,6 +940,7 @@
         shared_worker_lock: LockType,
         is_driver_worker: bool,
         inherited_fds: list[int] | None = None,
+        stage_event_channel=None,
     ) -> UnreadyWorkerProcHandle:
         context = get_mp_context()
         # Ready pipe to communicate readiness from child to parent
@@ -684,6 +960,7 @@
             "death_pipe": death_reader,
             "shared_worker_lock": shared_worker_lock,
             "is_driver_worker": is_driver_worker,
+            "stage_event_channel": stage_event_channel,
             # Have the worker close parent end of this worker's pipes too
             "inherited_fds": inherited_fds if inherited_fds is not None else [],
         }
@@ -768,6 +1045,9 @@
         return cast(list[WorkerProcHandle], ready_proc_handles)
 
     def shutdown(self):
+        if publisher := getattr(self, "_ks_stage_publisher", None):
+            publisher.close()
+            self._ks_stage_publisher = None
         if self.rpc_broadcast_mq is not None:
             self.rpc_broadcast_mq.shutdown()
         if self.worker_response_mq is not None:
@@ -777,6 +1057,48 @@
         self.worker_response_mq = None
         destroy_model_parallel()
         destroy_distributed_environment()
+
+
+    def _init_stage_event_channel(self, stage_event_channel, vllm_config):
+        self._ks_stage_publisher = None
+        if stage_event_channel is None:
+            return
+        parallel = vllm_config.parallel_config
+        self._ks_stage_index = self.rank // (parallel.tensor_parallel_size
+                                            * parallel.prefill_context_parallel_size)
+        transport, failed = stage_event_channel
+        self._ks_stage_publisher = _KSStageEventPublisher(
+            transport, failed, self.rank, self._ks_stage_index,
+            tp_size=parallel.tensor_parallel_size,
+            pcp_size=parallel.prefill_context_parallel_size)
+        # Interface only. No LMCache/stream/event observer is installed here.
+        self.worker.ks_publish_stage_event = self._ks_stage_publisher.publish
+        self.worker.ks_publish_restore_ready = self._ks_stage_publisher.publish_restore_ready
+
+    def _handle_stage_command(self, command):
+        publisher = self._ks_stage_publisher
+        if publisher is None:
+            raise RuntimeError("stage command received without sidechannel")
+        command.validate()
+        if self._ks_stage_index != command.target_stage:
+            return
+        try:
+            method = getattr(self.worker, _KS_STAGE_METHODS[command.operation])
+            batch_key = (command.batch_id, command.generation)
+            if command.operation in ("release_ready", "release"):
+                value = method(batch_key,
+                    final_output_consumed=command.final_output_consumed)
+            else:
+                value = method(batch_key)
+            if type(value) not in (bool, type(None)):
+                raise TypeError("stage controls may return only bool or None")
+        except Exception as error:
+            publisher.publish(command.command_id, command.batch_id,
+                              command.generation, "CONTROL_ERROR",
+                              f"{type(error).__name__}: {error}"[:512])
+            return
+        publisher.publish(command.command_id, command.batch_id,
+                          command.generation, "CONTROL_RESULT", value)
 
     def monitor_death_pipe(self, death_pipe, shutdown_requested: threading.Event):
         if death_pipe is None:
@@ -973,6 +1295,10 @@
             method, args, kwargs, output_rank = self.rpc_broadcast_mq.dequeue(
                 indefinite=True
             )
+            if isinstance(method, KSStageCommand):
+                # Always consume this envelope here: never emit an old RPC reply.
+                self._handle_stage_command(method)
+                continue
             try:
                 if isinstance(method, str):
                     func = getattr(self.worker, method)
```


### LMCache：恢复观察器

新增隔离模块；尚未接入正常模型启动。真实设备唯一 owner、CPU 代理、成功 outbox 和关闭检查。

```diff
--- /dev/null
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/lmcache_stage_observer/stage_observer.py
@@ -0,0 +1,194 @@
+"""NOT_RUNTIME_INTEGRATED: single-owner LMCache device-future observer.
+
+The actual adapter patch injects the installed MessagingFuture as proxy_factory.
+No device library is imported here; context and scheduler identity are mandatory
+external inputs, not inferred from the LMCache KV-shard worker_id.
+"""
+from __future__ import annotations
+
+from dataclasses import dataclass
+import threading
+
+
+@dataclass(frozen=True)
+class StageRestoreIdentity:
+    request_id: str
+    restore_generation: int
+    stage_id: int
+    tp_rank: int
+    pcp_rank: int
+    global_worker_rank: int
+
+    def validate(self):
+        if not (type(self.request_id) is str and self.request_id):
+            raise ValueError("real request identity is required")
+        if any(type(value) is not int or value < 0 for value in (
+            self.restore_generation, self.stage_id, self.tp_rank,
+            self.pcp_rank, self.global_worker_rank)):
+            raise ValueError("real restore generation and PP/TP/PCP/global ranks required")
+
+
+@dataclass
+class _Operation:
+    identity: StageRestoreIdentity
+    signature: tuple
+    proxy: object
+    producer_event: object
+    real_future: object = None
+    cancelled: bool = False
+
+
+class LMCacheStageObserver:
+    def __init__(self, proxy_factory, device_context_factory):
+        if not callable(proxy_factory) or not callable(device_context_factory):
+            raise ValueError("installed MessagingFuture and owner device context required")
+        self.proxy_factory = proxy_factory
+        self.device_context_factory = device_context_factory
+        self.lock = threading.Lock()
+        self.entries = {}
+        self.latest_generation = {}
+        self.success_outbox = []
+        self.retained_errors = []
+        self.owner = None
+        self.thread = None
+        self.started = threading.Event()
+        self.stopping = threading.Event()
+        self.wakeup = threading.Event()
+        self.owner_error = None
+
+    def start(self):
+        if self.thread is not None:
+            raise RuntimeError("observer owner already started")
+        self.thread = threading.Thread(target=self._run, daemon=True,
+                                       name="LMCacheStageObserver")
+        self.thread.start()
+        if not self.started.wait(1) or self.owner_error is not None:
+            raise RuntimeError("observer device context did not initialize")
+
+    def reserve(self, identity, signature, producer_event):
+        if not isinstance(identity, StageRestoreIdentity):
+            raise ValueError("explicit StageRestoreIdentity required")
+        identity.validate()
+        with self.lock:
+            if self.owner is None or self.stopping.is_set() or self.owner_error:
+                raise RuntimeError("observer owner/context not ready")
+            key = (identity.request_id, identity.restore_generation)
+            previous = self.latest_generation.get(identity.request_id, -1)
+            if any(operation.identity.request_id == identity.request_id
+                   for operation in (*self.entries.values(), *self.retained_errors)):
+                raise ValueError("previous restore still owns device lifetime")
+            if key in self.entries or identity.restore_generation <= previous:
+                raise ValueError("duplicate or stale restore generation")
+            # Signature includes exact token span/skip/block IDs supplied by adapter.
+            proxy = self.proxy_factory()
+            if producer_event is not None:
+                proxy.retain_reference(producer_event)
+            operation = _Operation(identity, signature, proxy, producer_event)
+            self.entries[key] = operation
+            self.latest_generation[identity.request_id] = identity.restore_generation
+            return operation
+
+    def attach(self, operation, real_future):
+        # Ownership transfers before the adapter puts the CPU proxy in its dict.
+        with self.lock:
+            if operation.real_future is not None:
+                raise RuntimeError("real future already attached")
+            operation.proxy.retain_reference(real_future)
+            operation.real_future = real_future
+        self.wakeup.set()
+        return operation.proxy
+
+    def submission_failed(self, operation, error):
+        with self.lock:
+            key = (operation.identity.request_id, operation.identity.restore_generation)
+            self.entries.pop(key, None)
+            # submit_retrieve raising does not prove that no DMA was submitted.
+            # Retain the producer event and operation until a future safe-reap
+            # protocol establishes device quiescence; cancellation is insufficient.
+            operation.cancelled = True
+            self.retained_errors.append(operation)
+            operation.proxy.set_exception(error)
+
+    def invalidate_requests(self, request_ids):
+        # Invalidation never queries a real future and never releases its refs.
+        with self.lock:
+            for operation in self.entries.values():
+                if operation.identity.request_id in request_ids:
+                    operation.cancelled = True
+            self.success_outbox = [identity for identity in self.success_outbox
+                                   if identity.request_id not in request_ids]
+
+    def drain_success(self):
+        with self.lock:
+            events, self.success_outbox = self.success_outbox, []
+            return events
+
+    def poll_once(self):
+        if threading.get_ident() != self.owner:
+            raise RuntimeError("only the device-context owner may query real futures")
+        with self.lock:
+            snapshot = list(self.entries.items())
+        for key, operation in snapshot:
+            real = operation.real_future
+            if real is None:
+                continue
+            value, error = None, None
+            try:
+                # No lock is held for event import/query/result or device work.
+                if not real.query():
+                    continue
+                value = real.result(timeout=0)
+                if type(value) is not bool:
+                    raise TypeError("current RetrieveResult must be bool")
+            except Exception as exc:
+                error = exc
+            with self.lock:
+                if self.entries.get(key) is not operation:
+                    raise RuntimeError("observer operation identity changed")
+                if error is None:
+                    if value and not operation.cancelled:
+                        self.success_outbox.append(operation.identity)
+                    operation.proxy.set_result(value)
+                else:
+                    # query/import failures do not prove terminal device safety.
+                    self.retained_errors.append(operation)
+                    operation.proxy.set_exception(error)
+                del self.entries[key]
+
+    def _run(self):
+        try:
+            with self.device_context_factory():
+                self.owner = threading.get_ident()
+                self.started.set()
+                while True:
+                    self.poll_once()
+                    with self.lock:
+                        empty = not self.entries
+                    if self.stopping.is_set() and empty:
+                        return
+                    self.wakeup.wait(0.005)
+                    self.wakeup.clear()
+        except BaseException as error:
+            # Keep all operations referenced; do not fabricate success or free KV.
+            self.owner_error = error
+            self.started.set()
+
+    def close(self, timeout=0.2):
+        with self.lock:
+            self.stopping.set()
+            self.success_outbox.clear()
+            for operation in self.entries.values():
+                operation.cancelled = True
+        self.wakeup.set()
+        if self.thread is not None:
+            self.thread.join(timeout=timeout)
+        with self.lock:
+            return (not self.entries and not self.retained_errors
+                    and self.owner_error is None
+                    and (self.thread is None or not self.thread.is_alive()))
+
+    def assert_safe_adapter_shutdown(self):
+        if not self.close():
+            raise RuntimeError(
+                "observer not device-quiescent; retain operations and do not close "
+                "transfer context without explicit safe unregister/reap protocol")
```


### LMCache：adapter 对接

基于既有 adapter 归档的隔离候选，旧观测和兼容代码保留；未修改安装版本。

```diff
--- 04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022/lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/lmcache_stage_observer/vllm_multi_process_adapter.py
@@ -1328,6 +1328,190 @@
         self._health_events[url].clear()
 
 
+@dataclass(frozen=True)
+class StageRestoreIdentity:
+    request_id: str
+    restore_generation: int
+    stage_id: int
+    tp_rank: int
+    pcp_rank: int
+    global_worker_rank: int
+
+    def validate(self):
+        if not (type(self.request_id) is str and self.request_id):
+            raise ValueError("real request identity is required")
+        if any(type(value) is not int or value < 0 for value in (
+            self.restore_generation, self.stage_id, self.tp_rank,
+            self.pcp_rank, self.global_worker_rank)):
+            raise ValueError("real restore generation and PP/TP/PCP/global ranks required")
+
+
+@dataclass
+class _Operation:
+    identity: StageRestoreIdentity
+    signature: tuple
+    proxy: object
+    producer_event: object
+    real_future: object = None
+    cancelled: bool = False
+
+
+class LMCacheStageObserver:
+    def __init__(self, proxy_factory, device_context_factory):
+        if not callable(proxy_factory) or not callable(device_context_factory):
+            raise ValueError("installed MessagingFuture and owner device context required")
+        self.proxy_factory = proxy_factory
+        self.device_context_factory = device_context_factory
+        self.lock = threading.Lock()
+        self.entries = {}
+        self.latest_generation = {}
+        self.success_outbox = []
+        self.retained_errors = []
+        self.owner = None
+        self.thread = None
+        self.started = threading.Event()
+        self.stopping = threading.Event()
+        self.wakeup = threading.Event()
+        self.owner_error = None
+
+    def start(self):
+        if self.thread is not None:
+            raise RuntimeError("observer owner already started")
+        self.thread = threading.Thread(target=self._run, daemon=True,
+                                       name="LMCacheStageObserver")
+        self.thread.start()
+        if not self.started.wait(1) or self.owner_error is not None:
+            raise RuntimeError("observer device context did not initialize")
+
+    def reserve(self, identity, signature, producer_event):
+        if not isinstance(identity, StageRestoreIdentity):
+            raise ValueError("explicit StageRestoreIdentity required")
+        identity.validate()
+        with self.lock:
+            if self.owner is None or self.stopping.is_set() or self.owner_error:
+                raise RuntimeError("observer owner/context not ready")
+            key = (identity.request_id, identity.restore_generation)
+            previous = self.latest_generation.get(identity.request_id, -1)
+            if any(operation.identity.request_id == identity.request_id
+                   for operation in (*self.entries.values(), *self.retained_errors)):
+                raise ValueError("previous restore still owns device lifetime")
+            if key in self.entries or identity.restore_generation <= previous:
+                raise ValueError("duplicate or stale restore generation")
+            # Signature includes exact token span/skip/block IDs supplied by adapter.
+            proxy = self.proxy_factory()
+            if producer_event is not None:
+                proxy.retain_reference(producer_event)
+            operation = _Operation(identity, signature, proxy, producer_event)
+            self.entries[key] = operation
+            self.latest_generation[identity.request_id] = identity.restore_generation
+            return operation
+
+    def attach(self, operation, real_future):
+        # Ownership transfers before the adapter puts the CPU proxy in its dict.
+        with self.lock:
+            if operation.real_future is not None:
+                raise RuntimeError("real future already attached")
+            operation.proxy.retain_reference(real_future)
+            operation.real_future = real_future
+        self.wakeup.set()
+        return operation.proxy
+
+    def submission_failed(self, operation, error):
+        with self.lock:
+            key = (operation.identity.request_id, operation.identity.restore_generation)
+            self.entries.pop(key, None)
+            # submit_retrieve raising does not prove that no DMA was submitted.
+            # Retain the producer event and operation until a future safe-reap
+            # protocol establishes device quiescence; cancellation is insufficient.
+            operation.cancelled = True
+            self.retained_errors.append(operation)
+            operation.proxy.set_exception(error)
+
+    def invalidate_requests(self, request_ids):
+        # Invalidation never queries a real future and never releases its refs.
+        with self.lock:
+            for operation in self.entries.values():
+                if operation.identity.request_id in request_ids:
+                    operation.cancelled = True
+            self.success_outbox = [identity for identity in self.success_outbox
+                                   if identity.request_id not in request_ids]
+
+    def drain_success(self):
+        with self.lock:
+            events, self.success_outbox = self.success_outbox, []
+            return events
+
+    def poll_once(self):
+        if threading.get_ident() != self.owner:
+            raise RuntimeError("only the device-context owner may query real futures")
+        with self.lock:
+            snapshot = list(self.entries.items())
+        for key, operation in snapshot:
+            real = operation.real_future
+            if real is None:
+                continue
+            value, error = None, None
+            try:
+                # No lock is held for event import/query/result or device work.
+                if not real.query():
+                    continue
+                value = real.result(timeout=0)
+                if type(value) is not bool:
+                    raise TypeError("current RetrieveResult must be bool")
+            except Exception as exc:
+                error = exc
+            with self.lock:
+                if self.entries.get(key) is not operation:
+                    raise RuntimeError("observer operation identity changed")
+                if error is None:
+                    if value and not operation.cancelled:
+                        self.success_outbox.append(operation.identity)
+                    operation.proxy.set_result(value)
+                else:
+                    # query/import failures do not prove terminal device safety.
+                    self.retained_errors.append(operation)
+                    operation.proxy.set_exception(error)
+                del self.entries[key]
+
+    def _run(self):
+        try:
+            with self.device_context_factory():
+                self.owner = threading.get_ident()
+                self.started.set()
+                while True:
+                    self.poll_once()
+                    with self.lock:
+                        empty = not self.entries
+                    if self.stopping.is_set() and empty:
+                        return
+                    self.wakeup.wait(0.005)
+                    self.wakeup.clear()
+        except BaseException as error:
+            # Keep all operations referenced; do not fabricate success or free KV.
+            self.owner_error = error
+            self.started.set()
+
+    def close(self, timeout=0.2):
+        with self.lock:
+            self.stopping.set()
+            self.success_outbox.clear()
+            for operation in self.entries.values():
+                operation.cancelled = True
+        self.wakeup.set()
+        if self.thread is not None:
+            self.thread.join(timeout=timeout)
+        with self.lock:
+            return (not self.entries and not self.retained_errors
+                    and self.owner_error is None
+                    and (self.thread is None or not self.thread.is_alive()))
+
+    def assert_safe_adapter_shutdown(self):
+        if not self.close():
+            raise RuntimeError(
+                "observer not device-quiescent; retain operations and do not close "
+                "transfer context without explicit safe unregister/reap protocol")
+
+
 class LMCacheMPWorkerAdapter:
     def __init__(
         self,
@@ -1881,6 +2065,35 @@
                 self._build_store_kv_events(key)
             )
 
+
+    def configure_ks_stage_observer(self, observer, identity_resolver):
+        # Explicit integration hook only; no fabricated generation/rank defaults.
+        if os.environ.get("KS_LMCACHE_STAGE_OBSERVER") != "1":
+            raise RuntimeError("LMCache stage observer is disabled")
+        if getattr(self, "_ks_stage_observer", None) is not None:
+            raise RuntimeError("stage observer already configured")
+        if not isinstance(observer, LMCacheStageObserver) or not callable(identity_resolver):
+            raise ValueError("observer and scheduler identity resolver required")
+        self._ks_stage_observer = observer
+        self._ks_stage_identity_resolver = identity_resolver
+
+    def _ks_reserve_stage_retrieve(self, request_id, op, event):
+        observer = getattr(self, "_ks_stage_observer", None)
+        if observer is None:
+            return None
+        if request_id in self.retrieve_futures:
+            raise ValueError("retrieve proxy is still tracked for request")
+        identity = self._ks_stage_identity_resolver(request_id, op)
+        if not isinstance(identity, StageRestoreIdentity) or identity.request_id != request_id:
+            raise ValueError("scheduler restore identity is absent or mismatched")
+        signature = (op.start, op.end, op.skip_first_n_tokens, tuple(op.flat_block_ids))
+        return observer.reserve(identity, signature, event)
+
+    def _ks_invalidate_stage_retrieves(self):
+        observer = getattr(self, "_ks_stage_observer", None)
+        if observer is not None:
+            observer.invalidate_requests(set(self.retrieve_futures))
+
     @_lmcache_nvtx_annotate
     def submit_retrieve_request(
         self,
@@ -1928,6 +2141,7 @@
                 "Transfer context is not initialized. "
                 "Call register_kv_caches() before submitting retrieve requests."
             )
+        _ks_operation = self._ks_reserve_stage_retrieve(request_id, op, event)
         with _ks_rpc_context(
             request_id=request_id, lookup_id=request_id,
             instance_id=self.instance_id, worker_id=self.worker_id,
@@ -1937,15 +2151,22 @@
                 "adapter.retrieve_submit_enter", token_start=op.start,
                 token_end=op.end, skip_first_n_tokens=op.skip_first_n_tokens,
             )
-            future = self.transfer_ctx.submit_retrieve(
-                request_id,
-                key,
-                self.kv_caches,
-                self._block_ids_per_group(op),
-                event,
-                self.blocks_in_chunk,
-                skip_first_n_tokens=op.skip_first_n_tokens,
-            )
+            try:
+                future = self.transfer_ctx.submit_retrieve(
+                    request_id,
+                    key,
+                    self.kv_caches,
+                    self._block_ids_per_group(op),
+                    event,
+                    self.blocks_in_chunk,
+                    skip_first_n_tokens=op.skip_first_n_tokens,
+                )
+            except BaseException as _ks_error:
+                if _ks_operation is not None:
+                    self._ks_stage_observer.submission_failed(_ks_operation, _ks_error)
+                raise
+            if _ks_operation is not None:
+                future = self._ks_stage_observer.attach(_ks_operation, future)
             _ks_trace(
                 "adapter.retrieve_submit_return", future_id=id(future),
                 raw_future_id=id(getattr(future, "raw_future_", future)),
@@ -2111,6 +2332,7 @@
                 finished_retrieves.add(request_id)
                 self.error_block_ids.update(r_block_ids)
                 self._failed_request_ids.add(request_id)
+            self._ks_invalidate_stage_retrieves()
             self.store_futures.clear()
             self.retrieve_futures.clear()
             self.store_events.clear()
@@ -2251,6 +2473,7 @@
                 finished_retrieves.add(request_id)
                 self.error_block_ids.update(r_block_ids)
                 self._failed_request_ids.add(request_id)
+            self._ks_invalidate_stage_retrieves()
             self.store_futures.clear()
             self.retrieve_futures.clear()
             self.store_events.clear()
@@ -2491,6 +2714,8 @@
         on the closing request client, and a straggler in-flight cycle cannot
         re-register or flip the health event after unregistration.
         """
+        if _ks_observer := getattr(self, "_ks_stage_observer", None):
+            _ks_observer.assert_safe_adapter_shutdown()
         self._kv_event_stop.set()
         with self._kv_event_lock:
             if self._kv_event_stream is not None:
```


### 阶段控制器：资格与批次生命周期

新增原型；用于 CPU 和合成张量设备协议，不是实际 scheduler 的完整替代。

```diff
--- /dev/null
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/controller/stage_controller.py
@@ -0,0 +1,273 @@
+"""PP2分段就绪控制器：只实现CPU可检验的控制规则。
+
+NOT_RUNTIME_INTEGRATED：尚未接入真实vLLM核心、Connector通知或设备通信。
+调用者必须从真实设备完成条件产生device_complete，不能从RPC接受ACK推导。
+本类由一个核心线程调用；不自行创建线程、不搬运KV、不执行模型。
+"""
+from dataclasses import dataclass, field
+from typing import Any
+
+BatchKey = tuple[str, int]
+RequestKey = tuple[str, int]
+_NO_RESULT = object()
+
+
+@dataclass(frozen=True)
+class StageWork:
+    """一份不可变阶段安排；前后段必须沿用相同成员与token位置。"""
+    key: BatchKey
+    stage: int
+    workers: tuple[int, ...]
+    requests: tuple[RequestKey, ...]
+    token_positions: tuple[tuple[int, ...], ...]
+
+
+@dataclass(frozen=True)
+class Commit:
+    key: BatchKey
+    result: Any
+    cancelled: bool
+
+
+@dataclass
+class _Restore:
+    generation: int
+    ready: list[set[int]] = field(default_factory=lambda: [set(), set()])
+    invalid: bool = False
+
+
+@dataclass
+class _Batch:
+    key: BatchKey
+    requests: tuple[RequestKey, ...]
+    token_positions: tuple[tuple[int, ...], ...]
+    state: list[str] = field(default_factory=lambda: ["waiting", "waiting"])
+    completed: list[set[int]] = field(default_factory=lambda: [set(), set()])
+    received: set[int] = field(default_factory=set)
+    buffer_reserved: bool = False
+    result: Any = _NO_RESULT
+    cancelled: bool = False
+    drained: set[int] = field(default_factory=set)
+
+
+class StageController:
+    """两段、每段一个计算名额；整批名额和中间结果缓冲分别记账。"""
+
+    def __init__(self, stage_workers, max_inflight_batches=2,
+                 max_buffered_activations=2):
+        groups = tuple(tuple(group) for group in stage_workers)
+        flat = [worker for group in groups for worker in group]
+        if (len(groups) != 2 or any(not group for group in groups)
+                or len(flat) != len(set(flat))
+                or any(type(worker) is not int or worker < 0 for worker in flat)):
+            raise ValueError("PP2需要两组非空且不重复的worker")
+        if (type(max_inflight_batches) is not int or max_inflight_batches < 1
+                or type(max_buffered_activations) is not int
+                or max_buffered_activations < 1):
+            raise ValueError("批次名额和缓冲名额必须为正整数")
+        self.stage_workers = groups
+        self.max_inflight_batches = max_inflight_batches
+        self.max_buffered_activations = max_buffered_activations
+        self._restores: dict[str, _Restore] = {}
+        self._batches: dict[BatchKey, _Batch] = {}
+        self._retired: set[BatchKey] = set()
+        self._busy: list[BatchKey | None] = [None, None]
+
+    @property
+    def inflight_count(self):
+        return len(self._batches)
+
+    @property
+    def buffered_activations(self):
+        return sum(batch.buffer_reserved for batch in self._batches.values())
+
+    def _worker(self, stage, worker):
+        if type(stage) is not int or stage not in (0, 1):
+            raise ValueError("PP阶段只能是0或1")
+        if worker not in self.stage_workers[stage]:
+            raise ValueError("worker不属于报告的PP阶段")
+
+    def begin_restore(self, request_id, generation):
+        """登记本次恢复；旧恢复不能覆盖仍被模型使用的请求。"""
+        if (not isinstance(request_id, str) or not request_id
+                or type(generation) is not int or generation < 0):
+            raise ValueError("请求ID和恢复代次无效")
+        previous = self._restores.get(request_id)
+        if previous and previous.generation == generation:
+            if previous.invalid:
+                raise ValueError("该恢复代次已失效，必须重新恢复并增加代次")
+            return
+        if previous and generation < previous.generation:
+            raise ValueError("不能重新登记旧恢复代次")
+        if any(any(req == request_id for req, _ in batch.requests)
+               for batch in self._batches.values()):
+            raise RuntimeError("请求仍有在途批次，不能替换恢复代次")
+        self._restores[request_id] = _Restore(generation)
+
+    def mark_kv_ready(self, request_id, generation, stage, worker, *, device_complete):
+        """只计入本段本代次、真实设备就绪的worker确认。"""
+        self._worker(stage, worker)
+        restore = self._restores.get(request_id)
+        if (not restore or restore.generation != generation
+                or restore.invalid or device_complete is not True):
+            return False
+        old_size = len(restore.ready[stage])
+        restore.ready[stage].add(worker)
+        return len(restore.ready[stage]) != old_size
+
+    def admit_batch(self, batch_id, generation, requests, token_positions):
+        """冻结逻辑批成员；入批不代表两段模型均可执行。"""
+        key = (batch_id, generation)
+        if (not isinstance(batch_id, str) or not batch_id
+                or type(generation) is not int or generation < 0):
+            raise ValueError("批次ID或代次无效")
+        if key in self._batches or key in self._retired:
+            raise ValueError("批次ID与代次不可重复使用")
+        if self.inflight_count >= self.max_inflight_batches:
+            raise RuntimeError("整批在途名额已满")
+        members = tuple(tuple(req) for req in requests)
+        positions = tuple(tuple(row) for row in token_positions)
+        if (not members or len(members) != len(positions)
+                or any(len(req) != 2 for req in members)
+                or len({req[0] for req in members}) != len(members)
+                or any(not row or any(type(pos) is not int or pos < 0 for pos in row)
+                       or tuple(sorted(set(row))) != row for row in positions)):
+            raise ValueError("批次成员与各请求token位置必须一一对应且有序")
+        active_requests = {req for batch in self._batches.values()
+                           for req, _ in batch.requests}
+        for request_id, restore_generation in members:
+            restore = self._restores.get(request_id)
+            if not restore or restore.generation != restore_generation:
+                raise ValueError("批次包含未登记的请求恢复代次")
+            if restore.invalid:
+                raise ValueError("批次包含已失效的恢复代次")
+            # 此最小原型不重排同一请求的多个在途token/分块。
+            if request_id in active_requests:
+                raise RuntimeError("同一请求仍有在途批次")
+        self._batches[key] = _Batch(key, members, positions)
+        return key
+
+    def take_ready(self, stage):
+        """只在本段依赖齐全时预留名额并给出一次执行安排。"""
+        if type(stage) is not int or stage not in (0, 1):
+            raise ValueError("PP阶段只能是0或1")
+        if self._busy[stage] is not None:
+            return None
+        for batch in self._batches.values():
+            if batch.cancelled:
+                # 未确认设备停止前，不跨过失败批去复用其通信顺序。
+                if not self._all_drained(batch):
+                    return None
+                continue
+            if batch.state[stage] == "done":
+                continue
+            if batch.state[stage] != "waiting":
+                return None
+            for request_id, generation in batch.requests:
+                restore = self._restores[request_id]
+                if (restore.generation != generation
+                        or restore.ready[stage] != set(self.stage_workers[stage])):
+                    return None
+            if stage == 0:
+                if self.buffered_activations >= self.max_buffered_activations:
+                    return None
+                batch.buffer_reserved = True
+            elif (batch.state[0] != "done"
+                  or batch.received != set(self.stage_workers[1])):
+                return None
+            batch.state[stage] = "dispatched"
+            self._busy[stage] = batch.key
+            return StageWork(batch.key, stage, self.stage_workers[stage],
+                             batch.requests, batch.token_positions)
+        return None
+
+    def mark_model_complete(self, key, stage, worker, *, device_complete):
+        """worker方法返回不够；只接纳真实设备完成后的确认。"""
+        self._worker(stage, worker)
+        batch = self._batches.get(key)
+        if not batch or batch.cancelled or device_complete is not True:
+            return False
+        if batch.state[stage] == "waiting":
+            raise RuntimeError("本段尚未派发，不能报告模型完成")
+        if batch.state[stage] == "done":
+            return False
+        batch.completed[stage].add(worker)
+        if batch.completed[stage] != set(self.stage_workers[stage]):
+            return False
+        batch.state[stage] = "done"
+        if stage == 0:
+            self._busy[stage] = None
+        else:
+            # 后段真实执行结束后，才允许复用本批输入中间结果缓冲。
+            batch.buffer_reserved = False
+            # 末段runner仍保留execute_model_state等待sample，计算名额暂不释放。
+        return True
+
+    def mark_activation_received(self, key, stage, worker, *, device_complete):
+        """收到的是哪一批的中间结果，必须和后段计算安排严格匹配。"""
+        self._worker(stage, worker)
+        if stage != 1:
+            raise ValueError("只有后段需要接收前段中间结果")
+        batch = self._batches.get(key)
+        if not batch or batch.cancelled or device_complete is not True:
+            return False
+        if batch.state[0] == "waiting":
+            raise RuntimeError("前段尚未派发，不能确认中间结果已到达")
+        old_size = len(batch.received)
+        batch.received.add(worker)
+        return len(batch.received) != old_size
+
+    def mark_final_result(self, key, result):
+        """采样等最终工作完成后再登记结果；这与后段forward完成分开。"""
+        batch = self._batches[key]
+        if batch.cancelled or batch.state[1] != "done":
+            raise RuntimeError("最后段尚未完成，不能登记最终结果")
+        if batch.result is not _NO_RESULT:
+            raise RuntimeError("最终结果不可重复登记")
+        batch.result = result
+        # 调用方确认sample及必要输出已真实完成，末段才可接下一批execute。
+        self._busy[1] = None
+
+    def cancel(self, key):
+        """停止新派发；不凭取消指令就释放未知是否仍在使用的设备资源。"""
+        batch = self._batches.get(key)
+        if batch:
+            batch.cancelled = True
+            for request_id, _ in batch.requests:
+                # 取消/失败后的设备内容与未结束恢复可能未知，禁止复用旧就绪位。
+                self._restores[request_id].invalid = True
+
+    def mark_drained(self, key, worker, *, device_complete):
+        batch = self._batches.get(key)
+        if worker not in self.stage_workers[0] + self.stage_workers[1]:
+            raise ValueError("未知worker不能确认停止")
+        if not batch or not batch.cancelled or device_complete is not True:
+            return False
+        old_size = len(batch.drained)
+        batch.drained.add(worker)
+        return len(batch.drained) != old_size
+
+    def _all_drained(self, batch):
+        return batch.drained == set(self.stage_workers[0] + self.stage_workers[1])
+
+    def pop_committable(self):
+        """按原逻辑批顺序提交；前段完成不会提前释放整批名额。"""
+        if not self._batches:
+            return None
+        key = next(iter(self._batches))
+        batch = self._batches[key]
+        if batch.cancelled:
+            if not self._all_drained(batch):
+                return None
+            result = None
+        else:
+            if batch.state[1] != "done" or batch.result is _NO_RESULT:
+                return None
+            result = batch.result
+        for stage in (0, 1):
+            if self._busy[stage] == key:
+                self._busy[stage] = None
+        batch.buffer_reserved = False
+        del self._batches[key]
+        self._retired.add(key)
+        return Commit(key, result, batch.cancelled)
```


### 恢复绑定：请求和代次

新增绑定；真实事件组件组合已调用，真实调度元数据仍未接线。

```diff
--- /dev/null
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/restore_event_binding/restore_binding.py
@@ -0,0 +1,47 @@
+"""Explicit observer drain and core-thread registration. NOT_RUNTIME_INTEGRATED."""
+from __future__ import annotations
+
+
+def publish_observer_success(observer, publisher):
+    """Forward only already-confirmed identities; never query a device Future.
+
+    A publisher failure must fail the channel/caller, not be treated as delivery.
+    This explicit drain does not schedule an automatic runtime bridge thread.
+    """
+    identities = observer.drain_success()
+    for identity in identities:
+        publisher.publish_restore_ready(identity)
+    return len(identities)
+
+
+class RestoreEventBinding:
+    """Core callback records individual workers of an existing restore only."""
+    def __init__(self, controller, restore_event_type, *, tp_size=8, pcp_size=1,
+                 on_stage_event=None):
+        if (type(tp_size) is not int or tp_size < 1 or pcp_size != 1
+                or controller.stage_workers != (
+                    tuple(range(tp_size)), tuple(range(tp_size, 2 * tp_size)))):
+            raise ValueError("restore binding requires explicit single-node PP2/PCP1 layout")
+        if on_stage_event is not None and not callable(on_stage_event):
+            raise ValueError("existing stage event handler must be callable")
+        self.controller = controller
+        self.restore_event_type = restore_event_type
+        self.tp_size = tp_size
+        self.on_stage_event = on_stage_event
+
+    def __call__(self, event):
+        if not isinstance(event, self.restore_event_type):
+            if self.on_stage_event is not None:
+                if self.on_stage_event(event) is not None:
+                    raise ValueError("stage event callback must return None")
+            return None
+        event.validate()
+        if (event.stage != event.worker_id // self.tp_size
+                or event.tp_rank != event.worker_id % self.tp_size
+                or event.pcp_rank != 0):
+            raise ValueError("restore callback rank identity mismatch")
+        # Deliberately never call begin_restore here: unknown/stale events cannot
+        # create state. One worker's readiness cannot stand for all PP/TP workers.
+        self.controller.mark_kv_ready(event.request_id, event.restore_generation,
+                                      event.stage, event.worker_id, device_complete=True)
+        return None
```


### vLLM-Ascend：接收与计算拆分

基于既有 8 侧 worker 归档的候选；未部署为真实模型推理。

```diff
--- 04_修改策略与验证/01_修改策略与隔离候选/KV完成通知修改策略与隔离分支_20261022/capacity_followup/sources/m8/vllm_ascend/worker/worker.py
+++ 04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型/worker_split/worker.py
@@ -609,11 +609,217 @@
             self.torch_allocated / GiB_bytes,
         )
 
+    def _ks_pp_batch_signature(self, scheduler_output):
+        # 分段必须沿用相同的请求顺序、token位置和KV块，不能只比总token数。
+        tokens = tuple(scheduler_output.num_scheduled_tokens.items())
+        if not tokens or any(type(n) is not int or n <= 0 for _, n in tokens):
+            raise ValueError("stage execution requires positive scheduled tokens")
+        if sum(n for _, n in tokens) != scheduler_output.total_num_scheduled_tokens:
+            raise ValueError("scheduled token total differs from request members")
+        new = tuple(
+            (r.req_id, r.num_computed_tokens, repr(r.block_ids))
+            for r in scheduler_output.scheduled_new_reqs
+        )
+        cached = scheduler_output.scheduled_cached_reqs
+        return (tokens, new, tuple(cached.req_ids), tuple(cached.num_computed_tokens), repr(cached.new_block_ids))
+
+    def _ks_pp_stage_record(self, batch_key, scheduler_output=None, create=False):
+        if (not isinstance(batch_key, tuple) or len(batch_key) != 2
+                or not isinstance(batch_key[0], str) or not batch_key[0]
+                or type(batch_key[1]) is not int or batch_key[1] < 0):
+            raise ValueError("batch key must be (non-empty str, non-negative int generation)")
+        retired = getattr(self, "_ks_pp_stage_retired", set())
+        if batch_key in retired:
+            raise RuntimeError("a retired logical batch cannot execute again")
+        record = getattr(self, "_ks_pp_stage_current", None)
+        if record is None:
+            if not create:
+                raise KeyError(batch_key)
+            record = {
+                "key": batch_key,
+                "signature": self._ks_pp_batch_signature(scheduler_output),
+                "input": None, "handles": None, "input_event": None,
+                "model_event": None, "send_event": None,
+                "executed": False, "failed": False,
+            }
+            self._ks_pp_stage_current = record
+        if record["key"] != batch_key:
+            raise RuntimeError("another logical batch owns this stage buffer")
+        if record["failed"]:
+            raise RuntimeError("failed stage batch requires coordinated abort; retry is forbidden")
+        if scheduler_output is not None and record["signature"] != self._ks_pp_batch_signature(scheduler_output):
+            raise ValueError("logical batch members, positions or block IDs changed")
+        return record
+
+    def _ks_pp_handles_done(self, handles):
+        # 只使用真实通信句柄的完成接口；没有接口、查询失败均不放行。
+        if handles is None:
+            return False
+        for handle in handles:
+            probe = getattr(handle, "is_completed", None)
+            if not callable(probe):
+                return False
+            try:
+                if probe() is not True:
+                    return False
+            except Exception:
+                return False
+        return True
+
+    def prepare_pp_stage_input(self, batch_key, scheduler_output):
+        """Receive-only control operation, never a model forward.
+
+        irecv_tensor_dict still receives CPU metadata synchronously. The executor
+        must enqueue this non-blocking RPC before waiting for the front result.
+        """
+        if get_pp_group().is_first_rank:
+            raise ValueError("first PP stage has no upstream activation")
+        record = self._ks_pp_stage_record(batch_key, scheduler_output, create=True)
+        if record["input"] is not None:
+            return {"batch_key": batch_key, "prepared": True}
+        try:
+            tensor_dict, handles, postprocess = get_pp_group().irecv_tensor_dict(
+                all_gather_group=None if enable_sp() else get_tp_group()
+            )
+            if tensor_dict is None:
+                raise RuntimeError("PP receive returned no activation buffer")
+            record["handles"] = handles
+            record["input"] = AsyncIntermediateTensors(
+                tensor_dict, comm_handles=handles, comm_postprocess=postprocess
+            )
+        except Exception:
+            record["failed"] = True
+            raise
+        return {"batch_key": batch_key, "prepared": True}
+
+    def pp_stage_input_received(self, batch_key):
+        record = self._ks_pp_stage_record(batch_key)
+        return record["input"] is not None and self._ks_pp_handles_done(record["handles"])
+
+    def finalize_pp_stage_input(self, batch_key):
+        """Run collectively across the TP stage only after every rank received.
+
+        Postprocess may issue TP all-gather. Never invoke it from rank-local
+        readiness polling, since other TP members may not be ready yet.
+        """
+        record = self._ks_pp_stage_record(batch_key)
+        if record["input_event"] is not None:
+            return
+        if not self.pp_stage_input_received(batch_key):
+            raise RuntimeError("communication has not completed")
+        try:
+            # wait() also establishes the device-stream dependency; query alone
+            # must not be mistaken for permission to read the received tensor.
+            record["input"].wait_for_comm()
+            event = torch.npu.Event()
+            event.record()
+            record["input_event"] = event
+        except Exception:
+            record["failed"] = True
+            raise
+
+    def pp_stage_input_ready(self, batch_key):
+        record = self._ks_pp_stage_record(batch_key)
+        event = record["input_event"]
+        if event is None:
+            return False
+        try:
+            return event.query() is True
+        except Exception:
+            return False
+
+    def execute_pp_stage_model(self, batch_key, scheduler_output):
+        """Execute this PP stage once. Scheduler must already prove local KV ready.
+
+        This method verifies activation/ownership, not LMCache restore state.
+        It must be invoked for every TP/EP participant of the selected stage.
+        """
+        record = self._ks_pp_stage_record(batch_key, scheduler_output, create=True)
+        if record["executed"]:
+            raise RuntimeError("logical stage batch has already executed")
+        if self.model_runner.execute_model_state is not None:
+            raise RuntimeError("previous model output must be sampled first")
+        if not self._ks_pp_handles_done(self._pp_send_work):
+            raise RuntimeError("previous PP send still owns the output buffer")
+        if not get_pp_group().is_first_rank and not self.pp_stage_input_ready(batch_key):
+            raise RuntimeError("this PP stage activation is not ready")
+        record["executed"] = True
+        self._ks_pp_stage_running = record
+        try:
+            result = self.execute_model(scheduler_output)
+            event = torch.npu.Event()
+            event.record()
+            record["model_event"] = event
+            return result
+        except Exception:
+            record["failed"] = True
+            raise
+        finally:
+            self._ks_pp_stage_running = None
+
+    def pp_stage_model_ready(self, batch_key):
+        record = self._ks_pp_stage_record(batch_key)
+        event = record["model_event"]
+        if event is None:
+            return False
+        try:
+            return event.query() is True
+        except Exception:
+            return False
+
+    def pp_stage_release_ready(self, batch_key, final_output_consumed=False):
+        """Poll normal pending state without turning it into an RPC failure.
+
+        Holds every tensor reference. All TP members must report True before
+        the executor sends the separate release operation to the stage.
+        """
+        record = self._ks_pp_stage_record(batch_key)
+        if not record["executed"] or not self.pp_stage_model_ready(batch_key):
+            return False
+        if self.model_runner.execute_model_state is not None:
+            return False
+        # 非末段也可能保留Connector输出，必须经过sample路径消费；不能
+        # 因execute_model_state天然为None就让下一批覆盖这份完成消息。
+        if getattr(self.model_runner, "kv_connector_output", None) is not None:
+            return False
+        if get_pp_group().is_last_rank and final_output_consumed is not True:
+            return False
+        if not self._ks_pp_handles_done(self._pp_send_work):
+            return False
+        if self._pp_send_work:
+            if record["send_event"] is None:
+                for handle in self._pp_send_work:
+                    handle.wait()
+                event = torch.npu.Event()
+                event.record()
+                record["send_event"] = event
+            try:
+                send_ready = record["send_event"].query() is True
+            except Exception:
+                send_ready = False
+            if not send_ready:
+                return False
+        return True
+
+    def release_pp_stage_batch(self, batch_key, final_output_consumed=False):
+        record = self._ks_pp_stage_record(batch_key)
+        if not self.pp_stage_release_ready(batch_key, final_output_consumed):
+            raise RuntimeError("stage release was sent before all device/output dependencies completed")
+        self._pp_send_work = []
+        self._ks_pp_retained_send_tensors = None
+        retired = getattr(self, "_ks_pp_stage_retired", set())
+        retired.add(batch_key)
+        self._ks_pp_stage_retired = retired
+        self._ks_pp_stage_current = None
+
     @_ks_pipeline_step
     def execute_model(
         self,
         scheduler_output: "SchedulerOutput",
     ) -> ModelRunnerOutput | AsyncModelRunnerOutput | None:
+        if (getattr(self, "_ks_pp_stage_current", None) is not None
+                and getattr(self, "_ks_pp_stage_running", None) is None):
+            raise RuntimeError("stage batch owns worker state; do not mix legacy execute_model calls")
         self.log_memory_stats()
         # enable msMonitor to monitor the performance of vllm-ascend
         if get_ascend_config().msmonitor_use_daemon:
@@ -634,16 +840,21 @@
                 all_gather_group = None
             else:
                 all_gather_group = get_tp_group()
-            with _ks_span("pipeline.pp_metadata_receive"):
-                tensor_dict, comm_handles, comm_postprocess = get_pp_group().irecv_tensor_dict(
-                    all_gather_group=all_gather_group
+            stage_record = getattr(self, "_ks_pp_stage_running", None)
+            if stage_record is not None:
+                # 新入口只消费已完成且归属于此逻辑批的接收结果，不再次接收。
+                intermediate_tensors = stage_record["input"]
+            else:
+                with _ks_span("pipeline.pp_metadata_receive"):
+                    tensor_dict, comm_handles, comm_postprocess = get_pp_group().irecv_tensor_dict(
+                        all_gather_group=all_gather_group
+                    )
+                assert tensor_dict is not None
+                intermediate_tensors = AsyncIntermediateTensors(
+                    tensor_dict,
+                    comm_handles=comm_handles,
+                    comm_postprocess=comm_postprocess,
                 )
-            assert tensor_dict is not None
-            intermediate_tensors = AsyncIntermediateTensors(
-                tensor_dict,
-                comm_handles=comm_handles,
-                comm_postprocess=comm_postprocess,
-            )
 
         if self.profiler is not None:
             self.profiler.step()
@@ -662,6 +873,9 @@
             all_gather_group = None
         else:
             all_gather_group = get_tp_group()
+        if getattr(self, "_ks_pp_stage_running", None) is not None:
+            # 句柄/设备完成之前保留源tensor，不因Python方法返回就复用缓冲。
+            self._ks_pp_retained_send_tensors = output.tensors
         self._pp_send_work = get_pp_group().isend_tensor_dict(
             output.tensors,
             all_gather_group=all_gather_group,
```
