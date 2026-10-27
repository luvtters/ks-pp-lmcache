"""读取已归档源码与结果，刷新统一讲解；不连接服务器，不运行候选测试。"""
from pathlib import Path
from html import escape
import difflib
import json
import markdown
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter

HERE = Path(__file__).resolve().parent
OUT = next(p for p in HERE.parents if p.name == "output")
PROTO = HERE.parent / "20261027_PP分段就绪原型"
OLD = HERE.parent / "KV完成通知修改策略与隔离分支_20261022"
EV = OUT / "05_当前环境与实验准备/20261027_25环境准备"
BASE = EV / "25镜像源码与配置快照/sources/vllm/vllm"
STEM = "PP分段就绪调度_源码依据与修改设计"
TITLE = "PP分段就绪调度：修改讲解与验证结果"
index, sections = [], []


def read(p):
    return p.read_text(encoding="utf-8")


def rel(p):
    return p.relative_to(OUT).as_posix()


def md(text):
    return markdown.markdown(text, extensions=["tables", "fenced_code", "codehilite", "sane_lists"], extension_configs={"codehilite": {"guess_lang": False}})


def add(key, title, body, extra=""):
    sections.append(dict(key=key, title=title, body=body.strip(), extra=extra))


def code(title, p, start, end, explanation):
    lines = read(p).splitlines()[start-1:end]
    assert len(lines) == end-start+1
    index.append(dict(label=title, source=rel(p), begin=start, end=end, lines=lines))
    return f"\n### {title}\n\n{explanation}\n\n```python\n# 阅读提示：上方为中文解读；下面保留真实源码，不是伪代码。\n" + "\n".join(lines) + f"\n```\n\n来源：`{rel(p)}`，第 {start}—{end} 行。\n"


add("summary", "先把结论讲清楚", """
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
""")

diagram = """<div class="timeline-toolbar" role="group" aria-label="流程对照"><button data-mode="old" aria-pressed="true">原程序：整请求放行</button><button data-mode="new" aria-pressed="false">修改目标：按阶段放行</button></div><p class="diagram-scope">下图只表示依赖先后，不按毫秒比例绘制。两条请求固定不换代号。新流程中，完成通知链已做组件验证；真实 GLM 分段计算尚未接通。</p><div class="timeline-scroll"><div id="timeline" aria-live="polite"></div></div><div id="timeline-note" class="callout"></div>"""
add("flow", "从两条请求看：究竟少等哪一步", """
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
""", diagram)

add("changes", "这版实际改了哪些地方", """
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
""")

body = "以下是原代码和当前隔离候选的连续原文摘录。先读中文解释，再对应函数；这些代码不代表完整框架已经运行。\n"
snippets = [
    ("原调度器：整请求恢复完成才放行", BASE / "v1/core/sched/scheduler.py", 2192, 2203, "请求 ID 不在完成集合就返回 False。最新候选尚未改此函数，因此不能说收到前段事件后，原 scheduler 已自动安排计算。"),
    ("原核心：直接等待模型结果", BASE / "v1/engine/core.py", 554, 565, "Future 是尚未取得的结果凭据。核心进入 future.result() 后，返回前不能继续后面的正常结果处理。"),
    ("旧模型回复 FIFO 保留", PROTO / "draft_wait_followup/multiproc_executor.py", 246, 254, "模型结果仍按提交次序读取。新方案没有让第二个线程乱序抢读旧队列，而是给恢复事件另开通道。"),
    ("LMCache：真实设备状态由唯一线程查询", PROTO / "lmcache_stage_observer/stage_observer.py", 126, 142, "owner 指唯一可接触真实设备 Future 的线程。query 确认完成后才取结果；后面还要求成功值严格为 True，False 或异常不发布 READY。旧 getter 读取 CPU 代理。"),
    ("核心：按身份登记单 worker 就绪", PROTO / "restore_event_binding/restore_binding.py", 38, 47, "恢复代次表示本请求第几次恢复。旧代次或未知事件不能创建恢复记录；一个 worker 的确认也不能代表全段。当前显式限定 PCP=1，TP 身份取自消息核验。"),
    ("新核心：等待期间仍处理事件", PROTO / "draft_wait_followup/core.py", 575, 586, "唯一结果线程等模型，核心主线程运行此循环。回调出错也保留模型等待与资源，等真实结果结束后再报错。正常回调只处理阶段事实，不提前提交模型结果。"),
    ("调度器模型更新仍等真实结果", PROTO / "draft_wait_followup/core.py", 728, 740, "直到真实模型结果返回才调用 update_from_output。提前处理 KV 通知，与提前宣布整个模型批结束是两回事。"),
    ("阶段控制器：只检查本段的完整依赖", PROTO / "controller/stage_controller.py", 166, 177, "前段检查本批所有请求、本段全部 TP 成员的 KV 确认，并预留缓冲。后段还要求前段计算完成和本段全 TP 接收完成。这是原型资格判断，尚不是实际 scheduler 的分段派发。"),
    ("Ascend worker：后段先准备接收", PROTO / "worker_split/worker.py", 675, 686, "后段即便还不能算，也要有人接住前段张量。此方法独立建立接收记录。CPU 元信息接收可能阻塞，实际调用必须非阻塞排入，不能先同步等接收返回再启动前段。"),
    ("通信完成仍要建立设备使用依赖", PROTO / "worker_split/worker.py", 705, 716, "query 和 Python 方法返回不是允许模型直接读取张量的同义词。wait 建立计算流依赖，再记录设备事件；可能涉及 TP all-gather 的后处理要由本段全 TP 一致推进。"),
    ("MTP：保留同步接口，新增异步取结果", PROTO / "draft_wait_followup/multiproc_executor.py", 604, 609, "取草稿仍使用原 output rank，MTP3 与实际计算不变。先返回 Future，让核心在等待时处理独立事件。"),
    ("草稿广播仍由核心提交", PROTO / "draft_wait_followup/core.py", 548, 559, "核心线程写广播，唯一 reader 等回复，不让结果线程接管命令写入。model_batch=False 表示草稿等待不会新增或释放模型批名额。"),
]
for args in snippets:
    body += code(*args)
add("code", "对应代码：哪里拦住，哪里拆开", body)

result = json.loads(read(EV / "设备协议诊断/设备诊断汇总.json"))
assert result["stage_dependency"]["all_ranks_pass"]
assert result["real_event_to_core"]["scheduler_updates_at_callback"] == 0
evidence = {k: result[k] for k in ["stage_dependency", "real_event_to_core", "dual_inflight"]}
add("validation", "验证结果：已经走到哪一步", """
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
""" + json.dumps(evidence, ensure_ascii=False, indent=2) + "\n```\n")

add("gaps", "哪些等待保留，怎样才算真的改好", """
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
""")

entries = []
for folder, name in [("control_sidechannel", "test_control_sidechannel.py"), ("lmcache_stage_observer", "test_lmcache_stage_observer.py"), ("core_event_pump", "test_core_event_pump.py"), ("restore_event_binding", "test_restore_binding.py"), ("draft_wait_followup", "test_draft_wait_followup.py"), ("dual_inflight_probe", "probe_dual_inflight.py")]:
    path = PROTO / folder / name
    if not path.exists():
        choices = list((PROTO / folder).glob("test*.py"))
        assert len(choices) == 1, (folder, choices)
        path = choices[0]
    entries.append(f'python -X utf8 -B "{path}"')
add("reproduce", "文件、复现入口与启用状态", """
本次报告整理只在本地 Windows `E:\\work\\ks` 读文件、生成文档，**未 SSH、未占卡、未改推理源码、未启停服务，也未重跑候选 CPU 测试**。

已归档隔离环境为 25 上无 NPU 挂载的自有容器 `ks-luv-m25-prep`，工作区 `/opt/ks-stage-ready/framework/vllm`，分支 `ks/pp-stage-ready-luv-20261027`。核心／执行器侧候选应用在隔离 worktree，默认未启用；安装目录 `/vllm-workspace/vllm` 未改。LMCache 观察器和 Ascend worker 拆分为隔离候选材料。

核心／执行器累计 2 文件、501 行新增／6 行删除；**这是两个文件的范围，不是整套方案总代码量**。完整运行接线尚未完成，因此不提供虚构的“已能运行”`vllm serve` 命令或开关。

### CPU 复核命令（本报告未重新执行）

机器：本地 Windows；容器：无；工作目录：`E:\\work\\ks`。以下入口不加载模型、不调用 NPU，但会刷新对应测试回执；如需保留历史原件，应先复制候选目录到个人工作目录再运行。

```powershell
""" + "\n".join(entries) + """
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
""")


def syntax(s):
    return highlight(s, PythonLexer(), HtmlFormatter(nowrap=True)).rstrip("\n") or " "


def diff(title, old, new, scope):
    a = read(old).splitlines() if old else []
    b = read(new).splitlines()
    left_rows, right_rows = [], []
    for group in difflib.SequenceMatcher(None, a, b, autojunk=False).get_grouped_opcodes(3):
        for rows in (left_rows, right_rows):
            rows.append('<tr class="gap"><td colspan="2">… 改动与相邻上下文 …</td></tr>')
        for tag, x, y, z, w in group:
            for i in range(max(y-x, w-z)):
                l = x+i if x+i < y else None
                r = z+i if z+i < w else None
                lc = "removed" if l is not None and tag in ("replace", "delete") else ""
                rc = "added" if r is not None and tag in ("replace", "insert") else ""
                left_rows.append(f'<tr><td class="ln {lc}">{l+1 if l is not None else ""}</td><td class="{lc}"><code>{syntax(a[l]) if l is not None else " "}</code></td></tr>')
                right_rows.append(f'<tr><td class="ln {rc}">{r+1 if r is not None else ""}</td><td class="{rc}"><code>{syntax(b[r]) if r is not None else " "}</code></td></tr>')
    panes = []
    for heading, rows in (("原归档／文件不存在", left_rows), ("当前隔离候选", right_rows)):
        panes.append(f'<div class="diff-pane"><div class="diff-pane-title">{heading}</div><div class="diff-scroll diff-pane-scroll"><table class="diff-table"><colgroup><col style="width:55px"><col></colgroup><tbody>' + "".join(rows) + '</tbody></table></div></div>')
    h = f'<details class="full-diff"><summary>{escape(title)}</summary><p>{escape(scope)}</p><p class="source-path">候选：{escape(rel(new))}</p><div class="diff-scroll"><div class="diff-grid">' + "".join(panes) + '</div></div></details>'
    text = "\n".join(difflib.unified_diff(a, b, fromfile=rel(old) if old else "/dev/null", tofile=rel(new), n=3, lineterm=""))
    return h, f"\n### {title}\n\n{scope}\n\n```diff\n{text}\n```\n"


diffs = [
    ("vLLM 核心：等待与事件消费", BASE / "v1/engine/core.py", PROTO / "draft_wait_followup/core.py", "相对 25 原始核心的累计候选；隔离 worktree 已应用、默认关闭，包含 MTP 草稿等待。"),
    ("vLLM 执行器：独立通道与草稿接口", BASE / "v1/executor/multiproc_executor.py", PROTO / "draft_wait_followup/multiproc_executor.py", "隔离 worktree 已应用、默认关闭。保留旧模型 FIFO，增加独立事件及恢复身份。"),
    ("LMCache：恢复观察器", None, PROTO / "lmcache_stage_observer/stage_observer.py", "新增隔离模块；尚未接入正常模型启动。真实设备唯一 owner、CPU 代理、成功 outbox 和关闭检查。"),
    ("LMCache：adapter 对接", OLD / "lmcache_only_followup/source_current/lmcache/integration/vllm/vllm_multi_process_adapter.py", PROTO / "lmcache_stage_observer/vllm_multi_process_adapter.py", "基于既有 adapter 归档的隔离候选，旧观测和兼容代码保留；未修改安装版本。"),
    ("阶段控制器：资格与批次生命周期", None, PROTO / "controller/stage_controller.py", "新增原型；用于 CPU 和合成张量设备协议，不是实际 scheduler 的完整替代。"),
    ("恢复绑定：请求和代次", None, PROTO / "restore_event_binding/restore_binding.py", "新增绑定；真实事件组件组合已调用，真实调度元数据仍未接线。"),
    ("vLLM-Ascend：接收与计算拆分", OLD / "capacity_followup/sources/m8/vllm_ascend/worker/worker.py", PROTO / "worker_split/worker.py", "基于既有 8 侧 worker 归档的候选；未部署为真实模型推理。"),
]
diff_h, diff_m = [], []
for args in diffs:
    h, m = diff(*args)
    diff_h.append(h)
    diff_m.append(m)

css = """
:root{--ink:#1c2c44;--muted:#53667c;--blue:#185abd;--line:#dce5ee}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#f4f7fb;color:var(--ink);font:16px/1.85 'Segoe UI','Microsoft YaHei',sans-serif}a{color:var(--blue);text-decoration:none}a:hover{text-decoration:underline}.top{padding:18px 30px;background:white;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;gap:15px}.top span{font-size:13px;color:var(--muted)}.layout{display:grid;grid-template-columns:210px minmax(0,1120px);gap:35px;max-width:1450px;padding:32px 28px;margin:auto}.sidebar{align-self:start;position:sticky;top:20px}.sidebar a{display:block;padding:10px;border-left:3px solid transparent;font-size:14px;color:var(--muted)}.sidebar a.active{border-color:var(--blue);background:#e9f1ff;color:var(--blue)}main{min-width:0}header{padding:12px 0 28px}.eyebrow{color:var(--blue);font-size:14px;font-weight:650}h1{font-size:37px;line-height:1.4;margin:12px 0 20px;letter-spacing:-1px}h2{font-size:27px;line-height:1.5;margin:0 0 23px}h3{font-size:20px;margin:30px 0 14px}section{background:white;border:1px solid var(--line);border-radius:10px;padding:30px;margin-bottom:25px;scroll-margin-top:20px}p{margin:14px 0}li{margin:9px 0}strong{color:#153967}code{font:13px/1.7 Consolas,monospace}p code,li code{background:#edf2f8;padding:2px 5px;border-radius:4px;overflow-wrap:anywhere}.table-wrap{overflow-x:auto;margin:20px 0}table{border-collapse:collapse;width:100%;font-size:14px;line-height:1.75}td,th{padding:13px;border-bottom:1px solid var(--line);vertical-align:top;text-align:left}th{background:#edf3fb;color:#29446a}td:first-child{font-weight:600}.chips{display:flex;flex-wrap:wrap;gap:8px}.chip{background:#e4f3eb;color:#226b4d;padding:6px 10px;border-radius:4px;font-size:13px}.chip.pending{background:#fff0da;color:#8f590c}.callout{padding:16px 19px;margin:18px 0;background:#edf4ff;border-left:4px solid var(--blue);border-radius:4px}.codehilite{background:#142033;color:#e0e7f3;border-radius:7px;overflow-x:auto;margin:18px 0}.codehilite pre{margin:0;padding:20px;font-size:13px;line-height:1.65;white-space:pre;overflow-wrap:normal;word-break:normal}.codehilite code{font-size:inherit}.timeline-toolbar{display:flex;flex-wrap:wrap;gap:8px;margin-top:24px}button{cursor:pointer;border:1px solid #acbdd4;color:#294864;border-radius:6px;background:white;padding:11px 15px;font:600 14px 'Microsoft YaHei',sans-serif}button[aria-pressed=true]{color:white;background:var(--blue);border-color:var(--blue)}.diagram-scope,.source-path{font-size:13px;color:var(--muted);overflow-wrap:anywhere}.timeline-scroll{overflow-x:auto;margin-top:20px}#timeline{min-width:780px}.lane-head,.time-row{display:grid;grid-template-columns:65px repeat(4,minmax(0,1fr));gap:8px}.lane-head{font-size:13px;font-weight:650;margin-bottom:10px}.lane-head div{background:#e7eff9;border-radius:4px;text-align:center;padding:10px}.time-row{min-height:104px;border-top:1px dashed #d1dcec;padding:8px 0}.tick{padding:12px 4px;color:#63748e;font-size:13px}.event{padding:10px;border-radius:5px;border-left:3px solid #3e7fc7;background:#edf5ff;font-size:13px;line-height:1.7}.event b{display:block;font-size:14px}.event.wait{background:#fff0d6;border-left-color:#c38a2d}.event.done{background:#e4f3eb;border-left-color:#227452}.full-diff{border:1px solid var(--line);border-radius:7px;margin:14px 0}.full-diff summary{cursor:pointer;background:#edf3fb;padding:14px;font-weight:650}.full-diff>p{padding:0 15px}.diff-scroll{overflow-x:auto}.diff-table{width:1900px;table-layout:fixed;font-family:Consolas,monospace;font-size:12px;line-height:1.65;background:#f9fbfd}.diff-table th{text-align:center}.diff-table td{padding:2px 7px;border:0;white-space:pre;font-weight:400;overflow:hidden}.diff-table code{font-size:12px}.diff-table .ln{background:#eaf0f5;color:#748398;text-align:right;user-select:none}.diff-table td:nth-child(2){border-right:1px solid #becddb}.diff-table .removed{background:#ffe7e9}.diff-table .added{background:#dff4e6}.diff-table .gap td{background:#e6edf8;color:#4a668b;padding:7px}.diff-table .c1,.diff-table .c{color:#5f7568}.footer{font-size:13px;color:var(--muted);padding:12px 0 25px}@media(max-width:1050px){.layout{grid-template-columns:165px minmax(0,1fr);gap:20px;padding:24px 18px}section{padding:24px}h1{font-size:32px}}@media(max-width:720px){.top{display:block;padding:14px 16px}.top span{display:block}.layout{display:block;padding:16px 12px}.sidebar{position:static;display:flex;overflow-x:auto;margin-bottom:15px}.sidebar a{white-space:nowrap;border-left:0;border-bottom:3px solid transparent}.sidebar a.active{border-bottom-color:var(--blue)}section{padding:20px 16px}h1{font-size:29px}h2{font-size:24px}h3{font-size:19px}td,th{padding:10px;min-width:145px}.diff-table td{min-width:0}.codehilite pre{font-size:12px;padding:14px}p{overflow-wrap:anywhere}}@media print{.top,.sidebar,button{display:none}.layout{display:block;padding:0}body{background:white}section{border:0;padding:16px 0}.full-diff{display:none}}
""" + HtmlFormatter(style="monokai").get_style_defs(".codehilite") + HtmlFormatter(style="friendly").get_style_defs(".diff-table") + """
/* 长行保留原文；横向滚动容器，不裁掉单元格里的代码。 */
.diff-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);min-width:820px}
.diff-pane{min-width:0;border-right:1px solid #c5d1e0}
.diff-pane-title{padding:12px;text-align:center;background:#eaf1fb;color:#24466e;font-weight:650;font-size:14px}
.diff-pane .diff-table{min-width:100%;width:max-content;table-layout:auto}
.diff-pane .diff-table td{height:24px;line-height:20px}
.diff-table td{overflow:visible}.diff-table code{white-space:pre}
"""

js = """
const rows={old:[
['①','核心安排计算','正在计算的请求获得安排；缓存命中的请求异步恢复，本轮不算剩余token。','下发模型批','同一执行安排交给前后段。','前段计算','正在计算的请求通过前41层。','处理已有工作','是否空闲取决于在途批，图中不假定后段一直空闲。'],
['②','核心等待模型结果','暂不能继续正常结果处理。','原回复路径尚未结束','KV完成标记仍经模型输出路径汇总。','交给后段中间结果','这是计算张量，不是KV完成标记。','后段继续计算','正在计算的请求通过后37层及后续采样。'],
['③','还没处理到足够的KV确认','缓存命中的请求未重新获得计算安排。','仍等待本批结果','worker的KV确认不代表模型批已完成。','缓存KV可能已恢复好','不等于前段始终空闲；仍可做合法安排的其他工作。','缓存KV也可能已恢复好','恢复是另一动作，不要求两段顺序恢复。'],
['④','处理真实结果与完成信息','按请求ID更新；完成确认齐全后回到可调度状态。','交回真实模型结果','本模型批按序结束。','等待下一次合法安排','请求不会自行开始计算。','等待下一次合法安排','仍须完成剩余token的前后段计算。'],
['⑤','下一轮安排剩余token','整请求已好，且资源与顺序满足。','下发后续模型计算','不固定要求恰好相隔两个模型批。','计算剩余token的前41层','读取恢复到本段的历史KV。','计算后37层','读取本段KV和同批前段中间结果，形成输出。']
],new:[
['①','原模型批仍在进行','正在计算的请求的最终结果还没回来。','唯一reader按序等旧模型结果','核心线程不再一直卡在旧future.result。','本段KV真实就绪','缓存命中的请求：本段全TP所需确认齐全。','本段KV可能还没好','没有宣布恢复完成，也没有强行启动模型。'],
['②','先处理独立恢复事件','登记请求／代次／阶段／worker；不更新模型结果。','独立队列交付完成标记','没有大块KV，不等待旧模型输出。','获得前段执行资格','仍需真实调度、合法批次、名额和资源。','先准备接收中间结果','接收和后段模型计算分开。'],
['③','条件满足才派发前段','真实GLM的此处调度尚未接通。','只下发就绪阶段','不会凭通知多开一个模型批。','前段计算并发送结果','缓存命中的请求：剩余token通过前41层。','接收并保留中间结果','本段KV没好时仍不计算。'],
['④','后段依赖齐全才放行','本段全TP KV＋前段完成＋同批中间结果。','派发后段阶段工作','保持同一逻辑批身份与token位置。','发送安全完成前保留源','不提前复用tensor或KV缓冲。','后段计算剩余token','通过后37层，形成真实输出。'],
['⑤','最终结果按原序更新','没有提前宣布整批完成。','模型结果FIFO保留','事件早处理，模型输出仍有序。','按生命周期回收','阶段完成不等于整批结果已消费。','采样与结果返回','完整模型生命周期还需真实整链验收。']
]};
function show(mode){document.getElementById('timeline').innerHTML='<div class="lane-head"><div>向下为序</div><div>核心／调度器</div><div>执行器／返回通路</div><div>前段worker组 · TP8</div><div>后段worker组 · TP8</div></div>'+rows[mode].map((r,i)=>'<div class="time-row"><div class="tick">'+r[0]+'</div>'+[1,3,5,7].map((j,k)=>'<div class="event '+((mode==='old'&&i>=1&&i<=2&&k<2)?'wait':(mode==='new'&&i===1&&k<2)?'done':'')+'"><b>'+r[j]+'</b>'+r[j+1]+'</div>').join('')+'</div>').join('');document.querySelectorAll('[data-mode]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.mode===mode)));document.documentElement.dataset.mode=mode;document.getElementById('timeline-note').textContent=mode==='old'?'黄色区域：恢复完成事实可能已存在，但核心还在等模型结果或足够的完成确认。不是所有worker空闲，也不是KV在执行器里等搬运。':'绿色区域：真实事件到核心已做组件验证；后面的真实GLM阶段派发尚未接通。这里展示目标依赖，不代表已提速，也不预测可节省的毫秒数。';}
document.querySelectorAll('[data-mode]').forEach(b=>b.addEventListener('click',()=>show(b.dataset.mode)));show('old');
document.querySelectorAll('section table:not(.diff-table)').forEach(t=>{const w=document.createElement('div');w.className='table-wrap';t.parentNode.insertBefore(w,t);w.appendChild(t)});
const obs=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting)document.querySelectorAll('.sidebar a').forEach(a=>a.classList.toggle('active',a.getAttribute('href')==='#'+e.target.id))}),{rootMargin:'-5% 0px -70% 0px'});document.querySelectorAll('section').forEach(s=>obs.observe(s));document.documentElement.dataset.ready='true';
"""
nav = "".join(f'<a href="#{s["key"]}">{escape(s["title"])}</a>' for s in sections)
article = "".join(f'<section id="{s["key"]}"><h2>{escape(s["title"])}</h2>{md(s["body"])}{s["extra"]}</section>' for s in sections)
html = f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{TITLE}</title><style>{css}</style></head><body><div class="top"><strong>KS · PP＋LMCache 修改讲解</strong><span>统一当前版本 · 真实源码 / 分层验证 / 完整集成缺口</span></div><div class="layout"><nav class="sidebar" aria-label="目录">{nav}<a href="#full-diffs">完整左右差异</a></nav><main><header><div class="eyebrow">从“知道KV好了”到“允许本段开始计算”</div><h1>{TITLE}</h1><p>先理解依赖，再看代码怎样拆开；把已经验证的组件与尚未接通的模型链分开。</p><div class="chips"><span class="chip">真实事件到核心：组件通过</span><span class="chip">16卡阶段依赖：小张量通过</span><span class="chip pending">完整模型与性能：NOT_VERIFIED</span></div></header>{article}<section id="full-diffs"><h2>完整代码差异：左右对照</h2><p>左侧原归档，右侧当前隔离候选。红底删除，绿底新增，上下文保留；代码不强制折行，长行横向滚动。新增文件左侧为空，不代表旧系统没有相关功能。</p><p>这是真实候选差异，尚未实现的正常启动接线、真实scheduler分段派发不会伪装成已有代码。</p>{''.join(diff_h)}</section><p class="footer">样式、图示、代码和关键结果均已内嵌，可单独复制离线打开。文档检查不是模型或性能验收。</p></main></div><script>{js}</script></body></html>'''
report = f"# {TITLE}\n\n" + "\n\n".join(f'## {s["title"]}\n\n{s["body"]}' for s in sections)
report += "\n\n## 完整代码差异\n\nHTML 按文件折叠并左右高亮展示；这里保留相同原文差异。\n" + "\n".join(diff_m)
for text in [html, report]:
    assert "\ufffd" not in text and "???" not in text and "@@PATCH@@" not in text
(HERE / f"{STEM}.html").write_text(html, encoding="utf-8")
(HERE / f"{STEM}.md").write_text(report, encoding="utf-8")
(HERE / "源码摘录索引.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
(HERE / "README.md").write_text(f'''# {TITLE}

本目录保存统一的当前讲解。原 HTML 路径已刷新，正文不再追加“某日结论补充”或并列过时状态。

- `PP分段就绪调度_源码依据与修改设计.html`：独立离线文档，含双请求流程、代码解释、完整左右差异与实际证据。
- `PP分段就绪调度_源码依据与修改设计.md`：相同讲解正文和代码差异，便于复制归档。

完成事件→独立队列→核心等待期间消费→按请求代次登记阶段就绪，已做 CPU 与真实事件组件组合验证；16 卡合成张量阶段依赖通过。真实元数据、正常启动、真实 scheduler 阶段派发和完整 GLM 生命周期尚未组成整链，性能收益为 NOT_VERIFIED。

生成与文档检查只操作本地文件，不运行推理／候选测试：

```powershell
python -X utf8 -B "{HERE / '生成说明.py'}"
node "{HERE / '检查说明.mjs'}"
```

`源码摘录索引.json`保存连续原文与行号；`文档检查.json`仅证明浏览器呈现与交互。
''', encoding="utf-8")
print(json.dumps(dict(html=str(HERE / f"{STEM}.html"), markdown=str(HERE / f"{STEM}.md"), source_excerpts=len(index), diff_files=len(diffs), server_actions=0, model_tests=0), ensure_ascii=False))
