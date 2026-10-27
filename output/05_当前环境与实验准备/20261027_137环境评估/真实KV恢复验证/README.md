# 真实 LMCache KV 保存／恢复组件验证

准备日期：2026-10-27。运行目标为137自有容器`ks-pp-m137-priv`，只在容器私有`/opt/ks-stage-ready-luv-20261027/device-probe`工作。本目录不修改安装源码、不加载模型、不构成正式性能对照。执行结果以每次独立目录中的`result.json`及`events.jsonl`为准；仅准备或CPU检查不算设备通过。

## 本次补充的证据

`real_kv_roundtrip.py`直接调用已安装LMCache0.5.6rc5的`RequestClientFactory`、`create_transfer_context(mode="lmcache_driven")`，而非手工构造响应Future：

1. 真实注册2层NPU KV；每层显式为`tuple(one_plane)`，每plane形状`[8,16,1,128]`、FP16，共64KiB，不分配权重。此形状由官方detector识别为Ascend块搬运实际支持的`NL_X_NP_X_NB_BS_ONE_HS (17)`。
2. 在真实设备张量中写入可复现的整数值，使用源块`[1,2]`；记录并保留producer事件，调用真实MP`STORE`，等待实际`DeviceMessagingFuture.result()`完成。
3. 使用真实`LOOKUP`及`wait_prefetch_status`取得1个chunk命中并建立读锁。STORE设备completion event记录后，L1可读状态仍由独立native callback dispatcher发布；仅明确完成且返回0的LOOKUP允许20ms后再查，最多5秒，逐次记录真实命中，异常及None不重试。这段诊断等待不作为性能测量。将全部KV设备内容覆盖为`-7`，核实旧内容已被覆盖；随后将保存数据恢复到不同目标块`[4,5]`。
4. `submit_retrieve`返回的对象必须严格为已安装`DeviceMessagingFuture`。现有`LMCacheStageObserver`的唯一设备owner线程负责真实Future的查询、事件导入、结果及同步；诊断主线程只等CPU proxy。
5. 用线程内profile记录真实方法调用，不替换这些方法；验证restore的`import_event/query_event/synchronize_event`及`query/result`仅由owner执行。记录query布尔返回计数；若未见False→True，不能把结果写成观察到该转变。
6. 逐元素核对两层目标块与原始CPU参考一致，其余块仍全部为`-7`。成功后才关闭observer、等待UNREGISTER ACK并关闭客户端。STORE／RETRIEVE producer事件一直由引用持有至完成。

`real_kv_core_chain.py`使用同一真实roundtrip，并把已确认的恢复身份经已有`_KSStageEventPublisher`、独立`multiprocessing.Queue`、已有实际候选`EngineCore` AST等待缝送入`RestoreEventBinding`和`StageController`。CPU父进程不得导入torch／LMCache。核心callback必须发生在模型结果Future尚未完成、scheduler更新0、等待名额1时，只有前段组件worker就绪，后段保持未就绪；消费ACK收到后才关闭publisher。

**证据边界：模型响应始终是CPU gate fixture；候选核心通过`__new__`构造，未经过正常EngineCore bootstrap。身份／generation／PP及TP ranks显式来自组件计划，不是正常scheduler metadata传播。逻辑两段各1个worker用于检验登记，实际只用1张卡；未运行TP8/PP2模型、GLM execute/sample/MTP、Mooncake业务链，也不证明提速。**

## 原生运行入口

以下均为137、容器`ks-pp-m137-priv`、工作目录`/opt/ks-stage-ready-luv-20261027/device-probe`。环境需要沿用容器已安装CANN运行库；不安装或升级依赖。物理15号卡通过`ASCEND_RT_VISIBLE_DEVICES=15`限定，进程内部使用逻辑`npu:0`。占卡仍由主代理在已确认资源窗口统一执行。

官方缓存服务原生入口；本组件L1池64MiB、chunk32、单个设备处理线程，不能与历史正式用例的12GiB／512token／其他线程数混为同一配置：

```bash
ASCEND_RT_VISIBLE_DEVICES=15 /opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python -m lmcache.v1.multiprocess.server --host 127.0.0.1 --port 15577 --chunk-size 32 --supported-transfer-mode lmcache_driven --l1-size-gb 0.0625 --l1-use-lazy --eviction-policy LRU --l1-align-bytes 65536 --instance-id ks-luv-real-kv-component-20261027 --max-gpu-workers 1 --max-cpu-workers 1 --disable-metrics
```

先执行最小roundtrip；运行目录必须为尚不存在的新目录：

```bash
ASCEND_RT_VISIBLE_DEVICES=15 /opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready-luv-20261027/device-probe/real_kv_roundtrip.py --run-device-probe --device 0 --endpoint tcp://127.0.0.1:15577 --timeout 60 --output-dir /opt/ks-stage-ready-luv-20261027/device-probe/real-kv-roundtrip-run1
```

通过后可在同一缓存服务上执行真实恢复→独立通知→核心登记组件，不更换缓存参数：

```bash
ASCEND_RT_VISIBLE_DEVICES=15 /opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready-luv-20261027/device-probe/real_kv_core_chain.py --run-device-probe --device 0 --endpoint tcp://127.0.0.1:15577 --timeout 90 --output-dir /opt/ks-stage-ready-luv-20261027/device-probe/real-kv-core-run1
```

同目录需有`real_kv_roundtrip.py`、`real_kv_core_chain.py`、`stage_observer.py`、`chain_runtime.py`、`chain_source_map.json`、`restore_binding.py`、`stage_controller.py`。后五份复用已有25组件或原型源码；`chain_source_map.json`记录AST抽取对应的文件、行号及边界。没有修改原候选文件或已安装框架。

本机CPU准备入口不导入NPU库、不建立连接、不启动服务：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\真实KV恢复验证\real_kv_roundtrip.py" --cpu-plan
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\真实KV恢复验证\real_kv_core_chain.py" --cpu-plan --output-dir /opt/ks-stage-ready-luv-20261027/device-probe/real-kv-core-run1
```

## 失败保留规则

首次官方服务启动时，默认Prometheus `9090`已被占用，`init_observability→init_otel_metrics→prometheus_client.start_http_server`报`OSError: [Errno 98] Address already in use`，其后进程报告`corrupted size vs prev_size`并退出；业务端口15577未监听，尚未执行client。第二次仅在同一业务命令上增加官方`--disable-metrics`，关闭该附带metrics监听，不修改源码。现场`mp_observability/config.py:136`定义此参数，`config.py:417`仅在`enabled and metrics_enabled`时调用OTel metrics初始化。相关源码见`official_observability.txt`。

第二次启动在eager `MixedMemoryAllocator→alloc_pinned_ptr→aclrtMallocHost`分配L1时返回107002，client仍未执行。第三次仅去掉`--no-l1-use-lazy`恢复官方默认lazy，服务成功监听15577。**现场Ascend插件会替换LazyMemoryAllocator：其`__init__.py`的官方`_patch_lazy_memory_allocator`在调用`aclrtMallocHost`前先`torch.npu.set_device(torch.npu.current_device())`。因此不能仅用upstream lazy的`NpuPinMemoryBackend._ensure_context`解释本次实际路径。**eager Mixed路径未经过该lazy替换。默认init20GiB经配置`min(init_size, final_size)`收敛至本次64MiB，不会因此分配20GiB。无安装源码修改。

首轮client成功注册了裸tensor布局`[8,16,128]`，但被识别为enum3；真实STORE报官方Ascend块搬运只支持enum16／17、当前为3。`stored.result()`返回终态False，旧诊断在断言True后进入保留循环，未发LOOKUP／RETRIEVE。这是诊断形状不匹配，不是实际KV恢复成功。新版按上述单plane tuple布局改为enum17，并在断言成功前先记录真实Future已返回终态；False同样先完成远端completion event同步，不再被错误归为未知DMA。

第二轮client实际enum17注册及STORE32token成功，但紧接着LOOKUP为0，未进入RETRIEVE。其失败清理已完成本地同步及UNREGISTER ACK；旧清理随后错误地等待END_SESSION，而UNREGISTER已移除布局描述，END_SESSION在`lookup.py:492/521`报`No attention-window descriptor registered`，因此没有正常回复，最终又进入保留循环。外部首轮清理助手也曾包含同一多余等待；现已删除其END_SESSION调用，失败清理只以设备同步和UNREGISTER ACK作为设备引用清理依据。

**END_SESSION不是oneway：**官方handler标为BLOCKING，官方benchmark helper同样等待其Future。它用于session及缓存键touch，需要布局仍注册；新版成功路径将END_SESSION及回复等待放在UNREGISTER之前。失败／外部释放路径不调用它，不把它误当设备静止条件。第二轮LOOKUP0是否由独立L1发布回调尚未drain造成，目前只是源码支持的机理候选，需以后续实际lookup尝试记录判断；不能将其写成已确认根因。

未知提交／导入／查询／同步错误不会证明设备静止。新版显式区分`remote_transfer_inflight`：尚未尝试远端传输，或真实设备Future已返回True／False时，失败清理先执行本地设备同步，再等待官方UNREGISTER ACK，关闭context／client，记录`FAIL_CLEANED`并退出。原始错误和清理结果都保留。设备Future抛异常或超时不会被当作终态False。

存在未证实完成的远端设备工作，或安全清理失败时，诊断写出`FAIL_RETAINING_DEVICE_REFERENCES`、PID、异常和traceback，保留原对象与进程；不会仅凭client同步就推定另一个LMCache进程已完成DMA。核心入口会在父进程结果中保留仍存活的worker PID；不把超时或遗留进程称为通过。

对于首轮已证实`stored.result()`返回False并进入无新提交的保留循环，`unregister_completed_instance.py`可通过官方客户端等待原实例UNREGISTER ACK；该脚本不发进程信号，也不能用于未知完成状态。只有ACK确认后，监督者才可以核实PID身份并结束已静止的诊断client，服务保留。示例仅对应首轮历史PID，执行前仍需核对当前实际PID，不得机械复用：

```bash
ASCEND_RT_VISIBLE_DEVICES=15 /opt/ks-lmcache-stage2/official-mp-v0.5.6rc5/venv/bin/python /opt/ks-stage-ready-luv-20261027/device-probe/unregister_completed_instance.py --endpoint tcp://127.0.0.1:15577 --instance-id 1415976 --request-id real-kv-component-1415976 --confirmed-terminal-future-returned --output-file /opt/ks-stage-ready-luv-20261027/device-probe/run1-unregister-ack.json
```

## 来源与当前检查

已从137自有容器只读提取原安装源码，保存为`official_worker_server.txt`、`official_factory_config.txt`、`official_transport_types.txt`、`official_bench_layout.txt`及`official_futures_npu_context.txt`。这些是源代码摘录，文件内保留原始路径和行号。`read_remote_sources.py`是只读源码检查工具，不导入设备框架。两个入口的CPU plan和Python语法检查通过；设备结果另见下节，不能由CPU检查外推。

## run3实际设备内容与异常退出边界

`执行记录/real-kv-roundtrip-run3/`及相邻console log已归档。本次使用真实MP服务、enum17两层KV：

- 真实STORE32token的设备Future返回True；第1次实际LOOKUP为0，20ms后第2次为1。此结果说明不能把STORE设备Future完成直接当作已观察到L1查询命中；与独立CPU发布回调的源码路径一致，但未独立测量dispatcher时间。
- 设备全部覆写`-7`后，真实RETRIEVE成功；两层各4096元素与保存前参考值逐元素相同，其余块未被覆盖。
- 实际restore Future `query`观察到False1次、True1次；`result`1次。导入／查询／同步／result均由记录的唯一owner线程调用。**后端`query_event`本身仅记录True1次，不能将Future False→True写成观察到了设备event本身的False→True。**
- observer仅产出一次成功身份；producer事件保留至完成；本地同步和UNREGISTER ACK完成，`safe_unregister_ack=True`。
- 在写出组件内部`result.json: status=PASS`之后，console仍出现`corrupted size vs. prev_size`，进程未正常退出。**本次应表述为“真实数据与observer组件验证通过，但解释器／原生运行时退出异常”；不能把内部PASS写成全进程正常通过，更不能称模型、正常核心bootstrap或性能通过。**原始PASS文件保留原状，不覆盖退出错误。

新增`import_exit_diagnostic.py`及本机入口`run_import_exit_diagnostics.py`，只做global／venv的模块导入与正常Python退出对照；诊断不主动建tensor或调用设备API，包导入内部检测照常执行，不使用`os._exit`绕过析构，不改依赖／源码。将各case的退出码、stdout／stderr及Python atexit标记分别保存，用于判断heap错误是否在无设备张量的导入路径即可复现。

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\真实KV恢复验证\run_import_exit_diagnostics.py"
```

第一组8个case均正常退出0，无heap错误，原始记录为`执行记录/import-exit-20261027-182927/summary.json`。该组将`ASCEND_RT_VISIBLE_DEVICES`置空，LMCache日志明确选择CPU stub，因此不能据此排除真实NPU后端导入路径。补充组仅复查两个venv LMCache／Ascend插件导入case，可见设备恢复15，但仍不主动创建tensor或执行设备API：

```powershell
python -X utf8 -B "E:\work\ks\output\05_当前环境与实验准备\20261027_137环境评估\真实KV恢复验证\run_import_exit_diagnostics.py" --matching-npu-imports-only --visible-device 15
```

补充组两项同样正常退出0、无heap错误，记录为`执行记录/import-exit-20261027-183158/summary.json`。其中LMCache日志明确选择NPU后端，两个case均记录Python atexit标记。**因此这10个import-only正常退出对照未复现heap错误；不能将run3析构异常归为“仅导入即可复现的既有环境故障”。真实张量／IPC／注册恢复后的原生析构仍未定位，尚无正常退出验收结论。**不通过跳过析构、改依赖或源码来掩盖。
