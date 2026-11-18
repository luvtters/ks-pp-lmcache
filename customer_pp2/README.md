# 客户现场：应用已验证的 PP2 修复

## 交付范围

本包针对当前 **两台组成一个 P、TP16／PP2、41／37层、MoE权重卸载开启、两个轮换slot** 的现场。
默认安装只修改已安装的 LMCache 八个 Python 文件和 **vllm-ascend 的 worker.py**；
不替换整份框架，不安装依赖，不重新编译，保留客户原模型、IP、网卡、TP／PP／MTP和正常STORE。

包括：

1. LMCache `e8e2f1d7 → 91de3c85`：首次 LOOKUP 回复直接携带已就绪命中数，未就绪仍走 STATUS；两端正命中取公共前缀。
2. 使用已有 `lmcache.mp.eager_prefetch=true`：请求进入引擎等待队列时提前查询。
3. Ascend 启动门：32个worker先完成原生Bifrost权重镜像和两个slot，再建立各机L1，缓存就绪后放行KV初始化。

**模型和缓存都需要在计划的停机窗口中重启，池子需要重新预热。**
安装脚本不会停止服务或容器，发现本容器中仍有模型／缓存进程会拒绝应用。

本分支的完整LMCache源码包含11月12日前的PP兼容与可靠性修改。
安装器只应用上述八文件增量，要求现场已包含这些前置修改。
如果现场源码不同，`check` 会失败并保持原文件；不要用新分支整目录覆盖现场，也不要直接 `pip install -e` 本仓库。

## 1. 两台模型／缓存容器都下载同一交付分支

在711和695各自实际运行vLLM／LMCache的**容器里**执行。若缓存不在模型容器内，另在缓存容器应用LMCache部分，并先说明实际拓扑；本启动门要求同一PID命名空间。

```bash
source /opt/ks-luv-pp/env.sh
git clone --single-branch --branch delivery/customer-pp2-verified-20261118 https://github.com/luvtters/ks-pp-lmcache.git /opt/ks-pp2-verified-20261118
cd /opt/ks-pp2-verified-20261118
git log -1 --oneline
python -B customer_pp2/deploy.py check
```

最后应显示实际 `lmcache_root`、`ascend_root`，以及 `READY_TO_APPLY` 或 `ALREADY_APPLIED`。
`check`允许服务运行，不导入torch／vLLM／LMCache包，不启动设备操作。

若自动定位失败，使用现场真实源码路径；下面是常见路径示例，必须与实际加载位置相符：

```bash
python -B -S customer_pp2/deploy.py check --lmcache-root /opt/ks-luv-pp/src/LMCache --ascend-root /workspace/vllm-ascend
```

只验证LOOKUP、暂不安装镜像启动门时可加 `--only-lookup`。这种方式保留原先启动顺序，也不包含A线扩池修复。

## 2. 两台停止原模型与原LMCache后应用

在原服务终端用原方式停止对应模型、缓存。不要退出或停止整个容器，不要清理模型或日志。
两台都确认停服后，在上节同一容器和同一Python环境中执行：

```bash
cd /opt/ks-pp2-verified-20261118
python -B customer_pp2/deploy.py apply
python -B customer_pp2/deploy.py check
```

如果上一步用了显式 `--lmcache-root`／`--ascend-root`，这里继续带上相同参数。
成功后有 `status: APPLIED` 和 `backup` 路径；复查两项应为 `ALREADY_APPLIED`。
备份与回执存于 `/opt/ks-luv-pp/pp2-update-backups/`。源码不匹配时脚本会停止，不强行覆盖其他修改。

## 3. 只修改两份原模型启动脚本的两处

711的PP0脚本和695的headless PP1脚本都修改。脚本名沿用现场已有名称。

**第一处：在原 `vllm serve $MODEL_PATH` 的上一行加入：**

```bash
source /opt/ks-pp2-verified-20261118/customer_pp2/enable.sh || exit 1
```

这是加进启动脚本的内容。每次冷启动会创建本机新门目录并打印 `PP2_GATE ...`，无需手动更改目录。
该行放在原脚本所有环境设置之后，保证helper目录在worker的 `PYTHONPATH` 中。

**第二处：在原 `LMCacheMPConnector` 的 `kv_connector_extra_config` 内增加：**

```json
"lmcache.mp.eager_prefetch": true
```

保留原有 `server_urls`、`autostart=false`、`nonblocking_lookup_status=true`、`pp_query_fastpath=true` 和 `pp_control_outbound_budget=32`。
注意JSON逗号。它必须放在LMCache子连接器内，不是MultiConnector顶层，也不是Mooncake子连接器。

模型路径、seed、APC、MoE卸载、pin_memory、EPLB、max-model-len及其余启动参数继续沿用客户原值。

## 4. 启动顺序：先两端模型，后两端LMCache

本节应用480GiB；缓存脚本沿用现场版本，每端 `--l1-size-gb 480`，本地状态接口为15565。

1. **711与695同时启动各自原模型脚本**。这次先不启动LMCache。每个worker完成镜像和slot后等待放行，此刻API尚未READY属于预期。
2. 两端各开另一个终端，进入相同容器。分别执行：

711／PP0：

```bash
bash /opt/ks-pp2-verified-20261118/customer_pp2/gate.sh check 0
```

695／PP1：

```bash
bash /opt/ks-pp2-verified-20261118/customer_pp2/gate.sh check 16
```

各自应输出16份rank记录。没有齐全时保持现状，查看这轮模型日志，不要启动第二个模型实例。

3. **只有两台均有16份记录后，才在两台分别启动原LMCache脚本**。先前那个缓存必须已经停止，新缓存须晚于镜像完成。
4. 两台分别检查缓存：

```bash
curl --noproxy '*' -fsS --max-time 10 http://127.0.0.1:15565/status
```

应健康且实际池容量为 `515396075520` 字节。
5. **确认两台缓存都健康后**，分别放行：

711／PP0：

```bash
bash /opt/ks-pp2-verified-20261118/customer_pp2/gate.sh open 0 --pool-gib 480
```

695／PP1：

```bash
bash /opt/ks-pp2-verified-20261118/customer_pp2/gate.sh open 16 --pool-gib 480
```

脚本只在本容器唯一LMCache server存在时自动识别PID；若有多个，会拒绝并要求明确 `--cache-pid PID`。
状态端口不同可以显式增加 `--status-url http://127.0.0.1:实际端口/status`。
它不会分配或调整缓存容量，`--pool-gib`只用于核对现场实际值。

6. 等待两端各16个KV客户端注册和711 API正常：

```bash
curl --noproxy '*' -fsS --max-time 5 http://127.0.0.1:8000/health
```

在711执行，HTTP200说明API可用。695是headless，不要求本机8000健康。

## 5. 在原发压机验证

发压机和模型容器可以是不同机器。下面的 `192.0.2.10` 是文档示例地址，复制前替换为客户真实P0服务IP；模型路径也填写发压端能读取的原路径。
不加 `--load-format dummy`，不加 `--profile`。

先沿用客户方法预热池。用于与本次蓝区对应的无共享预热命令为：

```bash
vllm bench serve \
  --backend vllm \
  --base-url http://192.0.2.10:8000 \
  --model /media/ssd1/models/GLM-5.2-w4a8/ \
  --served-model-name glm-5 \
  --dataset-name random \
  --num-prompts 2000 --request-rate 20 \
  --random-prefix-len 0 --random-input-len 8192 --random-output-len 1 \
  --random-range-ratio 0.0 --num-warmups 0 --seed 2002 \
  --ready-check-timeout-sec 0 \
  --percentile-metrics ttft --metric-percentiles 50,90,99 \
  --save-result --save-detailed \
  --result-dir ./customer-pp2-warm2000 --result-filename benchmark.json \
  2>&1 | tee customer-pp2-warm2000.log
```

预热必须等2000条全部完成。再发90%共享前缀正式4000条：

```bash
vllm bench serve \
  --backend vllm \
  --base-url http://192.0.2.10:8000 \
  --model /media/ssd1/models/GLM-5.2-w4a8/ \
  --served-model-name glm-5 \
  --dataset-name random \
  --num-prompts 4000 --request-rate 20 \
  --random-prefix-len 7373 --random-input-len 819 --random-output-len 1 \
  --random-range-ratio 0.0 --num-warmups 0 --seed 2002 \
  --ready-check-timeout-sec 0 \
  --percentile-metrics ttft --metric-percentiles 50,90,99 \
  --save-result --save-detailed \
  --result-dir ./customer-pp2-shared4000 --result-filename benchmark.json \
  2>&1 | tee customer-pp2-shared4000.log
```

`--ready-check-timeout-sec 0`沿用蓝区已测bench。若现场 `vllm bench serve --help` 没有此参数，省去该参数，并记录客户端版本差异。
不同版本随机数据的生成和APC命中情况也可能不同，原生APC对照必须使用同一客户端、同样的数据参数和预热条件。
各轮使用新的结果目录，保留原生JSON、终端输出、两端完整模型及缓存日志。
还应沿用现有监测保存正式测试前后 `/metrics` 和缓存 `/status`；bench不会代替这些服务端观测。

## 6. 需要回退时

先停止两端本轮模型和缓存，在各自容器执行：

```bash
cd /opt/ks-pp2-verified-20261118
python -B customer_pp2/deploy.py rollback
```

它恢复本机上次安装前的文件。若安装后又有人改了这些文件，会拒绝覆盖，保留备份供核对。
然后从模型脚本移除 `source .../enable.sh` 和新增的 `eager_prefetch` 字段，按原先顺序冷启动并重新预热。
若现场原本已有这些配置，应恢复为修改前记录的值。

## 已验证范围

蓝区真实权重的三条零命中单请求，480GiB缓存先启动的TTFT均值1689.036ms；
改为镜像先分配后937.761ms，同顺序128GiB为933.081ms。该结果针对A线扩池单请求。

本包默认的“首次回包＋提前查询＋镜像先分配”在蓝区**dummy权重**的4000共享请求中为4000成功／0失败，
平均TTFT2.761s、P90 3.320s；对应原生APC为1.812s／2.270s，仍没有超过APC。
客户真实权重和现有环境需要独立验证，不把蓝区数字当客户承诺。

设备间隙接纳的已测原生vLLM补丁在 `reference/verified-native-vllm-admission.patch`，未加入默认安装。
它在同系列蓝区测得平均2.218s、P90 2.766s；迁到Ascend后的版本未验证。
未生效的步尾20ms、部分PP命中上界等候选均未加入本次部署。

本包安装和回退入口通过本地临时源码副本验证。客户机器上的应用、启动和性能尚待实际执行。
