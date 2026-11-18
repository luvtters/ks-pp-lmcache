# 480 GiB、权重卸载开启的双机 PP2 验证基线

用于两台各有 16 个 rank、原来已经能运行 Bifrost 权重卸载和 LMCache 的 P 节点。每台容器各起一个原生 `lmcache server` 和一个 `vllm serve`。PP0 对外提供 API，PP1 使用客户的 headless 启动参数。两项服务需要在本机同一容器、同一用户下启动。

### 1. 两台容器都准备一次

使用客户原来的模型与缓存运行环境。已有服务需要换版时，先由现场按原流程停旧模型和旧缓存。以下使用原客户目录，并新建独立交付目录。安装全程使用现有 Python 和本地源码。

```bash
(
  set -e
  cd /opt/ks-luv-pp
  source ./env.sh
  pkg=/opt/ks-pp2-lm480-20261119
  if [ ! -d "$pkg" ]; then
    git clone --single-branch -b delivery/customer-pp2-lm480-20261119 https://github.com/luvtters/ks-pp-lmcache.git "$pkg"
  fi
  git -C "$pkg" log -1 --format='DELIVERY %H'
  entry="$pkg/customer_pp2/baseline19"
  python -B "$entry/prepare.py"
  python -B "$entry/install.py" apply
)
```

`prepare.py` 读取本机的 `/opt/ks-luv-pp/cache-foreground.sh` 和原模型脚本，自动识别 node-rank。可选原脚本有多份时，终端显示编号；若原件在别处，给 `prepare.py` 加 `--model /实际路径/模型.sh --cache /实际路径/缓存.sh`。生成的两份启动副本在 `/opt/ks-luv-pp/pp2-lm480-19/`。容量固定每台 480 GiB；使用客户原 IP、端口、模型、权重加载方式、990000 上下文及显存比例。两端分别打印 `PP0 … ASYNC_EXPORT=0` 和 `PP1 … ASYNC_EXPORT=1`。

`install.py` 先检查实际解释器查到的 LMCache、vLLM 和 vllm-ascend，再写入补丁。首次成功打印 `APPLIED` 和备份目录，已装好打印 `ALREADY_APPLIED`。只检查时用 `check`。遇到 `STOP` 时先看报错中的源码路径。本交付针对原 PP2 兼容栈，沿用原 LMCache-Ascend 的 NPU 固定内存分配适配及已编译的 c_ops。蓝区参考插件版本为 `ac1e4c73bc3115d3b6b2179da31d9ccb4ebed28e`。

### 2. 两端都装好后，在两台各执行一次

```bash
bash /opt/ks-pp2-lm480-20261119/customer_pp2/baseline19/start.sh
```

看到 `SUBMITTED` 表示本机两条命令已发出。立刻在另一台执行同一条。缓存会等本机 16 个权重镜像就绪才分配 L1，两端都启动后再等健康检查。两份原生服务日志为 `/opt/ks-luv-pp/pp2-lm480-19/vllm.log` 和同目录的 `lmcache.log`。

### 3. 确认本轮服务

两台各查一次，此命令只读本轮日志和当前进程。模型仍在加载时稍后重复。

```bash
python -B /opt/ks-pp2-lm480-20261119/customer_pp2/baseline19/status.py
```

期望两端各自出现 `ORDER_OK … workers=16/16 pool=480GiB`。在 PP0 原 API 端口为 8000 的情况下，再运行

```bash
curl -fsS --max-time 5 http://127.0.0.1:8000/health -o /dev/null -w 'API_HTTP=%{http_code}\n'
```

两端顺序检查通过、PP0 返回 200 后即可从原发压机发请求。PP1 初始显示 `AWAITING_FIRST_STORE` 属预期。发一条能触发 KV 写入的新前缀请求，再查 PP1；`PP1_ASYNC_STORE OBSERVED` 表示本轮实际写 rank 产生了后台事件导出记录。

### 基线包含的行为和成绩

- A 线由两项服务内部协调镜像与池的顺序。B 线沿用首次 LOOKUP 回复带回命中块数、请求提前查询、引擎在等待设备时接纳新请求。20 毫秒步尾等待关闭。
- 正常写入、恢复及块释放沿用原流程。仅 PP1 写 rank 16 把 producer event 的 IPC 句柄导出移出模型线程，消息仍按序发送。验证版同时开启本轮 STORE 和调度接纳观测，输出在生成脚本目录下。
- 蓝区预热 2000 条后，20 RPS、约 90% 共享前缀、8192 入／1 出的 4000 条正式请求全部成功。改前和改后 P90 TTFT 分别为 2.656 和 2.532 秒；同机早期原生 APC 为 2.088 秒。改后仍高于 2.50 秒目标。蓝区正式组使用 dummy 权重、显存比例 0.922；客户真实权重和原脚本显存比例的结果请另行记录。

需要恢复时，先停本次两个服务，再运行 `python -B /opt/ks-pp2-lm480-20261119/customer_pp2/baseline19/install.py rollback --backup <本机安装时打印的备份目录>`，随后使用原客户脚本启动。再次验证可用 `prepare.py --output 新目录` 和 `start.sh 新目录` 保留每次日志。
