# LMCache PP2 可靠性修复：终端应用

本分支交付已经在 8/12 跨机 PP2 测试的可靠性修复，目标 Core 提交为 `02ffb6943924d1ebd78e31dc3a608dd4af3de271`。补丁基于既有原生 PP 适配版 `89d93a2ea62030f6890b5bbe8f902c5b92945c8d`，包含 14 个 Python 文件；不修改 vLLM、Ascend 插件或原生扩展源码。

这不是任意官方版本都可直接使用的通用补丁。目标环境必须已有配套的原生 PP / Ascend 适配；应用检查失败时保持原状，不强行覆盖，也不将它叠加到另一套 STATUS 重查候选上。

## 在容器内使用

在个人可写目录操作，使用原本启动服务的 Python 环境。私有仓库需要当前环境具有访问权限；不要把访问令牌写进命令、脚本或仓库地址。

```bash
git clone --depth 1 --single-branch \
  --branch delivery/lmcache-pp-reliability-20261111 \
  https://github.com/luvtters/ks-pp-lmcache.git lmcache-fix-20261111

python lmcache-fix-20261111/apply_patch.py --check
python lmcache-fix-20261111/apply_patch.py
```

脚本通过当前 Python 查找 `lmcache`，先打印目标目录，完成 Git 应用检查，然后备份受影响的旧文件、应用补丁并做内存内语法检查。`--check` 不修改目标。脚本不导入设备框架、不安装依赖、不重编译、不自动启停服务。

如果服务是通过另一条 `PYTHONPATH` 或源码加载器运行，需要先让当前终端采用同一环境。也可以用 `--root /实际包含lmcache目录的根目录` 指定目标；显式目标必须确实是服务加载的目录。

两台 PP 机器都要更新，模型端与缓存端必须加载同一套代码。运行中的 Python 进程不会自动加载这些修改，应在完成两端代码和配置更新后安排正常服务切换。

## 同步启动配置

两端模型、缓存的启动脚本均添加：

```bash
export KS_LMCACHE_MP_READ_POLICY=transfer_borrow
unset KS_LMCACHE_L1_RECHECK_ON_STATUS
```

原 `--kv-transfer-config` 的外层 MultiConnector，以及 `connectors` 数组里的 LMCache 子连接器，均在各自配置对象直接设置：

```json
"kv_load_failure_policy": "recompute"
```

仅设置外层不够；实际 MultiConnector 会重新构建子连接器配置。保留现有 APC、模型、TP/PP、MTP、Mooncake 及其他计算参数。普通未命中应计算；恢复时失去源数据应在设备操作安全收尾后重算。

## 回退代码

在当前补丁目标根目录执行，使用交付补丁的绝对路径：

```bash
export GIT_CEILING_DIRECTORIES="$(dirname "$PWD")"
git -c core.autocrlf=false apply --reverse --check /实际路径/lmcache-fix-20261111/lmcache-reliability-89d93a2-to-02ffb69.patch && \
git -c core.autocrlf=false apply --reverse /实际路径/lmcache-fix-20261111/lmcache-reliability-89d93a2-to-02ffb69.patch
```

同时将启动配置恢复为应用前的值，并通过正常服务切换统一加载旧代码。原文件备份位于交付目录下脚本打印的 `backup-before-02ffb69-*` 子目录，包含原文件和 `backup.json`。回退检查失败时不要覆盖后续修改。

## 验证范围

本版修复实际搬运期间源对象读保护、失败重算、部分 APC 失败后的 STORE 范围、迟回访健康误判及 PREEMPTED 会话恢复。后续 7000 条性能对照和 128/800GiB 池对照使用这一版本；这些完成记录不能替代全部异常设备场景或模型严格数值验收。

APC 已覆盖时跳过外部查询的性能优化尚未实现。CPU 候选 C 的 STATUS 固定预算修改也不在此交付中。现有 APC 对照中的性能下降尚未解决，不能把本版作为性能优化通过的结果。
