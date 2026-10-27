#!/usr/bin/env bash
# PREPARED_NOT_STARTED. Machine 137; container ks-pp-m137-priv.
export KS_P_SINGLE=/opt/ks-stage-ready-luv-20261027/reduced-pd
export KS_MP=/opt/ks-lmcache-stage2/official-mp-v0.5.6rc5
export KS_P_COMMON_RUNTIME="$KS_P_SINGLE/runtime/common"
export PATH="$KS_MP/venv/bin:$PATH"
export PYTHONPATH="$KS_P_COMMON_RUNTIME:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:/usr/local/Ascend/ascend-toolkit/latest/opp/built-in/op_impl/ai_core/tbe:/usr/local/Ascend/cann-9.1.0/python/site-packages:/usr/local/Ascend/cann-9.1.0/opp/built-in/op_impl/ai_core/tbe"
# A single-module import finder preserves the former approved one-line pinned
# index staging in both groups and cache; installed packages remain unchanged.
export KS_WORK_HOME="$KS_P_SINGLE/home"
export HOME="$KS_WORK_HOME"
export TMPDIR="$KS_P_SINGLE/tmp"
export XDG_CACHE_HOME="$KS_P_SINGLE/cache"
export HF_HOME="$XDG_CACHE_HOME/huggingface"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub"
export HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
export VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
unset TRANSFORMERS_CACHE TORCH_DEVICE_BACKEND_AUTOLOAD LMCACHE_CONFIG_FILE
export PYTHONDONTWRITEBYTECODE=1 LMCACHE_TRACK_USAGE=false
export ASCEND_RT_VISIBLE_DEVICES=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
export PYTHONHASHSEED=0 TE_PARALLEL_COMPILER=1
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib"
export VLLM_ASCEND_ENABLE_FUSED_MC2=1 HCCL_OP_EXPANSION_MODE=AIV
export HCCL_IF_IP=192.168.0.223 HCCL_NPU_SOCKET_PORT_RANGE=auto
export GLOO_SOCKET_IFNAME=enp23s0f3 TP_SOCKET_IFNAME=enp23s0f3 HCCL_SOCKET_IFNAME=enp23s0f3
export OMP_PROC_BIND=false OMP_NUM_THREADS=1
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:False HCCL_BUFFSIZE=400
export ACL_OP_INIT_MODE=1 ASCEND_A3_ENABLE=1 VLLM_ASCEND_ENABLE_FLASHCOMM1=1
unset VLLM_PP_LAYER_PARTITION
export VLLM_KV_CACHE_LAYOUT=NHD
mkdir -p "$KS_P_SINGLE/work" "$KS_WORK_HOME" "$TMPDIR" "$XDG_CACHE_HOME"
cd "$KS_P_SINGLE/work"
export KS_ASCEND_PYTHONPATH=/usr/local/Ascend/ascend-toolkit/latest/python/site-packages:/usr/local/Ascend/ascend-toolkit/latest/opp/built-in/op_impl/ai_core/tbe:/usr/local/Ascend/cann-9.1.0/python/site-packages:/usr/local/Ascend/cann-9.1.0/opp/built-in/op_impl/ai_core/tbe
unset KS_PP_STAGE_DRIVER KS_PP_STAGE_EVENTS KS_CORE_STAGE_EVENT_PUMP KS_LMCACHE_STAGE_OBSERVER KS_STAGE_TRACE_DIR
export KS_N16_MOONCAKE_OVERRIDE=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/mooncake-n16/mooncake_connector_n16.py
export KS_N16_DRAFT_PROPOSER_OVERRIDE=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/draft-n16/llm_base_proposer_n16.py
