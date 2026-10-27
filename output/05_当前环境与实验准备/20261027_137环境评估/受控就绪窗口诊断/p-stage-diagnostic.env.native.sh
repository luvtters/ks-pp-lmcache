#!/usr/bin/env bash
export ASCEND_RT_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
export VLLM_PP_LAYER_PARTITION=8,8
export HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/home/p TMPDIR=/opt/ks-stage-ready-luv-20261027/reduced-pd/tmp/p
export XDG_CACHE_HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/cache/p
export HF_HOME="$XDG_CACHE_HOME/huggingface" VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub" HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
mkdir -p "$HOME" "$TMPDIR" "$XDG_CACHE_HOME"
export KS_PP_STAGE_DRIVER=1 KS_PP_STAGE_EVENTS=1 KS_CORE_STAGE_EVENT_PUMP=1 KS_LMCACHE_STAGE_OBSERVER=1
export KS_STAGE_TRACE_DIR=/opt/ks-stage-ready-luv-20261027/reduced-pd/results/stage-diagnostic-trace
export PYTHONPATH=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/diagnostic-back-ready:/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher:/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/p-original:/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH
export VLLM_ASCEND_ENABLE_FUSED_MC2=0
export HCCL_BUFFSIZE=512
export KS_LMCACHE_DIAGNOSTIC_BACK_READY_DELAY_MS=500
