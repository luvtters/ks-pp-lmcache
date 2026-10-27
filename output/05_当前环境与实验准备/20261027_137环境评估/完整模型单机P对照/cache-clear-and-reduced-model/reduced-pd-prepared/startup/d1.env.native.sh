#!/usr/bin/env bash
export ASCEND_RT_VISIBLE_DEVICES=12,13,14,15
unset VLLM_PP_LAYER_PARTITION
export KS_SERVICE=d1
export HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/home/d1 TMPDIR=/opt/ks-stage-ready-luv-20261027/reduced-pd/tmp/d1
export XDG_CACHE_HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/cache/d1
export HF_HOME="$XDG_CACHE_HOME/huggingface" VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub" HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
export PYTHONPATH=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH
mkdir -p "$HOME" "$TMPDIR" "$XDG_CACHE_HOME"
