#!/usr/bin/env bash
export ASCEND_RT_VISIBLE_DEVICES=8,9,10,11
unset VLLM_PP_LAYER_PARTITION
export KS_SERVICE=d0
export HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/home/d0 TMPDIR=/opt/ks-stage-ready-luv-20261027/reduced-pd/tmp/d0
export XDG_CACHE_HOME=/opt/ks-stage-ready-luv-20261027/reduced-pd/cache/d0
export HF_HOME="$XDG_CACHE_HOME/huggingface" VLLM_CACHE_ROOT="$XDG_CACHE_HOME/vllm"
export HF_HUB_CACHE="$HF_HOME/hub" HUGGINGFACE_HUB_CACHE="$HF_HOME/hub" HF_DATASETS_CACHE="$HF_HOME/datasets" TORCH_HOME="$XDG_CACHE_HOME/torch"
export PYTHONPATH=/opt/ks-stage-ready-luv-20261027/reduced-pd/runtime/common:$KS_MP/LMCache-Ascend-official-v0.5.6rc5:$KS_ASCEND_PYTHONPATH
mkdir -p "$HOME" "$TMPDIR" "$XDG_CACHE_HOME"
