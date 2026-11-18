#!/usr/bin/env bash
# Source this immediately before vllm serve in the customer's model script.
# Do not change the caller's shell options or launch/stop any service here.
_ks_pp2_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)" || return 1
_ks_pp2_root="${KS_LMC:-/opt/ks-luv-pp}"
python -B "$_ks_pp2_dir/deploy.py" check --full --require-applied || return 1
export KS_LMCACHE_ADMIT_DURING_MODEL_WAIT=1
export KS_LMCACHE_LOOKUP_TAIL_20MS=1
export KS_LMCACHE_INLINE_LOOKUP_TRACE=1
export KS_L1_REGISTERED_MMAP=0
export KS_WEIGHT_REGISTERED_MMAP=0
mkdir -p -- "$_ks_pp2_root" || return 1
KS_ORDER_GATE_DIR="$(mktemp -d "$_ks_pp2_root/pp2-mirror-first-XXXXXX")" || return 1
export KS_ORDER_GATE_DIR
export PYTHONPATH="$_ks_pp2_dir${PYTHONPATH:+:$PYTHONPATH}"
printf '%s\n' "$KS_ORDER_GATE_DIR" > "$_ks_pp2_root/pp2-active-gate.txt" || return 1
printf 'PP2_GATE %s\n' "$KS_ORDER_GATE_DIR"
unset _ks_pp2_dir _ks_pp2_root
