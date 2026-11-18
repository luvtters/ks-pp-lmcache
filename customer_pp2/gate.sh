#!/usr/bin/env bash
set -eu
ks_pp2_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ks_pp2_root="${KS_LMC:-/opt/ks-luv-pp}"
if [ "$#" -lt 2 ]; then
    echo 'usage: bash gate.sh check|open 0|16 [--cache-pid PID] [--pool-gib 480]' >&2
    exit 2
fi
ks_pp2_gate="$(cat "$ks_pp2_root/pp2-active-gate.txt")"
ks_pp2_action="$1"
ks_pp2_first="$2"
shift 2
python -B -S "$ks_pp2_dir/ks_order_gate.py" "$ks_pp2_action" "$ks_pp2_gate" "$ks_pp2_first" "$@"
