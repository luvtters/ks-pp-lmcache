#!/usr/bin/env bash
# Prepared candidate environment only. Source common, local-completion, then this.
# Native JSON keeps ks_local_p_complete=false and ks_local_p_first_token_only=true.
export KS_PP_STAGE_DRIVER=1
export KS_PP_STAGE_EVENTS=1
export KS_CORE_STAGE_EVENT_PUMP=1
export KS_LMCACHE_STAGE_OBSERVER=1
export KS_STAGE_TRACE_DIR=/opt/ks-stage-ready-luv-20261027/standalone-p/results/stage-candidate-trace
export PYTHONPATH="/opt/ks-stage-ready-luv-20261027/standalone-p/runtime/stage-launcher:$PYTHONPATH"
