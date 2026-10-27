#!/usr/bin/env bash
# PREPARED ONLY. N16 real PD P; source common.env and p-stage-candidate.env; no local feedback.
vllm serve /opt/ks-stage-ready-luv-20261027/reduced-pd/model-N16 \
  --host 0.0.0.0 \
  --port 18100 \
  --tensor-parallel-size 4 \
  --pipeline-parallel-size 2 \
  --enable-expert-parallel \
  --distributed-executor-backend mp \
  --master-addr 127.0.0.1 \
  --master-port 18190 \
  --nnodes 1 \
  --node-rank 0 \
  --hf-overrides '{"use_index_cache":true,"index_topk_freq":4}' \
  --speculative-config '{"num_speculative_tokens":3,"method":"deepseek_mtp","enforce_eager":true,"quantization":"ascend","model":"/opt/ks-models/GLM-5.1-w8a8c8-MTP"}' \
  --seed 1024 \
  --served-model-name glm-5 \
  --max-model-len 49152 \
  --additional-config '{"fuse_muls_add":true,"recompute_scheduler_enable":false,"multistream_overlap_shared_expert":true,"enable_dsa_cp":true,"enable_sparse_sfa_c8":true,"enable_sparse_li_c8":true,"c8_enable_reshape_optim":true}' \
  --max-num-batched-tokens 16384 \
  --max-num-seqs 64 \
  --trust-remote-code \
  --no-enable-prefix-caching \
  --quantization ascend \
  --gpu-memory-utilization 0.94 \
  --enforce-eager \
  --enable-auto-tool-choice \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --kv-transfer-config '{"kv_role":"kv_producer","kv_port":28000,"engine_id":"ks-luv-N16-p","kv_connector":"MultiConnector","kv_connector_extra_config":{"ks_local_p_complete":false,"ks_local_p_first_token_only":false,"connectors":[{"kv_connector":"MooncakeConnectorV1","kv_role":"kv_producer","kv_port":28000,"kv_connector_extra_config":{"use_ascend_direct":true,"prefill":{"dp_size":1,"tp_size":4,"pp_size":2,"pp_layer_partition":"8,8"},"decode":{"dp_size":2,"tp_size":4,"pp_size":1}}},{"kv_connector":"LMCacheMPConnector","kv_role":"kv_both","kv_connector_module_path":"lmcache.integration.vllm.lmcache_mp_connector","kv_connector_extra_config":{"lmcache.mp.host":"tcp://127.0.0.1","lmcache.mp.port":5565,"lmcache.mp.autostart":false,"lmcache.mp.nonblocking_lookup_status":false}}]}}'
