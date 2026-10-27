#!/usr/bin/env bash
# PREPARED_NOT_STARTED. Keep default lazy L1 allocator; do not add --no-l1-use-lazy.
# Source common.env.native.sh first; cache worker count matches original async ON.
lmcache server --host 127.0.0.1 --port 5565 --http-host 127.0.0.1 --http-port 15565 --chunk-size 512 --supported-transfer-mode lmcache_driven --l1-size-gb 12 --eviction-policy LRU --l1-align-bytes 65536 --disable-metrics --instance-id ks-luv-standalone-p-20261027 --max-gpu-workers 1
