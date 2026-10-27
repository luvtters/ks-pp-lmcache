#!/usr/bin/env bash
# Source common.env. Real remote-prefill/decode semantics; never discards Mooncake.
python /opt/ks-stage-ready-luv-20261027/reduced-pd/proxy_v023.py --host 127.0.0.1 --port 18083 --workers 1 --max-retries 1 --prefiller-hosts 192.168.0.223 --prefiller-ports 18100 --decoder-hosts 192.168.0.223 192.168.0.223 --decoder-ports 18200 18201
