#!/usr/bin/env bash
# One-shot submitter, run inside the same container as both native CLIs.
set -e
here=$(cd -- "$(dirname -- "$0")" && pwd -P)
dir=$(cd -- "${1:-/opt/ks-luv-pp/pp2-lm480-19}" && pwd -P)
cd /opt/ks-luv-pp
source ./env.sh
test -r "$dir/cache.sh" && test -r "$dir/model.sh" || { echo "Run prepare.py first"; exit 1; }
python -B "$here/install.py" check --require-applied
python -B - "$here/.." <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from deploy import service_pids
pids = service_pids()
if pids:
    sys.exit(f"Existing model/cache processes in this container: {pids}")
PY
mkdir -- "$dir/launch" || { echo "Already submitted from $dir; inspect its logs. For a fresh run prepare a new --output."; exit 1; }
nohup bash "$dir/cache.sh" >> "$dir/lmcache.log" 2>&1 </dev/null &
echo "$!" > "$dir/launch/cache.pid"
nohup bash "$dir/model.sh" >> "$dir/vllm.log" 2>&1 </dev/null &
echo "$!" > "$dir/launch/model.pid"
echo "SUBMITTED cache=$(cat "$dir/launch/cache.pid") model=$(cat "$dir/launch/model.pid") logs=$dir"
