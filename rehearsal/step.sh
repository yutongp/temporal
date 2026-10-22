#!/usr/bin/env bash
# One upgrade step from the current checkout, in the runbook's order:
# harness load, ES mapping (if given), frontend, history, matching, worker.
#
#   rehearsal/step.sh v1.23.1 '{"properties":{"ParentWorkflowId":{"type":"keyword"},"ParentRunId":{"type":"keyword"}}}'
set -euo pipefail
cd "$(dirname "$0")"
label=$1
mapping=${2:-}
logdir=out/logs/$label
mkdir -p "$logdir"
since=$(date -u +%FT%TZ)
t0=$(date +%s)
echo "== $label start $since"

docker compose --profile harness run --rm -T harness python /harness/harness.py step "$label" 2>/dev/null

if [ -n "$mapping" ]; then
  # The exact command for the Render frontend shell (env vars come from render.yaml).
  docker compose exec -T -e MAPPING="$mapping" temporal-frontend bash -c \
    'curl -sS --fail -u "$ES_USER:$ES_PWD" -X PUT "http://$ES_SEEDS:9200/temporal_visibility_v1_dev/_mapping" -H "Content-Type: application/json" -d "$MAPPING"; echo'
fi
t1=$(date +%s)

./deploy.sh temporal-frontend 7233 "$logdir"
t2=$(date +%s)
./deploy.sh temporal-history 7234 "$logdir"
t3=$(date +%s)
./deploy.sh temporal-matching 7235 "$logdir"
t4=$(date +%s)
./deploy.sh temporal-worker 6939 "$logdir"
t5=$(date +%s)

echo "$label timings (s): es=$((t1 - t0)) frontend=$((t2 - t1)) history=$((t3 - t2)) matching=$((t4 - t3)) worker=$((t5 - t4)) total=$((t5 - t0))" | tee "$logdir/timings.txt"
echo "since=$since" >>"$logdir/timings.txt"
