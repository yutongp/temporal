#!/usr/bin/env bash
# After-step checks: harness report, Postgres schema version, ES mapping,
# ERROR lines per service since a timestamp.
#
#   rehearsal/verify.sh v1.23.1 2026-10-22T00:10:00Z
set -euo pipefail
cd "$(dirname "$0")"
label=$1
since=$2
mkdir -p out

{
  echo "### harness check"
  # stderr is noisy: SDK 1.5.0's replayer prints a debug dump per workflow task.
  if ! docker compose --profile harness run --rm -T harness python /harness/harness.py check "$label" 2>"out/check-$label.stderr"; then
    echo "HARNESS CHECK FAILED"
    grep -v '^\[sdk-core\|^    \|^}' "out/check-$label.stderr" | tail -30
  fi

  echo "### postgres schema_version"
  docker compose exec -T temporal-db psql -U temporal -d temporal -Atc \
    "select db_name || ' curr=' || curr_version || ' min_compatible=' || min_compatible_version from schema_version"
  docker compose exec -T temporal-db psql -U temporal -d temporal -Atc \
    "select update_time || ' ' || old_version || ' -> ' || new_version || ' ' || description from schema_update_history order by update_time"

  echo "### elasticsearch index mapping"
  curl -s -u elastic:rehearsal-es-pwd localhost:19200/temporal_visibility_v1_dev/_mapping |
    python3 -c '
import json, sys
props = json.load(sys.stdin)["temporal_visibility_v1_dev"]["mappings"]["properties"]
want = {"v6": ["ParentWorkflowId", "ParentRunId"], "v7": ["RootWorkflowId", "RootRunId"],
        "v8": ["TemporalPauseInfo"],
        "v9": ["TemporalWorkerDeploymentVersion", "TemporalWorkflowVersioningBehavior", "TemporalWorkerDeployment"]}
print(len(props), "fields")
for v, fields in want.items():
    print(v, {f: f in props for f in fields})
'
  curl -s -u elastic:rehearsal-es-pwd "localhost:19200/_cat/indices/temporal*?h=index,health,docs.count"

  echo "### ERROR lines since $since"
  for s in frontend history matching worker; do
    docker compose logs --no-log-prefix --since "$since" "temporal-$s" 2>&1 |
      python3 -c '
import collections, json, sys
c = collections.Counter()
for line in sys.stdin:
    if line.startswith("{") and "\"level\":\"error\"" in line:
        e = json.loads(line)
        c[(e.get("msg"), str(e.get("error", ""))[:120])] += 1
print(sys.argv[1], sum(c.values()), "errors")
for (msg, err), n in c.most_common():
    print("   ", n, msg, "|", err)
' "$s"
  done
  echo "### host pauses since $since (a sleeping laptop freezes the Docker VM)"
  docker compose logs --no-log-prefix --since "$since" temporal-elasticsearch 2>&1 |
    grep -o 'timer thread slept for [^ ]*' || echo "none"
  echo "### containers"
  docker compose ps --format '{{.Name}} {{.Status}}'
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' | grep temporal-rehearsal
} 2>&1 | tee "out/verify-$label.txt"
