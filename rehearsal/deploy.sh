#!/usr/bin/env bash
# Render-style deploy of one service from the current checkout: build, start
# the new instance beside the old one, wait until it listens on its port, then
# stop the old one (SIGTERM, 30 s grace) and keep its log. Ports: frontend
# 7233, history 7234, matching 7235, worker 6939 (it only opens membership).
#
#   rehearsal/deploy.sh temporal-frontend 7233 out/logs/v1.23.1
set -euo pipefail
cd "$(dirname "$0")"
svc=$1
port=$2
logdir=${3:-out/logs}
mkdir -p "$logdir"

old=$(docker compose ps -q "$svc")
docker compose build -q "$svc"
t0=$(date +%s)
docker compose up -d --no-deps --no-recreate --scale "$svc=2" "$svc" >/dev/null 2>&1
new=$(docker compose ps -q "$svc" | grep -v "$old")
until docker exec "$new" sh -c "nc -z \$(getent hosts \$(hostname) | awk '{print \$1}') $port" 2>/dev/null; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$new")" != true ]; then
    echo "$svc: new container exited"
    docker logs --tail 50 "$new"
    exit 1
  fi
  sleep 1
done
t1=$(date +%s)
docker stop -t 30 "$old" >/dev/null
docker logs "$old" >"$logdir/$svc-old.log" 2>&1
docker rm "$old" >/dev/null
t2=$(date +%s)
echo "$svc: listening after $((t1 - t0))s, old stopped after $((t2 - t1))s"
