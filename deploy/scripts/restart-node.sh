#!/usr/bin/env bash
# Crash (docker stop) and restart one node, then wait for it to rejoin
# as a follower. This is a crash-recovery test, distinct from
# partition.sh/heal.sh's network-level test -- see deploy/README.md's
# "crash vs. partition" section.
#
# Usage: deploy/scripts/restart-node.sh forgedb-2
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

svc="${1:?usage: restart-node.sh <forgedb-1|forgedb-2|forgedb-3>}"

echo "Stopping $svc ..."
docker stop -t 5 "$svc" >/dev/null

echo "Waiting for the remaining nodes to elect/keep a leader..."
leader="$(wait_for_leader 30)"
echo "Leader while $svc is down: $leader"

echo "Starting $svc ..."
docker start "$svc" >/dev/null

echo "Waiting for $svc to report healthy..."
waited=0
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null)" = "healthy" ]; do
  if [ "$waited" -ge 30 ]; then
    echo "ERROR: $svc did not become healthy within 30s" >&2
    print_diagnostics
    exit 1
  fi
  sleep 1
  waited=$((waited + 1))
done

echo "$svc is healthy again. Current cluster view:"
cluster_json "$svc"
echo
