#!/usr/bin/env bash
# Shared helpers for deploy/scripts/*.sh. These scripts are thin
# wrappers around `docker` / `docker compose` and `curl` for the
# operations deploy/README.md documents by hand -- they do not implement
# any failure injection of their own (Docker's own network/process
# controls do that; see docs/deployment/phase14-docker-deployment.md's
# "real network chaos boundary" section). This is not a second chaos
# framework alongside Phase 10's `chaos` package -- it has no scenario
# engine, no determinism guarantees, and no invariant checker; it is
# purely operational convenience for a human running these tests against
# a real Docker Compose cluster by hand.
set -euo pipefail

COMPOSE_PROJECT="${COMPOSE_PROJECT:-forgedb}"
NETWORK="${COMPOSE_PROJECT}_forgedb-net"
NODES=(forgedb-1 forgedb-2 forgedb-3)
declare -A HTTP_PORT=( [forgedb-1]=8081 [forgedb-2]=8082 [forgedb-3]=8083 )

# cluster_json <service> prints that node's /cluster JSON, or nothing if
# unreachable.
cluster_json() {
  local svc="$1"
  curl -s --max-time 3 "http://localhost:${HTTP_PORT[$svc]}/cluster" || true
}

# role_of <service> prints that node's current Raft role (Leader/
# Candidate/Follower), or "unreachable".
role_of() {
  local json
  json="$(cluster_json "$1")"
  if [ -z "$json" ]; then
    echo "unreachable"
    return
  fi
  echo "$json" | grep -o '"Role":"[A-Za-z]*"' | cut -d'"' -f4
}

# find_leader prints the service name of whichever node currently
# reports itself as Leader, or nothing if none does right now.
find_leader() {
  for svc in "${NODES[@]}"; do
    if [ "$(role_of "$svc")" = "Leader" ]; then
      echo "$svc"
      return
    fi
  done
}

# wait_for_leader <timeout_seconds> polls until some node reports
# itself Leader (bounded -- never hangs forever), printing its service
# name, or exits non-zero after the timeout with diagnostics.
wait_for_leader() {
  local timeout="${1:-30}"
  local waited=0
  while [ "$waited" -lt "$timeout" ]; do
    local leader
    leader="$(find_leader)"
    if [ -n "$leader" ]; then
      echo "$leader"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo "ERROR: no leader elected within ${timeout}s" >&2
  print_diagnostics
  return 1
}

# print_diagnostics dumps docker compose ps and each node's /cluster and
# /health, for use when a bounded wait above times out -- see
# docs/deployment/phase14-docker-deployment.md's "failure diagnostics"
# guidance: a failed distributed-system check should be diagnosable, not
# just reported as "timed out."
print_diagnostics() {
  echo "--- docker compose ps ---" >&2
  docker compose -p "$COMPOSE_PROJECT" ps >&2 || true
  for svc in "${NODES[@]}"; do
    echo "--- $svc /health ---" >&2
    curl -s --max-time 3 "http://localhost:${HTTP_PORT[$svc]}/health" >&2 || echo "(unreachable)" >&2
    echo >&2
    echo "--- $svc /cluster ---" >&2
    cluster_json "$svc" >&2 || echo "(unreachable)" >&2
    echo >&2
    echo "--- $svc recent logs ---" >&2
    docker compose -p "$COMPOSE_PROJECT" logs --tail=20 "$svc" >&2 || true
  done
}
