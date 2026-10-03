#!/usr/bin/env bash
# Deterministic end-to-end smoke test for the Phase 14 Docker deployment.
# Starts the cluster (if not already running), waits for a leader with a
# bounded poll (never an arbitrary sleep), exercises PUT/GET/DELETE,
# checks /cluster and /metrics, restarts a follower, and confirms its
# state survives. Exits non-zero (with diagnostics) on any failure.
#
# Usage: deploy/scripts/smoke-test.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh
cd ../..  # repo root, for `docker compose`

fail() {
  echo "SMOKE TEST FAILED: $1" >&2
  print_diagnostics
  exit 1
}

echo "==> Building and starting the cluster"
docker compose -p "$COMPOSE_PROJECT" up -d --build >/dev/null

echo "==> Waiting for a leader"
leader="$(wait_for_leader 30)" || fail "no leader elected"
leader_port="${HTTP_PORT[$leader]}"
echo "Leader: $leader (http://localhost:$leader_port)"

key="smoke-$$"
value="smoke-value-$RANDOM"
auth_header="Authorization: Bearer ${FORGEDB_API_TOKEN}"

echo "==> PUT /kv/$key"
put_status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H "$auth_header" -X PUT --data-binary "$value" "http://localhost:$leader_port/kv/$key")"
[ "$put_status" = "200" ] || fail "PUT returned status $put_status, want 200"

echo "==> GET /kv/$key"
got="$(curl -s --max-time 5 -H "$auth_header" "http://localhost:$leader_port/kv/$key")"
[ "$got" = "$value" ] || fail "GET returned '$got', want '$value'"

echo "==> DELETE /kv/$key"
del_status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H "$auth_header" -X DELETE "http://localhost:$leader_port/kv/$key")"
[ "$del_status" = "200" ] || fail "DELETE returned status $del_status, want 200"

echo "==> GET /kv/$key after delete (want 404)"
get_status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H "$auth_header" "http://localhost:$leader_port/kv/$key")"
[ "$get_status" = "404" ] || fail "GET after DELETE returned status $get_status, want 404"

echo "==> GET /cluster"
cluster="$(cluster_json "$leader")"
echo "$cluster" | grep -q '"Role":"Leader"' || fail "/cluster did not report Role=Leader on $leader"

echo "==> GET /metrics"
metrics_status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H "$auth_header" "http://localhost:$leader_port/metrics")"
[ "$metrics_status" = "200" ] || fail "/metrics returned status $metrics_status, want 200"

echo "==> GET /metrics without a token (want 401)"
unauth_status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://localhost:$leader_port/metrics")"
[ "$unauth_status" = "401" ] || fail "/metrics without a token returned status $unauth_status, want 401"

echo "==> Restart a follower and confirm its state survives"
follower=""
for svc in "${NODES[@]}"; do
  if [ "$svc" != "$leader" ]; then
    follower="$svc"
    break
  fi
done
persist_key="smoke-persist-$$"
persist_value="persist-$RANDOM"
curl -s -o /dev/null --max-time 5 -H "$auth_header" -X PUT --data-binary "$persist_value" "http://localhost:$leader_port/kv/$persist_key"

docker restart -t 5 "$follower" >/dev/null
waited=0
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$follower" 2>/dev/null)" = "healthy" ]; do
  [ "$waited" -lt 30 ] || fail "$follower did not become healthy again within 30s after restart"
  sleep 1
  waited=$((waited + 1))
done

# Read the persisted key back through whichever node is leader now (a
# restart can, but need not, trigger a leader change).
leader="$(wait_for_leader 30)" || fail "no leader after restarting $follower"
leader_port="${HTTP_PORT[$leader]}"
recovered="$(curl -s --max-time 5 -H "$auth_header" "http://localhost:$leader_port/kv/$persist_key")"
[ "$recovered" = "$persist_value" ] || fail "value did not survive follower restart: got '$recovered', want '$persist_value'"

echo
echo "SMOKE TEST PASSED"
