# ForgeDB 1.0 Demo Runbook

A repeatable, exact walkthrough of ForgeDB's core guarantees against a
real local 3-node Docker cluster: health, authentication, CORS, KV
read/write/delete, leader failover, snapshot creation, and graceful
shutdown/recovery. Every command and status code below was actually
run and observed during the ForgeDB 1.0 release engineering pass; where
output is shown, it is real output from that run, not illustrative.

Prerequisites: Docker Desktop running, a `.env` file with
`FORGEDB_API_TOKEN` set (`cp .env.example .env` and edit it — see
`deploy/README.md` Section 1.1).

## 1. Start the cluster

```powershell
docker compose build
docker compose up -d
docker compose ps
```

Expected: `forgedb-1`, `forgedb-2`, `forgedb-3`, and `forge-gateway` all
reach `Up ... (healthy)` within about 10 seconds.

## 2. Check health and readiness (no auth required)

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8090/health   # 200
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8090/ready    # 200
```

## 3. Verify authentication is enforced

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8090/cluster                                   # 401, no Authorization header
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer wrong-token" http://127.0.0.1:8090/cluster  # 401, wrong token
curl -s -w "\nHTTP:%{http_code}\n" -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://127.0.0.1:8090/cluster # 200, correct token
```

Observed: both failure cases returned 401; the correct token returned
200 with the current Raft status (role, term, leader ID, commit index).

## 4. Verify CORS

```bash
curl -s -i -X OPTIONS http://127.0.0.1:8090/kv/demo/corstest \
  -H "Origin: http://evil.example" -H "Access-Control-Request-Method: PUT"
```

Observed: `204 No Content` with no `Access-Control-Allow-Origin` header
(since `FORGEDB_CORS_ORIGINS` was unset) — a cross-origin browser
request from an unlisted origin is correctly rejected by the browser's
own CORS enforcement, not by the server refusing the preflight outright.

## 5. KV lifecycle through the gateway

```bash
curl -s -w "\nHTTP:%{http_code}\n" -X PUT http://127.0.0.1:8090/kv/demo/smoke1 \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"value":"hello-forgedb"}'                                        # {"status":"ok"} / 200
curl -s -w "\nHTTP:%{http_code}\n" http://127.0.0.1:8090/kv/demo/smoke1 \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN"                          # {"value":"hello-forgedb"} / 200
curl -s -w "\nHTTP:%{http_code}\n" -X DELETE http://127.0.0.1:8090/kv/demo/smoke1 \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN"                          # {"status":"deleted"} / 200
curl -s -w "\nHTTP:%{http_code}\n" http://127.0.0.1:8090/kv/demo/smoke1 \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN"                          # "key not found" / 404
```

## 6. Identify the leader and write a pre-failover key

```bash
curl -s http://127.0.0.1:8090/cluster -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: `LeaderID":"node-2"`.

```bash
curl -s -X PUT http://127.0.0.1:8090/kv/demo/before-failover \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN" -d '{"value":"written-before-leader-stop"}'
```

## 7. Stop the leader and observe a new election

```bash
docker stop forgedb-2
```

```bash
curl -s http://127.0.0.1:8090/cluster -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: new leader `node-1`, term advanced from 22 to 23.

## 8. Write a new key through the same gateway address

```bash
curl -s -X PUT http://127.0.0.1:8090/kv/demo/after-failover \
  -H "Authorization: Bearer $FORGEDB_API_TOKEN" -d '{"value":"written-after-failover"}'
```

Observed: `{"status":"ok"}` / 200 — no client-visible change in the
request, even though the request was transparently rerouted to the new
leader.

## 9. Read both keys

```bash
curl -s http://127.0.0.1:8090/kv/demo/before-failover -H "Authorization: Bearer $FORGEDB_API_TOKEN"
curl -s http://127.0.0.1:8090/kv/demo/after-failover  -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: both return their correct values — no data lost across the
failover.

## 10. Restart the old leader and verify rejoin + catch-up

```bash
docker start forgedb-2
```

```bash
curl -s http://127.0.0.1:8082/cluster -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: `node-2` rejoined as `Follower`, and its `CommitIndex`/
`LastApplied` matched the cluster's current leader's values — fully
caught up. (A second election occurred during this test, moving
leadership to `node-3`; this is expected Raft behavior during a node
rejoin and does not affect correctness — the gateway transparently
followed it.)

```bash
curl -s http://127.0.0.1:8090/kv/demo/before-failover -H "Authorization: Bearer $FORGEDB_API_TOKEN"
curl -s http://127.0.0.1:8090/kv/demo/after-failover  -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: both keys still readable via the gateway after the full
failover-and-rejoin cycle.

## 11. Trigger a snapshot on every node

```bash
curl -s -X POST http://127.0.0.1:8081/admin/snapshot -H "Authorization: Bearer $FORGEDB_API_TOKEN"
curl -s -X POST http://127.0.0.1:8082/admin/snapshot -H "Authorization: Bearer $FORGEDB_API_TOKEN"
curl -s -X POST http://127.0.0.1:8083/admin/snapshot -H "Authorization: Bearer $FORGEDB_API_TOKEN"
```

Observed: all three returned `{"status":"ok","snapshot_index":14}`.

## 12. Graceful shutdown and recovery

```bash
docker stop forgedb-3
docker compose logs forgedb-3 --tail=10
```

Observed clean shutdown sequence in logs: `shutdown signal received` →
`node.stopped` (dbnode) → `node.stopped` (main) — no errors, no
double-close.

```bash
docker start forgedb-3
docker compose logs forgedb-3 --tail=10
```

Observed clean recovery: `wal.recovery records=52 error=""`, then
`node.recovered ... term=25 last_log_index=14 snapshot_index=14` —
state restored from the snapshot plus WAL, no corruption, node rejoined
and became healthy again.

## 13. Check observability

```bash
curl -s http://127.0.0.1:8090/metrics -H "Authorization: Bearer $FORGEDB_API_TOKEN" | head -40
```

Observed: well-formed Prometheus text exposition (285 lines in this
run) including live election/replication/snapshot counters reflecting
the failover performed above (e.g.
`forgedb_raft_elections_started_total`,
`forgedb_raft_snapshots_created_total`).

## 14. Stop the cluster safely

```powershell
docker compose down        # stops containers, PRESERVES named volumes
```

**Never** run `docker compose down -v` unless you specifically intend
to destroy the cluster's persistent data — it deletes the named
volumes created in Step 1.
