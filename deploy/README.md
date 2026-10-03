# ForgeDB Phase 14 Deployment Guide

This is a **local development/demo deployment**: a three-node ForgeDB
cluster running as real Docker containers, communicating over a real
Docker network via gRPC, with persistent per-node volumes. It is not a
production security configuration -- see [Security notes](#security-notes)
and [Limitations](#limitations) below.

For the full design writeup (architecture, transport, configuration,
correctness properties verified), see
[`docs/deployment/phase14-docker-deployment.md`](../docs/deployment/phase14-docker-deployment.md).
This file is the hands-on operator guide.

## 1. Prerequisites

- Docker Engine with Compose v2 (`docker compose version`).
- Nothing else -- the Go toolchain is only needed inside the build stage.
- A `FORGEDB_API_TOKEN` value -- see [Section 1.1](#11-authentication) below.
  Every command in this guide that hits a protected endpoint assumes
  `$FORGEDB_API_TOKEN` is exported in your shell.

### 1.1 Authentication

Every HTTP endpoint except `/health` and `/ready` now requires
`Authorization: Bearer <token>` (see
[`docs/deployment/phase14-docker-deployment.md`](../docs/deployment/phase14-docker-deployment.md#21-http-api-authentication)
for the full design rationale). Before starting the cluster, set a
token:

```sh
cp .env.example .env
# edit .env and replace the placeholder with a real value, e.g.:
#   openssl rand -hex 32
export FORGEDB_API_TOKEN=$(grep -oP '(?<=^FORGEDB_API_TOKEN=).*' .env)
```

`docker compose` reads `.env` automatically from the project root, so
`docker compose up` picks it up without any further flags; the curl
examples below need it exported in your own shell too. Never commit
`.env` -- only `.env.example` (a placeholder) is tracked.

## 2. Building the image

```sh
docker compose build
```

This runs `Dockerfile`'s multi-stage build: a `golang:1.27-alpine`
builder stage compiles static `forgedb` and `forge-client` binaries
(`CGO_ENABLED=0`), and a minimal `alpine:3.20` runtime stage runs them as
a non-root user (`forgedb`, uid 10001). The resulting image is small
(roughly 15 MB of actual content on top of the Alpine base).

## 3. Starting the cluster

```sh
docker compose up -d
docker compose ps
```

This starts three containers -- `forgedb-1`, `forgedb-2`, `forgedb-3` --
each with its own `NODE_ID`, its own persistent named volume
(`forgedb-node{1,2,3}-data`, mounted at `/data`), and its own
host-published ports:

| Service    | Host HTTP | Host gRPC | Container internal |
|------------|-----------|-----------|---------------------|
| forgedb-1  | 8081      | 9091      | 8080 / 9090         |
| forgedb-2  | 8082      | 9092      | 8080 / 9090         |
| forgedb-3  | 8083      | 9093      | 8080 / 9090         |
| forge-gateway | 8090   | --        | 8090 (`GATEWAY_ADDR`) |

All three nodes share one `PEERS` roster and one Docker network
(`forgedb-net`); every node addresses its peers by Compose service name
(`forgedb-1`, `forgedb-2`, `forgedb-3`), never by `localhost` or a
hard-coded IP -- see the deployment doc's network section for why that
distinction matters.

Containers may start in any order: Raft's own election logic, not a
fixed sleep, is what establishes a leader once a majority of nodes can
reach each other. `docker-compose.yml`'s `HEALTHCHECK` (inherited from
the image) only reports process liveness, not leadership -- a follower
is just as healthy as a leader (see
[Health vs. readiness vs. leadership](#health-vs-readiness-vs-leadership)).

## 4. Checking health

```sh
curl http://localhost:8081/health
curl http://localhost:8081/ready
docker compose ps        # STATUS column shows "healthy" once each
                          # container's own HEALTHCHECK passes
```

Example output:

```json
{"status":"alive","node_id":"node-1"}
{"status":"ready","node_id":"node-1"}
```

## 5. Finding the leader

```sh
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8081/cluster
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8082/cluster
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8083/cluster
```

Each response's `raft.LeaderID` and `raft.Role` show who the cluster
currently agrees is leader (once one is elected -- allow a second or two
after startup). Example (abbreviated):

```json
{"node_id":"node-1","raft":{"Role":"Leader","Term":1,"LeaderID":"node-1","CommitIndex":0,...}}
```

You do not need to find the leader yourself to use `forge-client` --
see the next section. You also do not need to find the leader yourself
when going through `forge-gateway` -- see
[Section 5a](#5a-leader-aware-public-routing-forge-gateway) below.

### 5a. Leader-aware public routing (forge-gateway)

A fourth container, `forge-gateway`, runs alongside the three nodes
(see `docker-compose.yml`'s `forge-gateway` service and
[`docs/deployment/phase15-leader-aware-gateway.md`](../docs/deployment/phase15-leader-aware-gateway.md)
for the full design writeup). It is a small, persistent reverse proxy
that forwards every request it receives to whichever node currently
claims to be the Raft leader, following that node's own 421
`leader_http` hint (see Section 6 below) when it's wrong, and only ever
answering the caller once some node has actually accepted the request.
It never validates or inspects the `Authorization` header itself --
every protected endpoint's authentication still happens exactly as
before, inside whichever node ultimately answers.

```sh
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" -X PUT --data-binary "hello world" http://localhost:8090/kv/greeting
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8090/kv/greeting
```

This is what a public entrypoint (Caddy, behind Cloudflare) should
point at instead of any single `forgedb-N` node -- see
[`deploy/Caddyfile.example`](Caddyfile.example). `forge-gateway`'s own
address never needs to change when leadership moves, unlike pointing
Caddy directly at a node.

If every node currently reports it is not the leader and gives no usable
hint (e.g. mid-election), `forge-gateway` returns an explicit
`503 {"error":"no_leader_available"}` rather than guessing -- retry
shortly. If no node can be reached at all, it returns
`502 {"error":"no_backend_reachable"}`.

## 6. Sending a PUT / GET / DELETE

The client API is `/kv/{key}`: `PUT` (body = value), `GET`, `DELETE`.
Using plain `curl` against the host-published ports:

```sh
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" -X PUT --data-binary "hello world" http://localhost:8081/kv/greeting
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8081/kv/greeting
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" -X DELETE http://localhost:8081/kv/greeting
```

A request sent to a follower gets back **HTTP 421 Misdirected Request**
with a JSON body naming the current leader:

```json
{"error":"not_leader","leader_id":"node-2","leader_http":"forgedb-2:8080"}
```

`forge-client` (built into the same image) handles this automatically --
give it every node's address and it finds the leader itself:

```sh
docker run --rm --network forgedb_forgedb-net --entrypoint forge-client \
  forgedb:latest -addrs http://forgedb-1:8080,http://forgedb-2:8080,http://forgedb-3:8080 \
  -token "$FORGEDB_API_TOKEN" put greeting "hello from docker"

docker run --rm --network forgedb_forgedb-net --entrypoint forge-client \
  forgedb:latest -addrs http://forgedb-1:8080,http://forgedb-2:8080,http://forgedb-3:8080 \
  -token "$FORGEDB_API_TOKEN" get greeting
```

(`--entrypoint forge-client` is required because the image's default
entrypoint is the `forgedb` server binary.) The Compose project name
prefixes the network name -- adjust `forgedb_forgedb-net` if you ran
`docker compose` from a different directory name or with `-p`.

## 7. Checking metrics

```sh
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8081/metrics
```

Prometheus text-exposition format (Phase 12's hand-rolled registry --
see `internal/metrics`). No separate metrics daemon or Prometheus
deployment is required for Phase 14; wiring one up against this endpoint
is optional and left to the operator.

## 8. Checking logs

```sh
docker compose logs forgedb-1
docker compose logs -f forgedb-2   # follow
```

Logs are structured (`log/slog`, text format) and go to stdout --
Docker collects them natively; there is no log file inside the
container to find. Every line carries `node_id` and `component`, and
Raft-level events additionally carry `term`; a connectivity problem
between nodes shows up as a `transport:` error wrapping
`raft: peer unreachable` in the sending node's own log.

## 9. Restarting a node

```sh
docker compose restart forgedb-2
```

This stops and starts the container in place; its named volume (and
therefore its Raft term/vote/log/snapshot and KV data) is untouched.
After restarting, the node rejoins as a follower, recovers its persisted
state, and catches up via normal replication (or `InstallSnapshot`, if
it fell far enough behind -- see the deployment doc's snapshot section).

## 10. Stopping a node

```sh
docker stop forgedb-2     # or: docker compose stop forgedb-2
```

This is a **crash**, not a network partition (see
[Crash vs. partition](#crash-vs-partition)): the process is gone, not
merely unreachable.

## 11. Testing failure

### Leader crash and failover

```sh
# 1. find the leader (step 5)
# 2. stop its container
docker stop forgedb-1
# 3. poll the remaining nodes' /cluster until a new LeaderID appears
curl http://localhost:8082/cluster
# 4. issue a write against the new leader (or just use forge-client or
#    forge-gateway, either of which find it automatically)
# 5. restart the old leader and confirm it rejoins as a follower and
#    converges
docker start forgedb-1
```

### Leader crash and failover through the public entrypoint

The scenario above requires the operator to notice the new leader. The
point of `forge-gateway` (see [Section 5a](#5a-leader-aware-public-routing-forge-gateway))
is that a client never has to:

```sh
# Write before the failure, through the SAME endpoint both times:
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" -X PUT --data-binary "v1" http://localhost:8090/kv/k

# Find and stop the current leader (step 5 / docker stop <leader>),
# wait a moment for the election, then write again -- same URL:
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" -X PUT --data-binary "v2" http://localhost:8090/kv/k
curl -H "Authorization: Bearer $FORGEDB_API_TOKEN" http://localhost:8090/kv/k   # -> v2
```

Both requests go to `http://localhost:8090`, regardless of which node
is leader at the time.

### Real network partition (not a crash)

A stopped container is a crash. A genuine partition keeps the process
alive but removes its network path:

```sh
docker network disconnect forgedb_forgedb-net forgedb-1
# forgedb-1 is now alive but unreachable from forgedb-2/forgedb-3 and,
# because Docker's published-port forwarding for a container depends on
# that same network attachment, also unreachable from the host at
# localhost:8081 until it is reconnected.
curl http://localhost:8082/cluster   # majority side: should still have
                                      # (or elect) a leader and accept
                                      # writes
docker network connect forgedb_forgedb-net forgedb-1
# forgedb-1 rejoins, catches up, and localhost:8081 becomes reachable
# again.
```

See [`deploy/scripts/`](scripts/) for small wrapper scripts around
these operations.

## 12. Restoring a node

Restoring a stopped or disconnected node is exactly steps 9/11 above:
`docker start` (crash) or `docker network connect` (partition). Nothing
else is required -- ForgeDB's own Raft replication (and, if it fell far
enough behind, `InstallSnapshot`) brings it back up to date automatically.

## 13. Persistent volumes

Each node's `/data` is a distinct named Docker volume
(`forgedb-node1-data`, `forgedb-node2-data`, `forgedb-node3-data`) --
never the same volume shared between nodes, which would violate node
isolation and could corrupt state. Under each, ForgeDB lays out:

```
/data/raft/raft-state            Raft's currentTerm, votedFor, and log
/data/raft/raft-state.snapshot   Raft's most recent snapshot, if any
/data/kv/wal.log                 the KV engine's write-ahead log
```

(`internal/dbnode.Open` joins `DATA_DIR` with `raft` and `kv`
subdirectories; see `cmd/forgedb/main.go`.)

```sh
docker volume ls | grep forgedb
docker volume inspect forgedb_forgedb-node1-data
```

## 14. Shutting down

```sh
docker compose down
```

Stops and removes the containers **but keeps the named volumes** -- your
data survives. This is the normal, safe way to shut the cluster down.

## 15. Resetting the cluster

```sh
docker compose down -v
```

**This deletes the named volumes.** Every node starts from nothing the
next time you run `docker compose up -d` -- fresh Raft state, empty KV
store, new elections. Only run this when you actually want to discard
all cluster state. `docker compose down` (no `-v`) never does this.

## 16. Limitations

- **Plaintext gRPC.** Node-to-node traffic is unencrypted. This is
  acceptable inside a private Docker network for local
  development/testing/demo purposes; it is not a production security
  posture. See [Security notes](#security-notes).
- **Static membership.** The cluster size and identities are fixed at
  startup via `PEERS`; there is no add/remove-node operation.
- **Single-host topology.** `docker-compose.yml` runs all three nodes on
  one Docker host. Nothing in ForgeDB's gRPC transport assumes that, but
  Phase 14 does not ship a multi-host (e.g. Swarm/Kubernetes) topology.
- **No automatic snapshot policy.** Raft log compaction
  (`POST /admin/snapshot`) is manual/operator-triggered, exactly as
  Phase 9 designed it -- Phase 14 does not add a background policy.
- **The Raft persistence cost found in Phase 13's benchmarking is
  unchanged**: every proposal still rewrites and fsyncs the full
  persisted Raft log, which real network latency makes more visible, not
  less. Phase 14 deliberately does not re-optimize this.
- **SSTable/compaction storage engine pieces** (`internal/storage/sstable`,
  `manifest`, `compaction`) exist in the tree but are not wired into the
  live `MemStore` write path -- unchanged from before Phase 14.

## 17. Security notes

This deployment configuration is intended for **local development,
testing, demonstration, and controlled environments**. It is **not** a
production security configuration:

- gRPC between nodes is plaintext (no TLS). A production deployment
  would need to add transport security -- out of scope for Phase 14.
- The image runs as a non-root user with a minimal Alpine base, no
  secrets baked in, and no unnecessary exposed ports, which are
  reasonable baseline hardening steps, but they do not substitute for
  network-level security (e.g. running this on an untrusted network).
- Do not expose the gRPC or HTTP ports directly to the public internet
  as configured here.
- The HTTP API requires a static bearer token (`FORGEDB_API_TOKEN`,
  see [Section 1.1](#11-authentication)) on every endpoint except
  `/health`/`/ready`. This is a minimal boundary control, not a full
  authorization system -- there is one shared token for every caller
  (no per-user identity, scopes, or rotation), and the connection itself
  is still plaintext HTTP unless you terminate TLS in front of it (e.g.
  at the Cloudflare tunnel). Treat the token like any other secret:
  generate it randomly, never commit it, and rotate it (restart every
  node with a new `FORGEDB_API_TOKEN`) if it may have leaked.

## Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| Container cannot connect to a peer | Wrong service name/port in `PEERS`, or the peer container is down | `docker compose logs <service>` for `transport:` errors wrapping `peer unreachable`; `docker compose ps` |
| No leader elected after startup | Still within normal election time (allow a few seconds); or fewer than a majority of containers are actually running | `curl .../cluster` on each node; `docker compose ps` |
| `bind: address already in use` on `docker compose up` | A host port (8081-8083, 9091-9093) is already used by something else | `netstat`/`Get-NetTCPConnection` for the port in question, or edit the `ports:` mapping |
| `permission denied` touching `/data` | A volume was previously created with different ownership (e.g. by an old image version) | `docker volume rm` the affected volume if its data is not needed, or `docker exec` in and `chown` |
| Volume contains stale state from a previous experiment | You reused a named volume across unrelated test runs | `docker compose down -v` to reset (destroys data -- see section 15) |
| A node has the wrong ID / duplicate ID | Typo in `NODE_ID`/`PEERS` across services | `cmd/forgedb` fails fast at startup with a `config:` error naming the problem (see `internal/config.Validate`) -- check `docker compose logs` |
| Invalid peer configuration | Malformed `PEERS` entry, mismatched self address | Same as above: the process refuses to start rather than run misconfigured, and logs exactly which check failed |
| `context deadline exceeded` between nodes | A peer is slow, overloaded, or network-partitioned | `internal/transport.Options.RPCTimeout` (`RPC_TIMEOUT` env var) controls this bound; a partitioned peer is expected to time out, not hang forever |
| HTTP endpoint unreachable from the host | Container not yet healthy, or its network was disconnected (see the partition test above) | `docker compose ps`, `docker network inspect forgedb_forgedb-net` |
| `401 Unauthorized` from `/kv`, `/cluster`, `/metrics`, or `/admin/snapshot` | Missing/wrong `Authorization: Bearer <token>` header, or your shell's `$FORGEDB_API_TOKEN` doesn't match what the containers were started with | Confirm the header is set exactly as `Authorization: Bearer <token>` (not a query parameter); re-check `.env` / the exported value |
| `docker compose up`/`config` fails with "FORGEDB_API_TOKEN must be set" | `FORGEDB_API_TOKEN` isn't set in your shell or `.env` | See [Section 1.1](#11-authentication) |

## What Phase 14 proves

- The existing Raft implementation (Phases 5-9), state machine (Phase 7),
  storage engine (Phases 1-4), and observability surface (Phase 12) all
  run correctly as independent OS processes in separate containers,
  communicating over a real Docker network via real gRPC, with
  persistent per-node storage.
- Leader election, replication, linearizable reads (`ConsistentGet`),
  request deduplication, snapshotting, and recovery-from-persisted-state
  all behave the same way they do in-process -- real networking does not
  weaken any of Raft's or ForgeDB's existing correctness guarantees.
- The cluster tolerates a node crash, a node restart, a full cluster
  restart, and a genuine network partition, in every case continuing to
  make progress on the majority side and recovering the minority side
  once it rejoins.

## What remains beyond this roadmap

Phase 14 is the final planned roadmap phase. It does **not** establish
production readiness: dynamic membership, sharding, TLS, multi-host
orchestration (Kubernetes or otherwise), automatic backups, and the
Phase-13-documented Raft persistence cost all remain future work, should
the project continue beyond its original roadmap.

---

### Health vs. readiness vs. leadership

- **`/health`** -- is the process up and answering HTTP at all.
- **`/ready`** -- has this node finished initializing and can it reach
  its own storage. A follower is exactly as ready as a leader.
- **Leadership** (`/cluster`'s `raft.Role`/`raft.LeaderID`) -- a separate
  concept from both of the above. A perfectly healthy, perfectly ready
  follower still cannot serve a write or a linearizable read itself (see
  the 421 response in section 6) -- that is expected, not a fault.

### Crash vs. partition

- **Crash**: the process is gone (`docker stop`/`docker kill`). Other
  nodes eventually stop receiving any response from it at all.
- **Partition**: the process stays alive, but its network path to some
  or all peers is removed (`docker network disconnect`). This is a
  materially different failure mode worth testing separately -- see
  section 11.
