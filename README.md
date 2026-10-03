# ForgeDB

Distributed Key-Value Database Engine with Raft Consensus

## Overview

ForgeDB is a from-scratch, replicated key-value store built around a
real Raft consensus implementation: persistent log replication, leader
election, a deduplicating state machine, log-structured local storage
with a write-ahead log, snapshot-based log compaction (including
chunked snapshot transfer for large state), a leader-aware HTTP
gateway, token authentication, and a React dashboard for operating and
observing a running cluster. It ships with a Docker Compose deployment
for a local 3-node cluster and documented steps for a public, Caddy +
Cloudflare-fronted deployment on a single Azure VM.

This README reflects what has actually been built, tested, and
verified as of this release. Where a component exists in the tree but
is not on the live request path, or where a deployment property is
aspirational rather than currently running, that distinction is called
out explicitly rather than implied away.

## Why ForgeDB

ForgeDB exists to demonstrate the full, unglamorous middle of building
a distributed database: not just "Raft works" in isolation, but Raft
wired to a real durable storage engine, a deduplicating state machine
that survives restarts and retries, snapshotting that doesn't corrupt
state under interruption, a gateway that tracks leadership changes
without client-side retry logic, and a dashboard that is honest about
which of its numbers come from a live node versus illustrative mock
data. Each of these is independently easy to fake; ForgeDB's test
suite (chaos scenarios, a linearizability checker, and real 3-node
Docker failover) is built specifically to make faking them hard.

## Architecture

```
                    Client
                      |
                      v
                HTTP API
                      |
                      v
              Leader-Aware Gateway
                      |
                      v
              +---------------+
              |  Raft Cluster |
              +---------------+
                /      |      \
               /       |       \
            Node 1   Node 2   Node 3
               \       |       /
                \      |      /
                 Replicated Log
                       |
                 Majority Commit
                       |
                 State Machine
                       |
                 Persistent Storage
```

Storage path (per node):

```
    WAL
     |
     v
    MemTable
     |
     v
    Immutable MemTable
     |
     v
    SSTables  --> Bloom filters, Indexes   [implemented, NOT on live write path]
     |
     v
    Manifest / Version State                [implemented, NOT on live write path]
     |
     v
    Compaction                              [implemented, NOT on live write path]
```

**Important:** `internal/storage/sstable`, `internal/storage/manifest`,
and `internal/storage/compaction` are real, independently tested
packages, but `storage.MemStore` (the implementation actually wired
into `dbnode.Node`) never calls into any of them. The live write path
today is:

```
Propose -> Raft commit -> Applier.ApplyAvailable -> KVStateMachine.Apply
  -> storage.MemStore.Put/Delete -> WAL Append+Sync -> MemTable
```

Durability on the live path comes from the WAL plus Raft's replicated,
persisted log — not from SSTables. See `benchmark/README.md`'s "What
the live write path actually exercises" section for the exact
reasoning and the tests this is based on.

Snapshot path:

```
    Raft Snapshot
          |
          v
    Chunked Transfer
          |
          v
    Per-chunk Validation (checksum, gap/duplicate/overflow detection)
          |
          v
    Complete Snapshot
          |
          v
    Existing Snapshot Validation
          |
          v
    Atomic Persistence
          |
          v
    Snapshot Boundary
          |
          v
    State Machine Restoration
```

Public deployment architecture (as documented; see Deployment and
Known Limitations below for what is currently actually running):

```
    ForgeDB Dashboard
          |
        Vercel
          |
    Cloudflare Quick Tunnel
          |
       Azure VM
          |
        Caddy
          |
    ForgeDB Gateway
          |
     Current Raft Leader
          |
      3-node cluster
```

## Raft Consensus

`internal/raft` implements leader election, persistent log replication,
AppendEntries/RequestVote/InstallSnapshot RPCs, majority-based commit
advancement, term-based safety (at most one leader per term, stale-term
rejection, higher-term step-down), and snapshot-aware log truncation at
an exact boundary. It has no dependency on any particular transport
(`internal/transport` carries it over gRPC in production;
`InMemoryTransport` carries it in-process for tests and benchmarks) or
storage backend (`FilePersister` for production, `MemoryPersister` for
tests).

Verified in this pass: a real 3-node Docker cluster survives a leader
`docker stop`, elects a new leader, continues accepting writes through
the same gateway address, and the old leader rejoins as a follower and
catches up to the cluster's commit index after restart (see Testing
below).

## Storage Engine

Each node's local storage (`internal/storage`) is a WAL-backed MemTable
(`storage.MemStore`): every Put/Delete is appended to the WAL, fsynced,
and applied to an in-memory table before the call returns. Crash
recovery (`internal/recovery` logic inside `internal/storage/wal`)
replays the WAL on restart, including a torn/partial final record. A
complete LSM pipeline exists as independently tested components
(`internal/storage/sstable`, `internal/storage/manifest`,
`internal/storage/compaction`) but is not wired into the live write
path — see Architecture above.

## State Machine and Request Deduplication

`internal/statemachine` applies committed Raft log entries to the
storage layer exactly once, keyed by client-supplied request IDs, so a
client or gateway retry after a timeout or leader change cannot
duplicate a write. This dedup state is itself part of what a snapshot
captures and a restart recovers, verified by
`internal/dbnode`'s restart/convergence test suite
(`TestNode_FullClusterRestart_ConvergesToRecoveredState`,
`TestNode_RepeatedSnapshotRestartCycles_StateCorrect`, and related
tests).

## Consistency Model

Writes go through Raft and are only visible once committed by a
majority and applied locally. Reads can be served two ways:

- A plain `GET` against a follower or the leader returns that node's
  locally applied state (bounded staleness on a follower).
- `ConsistentGet` (used internally for linearizable reads) performs a
  Raft `ReadIndex` round before reading, so the result reflects every
  write committed up to the point the read was issued, even when
  served from the current leader rather than a quorum round-trip per
  read.

`correctness/` includes a linearizability checker that replays
recorded concurrent histories (including failures, retries, and leader
changes) against a reference model and confirms no observed
anomaly is possible under any legal linearization.

## Snapshots and Recovery

A node snapshots its state machine on operator request
(`POST /admin/snapshot`), recording a `(index, term)` boundary,
persisting the snapshot atomically, and compacting the Raft log up to
that boundary only after the snapshot itself is durably saved — a
save failure at either step leaves the prior state intact rather than
a torn boundary (`TestCreateSnapshot_SnapshotSaveFailure_LeavesLogUntouched`,
`TestCreateSnapshot_LogSaveFailure_RollsBackCompactionButKeepsSnapshot`).
Restart reconstructs the log either from a snapshot alone, from a
snapshot plus a suffix of log entries with correct overlap handling, or
fails closed on a corrupted snapshot file rather than silently losing
data.

## Chunked Snapshot Transfer

When a follower is far enough behind that the leader can no longer
serve it from its log, the leader transfers its snapshot in bounded
chunks (`InstallSnapshot` with offset/chunk semantics) rather than one
unbounded RPC payload. The transfer protocol is covered by a dedicated
test suite exercising: multi-chunk reconstruction byte-for-byte,
corrupted-chunk rejection, missing-chunk-gap rejection, duplicate-chunk
rejection, overflow-beyond-declared-total rejection, an incomplete
transfer never activating as a durable snapshot, a restart during an
in-progress transfer not resurrecting the partial data, a newer
snapshot correctly superseding an older in-flight transfer, and an
existing good snapshot surviving a failed incoming transfer untouched.
All of these were re-run and passed in this release (see Testing).

## Failure Handling

Demonstrated in this pass against a real 3-node Docker cluster (not a
simulation): leader crash → election → gateway reroutes writes to the
new leader with no client-visible API change → old leader restarts,
rejoins as a follower, and catches up to the cluster's commit index,
with both pre- and post-failover writes intact throughout. Graceful
shutdown (`docker stop`, i.e. SIGTERM) was verified to drain cleanly —
HTTP, Raft, and storage all report a stopped state with no error in the
logs — and the node recovers cleanly from the WAL and its last snapshot
on restart.

## Observability

Every node exposes Prometheus-format metrics at `GET /metrics`
(protected, like every endpoint except `/health`/`/ready`): Raft
election/term/commit counters, AppendEntries send/success/fail
counters, replication and commit counters, snapshot
created/installed counters, and per-component error counters, among
others. The dashboard's Observability page parses this text directly
(`dashboard/src/lib/metrics/parsePrometheusText.ts`) rather than a
separate structured API. Verified in this pass: a live 3-node cluster's
`/metrics` endpoint returns well-formed Prometheus text reflecting real
election/replication activity generated during the failover test.

## API

Router source of truth: `internal/api/server.go`.

| Method | Path | Auth | Notes |
|---|---|---|---|
| GET | `/health` | none | Liveness; always 200 while the process is up. |
| GET | `/ready` | none | Readiness. |
| GET | `/metrics` | Bearer | Prometheus text exposition. |
| GET | `/cluster` | Bearer | This node's Raft/storage status (role, term, indices, peer lag when leader). |
| GET | `/kv/{key}` | Bearer | 200 + raw bytes, 404 not found, 421 not-leader (with `leader_id`/`leader_http` hint) on a linearizable-read path, 503 if a read can't currently be served. |
| PUT | `/kv/{key}` | Bearer | Body is the value, sent verbatim. 421 not-leader hint on a follower. |
| DELETE | `/kv/{key}` | Bearer | 421 not-leader hint on a follower. |
| POST | `/admin/snapshot` | Bearer | Triggers a snapshot at the node's current `LastApplied` index. |

Authentication: `Authorization: Bearer YOUR_TOKEN`, checked by
`internal/api/auth.go` before any protected handler runs. The gateway
(`internal/gateway`) does not itself hold or check the token — it
forwards whatever `Authorization` header the client sent, unchanged, to
whichever node currently answers as leader; that node's own middleware
is still the only thing that validates it.

CORS is governed by `FORGEDB_CORS_ORIGINS` (exact-origin allow-list,
empty by default — i.e. disabled unless explicitly configured); see
`internal/api/cors.go`.

## Dashboard

`dashboard/` is a Vite + React + TypeScript app with pages for an
overview, cluster topology, a Raft internals explorer, a KV console,
and observability. Most pages currently render from `src/data/mock*.ts`
and are labeled as such in the UI; the service layer
(`src/services/forgedbApi.ts`) defines the full intended live contract
and already implements real calls for health, cluster status, KV
read/write/delete, and raw metrics text, each with its own tagged
success/failure result type so a caller can distinguish "the backend
legitimately said not-found/not-leader" from "the backend could not be
reached" and decide whether to fall back to mock data. `useBackendCluster`
and `useKvConsole` are the hooks that make that decision for the
Overview/Cluster and KV Console pages respectively.

**Authentication note (fixed in this release):** every protected
backend endpoint requires a bearer token, but the dashboard's browser
code deliberately never attaches one — doing so would bake a real
`FORGEDB_API_TOKEN` into the publicly readable built JS bundle. Before
this release, that meant the dashboard's "live" mode could only ever
successfully reach the one unauthenticated endpoint, `/health`; `/cluster`,
`/kv/*`, and `/metrics` would 401 even in local development.
`dashboard/vite.config.ts`'s dev-only proxy now reads a non-`VITE_`-
prefixed `FORGEDB_API_TOKEN` from a local, untracked `dashboard/.env`
(server-side, in Node — never inlined into client code) and attaches it
to proxied requests, so a developer running `npm run dev` against a
real local cluster now gets genuinely live Cluster/KV/Observability
data. This fix is local-dev-only by design: a deployed build (e.g. on
Vercel) still has no server-side proxy to attach a token through, so
its "live" mode is still limited to `/health` unless and until a proper
backend-for-frontend/session layer is built — see Known Limitations.

## Local Development

```powershell
go build ./...
go test ./...
go vet ./...

cd dashboard
npm install
npm run dev     # proxies /api/* to a local ForgeDB node; see .env.example
```

## Docker Deployment

```powershell
cp .env.example .env
# edit .env: set FORGEDB_API_TOKEN to a random value (e.g. openssl rand -hex 32)

docker compose build
docker compose up -d
docker compose ps
```

This starts a 3-node Raft cluster (`forgedb-1/2/3`, each with its own
named volume) plus `forge-gateway`, the leader-aware reverse proxy
public clients should use. Every published port is bound to
`127.0.0.1` only — see `docker-compose.yml`'s comments and
`deploy/README.md` for the full operational guide (health, leader
discovery, failure testing, persistence, reset).

```powershell
docker compose down       # stops containers, PRESERVES volumes
docker compose down -v    # DESTROYS volumes -- do not run this casually
```

## Azure Deployment

The documented production topology runs all three nodes, the gateway,
and Caddy on a single Azure VM, fronted by either a temporary
Cloudflare Quick Tunnel (no inbound firewall rule required, hostname
changes on every tunnel restart) or a stable Caddy-terminated HTTPS
hostname (requires DNS plus inbound 80/443 in the VM's NSG) — see
`deploy/README.md` Section 18 and `docs/deployment/phase16-public-https.md`.
As of this release the VM's network security group allows only inbound
SSH; the stable-HTTPS NSG rules described in Section 18.2 are not
currently applied, consistent with the Cloudflare Quick Tunnel being
the active public path rather than direct Caddy exposure. This release
pass could not independently verify the live public endpoint or which
services are currently running on the VM beyond that it is powered on
and reachable on port 22 — see Known Limitations.

## Testing

```powershell
go test ./...          # all 25 packages; 19 contain tests, all pass
go vet ./...
go build ./...
go test -race ./...    # requires CGO; blocked on this Windows environment (no gcc)
```

Test suites of particular note:

- `internal/raft`: 44+ tests covering snapshots, chunked
  InstallSnapshot transfer (gap/duplicate/overflow/corruption
  rejection, restart-safety, supersession), and boundary conditions.
- `internal/dbnode`: restart/recovery/convergence tests including
  repeated snapshot-restart cycles and idempotent `Close`.
- `chaos/`: scripted and seeded-random fault injection (crashes,
  partitions, persistence/storage faults) against the real `dbnode.Node`
  composition, checking Raft safety invariants and eventual
  convergence.
- `correctness/`: a linearizability checker replaying concurrent
  operation histories against a reference model.

This release additionally ran a live, real 3-node Docker cluster
through: authentication (401/200 paths), CORS preflight behavior, full
KV PUT/GET/DELETE/404 lifecycle, a real leader `docker stop` →
election → gateway reroute → old-leader rejoin-and-catch-up cycle,
snapshot creation on all three nodes, and a graceful-shutdown →
restart → WAL+snapshot recovery cycle — see the demo runbook for the
exact steps and observed output.

## Benchmarking

See `benchmark/README.md` for the full methodology (what's measured,
what explicitly is not safe to compare, determinism, concurrency
scope) and `docs/benchmarking/phase13-benchmarking.md` for recorded
results with their git revision, Go version, and hardware. No
benchmark numbers are reproduced here, to avoid them going stale next
to code changes; run `go test ./... -run '^$' -bench=.` per that
README's instructions for current numbers on your own hardware.

## Project Structure

```
internal/raft           Raft consensus (election, replication, snapshots)
internal/dbnode         Node composition wiring Raft + storage + state machine
internal/statemachine   Deduplicating KV state machine
internal/storage        WAL-backed MemStore (the live storage path)
internal/storage/sstable, manifest, compaction   LSM components (not on live path)
internal/transport      gRPC transport for Raft RPCs
internal/api            HTTP API, auth, CORS
internal/gateway        Leader-aware reverse proxy
internal/config         Environment-driven configuration + validation
internal/metrics        Prometheus metric definitions
internal/logging        Structured logging
chaos/                  Deterministic fault-injection test harness
correctness/            Linearizability checker
benchmark/              Benchmarks and end-to-end workload harness
cmd/forgedb             Node binary
cmd/forge-gateway        Gateway binary
cmd/forge-client        Minimal HTTP CLI client
cmd/forge-chaos         Chaos scenario runner
dashboard/              React/TypeScript operator dashboard
deploy/                 Caddyfile, systemd env templates, operational scripts
docs/                   Per-phase design writeups (architecture, storage, raft, deployment, ...)
```

## Design Invariants

1. Raft owns replicated ordering.
2. Only committed entries are applied.
3. Followers do not independently mutate replicated state.
4. Raft durability and storage durability are conceptually separate.
5. Request deduplication survives restarts and snapshots.
6. Snapshot validation occurs before activation; a partial chunked
   transfer never becomes an active durable snapshot.
7. The gateway follows the current leader rather than pinning to one
   node.
8. Protected API endpoints require authentication, checked before any
   protected handler runs.
9. CORS is allow-listed, never wildcard, by default disabled.
10. Node ports are private (loopback-only in Docker); the gateway is
    the intended public entrypoint.
11. Graceful shutdown drains HTTP, Raft, and storage before exit.
12. Recovery (WAL + snapshot) reconstructs exactly the committed state
    that existed before a crash or restart.

## Known Limitations

- **SSTable/Manifest/Compaction are not on the live write path.** They
  exist as independently tested packages; `storage.MemStore` never
  calls into them. See Storage Engine above.
- **Cluster membership is static.** `PEERS` is fixed at process start
  for every node and the gateway; there is no add/remove-node
  operation.
- **gRPC between nodes is plaintext.** Acceptable only because that
  traffic stays inside a network not reachable from outside the
  Docker host, not because the protocol itself is secured.
- **One shared bearer token, no scopes or rotation.**
  `FORGEDB_API_TOKEN` authenticates every caller identically; rotating
  it means restarting every node with a new value.
- **Dashboard live mode in a deployed (non-dev) build is limited to
  `/health`.** The dev-proxy token-injection fix in this release only
  applies to local `npm run dev`; a production SPA build still cannot
  safely hold a bearer token. See Dashboard above.
- **Single-host topology.** All three nodes, the gateway, and Caddy
  run on one Azure VM in the documented deployment; losing that VM
  loses the cluster regardless of Raft's own replication guarantees.
- **The Cloudflare Quick Tunnel is temporary by design.** Its hostname
  changes on every restart and is never committed to tracked
  configuration.
- **Windows race-detector testing is environment-blocked.** `go test
  -race` requires CGO, and no C toolchain (gcc) is present in this
  environment; this is a toolchain limitation of this machine, not a
  known ForgeDB data race.
- **This release's Azure/public-endpoint verification is incomplete.**
  The VM is powered on and reachable on SSH (confirmed via `az vm
  list`/NSG inspection), but this pass could not interactively SSH in
  (the Azure CLI's AAD-certificate SSH flow requires an interactive
  session this automated pass could not drive) to confirm which
  services are currently running, or reach the ephemeral Cloudflare
  Quick Tunnel URL. Nothing on the VM was started, stopped, or
  modified during this attempt.
- **No automatic snapshot policy.** `POST /admin/snapshot` is
  manual/operator-triggered; there is no background compaction
  trigger.

## Future Work

- Dynamic cluster membership (add/remove nodes without a full restart).
- mTLS or another inter-node transport security mechanism for gRPC.
- A backend-for-frontend/session layer so a deployed (non-dev)
  dashboard build can safely reach authenticated endpoints without
  embedding a long-lived bearer token in client JS.
- A stable, non-Cloudflare-Quick-Tunnel production ingress as the
  default rather than an alternate path.
- Per-caller tokens/scopes in place of the single shared bearer token.
- Wiring `internal/storage/sstable`/`manifest`/`compaction` into the
  live write path, if and when MemTable-only storage stops being
  sufficient.

These are deliberately out of scope for ForgeDB 1.0.

## Demo

See [`docs/deployment/forgedb-1.0-demo-runbook.md`](docs/deployment/forgedb-1.0-demo-runbook.md)
for an exact, repeatable walkthrough: start the cluster, exercise
health/auth/CORS/KV, find the leader, fail it over, restart it, trigger
a snapshot, and shut down cleanly — with the actual output observed
when this runbook was followed during this release.

## License

No `LICENSE` file is currently present in this repository. In the
absence of one, default copyright applies (all rights reserved by the
author) — add a `LICENSE` file before treating this project as open
for reuse by others.
