# Phase 14 — Docker, Real-Network Deployment & Operational Hardening

This document is the design writeup for Phase 14: why each piece exists,
how it fits the pre-existing Phase 0-13 architecture, and which
correctness properties were actually verified against a real,
multi-container deployment. For hands-on operator instructions (build,
start, test failures, reset), see [`deploy/README.md`](../../deploy/README.md).

Phase 14 is the final planned roadmap phase. It answers one question:
**can the existing ForgeDB architecture run correctly as real
independent processes, communicating over a real network, with
persistent storage?** It does not redesign Raft, storage, snapshots, or
the consistency model, and it does not establish production readiness
(see [Known limitations](#known-limitations)).

## 1. Architecture

Before Phase 14, ForgeDB's only transport was `raft.InMemoryTransport`:
a synchronous, in-process map of node ID to handler, used by every test,
by the chaos harness, and by `cmd/forgedb`'s pre-Phase-14 demo code.
There was no gRPC, no protobuf, no environment-driven configuration, no
client-facing write API, and no Docker artifacts anywhere in the
repository (confirmed by inspection before writing any Phase 14 code).

Phase 14 adds exactly four things, each behind an existing abstraction
or filling an acknowledged gap, never a parallel implementation:

```
                    Client
                       |
                       | HTTP
                       v
          +-------------------------+
          |     internal/api        |  Phase 12 diagnostics (unchanged)
          |  /health /ready         |  + Phase 14 addition:
          |  /cluster /metrics      |  /kv/{key} PUT GET DELETE
          |  /admin/snapshot (new)  |
          +------------+------------+
                       |
                       v
          +-------------------------+
          |     internal/dbnode     |  unchanged: Propose / ConsistentGet
          +------------+------------+
                       |
                       v
          +-------------------------+
          |      internal/raft      |  unchanged: consensus logic,
          |  Transport / RPCHandler |  term/vote/log/commit/snapshot
          |  interfaces (Phase 5)   |  rules
          +------------+------------+
                       |
          +------------+-------------+
          |                          |
          v                          v
  InMemoryTransport           internal/transport   <- Phase 14, new
  (tests, chaos, Phase 10)    GRPCTransport/Server
                                     |
                                     v
                              real gRPC, real TCP,
                              separate OS processes
```

### 1.1 Transport

`internal/raft.Transport` and `internal/raft.RPCHandler` (Phase 5) are
unchanged. `internal/transport` (previously an empty placeholder
directory) is Phase 14's gRPC implementation of both:

- **`GRPCTransport`** (`internal/transport/grpc_transport.go`) implements
  `raft.Transport`: `SendRequestVote`/`SendAppendEntries`/
  `SendInstallSnapshot` each dial (lazily, once, and reuse the
  connection thereafter) the target peer's gRPC address, convert the
  request to protobuf, call it under a bounded `context.WithTimeout`,
  convert the reply back, and wrap any failure as
  `raft.ErrPeerUnreachable` -- the exact error `InMemoryTransport`
  already used for an unreachable peer, so `internal/raft` needs zero
  changes to treat a real network failure exactly like a dropped
  packet.
- **`Server`** (`internal/transport/grpc_server.go`) implements the
  generated `raftpb.RaftTransportServer`: it decodes each incoming RPC
  and forwards it straight to a `raft.RPCHandler` (in production, a
  `dbnode.Node`'s `*raft.Node`, via `Node.Raft()`). It holds no Raft
  state and makes no decisions -- every term check, vote, log-match, and
  commit rule still happens inside `internal/raft`, exactly as it does
  for `InMemoryTransport` today.
- **`internal/transport/convert.go`** is pure, allocation-only
  field-for-field conversion between `internal/raft`'s structs and the
  generated protobuf messages. No business logic.

`GRPCTransport` deliberately does **not** implement the `registrar`
interface `dbnode.Open` checks for (the one `*InMemoryTransport`
implements, for test convenience). A real network transport has no
in-process handler table to register against; reachability instead
depends on `Server` actually listening on this node's own configured
gRPC address. `dbnode.Open` already tolerates a transport that isn't a
`registrar` (it silently skips that step) -- no change was needed there.

### 1.2 Protobuf contract

`api/proto/raft.proto` defines one gRPC service, `RaftTransport`, with
three RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`) and
messages that mirror `internal/raft/message.go`'s structs field for
field. Generated code lives in `api/proto/raftpb/` (`raft.pb.go`,
`raft_grpc.pb.go`), produced by `protoc` + `protoc-gen-go` +
`protoc-gen-go-grpc`. `InstallSnapshot`'s `data` field carries a whole,
unchunked snapshot in one message, matching Phase 9's existing
"snapshots are not chunked" design -- Phase 14 does not add chunking
(see [Known limitations](#known-limitations)); it only makes sure the
gRPC message-size ceiling (`internal/transport.Options.MaxMessageBytes`,
default 64 MiB) doesn't silently truncate a realistic snapshot the way
gRPC's unconfigured 4 MiB default would.

### 1.3 Client-facing API: a real gap, not a Phase 14 redesign

Before Phase 14, `internal/api` was **exclusively** a read-only
diagnostics surface (`/health`, `/ready`, `/cluster`, `/metrics`) --
every route was wrapped in a `readOnly()` guard that rejected non-GET
requests, by design. There was no `/kv` endpoint of any kind, and
`cmd/forge-client` was an empty directory. This is a real, acknowledged
gap: Phase 14's test matrix (PUT/GET/DELETE against a deployed cluster,
leader-failure tests, persistence tests) is impossible to exercise at
all without *some* client-facing write path.

`internal/api/kv.go` adds the minimum necessary surface:

- `PUT /kv/{key}`, `DELETE /kv/{key}` -- build a `statemachine.Command`
  and call the existing `(*dbnode.Node).Propose`, then
  `(*dbnode.Node).WaitApplied` (a small refactor pulling the private
  `waitForApplied` `ConsistentGet` already used out into an exported
  method, so this new code can reuse it instead of duplicating it).
- `GET /kv/{key}` -- calls the existing `(*dbnode.Node).ConsistentGet`
  (Phase 8.5's linearizable read) directly. No new read path.
- `POST /admin/snapshot` -- calls the existing
  `(*dbnode.Node).CreateSnapshot` at the node's own current
  `LastApplied`. Phase 9 deliberately has no automatic snapshot policy;
  this operator/test trigger does not add one -- it exists so a
  real-network integration test can force a snapshot boundary the same
  way an in-process test would by calling `CreateSnapshot` directly,
  which a separate OS process cannot do.

None of this is a second write path or a new consistency model --
every byte still goes through the one `Propose` -> majority commit ->
`Applier` -> `KVStateMachine` -> `storage.Store` pipeline Phase 8
already built, and every read still goes through Phase 8.5's ReadIndex
barrier.

### 1.4 Configuration

`internal/config` previously had no loading mechanism at all -- `Config`
and `Node` were plain structs with no env/flag/file parsing, and
`cmd/forgedb` hard-coded a single-node `Config{}` literal. Phase 14 adds
`Load()` (`internal/config/load.go`), reading the environment variables
listed in [Section 4](#4-node-configuration) below, plus `Validate()`
(fail-fast checks) and three accessor methods
(`PeerIDs`/`PeerGRPCAddrs`/`PeerHTTPAddrs`) that derive what
`internal/raft`, `internal/transport`, and `internal/api` each need from
one shared `Config`.

`config.Node{ID, HTTP, GRPC}` (which already existed, unused, before
Phase 14) is reused as the peer-roster entry type rather than
introducing a parallel struct.

## 2. Configuration

A node's full configuration comes from environment variables (see
`internal/config/load.go` for the authoritative list and defaults):

| Variable | Default | Meaning |
|---|---|---|
| `NODE_ID` | *(required)* | This node's unique ID |
| `HTTP_ADDR` | `:8080` | Client-facing HTTP bind address |
| `GRPC_ADDR` | `:9090` | Node-to-node gRPC bind address |
| `DATA_DIR` | `data` | Root of this node's persistent storage |
| `PEERS` | `` (none) | Full cluster roster, see below |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |
| `METRICS_ENABLED` | `true` | Only affects the pre-Phase-14 demo path |
| `RPC_TIMEOUT` | *(transport default, 2s)* | Per-RPC gRPC deadline |
| `TICK_INTERVAL` | *(100ms)* | Real-time interval between logical Raft ticks |
| `FORGEDB_API_TOKEN` | *(required)* | Static bearer token for the client-facing HTTP API -- see [Section 2.1](#21-http-api-authentication) |

`PEERS` is a comma-separated list of `id=grpcAddr` or
`id=grpcAddr|httpAddr` entries, naming **every** node in the cluster,
including the current one:

```
PEERS=node-1=forgedb-1:9090|forgedb-1:8080,node-2=forgedb-2:9090|forgedb-2:8080,node-3=forgedb-3:9090|forgedb-3:8080
```

The same `PEERS` value is given to every node (see `docker-compose.yml`'s
`x-peers` YAML anchor) -- only `NODE_ID`, `HTTP_ADDR`, and `GRPC_ADDR`
differ per node. `Config.PeerIDs()`/`PeerGRPCAddrs()` filter out the
current node's own entry (Raft's `Peers` must never include the node
itself); `PeerHTTPAddrs()` keeps every entry, including self, for the
not-leader redirect hint (see [Section 7](#7-leader-redirection)).

### 2.1 HTTP API authentication

Phase 14's `internal/api` was unauthenticated by design -- a reasonable
posture while the only way to reach it was a private Docker network.
That stopped being true once this deployment became reachable through
a public Cloudflare Quick Tunnel: an unauthenticated `/kv` write path
(and `/admin/snapshot`, and `/cluster`/`/metrics`'s information
disclosure) on the open internet is not acceptable, so Phase E
(`internal/api/auth.go`) adds one reusable authentication middleware in
front of every route except two:

| Endpoint | Auth required? | Why |
|---|---|---|
| `GET /health` | No | Liveness probe; infrastructure health checks have no way to carry a token |
| `GET /ready` | No | Readiness probe; same reasoning as `/health` |
| `GET /metrics` | **Yes** | Exposes internal counters/histograms |
| `GET /cluster` | **Yes** | Exposes Raft term/role/leader and storage stats |
| `GET/PUT/DELETE /kv/{key}` | **Yes** | The client-facing read/write surface itself |
| `POST /admin/snapshot` | **Yes** | Operator-only trigger |

**Request format** -- exactly one scheme is accepted, the standard
RFC 6750 bearer form:

```
Authorization: Bearer <FORGEDB_API_TOKEN>
```

A token passed as a query parameter (`?token=...`) or a cookie is never
consulted. Missing, malformed, or wrong credentials all return `401
Unauthorized` without ever invoking the underlying handler. The
configured token is compared with `crypto/subtle.ConstantTimeCompare`
and never appears in a log line, a metric label, or a response body.

**Configuration** -- `FORGEDB_API_TOKEN` has no default:
`config.Config.Validate()` fails the process at startup, before any
listener opens, if it is unset or empty -- the same "fail fast rather
than run misconfigured" rule every other required setting already
follows (see [Validation](#validation) below). Generate a strong random
value for real use, e.g. `openssl rand -hex 32`; never commit it (see
`.env.example` and `docker-compose.yml`'s `FORGEDB_API_TOKEN` entry).
`forge-client` (`cmd/forge-client`) takes the same value via `-token` or
`FORGE_CLIENT_TOKEN`.

CORS is explicitly out of scope for this change and will be addressed
separately.

### Validation

`Config.Validate()` runs before anything else starts, and a failure
exits the process immediately (never a partially-configured node):

- `NodeID`, `HTTP`, `GRPC`, `DataDir` all non-empty; `HTTP`/`GRPC` parse
  as valid `host:port`.
- `FORGEDB_API_TOKEN` is non-empty (see [Section 2.1](#21-http-api-authentication)).
- No two peer entries share an ID; no two distinct peer IDs share a gRPC
  address.
- **Self-address coherence, with a wildcard-bind exception.** If `PEERS`
  includes an entry for this node's own ID (the normal case), its gRPC
  port must match the configured `GRPC` port, and its host must match
  too -- *unless* `GRPC` binds a wildcard address (`0.0.0.0`, `::`, or
  the host-less `:9090` form), in which case only the port is checked.
  This exception is not cosmetic: it is exactly the standard Docker
  pattern every service in `docker-compose.yml` uses (bind `0.0.0.0:9090`
  inside the container, advertise `forgedb-1:9090` to peers via the
  Compose service name) -- see [Section 9](#9-a-real-defect-found-and-fixed-by-this-deployment-work)
  for how an earlier, stricter version of this check was caught by
  actually running the containers.

## 3. Docker architecture

`Dockerfile` is a two-stage build:

1. **builder** (`golang:1.27.1-alpine`): `go mod download`, then
   `CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w"` for both
   `cmd/forgedb` and `cmd/forge-client`, producing static binaries.
2. **runtime** (`alpine:3.20`): installs only `ca-certificates` and
   `curl` (the latter solely for `HEALTHCHECK`), creates a non-root user
   `forgedb` (uid/gid 10001) owning `/data`, copies in the two binaries,
   and runs as that user. `ENTRYPOINT ["forgedb"]`; `forge-client` is
   reached with `docker run --entrypoint forge-client ...` (see
   `deploy/README.md` section 6).

Resulting image: ~15 MB of actual content (`docker images` "Content
size") on top of the Alpine base -- small, but correctness and
maintainability were prioritized over shaving every extra megabyte.

`HEALTHCHECK` curls this process's own `/health` -- liveness only, never
leadership (a follower reports healthy exactly like a leader; see
`deploy/README.md`'s "health vs. readiness vs. leadership" section).

## 4. Network configuration

`docker-compose.yml` defines one bridge network, `forgedb-net`, shared
by all three services. Every node resolves its peers by **Compose
service name** (`forgedb-1`, `forgedb-2`, `forgedb-3`), never by
`localhost` or a hard-coded container IP -- inside a container,
`localhost` means that container, and container IPs can change. Each
node's internal ports are identical (`8080` HTTP, `9090` gRPC); only the
**host-published** ports differ per node (`8081/8082/8083` and
`9091/9092/9093`), via standard Compose `ports:` mappings.

## 5. Volumes

Each node has its own named Docker volume (`forgedb-node1-data`,
`forgedb-node2-data`, `forgedb-node3-data`), mounted at `/data` inside
its container. Volumes are never shared between nodes -- sharing one
would silently corrupt state by letting two independent Raft/storage
instances write to the same files.

## 6. Persistent storage layout

Unchanged from pre-Phase-14 `internal/dbnode`/`internal/raft`/
`internal/storage` layout; Phase 14 only chooses where `DATA_DIR` points
inside a container:

```
$DATA_DIR/raft/raft-state            Raft's currentTerm, votedFor, log
                                      (internal/raft.FilePersister)
$DATA_DIR/raft/raft-state.snapshot   Raft's most recent snapshot
$DATA_DIR/kv/wal.log                 KV engine's write-ahead log
                                      (storage.NewMemStore)
```

`cmd/forgedb/main.go` joins `cfg.DataDir` with `raft`/`kv` exactly as
`dbnode`'s own package doc requires each node's two directories be
distinct.

## 7. Leader redirection

`internal/api`'s diagnostic server had no client write path before
Phase 14, so there was no existing not-leader behavior to "verify ...
works over the real network" -- both sides were built together. A
write or `ConsistentGet` attempted against a non-leader node returns
**HTTP 421 Misdirected Request** with:

```json
{"error":"not_leader","leader_id":"node-2","leader_http":"forgedb-2:8080"}
```

`leader_id` comes from `raft.Node.State()`'s `leaderID` (empty if
genuinely unknown, e.g. mid-election). `leader_http` is populated only
when this node's own `PeerHTTPAddrs()` has an entry for that ID -- no
new client routing protocol beyond this one hint. `cmd/forge-client`
follows it automatically; a plain `curl` caller can follow it by hand.

## 8. Operational tests performed

All of the following were run against the **actual** three-container
Docker Compose cluster on this machine (Docker Desktop, Windows host),
not simulated:

- **Startup**: all three containers start, each becomes `healthy` per
  its `HEALTHCHECK`, and a leader is elected within a couple of seconds
  regardless of start order.
- **PUT/GET/DELETE**: via both raw `curl` and `forge-client` (the latter
  run as a one-off container on `forgedb-net`, finding the leader
  itself without being told which node it is).
- **Leader crash + failover**: `docker stop` on the leader; remaining
  two nodes elect a new leader; a write through the new leader succeeds
  and is immediately readable; restarting the old leader rejoins it as
  a follower that catches up to the new leader's log.
- **Follower crash + restart**: `docker stop`/`docker start` on a
  follower while the leader continues serving writes; the restarted
  follower recovers its persisted Raft state from disk and catches up.
- **Full cluster restart**: `docker compose down` (volumes preserved)
  then `docker compose up -d`; previously-committed data is present
  immediately after the restart (read directly from the recovered WAL);
  a fresh write after restart requires -- and gets -- a new
  current-term commit (see [Section 10](#10-real-network-current-term-behavior)).
- **Real network partition**: `docker network disconnect` on the
  current leader (not a crash -- the process stays alive). The majority
  side elects a new leader and continues accepting writes; the isolated
  node becomes unreachable even from the host's published port (an
  observed, documented consequence of how Docker's port-forwarding is
  tied to the network attachment); `docker network connect` heals it,
  and the cluster converges to a single leader and a consistent
  `CommitIndex` across all three nodes (see
  [Section 10](#10-real-network-current-term-behavior) for a note on
  the re-election churn this specific scenario produced).
- **Persistence**: a value written before `docker compose down` is
  readable immediately after `docker compose up -d` with no data loss.
- **Smoke test script** (`deploy/scripts/smoke-test.sh`): builds,
  starts, waits for a leader (bounded poll), PUT/GET/DELETE/verify-404,
  checks `/cluster` and `/metrics`, restarts a follower, and confirms a
  value written before the restart is still readable after -- all
  automated, all passing, with no arbitrary sleeps as synchronization
  (only bounded polling loops).

## 9. A real defect found and fixed by this deployment work

Two genuine defects surfaced only once ForgeDB actually ran as separate
processes/containers -- exactly the kind of thing Phase 14 exists to
find. Both are documented here in full per the "do not claim a
pre-existing issue was fixed unless it actually was" rule; neither is a
pre-existing Phase 0-13 bug -- both are in code Phase 14 itself added.

### 9.1 Default Raft election-timeout seeding could collide across real processes

`raft.NewNode`'s default `Options.Rand` (used whenever a caller does not
supply one -- true of `cmd/forgedb`'s production wiring) seeded from
`rand.NewSource(time.Now().UnixNano())`. Starting three real,
independent OS processes within the same coarse clock tick (observed in
practice on Windows, whose clock resolution is roughly 15ms) let two or
more nodes read the identical nanosecond timestamp and therefore produce
byte-for-byte identical "randomized" election timeout sequences --
every subsequent election then split-voted identically, forever, with
no leader ever elected. This was reproduced directly: three real
`forgedb` processes on localhost sat at `Role: Candidate` through 22+
terms over 25 seconds before the fix.

**Fix** (`internal/raft/raft.go`): the default source is now seeded from
`crypto/rand` instead of wall-clock time (`randomSeed()`), which cannot
collide regardless of clock resolution, whether across processes or
across two `NewNode` calls made in the same process in quick succession
(the same root cause had also been causing intermittent flakiness in
this session's own new `internal/api` tests before it was tracked down).
This changes nothing about election/heartbeat/commit semantics or
timing *behavior* -- only the quality of the randomness source behind
`Options.Rand`'s already-documented purpose. Every existing test that
supplies its own deterministic `Rand` is completely unaffected. Covered
by `internal/raft/raft_test.go`'s
`TestRandomSeed_ProducesDistinctValues` and
`TestNewNode_DefaultRand_TwoNodesDoNotLockstep`.

### 9.2 Cross-node request-ID collision silently dropped a write after failover

Phase 14's first `/kv` implementation used one constant `ClientID`
(`"forgedb-http-api"`) for every node's API server, each with its own
independent, process-local `RequestID` counter starting at zero. A real
three-process failover test exposed the consequence directly: `PUT
persist-key=v1` through node-1 (then leader), kill node-1, `PUT
persist-key=v2` through the newly-elected node-2 -- and a subsequent
`GET` returned the stale `v1`. The state machine's `(ClientID,
RequestID)` deduplication (Phase 7, entirely correct on its own) had no
way to tell node-2's genuinely new write apart from a retried duplicate
of node-1's, because both nodes' counters had reached the same number
and shared the same `ClientID`.

**Fix** (`internal/api/server.go`, `kv.go`): each `Server`'s `clientID`
is now derived from its own node's ID (`"forgedb-http-api-" +
node.ID()`), making every `(ClientID, RequestID)` pair this process can
ever produce unique across the whole cluster for the lifetime of that
node identity. `internal/raft`'s commit/replication logic and
`internal/statemachine`'s deduplication logic were not touched -- the
bug and the fix are both entirely within Phase 14's own new HTTP layer.
Covered by
`internal/api/kv_test.go`'s
`TestClientID_UniquePerNode_PreventsCrossNodeDedupCollision`, and
re-verified against the real Docker cluster after the fix (the same
kill-leader-write-through-new-leader sequence now returns the correct,
latest value).

## 10. Real-network current-term behavior

After a full cluster restart, `/cluster` briefly reports `CommitIndex:
0` even though `LastLogIndex` already reflects the fully-recovered
persisted log -- `commitIndex` is volatile Raft state, reset by design
on every `NewNode` (see `docs/raft/phase6-raft-persistence.md`), and the
standard Raft rule that a leader can only *directly* advance commit
using an entry from its own current term means old-term entries are not
immediately re-marked committed. This is unchanged, pre-existing
behavior that Phase 14 deliberately does not alter (see
`internal/raft/raft.go`'s `NewNode` doc comment). In practice this was
never visible to a client: `ConsistentGet` immediately after a full
restart still returned the correct, already-durable value, because its
`WaitApplied` barrier trivially passed at target `0`, and `storage.Store`
itself (independent of Raft's commit bookkeeping) already held the
recovered state from the WAL.

Separately, healing the real network partition in testing (Section 8)
produced several rounds of term inflation and re-election churn after
the previously-isolated node reconnected with a much higher term than
the rest of the cluster (it had kept timing out and starting new
elections, alone, the entire time it was partitioned). The cluster did
converge -- to a single leader and a consistent `CommitIndex` across all
three nodes, with zero data loss or divergence -- but took on the order
of 10-15 seconds of churn to do so in this run. This is the well-known
consequence of not implementing Raft's optional "PreVote" extension,
documented in the original Raft paper; implementing PreVote would be a
Raft algorithm change, explicitly out of scope for Phase 14 (see
[Known limitations](#known-limitations)).

## 11. Observability

Reused without modification: `internal/metrics` (Phase 12's
hand-rolled Prometheus-format registry) and `internal/logging` (`slog`,
to stdout, picked up natively by `docker compose logs`). `/health`,
`/ready`, `/cluster`, `/metrics` are the same Phase 12 handlers; Phase
14 added no second metrics or logging implementation.

## 12. Validation

```
gofmt -l .                        -> no output (nothing to reformat)
go vet ./...                      -> clean
go build ./...                    -> clean
go build ./cmd/forgedb            -> clean
go build ./cmd/forge-client       -> clean
go build ./cmd/forge-chaos        -> clean
go test ./...                     -> ok (all packages)
go test ./internal/raft/...       -> ok
go test ./internal/transport/...  -> ok (real gRPC loopback tests)
go test ./internal/config/...     -> ok
go test ./internal/api/...        -> ok
docker compose config             -> valid
docker compose build               -> succeeds
docker compose up -d               -> all three containers healthy
deploy/scripts/smoke-test.sh      -> PASSED
go test -race ./...               -> unavailable: CGO_ENABLED requires a
                                      C compiler, and none is installed
                                      in this environment (no gcc/cc/
                                      clang found) -- a pre-existing
                                      environment limitation already
                                      noted in Phase 13, not something
                                      Phase 14 introduced or attempted
                                      to work around.
```

## Known limitations

- **Plaintext gRPC.** No TLS between nodes. Acceptable for local
  development/test/demo inside a private Docker network; not a
  production security posture.
- **Static membership only.** No add/remove-node; `PEERS` is fixed for a
  process's lifetime, exactly as `raft.Options.Peers` already required.
- **Single-host topology.** `docker-compose.yml` runs all three nodes on
  one Docker host; nothing in the gRPC transport assumes that, but no
  multi-host orchestration is shipped.
- **`InMemoryTransport` remains in use** by every existing unit test, the
  `chaos` package, and `correctness` harness -- unchanged, and correctly
  so: Phase 14 adds a second, real transport behind the same interface,
  it does not replace the deterministic one those suites depend on.
- **No snapshot chunking.** `InstallSnapshot` still sends an entire
  snapshot as one RPC/message (bounded by a deliberately configured,
  documented gRPC message-size limit, not an unlimited one) -- unchanged
  from Phase 9.
- **The Phase 13-documented Raft persistence cost is unchanged**: every
  proposal still rewrites and fsyncs the entire persisted log. Real
  network latency makes this more visible operationally, which is
  useful information Phase 14 surfaces but deliberately does not fix.
- **SSTable/Manifest/compaction** (`internal/storage/sstable`,
  `manifest`, `compaction`) remain unwired from the live `MemStore`
  write path, exactly as before Phase 14.
- **No PreVote extension**: a node that was network-partitioned for a
  while and then reconnects can trigger several rounds of unnecessary
  re-election before the cluster re-converges (Section 10) -- a known,
  accepted Raft tradeoff this phase does not address, since doing so
  would be a Raft algorithm change.
- **No production orchestration, secrets management, dynamic
  reconfiguration, sharding, or multi-region replication.** None of
  these were in scope.

This phase does not establish production readiness. It demonstrates
real-process deployment, persistent volumes, real networking, and
operational lifecycle/failure behavior on top of the Raft, storage, and
state-machine work already proven correct in Phases 0-13.
