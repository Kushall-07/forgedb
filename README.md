<div align="center">

# ⚡ ForgeDB

**A distributed key-value database engine built from scratch in Go, using Raft consensus to replicate writes across a cluster and survive node failures.**

<p>
  <img src="https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go" />
  <img src="https://img.shields.io/badge/Raft-Consensus-6f42c1?style=for-the-badge" alt="Raft Consensus" />
  <img src="https://img.shields.io/badge/gRPC-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="gRPC" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB" alt="React" />
  <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white" alt="Prometheus" />
</p>
<p>
  <img src="https://img.shields.io/badge/Tests-25%20packages-success?style=for-the-badge" alt="Tests" />
  <img src="https://img.shields.io/badge/Release-v1.0.0-informational?style=for-the-badge" alt="Release v1.0.0" />
</p>

### 🗺️ System Architecture

<img src="docs/architecture/forgedb-architecture.png" alt="ForgeDB architecture: client to HTTP API to leader-aware gateway to Raft cluster to replicated log to majority commit to state machine to WAL/MemTable storage, with a separate snapshot/recovery path and dashboard" width="850" />

</div>

A normal key-value store — think of it as a simple save/load box for `key → value` pairs — saves its data on one machine. If that machine dies, the data is unavailable (or gone) until someone fixes it. ForgeDB explores what happens once that single machine is no longer allowed to be the one source of truth: the same data lives on several machines at once, writes have to survive a crashed leader, a restarted node, and a retried request, and the whole system still has to agree on what the "true" value of each key is — without losing or duplicating anything along the way.

You don't need any prior background in distributed systems to follow this README. The next section explains the core idea — Raft consensus — in plain language before anything else.

---

## What is ForgeDB?

ForgeDB is a replicated key-value store, meaning the same data is copied across several machines ("nodes") instead of living on just one. It uses an algorithm called **Raft** to keep every node's copy of the data in agreement, even while machines crash, restart, or fall behind. Here's the whole idea, broken into plain-language steps:

- **Multiple nodes** (3 by default) each keep a full copy of the data — so losing one node doesn't mean losing the data.
- **One node is the leader at a time**, and only the leader is allowed to accept new writes. This avoids two nodes accepting different, conflicting writes at the same moment.
- Every write the leader accepts is **replicated** — sent — to the follower nodes, and is only counted as **committed** (i.e. safely durable) once a **majority** of nodes have confirmed they have it. Two out of three is enough; the system doesn't wait for a node that's slow or down.
- Once committed, the write is applied to a **state machine** (the actual key-value data) and saved to disk on each node, so it survives a process restart or a crash.
- If the current leader crashes, the remaining nodes hold a **leader election** and agree on a new leader within a few seconds, continuing to accept writes through it — client applications don't need to notice or do anything special.
- When the old leader comes back online, it automatically **recovers** from its own saved data and catches back up to whatever the rest of the cluster agreed on while it was gone.

That's Raft, explained in six everyday-language steps. Everything below describes how ForgeDB actually builds each of those pieces in code — and, honestly, which parts have been tested against a real multi-node cluster versus which are implemented and unit-tested but not yet exercised end-to-end.

## Why is this interesting?

ForgeDB isn't simply "an HTTP API in front of a map in front of a database file." It's a combination of several distributed-systems problems that are each, individually, easy to fake with a shortcut that only works in a demo. ForgeDB's test suite — chaos scenarios that kill processes mid-operation, a linearizability checker that mathematically verifies correctness, and real 3-node Docker failover drills — exists specifically to make those shortcuts hard to get away with.

| Capability | Why it matters |
|---|---|
| 🧠 **Raft consensus** | Prevents two nodes from independently accepting conflicting writes at the same time |
| 🛡️ **Majority commit** | A write isn't considered safely durable until a quorum (a majority) of nodes has it — not just the one node that received it first |
| 🔄 **Request deduplication** | If a client retries a write after a timeout or a leader change, the system recognizes the retry and doesn't apply it twice |
| 📡 **Leader-aware gateway** | Client applications just send requests to one address; they never have to track which node is currently the leader themselves |
| 📸 **Snapshots + chunked transfer** | The replicated log is compacted and a far-behind node can catch up without ever needing one giant, unbounded network payload |
| 🧪 **Chaos + linearizability testing** | Node crashes and network partitions are actually injected during tests and checked against a reference model, not just assumed to work |

## What makes it different?

It's easy to confuse ForgeDB with a typical beginner CRUD (Create/Read/Update/Delete) project, since both ultimately let a client read and write key-value data over HTTP. The difference is everything *underneath* that surface. A simple, single-process CRUD project built as a learning exercise usually doesn't need to think about any of the rows below — ForgeDB was built specifically to confront them:

| A typical single-node CRUD project | ForgeDB |
|---|---|
| Runs as a single process with one source of state | Runs as a multi-node cluster backed by a replicated Raft log |
| Writes go straight to the database | Writes pass through a consensus protocol first, and only "count" once a majority agrees |
| No concept of a leader | Raft leader election, with safety guarantees tied to each election "term" |
| Restarting the process loses anything not yet saved to disk | A write-ahead log plus snapshots mean a restart recovers exactly the state that existed before the crash |
| Failure handling is rarely tested, if ever | Leader failure is tested against a real cluster with `docker stop`, not a mock |
| The client needs to know the backend's address directly | A leader-aware gateway means the client never needs to know which node is in charge |

## Verified capabilities

It's easy for a README to *claim* a feature works. This table is deliberately split into two honesty tiers, so that claim is backed by something specific: ✅ means it was actually exercised against a real, running 3-node Docker cluster during this release's test pass — not just imagined to work. ⚙️ means the code is implemented and covered by its own unit/integration tests, but hasn't (yet) been exercised against a live multi-node cluster in this pass.

| | |
|---|---|
| ✅ Leader election, majority commit | ✅ Leader failure → re-election → gateway reroute |
| ✅ Old-leader rejoin + commit-index catch-up | ✅ Bearer-token auth (401/200 paths) |
| ✅ Prometheus `/metrics` under real traffic | ✅ Graceful shutdown (SIGTERM) |
| ✅ Snapshot creation (manual trigger) | ⚙️ WAL crash recovery |
| ⚙️ Chunked snapshot transfer (corruption/gap/dup/overflow) | ⚙️ Request deduplication across restarts |
| ⚙️ Linearizability checker | ⚙️ Chaos testing (crashes, partitions) |

## What happens when the leader dies?

This is the single most important failure scenario a distributed database has to handle gracefully, so it's worth walking through step by step. Say Node 1 is currently the leader and it suddenly stops — a crash, a container being killed, a power loss, anything:

```text
Node 1 (leader) fails  →  Raft election  →  Node 2 or 3 becomes leader
     →  Gateway follows the new leader, writes continue
     →  Node 1 restarts, rejoins as a follower, catches up
```

The remaining two nodes notice the leader has gone silent, hold an election among themselves, and agree on a new leader — all without any human intervention. The leader-aware gateway (the thing client applications actually talk to) detects the change and starts forwarding writes to the new leader automatically, so from a client's point of view, writes just keep working. When Node 1 eventually restarts, it rejoins the cluster as an ordinary follower and replays whatever it missed until it's fully caught up again.

This exact sequence was tested with a real `docker stop` command against the live leader in an actual running cluster — not simulated in a unit test. Writes made both before and after the failover stayed intact throughout.

## Storage engine

Every database needs somewhere to actually keep its data on disk, separate from the consensus logic that decides *what* gets written. In ForgeDB, the path a write takes to reach disk today is:

**Live write path:** `Raft commit → State Machine → MemStore → WAL (fsync) → MemTable`

In plain terms: once Raft has confirmed a write is committed, the state machine applies it to an in-memory table (`MemStore`), but not before it's also appended to a **write-ahead log (WAL)** and flushed to disk (`fsync`). That WAL is what lets a crashed or restarted node rebuild its exact prior state, rather than silently losing whatever was only sitting in memory. So durability on this path comes from two things together: the WAL on each node, and Raft's own replicated, persisted log across nodes.

Worth being upfront about: the repository *also* contains independently tested **SSTable**, **Manifest**, and **Compaction** packages — the building blocks of a more traditional LSM-tree storage engine, the kind used by databases like LevelDB or RocksDB. They're implemented and have their own tests, but as of this release they are **not wired into the live write path** described above — the code that handles real writes today never calls into them. This is a deliberate scope boundary for 1.0, not a bug or an oversight (see [Future Work](#future-work) for the plan to change that).

## API

Underneath the gateway, every node exposes a plain HTTP API. This is the full surface a client (or the dashboard) actually talks to:

| Method | Path | Auth | Notes |
|---|---|---|---|
| GET | `/health`, `/ready` | none | Liveness / readiness checks, used by Docker and load balancers |
| GET | `/metrics` | Bearer | Prometheus-format metrics text |
| GET | `/cluster` | Bearer | This node's role, Raft term, and log indices |
| GET / PUT / DELETE | `/kv/{key}` | Bearer | The actual key-value operations; returns a 421 with a leader hint if you hit a follower |
| POST | `/admin/snapshot` | Bearer | Manually triggers a snapshot of the current state |

Every protected endpoint expects an `Authorization: Bearer <token>` header, checked before the request is allowed to do anything. Cross-origin browser access is controlled by `FORGEDB_CORS_ORIGINS`, an exact-origin allow-list that is disabled (empty) unless you explicitly configure it.

## Dashboard

Alongside the Go backend, ForgeDB ships a React + TypeScript dashboard — a web UI for actually operating and watching a running cluster: cluster topology, a KV console for reading/writing keys by hand, a Raft internals explorer, and live metrics.

There's one honest limitation worth calling out rather than hiding: every protected backend endpoint needs a bearer token, but a token baked directly into a deployed website's JavaScript would be visible to anyone who opens the browser's dev tools. In local development (`npm run dev`), the dashboard works around this safely by proxying requests through a small server-side layer that attaches the token itself, so it never reaches the browser — giving you genuinely live data while developing. **A deployed build (for example, one hosted on Vercel) has no such server-side proxy, so it currently cannot safely show live authenticated data** — that's a known gap, tracked in [Limitations](#limitations), until a proper backend-for-frontend layer is built.

## Run it yourself

```bash
git clone https://github.com/Kushall-07/forgedb.git
cd forgedb
cp .env.example .env          # set FORGEDB_API_TOKEN — never commit a real token

docker compose build
docker compose up -d
docker compose ps

curl http://127.0.0.1:8090/health                                   # via gateway
curl -X PUT http://127.0.0.1:8090/kv/foo -H "Authorization: Bearer <token>" -d "bar"
curl http://127.0.0.1:8090/kv/foo -H "Authorization: Bearer <token>"
```

Full walkthrough: [`docs/deployment/forgedb-1.0-demo-runbook.md`](docs/deployment/forgedb-1.0-demo-runbook.md) (health, auth, KV lifecycle, leader failure/recovery, snapshots, shutdown).

## Testing

```bash
go test ./...     # 25 packages, 19 with tests, all pass
go vet ./...
go build ./...
```

`-race` requires CGO/gcc, unavailable on this Windows dev environment — not a known ForgeDB data race. Notable suites: `internal/raft` (44+ tests incl. snapshot transfer), `internal/dbnode` (restart/recovery), `chaos/` (fault injection), `correctness/` (linearizability checker).

<details>
<summary><strong>Project structure</strong></summary>

```text
internal/raft                                    Raft consensus
internal/dbnode                                  Node composition (Raft + storage + state machine)
internal/statemachine                            Deduplicating KV state machine
internal/storage                                 WAL-backed MemStore (live path)
internal/storage/sstable, manifest, compaction   LSM components (not on live path)
internal/transport                               gRPC transport
internal/api                                     HTTP API, auth, CORS
internal/gateway                                 Leader-aware reverse proxy
chaos/, correctness/                             Fault injection, linearizability checker
dashboard/                                        React/TypeScript operator dashboard
deploy/, docs/                                   Deployment scripts, design writeups
```

</details>

## Limitations

No project is finished, and pretending otherwise just wastes the next person's time. These are the known, current boundaries of ForgeDB 1.0 — framed as scope, not as failures:

- SSTable/Manifest/Compaction exist as real code but aren't on the live write path yet.
- Cluster membership is static — adding or removing a node requires restarting the cluster with a new configuration.
- Inter-node gRPC traffic is plaintext, and there's one shared bearer token with no per-caller scopes or rotation.
- Deployed (non-dev) dashboard builds can't safely hold a bearer token, so their live data is limited to `/health`.
- The documented deployment topology runs on a single host; Azure/public-endpoint status is not independently verified in this release.
- `-race` testing is blocked on this Windows development environment because the required C/gcc toolchain isn't present — a local toolchain gap, not a known ForgeDB data race.
- There's no automatic snapshot policy yet — `/admin/snapshot` has to be triggered manually by an operator.

## Future Work

The items below are deliberately out of scope for 1.0, but are the natural next steps: dynamic membership · mTLS for inter-node gRPC · a backend-for-frontend/session layer for the deployed dashboard · a stable production ingress · per-caller tokens · wiring SSTable/Manifest/Compaction into the live write path · an automatic snapshot policy.

---

<div align="center">

**[v1.0.0](https://github.com/Kushall-07/forgedb/releases/tag/v1.0.0)** · No `LICENSE` file present — all rights reserved until one is added.

</div>
