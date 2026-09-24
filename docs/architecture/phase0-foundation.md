# ForgeDB — Phase 0: Foundation

## What ForgeDB is

ForgeDB is a distributed key-value database engine being built from scratch
in Go. Its codename is **ForgeKV**. The core design principle is:

> Raft decides the replicated order of operations. The state machine applies
> committed operations. The storage engine durably stores the resulting
> state.

Followers never mutate their local database independently; every replicated
write flows through the Raft log and is only applied once committed.

## High-level architecture (planned)

```text
Client
   ↓
HTTP/JSON API
   ↓
Raft
   ↓
Replicated Raft Log
   ↓
Majority Commit
   ↓
State Machine
   ↓
Storage Engine
   ↓
WAL → MemTable → Immutable MemTable → SSTables → Disk
```

### Raft vs. State Machine vs. Storage

These are three distinct responsibilities and must not be mixed:

- **Raft** owns replicated ordering: leader election, log replication, and
  majority commit. It decides *what order* operations happen in across the
  cluster.
- **State Machine** applies committed Raft log entries, and only committed
  entries, to produce the next application state.
- **Storage Engine** owns persistence: the WAL, MemTable, SSTables, and
  eventually compaction and recovery. It decides *how* state is durably
  written to disk.

Raft durability (an entry being committed in the log) and storage durability
(the storage engine having safely persisted it) are separate concerns and
are not assumed to be equivalent.

None of Raft, the state machine, or the storage engine are implemented yet.
This document describes only the Phase 0 foundation.

## Node identity

A ForgeDB node has a stable identity, represented independently from future
Raft persistent state (`currentTerm`, `votedFor`, `log`), which will be
introduced in a later phase:

```go
type Node struct {
    ID   string
    HTTP string
    GRPC string
}
```

## Configuration

Node-level configuration is a plain struct, deliberately free of external
configuration frameworks:

```go
type Config struct {
    NodeID  string
    HTTP    string
    GRPC    string
    DataDir string
}
```

## Current executable entry point

`cmd/forgedb/main.go` is the application entry point. It is intentionally
minimal: it initializes logging and prints a startup message. No business
logic accumulates here.

```text
go run ./cmd/forgedb
2026/... [INFO] ForgeDB starting...
```

## Logging

`internal/metrics/logger.go` provides `Info` and `Error` helpers wrapping
the standard library `log` package. This is intentionally simple for
Phase 0 and will expand as Raft, storage, recovery, and networking
subsystems are added.

## Current development status

**Implemented:**

- Go module (`github.com/Kushall-07/forgedb`)
- Repository directory structure
- `cmd/forgedb` executable entry point
- `internal/config.Config` — node configuration structure
- `internal/config.Node` — node identity structure
- `internal/metrics` — basic `Info`/`Error` logging
- Unit tests for configuration, node identity, and logging
- `.gitignore`

**Planned (not implemented):**

- Raft consensus (leader election, PreVote, RequestVote, AppendEntries,
  persistent state, log replication, membership)
- Storage engine (WAL, MemTable, immutable MemTable, SSTables, bloom
  filters, sparse index, manifest/version set, compaction, recovery)
- State machine that applies committed Raft entries to the storage engine
- HTTP/JSON client API and gRPC cluster transport
- Deployment (Docker, docker-compose), monitoring (Prometheus, Grafana)
- Chaos and linearizability testing

Phase 0 is complete once the foundation above builds, tests, and vets
cleanly, and is pushed to `main`. The next phase, **Phase 1 — Single-Node
Storage Engine**, is out of scope for this document.
