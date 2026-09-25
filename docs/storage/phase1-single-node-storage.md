# ForgeDB — Phase 1: Single-Node Storage Engine

## Scope

Phase 1 implements the beginning of ForgeDB's storage engine: an
in-memory, ordered key/value store behind a clean `Store` interface. It
does **not** implement persistence, replication, or any part of Raft.
Those are later phases.

## Where this sits in the architecture

```text
Raft
  ↓
committed command
  ↓
State Machine
  ↓
Storage Engine   <-- Phase 1 lives here
  ↓
WAL / MemTable / SSTables
```

The storage engine does not know who the leader is, what a Raft term or
log index is, what a quorum is, or how a command reached it. It only
implements key/value storage semantics. This keeps the dependency
direction one-way:

```text
storage  ↛  raft
memtable ↛  raft
```

A future State Machine package will translate committed Raft log entries
into calls against `storage.Store`. WAL and SSTables can later be added
underneath `Store` without changing this public contract, because nothing
outside `internal/storage` depends on how `Store` is implemented today.

## Implemented

### `internal/storage` — the `Store` interface

```go
type Store interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Close() error
}
```

`MemStore` is the Phase 1 implementation: a `Store` backed by a single
`MemTable`, holding no on-disk state. `Close` is a no-op today; it exists
so a future disk-backed `Store` can be substituted without changing
callers.

### `internal/storage/memtable` — the MemTable

The MemTable is the ordered, in-memory structure the architecture calls
for as the first stage of the eventual LSM tree
(MemTable → immutable MemTable → SSTables). It is implemented as a
**skip list** (`internal/storage/memtable/skiplist.go`), not a plain map,
because key ordering is part of the MemTable's contract going forward
(range scans and SSTable flushing will depend on it in later phases).

The skip list is a classic Pugh-style structure: up to 16 forward-pointer
levels per node, with each node's level chosen randomly (p = 0.25) at
insertion time. Search, insert, and update are all `O(log n)` on average.

`MemTable` wraps the skip list with a `sync.RWMutex` to make it safe for
concurrent use. The skip list itself is not safe for concurrent use in
isolation — synchronization is the MemTable's responsibility, kept as a
single coarse lock rather than a more elaborate concurrency scheme.

## Put / Get / Delete semantics

- **Put** inserts a key, or replaces its value if the key already exists.
  A later `Put` always wins:

  ```text
  Put("a", "value1")
  Put("a", "value2")
  Get("a") → "value2"
  ```

- **Get** on a key that was never written, or whose most recent write was
  a `Delete`, returns `ErrKeyNotFound`. It never silently returns an
  empty value for a missing key.

- **Delete** writes a **tombstone** rather than removing the key's node
  from the skip list. This matters because the eventual architecture is
  an LSM tree: once SSTables exist, a delete must be able to shadow an
  older value that already lives in an immutable MemTable or on disk, not
  just remove a MemTable entry. Recording deletes as tombstones now means
  the semantic doesn't need to change when SSTables are introduced.

  ```text
  Put("a", "value")
  Delete("a")
  Get("a") → ErrKeyNotFound
  ```

  Deleting a key that does not exist (or is already deleted) is not an
  error — it simply (re)writes the tombstone, which is the standard,
  idempotent way deletes behave in an LSM-style store.

- Writing to a key after it has been deleted makes it live again with the
  new value (the tombstone is overwritten by the new `Put`).

## Key and value validity

- **Nil or empty keys are invalid.** `Put`, `Get`, and `Delete` all
  reject them with `ErrEmptyKey`. There is no valid interpretation of an
  empty key in this design, so it is rejected explicitly rather than
  silently accepted.
- **Empty values are valid** and distinct from a missing key: `Put(k,
  [])` followed by `Get(k)` returns a zero-length value, not
  `ErrKeyNotFound`.
- **Nil values are treated as empty values**, not as an error.

## Memory ownership

`Put` copies both the key and the value it is given, and `Get` returns a
copy of the stored value. This means:

```go
value := []byte("hello")
store.Put(key, value)
value[0] = 'X' // does not affect the stored value

got, _ := store.Get(key)
got[0] = 'X' // does not affect the stored value either
```

Both directions are covered by tests (`TestValueOwnershipOnPut`,
`TestValueOwnershipOnGet`, `TestKeyOwnershipOnPut`, and their
`memtable`-package equivalents).

## Concurrency

`MemTable` serializes all access with a single `sync.RWMutex`. This is a
deliberately simple model for Phase 1 — no lock-free structures, no
per-node locking, no elaborate concurrency framework. `MemStore` adds no
further synchronization beyond delegating to `MemTable`.

**Known environment limitation:** this development machine has no C
compiler available, so `go test -race` cannot run (`cgo` is required for
the race detector). This is the same limitation noted in the Phase 0
foundation document. The implementation was written with a single coarse
lock specifically to keep the concurrency model easy to reason about by
inspection while this limitation persists.

## Errors

| Error | Meaning |
|---|---|
| `storage.ErrKeyNotFound` | `Get` was called for a key with no live value (never written, or deleted). |
| `storage.ErrEmptyKey` | `Put`, `Get`, or `Delete` was called with a nil or zero-length key. |

The `memtable` package defines its own equivalent sentinel errors
(`memtable.ErrKeyNotFound`, `memtable.ErrEmptyKey`) since it is usable as
a standalone ordered key/value structure independent of the `Store`
interface. `MemStore` validates key emptiness itself at the `Store`
boundary and returns `storage.ErrEmptyKey` directly, rather than
depending on `memtable`'s error identity.

## Tests

- `internal/storage/memtable/memtable_test.go` — white-box tests
  (same package) covering Put/Get, update-overwrite, delete then get,
  deleting a non-existent key, tombstone persistence in the skip list,
  resurrecting a deleted key, multi-key independence, empty/nil key
  rejection, empty-value storage, key/value ownership, and skip-list
  ordering (verified by walking the level-0 linked list — no public
  iteration API was added since the architecture doesn't currently call
  for one).
- `internal/storage/storage_test.go` — black-box tests against the
  `Store` interface (not the `MemStore` type directly) covering the same
  behavioral contract: Put/Get, update, delete, missing-key and
  empty-key errors, multi-key independence, and value ownership.

## Not implemented yet

The following are explicitly out of scope for Phase 1 and belong to
later phases:

- WAL (write-ahead log) and crash recovery
- SSTables, bloom filters, sparse indexes
- Manifest / Version Set
- Compaction
- Snapshots
- Raft (leader election, PreVote, RequestVote, AppendEntries, replicated
  log, cluster membership)
- A State Machine that applies committed Raft entries to this storage
  engine
- Any client-facing or cluster networking (HTTP, gRPC)

**Phase 1 storage is in-memory only and is not durable across process
restarts.** This is intentional and is not a bug: durability is Phase
2's responsibility (WAL and recovery), layered underneath the `Store`
interface defined here.
