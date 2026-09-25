# ForgeDB — Phase 2: WAL + Crash Recovery

## Scope

Phase 2 adds ForgeDB's first real durability layer: a write-ahead log
(WAL) and crash recovery. It does **not** implement SSTables, compaction,
snapshots, a manifest/version set, or any part of Raft. Those remain
later phases.

## Where this sits in the architecture

```text
Raft
  ↓
committed command
  ↓
State Machine
  ↓
Storage Engine
  ↓
Store.Put/Delete
  ↓
WAL append + sync   <-- durability boundary
  ↓
MemTable mutation
```

On restart:

```text
NewMemStore(dataDir)
  ↓
wal.Open
  ↓
wal.Replay
  ↓
reconstruct MemTable
  ↓
Store ready
```

The `wal` package (`internal/storage/wal`) is a self-contained storage
primitive. It knows nothing about `MemTable`, Raft, replication, or
networking; `MemStore` is the only thing that connects it to the
in-memory storage layer.

## The core invariant

> A mutation must be represented in the WAL, durably, before it is
> considered applied to the MemTable.

Concretely, `MemStore.Put` and `MemStore.Delete` (`internal/storage/memstore.go`)
do, in this order:

1. Encode the operation as a WAL record and append it (`wal.WAL.Append`).
2. Fsync the WAL (`wal.WAL.Sync`).
3. Only then mutate the MemTable.

If step 1 or step 2 fails, the method returns the error immediately and
the MemTable is left unmutated — the caller never sees a mutation that
has no durable record behind it. The reverse is not guaranteed: it is
possible, though not expected, for a record to exist in the WAL file
without ever having been reflected in a MemTable the caller observed (see
**Known limitations**).

## WAL record format

Defined in `internal/storage/wal/record.go`. Every record is:

```text
┌───────────────┬──────┬─────────────┬───────────────┬─────┬───────┐
│ CRC-32C (u32) │ type │ keyLen(u32) │ valLen (u32)  │ key │ value │
├───────────────┼──────┴─────────────┴───────────────┼─────┴───────┤
│ 4 bytes       │ 9 bytes (type + 2 lengths)          │ keyLen +    │
│               │                                     │ valLen bytes│
└───────────────┴─────────────────────────────────────┴─────────────┘
```

- **Checksum** (bytes 0–4): a CRC-32C (Castagnoli polynomial, the same
  one LevelDB/RocksDB use for log records) over every byte from offset 4
  onward — the type byte, both length fields, the key, and the value.
  Corruption in any of those fields is caught by recomputing and
  comparing this checksum.
- **Type** (byte 4): `1` = Put, `2` = Delete. Any other value is invalid
  and treated as corruption.
- **Key length / value length** (bytes 5–9, 9–13): `uint32`,
  little-endian. A Delete record always has `valLen == 0`.
- **Key / value** (bytes 13+): raw bytes, exactly `keyLen + valLen` of
  them.

All integers are encoded explicitly with `encoding/binary` in
little-endian order — the format does not depend on Go's in-memory struct
layout, so it is stable across platforms, architectures, and Go versions.

### Length validation

`keyLen` is bounded to 1 MiB and `valLen` to 64 MiB
(`maxKeySize`, `maxValueSize` in `record.go`). Every length read from a
header is checked against these bounds, and against being zero for a
key, **before** any buffer is sized from it. A corrupted length field —
whether from a bit flip or a hand-crafted malicious file — is rejected as
soon as the header is decoded and can never cause an oversized
allocation. This is exercised by
`TestReplayRejectsOversizedLength` in `wal_test.go`, which hand-crafts a
header with `keyLen = 0xFFFFFFFF` without ever going through the encoder.

## Durability: write vs. sync

- `WAL.Append` encodes a record and calls the file's `Write`. This only
  guarantees the bytes were handed to the operating system, which may
  still be holding them in a page-cache buffer. Append alone is **not**
  a durability guarantee.
- `WAL.Sync` calls `File.Sync` (`fsync`). Only after `Sync` returns `nil`
  are the records appended before it guaranteed to survive a process
  crash or OS restart.
- `MemStore.Put`/`Delete` always call `Append` followed by `Sync` before
  touching the MemTable, so from the `Store` interface's perspective,
  every successful `Put`/`Delete` is durable by the time it returns.

## Recovery / replay

`WAL.Replay` (`wal.go`) reads the file from the beginning, record by
record, in the order they were appended, invoking a callback per record.
`MemStore` supplies a callback that applies `Put`/`Delete` to a fresh
`MemTable` — replay always re-applies the full logical history in order
(it does not deduplicate), so the MemTable's own semantics (later Put
wins, Delete is a tombstone, Put-after-Delete resurrects) determine the
final visible state, exactly as they do during normal operation.

Example:

```text
PUT a=1
PUT a=2
DELETE a
PUT b=3
```

After recovery: `Get(a) → ErrKeyNotFound`, `Get(b) → "3"`.

### Torn-tail handling

A crash can leave an incomplete final record on disk — a partial header,
or a header whose declared key/value bytes are not all present. This is
the expected shape of damage from a crash mid-write, since a record is
written as a single `Write` call whose bytes can still be split across
the write/fsync boundary by the OS or disk.

**Policy: a torn tail is silently truncated.** `Replay` detects it (an
`io.EOF`/`io.ErrUnexpectedEOF` while reading a header or payload),
truncates the file to the end of the last complete record, repositions
the file for future appends, and returns `nil` — no error. This is safe
because `Append`+`Sync` never reported success for the torn record: no
caller was ever told that mutation was durable.

### Corruption handling

**Policy: corruption of an otherwise complete record is never silently
skipped.** If a record's header and payload are fully present (every
declared byte exists) but its checksum does not match, or its header
declares an invalid type or an out-of-bounds length, `Replay` stops
immediately and returns an error wrapping `wal.ErrCorrupt`. It does not
skip the bad record and continue reading what follows — doing so could
silently drop data or apply a wrong value, and there is no way to tell,
from a corrupted middle record alone, whether records after it are
trustworthy either.

The distinguishing test is *where* the missing/wrong bytes are relative
to the file's true end: running out of bytes exactly at EOF is a torn
tail; a checksum or framing failure on bytes that are all present is
corruption.

| Situation | Detected as | Behavior |
|---|---|---|
| Header or payload cut off at EOF | Torn tail | Truncate, return `nil` |
| Complete record, checksum mismatch | Corruption | Return error (`ErrCorrupt`) |
| Complete header, invalid type/length | Corruption | Return error (`ErrCorrupt`) |

`NewMemStore` propagates a `Replay` error as its own error (wrapped with
`%w`) and does not return a usable `Store` in that case — a corrupt WAL
is a hard recovery failure in Phase 2, not something the engine attempts
to auto-repair.

## Store / WAL integration

`MemStore` (`internal/storage/memstore.go`) is unchanged from the outside:
it still implements `Store` (`Put`, `Get`, `Delete`, `Close`) exactly as
Phase 1 defined it, and `ErrKeyNotFound`/`ErrEmptyKey` behave identically.
The one public API change is `NewMemStore`, which now takes a `dataDir`
string and returns `(*MemStore, error)` instead of taking no arguments:
opening a WAL is an operation that can fail (I/O error, corrupt file), so
the constructor needs to be able to report that, which a zero-argument,
error-free constructor could not.

`NewMemStore`:

1. Opens (or creates) `<dataDir>/wal.log`, creating `dataDir` if needed.
2. Replays it into a fresh `MemTable`.
3. Returns a `MemStore` wrapping both, ready for use.

`Get` is unchanged: it only reads the MemTable and never touches the WAL.

## File handling

- `wal.Open` creates the WAL's parent directory (`os.MkdirAll`) if
  missing, and creates the file itself if it doesn't exist. It never
  truncates an existing WAL on open — recovery only happens through
  `Replay`, and only `Replay` may shorten the file (and only to discard a
  torn tail).
- The WAL file is **not** opened with `O_APPEND`. On Windows, Go's
  `O_APPEND` only grants `FILE_APPEND_DATA` access, which is not
  sufficient for `Truncate` — and `Replay` must be able to truncate a
  torn tail. Instead, `Replay` explicitly seeks the file to the end of
  the last valid record before returning (whether it truncated or hit a
  clean EOF), and every `Append` after that writes at that
  mutex-serialized file position. There is a single writer per `WAL`
  (enforced by an internal mutex), so this manual positioning is safe.
- No WAL rotation, segmentation, or log cleaning is implemented in Phase
  2. There is exactly one WAL file per data directory.

## Memory ownership

`WAL.Append` encodes `rec.Key`/`rec.Value` into a new buffer
(`encodeRecord`) synchronously, before `Append` returns, so the WAL never
retains a reference to the caller's slice. `MemTable.Put`/`Delete`
(unchanged from Phase 1) copy their key/value arguments before storing
them, and `MemTable.Get` returns a copy. Together, mutating a slice after
passing it to `Store.Put`, or mutating a slice returned from `Store.Get`,
never affects stored state — the existing Phase 1 ownership tests
(`TestStoreValueOwnershipOnPut`, `TestStoreValueOwnershipOnGet`, and their
`memtable`-package equivalents) are unchanged and still pass.

## Implemented

- `internal/storage/wal` — a WAL package independent of MemTable, Raft,
  and networking, providing `Open`, `Append`, `Sync`, `Replay`, `Close`.
- A deterministic, checksummed, length-bounded binary record format.
- CRC-32C integrity checking on every record.
- Torn-tail detection and truncation, distinct from corruption detection.
- Corruption detection (checksum, invalid type, out-of-bounds length)
  that halts recovery with an error rather than skipping bad data.
- `MemStore` upgraded to append-then-sync to the WAL before every
  MemTable mutation, and to replay the WAL to reconstruct the MemTable on
  open.
- Real-filesystem tests: WAL-level reopen/replay tests and `MemStore`-level
  reopen tests, both using real temporary directories and real files
  (not just an in-memory replay function).

## Not implemented (future phases)

- SSTables, bloom filters, sparse indexes
- Manifest / Version Set
- Compaction
- Snapshots
- WAL rotation, segmentation, or log cleaning
- Raft (leader election, PreVote, RequestVote, AppendEntries, replicated
  log, cluster membership)
- Distributed recovery
- Any client-facing or cluster networking (HTTP, gRPC)

## Known limitations

- **This is not power-loss testing.** The tests in `wal_test.go` and
  `storage_wal_test.go` cover reopening after a clean `Close`, truncated
  files, corrupted records, replay ordering, and checksum validation —
  all of it by manipulating real files on a real filesystem. None of this
  simulates actual hardware power loss, disk write reordering, or
  filesystem-level partial-sector writes; it tests the WAL's *logic* for
  handling the file shapes a crash can plausibly leave behind, not the
  underlying storage hardware's behavior during a real outage.
- **A failed `Sync` after a successful `Append` is not fenced.** If
  `Append` succeeds but the subsequent `Sync` fails, `Put`/`Delete`
  returns an error and the MemTable is left unmutated — but the record's
  bytes may already be sitting in the file (and could still reach disk
  later, e.g. via a subsequent OS-level flush) even though the caller was
  told the mutation failed. A production WAL typically responds to an
  `fsync` failure by refusing to continue (since the file's state is no
  longer trustworthy); Phase 2 does not implement that fencing and simply
  surfaces the error.
- **No race-detector run.** As in Phase 0/1, this development machine has
  no C compiler, so `go test -race` cannot run (`cgo` is required). This
  is an environment limitation, not a result — it is reported here rather
  than worked around.
- **Single WAL file, no rotation.** A long-running store accumulates one
  ever-growing WAL file. This is expected and acceptable for Phase 2;
  compaction/SSTables in a later phase are what will bound WAL size.
