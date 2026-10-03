# ForgeDB — Phase 19: Chunked Snapshot Transfer

## Scope

Phase 19 improves the existing Raft `InstallSnapshot` path so a large
snapshot can be transferred incrementally, in bounded chunks, instead of
requiring the entire payload in a single RPC. This is **not** a snapshot
redesign, a new storage engine, or a Raft redesign: the snapshot format,
checksum validation, atomic persistence, restore semantics, snapshot
boundary semantics, state-machine restoration handoff, and every existing
recovery behavior documented in `docs/raft/phase9-snapshots.md` and
`docs/deployment/phase18-snapshot-robustness.md` remain exactly as they
were. Both of those documents explicitly named "no chunking/streaming (no
`offset`/`done` fields, as in the Raft paper)" as the one deliberately
deferred limitation in `InstallSnapshot`; this phase fills in exactly
that, and nothing else.

## 1. Why chunking exists

Before this phase, `(*raft.Node).HandleInstallSnapshot` always received a
snapshot's entire opaque `Data` payload in one `InstallSnapshotArgs`, and
the leader's `sendInstallSnapshot` always sent it in one
`Transport.SendInstallSnapshot` call. Over `InMemoryTransport` (used by
every unit/chaos test) this is free — direct in-process function calls
have no real size limit. Over the real gRPC transport
(`internal/transport`), it meant a single snapshot had to fit inside one
gRPC message, bounded only by `Options.MaxMessageBytes` (64 MiB by
default). A snapshot larger than that limit could never be installed at
all, and even a snapshot comfortably under the limit still had to be
fully buffered and sent as one indivisible unit, with no way to bound the
memory/network burst a single RPC represents or to make partial progress
across retries.

Chunking removes that ceiling: a snapshot of any size (still bounded by
`internal/raft/format.go`'s existing `maxSnapshotDataSize`, 256 MiB,
which this phase does not change) can now be transferred as a sequence of
bounded pieces, each one comfortably inside the gRPC message-size limit
regardless of how large the underlying snapshot grows.

## 2. Current (unchanged) snapshot flow

For the full picture see `docs/raft/phase9-snapshots.md` §16–18. In
outline, the pieces this phase does not touch:

- `raft.Snapshot{LastIncludedIndex, LastIncludedTerm, Data []byte}` is the
  Raft-level snapshot: an opaque state-machine payload plus the log
  position it represents.
- `FilePersister.SaveSnapshot`/`LoadSnapshot` (`file_persister.go`) persist
  it to a single file via `internal/storage/atomicfile.Write`: write to a
  temp file, fsync, atomic rename, best-effort directory fsync. A reader
  can never observe a partially written snapshot file.
- `encodeSnapshot`/`decodeSnapshot` (`format.go`) define that file's
  on-disk representation: magic + version + `LastIncludedIndex` +
  `LastIncludedTerm` + `Data` + a trailing CRC-32C checksum (the
  `crc32.Castagnoli` table, `crcTable`) over everything before it.
- `(*Node).HandleInstallSnapshot` (`snapshot.go`), on the follower:
  term/step-down handling identical to `HandleAppendEntries`; a stale or
  duplicate snapshot (`LastIncludedIndex` at or behind this node's current
  boundary) is acknowledged as a trivial no-op; otherwise the snapshot is
  persisted durably *before* any in-memory log mutation, the log's
  compaction boundary is updated (`Log.installSnapshotBoundary`,
  preserving a matching suffix or discarding a conflicting one), the
  smaller log is itself persisted (rolled back on failure), and only then
  does in-memory state (`commitIndex`, `pendingSnapshot`) advance.
- State-machine restoration happens later and separately, via
  `PendingSnapshot`/`ConfirmSnapshotRestored` and
  `internal/statemachine`'s `Snapshotter.RestoreSnapshot` — Raft never
  calls it itself.

Every one of these remains, after this phase, called exactly once per
snapshot, with exactly the same arguments and exactly the same ordering,
once a transfer (chunked or not) has produced the complete payload.

## 3. Chunked transfer flow (new)

### Wire protocol

`InstallSnapshotArgs` (`internal/raft/message.go`) gains five fields,
all ignored unless `Chunked` is true:

| Field       | Meaning                                                              |
|-------------|-----------------------------------------------------------------------|
| `Chunked`   | false (the zero value) ⇒ `Data` is the complete payload, exactly as before. true ⇒ `Data` is one bounded piece. |
| `Offset`    | This chunk's byte position within the full payload. The first chunk of every transfer (including every retry) has `Offset == 0`. |
| `Final`     | True on the transfer's last chunk.                                    |
| `TotalSize` | The full payload's total length, carried on every chunk.              |
| `Checksum`  | CRC-32C (the same `crcTable` `format.go` already uses) over this chunk's own `Data`. |

Every pre-Phase-19 caller (including every existing unit test) never sets
these fields, so `Chunked` is false and `HandleInstallSnapshot` runs the
identical, unmodified non-chunked code path described in §2. This is what
lets every existing `InstallSnapshot`/snapshot-restore/Raft-snapshot test
keep passing unmodified (see §6).

The same five fields were added to the gRPC wire contract
(`api/proto/raft.proto`'s `InstallSnapshotRequest`, protobuf field numbers
6–10) and regenerated into `api/proto/raftpb` and wired through
`internal/transport/convert.go`, so a real network transfer carries them
exactly as `InMemoryTransport` does.

### Leader side (`sendInstallSnapshot`, `replication.go`)

Instead of sending `snap.Data` in one RPC, the leader now splits it into
chunks of `Options.SnapshotChunkSize` bytes (a new `raft.Options` field;
defaults to `DefaultSnapshotChunkSize`, 1 MiB, when left at zero — tests
that want deterministic multi-chunk transfers over a tiny payload set
this to a small value) and sends them **sequentially**, one RPC at a
time, waiting for each reply before sending the next. This is what gives
the receiver a well-defined, strictly increasing `Offset` to validate
against even though the transport itself (a real network) makes no
ordering guarantee across separate RPCs — ordering here comes from the
sender never issuing chunk *N+1* until chunk *N*'s reply has arrived.

Any failure anywhere in the sequence — a transport error, a higher-term
reply (triggering this node's own step-down, exactly as
`sendAppendEntries` already does), no longer being leader, or an explicit
`Success=false` — aborts the whole attempt immediately; no further chunks
are sent. There is deliberately no partial-progress tracking or resume
logic on the leader side: the next heartbeat round that still finds this
peer behind the snapshot boundary starts an entirely new attempt from
chunk 0, exactly as a single failed non-chunked `InstallSnapshot` always
retried from scratch. Only once the **Final** chunk's reply reports
success does the leader advance that peer's `matchIndex`/`nextIndex`, the
same point at which the old, non-chunked path always did so.

### Follower side (`HandleInstallSnapshot` / `receiveSnapshotChunkLocked`, `snapshot.go`)

A new `Node.chunkTransfer *inProgressSnapshotTransfer` field holds, purely
in memory, the snapshot identity (`LastIncludedIndex`/`LastIncludedTerm`),
declared `totalSize`, and the bytes received so far for whichever
transfer is currently in progress — at most one at a time. It is never
persisted anywhere; see §4.

`HandleInstallSnapshot` branches on `args.Chunked` immediately after the
existing term/step-down/stale-boundary checks (which run unchanged either
way). For a chunked call, `receiveSnapshotChunkLocked` folds the new
chunk in:

1. **Offset 0 always starts a fresh transfer** for this chunk's own
   `LastIncludedIndex`/`LastIncludedTerm`/`TotalSize`, discarding whatever
   transfer (if any) was previously in progress. This is the single rule
   that both lets a newer snapshot cleanly supersede an incomplete older
   one (§4) and is how the leader's own from-scratch retries (§3) are
   accepted normally.
2. A **non-zero offset** is only accepted if it exactly matches an
   already-in-progress transfer's identity, declared total, *and* current
   accumulated length (`len(buf) == args.Offset`). Any mismatch — unknown
   transfer, wrong identity, wrong declared total, a gap, a duplicate, or
   any other out-of-order position — is rejected and the in-progress
   buffer is discarded.
3. The chunk's own **CRC-32C** (`args.Checksum`) must match
   `crc32.Checksum(args.Data, crcTable)`; a mismatch is rejected and the
   buffer discarded.
4. The chunk is appended; if that would exceed the transfer's declared
   `TotalSize`, it is rejected and the buffer discarded.
5. If `args.Final`, the accumulated length must equal `TotalSize` exactly
   (not more, not less) or it is rejected and the buffer discarded.

Only once step 5 succeeds does `HandleInstallSnapshot` fall through to
**exactly** the same `SaveSnapshot` → `installSnapshotBoundary` →
`persistLocked` → `pendingSnapshot`/`commitIndex` sequence the non-chunked
path has always run, now fed the fully reassembled and validated buffer
instead of a single RPC's `Data`. Every one of those calls, and the
checksum/atomicity guarantees they already provide (§2), is unchanged
code, reached only after a complete, validated payload exists — a partial
or corrupt chunk stream can never reach it.

## 4. Partial-transfer safety

A partially received snapshot can never become the active snapshot,
because the in-progress buffer is:

- **Entirely in memory.** `inProgressSnapshotTransfer` is never written
  to disk, to the `Persister`, or anywhere else durable. The only durable
  write this phase ever performs is the single, existing
  `persister.SaveSnapshot` call once a transfer is complete and validated
  — identical in every respect (including the atomic
  write-temp/fsync/rename discipline) to what the non-chunked path always
  did.
- **Discarded, not left dangling, on any failure.** Every rejection path
  in `receiveSnapshotChunkLocked` sets `n.chunkTransfer = nil` before
  returning. A stale/superseded transfer is likewise cleared the moment a
  snapshot at or behind it is observed. There is no code path that leaves
  a transfer "paused" in some ambiguous state.
- **Invisible across a restart.** Since nothing about an in-progress
  transfer is ever persisted, a crash (or ordinary process restart) at any
  point during one loses it completely. `NewNode`'s startup path (which
  calls `persister.LoadSnapshot`) sees only whatever complete snapshot was
  last durably saved — there is no on-disk artifact from an incomplete
  transfer for it to misinterpret as valid. The next transfer attempt
  simply starts fresh at offset 0.
- **Never destroys the existing valid snapshot.** Because the only
  durable write happens after full reassembly and validation, an
  incoming transfer that fails at any point — even after being fully
  installed once before, with a second, newer transfer failing midway —
  leaves whatever snapshot this node had previously, fully installed,
  completely untouched.

## 5. Validation behavior

`receiveSnapshotChunkLocked` treats every one of the following the same
way — reject (`Success=false`) and discard the in-progress buffer, never
guessing at a partial recovery:

1. A chunk at a non-zero offset when no transfer is in progress.
2. A continuation chunk whose identity (`LastIncludedIndex`/
   `LastIncludedTerm`) or declared `TotalSize` disagrees with the
   in-progress transfer.
3. A gap (offset beyond what has been received so far).
4. A duplicate (offset at or behind what has already been consumed).
5. A corrupted chunk (CRC-32C mismatch).
6. A chunk that would push the accumulated payload beyond its own
   declared `TotalSize`.
7. A `Final` chunk that lands short of the declared `TotalSize`
   ("completion without the expected amount of data").

Receiving a newer, non-stale snapshot (a higher `LastIncludedIndex`) while
an older transfer is still incomplete is explicitly **not** an error:
since any chunk at `Offset == 0` always starts fresh, the newer transfer
simply and cleanly replaces whatever partial state the older one had left
behind.

## 6. Tests added

All in `internal/raft/snapshot_chunk_test.go`, alongside the
pre-existing, entirely unmodified `installsnapshot_test.go`,
`snapshot_test.go`, `snapshot_format_test.go`, and `log_snapshot_test.go`
(which continue to prove the non-chunked path, still reachable and
unchanged):

- A full leader-driven, multi-chunk transfer (small `SnapshotChunkSize`,
  a 35-byte payload spanning 8 full 4-byte chunks plus a short final one)
  through the real `Tick`/`Propose`/`Drain` machinery, proving exact
  byte-for-byte reconstruction on the follower and correct
  `matchIndex`/`nextIndex` advancement.
- An empty (zero-byte) snapshot sent as a single final chunk.
- An interrupted transfer (non-final chunks only) installs nothing:
  `SnapshotIndex`, `PendingSnapshot`, and `CommitIndex` are all left
  unchanged.
- A previously-installed, valid snapshot survives a second, failed
  incoming transfer (corrupted final chunk) completely untouched.
- A simulated crash/restart (a real `FilePersister` against a temp
  directory, a brand new `Node` constructed against the same persister)
  proves an incomplete transfer leaves no trace to misinterpret on
  restart, and that a fresh transfer afterward still works normally.
- A corrupted chunk (bad checksum) is rejected.
- A missing/gapped chunk offset is rejected.
- A duplicate (already-consumed) chunk offset is rejected.
- A `Final` chunk landing short of the declared total is rejected.
- A chunk that would overflow the declared total is rejected.
- A non-zero-offset chunk with no transfer in progress is rejected.
- A newer snapshot's transfer cleanly supersedes an incomplete older one.
- A continuation chunk for a different (older) snapshot identity, sent
  mid-transfer, is rejected rather than corrupting the in-progress buffer.

## 7. Known limitations

- `go test -race ./...` still fails with `-race requires cgo; enable cgo
  by setting CGO_ENABLED=1` — the same pre-existing Windows/no-C-compiler
  limitation already documented in Phases 6/7/8/8.5/9/18. Concurrency
  safety here is backed by the same lock-discipline argument as the rest
  of this package (every new field is only ever read or written with
  `n.mu` held) plus the repeated (`-count=20`) deterministic test runs in
  §6, which show no flakiness.
- There is no resumable/partial-progress tracking across separate
  `sendInstallSnapshot` attempts: any failure anywhere in a chunked send
  restarts the *entire* transfer from offset 0 on the next attempt, never
  resuming partway through. This mirrors the non-chunked path's own
  always-retry-from-scratch behavior and was a deliberate simplicity
  choice, not an oversight — Step 6 of this phase's own brief explicitly
  asked for a bounded chunk size without a large configuration surface,
  and resumable transfer would require the leader to track and persist
  per-peer progress, which is well beyond "the smallest
  architecture-compatible change."
- `Options.SnapshotChunkSize` is the only new configuration surface
  introduced, and it is optional (defaults to `DefaultSnapshotChunkSize`,
  1 MiB).
- As before Phase 19, `maxSnapshotDataSize` (256 MiB, `format.go`) remains
  the hard ceiling on a single snapshot's total payload; chunking lets a
  payload up to that ceiling be transferred within gRPC's message-size
  limit, it does not raise the ceiling itself.

## 8. Files changed in this phase

- `internal/raft/message.go` — `InstallSnapshotArgs`'s new, opt-in chunk
  fields.
- `internal/raft/raft.go` — `Options.SnapshotChunkSize`,
  `DefaultSnapshotChunkSize`, `Node.snapshotChunkSize`,
  `Node.chunkTransfer`.
- `internal/raft/snapshot.go` — `inProgressSnapshotTransfer`,
  `receiveSnapshotChunkLocked`, and `HandleInstallSnapshot`'s chunked
  branch.
- `internal/raft/replication.go` — `sendInstallSnapshot`'s chunked send
  loop.
- `api/proto/raft.proto`, `api/proto/raftpb/raft.pb.go`,
  `api/proto/raftpb/raft_grpc.pb.go` — the five new
  `InstallSnapshotRequest` wire fields (regenerated via `protoc` +
  `protoc-gen-go`/`protoc-gen-go-grpc`).
- `internal/transport/convert.go` — the corresponding
  proto↔`raft.InstallSnapshotArgs` field mappings.
- `internal/raft/snapshot_chunk_test.go` — new (see §6).
- `docs/deployment/phase19-chunked-snapshot-transfer.md` — this document.
