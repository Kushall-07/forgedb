# ForgeDB — Phase 9: Snapshots

## Scope

Phase 9 introduces Raft snapshots, safe log truncation, snapshot
installation/recovery, and restart correctness, so a ForgeDB Raft log no
longer grows without bound. It adds:

- A `Snapshot` type and two new `Persister` methods (`SaveSnapshot` /
  `LoadSnapshot`) in `internal/raft`, implemented by both `MemoryPersister`
  and `FilePersister`.
- An explicit, versioned, checksummed on-disk snapshot format
  (`internal/raft/format.go`), following the exact conventions
  `encodeState`/`decodeState` already established in Phase 6.
- A generalized `Log` (`internal/raft/log.go`) whose sentinel now
  represents either "the position before index 1" (unchanged from Phase 5)
  or "the position covered by the most recent snapshot" — the same
  mechanism, not a second one.
- `(*raft.Node).CreateSnapshot`, `PendingSnapshot`, `ConfirmSnapshotRestored`,
  and `HandleInstallSnapshot` (`internal/raft/snapshot.go`), plus the
  `InstallSnapshot` RPC itself (`internal/raft/message.go`, `transport.go`).
- `statemachine.Snapshotter`, implemented by `KVStateMachine`
  (`internal/statemachine/snapshot.go`), and `(*Applier).CreateSnapshot` /
  the pending-snapshot restore path inside `ApplyAvailable`
  (`internal/statemachine/applier.go`).
- `storage.Store.Snapshot`, implemented by `MemStore` via a new
  `MemTable.All`/`skipList.forEach` (`internal/storage`).
- `(*dbnode.Node).CreateSnapshot` / `SnapshotIndex`
  (`internal/dbnode/node.go`), the thin composition-root passthrough.

Phase 9 does **not** add: automatic/background snapshot scheduling, a real
network transport, cluster membership changes, a chaos framework, a
linearizability checker, observability, or any change to the KV WAL's own
format. See §18/§19.

## 1. Why snapshots are necessary

Through Phase 8.5, a Raft node's log only ever grows: every committed
command stays in `Log.entries` forever, and `FilePersister.SaveState`
rewrites the *entire* log to disk on every single mutation. For a
long-running cluster this means:

- Persistent Raft state grows without bound, and every `SaveState` call
  gets more expensive as the log grows (Phase 6's format is a full
  snapshot of current state, not an append-only edit log — see
  `docs/raft/phase6-raft-persistence.md`).
- Restart recovery cost grows without bound, since `NewNode` replays the
  entire persisted log into memory.
- A new or badly lagging follower needs the *entire* historical log to
  catch up, even though the leader's own state machine may have long since
  reduced all of that history down to a much smaller current KV state.

A snapshot captures the state machine's state at a specific, already-
applied log index, letting Raft safely discard (compact) every log entry
at or before that index — the committed history that index represents is
still fully recoverable, just no longer as a list of individual commands.

```text
Before:                              After snapshot at index 10:

Raft log:                            Snapshot: index=10, term=T
1 2 3 4 5 6 7 8 9 10 11 12 ...            |
                                           v
                                      Raft log:
                                      11 12 ...
```

## 2. Snapshot definition and boundary semantics

A `raft.Snapshot` is:

```go
type Snapshot struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    Data              []byte // opaque state-machine payload
}
```

`internal/raft` never interprets `Data` — see §9's architectural boundary.
`LastIncludedIndex`/`LastIncludedTerm` are exactly what the Raft paper
calls them: the position and term of the last log entry the snapshot's
state reflects. This pair is what lets AppendEntries's consistency check
and RequestVote's up-to-date check keep working correctly for a position
that no longer has a physically retained log entry (§6, §10).

**`CreateSnapshot(index, data)` requires `index` to exactly equal this
node's current `LastApplied()`** — not merely `<= LastApplied()`. This is
a deliberate, explicit resolution of what the phase brief leaves open
("define safe behavior explicitly"): `data` is always the state machine's
*entire current* accumulated state, not a reconstructible point-in-time
view at an arbitrary earlier index (ForgeDB's storage engine has no MVCC
or rollback capability — see `docs/storage/`). If `index` were allowed to
be strictly less than `LastApplied()`, the snapshot's declared boundary
would understate what `data` actually contains. Phase 9 closes this gap by
requiring an exact match rather than defining (and then having to prove)
a safe-but-subtle "the snapshot may contain more than it claims" invariant.
`(*statemachine.Applier).CreateSnapshot` enforces this exact-match check
while holding the same lock `ApplyAvailable` does, closing the TOCTOU
window a naive two-step "check then snapshot" would otherwise have (§8).

Other validation, enforced by `(*raft.Node).CreateSnapshot` itself:

| Condition | Result |
|---|---|
| `index == 0` | rejected (`ErrInvalidSnapshotIndex`) — nothing exists before the log begins |
| `index > LastApplied()` | rejected — would snapshot unapplied state |
| `index > CommitIndex()` | rejected — redundant given `LastApplied() <= CommitIndex()`, checked anyway (defense in depth, matching this codebase's existing layered-validation style) |
| `index <= current SnapshotIndex()` | rejected — a stale/duplicate request; the existing snapshot is left untouched rather than silently treated as a no-op |
| otherwise | proceeds |

## 3. State-machine snapshot contents

`KVStateMachine.CreateSnapshot()` (`internal/statemachine/snapshot.go`)
captures, atomically (holding `sm.mu` for the whole call):

- every live key/value pair, via the new `storage.Store.Snapshot()` method
  (sorted by key, tombstones omitted — a deleted key's absence is already
  fully represented by the key simply not appearing);
- the **entire deduplication table** (`(ClientID) -> (RequestID, Op, Key,
  Value, Result)`), sorted by `ClientID` before encoding for deterministic
  output (Go map iteration order is randomized).

Including the dedup table is not optional. Phase 7's `KVStateMachine`
depends on it for `ErrRequestIDConflict`/`ErrStaleRequest`/replay
semantics; once log compaction has discarded the entries that originally
built that table, replaying the log from scratch to reconstruct it is no
longer possible (the whole point of compaction is that those entries are
gone). A snapshot that captured only the raw KV map would silently forget
every client's dedup state on the next restore, reopening exactly the
"duplicate request re-executed" bug Phase 7 exists to prevent.

`RestoreSnapshot(data)` **reconciles**, rather than merely writes to,
storage: every key `data` describes is `Put` with its snapshot value, and
every key *currently* in storage that `data` does **not** mention is
explicitly `Delete`d. This matters specifically for a follower receiving
`InstallSnapshot` after having already applied some of its own history: if
it once applied `PUT k=v` and the leader's snapshot reflects a *later*
`DELETE k`, a naive "only write what the snapshot has" restore would leave
`k` behind forever. `TestKVStateMachine_Restore_DeletesKeysNotInSnapshot`
proves this directly.

## 4. Snapshot persistence model and format

### Raft-level persistence

`Persister` gained two methods, implemented symmetrically to the existing
`SaveState`/`LoadState`:

```go
SaveSnapshot(snap Snapshot) error
LoadSnapshot() (Snapshot, error) // ErrNoSnapshot if never saved
```

`FilePersister` derives its snapshot file path from its existing
single-argument constructor (`path + ".snapshot"`), so **`NewFilePersister`'s
signature never changed** — every pre-Phase-9 call site
(`internal/dbnode/node.go`, every Phase 6 test) continues to work
unmodified. The snapshot file uses the same write-temp/fsync/atomic-
rename/fsync-directory protocol (`internal/storage/atomicfile`) as the
state file, in a file of its own — a crash mid-write to one can never
corrupt the other.

`MemoryPersister` gained matching in-memory fields plus
`FailNextSnapshotSave()`, mirroring `FailNextSave()` exactly, for
deterministic fault-injection tests.

### Wire format

`internal/raft/format.go` adds `encodeSnapshot`/`decodeSnapshot`, following
precisely the conventions `encodeState`/`decodeState` already established:

```text
magic (8 bytes "ForgeSS1")
formatVersion (u32 LE)
lastIncludedIndex (u64 LE)
lastIncludedTerm  (u64 LE)
dataLen (u32 LE) | data bytes
checksum (u32 LE, CRC-32C over every preceding byte)
```

`dataLen` is bounds-checked (`maxSnapshotDataSize` = 256 MiB,
`maxSnapshotFileSize` = 512 MiB) **before** it is ever used to size an
allocation or slice a buffer — the same defense against a malicious or
corrupted length field every other format in this codebase uses. A bad
magic, unsupported version, truncated header/payload, or checksum mismatch
all return a wrapped `ErrCorrupt`, exactly like `decodeState`.

The **state-machine-level** snapshot format
(`internal/statemachine/snapshot.go`) is a second, independent format —
`ForgeSM1` — for the opaque bytes `raft.Snapshot.Data` carries: magic,
version, the sorted KV entries, the sorted dedup entries (encoding each
`Result.Err` as one of a small closed set of codes — `nil`,
`ErrRequestIDConflict`, `ErrStaleRequest`, `storage.ErrEmptyKey`, the only
values `KVStateMachine.Apply` can ever produce), and a trailing checksum.
Every length is bounded and validated the same way.

### Why two formats, not one

`internal/raft` must be able to validate a `Snapshot`'s own header
(index/term/length) without understanding what `Data` means, and
`internal/statemachine` must be able to validate `Data`'s own structure
without `internal/raft` ever being involved. Keeping these as two
independently-versioned formats is what lets the state-machine layer
evolve its own snapshot content in a later phase without touching Raft's
wire format at all, and vice versa.

## 5. Log compaction: extending the sentinel, not adding a second mechanism

Before Phase 9, `Log.entries[0]` was *always* a fixed sentinel `{Index: 0,
Term: 0}`, standing for "the position immediately before the log begins."
Phase 9 generalizes this directly rather than bolting on separate
boundary-tracking state: once a snapshot exists, `entries[0]` instead
holds `{Index: lastIncludedIndex, Term: lastIncludedTerm}` — the position
immediately before the log begins is now wherever the snapshot says it is.
Every method that used to special-case index/offset 0 (`TermAt`,
`EntryAt`, `Slice`, `Range`, `Append`, `AppendAfter`) now compares against
`entries[0].Index` instead, and behaves **identically to Phase 5** when
that value happens to be 0 — this is why every single pre-Phase-9 test in
`internal/raft` (persistence, elections, replication, reads) passes
unmodified.

Physical slice offset and logical Raft index are no longer the same
number once compaction has happened: logical index `i` lives at physical
offset `i - entries[0].Index`. This arithmetic is entirely internal to
`log.go`; every caller (`Node`) only ever reasons in real, logical Raft
indexes, per the phase brief's explicit requirement.

Two new unexported `Log` methods:

- **`compact(index)`** — used by `CreateSnapshot`: this node's *own* log
  always already agrees with itself at `index` (there is no leader to
  disagree with), so this simply looks up `index`'s term via `TermAt` and
  rewrites the sentinel, keeping everything after it untouched.
- **`installSnapshotBoundary(lastIncludedIndex, lastIncludedTerm)`** — used
  by `HandleInstallSnapshot`: a *leader's* snapshot boundary may conflict
  with what this log currently holds. If this log already agrees (same
  term at `lastIncludedIndex`, which also covers that index already being
  exactly this log's own current sentinel), everything after it is a
  **matching suffix** and is preserved untouched. Otherwise (a real
  conflict, or this log doesn't reach that far), **every** existing entry
  is discarded and the log starts empty immediately after the new
  boundary.

`compact` never has a conflict to resolve (defense-in-depth: it still
returns an explicit error, changing nothing, if asked to compact at an
index not actually present); `installSnapshotBoundary` always does, by
construction, since it is reacting to another node's authoritative claim.

## 6. AppendEntries / RequestVote correctness after compaction

Because the compaction boundary is represented as an ordinary (if
synthetic) log position, `TermAt(entries[0].Index)` always returns
`(entries[0].Term, true)` — the boundary behaves exactly like a real
retained entry for every consistency check that matters:

- **`prevLogIndex == lastIncludedIndex`** (AppendEntries right at the
  boundary): `TermAt` succeeds without needing a physical entry there.
  `TestAppendEntries_WorksExactlyAtSnapshotBoundary` proves this.
- **`prevLogIndex < lastIncludedIndex`**: the leader should never normally
  construct such a request once its own replication state correctly
  detects a peer needs a snapshot instead (§7) — but if it somehow did,
  `TermAt` simply returns `(0, false)` (compacted away), causing the
  ordinary consistency-check rejection path to fire exactly as it would
  for any other mismatch. No indexing into nonexistent data, no panic.
- **Conflict resolution after compaction**: a follower whose retained
  suffix starts right after the boundary still runs the exact same
  `Log.AppendAfter` conflict-resolution logic as before — it is
  structurally incapable of truncating *below* the boundary, since every
  index `AppendAfter` ever touches is derived from `prevIndex + 1 + i` with
  `prevIndex >= entries[0].Index`. `TestAppendEntries_ConflictResolutionAfterCompaction`
  exercises this directly.
- **RequestVote's up-to-date check** (`logIsUpToDate`) only ever consumes
  `LastIndex()`/`LastTerm()`, which already correctly fall back to the
  sentinel/boundary when no entries are retained past it (§2's "logical
  last position" requirement). No change to `logIsUpToDate` itself was
  needed. `TestRequestVote_WorksAfterCompaction` /
  `TestRequestVote_RejectsStaleCandidateAfterCompaction` confirm both
  directions.
- **The current-term commit rule** (`maybeAdvanceCommitIndexLocked`,
  §5.4.2) is completely unmodified: it only ever considers indexes above
  `commitIndex`, and the boundary itself is never a candidate for that
  loop to "commit" through this mechanism — it already represents
  committed state by construction (it came from an already-applied,
  already-durable snapshot).

## 7. InstallSnapshot RPC and leader replication

`InstallSnapshotArgs`/`InstallSnapshotReply` (`message.go`) and a third
`Transport`/`RPCHandler` method (`SendInstallSnapshot`/
`HandleInstallSnapshot`) extend the existing RPC machinery — no second
networking system, no change to `InMemoryTransport`'s fundamental
same-goroutine, synchronous-or-`ErrPeerUnreachable` delivery model. Phase 9
sends the entire snapshot payload as a single RPC (no chunking/streaming);
see §16's known limitations.

**Leader side** (`broadcastAppendEntriesLocked`, `replication.go`): for
each peer, if `nextIndex[peer] <= SnapshotIndex()` (the leader's own
compaction boundary), it sends `InstallSnapshot` instead of constructing an
`AppendEntries` — critically, it does this check *before* ever calling
`Log.Slice`/`TermAt` for that peer, because those calls would otherwise
silently clamp `from` up to the boundary and produce an
internally-inconsistent `AppendEntriesArgs` (a `PrevLogIndex` far below the
boundary, paired with `Entries` that actually start at the boundary — a
real correctness bug this implementation deliberately avoids by branching
explicitly rather than relying on `Slice`'s clamping to "accidentally" do
the right thing).

`sendInstallSnapshot` processes the reply exactly like
`sendAppendEntries`: a higher term steps this node down; otherwise, a
successful reply advances `matchIndex[peer]` to at least
`LastIncludedIndex` and `nextIndex[peer]` to exactly `LastIncludedIndex +
1`, then re-checks whether a new commit index can be established — after
which **ordinary `AppendEntries` replication resumes automatically** on
the very next heartbeat, since `nextIndex[peer]` is now above the
boundary. `TestHandleInstallSnapshot_LeaderCatchesUpFarBehindFollower` and
`internal/dbnode`'s `TestNode_LaggingFollower_InstallsSnapshotAndCatchesUp`
both exercise the full cycle: far-behind follower → InstallSnapshot →
catch-up → new entries committed normally afterward.

A failed/rejected `InstallSnapshot` reply needs no back-off (unlike a
failed `AppendEntries`): `nextIndex` is simply left unchanged, and the next
heartbeat round retries — most commonly because the follower's own
persistence failed transiently (`HandleInstallSnapshot` below).

## 8. Follower-side InstallSnapshot handling

`(*Node).HandleInstallSnapshot` (`internal/raft/snapshot.go`) follows the
exact ordering the phase brief requires:

1. Term check / step-down, identical in spirit to `HandleAppendEntries`.
2. **Stale/duplicate check**: `LastIncludedIndex <= SnapshotIndex()` is a
   trivial success with no mutation — this node is already at or past this
   boundary.
3. **`SaveSnapshot` first.** If it fails, the RPC is rejected and *nothing*
   else happens — no log mutation, no pending-snapshot flag. The in-memory
   state can never claim a snapshot exists that was not actually persisted.
4. Only once durable: `Log.installSnapshotBoundary` (preserving a matching
   suffix or discarding a conflicting one, §5), then `persistLocked()` to
   durably save the resulting (possibly much smaller) log. If *this* fails,
   the in-memory log is rolled back to its pre-mutation snapshot — exactly
   the same "snapshot old value, mutate, persist, roll back on failure"
   discipline every other mutating path in this package
   (`becomeFollowerLocked`, `Propose`, `HandleAppendEntries`) already uses.
5. Only once both are durable: `n.snapshotData` is updated, a
   `pendingSnapshot` is recorded for the state-machine layer (§9), and
   `CommitIndex` is raised to at least `LastIncludedIndex` (the snapshot
   itself proves that much was committed, even if this follower's own
   `CommitIndex` had not caught up yet).

`LastApplied` is **deliberately not touched** by `HandleInstallSnapshot`
itself — only the state machine, by calling `ConfirmSnapshotRestored` once
it has actually restored `Data`, is entitled to advance it. This preserves
the invariant that `LastApplied` is never raised to a position whose
effects have not genuinely been applied *somewhere* real.

`TestHandleInstallSnapshot_PersistenceFailure_RejectsWithoutCorruptingState`
and `TestCreateSnapshot_LogSaveFailure_RollsBackCompactionButKeepsSnapshot`
cover the crash-window-adjacent failure paths; §13 covers full process
crashes.

## 9. The Raft/state-machine snapshot boundary (`PendingSnapshot`)

The hardest correctness problem in this phase is **not** persistence — it
is making sure a state machine actually gets a chance to restore itself
before anything built on `LastApplied` assumes it is already caught up.

Once a snapshot has discarded a log prefix, `Applier.ApplyAvailable`'s
usual `CommittedEntries(LastApplied()+1)` would, if nothing else changed,
silently have `Log.Range` clamp `from` up to just past the new boundary —
meaning entries 1..N would simply never be decoded or applied at all,
while `MarkApplied` has no way to notice anything was skipped (it only
requires monotonic progress, not contiguous +1 steps). The state
machine's dedup table and (for a lagging follower) its KV data would
silently never be recovered.

Phase 9 closes this gap with an explicit handoff, entirely at the
`raft.Node` level (never requiring Raft to know what a "snapshot restore"
even means):

```go
func (n *Node) PendingSnapshot() (Snapshot, bool)
func (n *Node) ConfirmSnapshotRestored(lastIncludedIndex uint64) error
```

`pendingSnapshot` is set exactly twice: when `HandleInstallSnapshot`
succeeds, and when `NewNode` finds a persisted snapshot at startup (§13).
It is **never** set by `CreateSnapshot` itself — that path's own state
machine is the snapshot's source, already in exactly that state, with
nothing to restore.

`(*statemachine.Applier).ApplyAvailable` checks `PendingSnapshot` **first**,
before computing `from := LastApplied()+1`:

```text
ApplyAvailable
  |
  +-- PendingSnapshot()? ---- no --> apply committed entries as before
  |         |
  |        yes
  |         v
  |   sm.(Snapshotter).RestoreSnapshot(snap.Data)
  |         v
  |   node.ConfirmSnapshotRestored(snap.LastIncludedIndex)
  |         |  (advances LastApplied to the boundary, clears pending)
  |         v
  +-- apply any further committed entries normally
```

If `sm` does not implement `Snapshotter` (e.g. a minimal test fake), this
fails with a clear error rather than silently skipping the restore and
later applying real entries against a state machine missing everything
the snapshot represented —
`TestApplier_ApplyAvailable_PendingSnapshotWithoutSnapshotterSupport_FailsClearly`
proves the pending flag survives such a failure untouched, so a later
retry (once given a real `Snapshotter`) can still succeed.

`ConfirmSnapshotRestored` re-validates independently (matching
`LastIncludedIndex`, not exceeding `CommitIndex`) rather than trusting the
caller, mirroring `MarkApplied`'s own defensive style.

## 10. Raft/StateMachine architectural boundary preserved

`internal/raft` imports nothing from `internal/statemachine` or
`internal/storage`. `Snapshot.Data` is `[]byte` from Raft's point of view,
start to finish — `CreateSnapshot`, `PendingSnapshot`, and
`HandleInstallSnapshot` only ever move it around, persist it, and hand it
back; they never decode it. The `Snapshotter` interface
(`CreateSnapshot()([]byte,error)` / `RestoreSnapshot([]byte)error`) lives
in `internal/statemachine`, which already depends on `internal/storage`
(unchanged from Phase 7) — `internal/raft`'s dependency graph is exactly
as narrow as it was in Phase 8.5.

```text
internal/raft           <-- opaque []byte, index, term only
      ^
      | Snapshotter interface (defined here, not in raft)
      |
internal/statemachine   <-- owns KV + dedup serialization
      |
internal/storage        <-- owns Store.Snapshot() (live KV enumeration)
```

## 11. The storage-layer addition: `Store.Snapshot()`

`internal/storage`'s `Store` interface gained one method:

```go
Snapshot() ([]Entry, error) // every live key/value pair, sorted by key
```

This was unavoidable: `KVStateMachine` had no way to enumerate the full
key space it is responsible for snapshotting (`MemTable` previously
exposed only point `Get`/`Put`/`Delete`). It is implemented via a new
`skipList.forEach` (a level-0 forward-pointer walk, which already visits
every node in sorted order by construction) and `MemTable.All()` (filters
out tombstones). It does **not** touch the WAL, does not imply or require
SSTable/compaction support, and does not change `Store`'s read/write
semantics in any way.

Because the only existing `Store` implementation is `MemStore`, and every
test-only `Store` wrapper in this codebase (`countingStore`, `faultyStore`
in `internal/dbnode`/`internal/statemachine`) embeds `storage.Store` as an
interface field rather than implementing it from scratch, this addition
required **zero changes** to any pre-existing test file — the new method
is simply promoted through the embedding automatically.

## 12. Persistence model: what is and isn't durable

| State | Durable? | Mechanism |
|---|---|---|
| `currentTerm`, `votedFor`, log suffix after the snapshot boundary | yes | `Persister.SaveState` (unchanged format) |
| Snapshot (`LastIncludedIndex`/`Term`/`Data`) | yes | `Persister.SaveSnapshot` (new) |
| KV data | yes | the KV WAL (`internal/storage`), entirely independent, unchanged |
| `commitIndex`, `lastApplied` | **no** (volatile) | always reset to 0 on `NewNode`, exactly as Phase 6/7 decided |
| `pendingSnapshot` flag | no (derived) | recomputed every `NewNode` call from whether `LoadSnapshot` finds anything |
| role, timers, `nextIndex`/`matchIndex` | no (volatile) | unchanged from Phase 5 |

Phase 9 deliberately does **not** change the Phase 6/7 decision that
`commitIndex`/`lastApplied` stay volatile. This remains safe post-
compaction for a subtle but important reason: `ConfirmSnapshotRestored`
can only advance `LastApplied` to the snapshot boundary once `CommitIndex`
has *itself* reached at least that far — and `CommitIndex` only does that
through the ordinary, pre-existing mechanisms (a follower's
`HandleAppendEntries` accepting a fresh `LeaderCommit`, or a newly
re-elected leader's current-term commit rule once a new entry replicates).
There is no unsafe window in between: `ApplyAvailable` always checks
`PendingSnapshot` before touching any committed entry, so even if a
restarted node briefly has `CommitIndex == 0` while the snapshot boundary
is `N > 0`, it simply cannot apply anything at all until `CommitIndex`
genuinely reaches `N` again through normal Raft mechanics — at which point
the snapshot restore (and therefore the dedup table) is guaranteed to have
already happened before any post-boundary entry is ever applied.

## 13. Restart recovery

`NewNode` (`internal/raft/raft.go`) now loads **both** `LoadState` and
`LoadSnapshot` before constructing its `Log`:

```text
hasState?  hasSnapshot?   floor                  retained log
   no          no         0 / 0 (unchanged)      state.Log (or none)
  yes          no         0 / 0 (unchanged)      state.Log (Phase 6 behavior, byte-identical)
   no         yes         snapshot's own bound   none
  yes         yes         snapshot's own bound   state.Log entries with Index > floor
```

In the last case, any persisted log entries *at or below* the snapshot
boundary are dropped — this is what makes the crash-window below safe —
and a gap between the boundary and the first retained entry (which should
never happen given the write ordering below, but is checked rather than
trusted) fails closed with a wrapped `ErrCorrupt`, never silently
proceeding with missing history.

A `pendingSnapshot` is always set when `hasSnapshot` is true, **even if
this is the very same process/data that created the snapshot** — the
freshly-reconstructed `KVStateMachine` (Phase 8's composition root
rebuilds it from scratch on every `Open`) has no memory of its own dedup
table or, for the KV side, no way to know its WAL-replayed MemTable state
already matches without checking. Making the Applier restore
unconditionally keeps the logic in one place and is idempotent: restoring
a snapshot's KV entries onto a `MemTable` that already holds the identical
values is a correct no-op (ordinary `Put` overwrite semantics).

`TestNewNode_RestartWithSnapshotOnly_ReconstructsFlooredLog`,
`TestNewNode_RestartWithSnapshotAndSuffix_DropsOverlapNoGap`, and the
`internal/dbnode` restart tests (§17) exercise this end to end, including
through real files (`FilePersister`, `MemStore`'s real WAL).

## 14. Crash-window analysis

| Crash point | Disk state afterward | Recovery |
|---|---|---|
| Before `SaveSnapshot` returns | old (uncompacted) log/state, no new snapshot file (or the old one, untouched) | identical to no `CreateSnapshot` call ever having happened |
| After `SaveSnapshot`, before the following `SaveState` | new snapshot **+** old, still-full log | `NewNode` drops the overlapping prefix of the old log (§13); correct, just briefly redundant on disk until the next successful snapshot |
| After both succeed | new snapshot + compacted log | ordinary recovery, §13's last row |
| During `SaveSnapshot`/`SaveState`'s own write (mid write-temp, mid rename) | `atomicfile.Write`'s protocol guarantees the target path is *either* the complete old file *or* the complete new one, never a partial write (`internal/storage/atomicfile`, unchanged) | one of the two rows above |
| `HandleInstallSnapshot`'s own `SaveSnapshot` fails | RPC rejected, nothing mutated at all | leader retries on the next heartbeat |
| `HandleInstallSnapshot`'s `SaveSnapshot` succeeds, its `persistLocked` (log) fails | durable snapshot, in-memory log rolled back to its pre-call state (still matches the last successfully-persisted log) | same as row 2, from the follower's perspective |

`TestCreateSnapshot_SnapshotSaveFailure_LeavesLogUntouched`,
`TestCreateSnapshot_LogSaveFailure_RollsBackCompactionButKeepsSnapshot`,
and `TestHandleInstallSnapshot_PersistenceFailure_RejectsWithoutCorruptingState`
simulate these with `MemoryPersister.FailNextSnapshotSave`/`FailNextSave`
— deterministic persistence-failure injection, not a physical
power-loss simulator, per the phase brief's own allowance for this.

## 15. Snapshot safety under partition

A node's own `CreateSnapshot` can never represent anything beyond what
that node has itself genuinely applied: `index > LastApplied()` is
rejected unconditionally, regardless of role or partition state.
Concretely, a leader isolated into a minority (still believing
`role == Leader` locally, per the Phase 8.5 doc's own description of this
exact scenario) that proposes new entries which can never commit (no
majority to replicate to) simply cannot snapshot past its last genuinely
committed/applied index — `TestNode_PartitionedMinorityLeader_CannotSnapshotBeyondTrueCommitIndex`
proves this directly: `CreateSnapshot` at the uncommitted index fails,
while `CreateSnapshot` at the real, already-applied index still succeeds
(snapshotting one's own safe history never requires a majority, which is
correct — it is purely local bookkeeping about state already known-safe).

After the partition heals, convergence happens through the **ordinary**
Raft mechanisms unchanged by this phase (term comparison, log matching,
leader authority) — `InstallSnapshot` never bypasses any of them; it is
reached only via the same `nextIndex[peer] <= SnapshotIndex()` check every
other replication decision goes through, itself gated by `role == Leader`
at the correct term exactly like `AppendEntries`.

## 16. Concurrency and lock discipline

- `(*raft.Node).CreateSnapshot`/`HandleInstallSnapshot` hold `n.mu` for
  their entire duration but perform no I/O to another node while holding
  it (no outbound RPC is issued from either) — the persistence calls
  (`SaveSnapshot`/`SaveState`) are local disk/memory operations, consistent
  with every other mutating method in this package
  (`HandleAppendEntries`, `Propose`) already calling `persistLocked` while
  holding `n.mu`.
- `broadcastAppendEntriesLocked`'s new InstallSnapshot branch follows the
  existing `trackRPC` pattern exactly: the actual `SendInstallSnapshot`
  call happens in a tracked background goroutine, never while `n.mu` is
  held — `Drain()` still correctly waits for it.
- `(*statemachine.Applier).CreateSnapshot` and the pending-snapshot restore
  inside `ApplyAvailable` both hold `a.applyMu` for their entire duration —
  the same lock, so a `CreateSnapshot` call and a concurrent
  `ApplyAvailable` (e.g. from `Run`'s background goroutine) can never
  interleave. This is what makes the "`index` must exactly equal
  `LastApplied`" check in §2 race-free: nothing can advance `LastApplied`
  between that check and the `sm.CreateSnapshot()` call it guards.
  `TestNode_ConcurrentCreateSnapshotAndApplyAvailable_NoCorruption`
  exercises this with concurrent goroutines.
- `KVStateMachine.CreateSnapshot`/`RestoreSnapshot` hold `sm.mu` for their
  entire duration, exactly like `Apply` — no torn reads of `sm.dedup` or
  interleaved storage mutation are possible.
- No lock is ever held across a call into a different layer that could
  block or call back into this one: `Applier` never holds `a.node`'s
  mutex while calling into `sm`, and `raft.Node` never calls into
  `internal/statemachine` or `internal/storage` at all (§10) — the same
  lock-ordering discipline every prior phase established is unchanged.

## 17. Test coverage

### `internal/raft`

- **Format** (`snapshot_format_test.go`): round trip (including empty
  data), bad magic, unsupported version, truncated, checksum mismatch,
  oversized data at encode time, a hand-crafted malicious length rejected
  without a huge allocation.
- **Log boundary mechanics** (`log_snapshot_test.go`): `compact`
  (updates sentinel, drops prefix, rejects an absent/already-compacted
  index, compacting the entire log), `installSnapshotBoundary` (matching
  suffix preserved, conflicting suffix discarded, no-overlap discards
  everything), logical last-position-equals-boundary with no retained
  entries, `Range`/`Slice` clamping at the boundary, `AppendAfter` working
  exactly at the boundary.
- **`CreateSnapshot`** (`snapshot_test.go`): rejects index 0 / beyond
  `LastApplied` / at-or-below the existing boundary; success path records
  the boundary and compacts the log; persists the snapshot before
  compacting the log (verified against both `MemoryPersister` and
  `FilePersister`); both persistence-failure crash windows (§14); restart
  reconciliation with and without a retained suffix.
- **AppendEntries/RequestVote after compaction**: boundary-exact
  AppendEntries, conflict resolution after compaction, RequestVote
  granted/denied correctly relative to the compacted logical history.
- **InstallSnapshot** (`installsnapshot_test.go`): basic
  persist-and-update, a far-behind follower genuinely catching up through
  a live 3-node cluster (then committing new entries normally afterward),
  matching-suffix preservation, conflicting-suffix discard, higher-term
  step-down, stale-term rejection, stale/duplicate snapshot as a trivial
  no-op success, persistence-failure rejection without state corruption,
  and `ReadIndex` correctly treating a peer behind the snapshot boundary
  as a non-acknowledgment for that round while still succeeding via the
  rest of the majority.

### `internal/statemachine`

- **Snapshot content** (`snapshot_test.go`): empty state, PUT/DELETE
  survival, dedup-state survival for duplicate/conflicting/stale requests
  and across multiple distinct clients, storage reconciliation (deleting
  keys the new snapshot omits), deterministic encoding across repeated
  calls against identical state, a 500-key round trip, and corruption
  detection (garbage, truncated, checksum-mutated) with storage left
  untouched on a rejected restore.
- **Applier integration** (`applier_snapshot_test.go`): `CreateSnapshot`
  success/rejection (wrong index, non-`Snapshotter` state machine);
  `ApplyAvailable` restoring a pending snapshot (built from a real
  `InstallSnapshot` RPC handed a real snapshot captured from a second,
  independent state machine) and correctly applying one further
  post-boundary entry in the same call; a pending snapshot against a
  non-`Snapshotter` state machine failing clearly without silently
  clearing the pending flag.

### `internal/dbnode`

Real `FilePersister` + real `MemStore`, never a bare fake, matching every
earlier phase's integration-test convention: log truncation after
`CreateSnapshot`; snapshot-then-restart with no suffix; snapshot-then-more-
writes-then-restart with a retained suffix, verifying both pre- and
post-snapshot values; a leader that snapshots, stops, restarts, and can be
re-elected and keep serving writes; a follower that falls fully behind a
3-node cluster's compacted log, receives `InstallSnapshot`, catches up,
and the cluster continues committing normally; `ConsistentGet`
(Phase 8.5's linearizable read) continuing to work correctly both before
and after a snapshot; a full three-node cluster restart after every node
has snapshotted, converging and accepting a fresh post-restart proposal;
partition safety (§15); and concurrent `CreateSnapshot`/`ApplyAvailable`
calls producing no corruption.

All pre-existing Phase 0–8.5 tests pass completely unmodified alongside
every new Phase 9 test.

## 18. Deliberately out of scope

- **Automatic/background snapshot scheduling.** `CreateSnapshot` is an
  explicit, manual trigger (`(*dbnode.Node).CreateSnapshot(index)`), per
  the phase brief's own instruction to prioritize correctness over
  triggering policy. A future phase can add a size/count-based policy on
  top of this exact mechanism without changing anything here.
- **Snapshot chunking/streaming.** `InstallSnapshot` sends the entire
  payload in one RPC. `InMemoryTransport` has no practical size limit this
  matters for; a future real network transport would need to reconsider
  this (see the Raft paper's own `offset`/`done` chunking fields, not
  implemented here).
- **Any change to the KV WAL's format or its own compaction/SSTable
  story.** The Raft snapshot and the KV WAL remain two entirely separate
  durability mechanisms, exactly as the phase brief requires: `KV WAL ≠
  Raft log ≠ Raft snapshot`. No KV WAL segment is ever deleted as part of
  this phase.
- **Cluster membership changes, a real network transport, observability,
  a chaos framework, or a linearizability checker** — unchanged exclusions
  from every prior phase, still not introduced here.

## 19. Known limitations

- `go test -race ./...` still fails with `-race requires cgo; enable cgo
  by setting CGO_ENABLED=1` — the same pre-existing Windows/no-C-compiler
  limitation already documented in Phases 6/7/8/8.5, not something this
  phase introduces or could resolve. Every concurrency claim in §16 is
  instead backed by the existing lock-discipline argument plus repeated
  (`-count=30`) deterministic test runs, which show no flakiness.
- `InstallSnapshot` sends its entire payload as a single RPC (§18) — fine
  for every test/demo scenario `InMemoryTransport` supports, but a future
  real network transport would need to add chunking.
- `commitIndex`/`lastApplied` remain volatile and reset to 0 on every
  restart (§12) — unchanged from Phase 6/7's own decision. This means a
  node with a snapshot cannot apply *anything*, including its own restored
  snapshot, until `CommitIndex` is re-established through ordinary Raft
  mechanics after a restart; this is a short, safe window (§12), not a
  correctness gap, but worth knowing if a caller expects
  `PendingSnapshot`/dedup state to be available immediately after `Open`.
- The exact-match (`index == LastApplied()`) rule for `CreateSnapshot`
  (§2) means a caller cannot "snapshot slightly behind" on purpose; a
  future automatic-scheduling phase (§18) would call `CreateSnapshot` with
  whatever `LastApplied()` happens to be at the moment it decides to
  trigger, not a value chosen in advance.
