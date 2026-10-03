# ForgeDB — Phase 18: Snapshot Robustness Hardening and Validation

## Scope

Phase 18 is a correctness-and-validation pass over the Raft snapshot
mechanism Phase 9 already built (`docs/raft/phase9-snapshots.md`), not a
redesign. The brief called for inspecting snapshot creation, persistence,
restoration, log-compaction interaction, and restart/recovery behavior
first, and to make only narrowly-justified changes if a concrete defect
turned up. This document records what the inspection found, the two test
gaps it closed, and the end-to-end validation performed against a real
Docker cluster.

## 1. Summary of findings

**No correctness defect was found in the existing snapshot implementation.**
Phase 9's own documentation (`docs/raft/phase9-snapshots.md`) already
describes, and its existing test suite already exercises, every invariant
this phase's brief asked to re-verify: snapshot persistence ordering
(snapshot-before-compaction), crash-window safety via deterministic
failure injection, the Raft/state-machine `PendingSnapshot` handoff,
follower `InstallSnapshot` catch-up, partition safety, concurrent
`CreateSnapshot`/`ApplyAvailable` access, and full-cluster restart
convergence. Reading the code (`internal/raft/snapshot.go`, `log.go`,
`format.go`, `file_persister.go`, `raft.go`'s `NewNode`,
`internal/statemachine/snapshot.go`, `applier.go`) confirmed the
implementation matches that documentation; nothing was rewritten.

Two **test-coverage gaps** were identified and closed (see §4): neither is
a defect in the shipped code — in both cases the production code path
already fails closed correctly — but neither had a test proving it, and the
phase brief explicitly requires proving "corrupt/truncated snapshot is
rejected rather than silently accepted" and "repeated snapshot/recovery is
safe."

## 2. Existing snapshot architecture (as inspected, unchanged)

### What a snapshot contains

A `raft.Snapshot{LastIncludedIndex, LastIncludedTerm, Data}` is Raft's
own, opaque wrapper; `Data` is produced and consumed entirely by
`internal/statemachine` (`KVStateMachine.CreateSnapshot`/`RestoreSnapshot`,
`internal/statemachine/snapshot.go`), which encodes every live key/value
pair (sorted, tombstones omitted) **and the entire request-deduplication
table** — not just KV data. `internal/raft` never interprets `Data`; see
Phase 9 doc §10 for the full layering argument, unchanged here.

### Boundary representation

`Log.entries[0]` is the sentinel: before any snapshot it is `{0, 0}`;
once a snapshot exists it is `{LastIncludedIndex, LastIncludedTerm}` —
the same mechanism generalized, not a second one. `TermAt`, `EntryAt`,
`Slice`, `Range`, `compact`, and `installSnapshotBoundary`
(`internal/raft/log.go`) all reason in terms of this one sentinel.

### Persistence and atomicity

`FilePersister` (`internal/raft/file_persister.go`) writes the snapshot to
a file derived from the state file's own path (`<path>.snapshot`), using
the same write-temp → fsync → atomic-rename → best-effort-fsync-directory
protocol as every other durable file in this codebase
(`internal/storage/atomicfile`). A reader can never observe a torn or
partially-written snapshot file. The on-disk format
(`internal/raft/format.go`, magic `"ForgeSS1"`) is versioned, every length
is bounds-checked before use, and the payload carries a trailing CRC-32C
checksum validated before any field is parsed.

### State-machine restoration

`(*statemachine.Applier).ApplyAvailable` checks `node.PendingSnapshot()`
**before** computing which committed entries to apply next. If one is
pending, it calls `RestoreSnapshot` (which reconciles storage — deleting
keys the snapshot doesn't mention, not just writing the ones it does — see
Phase 9 doc §3) and then `ConfirmSnapshotRestored`, which is what actually
advances `LastApplied` to the snapshot boundary. This ordering is what
makes it impossible to apply a post-boundary entry against a state machine
that hasn't yet recovered the pre-boundary state the entry's effects
depend on (e.g. the dedup table a retried request needs to be recognized
against).

### Log compaction and follower catch-up

A node's own `CreateSnapshot` (`internal/raft/snapshot.go`) persists the
snapshot **before** touching the in-memory log, and persists the smaller
compacted log only after that succeeds — rolling the in-memory compaction
back if the second save fails, so in-memory state can never be ahead of
what a restart could actually recover (§14 of the Phase 9 doc has the full
crash-window table). A follower that has fallen behind a leader's own
compaction boundary receives `InstallSnapshot`
(`HandleInstallSnapshot`) instead of `AppendEntries`; its log either keeps
a matching suffix or discards a conflicting one
(`Log.installSnapshotBoundary`), and ordinary `AppendEntries` replication
resumes automatically on the next heartbeat once its `nextIndex` is past
the new boundary.

### Restart recovery

`NewNode` (`internal/raft/raft.go`) loads both the persisted state and the
persisted snapshot, floors the retained log at the snapshot's boundary,
drops any overlapping prefix the persisted log might still contain (the
harmless crash-window case), and fails closed with a wrapped `ErrCorrupt`
if the retained suffix has a gap immediately after the boundary rather
than silently proceeding with missing history. A `pendingSnapshot` is
always set when a snapshot was found, even if it is the same process that
created it, so the state machine always gets a chance to (idempotently)
restore it.

## 3. Invariants reconfirmed, not changed

Every invariant the phase brief listed as "preserve" was checked against
the current code and found already upheld:

| Invariant | Where enforced |
|---|---|
| Raft owns replicated ordering and consensus | unchanged; snapshots never bypass `AppendEntries`/`RequestVote` term and log-matching rules |
| Only committed entries are applied | `Applier.ApplyAvailable` bounds by `CommitIndex`; `ConfirmSnapshotRestored` re-validates `lastIncludedIndex <= CommitIndex` independently rather than trusting the caller |
| State machine restored consistently with the snapshot | `RestoreSnapshot` reconciles (not merely overlays) storage; dedup table restored atomically alongside KV data |
| Persistent Raft metadata remains durable | `SaveSnapshot`/`SaveState` both use the atomic-file protocol; both independently checksummed |
| Log entries after the snapshot boundary remain valid | `Log.compact`/`installSnapshotBoundary` never touch entries above the boundary; `TestLog_Range_ClampsAtCompactionBoundary` etc. |
| Followers never independently mutate application state | a follower only ever adopts `InstallSnapshot`'s leader-sent boundary; it never calls `CreateSnapshot` on its own initiative from anywhere but the same explicit, caller-driven API a leader uses |
| Snapshot creation never produces an inconsistent image | `KVStateMachine.CreateSnapshot` holds `sm.mu` for the entire call; `Applier.CreateSnapshot` holds `applyMu` and requires `index == LastApplied()` exactly, closing the TOCTOU window a looser `<=` check would leave |
| Recovery never silently accepts a corrupted/partial snapshot | `decodeSnapshot`'s checksum check; **now also proven at the `NewNode`/`dbnode.Open` integration level, not just the format-decode unit level — see §4** |
| Snapshot installation + log truncation preserve Raft safety | `installSnapshotBoundary`'s matching-suffix-preserved / conflicting-suffix-discarded logic; `HandleInstallSnapshot`'s term/step-down discipline identical to `HandleAppendEntries` |

## 4. Test gaps found and closed

Inspection of the existing test suite (`internal/raft/snapshot_test.go`,
`snapshot_format_test.go`, `installsnapshot_test.go`, `log_snapshot_test.go`,
`internal/statemachine/snapshot_test.go`, `applier_snapshot_test.go`,
`internal/dbnode/snapshot_test.go`) found thorough coverage of every
documented invariant, with two exceptions, both now closed:

1. **No test exercised a genuinely corrupted on-disk snapshot file through
   the actual startup path** (`raft.NewNode` / `dbnode.Open`). The format
   decoder's checksum/bounds checks were already unit-tested directly
   (`snapshot_format_test.go`), and `NewNode`'s error-propagation code for
   a `LoadSnapshot` failure was already correct, but nothing proved the
   two wired together — i.e. that a node whose `.snapshot` file was
   damaged after a clean shutdown (bit rot, a bad restore, a tampered
   file) actually refuses to start rather than silently discarding the
   snapshot or starting with partial data. Added:
   - `TestNewNode_CorruptedSnapshotFile_FailsClosed`
     (`internal/raft/snapshot_test.go`) — flips a byte in a real,
     `FilePersister`-written snapshot file on disk and confirms `NewNode`
     returns a wrapped `ErrCorrupt`.
   - `TestNode_RestartWithCorruptedSnapshotFile_FailsClosed`
     (`internal/dbnode/snapshot_test.go`) — the same proof one layer up,
     through the real `dbnode.Open` composition root and a real 3-node
     cluster's follower.

2. **No test covered the `NewNode` "gap after snapshot boundary" check**
   (`raft.go`, the `suffix[0].Index != floorIndex+1` branch) — defense-in-
   depth code that should be unreachable given this package's own write
   ordering, but was otherwise completely unexercised. Added:
   - `TestNewNode_PersistedLogGapAfterSnapshotBoundary_Rejected`
     (`internal/raft/snapshot_test.go`) — constructs a persisted state/
     snapshot pair with a deliberate gap and confirms `NewNode` fails
     closed with a wrapped `ErrCorrupt` instead of silently proceeding
     with missing history.

3. **No test drove more than two generations of snapshot-then-restart
   against the same node.** Every existing restart test (Phase 9's own,
   `internal/dbnode/snapshot_test.go`) covers a single snapshot followed
   by a single restart; nothing proved that repeating the cycle against
   the same on-disk files doesn't accumulate corruption or regress the
   boundary. Added:
   - `TestNode_RepeatedSnapshotRestartCycles_StateCorrect`
     (`internal/dbnode/snapshot_test.go`) — three successive write →
     snapshot → close → reopen cycles against the same follower,
     asserting `SnapshotIndex` and every previously-written key survive
     each generation.

No production code was changed. All four new tests pass; no existing test
was modified.

## 5. Snapshot-transfer limitation (unchanged, confirmed still accurate)

`InstallSnapshot` sends its entire payload as a single RPC — there is no
chunking/streaming (no `offset`/`done` fields as in the original Raft
paper). This was Phase 9's explicitly documented, deliberate scope
boundary and remains unchanged and acceptable for this phase: the brief
for Phase 18 explicitly excludes turning this into a streaming-transfer
project. `InMemoryTransport` has no practical size limit this matters for,
and the real gRPC transport (`internal/transport`) was not modified.

## 6. What was tested

### Go test suite

- `go build ./...` — clean, no errors.
- `go vet ./...` — clean, no warnings.
- `go test ./...` — all packages pass, including the three new tests
  above alongside every pre-existing test.
- `go test -race ./...` — fails immediately with `-race requires cgo;
  enable cgo by setting CGO_ENABLED=1`. This is the same pre-existing
  Windows/no-C-compiler environment limitation every prior phase's docs
  already record (no `gcc` on `PATH` in this environment); it is not
  something this phase introduced or could resolve without installing a
  C toolchain. Concurrency correctness for the snapshot path continues to
  rest on the lock-discipline argument in Phase 9 doc §16 plus this
  phase's own and the existing `TestNode_ConcurrentCreateSnapshotAndApplyAvailable_NoCorruption`
  test, which exercises concurrent `CreateSnapshot`/`ApplyAvailable`
  access without `-race` instrumentation.

### Docker integration validation

A real three-node cluster was built and run via `docker compose` (project
name `forgedb18`, a disposable namespace distinct from any existing
deployment) to validate the full snapshot lifecycle end to end:

1. Started all three nodes + `forge-gateway`; waited for leader election.
2. Wrote 5 keys through the leader; read them all back successfully.
3. Triggered `/admin/snapshot` on the leader (`SnapshotIndex` advanced to
   5).
4. Wrote 3 more keys and deleted one of the original 5 (exercising
   `RestoreSnapshot`'s delete-reconciliation path across a future
   restart), then triggered `/admin/snapshot` on both followers
   (`SnapshotIndex` advanced to 9 on each, matching their own
   `LastApplied`).
5. Confirmed `/cluster` reported consistent `CommitIndex`/`LastApplied`
   across all three nodes before any restart.
6. `docker restart`ed the **leader** container (the harder case: snapshot
   + a retained post-snapshot suffix, plus a subsequent re-election),
   waited for its healthcheck to report `healthy` again.
7. Confirmed a new leader was elected (`node-2`, term advanced from 1 to
   2) and that all three nodes converged on identical
   `CommitIndex=9`/`LastApplied=9`/consistent per-node `SnapshotIndex`.
8. Queried every one of the original 8 keys through the new leader and
   through `forge-gateway`: the deleted key correctly returned 404, and
   every other key (both pre- and post-snapshot writes) returned its
   correct value — proving both snapshot restoration and the
   `RestoreSnapshot` delete-reconciliation path survived a real process
   restart, not just an in-process test.
9. Performed a fresh write through `forge-gateway` after the restart and
   read it back successfully, confirming the cluster continues operating
   normally afterward.
10. Checked `/health` on all three nodes: all `200`.

All steps passed. The test cluster was torn down with `docker compose down`
(never `-v`); its three named volumes (`forgedb18_forgedb-node{1,2,3}-data`)
were deliberately left in place rather than deleted, per this phase's
explicit instruction never to remove persistent volumes — an operator can
remove them by hand once no longer needed.

## 7. Known limitations (carried over from Phase 9, still accurate)

- `InstallSnapshot` has no chunking/streaming support (§5) — unchanged,
  out of scope for this phase.
- `commitIndex`/`lastApplied` remain volatile and reset to 0 on every
  restart; a node cannot apply anything, including its own restored
  snapshot, until `CommitIndex` is re-established through ordinary Raft
  mechanics after a restart. This is a short, safe window, not a
  correctness gap (Phase 9 doc §12).
- `CreateSnapshot`'s exact-match (`index == LastApplied()`) rule means a
  caller cannot snapshot slightly behind its own current state on
  purpose; any future automatic-scheduling phase would call it with
  whatever `LastApplied()` is at the moment it decides to trigger.
- `go test -race` cannot run in this environment (no cgo/C toolchain on
  `PATH`); concurrency correctness rests on code-level lock-discipline
  analysis and repeated deterministic concurrent-access tests instead.

## 8. Files changed in this phase

- `internal/raft/snapshot_test.go` — added
  `TestNewNode_CorruptedSnapshotFile_FailsClosed` and
  `TestNewNode_PersistedLogGapAfterSnapshotBoundary_Rejected`.
- `internal/dbnode/snapshot_test.go` — added
  `TestNode_RestartWithCorruptedSnapshotFile_FailsClosed` and
  `TestNode_RepeatedSnapshotRestartCycles_StateCorrect`.
- `docs/deployment/phase18-snapshot-robustness.md` — this document.

No non-test production code was changed.
