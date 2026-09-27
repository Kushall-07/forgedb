# ForgeDB — Phase 8: Raft + Storage Integration

## Scope

Phase 8 adds one new package, `internal/dbnode`, that composes the three
subsystems built in Phases 1–7 — Raft consensus (`internal/raft`), the
state machine (`internal/statemachine`), and WAL-backed KV storage
(`internal/storage`) — into a single object (`dbnode.Node`) representing
one ForgeDB replica, and a matching integration test matrix that exercises
the resulting write path with real, file-backed persistence rather than
bare in-memory fakes. It does **not** add snapshots, Raft log compaction,
cluster membership changes, a real network transport (gRPC, HTTP, TCP),
read consistency / linearizable reads, a chaos framework, benchmarking, or
production observability — see §15.

Phase 8 changes exactly two existing files, both additive and both
required for a correctness gap this phase's own integration tests exposed:

- `internal/statemachine/applier.go`: `Applier.Stop` now blocks until its
  background goroutine has actually returned (see §13).
- `internal/statemachine/applier_test.go` / `internal/raft` tests:
  unaffected, still pass unmodified.

Every other Phase 0–7 file is untouched. `internal/raft` and
`internal/storage` still do not import each other, and neither imports
`internal/dbnode`; `internal/dbnode` is the only new edge in the import
graph, and it points inward from a new leaf, not outward from an existing
one.

## 1. Why Phase 8 exists

Phases 5–7 each ended at a real, working boundary, but nothing before this
phase had ever opened all three subsystems together against real files and
proposed a write through the actual leader:

```text
Phase 5:  Raft replicates and commits an opaque byte log.        (in-memory only)
Phase 6:  that log (and term/vote) survives a restart.           (in-memory OR file, tested separately)
Phase 7:  a committed entry is decoded and applied to a Store.   (real Store, but Raft persistence
                                                                    was MemoryPersister in every
                                                                    cluster/convergence test)
```

Phase 7's own cluster test (`TestApplier_ClusterConvergesAfterMajorityCommit`)
already proves the mechanical wiring works — a real 3-node Raft cluster,
Phase 7's real `Applier`/`KVStateMachine`, and real `storage.MemStore`
instances converge after a majority commit. What it does not prove is
anything about **persistence across a restart of the composed system**:
that test's Raft nodes use the default in-memory persister, and nothing in
Phases 5–7 ever opens two independently-directoried, file-backed replicas
side by side and restarts one (or all) of them. Phase 8 is that: the
composition root that owns a real `RaftDir` and a real `KVDir` per node,
opens both real persistence layers together, and a test matrix that
restarts real files, kills real leaders, and partitions real (in-memory
transport) network paths against that composition.

## 2. The write path

```text
                         Client
                           |
                           v
                    +-------------+
                    |    Raft     |   internal/raft — ordering, replication,
                    +------+------+   term/vote/log persistence (FilePersister)
                           |
                    replicate log
                           |
                           v
                     Majority Commit
                           |
                           v
                    +-------------+
                    | Applier /   |   internal/statemachine — apply-in-order
                    | StateMachine|   boundary, (ClientID, RequestID) dedup
                    +------+------+
                           |
                           v
                    +-------------+
                    |  KV Store   |   internal/storage — Store interface
                    +------+------+
                           |
                    +------+------+
                    v             v
                   WAL         MemTable
                                  |
                                  v
                              SSTables
                                  |
                                  v
                              Compaction
```

`dbnode.Node` (`internal/dbnode/node.go`) is the object that owns one
instance of each box down to `KV Store`: a `*raft.Node`, a
`statemachine.KVStateMachine`, a `statemachine.Applier`, and a
`storage.Store` (concretely `*storage.MemStore`). SSTables/Manifest/
Compaction remain exactly the existing Phase 3/4 machinery underneath
`MemStore`; Phase 8 does not touch them, invoke them explicitly, or add a
flush hook — the current `WAL -> MemTable` path is the only one any Phase
8 test exercises, per the phase's own boundary.

The one invariant every part of this phase is built around: **Raft
determines the authoritative ordering and commit point. The state machine
applies only committed commands. Storage persists the resulting state.**
Concretely, nothing in `internal/raft` (in particular, `HandleAppendEntries`)
ever calls anything in `internal/storage` or `internal/statemachine` —
verified both by inspection (`internal/raft`'s only reference to
`internal/storage` is `file_persister.go`'s import of
`internal/storage/atomicfile`, an unrelated low-level file-write helper
also used by the Phase 3 Manifest, not the KV `Store` interface at all)
and behaviorally, by
`TestNode_MinorityPartition_FollowerLogsUncommittedEntry_StorageUnaffected_ThenHeals`
(§8).

## 3. Two durabilities, two logs

Phase 8 introduces no new persistence mechanism of its own — it only opens
the two that already exist, side by side, per node:

```text
Raft persistence (internal/raft.FilePersister):
    currentTerm + votedFor + Raft log
    -> answers "what did this cluster agree on, and in what order?"

KV persistence (internal/storage.MemStore):
    WAL + MemTable
    -> answers "what is the current key/value state?"
```

They are never merged. `dbnode.Config` requires distinct `RaftDir` and
`KVDir` per node specifically so the two on-disk artifacts never share a
file or directory (see §6); `Node.Open` never uses the KV WAL to store
Raft's log, and never asks `FilePersister` to hold SSTables. A Raft log
entry (`index 20 -> PUT x=10`) means "the cluster agreed this command
belongs at position 20"; the KV WAL entry that results from applying it
means "this mutation is durable on this replica's disk" — two different
questions, two different files, two different subsystems, exactly as
Phase 7's own docs already drew this line for the in-memory case.

This split has a real, observable consequence Phase 8's restart tests
depend on: **KV state recovers from a node's own WAL independent of
anything Raft remembers.** `TestNode_Restart_RecoversKVAndRaftLogFromRealFiles`
and `TestNode_FullClusterRestart_ConvergesToRecoveredState` both verify
that a reopened node's `Store().Get` already returns the correct,
previously-committed value **before any Raft activity happens at all** —
before an election, before a single heartbeat, before `ApplyAvailable` is
even called once. Raft's own bookkeeping (`CommitIndex`, `LastApplied`) is
volatile by Phase 6/7's own design (see
`docs/raft/phase6-raft-persistence.md`) and always resets to 0 on restart;
KV durability was never contingent on that bookkeeping surviving.

## 4. Node composition (`internal/dbnode`)

```go
type Config struct {
    ID, Peers          // exactly raft.Options.ID/Peers
    Transport          // exactly raft.Options.Transport
    RaftDir, KVDir      // this node's two independent directories (see §6)
    ElectionTickMin, ElectionTickMax, HeartbeatTick, Rand  // passed through to raft.Options
    WrapStore          // test-only hook; nil in production (see §11)
}

func Open(cfg Config) (*Node, error)

type Node struct { /* unexported: *raft.Node, storage.Store, *statemachine.KVStateMachine, *statemachine.Applier */ }
```

`Open` performs exactly the lifecycle the phase's design calls for:

```text
Open
  |
  +-- storage.NewMemStore(cfg.KVDir)         -- opens + replays KV WAL
  |
  +-- raft.NewFilePersister(cfg.RaftDir/raft-state)
  +-- raft.NewNode(... Persister: that ...)   -- restores currentTerm/votedFor/log if present
  |
  +-- (if cfg.Transport supports it) Transport.Register(cfg.ID, the raft.Node)
  |
  +-- statemachine.NewKVStateMachine(store)
  +-- statemachine.NewApplier(raftNode, sm)
```

`Node` introduces no consensus or storage logic of its own; every method
(`Propose`, `Tick`, `Drain`, `ApplyAvailable`, `IsLeader`, `Store`, `Raft`,
`Run`, `Close`) is a thin, direct delegation to the one already-existing
package that owns that behavior. `Propose(cmd statemachine.Command)`
encodes `cmd` via `Command.Encode()` (Phase 7's existing wire format,
unchanged — see §9) and calls `(*raft.Node).Propose` with the result;
`ApplyAvailable` calls `(*Applier).ApplyAvailable` directly, giving tests
the same deterministic, no-arbitrary-sleep story Phases 5–7 already
established (`Tick`/`Drain` for Raft, `ApplyAvailable` for application).

`registrar` is a small, unexported interface (`Register(id string, h
raft.RPCHandler)`) that `*raft.InMemoryTransport` happens to satisfy: `Open`
uses it, when present, to register the freshly built `raft.Node`
automatically, so a cluster test never needs a separate manual `Register`
call per node. A future real network transport that has no such
registration step simply does not implement `registrar`, and `Open` skips
it silently — this is the "clean integration hook" the phase asked for,
added without touching `raft.Transport` itself.

## 5. Per-node storage directories

Every `dbnode.Node` in a test cluster is opened with its own, freshly
created `t.TempDir()` subtree, split into two independent subdirectories:

```text
<tempdir>/
├── raft/
│   └── raft-state        (FilePersister's single state file)
└── kv/
    └── wal.log            (MemStore's WAL)
```

`Open` does not itself detect or prevent two nodes sharing a directory —
that remains the caller's responsibility, exactly as `raft.NewNode` and
`storage.NewMemStore` never detected it either — but every test in
`internal/dbnode` builds its cluster through the shared `newCluster`
helper (`internal/dbnode/cluster_test.go`), which always allocates a fresh
`t.TempDir()` per node, and
`TestNode_IndependentPerNodeDirectories` checks both that every
`RaftDir`/`KVDir` pair across the whole cluster is a distinct absolute
path and that each node's `kv/wal.log` is a real, non-empty file on disk
after a replicated write — physically separate from every other node's.

## 6. Deterministic testing: `Tick`/`Drain`/`ApplyAvailable`, no sleeps

Every `internal/dbnode` test drives its cluster exactly the way
`internal/raft` and `internal/statemachine` already do: `Tick` to advance
logical time, `Drain` to wait for a node's own in-flight RPC goroutines,
`ApplyAvailable` to apply whatever is currently committed. No test in this
package uses `time.Sleep` to make an assertion pass.

One genuinely subtle timing detail surfaced while building this test
matrix, worth documenting since it is easy to get wrong: **a follower only
learns of a new commit index on the *next* AppendEntries round after the
one that actually advanced it**, because the leader's broadcast reads its
own `CommitIndex` synchronously at the moment it constructs the RPC
(`broadcastAppendEntriesLocked` in `internal/raft/replication.go`), before
that same round's replies (which are what advance `CommitIndex` in the
first place) have necessarily been processed. The test helper
`settleCommit` (`internal/dbnode/cluster_test.go`) handles this
correctly — tick, **fully drain**, tick again, drain again — specifically
so the second round's heartbeat is guaranteed to carry an
already-up-to-date `LeaderCommit`, rather than racing that update. An
earlier draft of this helper ticked twice before draining at all and was
measurably flaky under `-count=30` for exactly this reason; the fixed
version has run clean.

## 7. Test matrix

`internal/dbnode/node_test.go` (plus its `cluster_test.go` helpers)
implements the required integration/failure matrix. Each test uses real
`FilePersister` + real `MemStore` under distinct temporary directories,
never a bare in-memory stand-in for either:

- **Single-leader PUT/DELETE through the actual commit boundary**
  (`TestNode_SingleLeader_PutAndDeleteThroughCommit`) — see §8 for why this
  uses a 3-node cluster's leader rather than a literal one-node cluster.
- **Three-node replication** (`TestNode_ThreeNodeCluster_ReplicatesPutAndDelete`) —
  PUT and DELETE each replicate to a majority and converge to identical
  final state on all three stores.
- **Minority partition: follower logs an uncommitted entry, storage stays
  untouched everywhere, then heals**
  (`TestNode_MinorityPartition_FollowerLogsUncommittedEntry_StorageUnaffected_ThenHeals`) —
  see §8 for why this needs 5 nodes, not 3.
- **Leader failure: surviving majority retains committed state and elects
  a new leader** (`TestNode_LeaderFailure_SurvivingMajorityRetainsCommittedStateAndElectsNewLeader`).
- **Node restart from real files**
  (`TestNode_Restart_RecoversKVAndRaftLogFromRealFiles`) — see §3 and §8.
- **Full cluster restart**
  (`TestNode_FullClusterRestart_ConvergesToRecoveredState`) — see §8.
- **Duplicate request through the full Raft path**
  (`TestNode_DuplicateRequestThroughRaft_MutatesStorageExactlyOnce`) — see §9.
- **Storage failure through the full path halts application without
  advancing `LastApplied`, then a retry completes it**
  (`TestNode_StorageFault_StopsWithoutAdvancingLastApplied_ThenRetrySucceeds`) —
  see §10.
- **Apply ordering** (`TestNode_ApplyOrdering_StrictlySequential`) — four
  commands against the same key apply strictly in log order.
- **Concurrent apply protection**
  (`TestNode_ConcurrentApplyAvailable_NoDoubleApplication`) — eight
  goroutines call `ApplyAvailable` on the same node concurrently; every
  committed entry is still applied exactly once.
- **Independent per-node directories** (`TestNode_IndependentPerNodeDirectories`) —
  see §5.

All eleven tests, plus every pre-existing `internal/raft` and
`internal/statemachine` test, pass under `-count=30` (see §14).

## 8. Adaptations forced by an existing Raft limitation

`internal/raft` has one pre-existing, already-documented behavior (see
`internal/raft/apply_test.go`'s `commitUpTo` comment) that directly shapes
several Phase 8 tests: **a literal one-node Raft cluster (zero peers) never
advances its own `CommitIndex`.** `maybeAdvanceCommitIndexLocked` — the
only place `CommitIndex` is ever raised on a leader — is invoked solely
from a peer's `AppendEntries` reply handler; with zero peers, that handler
is never invoked, so nothing ever calls it. This is not a bug Phase 8
introduces or a design limitation Phase 8 is asked to fix (per the phase's
own "do not redesign Raft" boundary) — it is a fact about the existing
implementation that Phase 8's own "primary goal" wording already
anticipates by describing a cluster of nodes (leader + followers), not a
single lone node. Every Phase 8 test that needs an actual committed entry
therefore uses at least a 3-node cluster and treats "the leader" as the
node under real test, exactly as Phase 7's own cluster test already does.

A related, more subtle consequence of Raft's **current-term commit rule**
(§5.4.2 of the Raft paper; enforced in `maybeAdvanceCommitIndexLocked` via
`term != n.currentTerm`) shows up in the restart tests: a newly (re-)elected
leader cannot advance its own `CommitIndex` to cover entries from a
*previous* term purely by having them already replicated to a majority —
it can only do so once a *new* entry, proposed in its own current term,
itself reaches a majority, at which point `CommitIndex` jumps forward to
cover that new entry *and* everything before it in one step. This is why:

- `TestNode_Restart_RecoversKVAndRaftLogFromRealFiles` restarts a
  **follower**, not the leader: a follower's `CommitIndex` catches up
  through the ordinary mechanism (accepting the still-active leader's
  `LeaderCommit` directly in `HandleAppendEntries`, which carries no
  current-term restriction), so no new proposal is needed for it
  specifically to recover.
- `TestNode_FullClusterRestart_ConvergesToRecoveredState`, which restarts
  *every* node, explicitly proposes one new command (`"restart-marker"`)
  after the fresh post-restart election, and only then asserts
  `CommitIndex` has caught up across the reopened cluster. The test
  asserts the more important claim — that KV state was already fully and
  correctly recovered from each node's own WAL — *before* that proposal,
  precisely to keep this distinction visible rather than papering over it.

And why `TestNode_MinorityPartition_FollowerLogsUncommittedEntry_StorageUnaffected_ThenHeals`
uses a 5-node cluster: with only 3 nodes, any single reachable follower
plus the leader already forms a majority (2 of 3), which would commit the
entry immediately and never exercise "a follower has logged an entry its
leader cannot yet commit" at all. A 5-node cluster, with the leader able to
reach only itself and one follower (2 of 5, short of the 3-node majority),
reproduces the scenario the test needs.

None of this required changing `internal/raft`. It is documented here,
honestly, as the shape Phase 8's tests had to take to work correctly with
the Raft implementation that actually exists — per this phase's own
instruction to adapt to the existing implementation rather than silently
redesigning it.

## 9. Deduplication through the full path

Raft itself never deduplicates: `Propose`ing the same encoded `Command`
bytes twice (identical `ClientID`, `RequestID`, `Op`, `Key`, `Value`)
appends two distinct `LogEntry` values at two distinct indices, and both
commit independently — `TestNode_DuplicateRequestThroughRaft_MutatesStorageExactlyOnce`
asserts `CommitIndex == 2` after proposing the same command twice,
confirming this explicitly. Deduplication remains exactly where Phase 7
put it: `KVStateMachine.Apply`'s per-`ClientID` dedup table. When
`Applier.ApplyAvailable` applies the second (replayed) entry, `Apply`
recognizes the identical `(RequestID, Op, Key, Value)` tuple, returns the
cached `Result` with `Replayed = true`, and never calls `Store.Put` a
second time. The test verifies this at the storage layer directly, via a
`countingStore` that wraps the real `MemStore` (through `Config.WrapStore`,
see §11) and counts genuine `Put` calls: after both entries apply, the
count is exactly 1, on every node.

## 10. Storage failure and retry through the full path

Section 22–24 of the phase's own scope ask for this to be demonstrated
through the actual Raft commit boundary, not just at the
`statemachine.Applier` level where Phase 7 already covers it in isolation
(`TestApplier_StorageFault_StopsWithoutAdvancingLastApplied`). Phase 8
repeats the same shape of test one layer up, through `dbnode.Node`:
`TestNode_StorageFault_StopsWithoutAdvancingLastApplied_ThenRetrySucceeds`
injects a fault into the leader's real `MemStore` via `Config.WrapStore`
(§11), commits three entries, and verifies:

- `ApplyAvailable` returns a non-nil error and applies zero entries once it
  hits the failing one;
- `LastApplied` does **not** advance past the entry before the failure;
- the failed entry's key is genuinely absent from storage (the failure is
  never pretended away);
- Raft's own `CommitIndex` is completely unaffected by the local storage
  fault — consensus and local application are separate concerns, and nothing
  here ever "un-commits" an entry because this replica failed to apply it;
- a later retry (fault cleared) resumes at exactly the failed entry and
  completes the rest in order, without creating a new consensus entry —
  the original committed entry remains authoritative throughout.

## 11. `Config.WrapStore`: the one test-only seam

`dbnode.Node` needs no second, parallel storage implementation to test
storage-layer failure or call-counting (§9, §10) — `Config.WrapStore func(storage.Store) storage.Store`,
if non-nil, wraps the real `*storage.MemStore` `Open` already constructs,
before handing it to `KVStateMachine`. It is `nil` in every non-test path
and in most tests; only the two tests that specifically need to observe or
fail individual `Store` calls set it, each with a small unexported wrapper
type local to `node_test.go` (`countingStore`, `faultyStore` — deliberately
not reused from `internal/statemachine`'s own equivalent test helpers,
since those are unexported to that package). This keeps the seam minimal
and honest: it exists purely so a test can observe/interfere with real
`Store` calls, never to introduce an alternate code path production code
could ever take.

## 12. Lifecycle and shutdown

```text
Open
  |
  +-- KV storage opened, WAL replayed
  +-- Raft persistence opened, state restored
  +-- raft.Node constructed (Follower, term/log restored, commitIndex/lastApplied = 0)
  +-- registered with Transport (if it supports registrar)
  +-- KVStateMachine + Applier constructed
  |
  v
(Tick/Drain/ApplyAvailable for tests, or Run for production)
  |
  v
Close
  |
  +-- raft.Node.Stop()      -- halts Run's background ticking, if started
  +-- raft.Node.Drain()     -- waits for in-flight outbound RPC goroutines
  +-- Applier.Stop()        -- halts Run's background loop AND BLOCKS until
  |                            it has actually returned (see §13)
  +-- store.Close()         -- only now, once nothing can still be
                                writing to it
```

This ordering is what prevents a shutdown race between the applier still
writing to storage and storage being closed out from under it. `Close` is
safe to call even if `Run` was never called (the common case in every
deterministic test in this package, which drives everything via
`Tick`/`Drain`/`ApplyAvailable` directly and never starts either
background loop), and it does not delete or otherwise touch a node's
on-disk directories — a later `Open` with the same `Config` reopens
exactly what `Close` left behind (this is exactly what every restart test
in §7 relies on).

## 13. The one change to existing code: `Applier.Stop` now waits

Building the shutdown ordering in §12 exposed a real, if narrow, gap in
Phase 7's `Applier`: `Stop` (before this phase) only closed `stopCh` and
returned immediately — it never waited for `Run`'s background goroutine to
actually exit. If that goroutine happened to be in the middle of
`ApplyAvailable` (and therefore in the middle of a blocking `Store.Put`/
`Delete` call) at the moment `Stop` was called, a caller that closed
storage as soon as `Stop` returned could race an in-flight write against
`Store.Close`. Phase 8's own lifecycle rule (§12, and the phase's original
"do not close storage while the applier can still be writing to it"
requirement) cannot be honored without fixing this.

The fix is small and purely additive: `Applier` gained an unexported
`doneCh`, created by `Run` and closed when its goroutine returns; `Stop`
now closes `stopCh` (as before) and then, if `Run` was ever called, blocks
on `doneCh`. `Run` is now also idempotent (a second call is a no-op if the
background goroutine is already running), which it implicitly wasn't
guaranteed to be before. No existing method signature, field, or test
changed; `TestApplier_Run_CatchesUpInBackground` still passes unmodified.
A new test, `TestApplier_Stop_WaitsForInFlightApplyBeforeReturning`
(`internal/statemachine/applier_test.go`), proves the fix directly: it
blocks a `Put` call mid-flight via a small `blockingStore` test wrapper,
confirms `Stop` has *not* returned 50ms after being called concurrently,
then unblocks the `Put` and confirms `Stop` returns immediately afterward
with `LastApplied` fully caught up.

This is exactly the kind of "smallest correctness-preserving extension"
the phase's own instructions anticipated for a lower-phase limitation that
blocks safe integration (originally written with `internal/raft` in mind,
but the same reasoning applies one phase up, to `internal/statemachine`,
which Phase 8 is equally forbidden from redesigning wholesale).

## 14. Validation

```powershell
gofmt -w .                                        # clean
go vet ./...                                      # clean
go build ./...                                    # clean
go test ./...                                     # all packages pass
go test ./internal/raft/... -count=30             # pass
go test ./internal/statemachine/... -count=30     # pass
go test ./internal/dbnode/... -count=30           # pass
go run ./cmd/forgedb                              # unchanged Phase 2 demo still runs end to end
```

`go test -race ./...` still fails with `-race requires cgo; enable cgo by
setting CGO_ENABLED=1` — the same pre-existing Windows/no-C-compiler
limitation already documented in `docs/raft/phase6-raft-persistence.md`
and `docs/raft/phase7-state-machine.md`, not something this phase
introduces or could resolve.

## 15. What Phase 8 intentionally does not solve

Per the phase boundary, Phase 8 does **not** include:

- **Real networking**: no gRPC, no HTTP, no TCP, no protobuf wire format.
  `raft.InMemoryTransport` is reused completely unchanged; `dbnode.Node`'s
  only accommodation for a future real transport is the small, optional
  `registrar` interface (§4), which a transport with no registration step
  simply does not implement.
- **Client redirection**: `Propose` on a non-leader `dbnode.Node` still
  returns `ErrNotLeader` (a direct re-export of `raft.ErrNotLeader|`); no
  HTTP redirect or leader-discovery protocol is added.
- **Read consistency / linearizable reads**: `GET` is still not a
  replicated command (unchanged from Phase 7). Every test in this package
  that needs to check KV state inspects a node's own `Store()` directly,
  exactly as the phase's own instructions say is acceptable for Phase
  8-level testing; no `ReadIndex`, lease read, quorum read, or
  follower-read policy exists. That remains Phase 8.5's job.
- **Snapshots or Raft log compaction**: the recovered log at restart is
  always the *complete* log; nothing here trims, compacts, or snapshots
  it.
- **Cluster membership changes**: `Peers` remains fixed for a `Node`'s
  lifetime, exactly as `raft.Options.Peers` already was.
- **A chaos framework, benchmarking, or production observability**: the
  test matrix uses `raft.InMemoryTransport`'s existing `Partition`/`Heal`
  directly; nothing new is added for fault injection beyond
  `Config.WrapStore` (§11), which exists solely as a test seam, not a
  chaos-testing framework.
- **Full exactly-once crash semantics**: Phase 7's own documented
  limitation stands unchanged — a client retry that lands after a crash
  and restart is not recognized by the (in-memory-only, per-process)
  dedup table, though the underlying `PUT`/`DELETE` operations remain
  idempotent at the storage level regardless (see
  `docs/raft/phase7-state-machine.md` §14). Phase 8 neither improves nor
  worsens this; it is unaffected by anything in this phase, since the
  dedup table's persistence was never in scope here.

`internal/raft`'s and `internal/storage`'s own internals (replication,
election, WAL, MemTable, SSTable, compaction) were not touched at all;
`internal/statemachine`'s only change is the additive `Applier.Stop` fix
in §13.
