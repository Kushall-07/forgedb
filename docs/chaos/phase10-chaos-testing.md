# Phase 10: Chaos / Failure Testing

## 1. Purpose

Phases 5 through 9 built ForgeDB's replicated write path -- Raft
consensus, Raft persistence, a deduplicating state machine, the
Raft+storage integration, linearizable reads, and snapshots/log
compaction -- and each phase's own tests prove that path correct under
healthy operation and under a handful of targeted, phase-specific
failures. Phase 10 asks a different, cross-cutting question:

> What happens when we deliberately break the system, in combinations
> and sequences no single earlier phase's tests tried?

It does this by building a small, deterministic chaos harness
(`chaos/`) on top of the *real* `internal/dbnode.Node` composition --
never a parallel fake implementation of Raft, persistence, or storage --
and driving that harness through scripted and (bounded, seeded) random
failure sequences, checking throughout that ForgeDB's safety invariants
hold and that the cluster still converges once failures stop.

Phase 10 is **not** a redesign of consensus, persistence, replication,
or snapshotting. Every fault it injects uses a mechanism one of those
phases already built (`raft.InMemoryTransport.Partition`, a real process
restart against the same on-disk directories, `raft.MemoryPersister`'s
fault-injection pattern generalized to the real `FilePersister`,
`node_test.go`'s storage-fault pattern generalized the same way). Where
Phase 10 needed a capability those phases didn't expose at all --
splitting a cluster into more than two sides, or injecting a persistence
fault through the real `dbnode.Node` composition root rather than only
inside `internal/raft`'s own unit tests -- it added the smallest possible
test seam to make that reachable (see §3), not new runtime behavior.

## 2. Failure model

Phase 10 models exactly the failures listed below. Every one of them is
expressed in terms of the real mechanism, not a simulated stand-in:

| Failure | Mechanism |
|---|---|
| Node crash | `raft.InMemoryTransport.Partition` (cuts the node off from everyone, both directions) + a real `(*dbnode.Node).Close` |
| Node restart | A fresh `dbnode.Open` against the **same** `RaftDir`/`KVDir` the crashed node used -- real on-disk recovery, not an in-memory reset |
| Network partition (one node) | `raft.InMemoryTransport.Partition`/`Heal` |
| Network partition (groups, e.g. "A B C \| D E") | `raft.InMemoryTransport.PartitionGroups`/`HealPartitions` (new, see §3) |
| Raft persistence failure (`SaveState`/`SaveSnapshot`) | A wrapping `Persister` that fails exactly the next call, generalizing `raft.MemoryPersister.FailNextSave`/`FailNextSnapshotSave` to the real `FilePersister` a `dbnode.Node` actually uses |
| KV storage failure (`Put`/`Delete`) | A wrapping `storage.Store` that fails exactly the next call, generalizing `node_test.go`'s `faultyStore` |
| Follower lag / snapshot catch-up | A real, extended `Partition` combined with real `CreateSnapshot` calls, so `InstallSnapshot` fires through its actual trigger condition (a follower's `nextIndex` behind the leader's compaction boundary), not a direct call |

A crashed node's on-disk state is **never** deleted; `RestartNode` always
reopens the exact same `RaftDir`/`KVDir` a `CrashNode` call closed, so
every restart in this package genuinely exercises Raft log/snapshot
recovery and KV WAL recovery, never merely resets an in-memory struct.

Real-network and Docker-level failures, and an external distributed test
framework, are explicitly out of scope for this phase -- see §17.

## 3. What was added vs. what was reused

Phase 10 reused every mechanism in the table above as-is, with two
minimal, additive extensions (no existing behavior changed; every
pre-Phase-10 test still passes unmodified):

- **`raft.InMemoryTransport.PartitionGroups`/`HealPartitions`**
  (`internal/raft/transport.go`). The pre-existing `Partition(id)`
  isolates `id` from *literally everyone*, which is sufficient for a
  minority-of-one split ("A B | C" is just `Partition("C")") but cannot
  express a split where both sides have more than one node (e.g. "A B C
  | D E": `Partition("D")` plus `Partition("E")` would incorrectly also
  cut D off from E). `PartitionGroups` assigns every node a group index
  and blocks delivery only *across* groups, layering cleanly on top of
  the existing `isolated` map with no change to its semantics.
- **`dbnode.Config.WrapPersister`** (`internal/dbnode/node.go`), the
  exact analogue of the pre-existing `Config.WrapStore`: it wraps the
  `raft.FilePersister` `Open` already constructs from `RaftDir` before
  handing it to `raft.NewNode`, letting a test inject a persistence fault
  at the composition-root level. Like `WrapStore`, production code has
  no reason to set it.

Everything else in `chaos/` is new test infrastructure built from those
two extensions plus the existing public APIs of `internal/dbnode`,
`internal/raft`, and `internal/statemachine`.

## 4. Chaos harness architecture

```
chaos/
    cluster.go      Cluster: opens/owns N real dbnode.Nodes + a shared
                     InMemoryTransport; crash/restart, partition/heal,
                     propose, consistent-get, snapshot, fault injection
    observe.go       Cluster.Observe (snapshot + log + invariant-check),
                     CurrentLeader[Among], AssertConverged
    wait.go          Bounded Wait* helpers (see §12)
    eventlog.go      EventLog / Step / NodeState / ClusterSnapshot / Dump
    invariants.go    InvariantTracker (see §9)
    faults.go        faultyPersister, faultyStore (fault injection + Put/
                     Delete counting)
    random.go        RandomPlan / NewRandomPlan / RunRandomScenario
    scenario_*_test.go   Deterministic scenarios A-J (§14)
    random_test.go   Bounded seeded randomized scenario (§13)
    helpers_test.go  Shared test helpers
```

`Cluster` is the harness's single entry point. It is deliberately **not**
safe for concurrent use -- a chaos scenario is a single sequential
narrative, matching the deterministic-scheduling requirement in §5 -- and
every state-changing method on it (`CrashNode`, `RestartNode`,
`Partition`, `Advance`, `Propose`, ...) calls `Observe` internally, so a
scenario test never has to remember to log or invariant-check a step by
hand.

## 5. Deterministic scheduler

Every method in `chaos/` that advances the cluster does so through the
same primitives Phases 5-9's own tests already use -- `Tick`, `Drain`,
`ApplyAvailable` -- and nothing in this package ever calls
`time.Sleep`. `Cluster.Advance(rounds)` is the sole timing primitive:
each round ticks every live node, drains, applies every committed entry
not yet applied, and records one `Observe` step. `Settle()` is
`Advance(2)`, mirroring `internal/dbnode/cluster_test.go`'s own
`settleCommit` (one tick/drain round commits an entry; a second is what
lets every *follower's* own `CommitIndex`, not just the leader's, catch
up -- see that function's doc comment for why one round is never
enough).

`ElectLeader(id)` ticks **only** `id`, repeatedly, past its configured
`ElectionTickMax` -- the same `electLeader` convention
`internal/dbnode/cluster_test.go` and `internal/raft/cluster_test.go`
already use -- so a scenario can deterministically control exactly which
node becomes leader. `WaitForLeader` (and the bounded `Wait*` family more
generally, see §12) instead ticks every live node uniformly and waits,
bounded, for a *natural* election to resolve -- which matters after
`CrashNode` kills the current leader and nobody is told in advance who
should replace it. Natural elections need their randomized timeouts to
actually differ across nodes, so unlike `internal/dbnode/cluster_test.go`'s
`ElectionTickMin == ElectionTickMax` convention (which only ever drives
elections by targeting one specific node), `chaos.Config` uses a real
spread (`DefaultElectionTickMin`/`Max` = 5/10) with each node getting its
own deterministic, index-seeded `*rand.Rand` -- reproducible across runs,
but spread enough that a natural election resolves within a bounded
number of rounds instead of repeating the same split vote forever.

## 6. Node crash/restart model

See §2's table. `CrashNode(id)`:

1. `transport.Partition(id)` -- cuts `id` off from every other node, in
   both directions, immediately;
2. `(*dbnode.Node).Close()` on `id` -- stops its background goroutines
   and flushes/closes its storage exactly as a graceful shutdown would.

`RestartNode(id)`:

1. `transport.Heal(id)`;
2. a fresh `dbnode.Open` against **the same** `RaftDir`/`KVDir` -- real
   `currentTerm`/`votedFor`/log recovery via `FilePersister`, real
   snapshot recovery, real KV WAL recovery, all through the exact code
   path a real process restart would use.

The restarted node always comes back a `Follower` with `CommitIndex` and
`LastApplied` reset to `0` (Raft's documented volatile-state-always-resets
behavior -- see `docs/raft/phase6-raft-persistence.md`); its term,
persisted log, and any persisted snapshot are exactly what survived.
`InvariantTracker`'s monotonicity baseline for that node's commit/applied
indexes is reset in step with it (see §9) so this expected reset is never
mistaken for a regression.

## 7. Network partition model

See §2 and §3. `Partition`/`Heal` isolate one node from everyone;
`PartitionGroups`/`HealPartitions` split the whole cluster into disjoint
groups. Both operate on the real `InMemoryTransport` every RPC in the
cluster already flows through -- a chaos scenario never "fakes" a
partition by directly manipulating a node's role or term.

One real consequence of this fidelity, worth calling out explicitly: a
node that is `Partition`'d (as opposed to `CrashNode`'d) keeps *ticking*.
Since it cannot reach anyone, it will eventually time out its own
election, bump its term, fail to get votes, and repeat -- its term climbs
the entire time it stays isolated. This is a real, expected liveness cost
of a Raft implementation without a pre-vote phase (which this codebase's
Phase 5 scope does not include), not a chaos-harness bug: healing such a
node can trigger one or more rounds of disruptive re-election among the
previously-stable nodes before a legitimate leader (one with an
up-to-date log) re-emerges. `WaitForSnapshotIndex` and the other `Wait*`
helpers are bounded generously enough to absorb this.

## 8. Persistence and storage failure model

See §2's table and §3. `faultyPersister`/`faultyStore` (`chaos/faults.go`)
wrap the real `FilePersister`/`storage.Store` a node already uses;
`Cluster.FailNextPersist`/`FailNextSnapshotPersist`/`FailNextPut`/`FailNextDelete`
arrange for exactly the next matching call to fail, after which the
wrapper returns to behaving normally. Every call either fully succeeds
against the real underlying implementation or fully fails without
touching it, so these never introduce partial writes or corruption of
their own -- they only exercise the rollback paths `internal/raft` and
`internal/dbnode` already implement for a failed `SaveState`/`SaveSnapshot`/`Put`/`Delete`
(see `(*raft.Node).Propose`, `CreateSnapshot`, and
`(*statemachine.Applier).ApplyAvailable`'s existing doc comments).

`faultyStore` additionally counts real `Put`/`Delete` calls
(`PutCount`/`DeleteCount`), generalizing `node_test.go`'s `countingStore`.
This matters specifically for deduplication scenarios (§14, Scenario G):
a replayed duplicate PUT with the *same value* as the original looks
identical to applying it twice if you only check the stored value
afterward. Counting real mutations is what actually distinguishes
"applied once" from "applied twice coincidentally producing the same
result".

## 9. Safety invariants

`InvariantTracker` (`chaos/invariants.go`) accumulates every
`NodeState` observation a scenario produces (via `Cluster.Observe`,
called automatically by almost every `Cluster` method) across the
scenario's entire history -- never only final state -- and flags:

1. **At most one leader per term.** Tracked by term -> set of node IDs
   ever observed as `Leader` at that term; a set growing past size 1 is
   a violation.
2. **Terms never decrease**, per node. Term is durably persisted and
   never reset, even across a restart.
3. **Snapshot index never decreases**, per node. Also durably persisted,
   never reset.
4. **Commit index never decreases**, per node, *within one incarnation*.
   Commit/applied indexes are volatile and intentionally reset to `0` on
   a real restart (see §6); `RestartNode` resets this tracker's baseline
   for that node at the same moment, so the expected reset is never
   itself flagged.
5. **Applied index never decreases**, per node, same incarnation rule as
   above.
6. **LastApplied never exceeds CommitIndex**, per node, every
   observation.

A node currently marked crashed is skipped entirely for every check
above: its reported values are frozen from the moment it crashed, not a
live observation.

The remaining invariants the Phase 10 brief lists --
**committed state is not lost**, **a minority cannot commit
independently**, **unsafe reads are rejected**, and **the cluster
eventually converges** -- are deliberately *not* folded into
`InvariantTracker`. Each requires knowing something scenario-specific
(what "before the fault" means, which nodes are the minority, what
"logically equivalent" means for two nodes' KV state) that a generic
tracker would have to be taught per-scenario anyway; it is simpler and
clearer for each scenario test to check them directly with `Cluster`'s
other methods (`LogicalState`, `AssertConverged`/`WaitForConvergence`,
`Node(id).Raft().CommitIndex()`, `ConsistentGet`) than to build a second,
parallel abstraction for the same checks. `Cluster.AssertInvariants()`
(backed by `InvariantTracker.Check()`) is what every scenario calls for
the four invariants above.

## 10. Convergence checks

`Cluster.LogicalState(id)` returns every live key/value pair on `id` as a
plain map -- the logical abstraction the Phase 10 brief calls for
comparing nodes by. Two nodes can have different Raft log length,
different snapshot boundaries, and different WAL contents and still
represent the exact same logical state (e.g. one has compacted past
index 100 and the other hasn't, but both apply to the identical KV
content); `AssertConverged`/`WaitForConvergence` compare *that*
abstraction, never physical log/snapshot layout.

`WaitForConvergence(maxRounds, ids...)` is the bounded, retrying version
a scenario actually wants after a fault: it advances the cluster and
re-applies on every round until every node in `ids` (or every live node,
if none given) agrees, or gives up after `maxRounds`. See §16 for the one
subtlety every convergence check after a leader change must account for.

## 11. Deduplication testing

Phase 7's deduplication table (keyed by `ClientID`, storing the latest
`RequestID` seen and its resolved `Result`) is what makes a client-level
retry safe even though Raft itself never deduplicates. Phase 9 made that
table part of the snapshot payload specifically so it survives log
compaction and `InstallSnapshot`. Scenario G (§14) exercises all three
failure conditions the brief calls out -- a leader change, a follower
restart, and a snapshot/snapshot-installation boundary -- checking real
`Put`/`Delete` call counts (§8), not just the final stored value, so an
idempotent duplicate can't hide a real double-application bug.

One genuinely interesting finding surfaced while building Scenario G
(documented in the test itself,
`TestScenarioG_DedupAcrossFollowerRestart`): a node restarted *without*
ever having taken a snapshot has **no** durable dedup table to recover --
only a snapshot persists it (§8 of `docs/raft/phase9-snapshots.md`).
What actually happens is still correct: the restarted node's
`ApplyAvailable` naturally re-executes its own still-persisted log from
`LastApplied = 0`, which re-derives an identical dedup table as a side
effect of re-running the identical deterministic command sequence. This
is not data loss and not a bug -- but it does mean a node's *first*
post-restart-without-snapshot application of an old command is a genuine
new `Put`, not a replay, which the test accounts for explicitly rather
than asserting a naive "zero new puts after restart".

## 12. Read consistency testing

Phase 10 does not build a general linearizability checker (that is
explicitly Phase 11's job -- see §17). It does check the specific unsafe-
read cases the brief calls out, directly against the existing
`ReadIndex`/`ConsistentGet` mechanism:

- A **follower** (any node that is not currently leader) always fails
  `ConsistentGet` with `raft.ErrNotLeader` / `dbnode.ErrNotLeader` --
  Phase 8.5 implements linearizable reads as leader-only by construction.
- A **minority leader** -- a node that still locally believes itself
  leader because it cannot detect its own partition, but cannot reach a
  majority to confirm it -- always fails `ConsistentGet` with
  `dbnode.ErrReadUnavailable`, because `ReadIndex`'s majority-confirmation
  round, not the node's own role check, is what catches this (see
  Scenario E, §14, and `TestReadConsistency_MinorityLeaderConsistentGetFailsSafely`).
- A **stale leader that has since lost leadership** behaves identically
  to the minority-leader case from this harness's point of view: both
  are caught by the same majority-confirmation failure, since the
  harness's synchronous, in-process RPC delivery has no way to interleave
  a leadership change strictly *during* a single `ReadIndex` round (doing
  so would start to overlap with Phase 11's own scope).

## 13. Randomized chaos and seed reproduction

`chaos/random.go` implements the bounded, seeded randomized scenario the
brief asks for, added only after every deterministic scenario already
passed, never in place of them. `NewRandomPlan(seed, ids, numSteps)`
generates a fixed-length `*RandomPlan` from a `*rand.Rand` seeded by
`seed` alone -- the same seed, cluster IDs, and step count always produce
the exact same plan, deterministically, with no other source of
randomness anywhere in `RunRandomScenario`. Actions are one of: `Put`,
`Delete`, `Get` (via `ConsistentGet`), `Crash`, `Restart`, `Partition`,
`Heal`, `Snapshot` -- exactly the set §25/§37 of the brief lists -- with
an invalid action for the current state (e.g. `Restart` on a node that
isn't crashed) logged as an explicit no-op rather than silently skipped.

`TestRandomChaos_BoundedSeededScenario` runs a 5-node cluster for 60
steps with a fixed default seed (`42`), overridable via the `CHAOS_SEED`
environment variable for local reproduction of a specific run. After the
plan finishes, it heals every partition, restarts every crashed node,
elects a leader, **proposes one new command** (required by Raft's
current-term commit rule before any older-term entries can commit
retroactively -- discovered the hard way while hardening this exact test,
see the test's own comment), and only then waits for convergence and
checks every invariant. `cmd/forge-chaos` (§13 continued, below) runs the
identical plan generator and reporting logic from the command line with
a configurable seed and step count, for exploring a specific seed or a
longer run outside the ordinary `go test` suite.

Reproducing a specific failure never requires re-deriving the seed:
`EventLog.Dump` (via `Cluster.Dump`) prints the seed a run was built from
directly in its header, alongside the scenario name and the full step-by-
step action/state sequence -- pasting that seed back into `CHAOS_SEED` or
`forge-chaos -seed` regenerates the identical plan.

## 14. Scenario catalog

Every scenario below is a deterministic Go test in `chaos/scenario_*_test.go`.
None rely on `time.Sleep`; every bounded wait uses the `Wait*` helpers
(§5, §12).

| Scenario | File | What it proves |
|---|---|---|
| A: leader crash | `scenario_crash_test.go` | Committed data survives a leader crash; a different node takes over; new writes keep committing; the restarted old leader never resumes stale authority and eventually converges |
| B: minority isolation | `scenario_partition_test.go` | A 2-of-3 majority keeps committing; the isolated minority node cannot advance its own `CommitIndex` or elect itself; healing converges |
| C: follower lag + snapshot catch-up | `scenario_snapshot_test.go` | A follower lagging far enough that ordinary replication can't catch it up receives and installs a real `InstallSnapshot`, and every key (not just indexes) is correct afterward |
| D: repeated leader changes | `scenario_crash_test.go` | Terms increase monotonically across several crash-and-reelect cycles; every committed write survives every subsequent crash; the cluster converges once every node is restarted and healed |
| E: stale leader | `scenario_partition_test.go` | A leader partitioned from the majority cannot commit or read safely despite still locally believing itself leader; it steps down and converges once healed |
| F: snapshot + restart | `scenario_snapshot_test.go` | A snapshot boundary plus a retained log suffix reconstruct the correct state after a crash/restart, for both a leader and a follower |
| G: dedup across failure | `scenario_dedup_test.go` | A retried identical `(ClientID, RequestID)` never double-applies, across a leader change, a follower restart, and a snapshot-installation boundary (see §11) |
| H: partition/heal cycle | `scenario_partition_test.go` | Three successive partition/heal cycles (isolating a different node each time) accumulate no hidden corruption; final state converges |
| I: persistence/storage failure | `scenario_fault_test.go` | A failed `SaveState`/`SaveSnapshot` rolls back cleanly and the node remains usable; a failed `Put` halts application without advancing `LastApplied`, without affecting Raft's own `CommitIndex`, and a retry completes it |
| J: full cluster restart | `scenario_crash_test.go` | Every node crashing and restarting together recovers KV and snapshot state from disk before any Raft activity, then resumes normal operation |

Two extra focused tests (`TestReadConsistency_*` in `scenario_fault_test.go`)
cover §12's read-consistency cases directly.

## 15. Failure diagnostics

Every `Cluster`-mutating call records one `EventLog.Step`: a step number,
the action taken, and every live node's role/term/leader/commit/applied/
snapshot/last-log-index, plus any active partition groups or isolated
nodes. `Cluster.Dump()` renders the full sequence in the format:

```
CHAOS FAILURE
scenario: TestScenarioE_StaleLeader
seed: 0
total steps: 17

step: 17
action: Crash(node2)
cluster:
  node0 role=Leader term=8 commit=42 applied=42 snapshot=30 lastlog=42 [running]
  node1 role=Follower term=8 commit=40 applied=40 snapshot=30 lastlog=42 [running]
  node2 role=Follower term=8 commit=42 applied=42 snapshot=30 lastlog=42 [crashed]
```

Every scenario test passes this to `t.Fatalf` on any assertion failure,
so a CI failure is diagnosable, and (for the randomized scenario) exactly
reproducible, from the test output alone.

## 16. Known limitations

- **Raft's current-term commit rule applies to convergence checks after
  a leader change.** A newly (or naturally re-) elected leader can only
  commit an entry from an earlier term as a side effect of committing
  one of its own current term. Any scenario that waits for convergence
  after a sequence of crashes/elections without ever proposing something
  new under the final leader's term can get stuck waiting forever on
  entries that are real and durable but, by Raft's own design, not yet
  committable -- this is not a chaos-harness bug, but it is a sharp edge
  every scenario (and the randomized one, explicitly) has to account for
  with one closing proposal. See §13 and `TestNode_FullClusterRestart_ConvergesToRecoveredState`
  in `internal/dbnode/node_test.go` for the pre-existing documentation of
  the same rule.
- **A `Partition`'d (not crashed) node keeps ticking and can inflate its
  own term while isolated**, since this codebase's Phase 5 scope does
  not include a pre-vote phase. Healing such a node can cost a few extra
  rounds of disruptive re-election; every bounded wait in this package
  budgets generously for that, but a scenario with an unusually long
  isolation period combined with a tight custom bound could still need a
  larger one. See §7.
- **No real network or Docker-level failures.** Everything here runs
  in-process against `InMemoryTransport`; a real-network transport
  (out of this phase's scope -- see `internal/raft`'s own package doc)
  would need its own chaos coverage for the failure modes unique to it
  (actual packet loss, actual latency, actual process isolation).
- **The randomized scenario is a coverage net, not a proof.** It
  increases confidence that the deterministic scenarios haven't missed
  an interaction, but a passing run at one seed says nothing about every
  other seed; it is deliberately bounded (step count, cluster size) to
  stay fast enough to run routinely rather than exhaustively.

## 17. What Phase 10 proves

- Every safety invariant in §9 holds across every deterministic scenario
  in §14 and across every randomized run checked so far.
- Committed data survives leader crashes, repeated leader changes, node
  restarts, and partition/heal cycles.
- A minority partition can never advance its own committed history or
  serve a safe linearizable read; a stale leader can never either, and
  always eventually steps down and converges once healed.
- A lagging follower actually exercises `InstallSnapshot` under a real
  failure sequence (not just the direct unit tests already in
  `internal/raft`/`internal/dbnode`) and converges to correct
  application-level state, not just matching indexes.
- Deduplication survives a leader change, a follower restart, and a
  snapshot-installation boundary.
- A failed Raft persistence write or a failed KV storage write rolls
  back cleanly, never advances state past what is actually durable, and
  never corrupts Raft consensus itself.
- The cluster eventually converges to identical logical KV state after
  every scenario and every randomized run checked, once failures stop
  and (per §16) at least one new command commits under the final term.

## 18. What Phase 10 intentionally does not prove

- **A formal linearizability guarantee.** Phase 10 checks the specific
  unsafe-read cases in §12 directly against `ConsistentGet`/`ReadIndex`;
  it does not record full operation histories, does not build a
  Herlihy/Wing-style checker, and does not attempt real-time interval
  analysis or exhaustive history search. That is Phase 11's explicit
  scope.
- **Exhaustive coverage of every possible failure interleaving.** The
  deterministic scenarios cover the specific sequences in §14; the
  randomized scenario samples a bounded space of additional sequences at
  a fixed seed set. Neither is a model checker, and neither claims to
  have explored every reachable state.
- **Real-network or process-level failure modes** (actual dropped
  packets, actual latency, an actual separate OS process crashing) -- see
  §16.
- **Anything about SSTables or storage-engine compaction (Phase 4)
  beyond their existing interaction with Raft log compaction.** Phase 10
  treats `storage.Store` as a boundary to inject faults at (§8); it does
  not test SSTable/compaction correctness itself, which remains that
  phase's own test suite's responsibility.
