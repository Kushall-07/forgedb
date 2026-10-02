# ForgeDB — Phase 8.5: Read Consistency

## Scope

Phase 8.5 defines and implements the consistency semantics of `GET`
operations before snapshots, chaos testing, and linearizability testing are
introduced in later phases. It adds one new read path,
`(*dbnode.Node).ConsistentGet`, backed by a new Raft-level primitive,
`(*raft.Node).ReadIndex`, and a small apply-barrier wait
(`(*dbnode.Node).waitForApplied`, backed by a new `(*raft.Node).AppliedCh`
notification). It does **not** add snapshots, log compaction, a real
network transport, cluster membership changes, observability, a chaos
framework, a linearizability checker, lease-based reads, or follower
reads — see §14.

Phase 8.5 touches four existing files, all additively:

- `internal/raft/replication.go`: `sendAppendEntries` now returns a `bool`
  reporting whether the round trip confirms this node's leadership at the
  term it was sent for — the exact signal `ReadIndex`'s quorum
  confirmation needs. Every existing call site (`broadcastAppendEntriesLocked`)
  simply ignores the new return value; no existing behavior changed.
- `internal/raft/apply.go`: `MarkApplied` now also notifies a new
  `appliedCh`, mirroring the existing `commitCh`/`notifyCommitLocked`
  pattern exactly. A new `AppliedCh()` accessor exposes it.
- `internal/raft/raft.go`: `Node` gained one new field, `appliedCh`,
  initialized in `NewNode` exactly like `commitCh`.
- `internal/dbnode/node.go`: gained `ErrReadUnavailable`, `ConsistentGet`,
  and the unexported `waitForApplied` helper.

Two new files were added: `internal/raft/read.go` (`ReadIndex` and
`ErrReadBarrierUnavailable`) and the two test files described in §12.
Nothing in `internal/storage` changed at all — storage remains completely
ignorant of Raft, consensus, and read consistency, exactly as every prior
phase's boundary requires.

## 1. Why local reads are unsafe

Every phase through Phase 8 built a correct, replicated *write* path:
`Propose` → majority commit → `Applier` → `KVStateMachine` → `Store`. Phase
8's own documentation (`docs/raft/phase8-raft-storage-integration.md` §15)
explicitly left reads unspecified: every Phase 8 test that needed to check
KV state called a node's `Store().Get` directly, which is fine for *test
assertions* but is not a specification of what a *client* `GET` is allowed
to return.

A naive implementation would do exactly that — `GET(key) → storage.Get(key)`
on whichever node receives the request — and it is unsafe in at least two
distinct ways:

**A follower can be behind.** `HandleAppendEntries` only advances a
follower's own `commitIndex` (and the `Applier` only applies entries) up to
whatever that follower has itself received and matched so far. A write the
leader considers committed may not have reached, or been applied by, every
follower yet. A follower-local `Get` can therefore return a value older
than the true, agreed-upon state of the cluster.

**A leader's local role is not proof of current leadership.** A node whose
`role` field says `Leader` believes this about itself, but belief is not
the same as a currently-valid fact. Consider:

```text
Node A = old leader, partitioned away from the rest of the cluster

        network partition
    A        |        B   C
 minority    |      majority
```

`A.role == Leader` remains true in `A`'s own memory indefinitely — nothing
tells `A` it has been superseded until it actually hears from the new
leader (or a peer with a higher term) again. Meanwhile `B` and `C`, still a
majority, can elect a new leader and commit new writes that `A` never sees.
If `A` served reads purely because `A.role == Leader`, it could return
state that is not just behind, but actively superseded by writes the rest
of the cluster has already agreed on and `A` doesn't even know exist.

**Committed is not applied.** Even on a node that is genuinely still
leader, `commitIndex` and `lastApplied` are two different numbers by
design (Phase 6/7's own documented distinction): an entry can be committed
in Raft's log before the local `Applier` has actually run it against
`storage.Store`. Reading storage before `lastApplied` has caught up to the
point a read needs to observe is unsafe for exactly the same reason a
follower read is — the state a client is about to see does not yet reflect
everything that already happened.

Phase 8.5 exists to close all three gaps with one mechanism.

## 2. Chosen consistency model: linearizable, leader-only reads

ForgeDB's `GET`, via `ConsistentGet`, provides **linearizability**: a
successful `ConsistentGet` call never returns a value that is stale
relative to any write that had already completed (committed) at the moment
the call was made. Concretely, once a `PUT`/`DELETE` has been proposed,
replicated to a majority, and reported committed, every subsequent
`ConsistentGet` — on the current leader, from that point forward —
reflects it or a later write, never an earlier state.

This is implemented as **leader-only reads with per-call quorum
confirmation** — a minimal ReadIndex-style protocol built directly from
existing Phase 5–8 primitives rather than a new RPC type:

```text
ConsistentGet(key)
      |
      v
raft.ReadIndex()              -- establish a quorum-confirmed read barrier
      |                          (leader-only; reconfirms leadership fresh,
      |                           every call -- see §3)
      v
waitForApplied(readIndex)     -- block (event-driven) until this node's own
      |                          Applier has applied through readIndex
      |                          (see §5)
      v
storage.Get(key)              -- only now read local state
```

### Why this model, for ForgeDB specifically

- **It reuses, rather than duplicates, the quorum machinery Raft already
  has.** `ReadIndex`'s confirmation round is literally the same
  `AppendEntries` RPC and the same `majority()` calculation that commit
  already uses (see §3) — no second heartbeat protocol, no new RPC type,
  no new wire format.
- **It needs no persisted lease, clock synchronization, or bounded-clock-skew
  assumption.** A lease-read design (the leader unilaterally serves reads
  for a bounded time after its last confirmed heartbeat, trusting its own
  clock) is explicitly listed as something to avoid "unless absolutely
  required" — it isn't required here, and ForgeDB's `InMemoryTransport` and
  file-backed persistence give no reason to introduce real-time
  assumptions into a package that has deliberately stayed free of them
  since Phase 5.
- **It fits the existing concurrency discipline exactly.** `ReadIndex`
  follows the same lock → capture state → unlock → RPC → reacquire →
  validate pattern every other RPC-issuing method in `internal/raft`
  already uses (`startElectionLocked`, `broadcastAppendEntriesLocked`). No
  new synchronization primitive was invented; `AppliedCh` is a direct,
  one-line copy of the existing `commitCh` pattern.
- **It is the smallest change that gives every one of the required
  properties (§7 of the phase brief) a real mechanism**, rather than an
  approximation: a follower truly cannot produce a linearizable read (it
  has no path to one at all — see §4); a leader truly cannot produce one
  without fresh majority confirmation (§3); a read truly cannot observe
  unapplied state (§5).

## 3. The read barrier: `(*raft.Node).ReadIndex`

`ReadIndex` (`internal/raft/read.go`) is the Raft-level primitive:

```go
func (n *Node) ReadIndex() (uint64, error)
```

On a node that is not currently leader, it returns `ErrNotLeader`
immediately — see §4.

On a leader, it:

1. Under `n.mu`, captures `term := n.currentTerm`, `index := n.commitIndex`,
   and builds one `AppendEntriesArgs` per peer from the leader's own
   current `nextIndex`/log state — exactly the same construction
   `broadcastAppendEntriesLocked` already performs for ordinary heartbeats.
   Then it unlocks.
2. Sends that round to every peer via `trackRPC` (the same mechanism every
   other outbound RPC in this package uses, so `Drain()` still waits on it
   correctly) and waits, outside the lock, for every reply.
3. Each reply is processed by the *existing* `sendAppendEntries` — now
   returning `true`/`false` to report whether that peer's reply counts as
   an acknowledgment of this node's leadership at `term`. A reply counts
   as an acknowledgment if it was delivered at all, carried no higher
   term, and this node is still leader at `term` when the reply is
   processed; it does **not** need `Success == true` — a log-matching
   mismatch still proves the peer recognizes this leader's term, it just
   also needs more log history (which the ordinary replication path
   handles separately and unconditionally, as it always has).
4. Once every reply (or transport failure) has been accounted for,
   `ReadIndex` reacquires `n.mu` one last time to check that this node is
   *still* leader at `term` — covering the case where a reply mid-round
   already forced a step-down. If a majority (via the existing
   `majority()`, never duplicated) acknowledged and this node is still
   current, it returns `index`. Otherwise it returns
   `ErrReadBarrierUnavailable`.

A single-node cluster (`len(n.peers) == 0`, so `majority() == 1`) needs no
RPC round at all: the leader's own vote already is the whole quorum, and
`ReadIndex` returns `index` immediately.

### Why `index := commitIndex` captured *before* the RPC round is safe

The read index is deliberately the leader's `commitIndex` at the moment
`ReadIndex` was called, not anything the confirmation round itself
advances further. This is safe because `commitIndex` on a leader is
monotonically non-decreasing for as long as it remains leader at the same
term (`maybeAdvanceCommitIndexLocked` only ever raises it), and the
confirmation round's entire purpose is to prove that this node *was*
(and, as of the final check, still is) leader throughout the round trip.
If it was leader the whole time, nothing committed under a *different*
leader could have been missed — any entry this leader didn't yet know
about when it captured `index` is, by definition, not yet reflected in
this call's answer, exactly as a linearizable read that started before
such a write is entitled to not reflect it yet.

### Higher-term discovery during the round

If any peer's reply carries a term higher than this node's own, the
existing `sendAppendEntries` step-down path (`becomeFollowerLocked`,
unchanged by this phase) runs exactly as it already does for an ordinary
heartbeat reply — this is the same mechanism, not a new one.
`TestNode_ReadIndex_StepsDownOnHigherTermDuringConfirmation` proves this
directly: a leader partitioned away while a new, higher-term leader is
elected by the remaining majority steps down the moment its own
`ReadIndex` call (not a heartbeat) reaches that new leader, and
`ReadIndex` correctly reports `ErrReadBarrierUnavailable` rather than the
stale index it had captured. One caveat, inherited unchanged from
`becomeFollowerLocked`: a step-down triggered by a *reply* (as opposed to
an incoming `AppendEntries` *request*, which carries `LeaderID`) clears
`leaderID` to `""`, since a bare reply never identifies who the new leader
actually is — this node learns that the next time the new leader's own
heartbeat reaches it, exactly as before this phase.

### Current-term commit rule is untouched

`ReadIndex` never calls `maybeAdvanceCommitIndexLocked` with different
logic, never bypasses the current-term restriction (§5.4.2), and never
advances `commitIndex` on its own — `sendAppendEntries`'s existing call to
`maybeAdvanceCommitIndexLocked` on a successful reply is the *only* place
`commitIndex` ever changes, completely unmodified by this phase.

## 4. Why follower reads are rejected, not forwarded

`ReadIndex` (and therefore `ConsistentGet`) returns `ErrNotLeader` for any
non-leader node, full stop. Phase 8.5 deliberately does not implement
follower-forwarded reads (a follower asking the leader for a read index on
the client's behalf) — the phase brief explicitly allows "`Follower GET` →
`not_leader`" as acceptable API semantics, and building a safe
follower-forwarding protocol would require a new RPC path and additional
leader-identification plumbing for no benefit this phase's test matrix
needs. `TestNode_ConsistentGet_FollowerRejected` proves this holds even
when the follower's local storage already happens to hold the correct,
fully-applied value — correctness of the *answer* is never the test;
whether the read path can *prove* it, is.

## 5. The apply barrier: committed vs. applied

`ReadIndex` only proves a Raft-level fact (this index is safe to read
*once applied*); it says nothing about whether this node's own state
machine has actually caught up to it yet. `ConsistentGet`
(`internal/dbnode/node.go`) connects the two:

```go
func (n *Node) ConsistentGet(ctx context.Context, key []byte) ([]byte, error) {
	readIndex, err := n.raft.ReadIndex()
	if err != nil {
		return nil, err
	}
	if err := n.waitForApplied(ctx, readIndex); err != nil {
		return nil, fmt.Errorf("dbnode: wait for state machine to apply through read index %d: %w", readIndex, err)
	}
	return n.store.Get(key)
}
```

`waitForApplied` is event-driven, not a busy-loop and not a `time.Sleep`:

```go
func (n *Node) waitForApplied(ctx context.Context, target uint64) error {
	for {
		if n.raft.LastApplied() >= target {
			return nil
		}
		select {
		case <-n.raft.AppliedCh():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
```

`AppliedCh` (`internal/raft/apply.go`) is a new one-slot notification
channel, filled by `MarkApplied` every time `lastApplied` advances —
written as a direct, intentional mirror of the pre-existing `commitCh`/
`notifyCommitLocked`/`CommitCh` pattern `internal/raft` already uses for
commit notifications. Like `commitCh`, a send can be coalesced or missed by
a slow reader; this is never a correctness problem because `LastApplied()`,
not the channel, is what `waitForApplied` actually checks — the channel is
purely a wake-up hint to avoid polling, exactly as documented on
`CommitCh`.

`TestNode_ConsistentGet_WaitsForApply` proves the barrier is real, not
assumed: it commits an entry, deliberately never calls `ApplyAvailable`,
and shows `ConsistentGet` with a bounded `context.Context` times out
(`context.DeadlineExceeded`) rather than ever reading `storage.Get` early
— then, with the exact same un-applied state, calling `ApplyAvailable`
once and retrying `ConsistentGet` (with `context.Background()`) succeeds
immediately and returns the correct value.

### Why `ctx` only here, not elsewhere in this phase

`ConsistentGet` is the only new API in this phase that takes a
`context.Context`, and deliberately so: `ReadIndex`'s own RPC round (§3)
follows the rest of `internal/raft`'s existing, context-free convention
(`Propose`, `Tick`, `HandleAppendEntries`, …), and is bounded in practice
because every call this phase's tests make goes through
`InMemoryTransport`, whose sends either return immediately or fail
immediately with `ErrPeerUnreachable` — there is no real blocking network
call anywhere yet for a timeout to meaningfully interrupt. The apply wait,
by contrast, can genuinely block indefinitely in production (an
`Applier.Run` background goroutine that is stalled, or simply has not
caught up yet under load) — this is new, user-facing surface where the
phase brief's request for "timeout/cancellation if the current
architecture supports context" has a real, correct answer: thread a
`context.Context` through exactly that one wait, without retrofitting
context onto the rest of `internal/raft`'s existing, unrelated API.

## 6. Failure semantics

`ConsistentGet` returns exactly four distinguishable outcomes:

| Outcome | Error | Meaning |
|---|---|---|
| Not leader | `dbnode.ErrNotLeader` (`= raft.ErrNotLeader`) | This node cannot serve a linearizable read at all; a client must find the actual leader. |
| Quorum unavailable | `dbnode.ErrReadUnavailable` (`= raft.ErrReadBarrierUnavailable`) | This node believes it is leader, but could not freshly confirm it with a majority (e.g. minority partition, or leadership changed mid-confirmation). |
| Apply wait cancelled/timed out | wrapped `ctx.Err()` | The read barrier was established, but this node's own state machine did not catch up to it before `ctx` ended. |
| Key not found | `storage.ErrKeyNotFound` | The read barrier was fully satisfied; the key genuinely has no live value. |

The first three all mean **"the read could not safely be performed"**; the
fourth means **"the read was performed, successfully, and the key does not
exist."** A caller distinguishes them with ordinary `errors.Is` checks —
no new overlapping error type was introduced, and every error re-exports an
existing one (`ErrNotLeader` already existed; `ErrReadUnavailable` is a
one-line re-export of the one new sentinel, `ErrReadBarrierUnavailable`,
this phase adds) rather than growing a parallel error hierarchy.

## 7. Concurrency

`ReadIndex` and `ConsistentGet` are both safe under concurrent use, and
neither holds a Raft mutex during any network RPC:

- `ReadIndex` captures the state it needs for the RPC round under `n.mu`,
  releases it, performs every RPC via the existing `trackRPC`/goroutine
  mechanism, and only reacquires `n.mu` afterward to validate the outcome
  — the same discipline every other RPC-issuing method in this package has
  followed since Phase 5. `TestNode_ReadIndex_DoesNotHoldMutexDuringRPC`
  proves this directly with a transport that blocks one peer's reply
  indefinitely, confirming a concurrent `State()` call still returns
  immediately.
- Multiple concurrent `ReadIndex` (or `ConsistentGet`) calls each run their
  own independent confirmation round and apply-wait; nothing shared
  between them is mutated unsafely, since every read of `commitIndex`,
  `lastApplied`, and peer state goes through the node's existing,
  already-synchronized accessors. `TestNode_ReadIndex_ConcurrentCalls` and
  `TestNode_ConsistentGet_ConcurrentGets` run eight concurrent calls each
  and verify every one returns the identical, correct result.
- A `GET` racing a concurrent `ApplyAvailable`/`Run` apply cycle is
  resolved correctly regardless of interleaving by the apply barrier
  itself (§5): if apply finishes first, `waitForApplied` never blocks at
  all; if apply finishes later, the `AppliedCh` notification wakes the
  waiter. `TestNode_ConsistentGet_RacesApplyAvailable` exercises both
  orderings without relying on a particular schedule.
- `storage.Store`'s concrete implementation, `MemStore`, was already safe
  for concurrent `Get`/`Put`/`Delete` before this phase (its `MemTable`
  uses a `sync.RWMutex` internally) — `ConsistentGet`'s final
  `n.store.Get(key)` call needed no new synchronization of its own.

## 8. Test coverage

### `internal/raft` (`read_test.go`)

- `TestNode_ReadIndex_SucceedsWithHealthyMajority` — normal leader read
  barrier after a majority-committed write (Case 1/3).
- `TestNode_ReadIndex_NotLeader` — a follower's `ReadIndex` fails
  immediately (Property A).
- `TestNode_ReadIndex_FailsWithoutMajority` /
  `..._FiveNodes` — an isolated leader (still locally `role == Leader`)
  cannot confirm quorum and gets `ErrReadBarrierUnavailable`, never a
  stale index (Property E / Case 4); confirms the failure does not itself
  corrupt local role state.
- `TestNode_ReadIndex_StepsDownOnHigherTermDuringConfirmation` — a
  partitioned former leader's own `ReadIndex` call is what discovers a
  new, higher-term leader elected in its absence and forces the
  step-down, proving Property D/B together (§3).
- `TestNode_ReadIndex_DoesNotHoldMutexDuringRPC` — proves the
  lock/RPC/reacquire discipline directly with a deliberately blocked
  transport call.
- `TestNode_ReadIndex_ConcurrentCalls` — eight concurrent `ReadIndex`
  calls, all consistent.
- `TestNode_ReadIndex_SingleNodeCluster` — the zero-peer shortcut path.

### `internal/dbnode` (`consistent_get_test.go`)

- `TestNode_ConsistentGet_LeaderReadsCommittedValue` (Case 1/3)
- `TestNode_ConsistentGet_FollowerRejected` (Case 2 / Property A)
- `TestNode_ConsistentGet_IsolatedLeaderFails` (Case 4 / Property E)
- `TestNode_ConsistentGet_WaitsForApply` (Case 6 / Property C)
- `TestNode_ConsistentGet_MissingKeyAfterBarrier` (Case 7)
- `TestNode_ConsistentGet_PutThenGet` / `..._DeleteThenGet` (Case 8/9)
- `TestNode_ConsistentGet_ConcurrentGets` / `..._RacesApplyAvailable`
  (concurrency requirements)
- `TestNode_ConsistentGet_LeaderFailoverThenNewLeaderServesGet` (Case 5 /
  Property D)
- `TestNode_ConsistentGet_NoStaleReadAfterCommittedWrite`

All pre-existing `internal/raft`, `internal/statemachine`, and
`internal/dbnode` tests pass unmodified alongside these.

## 9. Validation

```powershell
gofmt -w .                                        # clean
go vet ./...                                      # clean
go build ./...                                    # clean
go test ./...                                     # all packages pass
go test ./internal/raft/... -count=30             # pass
go test ./internal/dbnode/... -count=30           # pass
go test ./internal/statemachine/... -count=30     # pass
go build ./cmd/forgedb                            # clean
go run ./cmd/forgedb                              # unchanged Phase 2 demo still runs end to end
```

`go test -race ./...` still fails with `-race requires cgo; enable cgo by
setting CGO_ENABLED=1` — the same pre-existing Windows/no-C-compiler
limitation already documented in `docs/raft/phase6-raft-persistence.md`,
`docs/raft/phase7-state-machine.md`, and
`docs/raft/phase8-raft-storage-integration.md`, not something this phase
introduces or could resolve.

## 10. What guarantees `ConsistentGet` provides

- A successful `ConsistentGet` reflects every write that had already
  committed, cluster-wide, before the call was made — it never returns a
  value staler than that.
- A `ConsistentGet` that returns `storage.ErrKeyNotFound` has genuinely
  confirmed, via a fresh majority quorum, that no live value exists for
  that key as of a point no earlier than when the call was made.
- Every one of `ErrNotLeader`, `ErrReadUnavailable`, and a wrapped
  `ctx.Err()` means the read was correctly refused rather than silently
  answered with unconfirmed or stale state.

## 11. What `ConsistentGet` does *not* guarantee

- **No follower reads, of any staleness, at all.** A follower cannot serve
  `ConsistentGet` even for its own already-applied, happens-to-be-correct
  local state (§4). This is a deliberate availability/safety trade-off,
  not an oversight.
- **No bound on how long a call takes.** `ReadIndex`'s confirmation round
  has no timeout of its own (§5's "why `ctx` only here" explains why); a
  real network transport with slow or hanging peers could make
  `ReadIndex` itself take arbitrarily long. This is a known limitation,
  not a correctness problem, under the only transport this phase (or any
  phase through 8.5) actually ships: `InMemoryTransport`, whose sends never
  block indefinitely.
- **No read-your-own-writes session guarantee beyond ordinary
  linearizability.** ForgeDB has no client session or sticky-leader
  concept yet; a client that writes through one node and then reads
  through `ConsistentGet` on a *different* node that is not currently
  leader simply gets `ErrNotLeader`, not a redirect or a forwarded read.
- **No snapshot-consistent multi-key read.** `ConsistentGet` reads exactly
  one key per call; nothing here provides a consistent multi-key snapshot
  view (that would require, at minimum, holding one read index across
  several `storage.Get` calls, which this phase does not build since
  nothing in the current API surface needs it yet).
- **No exactly-once read-side deduplication.** Deduplication remains
  exactly where Phase 7 left it — the write path's `(ClientID,
  RequestID)` table in `KVStateMachine`. Reads have no request identity or
  dedup concept, nor do they need one (a `GET` is naturally idempotent).

## 12. Known limitations

- `ReadIndex`'s confirmation round assumes a transport whose sends
  complete promptly (succeed or fail) — true of every transport this
  codebase has (`InMemoryTransport`), but worth calling out explicitly for
  whichever future phase adds a real network transport: that phase should
  reconsider whether `ReadIndex` needs its own bounded timeout once a send
  can genuinely hang.
- As noted in §3, a step-down triggered by a stale-leader's own `ReadIndex`
  round trip (via a reply, not a request) clears `leaderID` to `""` rather
  than learning the new leader's identity immediately — an existing
  `becomeFollowerLocked` behavior, unchanged by and not a defect of this
  phase, but a caller relying on `State()`'s `leaderID` immediately after a
  failed `ConsistentGet` should be aware it may be momentarily empty.
- No HTTP/gRPC client API exists yet for `ConsistentGet` — it is an
  internal Go API on `dbnode.Node`, exactly matching the phase brief's
  explicit instruction that a client-facing API is out of scope here.

## 13. Why this design is suitable for later Phase 11 correctness testing

`ConsistentGet`'s contract (§10/§11) is stated precisely enough —
linearizable relative to committed writes, leader-only, with explicit,
distinguishable failure modes — to be the exact thing a future
linearizability checker (Phase 11) would verify against: feed it a
history of `ConsistentGet`/`Propose` calls across a simulated, randomly
partitioned cluster, and check that every successful `ConsistentGet`
result is consistent with *some* valid linearization of the committed
writes. Because `ReadIndex` performs a genuine, fresh quorum confirmation
on every single call rather than caching a belief about leadership, Phase
11's chaos/fault-injection work can partition and heal the cluster
arbitrarily between calls without needing to also account for a stale
lease or cached confirmation inside this phase's read path — every
`ConsistentGet` call is independently and fully re-verified, which is
exactly the property a correctness checker needs to reason about one call
at a time.

## 14. What Phase 8.5 intentionally does not solve

Per the phase boundary, Phase 8.5 does **not** include:

- **Snapshots or Raft log compaction.**
- **A real network transport** (gRPC, HTTP, TCP) — `ReadIndex` is built
  entirely on the existing `Transport` interface and `InMemoryTransport`.
- **Cluster membership changes.**
- **Observability, metrics, dashboards, or benchmarking.**
- **A chaos framework or random fault-injection framework** — tests use
  the existing `InMemoryTransport.Partition`/`Heal` directly, exactly as
  Phase 8's own test matrix already did.
- **A linearizability checker** — that is explicitly Phase 11's job (§13).
- **Lease-based or speculative reads** — not needed; see §2's rationale.
- **Follower reads of any kind** — see §4.
- **A new consensus algorithm, or any change to Raft's safety rules** —
  the current-term commit rule, term monotonicity, and the vote-granting
  rules are entirely unchanged; `ReadIndex` only *reads* existing state and
  reuses the existing `AppendEntries` RPC and step-down path.
- **Any change to the KV WAL/Raft persistence boundary** — `ConsistentGet`
  never touches `raft.FilePersister` or introduces any new persisted
  state; `AppliedCh`, like `commitCh` before it, is purely in-memory and
  intentionally not persisted, consistent with Phase 6/7's existing
  documented decision that `lastApplied` itself is never persisted.
