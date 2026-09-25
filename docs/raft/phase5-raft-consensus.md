# ForgeDB — Phase 5: Standalone Raft Consensus

## Scope

Phase 5 adds a **Raft consensus module** (`internal/raft`): the algorithm
by which a cluster of ForgeDB nodes will eventually agree on a single,
ordered log of commands, even when some nodes crash, restart, or
temporarily lose contact with each other. It is built and tested as a
standalone subsystem, driven by an in-memory `InMemoryTransport`
(`internal/raft/transport.go`) that simulates a network deterministically
enough for tests to exercise elections, replication, conflicts, and
network partitions without any real sleeping or real sockets.

Phase 5 does **not** connect Raft to anything else in ForgeDB. It does not
persist its state to disk (that is Phase 6), it does not apply committed
commands to the storage engine (Phase 7), and it does not talk gRPC or
HTTP to a real network (a later integration phase). Everything here lives
and is tested entirely within `internal/raft`.

## Why ForgeDB needs Raft

A single-node key-value store, which is everything Phases 0-4 built, has
an obvious weakness: if that one machine goes down, the database is
unavailable, and if its disk is lost, the data is gone. A distributed
database survives this by keeping copies of the data on several machines
(replicas). But replication introduces a new, harder problem: **if a
client can write to any replica, how does every replica agree on what
happened, and in what order?**

```text
Client writes X=1 to replica A
Client writes X=2 to replica B
                              ↓
              What is X now, on every replica?
```

Without a coordination mechanism, different replicas can disagree about
the order of writes, or about which writes happened at all -- especially
once you add crashes and network delays into the picture. **Consensus**
is the general problem of getting a group of machines to agree on a
single value (or, as here, a single ordered sequence of values) despite
some of them failing. Raft is a consensus *algorithm*: a precise
step-by-step protocol that solves this problem correctly, as long as more
than half the cluster is up and able to talk to each other.

## What problem consensus solves

Concretely, Raft's job is to produce **one agreed-upon, ordered log of
commands** that every node in the cluster will eventually have an
identical copy of, even though:

- only one node (the **leader**) accepts new commands at a time,
- nodes can crash and come back,
- messages between nodes can be delayed, lost, or arrive out of order,
- the network can partition into groups that can't talk to each other.

Raft guarantees that as long as a *majority* of the cluster is alive and
able to communicate, the cluster keeps making progress, and every command
that Raft reports as **committed** will never be lost or reordered by any
future leader -- no matter what combination of crashes and partitions
happens afterward. That guarantee is the entire foundation ForgeDB's
future distributed behavior will be built on.

## Follower, Candidate, Leader

Every Raft node is always in exactly one of three roles:

```text
             election timeout
 Follower ─────────────────────→ Candidate
    ↑                                │
    │                                │ wins majority of votes
    │  higher term discovered        ↓
    └──────────────────────────── Leader
```

- **Follower** is the default, passive role. A follower does nothing on
  its own; it only responds to RPCs from a candidate or leader, and
  starts an election if it stops hearing from a leader for too long.
- **Candidate** is a temporary role a follower enters when it decides an
  election is needed. A candidate votes for itself and asks every other
  node for its vote.
- **Leader** is the role a candidate reaches once it wins a majority of
  votes. Only the leader accepts new commands from clients and drives log
  replication; it keeps its authority by sending periodic heartbeats.

A node can only ever be in one role at a time -- `internal/raft/raft.go`
represents this directly as a single `Role` field (`Follower`,
`Candidate`, or `Leader`) on `Node`, never as independent flags that could
disagree with each other.

## Terms

Raft divides time into **terms**, each identified by a monotonically
increasing number. At most one leader can be elected per term (this is
enforced by the voting rule below), though a term can also end with no
leader at all if an election splits the vote.

```text
Term 1          Term 2          Term 3
[Leader A] ──x  [no leader,     [Leader B] ───→
  (crashes)      split vote]
```

Every RPC in Raft carries a term number, and this is how nodes detect
that they are out of date: **whenever a node sees a term higher than its
own, in any RPC or reply, it immediately adopts that term and steps down
to Follower** (`Node.becomeFollowerLocked` in `raft.go`, called from both
`HandleRequestVote` and `HandleAppendEntries`, and from the reply-handling
side of `sendRequestVote` / `sendAppendEntries`). This one mechanism is
what enforces two of Raft's core safety properties: a node's term never
decreases, and a leader that has been superseded (even if it hasn't
noticed a new election happened) gives up its authority the instant it
hears about the newer term.

## Elections

A follower that goes too long without hearing from a leader assumes the
leader is gone and starts an election: it increments its term, becomes a
candidate, votes for itself, and asks every other node for its vote
(`Node.startElectionLocked` in `election.go`). If it collects votes from
a **majority** of the cluster (itself included), it becomes leader
immediately and starts sending heartbeats.

```text
majority = floor(N / 2) + 1

N = 1  →  1        N = 3  →  2        N = 5  →  3
```

`Node.majority()` computes this from the actual configured cluster size
(`len(peers)+1`) rather than assuming any particular number of nodes, so
the same code handles a 3-node or a 5-node cluster correctly.

### Election timeouts are randomized

If every node waited exactly the same amount of time before starting an
election, a real cluster could repeatedly end up with several candidates
starting elections at the same instant, splitting the vote every single
term and never electing anyone. Raft avoids this by having each node pick
its next election timeout **randomly** from a configured range
(`Options.ElectionTickMin` / `ElectionTickMax`, defaulting to 10-20) every
time it resets its timer. In a real cluster this spread makes it very
likely that one candidate's timer fires well before any other's, so it
can win the election before a competing candidate even starts.

### Ticks, not wall-clock sleeps

Phase 5's timing is built around a small idea borrowed from other
production Raft implementations: **`Node.Tick()`** advances the node's
internal notion of time by one logical step, and all of Raft's timing
decisions (has an election timeout elapsed? is it time for a heartbeat?)
are just counters compared against a threshold, checked inside `Tick`.
`Node.Run` drives `Tick` from a real `time.Ticker` for production use, but
nothing about Raft's correctness depends on real time at all -- the
package's tests call `Tick()` directly, as many times as needed, which
makes election and heartbeat behavior exactly reproducible without a
single `time.Sleep` in the test suite.

## RequestVote

`RequestVote` is the RPC a candidate sends to every peer to ask for its
vote (`RequestVoteArgs` / `RequestVoteReply` in `message.go`, handled by
`Node.HandleRequestVote` in `election.go`). A voter grants its vote only
if *all* of the following hold:

1. The candidate's term is at least as large as the voter's own (a stale
   candidate is rejected outright).
2. The voter has not already voted for a *different* candidate this term
   (`votedFor`) -- each node casts at most one vote per term.
3. The candidate's log is **at least as up-to-date** as the voter's own.

The up-to-date check (`logIsUpToDate` in `log.go`) is the one rule that
is easy to get wrong, so it is worth stating precisely:

```text
Candidate's log is at least as up-to-date as the voter's if:

  candidateLastTerm > voterLastTerm

  OR

  candidateLastTerm == voterLastTerm
  AND candidateLastIndex >= voterLastIndex
```

Term is compared *first*, and always wins: a candidate with a shorter log
but a later last-entry term is still more up-to-date than a voter with a
longer log stuck on an older term, because only entries reachable from
the current leadership lineage can be part of the true committed history.
Log length by itself is never a sufficient comparison -- this is exactly
why `logIsUpToDate` takes both index and term for both sides, rather than
just comparing lengths.

## AppendEntries

`AppendEntries` is the leader's RPC for both replicating log entries and
sending heartbeats (`AppendEntriesArgs` / `AppendEntriesReply` in
`message.go`, handled by `Node.HandleAppendEntries` in
`replication.go`). Every AppendEntries carries `PrevLogIndex` /
`PrevLogTerm`: the index and term of the log entry the leader believes
comes immediately before whatever it's sending. A follower only accepts
the RPC if its own log actually has a matching entry there; otherwise it
rejects the RPC (`Success: false`) and appends nothing, so a leader can
never silently create a gap or a mismatch in a follower's log.

## Heartbeats

An `AppendEntries` RPC with zero entries is a **heartbeat**. A leader
sends one to every follower every `HeartbeatTick` ticks
(`Node.broadcastAppendEntriesLocked`, called from the `Leader` branch of
`Tick`). Every valid `AppendEntries` a follower accepts -- heartbeat or
not -- resets that follower's own election timer
(`resetElectionTimerLocked`, called at the top of
`HandleAppendEntries`). This is the entire mechanism that keeps a healthy
cluster from electing new leaders for no reason: as long as followers
keep hearing from a legitimate leader often enough, none of their
election timeouts ever get the chance to elapse.

```text
   Leader
     │
     ├── AppendEntries(empty) ──→ Follower A  (timer reset)
     ├── AppendEntries(empty) ──→ Follower B  (timer reset)
     └── AppendEntries(empty) ──→ Follower C  (timer reset)
```

## Log replication

When a client calls `Node.Propose(command)` on the leader, the leader
appends a new entry to its own log at its current term and immediately
broadcasts `AppendEntries` carrying it (`replication.go`). Each follower
that accepts the entry appends it to its own log via `Log.AppendAfter`.
The leader tracks, per follower, `nextIndex` (the next log index it will
try sending that follower) and `matchIndex` (the highest index it knows
that follower has actually replicated) -- the standard Raft leader
bookkeeping.

## Log conflicts

A follower's log can end up disagreeing with the leader's -- for example,
after that follower was itself briefly a leader in a term that never
reached majority commitment. `Log.AppendAfter` (`log.go`) is where this
gets resolved, entry by entry:

```text
Follower's log:   1:T1  2:T1  3:T2  4:T2
Leader sends:                3:T3  4:T3

              ↓ index 3's term (T2) does not match the incoming T3

Follower's log:   1:T1  2:T1  3:T3  4:T3
                  └──matching prefix──┘  └─replaced suffix─┘
```

The matching prefix (indexes 1 and 2, where the terms already agree) is
never touched. The conflicting suffix (the old index 3 and 4) is
discarded, and the leader's entries take their place. Just as important
in the other direction: if the leader happens to *resend* entries the
follower already has (a retried RPC, for instance), `AppendAfter`
recognizes every one of them as already matching and changes nothing at
all -- it never blindly duplicates entries just because it was told to
append them again.

## Majority commit

An entry becomes **committed** once the leader has confirmed it is
replicated on a majority of the cluster (itself included). This is
`Node.maybeAdvanceCommitIndexLocked` in `replication.go`, called every
time an `AppendEntries` reply reports success and updates that follower's
`matchIndex`.

```text
3-node cluster, majority = 2

Leader:      matchIndex = (self, always caught up)
Follower A:  matchIndex = 4   ←── 2 of 3 have index 4: COMMITTED
Follower B:  matchIndex = 2
```

There is one crucial extra rule, easy to miss: **a leader can only
directly commit an entry from its *own* current term.** An entry from an
earlier term is never committed just because a majority happens to have
replicated it -- it only becomes committed indirectly, by riding along
once a later entry from the current term is committed. Raft needs this
restriction for safety: without it, it's possible to construct a
scenario where an entry looks majority-replicated but a future,
differently-elected leader would still be entitled to overwrite it. This
is exactly what real-world Raft write-ups mean by "leaders can only
commit entries from their own term."

## Why only committed entries can eventually be applied

Anything in a leader's log that *isn't* committed yet is still
provisional. A leader can crash, or be superseded by a node whose log
diverges after the last commit point, and an uncommitted entry can be
silently overwritten by `Log.AppendAfter` as part of ordinary conflict
resolution -- with no error, because it was never guaranteed in the first
place. Only a **committed** entry is guaranteed, by Raft's core safety
property, to appear (at that same index, with that same command) in the
log of every future leader, forever. That is precisely the guarantee a
database needs before it is safe to let a command have an externally
visible effect: `Node.CommittedEntries` (`replication.go`) only ever
returns entries up to `CommitIndex`, never anything past it, which is the
boundary a future state machine (Phase 7) will use to know what it is
actually safe to apply.

## Why Phase 5 does not touch storage

`internal/raft` does not import `internal/storage`, and nothing in this
phase writes to a MemTable, a WAL, an SSTable, or the Manifest. This is a
deliberate architectural boundary, not an oversight:

```text
Client → API → Raft → committed entries → State Machine → Storage
```

Raft's only job is to decide the ordered, agreed-upon sequence of
commands. *What a command actually means* -- how it should change the
database's state -- is entirely the future state machine's concern
(Phase 7), which will read committed entries off of Raft (via
`CommitIndex` / `CommittedEntries` / `CommitCh`) and translate them into
calls against the existing `storage.Store` interface. Keeping Raft
ignorant of storage means the consensus logic can be tested exhaustively
on its own, as this phase does, without a database underneath it at all --
and it means a later phase can change how commands are applied to
storage without ever touching this package.

```text
             Raft Cluster

        ┌──────────────┐
        │    Leader     │
        └──────┬────────┘
               │  AppendEntries
        ┌──────┴───────┐
        ↓              ↓
   ┌─────────┐    ┌─────────┐
   │Follower │    │Follower │
   └─────────┘    └─────────┘
```

> **Raft decides the ordered committed history; the future state machine
> will decide how committed commands change database state.**

## What Phase 6 will add

Phase 5 keeps every piece of Raft state -- `currentTerm`, `votedFor`, and
the log itself -- entirely in memory (`Node`'s fields in `raft.go`, and
`Log` in `log.go`). That is fine for testing the algorithm, but it means
a `Node` in this phase has total amnesia across a restart: if the process
exits and starts again, it has no memory of votes it already cast, terms
it already saw, or entries it already logged. That is unsafe for a real
deployment -- a restarted node could vote twice in the same term, or a
leader could lose entries it had already told a client were committed.

Phase 6 is where Raft's persistent state gets written durably to disk (in
the same crash-safe spirit as the WAL from Phase 2) and correctly
recovered on restart, along with the stronger conflict-recovery behavior
a real deployment needs once nodes can actually lose their last few log
entries to a crash mid-write. None of that is implemented here.

## Transport

`internal/raft/transport.go` defines `Transport`, the interface a `Node`
uses to deliver RPCs, and `InMemoryTransport`, its Phase 5 implementation:
sending an RPC calls directly into the target node's handler in the
caller's own goroutine and returns its reply immediately -- there is no
real network, no serialization, and no background delivery goroutine.
This keeps every cluster test fully deterministic: a send either
completes synchronously or fails immediately with `ErrPeerUnreachable`,
with no timing to race against.

`InMemoryTransport.Partition(id)` / `Heal(id)` simulate a node being cut
off from the rest of the cluster in both directions -- every send to or
from a partitioned node fails immediately, as if every packet to or from
it were dropped. This is what the test suite uses to verify that a
minority can never falsely commit an entry, and that a leader correctly
steps down once a partition heals and it hears from a newer leader.

A future phase can add a real network transport (gRPC or otherwise)
behind the same `Transport` interface without changing anything in this
package.

## Concurrency

A `Node`'s entire state -- term, vote, log, role, commit index, leader
bookkeeping -- is protected by one internal mutex. Every RPC handler,
every `Tick`, and every `Propose` call takes that lock for the small
amount of synchronous work needed to read or update state.

The one rule the implementation is careful never to break: **the lock is
never held while waiting on a network call.** Sending an RPC to a peer
always happens in its own goroutine (`Node.trackRPC` in `raft.go`), spun
up *after* releasing the lock; only once that goroutine's `Transport`
call returns does it re-acquire the lock, briefly, to process the reply.
This is what makes it safe for two nodes to be sending each other RPCs at
the same moment without any risk of the two nodes' goroutines
deadlocking on each other's locks.

`Node.Drain()` exists purely to make this concurrency deterministically
testable: it blocks until every RPC a node has sent -- and anything that
processing a reply went on to trigger, such as a newly-elected leader
immediately broadcasting `AppendEntries` -- has finished. The package
tests follow a simple, repeated pattern: call `Tick()` (or `Propose()`)
some number of times, call `Drain()`, then assert on the resulting state.

## Tests

`internal/raft` has unit tests alongside every source file
(`log_test.go`, `transport_test.go`, `election_test.go`,
`replication_test.go`), plus shared cluster-building helpers in
`cluster_test.go`. Together they cover, among others: the initial
all-Follower state; the Follower→Candidate transition and self-vote on
election timeout; majority election in 3- and 5-node clusters, and
election failure when a majority is unreachable; one-vote-per-term and
every combination of the log up-to-date rule; heartbeats suppressing
unnecessary elections; leader step-down on a higher term (including a
full partition-then-heal scenario); every `AppendEntries` rejection case
(stale term, `PrevLogIndex` mismatch, `PrevLogTerm` mismatch); conflict
truncation and matching-prefix preservation; replication reaching
followers; majority commit, and a partitioned leader (or an insufficient
minority) failing to falsely commit; `Propose` on a non-leader returning
`ErrNotLeader`; the commit/apply boundary never exposing an uncommitted
entry; and a term-monotonicity stress test that drives repeated elections
and partitions while asserting no node's term is ever observed to
decrease.

## Limitations

- No persistence: a `Node`'s entire state is lost if the process exits.
  Durable, crash-recoverable Raft state is Phase 6.
- No log compaction / snapshotting: `Log` keeps every entry in memory
  forever. That is out of scope for Phase 5 and is not addressed here.
- No cluster membership changes: `Options.Peers` is fixed for the
  lifetime of a `Node`; there is no mechanism to add or remove nodes from
  a running cluster.
- `InMemoryTransport`'s conflict backoff on a failed `AppendEntries`
  decrements `nextIndex` by exactly one per rejected round-trip, the
  simplest correct form of retry; a faster backtracking optimization
  (e.g. skipping a whole conflicting term at once) is not implemented,
  since Phase 5 only needs a functionally correct example of the
  conflict-recovery path.
- No race-detector run was possible in this environment (`-race` requires
  `cgo`, and this machine has no C compiler configured), the same
  limitation Phase 4 reported. `go test ./internal/raft/...` was run
  repeatedly (30 iterations) with the default detector-free build to
  check for flakiness in the concurrent election/replication tests, with
  no failures observed.
- Not implemented, by design, per the phase's scope: any client-facing
  API, HTTP or gRPC transport, state machine, or connection of committed
  entries to `internal/storage`. See "Why Phase 5 does not touch
  storage" above.
