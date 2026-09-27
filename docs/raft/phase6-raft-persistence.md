# ForgeDB — Phase 6: Raft Persistence & Crash Recovery

## Scope

Phase 6 adds durability to `internal/raft`: a Node's `currentTerm`,
`votedFor`, and log now survive a process crash and restart. It does
**not** add a state machine (Phase 7), a real network transport, gRPC or
HTTP, snapshots, membership changes, or any integration with ForgeDB's
storage engine (`internal/storage`). Everything here still lives and is
tested entirely within `internal/raft`, exactly as Phase 5 was.

## Why Phase 5's in-memory Raft was insufficient

Phase 5's `Node` doc comment said it plainly: "Node's state is entirely in
memory... a restarted Node in Phase 5 has no memory of a previous run."
That is fine for a single test process, but fatal for a real cluster
node. Consider a 3-node cluster where node A is the current leader, has
voted for itself in term 6, and has replicated ten log entries:

```text
before crash:                after a Phase 5-style restart:
term=6, votedFor=A            term=0, votedFor="", log=[]
log=[e1..e10]
```

If node A's process crashes and restarts with no memory of any of this,
two different kinds of damage can happen:

- **It can violate Raft's one-vote-per-term safety rule.** If A is asked
  to vote again in term 6 after restarting, it has no record of having
  already voted for itself in term 6, so it might vote for a different
  candidate. Two leaders could then both believe they won term 6, and
  Raft's core safety guarantee (at most one leader per term) breaks.
- **It can silently lose committed data.** The ten log entries A had
  replicated to a majority are supposed to be permanent, by Raft's
  definition of "committed." If A's restart wipes its log, and a later
  leader election happens to need A's vote, A might vote for a candidate
  whose log is actually behind where the cluster's committed history
  really was -- because A itself no longer remembers that history.

Phase 6 exists to close exactly this gap: **currentTerm, votedFor, and the
log must survive a crash**, or none of the safety proofs Raft depends on
actually hold once real machines with real disks and real crashes are
involved.

## Persistent vs. volatile vs. leader-only volatile state

```text
             Raft Node
                 │
       ┌─────────┴─────────┐
       │                   │
 Persistent State      Volatile State
       │                   │
       ├─ currentTerm      ├─ commitIndex
       ├─ votedFor         ├─ lastApplied
       └─ log              ├─ role
                           ├─ election/heartbeat timers
                           └─ (leader-only)
                              ├─ nextIndex
                              └─ matchIndex
```

- **Persistent** (`PersistentState` in `persist.go`): `currentTerm`,
  `votedFor`, and `log`. These three are what Raft's safety proofs
  actually depend on surviving a crash -- see the next three sections for
  why each one specifically matters.
- **Volatile**: `commitIndex` and `lastApplied` (see
  [Commit-index recovery semantics](#commit-index-recovery-semantics)
  below for why these are deliberately *not* persisted), plus `role` and
  the election/heartbeat timers. None of these describe a promise Raft
  has made to the rest of the cluster; they are this process's private
  bookkeeping and are always safe to reset to a sensible default on
  restart.
- **Leader-only volatile**: `nextIndex` and `matchIndex`, which only ever
  exist while a node believes itself to be the leader (`becomeLeaderLocked`
  allocates them; no other role touches them). A restarted node never
  resumes an old leadership, so these are simply never restored -- a node
  that becomes leader again after a restart rebuilds them from scratch, as
  it always has since Phase 5.

### Why currentTerm must survive a crash

`currentTerm` is what makes term monotonicity enforceable across the
whole cluster: every RPC carries a term, and a node adopts (never
decreases to) the highest term it has ever seen. If a restarted node
forgot its term and reverted to a lower one, it could re-participate in
an election for a term the rest of the cluster has already moved past --
undermining the "at most one leader per term" guarantee the moment a
process restarts.

### Why votedFor must survive a crash

`votedFor` is what enforces one-vote-per-term. The safety argument for
"a candidate that wins a majority of votes in a term is the only possible
leader for that term" only holds if every voter can be trusted to have
voted at most once in that term -- including across a crash and restart in
the middle of it. If `votedFor` were forgotten, a restarted node could
grant a second, conflicting vote in a term it already voted in, and two
different candidates could both legitimately believe they won.

### Why the log must survive a crash

The log is the actual data Raft exists to protect: a leader's promise
that "this majority of the cluster has replicated this entry" is only
meaningful for the lifetime of a process if that entry is still there
after every process in that majority has been restarted. A log that
evaporates on restart makes "committed" a meaningless word.

## The Persister abstraction

`internal/raft/persist.go` defines a small interface rather than coupling
`Node` to a concrete file format or storage backend:

```go
type Persister interface {
    SaveState(state PersistentState) error
    LoadState() (PersistentState, error)
}
```

`LoadState` returns the sentinel `ErrNoState` when nothing has ever been
saved (an ordinary, expected condition for a brand-new node), and a
wrapped `ErrCorrupt` when something *was* saved but can't be read back
correctly (see [Corruption handling](#corruption-handling)). These are
deliberately different error conditions with different correct responses
-- see [Startup recovery](#startup-recovery).

Two implementations exist:

- **`FilePersister`** (`file_persister.go`) is the production
  implementation: one file on disk, replaced atomically on every save.
- **`MemoryPersister`** (`memory_persister.go`) is for tests: it lets a
  test simulate "the process crashed and restarted" by constructing one
  `MemoryPersister` and passing it to two successive `NewNode` calls, and
  it supports fault injection (`FailNextSave`) to test what happens when a
  save genuinely fails.

`Options.Persister` defaults to an unexported no-op `discardPersister`
when left `nil`, which reproduces Phase 5's original in-memory-only
behavior exactly -- `SaveState` always succeeds without doing anything,
and `LoadState` always reports no previous state. This is why every
Phase 5 test continues to pass unmodified: none of them set a Persister,
so they get the old behavior byte-for-byte.

## On-disk format

`FilePersister` stores a single file: a full snapshot of the current
state, not an append-only edit log, following the same convention as
`internal/storage/manifest`'s Manifest file. This keeps recovery trivial
(there is exactly one file to read, with no history to replay) at the
cost of rewriting the whole state on every save -- an acceptable trade at
Phase 6's scale, matching the reasoning `manifest.go` gives for the same
choice.

```text
magic (8 bytes, "ForgeRP1")
formatVersion (u32 LE)
currentTerm   (u64 LE)
votedForLen   (u32 LE) | votedFor bytes
numEntries    (u32 LE)
  [ index (u64 LE) | term (u64 LE) | commandLen (u32 LE) | command bytes ] * numEntries
checksum (u32 LE, CRC-32C over every preceding byte)
```

The log's index-0 sentinel (see `Log`) is never written -- only the real,
1-indexed entries are. `newLogFromEntries` (in `raft.go`) reconstructs the
sentinel when a Node loads a persisted log back.

### Bounds, checked before allocation

Every length read from disk is validated *before* it is used to size a
slice, exactly like `internal/storage/wal`'s record format and
`internal/storage/manifest`'s table list:

| Field                | Bound         |
|----------------------|---------------|
| `votedFor` length     | 4,096 bytes (`maxVotedForLen`) |
| number of log entries | 1,048,576 (`maxLogEntries`, `1<<20`) |
| a single command      | 1 MiB (`maxCommandSize`, `1<<20`) |
| whole file            | 1 GiB (`maxStateSize`, `1<<30`), checked before any parsing begins |

A corrupted 32-bit length field can therefore never trigger an unbounded
or unreasonably large allocation attempt -- decoding fails fast with
`ErrCorrupt` instead.

### Checksums and versioning

The trailing CRC-32C checksum (the same polynomial the WAL and Manifest
use) covers every byte before it, so any single-bit flip anywhere in the
file is detected. `formatVersion1` is checked explicitly and rejected if
it doesn't match, which is what lets the format evolve later (a future
version could add fields after `formatVersion`, guarded by a version
bump) without silently misinterpreting an old or new file as the wrong
shape.

## Atomic persistence

`FilePersister.SaveState` calls `internal/storage/atomicfile.Write`, the
same helper Phase 3 uses for SSTables and the Manifest, rather than a new
implementation:

```text
create temporary file (path + ".tmp")
       │
       ▼
write complete encoded state
       │
       ▼
fsync the temporary file
       │
       ▼
atomic rename over path
       │
       ▼
best-effort fsync of the containing directory
```

If any step before the rename fails, `path` is left completely untouched
-- whatever was there before (a previous valid state, or nothing) is still
there, and the temporary file is removed. A reader can never observe a
half-written state file. As documented on `atomicfile.Write` itself, the
final directory fsync is best-effort: Go's standard library cannot fsync
a directory on Windows, so on that platform the directory-entry update
itself is not separately made durable, though the rename that already
completed the file replacement is. This is an existing, previously
documented limitation (see `docs/storage/phase3-sstables-manifest.md`),
not something Phase 6 introduces or needs to work around differently.

`atomicfile.go` itself was not modified -- Phase 6 reuses it exactly as
Phase 3 built it.

## Persistence ordering: when state is saved, and in what order

Persistence happens synchronously, **while the Node's own mutex (`n.mu`)
is held**, at every point where `currentTerm`, `votedFor`, or the log
actually changes. Concretely:

```text
higher term observed (RequestVote or AppendEntries, request or reply)
       │
       ▼
becomeFollowerLocked: mutate currentTerm + votedFor in memory
       │
       ▼
persistLocked()  -- while n.mu is still held
       │
       ├─ success → apply role/leaderID/timer changes, continue processing
       └─ failure → roll back currentTerm/votedFor, return an error;
                    the RPC is rejected as if this node never saw it
```

```text
HandleRequestVote grants a vote
       │
       ▼
mutate votedFor = candidate  (in memory)
       │
       ▼
persistLocked()
       │
       ├─ success → reset election timer, reply VoteGranted=true
       └─ failure → roll back votedFor, reply VoteGranted=false
```

```text
HandleAppendEntries / Propose append or truncate the log
       │
       ▼
Log.AppendAfter / Log.Append mutate the log (in memory)
       │
       ▼
persistLocked()   -- only if the log actually changed
       │
       ├─ success → the entry is now locally durable; continue
       └─ failure → roll back the log mutation, reject/error out
```

Two deliberate design choices make this simple and safe:

1. **Persisting while holding the lock is intentional.** Phase 5's
   concurrency rule ("never hold the node mutex across an outbound RPC")
   is preserved exactly -- disk I/O is not a network call, and every
   actual `Transport` send still happens from a `trackRPC` goroutine
   outside the lock, unchanged from Phase 5. Serializing every persist
   through the node's own mutex is what makes it impossible for two
   concurrent state changes to be written to disk out of order (e.g. term
   9 persisted before term 8): since a Node only ever mutates its
   persistent fields while holding `n.mu`, and always persists before
   releasing it, in-memory mutation order and on-disk write order are
   identical by construction. No separate sequencing mechanism was
   needed. Correctness was prioritized over persistence throughput, as
   the phase calls for.

2. **Snapshot-then-rollback on failure, never a partially-applied
   mutation.** Every call site that mutates persistent state first
   snapshots the old value(s), attempts to persist the new value, and
   restores the snapshot if persistence fails. This means a node's
   in-memory state can never be observed to be "ahead" of what's actually
   durable on disk: if `SaveState` returns an error, the operation that
   triggered it is treated as if it never happened, and the RPC caller
   (or `Propose` caller) gets an explicit rejection or error instead of a
   false success. See [Persistence failure behavior](#persistence-failure-behavior).

## Startup recovery

`NewNode` now returns `(*Node, error)` (a deliberate, compatibility-
breaking change from Phase 5's `*Node`-only signature -- see
[Corruption handling](#corruption-handling) for why this was necessary).
Its recovery path:

```text
NewNode(opts)
    │
    ▼
opts.Persister.LoadState()
    │
    ├── ErrNoState        → start fresh: term=0, votedFor="", empty log
    ├── any other error   → NewNode fails, returns that error
    └── success           → restore currentTerm, votedFor, log
             │
             ▼
       every volatile field starts at its zero value:
       commitIndex=0, lastApplied=0, role=Follower,
       nextIndex/matchIndex=nil, election timer freshly randomized
             │
             ▼
        node is constructed, not yet registered with a Transport
        or ticking -- exactly like a fresh Phase 5 node from here
```

A restarted node **never** resumes an old `Leader` role, never restores
old `nextIndex`/`matchIndex`, and never treats an in-flight RPC as still
outstanding -- there is no such state to restore in the first place, since
none of it was ever persisted. It simply starts as an ordinary Follower
with its recovered term/vote/log, exactly as if it were joining the
cluster fresh except that it already knows its history. Normal Raft
mechanics (election timeouts, a new leader's heartbeats) take it from
there.

## Corruption handling

A distinction the phase leans on throughout: **"no state file" and
"corrupt state file" are different conditions requiring different
responses.**

- No state file (`ErrNoState`) is the normal, expected case for a node
  that has never started before. `NewNode` treats it as "initialize
  fresh" -- not a failure.
- A corrupt state file (wrapped `ErrCorrupt`) means this node previously
  promised to remember something (a vote, a term, log entries) and that
  promise can no longer be verified. Silently discarding it and starting
  fresh could mean re-voting in a term this node already voted in, or
  forgetting log entries it had already told a leader it accepted.
  `NewNode` therefore **fails outright** rather than guessing.

`decodeState` (`format.go`) detects, and returns `ErrCorrupt` for, every
category the phase calls out: a bad magic number, an unsupported version,
a truncated header, a truncated log entry, an out-of-bounds length (for
`votedFor`, the entry count, or a command), a checksum mismatch, and
trailing bytes left over after decoding everything the header claims is
there (strict EOF). It additionally validates two invariants that a
genuinely well-formed persisted log must satisfy, as extra corruption
detection beyond the wire format alone: each entry's `Index` must equal
its position in the log (1, 2, 3, ...), and entry terms must never
decrease along the log, nor exceed `currentTerm`.

This is why `NewNode`'s signature had to change from Phase 5's `*Node` to
`(*Node, error)`: there was previously no way for node construction to
fail at all, and "corrupt persisted state must fail startup explicitly"
cannot be honored without some way to report that failure. Every Phase 5
test that constructs a `Node` was updated to check this new error (via a
`mustNewNode` test helper); none of them configure a `Persister`, so
`NewNode` never actually fails for them, and their behavior is unchanged.

## Persistence failure behavior

If `SaveState` returns an error at any of the call sites described in
[Persistence ordering](#persistence-ordering-when-state-is-saved-and-in-what-order),
the Node:

- rolls back the in-memory mutation that triggered the save, restoring
  the exact previous value(s);
- returns an explicit failure to the caller instead of pretending the
  operation succeeded: `HandleRequestVote` returns `VoteGranted: false`
  (and, for a failed term step-down, the node's unchanged old term),
  `HandleAppendEntries` returns `Success: false`, and `Propose` returns a
  wrapped error instead of an index/term;
- for `startElectionLocked` (which has no RPC reply to report failure
  through -- it's driven by `Tick`), simply abandons the election attempt
  for now, deliberately leaving the election timer un-reset so the very
  next `Tick` retries once persistence has hopefully recovered.

The two RPC-reply-side call sites (`sendRequestVote` and
`sendAppendEntries`, handling a *reply* rather than an incoming RPC) call
`becomeFollowerLocked` best-effort: there is no RPC reply to fail on that
path (it's already processing a reply), so a persistence failure there
just means this node continues believing its old term for now, exactly
as `becomeFollowerLocked`'s own rollback already guarantees. It will
observe the same higher term again from the next message from any peer,
and try again.

In every case, the invariant is the same: **this node's in-memory state
is never observably ahead of what is actually durable on disk.** See
`persistence_test.go`'s `TestPersistence_SaveFailure_*` tests for this
exercised directly, using `MemoryPersister.FailNextSave`.

## Commit-index recovery semantics

`commitIndex` and `lastApplied` are **not** persisted, and are always
reset to `0` on restart. This is a deliberate, conservative choice, not
an oversight:

- Raft's safety property is about the *log*, not about a particular
  process's belief of how far it has committed. A restarted node that
  simply keeps its restored log and lets `commitIndex` climb back up
  through ordinary Raft mechanics (a leader's `LeaderCommit` on a fresh
  `AppendEntries`, or this node itself becoming leader and re-establishing
  a majority) never violates anything -- `maybeAdvanceCommitIndexLocked`
  and the `LeaderCommit` handling in `HandleAppendEntries` are completely
  unchanged from Phase 5 and re-derive the correct commit index from
  scratch using the restored log.
- **The implementation must never apply an entry merely because it
  existed in the persisted log.** Phase 6 has no state machine yet --
  `CommittedEntries` is a pull-based query API that nothing in this
  package calls automatically -- so there is no "apply" step for a
  restarted node to get wrong in this phase. But resetting `commitIndex`
  to 0 is what keeps that true structurally: a restarted node makes no
  claim about what was previously committed until it re-derives that
  fact the normal way.
- A future Phase 7, when it adds a real state machine that consumes
  `CommittedEntries`/`CommitCh`, will need its own re-application safety
  story (tracking `lastApplied` durably, or making command application
  idempotent) for the case where a node restarts after having applied
  some entries but not persisted that fact. That is explicitly out of
  scope for Phase 6, which only guarantees the *log* survives a restart,
  not any record of how far a (currently nonexistent) state machine had
  progressed through it.

## Relationship between Raft persistence and the KV WAL

`internal/raft`'s persistence and `internal/storage`'s WAL solve
unrelated problems and share no code beyond one utility function:

```text
                 ForgeDB
                    │
          ┌─────────┴─────────┐
          │                   │
       Raft layer         KV storage
          │                   │
  FilePersister            WAL / SSTables / Manifest
  (currentTerm,            (key-value mutations,
   votedFor, log)           MemTable, compaction)
```

- The KV WAL (`internal/storage/wal`) durably records **key-value
  mutations** applied to ForgeDB's storage engine, so `internal/storage`
  can recover its MemTable after a crash (Phase 2).
- Raft's `FilePersister` durably records **consensus state**: the
  opaque `Command` bytes inside a Raft log entry are never interpreted by
  this package (see `Command`'s doc comment, unchanged since Phase 5) and
  are not the same thing as a WAL record.
- The only code Phase 6 reuses from `internal/storage` is the generic
  `atomicfile.Write` helper -- the write-temp/fsync/rename protocol itself
  has nothing storage-engine-specific about it, and `atomicfile.go` was
  not modified to accommodate Raft. `raft` does not import, construct, or
  otherwise depend on `storage.Store`, `MemStore`, any SSTable type, or
  the Manifest, and none of those types know Raft exists.

A future integration phase will presumably have Raft's committed entries
drive writes into the storage engine's own WAL-backed path, but that
wiring does not exist yet and is explicitly out of scope here.

## What remains intentionally out of scope

Per the phase boundary, Phase 6 does **not** include: a Phase 7 state
machine or command application, client request deduplication, any
Raft-to-storage-engine integration, HTTP or gRPC APIs, a real network
transport, snapshots or snapshot installation, cluster membership changes
or joint consensus, read-consistency/linearizability guarantees, a chaos
testing framework, or metrics/observability. `internal/storage`'s
compaction and SSTable code was not touched.

## Known platform limitations

- **Directory fsync on Windows.** As already documented for
  `atomicfile.Write` (and inherited unchanged by `FilePersister`), Go's
  standard library cannot fsync a directory on Windows, so the
  directory-entry update following a rename is best-effort there. The
  file replacement itself (the rename) is still atomic and durable; only
  the extra belt-and-suspenders directory fsync is unavailable. This
  matches Phase 3's existing, previously documented behavior.
- **`go test -race` is unavailable in this environment.** The race
  detector requires cgo, which requires a C compiler; none is installed
  on this Windows machine (`CGO_ENABLED=0`, no `gcc`/`clang` on `PATH`).
  This is a pre-existing environment limitation, not something Phase 6
  introduces -- Phase 6's tests were run repeatedly (`-count=30`) under
  the normal (non-race) test runner instead, alongside the existing
  Phase 5 suite, with no failures.
