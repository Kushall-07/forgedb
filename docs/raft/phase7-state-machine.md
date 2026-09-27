# ForgeDB — Phase 7: State Machine & Request Deduplication

## Scope

Phase 7 adds a new package, `internal/statemachine`, that sits between
Raft (`internal/raft`) and ForgeDB's storage engine (`internal/storage`):
it applies committed Raft log entries to storage, in order, and
deduplicates retried client requests. It does **not** add snapshots, Raft
log compaction, gRPC, HTTP, real networking, cluster membership changes,
read consistency / linearizable reads, a chaos framework, or metrics.
`internal/raft` gains exactly two small additive methods
(`LastApplied`/`MarkApplied`, see below); every other Phase 0–6 file and
test is unchanged.

## 1. Why a state machine is needed

Phase 5 built replicated, ordered commitment. Phase 6 made that commitment
durable. But neither phase gave committed commands anywhere to go:

```text
Client command
      |
      v
     Raft
      |
      v
replicated log
      |
      v
   majority
      |
      v
   COMMITTED  <-- Phase 5/6 stopped here
      |
      v
 State Machine   <-- Phase 7
      |
      v
 KV Storage
```

`(*raft.Node).CommittedEntries` and `CommitCh` existed since Phase 5
specifically as "the boundary a future state machine will poll" (see that
method's doc comment) -- nothing in `internal/raft` ever called them
itself. Phase 7 is that future arriving: `internal/statemachine.Applier`
is the first and only consumer of that boundary.

## 2. Raft commit vs. state-machine application vs. storage durability

Three distinct concepts, kept in three distinct layers:

```text
                 ForgeDB
                    |
       +------------+------------+------------+
       |            |            |
   Raft layer   State Machine   Storage
       |            |            |
  "majority     "command      "WAL/MemTable
   replicated"   executed      mechanics
                 against       completed"
                 storage"
```

- **Raft committed**: a majority of the cluster has durably replicated
  this log entry. This is `(*raft.Node).CommitIndex()`, unchanged from
  Phase 5/6.
- **State-machine applied**: `statemachine.KVStateMachine.Apply` has
  executed the entry's command against storage (or determined, without
  touching storage, that it need not -- a duplicate replay, a conflict, or
  a stale request). This is new in Phase 7, tracked via
  `(*raft.Node).LastApplied()`.
- **Storage durable**: `internal/storage.Store`'s own WAL-before-MemTable
  discipline (Phase 2) has completed for that mutation. This package never
  second-guesses or duplicates that mechanism -- `KVStateMachine` calls
  `store.Put`/`store.Delete` exactly as any other caller of `Store` would.

`internal/raft` still knows nothing about `internal/storage`, and
`internal/storage` still knows nothing about Raft or `internal/statemachine`
-- both facts remain true after this phase, by construction (`go.mod`'s
import graph has no edge from either package to the other).

## 3. The `StateMachine` interface and `KVStateMachine`

```go
type StateMachine interface {
    Apply(cmd Command) (Result, error)
}
```

`KVStateMachine` (`statemachine.go`) is the only implementation: it wraps
an `internal/storage.Store` and a deduplication table (see §7). It
introduces no second key/value implementation -- every mutation goes
through the exact same `Store.Put`/`Store.Delete` any other ForgeDB
component would call.

`Apply`'s error return is deliberately narrow and reserved for exactly one
thing: **a storage-layer fault this replica could not durably execute**
(e.g. a real disk I/O error). Every other outcome -- success, a replayed
duplicate, `ErrRequestIDConflict`, `ErrStaleRequest`, or a deterministic
storage *validation* failure such as `storage.ErrEmptyKey` -- is folded
into `Result.Err` with a `nil` error from `Apply` itself, because every
replica resolves those identically and there is nothing left to retry.
This distinction is what lets `Applier` (§5) know, mechanically, whether
to advance `lastApplied` past an entry or stop and retry it later.

## 4. Command representation

`Command` (`command.go`) is ForgeDB's concrete, typed payload -- distinct
from `raft.Command`, which remains the opaque `[]byte` Raft itself
replicates without interpreting (unchanged since Phase 5):

```go
type Command struct {
    ClientID  string
    RequestID uint64
    Op        Op        // OpPut or OpDelete
    Key       []byte
    Value     []byte    // only meaningful for OpPut
}
```

`Command.Encode()` serializes a `Command` into a `raft.Command` (ready for
`(*raft.Node).Propose`); `DecodeCommand` reverses it. The wire format is a
simple length-prefixed binary layout (op byte, then `clientIDLen |
clientID`, `requestID`, `keyLen | key`, `valueLen | value`, all
little-endian), following the same "check every length before allocating
or slicing" discipline as `internal/raft/format.go` and
`internal/storage`'s on-disk formats, including strict-EOF validation (no
trailing bytes tolerated). No magic number or checksum is included here --
unlike `internal/raft/format.go`'s `PersistentState` encoding, this payload
is never itself the thing written to disk; it travels *inside* a
`raft.LogEntry.Command`, which Phase 6's `FilePersister` already wraps in
its own CRC-32C-checked, versioned envelope. Only `PUT` and `DELETE` are
represented -- no SQL, no query language, and no `GET` (see §6).

## 5. Apply ordering: `Applier`

`Applier` (`applier.go`) is the sole owner of application order for one
`*raft.Node`. Its core method:

```go
func (a *Applier) ApplyAvailable() (applied int, err error)
```

pulls the range `(node.LastApplied(), node.CommitIndex()]` via
`node.CommittedEntries`, and for each entry, strictly in log-index order:

```text
decode entry.Command -> cmd
        |
        v
   sm.Apply(cmd) -> (Result, err)
        |
   +----+----------------------------+
   |                                 |
err == nil                      err != nil
(fully resolved,                (unresolved storage fault)
 whatever Result says)                 |
   |                                   v
   v                          STOP: return immediately,
node.MarkApplied(entry.Index)  do not call MarkApplied,
   |                           do not continue to later entries
   v
continue to next entry
```

This is the mechanism behind the required invariant: **entry N+1 is never
applied before entry N**, and an entry Raft has not yet committed is never
even fetched, since `CommittedEntries` itself never returns past
`CommitIndex`. `ApplyAvailable` holds its own mutex for its entire call, so
two goroutines (a manual test call racing a `Run` background loop) can
never apply entries concurrently or interleave -- there is exactly one
owner of application order per `Applier`, as required.

`Applier.Run()` is the optional, production, background-goroutine variant
(applies once immediately, then again on every `node.CommitCh()` signal),
mirroring `raft.Node.Run`/`Tick`'s own "optional background loop; tests
drive the deterministic method directly" pattern from Phase 5.

`Applier` never holds `node`'s internal mutex while calling `sm.Apply`:
`LastApplied`, `CommittedEntries`, and `MarkApplied` are each independent,
already-synchronized calls into `raft.Node` that acquire and release its
mutex internally and return before any storage work happens. A blocking
`Store.Put`/`Store.Delete` call therefore never runs with Raft's lock held,
preserving the concurrency rule Phase 5 established for outbound RPCs.

## 6. `lastApplied`

Phase 5 declared `lastApplied` on `Node` as dead bookkeeping reserved for
"a future state machine." Phase 7 gives it that purpose via two small,
additive methods on `*raft.Node` (`internal/raft/apply.go`):

```go
func (n *Node) LastApplied() uint64
func (n *Node) MarkApplied(index uint64) error
```

`MarkApplied` is the *only* way `lastApplied` ever changes, and it
enforces the required invariant centrally rather than trusting every
caller to: it rejects any `index` that is not strictly greater than the
current `lastApplied` (monotonic; no re-marking the same or an earlier
index) and rejects any `index` that exceeds `commitIndex` (nothing can be
marked applied before Raft itself considers it committed). `Applier` is
the only intended caller. `Node` itself still never applies anything to
anything -- it just now has somewhere authoritative to be told that
something else did.

This was the one deliberately minimal, additive change to
`internal/raft`'s public API this phase required (per the phase's "avoid
unnecessary breaking changes" constraint): two new methods in a new file,
touching no existing method, field visibility, or test.

## 7. Client request identity and the deduplication table

A logical client request is identified by the pair `(ClientID,
RequestID)` -- never `RequestID` alone, since two different clients could
independently reuse the same request number. `KVStateMachine` keeps one
`dedupEntry` per `ClientID`:

```text
client_id -> { last request_id, op, key, value, result }
```

Deliberately **only the last request per client**, not a growing history
of every request ever seen -- see §11 for why. On `Apply(cmd)`:

```text
                      no record for cmd.ClientID yet
                              |
                              v
                        execute normally
                              |
   record exists for cmd.ClientID
              |
   +----------+----------------------------------+
   |          |                                  |
cmd.RequestID   cmd.RequestID == prev.RequestID    cmd.RequestID
  > prev            |                             < prev.RequestID
   |          same Op/Key/Value?                        |
   v            |         |                             v
execute       yes        no                    Result.Err = ErrStaleRequest
normally       |          |                     (never executed)
               v          v
          replay      Result.Err =
          prev.result ErrRequestIDConflict
          (Replayed=  (never executed,
           true, no    prev record left
           storage     untouched)
           call)
```

"Execute normally" means: call `store.Put`/`store.Delete` exactly once,
then record the new `dedupEntry` (including on a resolved validation
failure such as `storage.ErrEmptyKey` -- see §3 -- since that outcome is
just as deterministic and repeatable as a success). A genuine storage
fault (`Apply`'s own error return) is never recorded, precisely so a real
retry after the fault clears gets a real second attempt rather than a
cached failure (see `TestKVStateMachine_StorageFault_ReturnsError_NoDedupRecorded`).

## 8. Duplicate result replay

A replayed duplicate never touches storage a second time -- `Result` is
copied from the stored `dedupEntry.result` verbatim (with `Replayed` set
to `true` so a caller/test can distinguish a fresh execution from a
replay), not recomputed. This is what makes `PUT x=10` and its retry both
observably return "the same success," and `DELETE x` and its retry both
return "the same (already-deleted) outcome," without a second WAL append.

## 9. Request-ID conflicts

If `(ClientID, RequestID)` repeats but the command's `Op`/`Key`/`Value`
differ from what was originally recorded, that is not a valid duplicate --
it is a client bug (or a reused ID across logically different requests).
`Apply` returns `Result{Err: ErrRequestIDConflict}` (a `nil` error from
`Apply` itself -- see §3, this is a resolved, deterministic outcome), never
touches storage, and never overwrites the original `dedupEntry`: a
subsequent replay of the *original* request still replays correctly (see
`TestKVStateMachine_RequestIDConflict_ReturnsErrorAndLeavesRecordUnchanged`).

## 10. Stale (old) duplicates and the chosen ordering semantics

Because `KVStateMachine` retains only the *last* `RequestID` seen per
client, a request with `cmd.RequestID < prev.RequestID` cannot be resolved
by re-deriving its original result -- that record no longer exists. The
chosen semantics, and the reasoning behind them:

- A well-behaved client numbers its own requests with strictly increasing
  `RequestID` values over its lifetime (the same assumption the original
  Raft paper's client-interaction section makes).
- Given that assumption, any `RequestID` lower than the highest one this
  state machine has already recorded for that client can only be a stale
  retry of something already superseded -- whether or not *this specific*
  state machine instance happened to be the one that originally applied
  it (a request could, in principle, have committed at an earlier log
  index this replica has not replayed yet in some other ordering, but
  since `Applier` always applies in strict log-index order, that
  situation cannot actually arise in practice; the check is a conservative
  guard regardless).
- It is therefore *never* re-executed. `Apply` returns
  `Result{Err: ErrStaleRequest}` and leaves the newer `dedupEntry`
  completely untouched -- re-executing an old, lower-numbered request
  could otherwise clobber newer state with stale command semantics, which
  would be actively unsafe, not just imprecise.
- This is a deliberately conservative choice: a truly ancient duplicate
  gets a generic "already superseded" answer rather than its original,
  specific result, because that original result is no longer retained.
  Real clients only ever retry their single most recent in-flight
  request, so this does not affect the realistic retry scenario the phase
  is actually concerned with -- see §11.

## 11. Why the dedup table holds only one entry per client

Section 20 of the phase's scope explicitly forbids solving snapshots or
Raft log compaction here, while also requiring the deduplication metadata
not to grow without bound and to be designed so a *future* snapshot
mechanism (Phase 9) can include it. Retaining only the last `(RequestID,
command shape, result)` per `ClientID` satisfies both constraints at once:
the table's size is bounded by the number of distinct clients, never by
the number of requests ever issued, and it is already exactly the shape a
future snapshot would need to serialize (one small record per client) --
no redesign implied later, no unbounded growth now.

## 12. Determinism and replica convergence

`KVStateMachine.Apply` makes no decision based on wall-clock time, host
identity, random numbers, or map iteration order that could affect its
result (`dedup` is a `map[string]dedupEntry`, but it is only ever indexed
by `cmd.ClientID`, never iterated over in a way that could affect an
outcome). Given the same committed `Command` sequence, applied by two
different `KVStateMachine` instances (wrapping two different `Store`
instances) starting from the same initial state, both replicas reach
identical final storage state and identical dedup records --
`TestKVStateMachine_MultipleReplicas_Converge` exercises this directly by
feeding an identical five-command sequence (including one intentional
duplicate) into three independent `KVStateMachine`/`Store` pairs and
diffing their final `Get` results.

`TestApplier_ClusterConvergesAfterMajorityCommit` goes one level further:
a real 3-node `raft` cluster (leader + two followers, using the existing
Phase 5 in-memory transport -- no new networking) proposes one `PUT`
through the leader, drains an extra heartbeat round so the followers'
`AppendEntries`-carried `LeaderCommit` catches up (exactly how it would in
production -- the entry commits on the leader *before* any follower's own
`AppendEntries` reply can carry the new commit index back out), and then
runs each node's own independent `Applier`/`KVStateMachine`/`Store` and
confirms all three converge on the same key/value state.

## 13. Concurrency

- `Applier.ApplyAvailable` serializes all application through its own
  mutex (§5) -- the single "one clear owner of application order" the
  phase requires.
- `KVStateMachine.Apply` holds its own mutex for the dedup-table
  read-modify-write and the storage call together, so two concurrent
  `Apply` calls (which should never happen if `Applier` is used correctly,
  but the type is exported and reusable) cannot interleave a dedup lookup
  with another goroutine's write.
- No blocking storage call is ever made while `raft.Node`'s own mutex is
  held -- see §5's last paragraph.
- `Applier.Run`'s background goroutine and a direct, manual
  `ApplyAvailable` call are safe to use concurrently (serialized by
  `applyMu`), which is exercised implicitly by every test that calls
  `ApplyAvailable` directly without ever starting `Run`, plus
  `TestApplier_Run_CatchesUpInBackground` for the background path itself.

## 14. Crash / restart behavior -- what is and is not guaranteed

This is the one area where Phase 7 is deliberately conservative rather
than over-promising, per the phase's own instruction to "document the
exact limitation rather than pretending it is solved."

**What Phase 6 already guarantees:** a restarted `raft.Node` recovers its
`currentTerm`, `votedFor`, and full log from `FilePersister`. **What Phase
6 explicitly does not persist:** `commitIndex` and `lastApplied` -- both
reset to `0` on every restart (see
`docs/raft/phase6-raft-persistence.md`'s "Commit-index recovery
semantics"). Phase 7 does not change this: **`KVStateMachine`'s dedup
table and `Node.lastApplied` are both in-memory-only state.** Neither is
persisted anywhere. Concretely, after a crash and restart:

```text
1. state machine applies command at index N to storage (dedup recorded)
2. process crashes
3. Raft restarts: currentTerm/votedFor/log recovered; commitIndex and
   lastApplied both reset to 0; a fresh KVStateMachine has an empty
   dedup table
4. Raft re-derives commitIndex from the recovered log via ordinary
   mechanics (ownership re-established as leader, or a fresh leader's
   LeaderCommit) -- unchanged from Phase 6
5. Applier starts from lastApplied=0 and replays the *entire* recovered
   log through the state machine again, from index 1
```

Step 5 is the important consequence: **every previously-applied entry is
re-applied from scratch after a restart**, not just the ones that were
applied but not yet acknowledged. This is safe *only* because:

- `internal/storage.Store.Put`/`Delete` are themselves idempotent: applying
  the same `PUT key=value` or `DELETE key` twice leaves the same final
  value in place, and `MemStore` already reconstructs its own MemTable
  from its own WAL independently of Raft (Phase 2), so replaying the same
  mutations again on top of already-recovered storage state converges to
  the same correct final state -- it does not corrupt anything.
- The re-applied WAL entries are simply redundant, not incorrect: this
  phase does not attempt to make that redundant work disappear (that
  would require persisting `lastApplied`, explicitly out of scope here).

**What this means for request deduplication specifically:** a client that
retried a request *before* the crash, whose retry lands *after* the
restart, will **not** be recognized as a duplicate -- the dedup table that
would have caught it is gone. For an idempotent command (any `PUT` or
`DELETE`, which this phase's command set is limited to) the *storage
outcome* is still correct either way, because storage-level idempotency
covers it independently of the dedup table. What is genuinely lost is the
stricter guarantee that a retried request always gets back its *exact
original `Result` value* across a crash boundary -- after a restart, that
same retried request is treated as brand new and gets a freshly computed
(but functionally equivalent) result instead of a replayed one.

**This is a deliberate, documented limitation, not a bug**: providing
crash-surviving exactly-once *result* semantics would require persisting
`lastApplied` and the dedup table durably and atomically with (or
carefully ordered against) each storage mutation -- real design work that
belongs with Phase 9's snapshotting story (§11 already shapes the dedup
table to be snapshot-ready for exactly that reason), not smuggled in here
as an afterthought.

## 15. Storage integration

`KVStateMachine` maps commands onto `internal/storage.Store` exactly as
specified:

```text
PUT key=value   -> store.Put(key, value)
DELETE key      -> store.Delete(key)
```

`GET` is intentionally not a state-machine command: a read does not need
to be replicated or ordered through Raft to be answered in this phase (a
full read-consistency / linearizability story is Phase 8.5's job, per the
phase boundary). Nothing here creates a second key/value engine, a new
on-disk format, or an alternate durability path -- every mutation this
package makes is a call into the exact same `Store` interface Phases 1-4
already built and tested.

## 16. Tests added

- `internal/raft/apply_test.go`: `LastApplied` starts at 0;
  `MarkApplied` advances monotonically; rejects a non-increasing index;
  rejects an index beyond `commitIndex`.
- `internal/statemachine/command_test.go`: `Command.Encode`/`DecodeCommand`
  round-trip for `PUT` and `DELETE`, including an empty value; unknown
  `Op` rejected on encode and decode; truncated and trailing-bytes
  payloads rejected.
- `internal/statemachine/statemachine_test.go`: `PUT` writes through;
  `PUT` then `DELETE` removes the key; a three-command sequence
  (`PUT`/`PUT`/`DELETE`) applies in order with the correct final state; a
  duplicate `PUT` and a duplicate `DELETE` each execute against storage
  exactly once (verified via a call-counting `Store` wrapper) and replay
  the identical original `Result`; two different clients reusing the same
  `RequestID` are treated completely independently; an old duplicate
  (`RequestID` 10 replayed after 11) is rejected with `ErrStaleRequest`
  and does not clobber the newer value; a request-ID conflict is rejected
  with `ErrRequestIDConflict` without mutating storage or the original
  dedup record; three independent `KVStateMachine`/`Store` pairs fed an
  identical command sequence converge on identical final state; an
  injected storage fault causes `Apply` to return a non-nil error without
  recording a dedup entry, and a subsequent healthy retry genuinely
  re-executes; an empty-key `PUT` is treated as a resolved
  `storage.ErrEmptyKey` result (nil `Apply` error), not a halting fault.
- `internal/statemachine/applier_test.go`: committed entries apply in
  strict order with the correct final state and `LastApplied`; entries
  that never commit (an isolated leader) never reach storage and
  `LastApplied` stays at 0; a no-op `ApplyAvailable` call when nothing new
  has committed applies nothing; an injected storage fault on the second
  of three committed entries stops `ApplyAvailable` there without
  advancing `LastApplied` past the first, and a later retry resumes at
  exactly that entry and completes the rest in order; a real 3-node Raft
  cluster's leader and both followers each run their own `Applier` and
  converge on identical state after a majority commit; `Applier.Run`'s
  background goroutine catches up on a commit without any direct
  `ApplyAvailable` call from the test.

## 17. Validation

```powershell
gofmt -w .            # clean
go vet ./...          # clean
go build ./...        # clean
go test ./...         # all packages pass
go test ./internal/raft/... -count=30        # pass
go test ./internal/statemachine/... -count=30 # pass
go run ./cmd/forgedb  # unchanged Phase 2 demo still runs end to end
```

`go test -race ./...` still fails with `-race requires cgo; enable cgo by
setting CGO_ENABLED=1` -- the same pre-existing Windows/no-C-compiler
limitation already documented in
`docs/raft/phase6-raft-persistence.md`, not something this phase
introduces.

`git diff --stat` against every tracked Phase 0-6 file is empty: this
phase added `internal/raft/apply.go`, `internal/raft/apply_test.go`,
the new `internal/statemachine` package, and this document, and modified
nothing else.

## 18. What remains intentionally out of scope

Per the phase boundary, Phase 7 does **not** include: Raft log
compaction, snapshots or snapshot installation (though the dedup table is
deliberately shaped to make that future work easier -- see §11), cluster
membership changes, read consistency or linearizable reads (`GET` remains
outside this phase's replicated command set entirely -- see §15), gRPC,
HTTP, a real network transport (the existing Phase 5 in-memory transport
is reused unchanged), a chaos testing framework, or metrics/observability
beyond what already existed. `internal/storage`'s WAL, MemTable, SSTable,
and compaction code was not touched, and neither was any existing
`internal/raft` file other than the two new additive methods described in
§6.
