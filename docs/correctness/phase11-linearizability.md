# Phase 11: Correctness / Linearizability Testing

## 1. Why Phase 11 exists

Phases 5 through 9 built ForgeDB's replicated write path, and Phase 10
(`chaos/`) built a deterministic harness that deliberately breaks a
running cluster -- crashes, restarts, partitions, persistence/storage
faults -- and checks that ForgeDB's safety invariants (at most one leader
per term, monotonic term/commit/applied/snapshot indexes, eventual
convergence to identical logical state) hold throughout and that the
cluster converges once failures stop. Phase 10's own closing section asks
a narrower question than the one this phase answers:

> Does the system survive failures?

Phase 11 asks a different, strictly harder question:

> Given a concurrent history of client operations, failures, retries,
> leader changes, partitions, and recoveries, does the externally
> observable behavior satisfy linearizability?

This is a question about *client-visible operations*, not about internal
state. Two nodes converging to identical final KV content (Phase 10's
convergence check) says nothing about whether the sequence of PUT/GET/
DELETE calls that got them there could be explained by any legal
real-time ordering -- a bug that reorders two overlapping writes, or lets
a read observe a value that no legal ordering could have produced, can
still leave the final state looking perfectly normal. Phase 11 builds the
machinery to catch exactly that class of bug: an operation-history model,
a reference sequential KV specification, and a real linearizability
checker, and runs it against the actual ForgeDB cluster (via Phase 10's
own `chaos.Cluster`, never a second parallel implementation) under
deterministic sequential histories, deterministic concurrent/overlapping
histories, failure scenarios reusing Phase 10's fault injection, and a
bounded seeded randomized scenario.

## 2. Difference between Phase 10 and Phase 11

| | Phase 10 (`chaos/`) | Phase 11 (`correctness/`) |
|---|---|---|
| Question | Does the system survive failures? | Are the externally observed operations correct? |
| Unit of reasoning | Raft-level node state (term, role, commit/applied/snapshot index) and final logical KV state | Individual client-visible PUT/GET/DELETE operations, each with an invocation and completion |
| Checked against | Safety invariants (§9 of the Phase 10 doc) and final-state convergence | A reference sequential KV specification, via a real linearizability search |
| Concurrency model | A single deterministic scheduler tick advances the whole cluster | Overlapping operation intervals, explicitly recorded, searched over |
| New capability | Crash/restart, partition/heal, fault injection, randomized chaos scenarios | Operation-history recording, a backtracking linearizability checker, history minimization/replay |

Phase 11 does not redesign or duplicate Phase 10: `correctness.Harness`
(`correctness/harness.go`) is built directly on `*chaos.Cluster` and
calls nothing but `(*chaos.Cluster).Propose`/`ConsistentGet` (plus the
pre-existing `CrashNode`/`Partition`/etc. for failure scenarios) --
exactly the real `internal/raft`/`internal/statemachine`/`internal/storage`
code path every earlier phase already exercises.

## 3. Linearizability, informally

Linearizability means every operation appears to take effect at one
instant between its invocation and its completion, consistent with
every operation's own observed result. For non-overlapping operations
this reduces to "do things happen in the order they appear to"; the
interesting case is operations whose invocation/completion intervals
*overlap* -- there, more than one real-time-consistent ordering can be
legal, and a checker that only ever tries one fixed ordering (e.g. sort
by completion time and replay) will reject histories that are actually
fine, or worse, accept ones that are not.

## 4. Operation model (`correctness/operation.go`)

```go
type Operation struct {
    ID        int
    ClientID  string
    RequestID uint64
    Kind      OpKind   // OpGet, OpPut, OpDelete
    Node      string
    Key       string
    Value     string

    Invoke   int        // logical tick: always set
    Complete int        // logical tick: meaningful only if Outcome != OutcomeIncomplete

    Outcome Outcome      // OutcomeOK, OutcomeFailed, OutcomeIncomplete
    Found   bool         // Get + OutcomeOK only
    Result  string       // Get + OutcomeOK + Found only
    Err     error        // OutcomeFailed only
}
```

`Kind` covers exactly ForgeDB's public KV API: `OpPut`/`OpDelete`
(`(*dbnode.Node).Propose` with a `statemachine.Command`) and `OpGet`
(`(*dbnode.Node).ConsistentGet` -- never a local `storage.Store.Get`; see
§16 below). `ClientID`/`RequestID` are carried through unchanged from
`statemachine.Command`, so a genuine client retry is visible in a
`History` as two operations sharing the same pair (§15 below).

## 5. Invocation/completion model

Every `Operation` carries `Invoke`, and (when it resolved one way or the
other) `Complete`, as positions in a single, shared, strictly monotonic
logical sequence (`correctness.Clock`) -- never a wall-clock timestamp.
This is deliberate (§7 of the Phase 11 task): a `time.Now()`-based model
would make the resulting tests non-deterministic and timing-sensitive,
exactly what every earlier phase's own tests already avoid. Two
operations' `Invoke`/`Complete` values are only ever compared as plain
integers; the only property `Check` relies on is "lower value happened
first in whatever deterministic sequence of events produced this
`History`".

`History.Validate` (kept entirely separate from `Check` -- see §10) is
the single place that enforces `Invoke < Complete` for every operation
that actually completed one way or the other.

## 6. Real-time ordering

Two operations A and B are in a *forced* order -- A must precede B in
any legal linearization -- exactly when `A.Complete < B.Invoke`: A was
observed to finish before B was observed to start. When neither
`A.Complete < B.Invoke` nor `B.Complete < A.Invoke` holds, A and B
*overlap*, and either order may be legal, depending on what each one
observed. `correctness.Check` builds this precedence relation once per
search attempt (see §8) and only ever searches orderings consistent with
it -- it never falls back to sorting by completion time.

## 7. Reference KV specification (`correctness/model.go`)

```
PUT(k, v):    state[k] = v               -- always succeeds
DELETE(k):    delete(state, k)           -- always succeeds
GET(k):       found, v := state[k]       -- "not found" is a successful,
                                             resolved answer, never a failure
```

matching ForgeDB's actual, documented semantics
(`internal/storage.Store`/`statemachine.KVStateMachine`): `GET` on a
missing or deleted key returns `storage.ErrKeyNotFound`, which
`(*dbnode.Node).ConsistentGet` passes straight through -- it is not
`ErrReadUnavailable`, and `Harness.Resolve` maps it to `OutcomeOK` with
`Found == false`, never `OutcomeFailed` (§4.1/§8 below). The model's
initial state is always the empty map (§53 of the task); nothing in this
package seeds it with anything else, so a test against a cluster that
already holds data must record that data as ordinary preceding
operations, never as an assumed starting point.

## 8. History representation and the Failed/Incomplete/OK split

Every recorded `Operation` has exactly one `Outcome`:

- **`OutcomeOK`**: the operation definitely took effect (a
  committed-and-applied Put/Delete) or definitely observed a resolved
  value (a `ConsistentGet` that returned a value or a definitive
  "not found"). Only these feed the sequential-specification search.
- **`OutcomeFailed`**: the operation definitely did *not* take effect,
  and the caller received a clear, resolved negative answer
  (`dbnode.ErrNotLeader`, `dbnode.ErrReadUnavailable`, or any other
  error `Propose`/`ConsistentGet` can return). A failed operation
  imposes **no** state-transition requirement and is excluded from the
  linearization entirely.
- **`OutcomeIncomplete`**: the operation was invoked, but no resolved
  answer was ever observed (§9 below).

This is the direct implementation of the task's repeated warning (its
§8/§9/§11): `PUT(x,1) -> ErrNotLeader` does not mean `x` must become `1`,
and `ConsistentGet(x) -> ErrReadUnavailable` imposes no value
requirement. `History.Validate` enforces the structural half of this
(a `Failed` operation must carry a non-nil `Err`; an `OK` Get with
`Found == false` must carry an empty `Result`; and so on) completely
separately from `Check`'s own linearizability reasoning (§38 of the
task) -- a history can be *well-formed* and still *not linearizable*, and
only `Validate` rejecting it means something structural is wrong, versus
`Check` reporting `StatusViolation`, which means the recorded behavior
itself is inconsistent with any legal ordering.

## 9. Incomplete operations

An operation that was invoked but never resolved (e.g. its proposing node
crashed before a bounded wait could confirm commit/apply, or a test
deliberately interrupts it -- see `Harness.ResolveInterrupted`) is
modeled as `OutcomeIncomplete`. Per the classical Herlihy & Wing
definition of linearizability for a history with pending invocations,
`Check` does **not** simply drop every incomplete operation and move on;
it tries both of the two things that could legitimately have happened:

- the operation never actually took effect (excluded from the
  linearization), or
- the operation silently took effect at some point no earlier than its
  own `Invoke` (included, with no upper bound on where, and -- crucially
  -- never itself acting as another operation's real-time predecessor,
  since nothing about *when* it finished was ever observed).

`Check` enumerates every inclusion/exclusion combination across a
history's incomplete Put/Delete operations (bounded by
`maxOptionalWrites = 12`; beyond that it falls back to just
"exclude everything" / "include everything") and accepts the history if
*any* combination linearizes. An incomplete `Get` is never a candidate
for inclusion at all: nothing about it was ever observed, so there is no
response to give it that could matter to any other operation's own
check -- it is always simply excluded, with no loss of generality.

This is a real behavioral choice, documented here rather than left
implicit (the task's own §39/§40 explicitly demand this): a later,
successful `Get` observing an incomplete write's value is *accepted*,
not rejected, because the write might genuinely have landed before its
node crashed.

## 10. Linearizability algorithm (`correctness/checker.go`)

`Check(h History, opts CheckOptions) CheckResult` assumes `h` is already
structurally valid (call `h.Validate()` first -- see §8); it never
validates structure itself, keeping the two concerns separate per the
task's own instruction.

For one candidate set (the unconditionally-completed operations, plus
one chosen inclusion/exclusion combination of incomplete writes -- §9),
the search is a bounded recursive backtracking search:

1. Build the real-time precedence relation (`preds[B]` = every candidate
   `A` with `A.Complete < B.Invoke`) once.
2. At each step, a candidate is **ready** if every operation in its
   `preds` list has already been linearized. Because this relation is a
   strict partial order (it cannot contain a cycle: `A.Complete <
   B.Invoke` and `B.Complete < A.Invoke` together would force
   `A.Invoke < A.Invoke`), there is always at least one ready, not-yet-
   placed candidate while any remain -- a direct consequence of every
   finite DAG having a topological order.
3. For each ready candidate, in turn: clone the current reference-model
   state, tentatively apply the candidate, and check whether the result
   matches what was actually observed (`model.matches`). If it matches,
   recurse with that candidate placed and the cloned state committed; if
   the full recursion later fails, backtrack and try the next ready
   candidate instead. If it does not match, record diagnostic
   information (see §13) and try the next ready candidate without
   recursing.
4. Success is placing every candidate.

This is deliberately **not** "sort by completion time and replay" (the
task's own explicit warning: that is wrong the moment two operations
overlap) and deliberately **not** a general theorem prover with
memoization or SAT-style optimization -- for the bounded history sizes
this phase targets (a handful of clients, a few dozen operations), plain
backtracking bounded by a search budget is sufficient and is what the
task asks for.

## 11. Search bounds and Valid/Violation/Inconclusive semantics

`CheckOptions.MaxStates` (default `DefaultMaxStates = 200_000`) bounds
how many search nodes `Check` visits in total, across every
inclusion/exclusion attempt from §9. `CheckResult.Status` is one of
three values, and the distinction is load-bearing:

- **`StatusValid`**: a complete, legal linearization was found.
- **`StatusViolation`**: every combination of ready-candidate choices,
  across every inclusion/exclusion attempt, was exhausted (within
  budget) without ever completing a legal linearization -- the history
  genuinely is not linearizable.
- **`StatusInconclusive`**: the search budget ran out before either of
  the above could be established. **This is never reported as
  `StatusViolation`** -- exhausting a resource bound is not evidence of
  a real bug, and conflating the two was the task's single most
  frequently repeated warning (§45/§46, restated across the test-matrix
  and definition-of-done sections). `TestChecker_BudgetExhaustionIsInconclusiveNotViolation`
  (`correctness/checker_test.go`) pins this down directly: a history that
  actually is valid still reports `StatusInconclusive`, not
  `StatusValid` or `StatusViolation`, when given a budget too small to
  finish exploring it.

## 12. Counterexample / violation reporting (`correctness/result.go`)

A `StatusViolation` result carries a `*Violation`: the deepest partial
linearization any search branch managed to build (a genuine witness --
every operation in it really does legally precede the next and really
does reproduce its observed result when replayed in that order), the
specific operation that could not be legally placed next in any way that
matched its observation, and -- for a `Get` -- what the reference model
actually held at that point versus what was observed. `CheckResult.Report(h)`
renders this, together with the full observed history (sorted by
`Invoke`) and the search budget actually used, in the format:

```
LINEARIZABILITY CHECK: VIOLATION
operation count: ... (completed: ...)
states explored: ... (budget: ...)

observed history:
  id=... client=... request=... node=... Kind("key") invoke=... complete=... -> OUTCOME

LINEARIZABILITY VIOLATION
partial linearization reached (operation IDs, in order):
  [...]

no legal placement found for:
  id=... ...

reason:
  reference model at this point holds found=... value="..." for key "...",
  but the operation observed found=... value="..."
```

Every live test in this package that asserts `StatusViolation` prints
this report (and, for a live-cluster history, `chaos.Cluster.Dump()`
alongside it) on failure, so a CI failure is diagnosable without
re-deriving anything by hand.

## 13. History minimization (`correctness/reducer.go`)

`Minimize(h History, opts CheckOptions, maxPasses int) (History, CheckResult)`
implements the task's explicitly-optional, explicitly-simple bounded
reducer: given an `h` that `Check` already reports as `StatusViolation`,
it repeatedly tries dropping one operation at a time, keeping the drop
whenever the remaining history is *still* a violation, until a full pass
removes nothing further or `maxPasses` is reached. This is a plain greedy
reducer, not a delta-debugging implementation -- it is not guaranteed to
find the globally smallest counterexample, only a smaller one, which is
exactly what the task asks for ("do not spend excessive complexity...a
simple bounded reducer is enough"). `TestMinimize_ShrinksAFailingHistory`
demonstrates it reducing an 11-operation history (8 unrelated filler
writes plus the 2 operations that actually conflict) down to exactly the
2 that matter.

## 14. Replay (`correctness/replay.go`)

`SaveHistory`/`LoadHistory` serialize a `History` to/from plain JSON
(deliberately not a custom binary format -- the task explicitly rules
that out), so a failing history can be attached to a bug report and
re-checked later without re-running a live cluster at all;
`TestSaveLoadHistory_CheckResultMatchesAfterRoundTrip` confirms a
round-tripped history produces the identical `Check` verdict.
`Operation.Err` cannot round-trip through JSON as the `error` interface,
so a loaded history's `Err` is a `replayError` carrying only the original
message -- sufficient for `Check` (which only ever inspects presence/
absence of `Err`, never its identity via `errors.Is`) and for `Report`.

For the *randomized* correctness scenario (§15 below), the primary replay
mechanism is simply the seed itself, exactly as `chaos`'s own randomized
scenario already works: `RandomCorrectnessOptions.Seed` alone determines
every random choice `RunRandomCorrectness` makes, so
`CORRECTNESS_SEED=<n> go test -run TestRandomCorrectness` reproduces an
exact failing run.

## 15. Deterministic client harness (`correctness/harness.go`)

`chaos.Cluster` documents itself as **not** safe for concurrent use by
multiple goroutines -- every chaos scenario is a single sequential
narrative. `Harness` respects that constraint rather than working around
it with goroutines and locks. Every operation is split into two
explicit, separately-timestamped steps a test script drives by hand:

1. **`Invoke`** records a new tick on the shared `Clock` and returns a
   `*PendingOp` -- no call against the cluster happens yet.
2. **`Resolve`** (or **`ResolveInterrupted`**) performs the real,
   synchronous call against the cluster (`Propose` + a bounded wait for
   apply, for Put/Delete; `ConsistentGet`, for Get), determines the
   `Outcome`, and records the completion tick.

A test script creates genuine overlap deterministically simply by
calling `Invoke` for two different operations before calling `Resolve`
on either one: `Invoke(A); Invoke(B); Resolve(A); Resolve(B)` produces
`A.Invoke < B.Invoke < A.Complete < B.Complete`, a real overlap by
`Operation`'s own definition, even though the two underlying cluster
calls still happen one at a time, in-process, never concurrently. This
is a deliberate simplification, called out explicitly here rather than
left as a silent assumption: the resulting *history* is a faithful
stand-in for what two genuinely concurrent clients would observe (the
same invoke/complete ordering, the same results), even though no two
goroutines are ever actually racing inside `chaos.Cluster`.

`Resolve`'s outcome mapping (§8's three-way split, concretely):

| Call | Result | Outcome |
|---|---|---|
| `Propose` | immediate error (e.g. `ErrNotLeader`) | `OutcomeFailed` |
| `Propose` succeeds, `WaitForApplied` confirms within bound | -- | `OutcomeOK` |
| `Propose` succeeds, `WaitForApplied` times out | -- | `OutcomeIncomplete` |
| `ConsistentGet` | nil error | `OutcomeOK`, `Found=true` |
| `ConsistentGet` | `storage.ErrKeyNotFound` | `OutcomeOK`, `Found=false` |
| `ConsistentGet` | `ErrNotLeader`/`ErrReadUnavailable`/other | `OutcomeFailed` |

`ResolveInterrupted` is the deliberate counterpart for §9's incomplete-
write scenarios: it proposes exactly as `Resolve` would, but the moment
the proposal is accepted (no immediate error), it calls the caller's
`interrupt` closure (e.g. `c.CrashNode(node)`) and records
`OutcomeIncomplete` unconditionally, without ever checking whether the
write actually committed.

## 16. ConsistentGet / ReadIndex interaction

Every `Get` this package ever records goes through
`(*chaos.Cluster).ConsistentGet`, which itself calls
`(*dbnode.Node).ConsistentGet` -- never a direct
`storage.Store.Get`/`Cluster.LogicalState` lookup. This matters because
only `ConsistentGet` carries the linearizable-read guarantee Phase 8.5
built (`ReadIndex` + an apply-barrier wait); a local read can observe
stale state on a lagging follower. `TestLive_HarnessNeverUsesLocalReads`
pins this down directly: a read against a node that is leader but
isolated into a minority must come back `OutcomeFailed`
(`ErrReadUnavailable`), which only happens if the harness is actually
calling through `ConsistentGet`'s majority-confirmation round, not a
local lookup that would happily return the (stale but locally present)
value.

## 17. Chaos (Phase 10) integration

`Harness` is built directly on `*chaos.Cluster` -- the real Phase 10
harness, never a duplicate. Every failure scenario in
`integration_test.go` reuses Phase 10's own primitives verbatim:
`CrashNode`/`RestartNode`, `Partition`/`Heal`, `WaitForLeader`,
`WaitForSnapshotIndex`, `CreateSnapshot`, `Settle`/`Advance`. The
randomized correctness scenario (`correctness/random.go`) likewise calls
`CrashNode`/`RestartNode`/`Partition`/`Heal` directly for its bounded
fault injection, rather than reimplementing anything. The one genuinely
new piece of infrastructure Phase 11 adds on top is the operation-history
model and the checker itself -- nothing about Raft, the state machine, or
storage was changed to support this phase (see §19 below).

## 18. Deterministic correctness tests (`correctness/integration_test.go`)

Covers, against a real `chaos.Cluster`: sequential PUT/GET, sequential
overwrite, DELETE, independent keys, read-after-write, delete+recreate,
genuinely overlapping PUT/PUT and PUT/GET (via `Invoke`/`Resolve`
splitting), concurrent reads, a leader crash followed by a read from the
new leader, an incomplete write interrupted by a crash, a partitioned
minority leader's unavailable read, a client retry across a leader
change, and snapshot catch-up followed by a read from whichever node a
natural re-election elects leader. Every one of these is also exercised
purely synthetically, independent of any live cluster, in
`checker_test.go` (§19 below) -- the live tests confirm the real system's
actual behavior matches what the synthetic tests already established the
checker accepts/rejects correctly.

## 19. Checker self-tests (`correctness/checker_test.go`, `model_test.go`)

Per the task's explicit instruction ("do not trust the checker until it
has been tested against known histories"), every self-test here is built
by hand, entirely independent of `chaos.Cluster`:

- Valid: sequential PUT/GET, overwrite, delete, independent keys,
  genuinely overlapping writes with a GET compatible with at least one
  legal order, a GET observing either the pre- or post-write value of an
  overlapping write.
- Invalid: a stale GET after a non-overlapping PUT; a GET that finishes
  before a PUT it claims to have observed was even invoked; real-time
  order violated by a GET claiming to see a value a later DELETE already
  removed.
- Incomplete/failed handling: an incomplete write excluded without being
  required; an incomplete write that *may have silently succeeded*,
  accepted; a failed read excluded without poisoning the rest of the
  history; a failed write that does not force any state transition.
- `History.Validate`: duplicate IDs, completion-before-invocation, a
  `Failed` operation missing `Err`, and a well-formed history accepted.
- Search-budget behavior: a tiny budget reports `StatusInconclusive`
  (never `StatusViolation`) on a history that is actually valid; a
  sufficient budget finds it.
- `model_test.go` exercises the reference KV specification directly:
  initial empty state, overwrite, delete, delete+recreate, independent-
  key commutativity, clone independence, and `model.matches`'s exact
  found/value distinction.

## 20. Randomized correctness testing (`correctness/random.go`, `random_test.go`)

`RunRandomCorrectness(c *chaos.Cluster, opts RandomCorrectnessOptions)`
drives a bounded, seeded script of invoke/resolve/fault steps (PUT,
DELETE, GET, and -- if `InjectFaults` is set -- a bounded number of
crash+restart or partition+heal cycles) against a fresh `Harness`,
returning the resulting `History`. `opts.Seed` alone determines every
random choice; the same seed always reproduces the identical script.
`TestRandomCorrectness_BoundedSeededScenario` runs a 5-node cluster for
25 operations with fault injection enabled at a fixed default seed (`42`,
overridable via `CORRECTNESS_SEED`); `TestRandomCorrectness_MultipleSeeds`
runs five additional fixed seeds at a smaller size as further coverage.
Neither test attempts to drive the cluster to final convergence
afterward (unlike `chaos`'s own randomized scenario) -- Phase 11's claim
is entirely about the recorded operation history, via `Check`, never
about final KV state equality.

## 21. Failure/restart, snapshot, and deduplication coverage

- **Failure/restart**: `TestLive_LeaderChange_WriteSurvivesAndIsVisible`,
  `TestLive_IncompleteWrite_InterruptedByCrash`,
  `TestLive_PartitionedMinorityLeader_UnavailableReadExcluded`.
- **Snapshot**: `TestLive_SnapshotCatchUp` (writes, isolate a follower,
  snapshot the majority, heal, wait for `InstallSnapshot`, read from
  whichever node ends up leading afterward).
- **Deduplication**: `TestLive_RetryAcrossLeaderChange` -- a retried
  identical `(ClientID, RequestID)` across a leader change is recorded
  as two ordinary `Operation`s sharing that pair, and `Check` needs no
  special-casing for it (see §22 below): `statemachine`'s dedup table
  resolves the retry as a replay internally, but externally it is just
  another successful PUT of the same value, which the reference model
  already treats as idempotent.
- **Read consistency**: `TestLive_HarnessNeverUsesLocalReads` (§16) and
  the minority-leader case above.

## 22. Linearizability vs. exactly-once execution

These are different properties, and this package is careful not to
conflate them. Linearizability is about whether a recorded sequence of
*observable* operations can be explained as if each occurred atomically
at some instant consistent with real time. Deduplication (Phase 7) is
about whether a *repeated* client request is executed more than once
internally. A retried `(ClientID, RequestID)` pair produces two
`Operation`s in a `History`; `Check` treats them as two ordinary
successful PUTs of the same value, which the reference model handles
correctly (applying `PUT(x, v)` twice is already idempotent) without
needing to know anything about the internal dedup table at all. This
package never claims "operation executed exactly once internally" from
an externally valid history -- only `internal/statemachine`'s own tests
(Phase 7) and `chaos`'s dedup scenario (Phase 10, via `PutCount`) make
that specific claim, by directly counting real `Store.Put` calls.

## 23. Known limitations

- **The search is exponential in the worst case.** It is bounded by
  `CheckOptions.MaxStates` and reports `StatusInconclusive` rather than a
  false verdict when that bound is hit (§11) -- but a sufficiently
  adversarial, highly-overlapping history can still exhaust a generous
  budget. This is expected and documented, not a bug: the task
  explicitly permits this trade-off for a checker this simple.
- **No memoization.** The search does not cache (candidate-set, model-
  state) pairs across branches, so it can re-explore equivalent states
  reached via different orderings. Acceptable for the bounded history
  sizes this phase targets; a future phase wanting much larger histories
  would need a smarter algorithm (see §24).
- **Incomplete-write enumeration is combinatorial**, bounded by
  `maxOptionalWrites = 12`; a history with more incomplete writes than
  that falls back to only trying "exclude everything"/"include
  everything" rather than every combination, which could in principle
  miss a linearization that requires including some but not all of a
  larger set of incomplete writes. No test in this phase approaches that
  bound.
- **`Minimize` is a simple greedy reducer**, not delta-debugging; it
  finds *a* smaller counterexample, not necessarily the smallest one.
- **No real network or wall-clock timing.** Like Phase 10, everything
  here runs in-process against `InMemoryTransport`, driven by the same
  deterministic logical scheduler -- see `correctness.Clock`'s doc
  comment on why that is deliberate, not a gap.
- **`go test -race` could not be run** on this package either, for the
  same pre-existing reason documented in every earlier phase: this
  Windows environment's Go toolchain lacks the CGO/C-toolchain support
  `-race` requires (`cgo: C compiler "gcc" not found` /
  `-race requires cgo; enable cgo by setting CGO_ENABLED=1`). This is an
  environment limitation, not something Phase 11 introduced or could
  route around; `go test ./correctness/... -count=10` (and higher) was
  run instead as the available substitute for flakiness/ordering checks.

## 24. What Phase 11 proves, and what remains

Phase 11 establishes that, for every deterministic sequential history,
every deterministic overlapping/concurrent history, every deterministic
failure scenario (leader crash, incomplete write, partitioned minority
leader, retry across a leader change, snapshot catch-up), and every
randomized bounded scenario this package actually ran, ForgeDB's real
`ConsistentGet`/`Propose` API produced an operation history that *is*
linearizable -- i.e., explainable by at least one legal sequential
ordering consistent with real-time precedence and the reference KV
specification. It also proves the checker itself is meaningful: it
rejects every hand-built history that genuinely is not linearizable
(§19), and distinguishes a real violation from a merely-exhausted search
budget (§11).

This is **not** a formal, exhaustive proof of ForgeDB's correctness. It
is an executable correctness-testing framework whose coverage is exactly
the set of histories it was actually run against -- the deterministic
cases enumerated above, plus whatever the bounded randomized scenario's
fixed seeds happened to explore. A different seed, a larger cluster, a
longer history, or a fault interleaving none of these scenarios tried
could still expose a bug this phase's runs did not encounter.

What a later phase could build on top: a model-checker-style exhaustive
state-space exploration (rather than this phase's seeded random
sampling) for small configurations; a smarter, memoized linearizability
algorithm (e.g. a Wing-Gries-style visited-state cache) to support much
larger histories within the same time budget; extending the operation
model to cover a richer API surface if ForgeDB's public API grows beyond
PUT/GET/DELETE; and, as the original brief's own scope boundary states,
anything beyond this phase -- Phase 12 and later -- is deliberately not
started here.
