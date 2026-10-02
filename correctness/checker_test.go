package correctness

import (
	"errors"
	"testing"
)

// This file is the Phase 11 brief's §36 "checker self-tests": synthetic
// histories built by hand, entirely independent of any live ForgeDB
// cluster, that pin down exactly what Check should and should not
// accept. Nothing here touches chaos.Cluster -- see integration_test.go
// for histories recorded from a real cluster.

func mustValid(t *testing.T, h History) CheckResult {
	t.Helper()
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	result := Check(h, CheckOptions{})
	if result.Status != StatusValid {
		t.Fatalf("Check: got %s, want VALID\n\n%s", result.Status, result.Report(h))
	}
	return result
}

func mustViolation(t *testing.T, h History) CheckResult {
	t.Helper()
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	result := Check(h, CheckOptions{})
	if result.Status != StatusViolation {
		t.Fatalf("Check: got %s, want VIOLATION\n\n%s", result.Status, result.Report(h))
	}
	return result
}

// op builds a completed, successful Put/Delete.
func opWrite(id int, kind OpKind, node, key, value string, invoke, complete int) Operation {
	return Operation{ID: id, ClientID: "c", RequestID: uint64(id), Kind: kind, Node: node,
		Key: key, Value: value, Invoke: invoke, Complete: complete, Outcome: OutcomeOK}
}

// opGet builds a completed, successful Get with the given observed
// result.
func opGet(id int, node, key string, found bool, value string, invoke, complete int) Operation {
	return Operation{ID: id, ClientID: "c", RequestID: uint64(id), Kind: OpGet, Node: node,
		Key: key, Invoke: invoke, Complete: complete, Outcome: OutcomeOK, Found: found, Result: value}
}

func opFailed(id int, kind OpKind, node, key string, invoke, complete int) Operation {
	return Operation{ID: id, ClientID: "c", RequestID: uint64(id), Kind: kind, Node: node,
		Key: key, Invoke: invoke, Complete: complete, Outcome: OutcomeFailed, Err: errors.New("injected")}
}

func opIncomplete(id int, kind OpKind, node, key, value string, invoke int) Operation {
	return Operation{ID: id, ClientID: "c", RequestID: uint64(id), Kind: kind, Node: node,
		Key: key, Value: value, Invoke: invoke, Outcome: OutcomeIncomplete}
}

// --- §36 "valid history" ----------------------------------------------

func TestChecker_ValidSequentialPutGet(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", true, "1", 3, 4),
	}}
	mustValid(t, h)
}

func TestChecker_ValidSequentialOverwrite(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opWrite(2, OpPut, "n0", "x", "2", 3, 4),
		opGet(3, "n0", "x", true, "2", 5, 6),
	}}
	mustValid(t, h)
}

func TestChecker_ValidDelete(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opWrite(2, OpDelete, "n0", "x", "", 3, 4),
		opGet(3, "n0", "x", false, "", 5, 6),
	}}
	mustValid(t, h)
}

func TestChecker_ValidIndependentKeys(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "a", "1", 1, 2),
		opWrite(2, OpPut, "n0", "b", "2", 3, 4),
		opGet(3, "n0", "a", true, "1", 5, 6),
		opGet(4, "n0", "b", true, "2", 7, 8),
	}}
	mustValid(t, h)
}

// --- §36 "invalid history" ----------------------------------------------

// Case B (§52): a GET that starts strictly after a PUT completes must
// observe the PUT's value; observing NOT_FOUND instead is a violation.
func TestChecker_InvalidStaleGet(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", false, "", 3, 4),
	}}
	result := mustViolation(t, h)
	if result.Violation.FailingOp.ID != 2 {
		t.Fatalf("violation blamed op %d, want 2\n\n%s", result.Violation.FailingOp.ID, result.Report(h))
	}
}

// --- §36 "valid concurrent history" -------------------------------------

// Case C (§52): two overlapping PUTs to the same key, with a GET that is
// compatible with at least one of the two legal orders, must be accepted.
func TestChecker_ValidOverlappingWrites(t *testing.T) {
	h := History{Ops: []Operation{
		// A: invoke=1 complete=4 ; B: invoke=2 complete=3 -- B is nested
		// inside A's interval, so both A-then-B and B-then-A are legal
		// real-time orderings.
		opWrite(1, OpPut, "n0", "x", "A", 1, 4),
		opWrite(2, OpPut, "n0", "x", "B", 2, 3),
		opGet(3, "n0", "x", true, "A", 5, 6),
	}}
	mustValid(t, h)
}

// A GET that overlaps with the only write to a key may legally observe
// either the pre-write or post-write value.
func TestChecker_ValidConcurrentPutGetEitherValue(t *testing.T) {
	older := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 5), // write overlaps the read below
		opGet(2, "n0", "x", false, "", 2, 3),    // read observes the pre-write state
	}}
	mustValid(t, older)

	newer := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 5),
		opGet(2, "n0", "x", true, "1", 2, 3), // read observes the post-write state
	}}
	mustValid(t, newer)
}

// --- §36 "invalid concurrent history" -----------------------------------

// No sequential ordering can explain this: x starts unset, so a GET
// completing strictly before any PUT is invoked can only observe
// NOT_FOUND, never a value.
func TestChecker_InvalidConcurrentHistory(t *testing.T) {
	h := History{Ops: []Operation{
		opGet(1, "n0", "x", true, "1", 1, 2), // finishes before the PUT even starts
		opWrite(2, OpPut, "n0", "x", "1", 3, 4),
	}}
	mustViolation(t, h)
}

// --- Real-time ordering (§40) --------------------------------------------

func TestChecker_RealTimeOrderingEnforced(t *testing.T) {
	// A completes (tick 2) strictly before B is invoked (tick 3): A must
	// be linearized before B. A GET of x after a DELETE that strictly
	// precedes it must observe NOT_FOUND; observing the old value would
	// require placing the GET before the DELETE, which real-time order
	// forbids.
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opWrite(2, OpDelete, "n0", "x", "", 3, 4),
		opGet(3, "n0", "x", true, "1", 5, 6), // claims to see the deleted value
	}}
	mustViolation(t, h)
}

// --- Incomplete / failed operations (§9/§39) -----------------------------

func TestChecker_IncompleteOperationExcluded(t *testing.T) {
	// An incomplete PUT must not be required to appear in the
	// linearization: a GET that never observes its value is still valid.
	h := History{Ops: []Operation{
		opIncomplete(1, OpPut, "n0", "x", "1", 1),
		opGet(2, "n0", "x", false, "", 2, 3),
	}}
	mustValid(t, h)
}

func TestChecker_IncompleteOperationMayHaveSilentlySucceeded(t *testing.T) {
	// Equally, an incomplete PUT imposes no requirement that it did NOT
	// take effect: a later GET observing its value must also be
	// accepted, since linearizability makes no claim either way about an
	// operation that never resolved.
	h := History{Ops: []Operation{
		opIncomplete(1, OpPut, "n0", "x", "1", 1),
		opGet(2, "n0", "x", true, "1", 2, 3),
	}}
	mustValid(t, h)
}

func TestChecker_FailedOperationExcluded(t *testing.T) {
	// Case E (§52): a failed/unavailable read must not be treated as a
	// successful read that returned an impossible value -- it is simply
	// excluded, and the rest of the history is checked on its own.
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opFailed(2, OpGet, "n1", "x", 3, 4),
		opGet(3, "n0", "x", true, "1", 5, 6),
	}}
	mustValid(t, h)
}

func TestChecker_FailedWriteDoesNotForceStateTransition(t *testing.T) {
	// A PUT that fails (e.g. ErrNotLeader) must not require x to become
	// its value -- here, the subsequent GET correctly observes NOT_FOUND.
	h := History{Ops: []Operation{
		opFailed(1, OpPut, "n0", "x", 1, 2),
		opGet(2, "n0", "x", false, "", 3, 4),
	}}
	mustValid(t, h)
}

// --- History validation (§38), kept separate from Check ------------------

func TestHistory_ValidateRejectsDuplicateID(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opWrite(1, OpPut, "n0", "y", "2", 3, 4),
	}}
	if err := h.Validate(); err == nil {
		t.Fatalf("Validate: want error for duplicate ID, got nil")
	}
}

func TestHistory_ValidateRejectsCompletionBeforeInvocation(t *testing.T) {
	h := History{Ops: []Operation{
		{ID: 1, Kind: OpPut, Invoke: 5, Complete: 2, Outcome: OutcomeOK},
	}}
	if err := h.Validate(); err == nil {
		t.Fatalf("Validate: want error for completion before invocation, got nil")
	}
}

func TestHistory_ValidateRejectsMissingErrOnFailed(t *testing.T) {
	h := History{Ops: []Operation{
		{ID: 1, Kind: OpGet, Invoke: 1, Complete: 2, Outcome: OutcomeFailed},
	}}
	if err := h.Validate(); err == nil {
		t.Fatalf("Validate: want error for Failed without Err, got nil")
	}
}

func TestHistory_ValidateAcceptsWellFormedHistory(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", true, "1", 3, 4),
		opFailed(3, OpGet, "n1", "x", 5, 6),
		opIncomplete(4, OpPut, "n0", "y", "9", 7),
	}}
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// --- Search-budget / inconclusive behavior (§45/§46) ---------------------

func TestChecker_BudgetExhaustionIsInconclusiveNotViolation(t *testing.T) {
	// A long fully-overlapping chain of writes to the same key makes the
	// search revisit many orderings before it can exhaust them; with the
	// budget set to effectively nothing, Check must report
	// StatusInconclusive, never StatusViolation, even though this
	// particular history actually is valid (every write is to a
	// different key, so every order succeeds immediately -- the budget
	// is set so low that the search cannot even complete it).
	ops := make([]Operation, 0, 8)
	for i := 1; i <= 8; i++ {
		ops = append(ops, opWrite(i, OpPut, "n0", "k", "v", 1, 100)) // all fully overlapping
	}
	h := History{Ops: ops}
	if err := h.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	result := Check(h, CheckOptions{MaxStates: 1})
	if result.Status != StatusInconclusive {
		t.Fatalf("Check with tiny budget: got %s, want INCONCLUSIVE\n\n%s", result.Status, result.Report(h))
	}
}

func TestChecker_SufficientBudgetFindsValidOrdering(t *testing.T) {
	ops := make([]Operation, 0, 6)
	for i := 1; i <= 6; i++ {
		ops = append(ops, opWrite(i, OpPut, "n0", "k", "v", 1, 100))
	}
	h := History{Ops: ops}
	mustValid(t, h) // DefaultMaxStates is plenty for 6 fully-overlapping, order-irrelevant writes
}

// --- Report formatting sanity -------------------------------------------

func TestCheckResult_ReportDoesNotPanicForEveryStatus(t *testing.T) {
	valid := History{Ops: []Operation{opWrite(1, OpPut, "n0", "x", "1", 1, 2)}}
	r := Check(valid, CheckOptions{})
	_ = r.Report(valid)

	violating := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", false, "", 3, 4),
	}}
	r2 := Check(violating, CheckOptions{})
	_ = r2.Report(violating)

	ops := make([]Operation, 0, 8)
	for i := 1; i <= 8; i++ {
		ops = append(ops, opWrite(i, OpPut, "n0", "k", "v", 1, 100))
	}
	inconclusive := History{Ops: ops}
	r3 := Check(inconclusive, CheckOptions{MaxStates: 1})
	_ = r3.Report(inconclusive)
}
