package correctness

import (
	"fmt"
	"strings"
)

// Status is Check's three-way verdict (§45/§46 of the Phase 11 brief).
// Search exhaustion must never be reported as StatusViolation -- that
// would claim a correctness bug where there was only a resource limit; it
// is reported as StatusInconclusive instead.
type Status int

const (
	// StatusValid means Check found at least one sequential ordering of
	// every successfully-completed operation in the History that respects
	// real-time precedence and reproduces every observed result.
	StatusValid Status = iota

	// StatusViolation means Check exhaustively searched every ordering
	// consistent with real-time precedence (within budget) and none of
	// them reproduces the observed results: the History is not
	// linearizable.
	StatusViolation

	// StatusInconclusive means Check's search budget (CheckOptions.MaxStates)
	// was exhausted before either a valid linearization was found or every
	// possibility was ruled out. This is not evidence of a violation --
	// see Check's doc comment.
	StatusInconclusive
)

func (s Status) String() string {
	switch s {
	case StatusValid:
		return "VALID"
	case StatusViolation:
		return "VIOLATION"
	case StatusInconclusive:
		return "INCONCLUSIVE"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// Violation carries Check's best-effort diagnostic for a StatusViolation
// result (§15/§16 of the Phase 11 brief): not just "expected true, got
// false", but the deepest partial linearization the search managed to
// build, and the specific operation and mismatch that blocked every
// attempt to extend it further.
//
// The partial linearization is a genuine witness (every operation in it
// really does legally precede the next, and really does reproduce its
// recorded result when executed in that order against the reference
// model) -- only FailingOp, appended after it, is where every possible
// continuation failed.
type Violation struct {
	// Linearization lists, in order, the IDs of every operation the
	// search successfully placed before getting stuck.
	Linearization []int

	// FailingOp is the operation the search could not legally place next,
	// at the point described by Linearization, in any way that reproduced
	// its recorded result.
	FailingOp Operation

	// ExpectedFound/ExpectedValue describe what the reference model
	// actually holds for FailingOp.Key at the point in Linearization where
	// the search tried (and failed) to place it -- i.e. what a correct
	// Get would have had to observe, contrasted with what FailingOp.Found/
	// Result actually recorded.
	ExpectedFound bool
	ExpectedValue string
}

// CheckResult is Check's complete output.
type CheckResult struct {
	Status Status

	// Linearization lists, in order, the IDs of every completed operation
	// in the History -- only set when Status == StatusValid.
	Linearization []int

	// Violation is set only when Status == StatusViolation.
	Violation *Violation

	// StatesExplored is how many search nodes Check actually visited,
	// regardless of outcome -- useful for judging how close a
	// StatusInconclusive result came to the budget, and for tuning
	// CheckOptions.MaxStates.
	StatesExplored int

	// Budget is the MaxStates budget actually in effect for this Check
	// call (after CheckOptions' zero-value default was applied).
	Budget int
}

// Report renders result as a human-readable diagnostic, in the format
// §15 of the Phase 11 brief calls for: not a bare pass/fail, but enough
// to reproduce and understand the failure. h is the History result was
// computed from, used to render the full observed operation list
// alongside the verdict.
func (result CheckResult) Report(h History) string {
	var b strings.Builder
	fmt.Fprintf(&b, "LINEARIZABILITY CHECK: %s\n", result.Status)
	fmt.Fprintf(&b, "operation count: %d (completed: %d)\nstates explored: %d (budget: %d)\n\n",
		len(h.Ops), len(h.Completed()), result.StatesExplored, result.Budget)

	b.WriteString("observed history:\n")
	for _, op := range sortedByInvoke(h.Ops) {
		fmt.Fprintf(&b, "  %s\n", describeOp(op))
	}

	switch result.Status {
	case StatusValid:
		b.WriteString("\ncandidate linearization (operation IDs, in order):\n  ")
		b.WriteString(fmt.Sprint(result.Linearization))
		b.WriteString("\n")

	case StatusViolation:
		v := result.Violation
		b.WriteString("\nLINEARIZABILITY VIOLATION\n")
		b.WriteString("partial linearization reached (operation IDs, in order):\n  ")
		b.WriteString(fmt.Sprint(v.Linearization))
		b.WriteString("\n\nno legal placement found for:\n")
		fmt.Fprintf(&b, "  %s\n", describeOp(v.FailingOp))
		b.WriteString("\nreason:\n")
		if v.FailingOp.Kind == OpGet {
			fmt.Fprintf(&b, "  reference model at this point holds found=%v value=%q for key %q,\n  but the operation observed found=%v value=%q\n",
				v.ExpectedFound, v.ExpectedValue, v.FailingOp.Key, v.FailingOp.Found, v.FailingOp.Result)
		} else {
			b.WriteString("  no real-time-consistent placement of this write lets every other operation's observed result hold\n")
		}

	case StatusInconclusive:
		fmt.Fprintf(&b, "\nsearch budget of %d states was exhausted before a verdict was reached.\n", result.Budget)
		b.WriteString("this is NOT a linearizability violation -- increase CheckOptions.MaxStates or shrink the history to get a definite verdict.\n")
	}
	return b.String()
}

func describeOp(op Operation) string {
	var outcome string
	switch op.Outcome {
	case OutcomeOK:
		if op.Kind == OpGet {
			if op.Found {
				outcome = fmt.Sprintf("OK found=%q", op.Result)
			} else {
				outcome = "OK not-found"
			}
		} else {
			outcome = "OK"
		}
	case OutcomeFailed:
		outcome = fmt.Sprintf("FAILED err=%v", op.Err)
	case OutcomeIncomplete:
		outcome = "INCOMPLETE"
	}

	var val string
	if op.Kind == OpPut {
		val = fmt.Sprintf(" value=%q", op.Value)
	}

	completeStr := "?"
	if op.Outcome != OutcomeIncomplete {
		completeStr = fmt.Sprint(op.Complete)
	}

	return fmt.Sprintf("id=%d client=%s request=%d node=%s %s(%q)%s invoke=%d complete=%s -> %s",
		op.ID, op.ClientID, op.RequestID, op.Node, op.Kind, op.Key, val, op.Invoke, completeStr, outcome)
}

func sortedByInvoke(ops []Operation) []Operation {
	out := append([]Operation(nil), ops...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Invoke < out[j-1].Invoke; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
