package correctness

// DefaultMaxStates bounds how many search nodes Check will visit before
// giving up and reporting StatusInconclusive, when CheckOptions.MaxStates
// is left at zero. It is generous enough for the small, bounded histories
// §14/§35 of the Phase 11 brief call for (a handful of clients, a few
// dozen operations at most) while still keeping `go test ./...` fast --
// see §45/§46: search exhaustion is a resource limit, not a verdict, so
// this number matters for turnaround time, not correctness.
const DefaultMaxStates = 200_000

// CheckOptions configures Check.
type CheckOptions struct {
	// MaxStates bounds the number of search nodes Check will visit. Zero
	// means DefaultMaxStates.
	MaxStates int
}

// Check determines whether h is linearizable (§4/§11 of the Phase 11
// brief): whether there exists a sequential ordering of every
// successfully-completed operation in h (see History.Completed) that
// simultaneously
//
//  1. respects real-time precedence -- if operation A completed (at
//     A.Complete) before operation B was invoked (at B.Invoke), A must
//     appear before B in the ordering; and
//  2. reproduces, when replayed against the reference model (model.go),
//     exactly the result every operation in it actually observed.
//
// Every successfully-completed operation appears in the ordering exactly
// once (Rule 3, enforced structurally: the search below is a permutation
// of exactly that set). Failed and incomplete operations are excluded
// entirely (Rule 4) -- see History.Completed and Operation's doc comment.
//
// Check assumes h is already structurally valid; call h.Validate() first.
// It does not call Validate itself, keeping history validation and
// linearizability checking entirely separate (§38).
//
// Check never sorts by completion time and replays in that fixed order
// (§12: that is specifically wrong for overlapping operations) -- it
// performs a bounded backtracking search (§13) over every ordering
// consistent with Rule 1, trying each one against the reference model
// and backtracking on a mismatch, exactly the approach §13/§50 call for.
// It is not a general theorem prover and does not use memoization or any
// other asymptotic optimization beyond the real-time precedence pruning
// already described -- for the bounded history sizes this phase targets
// (§14/§35), that is sufficient, and keeping it simple is itself a
// requirement (§13: "do not over-engineer it into a general theorem
// prover").
//
// If the search exhausts CheckOptions.MaxStates before reaching a
// definite answer, Check returns StatusInconclusive, never
// StatusViolation (§45/§46): exhausting a search budget is never evidence
// of a real violation.
//
// Incomplete Put/Delete operations (§39/§40) are handled per the
// classical Herlihy & Wing definition of linearizability for a history
// with pending invocations: each one may either be entirely excluded from
// the linearization (the operation never actually took effect, as far as
// anyone can observe) or included at some point no earlier than its own
// Invoke (it may have silently taken effect before anyone could confirm
// it, e.g. a commit that landed a moment before its node crashed) --
// Check tries every combination of inclusion/exclusion across a history's
// incomplete writes and accepts the history if any combination
// linearizes. An included incomplete write is never itself required by,
// or able to satisfy, any other operation's real-time precedence (it has
// no observed Complete to reason from); it only receives the ordinary
// Invoke-based lower bound every other operation would.
//
// An incomplete Get is never a candidate for inclusion: nothing about it
// was ever observed, so there is no response to give it that could matter
// to any other operation's own check -- it is always simply excluded.
//
// This combinatorial enumeration is bounded by maxOptionalWrites: a
// history with more incomplete writes than that falls back to only
// trying "exclude everything" and "include everything" rather than every
// 2^k combination -- see the package doc on known limitations. Every
// bounded test this phase ships stays well under that limit.
func Check(h History, opts CheckOptions) CheckResult {
	maxStates := opts.MaxStates
	if maxStates <= 0 {
		maxStates = DefaultMaxStates
	}

	completed := h.Completed()
	var optional []Operation
	for _, op := range h.Ops {
		if op.Outcome == OutcomeIncomplete && (op.Kind == OpPut || op.Kind == OpDelete) {
			optional = append(optional, op)
		}
	}

	subsets := optionalSubsets(len(optional))

	best := CheckResult{Status: StatusViolation, Violation: &Violation{}}
	sawInconclusive := false
	budgetUsed := 0
	for _, mask := range subsets {
		remainingBudget := maxStates - budgetUsed
		if remainingBudget <= 0 {
			sawInconclusive = true
			break
		}

		included := make([]Operation, 0, len(optional))
		for i, op := range optional {
			if mask&(1<<i) != 0 {
				included = append(included, op)
			}
		}

		s := newSearcher(completed, included, remainingBudget)
		result := s.run()
		budgetUsed += result.StatesExplored

		if result.Status == StatusValid {
			result.StatesExplored = budgetUsed
			result.Budget = maxStates
			return result
		}
		if result.Status == StatusInconclusive {
			sawInconclusive = true
			continue
		}
		if result.Violation != nil && len(result.Violation.Linearization) >= len(best.Violation.Linearization) {
			best = result
		}
	}

	if sawInconclusive {
		best.Status = StatusInconclusive
	}
	best.StatesExplored = budgetUsed
	best.Budget = maxStates
	return best
}

// optionalSubsets returns every bitmask to try for k optional operations:
// every one of 0..2^k-1 when that is small enough to be cheap (k <=
// maxOptionalWrites), or just {0, 2^k-1} (exclude everything / include
// everything) otherwise -- see Check's doc comment.
const maxOptionalWrites = 12

func optionalSubsets(k int) []int {
	if k == 0 {
		return []int{0}
	}
	if k > maxOptionalWrites {
		return []int{0, (1 << k) - 1}
	}
	n := 1 << k
	subsets := make([]int, n)
	for i := range subsets {
		subsets[i] = i
	}
	return subsets
}

// searcher holds one search attempt's fixed inputs (a specific candidate
// set -- every unconditionally-completed operation, plus whichever
// incomplete writes this attempt chose to treat as included, see Check --
// and the real-time precedence relation derived from it) and mutable
// search state (how many nodes have been visited, and the best
// diagnostic found so far for a StatusViolation result).
type searcher struct {
	ops   map[int]Operation
	ids   []int
	preds map[int][]int // op ID -> IDs that must be linearized before it

	maxStates      int
	states         int
	budgetExceeded bool

	result []int // the successful linearization, once found

	// Diagnostic tracking for a StatusViolation report (§15/§16): the
	// deepest partial linearization any search branch reached, and the
	// operation/mismatch that blocked every attempt to extend it.
	bestDepth    int
	bestPrefix   []int
	bestFailedID int
	bestFoundAt  bool
	bestValueAt  string
}

// newSearcher builds a searcher for one candidate set: required
// (unconditionally-completed operations) plus included (this attempt's
// chosen subset of incomplete writes -- see Check). Only operations in
// required ever act as another operation's real-time predecessor: an
// included incomplete write has no observed Complete to reason from, so
// it is never placed in anyone else's preds list (though it still
// receives its own, from required's Complete values versus its own
// Invoke) -- see Check's doc comment.
func newSearcher(required, included []Operation, maxStates int) *searcher {
	all := make([]Operation, 0, len(required)+len(included))
	all = append(all, required...)
	all = append(all, included...)

	s := &searcher{
		ops:       make(map[int]Operation, len(all)),
		ids:       make([]int, 0, len(all)),
		preds:     make(map[int][]int, len(all)),
		maxStates: maxStates,
	}
	for _, op := range all {
		s.ops[op.ID] = op
		s.ids = append(s.ids, op.ID)
	}
	for _, b := range all {
		for _, a := range required {
			if a.ID != b.ID && a.Complete < b.Invoke {
				s.preds[b.ID] = append(s.preds[b.ID], a.ID)
			}
		}
	}
	return s
}

// run executes the search and reports a CheckResult scoped to this one
// candidate set (StatesExplored/Budget are filled in relative to this
// attempt's own maxStates; Check rescales them across every attempt it
// makes).
func (s *searcher) run() CheckResult {
	linearized := make([]int, 0, len(s.ids))
	done := make(map[int]bool, len(s.ids))
	ok := s.search(s.ids, linearized, done, newModel())

	result := CheckResult{StatesExplored: s.states, Budget: s.maxStates}
	switch {
	case s.budgetExceeded:
		result.Status = StatusInconclusive
	case ok:
		result.Status = StatusValid
		result.Linearization = append([]int(nil), s.result...)
	default:
		result.Status = StatusViolation
		result.Violation = s.bestFailure()
	}
	return result
}

// search tries to extend the partial linearization `done` (whose members
// are exactly `linearized`, in order) to cover every operation in
// `remaining`, against model state m. It returns true, with s.result set,
// the moment a complete, valid linearization is found. It returns false
// (with s.result left untouched) if every ready candidate at this point
// leads to failure, and sets s.budgetExceeded if the search budget runs
// out first -- the caller must check that flag before trusting a false
// return as a genuine violation.
func (s *searcher) search(remaining []int, linearized []int, done map[int]bool, m *model) bool {
	if len(done) == len(s.ops) {
		s.result = append([]int(nil), linearized...)
		return true
	}

	for _, id := range remaining {
		if done[id] {
			continue
		}
		if !s.ready(id, done) {
			continue
		}

		s.states++
		if s.states > s.maxStates {
			s.budgetExceeded = true
			return false
		}

		op := s.ops[id]
		candidate := m.clone()
		foundAt, valueAt := candidate.apply(op)
		if op.Kind == OpGet && (foundAt != op.Found || (foundAt && valueAt != op.Result)) {
			s.recordFailure(len(linearized), linearized, id, foundAt, valueAt)
			continue
		}

		done[id] = true
		next := append(linearized, id)
		if s.search(remaining, next, done, candidate) {
			return true
		}
		done[id] = false
		if s.budgetExceeded {
			return false
		}
	}
	return false
}

// ready reports whether every operation that must precede id (per
// real-time precedence -- s.preds[id]) has already been linearized. Since
// that precedence relation is a strict partial order over a finite set (it
// can have no cycle: id -> a.Complete < id.Invoke and a -> id.Complete <
// a.Invoke together would require id.Invoke < id.Invoke), there is always
// at least one ready, not-yet-done operation whenever `done` is not yet
// everything -- a plain consequence of every finite DAG having a
// topological order.
func (s *searcher) ready(id int, done map[int]bool) bool {
	if done[id] {
		return false
	}
	for _, p := range s.preds[id] {
		if !done[p] {
			return false
		}
	}
	return true
}

func (s *searcher) recordFailure(depth int, prefix []int, failedID int, foundAt bool, valueAt string) {
	if depth < s.bestDepth {
		return
	}
	s.bestDepth = depth
	s.bestPrefix = append([]int(nil), prefix...)
	s.bestFailedID = failedID
	s.bestFoundAt = foundAt
	s.bestValueAt = valueAt
}

func (s *searcher) bestFailure() *Violation {
	if s.bestPrefix == nil && s.bestFailedID == 0 {
		// Every candidate was always ready to try but the set of
		// completed operations was itself empty -- an empty history is
		// trivially linearizable (search returns true for len(done)==0
		// immediately), so reaching here with no recorded failure at all
		// means something unexpected happened upstream.
		return &Violation{}
	}
	return &Violation{
		Linearization: s.bestPrefix,
		FailingOp:     s.ops[s.bestFailedID],
		ExpectedFound: s.bestFoundAt,
		ExpectedValue: s.bestValueAt,
	}
}
