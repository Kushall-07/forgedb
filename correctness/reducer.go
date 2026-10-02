package correctness

// Minimize implements §17 of the Phase 11 brief's bounded history
// minimization: given a History that Check already reports as
// StatusViolation, it repeatedly tries dropping one operation at a time,
// keeping the drop whenever the remaining history is still a violation,
// until a full pass removes nothing further or maxPasses is reached.
//
// This is a simple greedy reducer, deliberately not a general
// delta-debugging implementation (§17: "do not spend excessive complexity
// implementing sophisticated delta debugging... a simple bounded reducer
// is enough"). It is not guaranteed to find the smallest possible
// counterexample, only a smaller one, bounded by maxPasses full scans
// over the current operation list; maxPasses <= 0 means a single pass.
//
// Minimize re-runs Check (with opts) once per candidate removal, so its
// total cost is O(maxPasses * len(h.Ops) * cost of Check) -- acceptable
// for the small, bounded histories this phase targets, not for an
// arbitrarily large one.
//
// If h is not already a violation under opts, Minimize returns h
// unchanged alongside Check's own (non-violation) result -- there is
// nothing to minimize.
func Minimize(h History, opts CheckOptions, maxPasses int) (History, CheckResult) {
	if maxPasses <= 0 {
		maxPasses = 1
	}

	current := History{Ops: append([]Operation(nil), h.Ops...)}
	result := Check(current, opts)
	if result.Status != StatusViolation {
		return current, result
	}

	for pass := 0; pass < maxPasses; pass++ {
		removedAny := false
		for i := 0; i < len(current.Ops); i++ {
			candidate := History{Ops: removeAt(current.Ops, i)}
			if err := candidate.Validate(); err != nil {
				continue // dropping this op alone desynchronized some other invariant; skip
			}
			candidateResult := Check(candidate, opts)
			if candidateResult.Status == StatusViolation {
				current = candidate
				result = candidateResult
				removedAny = true
				i-- // re-check the same index against the shrunk slice
			}
		}
		if !removedAny {
			break
		}
	}
	return current, result
}

func removeAt(ops []Operation, i int) []Operation {
	out := make([]Operation, 0, len(ops)-1)
	out = append(out, ops[:i]...)
	out = append(out, ops[i+1:]...)
	return out
}
