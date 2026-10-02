package correctness

import "testing"

func TestMinimize_ShrinksAFailingHistory(t *testing.T) {
	// A long chain of filler Puts to unrelated keys, all strictly before
	// the two operations that actually conflict (a stale Get right after
	// a Put). Minimize should strip away every filler operation and be
	// left with just the two that matter.
	ops := make([]Operation, 0, 20)
	tick := 1
	for i := 0; i < 8; i++ {
		ops = append(ops, opWrite(i+1, OpPut, "n0", "filler", "v", tick, tick+1))
		tick += 2
	}
	staleID := 100
	ops = append(ops, opWrite(staleID, OpPut, "n0", "x", "1", tick, tick+1))
	tick += 2
	ops = append(ops, opGet(staleID+1, "n0", "x", false, "", tick, tick+1))

	h := History{Ops: ops}
	before := Check(h, CheckOptions{})
	if before.Status != StatusViolation {
		t.Fatalf("setup: Check = %s, want VIOLATION", before.Status)
	}

	minimized, result := Minimize(h, CheckOptions{}, 5)
	if result.Status != StatusViolation {
		t.Fatalf("Minimize result: %s, want VIOLATION", result.Status)
	}
	if len(minimized.Ops) >= len(h.Ops) {
		t.Fatalf("Minimize did not shrink the history: %d ops -> %d ops", len(h.Ops), len(minimized.Ops))
	}
	if len(minimized.Ops) != 2 {
		t.Fatalf("Minimize left %d ops, want exactly the 2 conflicting ones\n\n%s", len(minimized.Ops), result.Report(minimized))
	}

	ids := map[int]bool{}
	for _, op := range minimized.Ops {
		ids[op.ID] = true
	}
	if !ids[staleID] || !ids[staleID+1] {
		t.Fatalf("Minimize removed one of the actually-conflicting operations")
	}
}

func TestMinimize_LeavesAValidHistoryUnchanged(t *testing.T) {
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", true, "1", 3, 4),
	}}
	minimized, result := Minimize(h, CheckOptions{}, 3)
	if result.Status != StatusValid {
		t.Fatalf("Minimize result: %s, want VALID", result.Status)
	}
	if len(minimized.Ops) != len(h.Ops) {
		t.Fatalf("Minimize changed a valid history: %d ops -> %d ops", len(h.Ops), len(minimized.Ops))
	}
}
