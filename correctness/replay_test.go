package correctness

import (
	"path/filepath"
	"testing"
)

func TestSaveLoadHistory_RoundTrips(t *testing.T) {
	original := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", true, "1", 3, 4),
		opFailed(3, OpGet, "n1", "x", 5, 6),
		opIncomplete(4, OpPut, "n0", "y", "9", 7),
	}}

	path := filepath.Join(t.TempDir(), "history.json")
	if err := SaveHistory(original, path); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}

	loaded, err := LoadHistory(path)
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if err := loaded.Validate(); err != nil {
		t.Fatalf("loaded history failed Validate: %v", err)
	}

	if len(loaded.Ops) != len(original.Ops) {
		t.Fatalf("loaded %d ops, want %d", len(loaded.Ops), len(original.Ops))
	}
	for i, want := range original.Ops {
		got := loaded.Ops[i]
		if got.ID != want.ID || got.Kind != want.Kind || got.Outcome != want.Outcome ||
			got.Key != want.Key || got.Value != want.Value || got.Invoke != want.Invoke ||
			got.Complete != want.Complete || got.Found != want.Found || got.Result != want.Result {
			t.Fatalf("op %d round-tripped incorrectly: got %+v, want %+v", i, got, want)
		}
		if want.Outcome == OutcomeFailed && (got.Err == nil || got.Err.Error() != want.Err.Error()) {
			t.Fatalf("op %d: Err did not round-trip: got %v, want message %q", i, got.Err, want.Err.Error())
		}
	}
}

func TestSaveLoadHistory_CheckResultMatchesAfterRoundTrip(t *testing.T) {
	// A round-tripped history must produce the identical Check verdict --
	// replay (§44) is only useful if it reproduces the exact same
	// failure.
	h := History{Ops: []Operation{
		opWrite(1, OpPut, "n0", "x", "1", 1, 2),
		opGet(2, "n0", "x", false, "", 3, 4), // stale: a violation
	}}
	before := Check(h, CheckOptions{})

	path := filepath.Join(t.TempDir(), "violation.json")
	if err := SaveHistory(h, path); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	loaded, err := LoadHistory(path)
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	after := Check(loaded, CheckOptions{})

	if before.Status != after.Status {
		t.Fatalf("Check status changed across round-trip: %s -> %s", before.Status, after.Status)
	}
	if before.Status != StatusViolation {
		t.Fatalf("setup: want a violation before round-tripping, got %s", before.Status)
	}
}

func TestLoadHistory_RejectsUnknownFile(t *testing.T) {
	if _, err := LoadHistory(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatalf("LoadHistory: want error for a missing file, got nil")
	}
}
