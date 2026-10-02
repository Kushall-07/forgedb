package raft

import "testing"

// buildLog returns a fresh Log with n entries appended, each at term 1,
// commands "e1".."eN".
func buildLog(n int) *Log {
	l := NewLog()
	for i := 1; i <= n; i++ {
		l.Append(LogEntry{Term: 1, Command: Command(string(rune('a' + i - 1)))})
	}
	return l
}

func TestLog_Compact_UpdatesSentinelAndDropsPrefix(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	if l.entries[0].Index != 3 || l.entries[0].Term != 1 {
		t.Fatalf("sentinel after compact = %+v, want {3 1}", l.entries[0])
	}
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex after compact = %d, want 5", l.LastIndex())
	}
	if _, ok := l.TermAt(2); ok {
		t.Fatalf("TermAt(2) after compacting through 3 should report false (compacted away)")
	}
	if term, ok := l.TermAt(3); !ok || term != 1 {
		t.Fatalf("TermAt(3) (the new boundary) = (%d, %v), want (1, true)", term, ok)
	}
	e4, ok := l.EntryAt(4)
	if !ok || string(e4.Command) != "d" {
		t.Fatalf("EntryAt(4) after compact = %+v, %v, want d", e4, ok)
	}
	if _, ok := l.EntryAt(3); ok {
		t.Fatalf("EntryAt(3) (the new boundary) should not report a real entry")
	}
}

func TestLog_Compact_ThroughEntireLog(t *testing.T) {
	l := buildLog(3)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	if l.LastIndex() != 3 || l.LastTerm() != 1 {
		t.Fatalf("after compacting entire log: LastIndex=%d LastTerm=%d, want 3,1", l.LastIndex(), l.LastTerm())
	}
	if got := l.Slice(1); got != nil {
		t.Fatalf("Slice(1) after compacting entire log = %+v, want nil", got)
	}
}

func TestLog_Compact_RejectsIndexNotPresent(t *testing.T) {
	l := buildLog(3)
	if err := l.compact(10); err == nil {
		t.Fatalf("compact(10) beyond LastIndex: want error, got nil")
	}
	if l.LastIndex() != 3 {
		t.Fatalf("log mutated despite rejected compact: LastIndex = %d, want 3", l.LastIndex())
	}

	if err := l.compact(2); err != nil {
		t.Fatalf("compact(2): %v", err)
	}
	if err := l.compact(1); err == nil {
		t.Fatalf("compact(1) at or below the existing boundary: want error, got nil")
	}
}

func TestLog_AppendAfter_WorksRightAtCompactionBoundary(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	// Leader resends starting immediately after the boundary -- a pure
	// AppendEntries consistency check at prevIndex == boundary.
	if term, ok := l.TermAt(3); !ok || term != 1 {
		t.Fatalf("TermAt(3) = (%d, %v), want (1, true) -- boundary must behave like a real entry for the consistency check", term, ok)
	}
	mutated := l.AppendAfter(3, []LogEntry{{Term: 1, Command: Command("d")}, {Term: 1, Command: Command("e")}})
	if mutated {
		t.Fatalf("AppendAfter with fully matching suffix should be a no-op")
	}
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", l.LastIndex())
	}
}

func TestLog_InstallSnapshotBoundary_MatchingSuffixPreserved(t *testing.T) {
	l := buildLog(5) // all at term 1
	l.installSnapshotBoundary(3, 1)
	if l.entries[0].Index != 3 || l.entries[0].Term != 1 {
		t.Fatalf("sentinel = %+v, want {3 1}", l.entries[0])
	}
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5 (matching suffix 4,5 must survive)", l.LastIndex())
	}
	e4, ok := l.EntryAt(4)
	if !ok || string(e4.Command) != "d" {
		t.Fatalf("EntryAt(4) = %+v, %v, want d (preserved)", e4, ok)
	}
}

func TestLog_InstallSnapshotBoundary_ConflictingSuffixDiscarded(t *testing.T) {
	l := NewLog()
	l.Append(LogEntry{Term: 1, Command: Command("a")})
	l.Append(LogEntry{Term: 1, Command: Command("b")}) // index 2, term 1 -- will conflict

	l.installSnapshotBoundary(2, 5) // leader's snapshot says index 2 is term 5, not 1
	if l.entries[0].Index != 2 || l.entries[0].Term != 5 {
		t.Fatalf("sentinel = %+v, want {2 5}", l.entries[0])
	}
	if l.LastIndex() != 2 || l.LastTerm() != 5 {
		t.Fatalf("after conflicting install: LastIndex=%d LastTerm=%d, want 2,5 (no stale suffix survives)", l.LastIndex(), l.LastTerm())
	}
}

func TestLog_InstallSnapshotBoundary_NoOverlapDiscardsEverything(t *testing.T) {
	l := buildLog(2) // log only reaches index 2
	l.installSnapshotBoundary(10, 7)
	if l.entries[0].Index != 10 || l.entries[0].Term != 7 {
		t.Fatalf("sentinel = %+v, want {10 7}", l.entries[0])
	}
	if l.LastIndex() != 10 || l.LastTerm() != 7 {
		t.Fatalf("LastIndex=%d LastTerm=%d, want 10,7", l.LastIndex(), l.LastTerm())
	}
}

func TestLog_LogicalLastPosition_NoRetainedEntriesEqualsBoundary(t *testing.T) {
	l := buildLog(3)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	if l.LastIndex() != 3 || l.LastTerm() != 1 {
		t.Fatalf("logical last position with no retained entries = %d/%d, want 3/1 (the snapshot boundary itself)", l.LastIndex(), l.LastTerm())
	}
}

func TestLog_Range_ClampsAtCompactionBoundary(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(2); err != nil {
		t.Fatalf("compact(2): %v", err)
	}
	got := l.Range(1, 5) // from=1 is below the boundary; must clamp to 3
	if len(got) != 3 || got[0].Index != 3 || got[2].Index != 5 {
		t.Fatalf("Range(1,5) after compacting through 2 = %+v, want entries 3..5", got)
	}
}

func TestLog_Slice_ClampsAtCompactionBoundary(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(2); err != nil {
		t.Fatalf("compact(2): %v", err)
	}
	got := l.Slice(1)
	if len(got) != 3 || got[0].Index != 3 {
		t.Fatalf("Slice(1) after compacting through 2 = %+v, want entries starting at 3", got)
	}
}
