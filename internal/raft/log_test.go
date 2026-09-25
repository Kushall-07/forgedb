package raft

import (
	"reflect"
	"testing"
)

func TestLog_SentinelAtIndexZero(t *testing.T) {
	l := NewLog()
	if got, ok := l.TermAt(0); !ok || got != 0 {
		t.Fatalf("TermAt(0) = (%d, %v), want (0, true)", got, ok)
	}
	if l.LastIndex() != 0 || l.LastTerm() != 0 {
		t.Fatalf("empty log: LastIndex=%d LastTerm=%d, want 0, 0", l.LastIndex(), l.LastTerm())
	}
	if _, ok := l.EntryAt(0); ok {
		t.Fatalf("EntryAt(0) should not report a real entry (sentinel only)")
	}
}

func TestLog_Append(t *testing.T) {
	l := NewLog()
	i1 := l.Append(LogEntry{Term: 1, Command: Command("a")})
	i2 := l.Append(LogEntry{Term: 1, Command: Command("b")})
	if i1 != 1 || i2 != 2 {
		t.Fatalf("indexes = %d, %d, want 1, 2", i1, i2)
	}
	if l.LastIndex() != 2 || l.LastTerm() != 1 {
		t.Fatalf("LastIndex=%d LastTerm=%d, want 2, 1", l.LastIndex(), l.LastTerm())
	}
	e, ok := l.EntryAt(1)
	if !ok || string(e.Command) != "a" {
		t.Fatalf("EntryAt(1) = %+v, %v", e, ok)
	}
}

// TestLog_AppendAfter_ConflictTruncation exercises the worked example from
// the phase spec: an existing log with entries at terms
// [1, 1, 2, 2] receives a leader's entries for index 3 onward at term 3.
// The conflicting suffix (old index 3, 4) must be discarded and replaced;
// the matching prefix (index 1, 2) must survive untouched.
func TestLog_AppendAfter_ConflictTruncation(t *testing.T) {
	l := NewLog()
	l.Append(LogEntry{Term: 1, Command: Command("i1")})
	l.Append(LogEntry{Term: 1, Command: Command("i2")})
	l.Append(LogEntry{Term: 2, Command: Command("i3-old")})
	l.Append(LogEntry{Term: 2, Command: Command("i4-old")})

	l.AppendAfter(2, []LogEntry{
		{Term: 3, Command: Command("i3-new")},
		{Term: 3, Command: Command("i4-new")},
	})

	if l.LastIndex() != 4 {
		t.Fatalf("LastIndex = %d, want 4", l.LastIndex())
	}
	if term, _ := l.TermAt(1); term != 1 {
		t.Errorf("index 1 term = %d, want 1 (matching prefix must survive)", term)
	}
	if term, _ := l.TermAt(2); term != 1 {
		t.Errorf("index 2 term = %d, want 1 (matching prefix must survive)", term)
	}
	e3, _ := l.EntryAt(3)
	if e3.Term != 3 || string(e3.Command) != "i3-new" {
		t.Errorf("index 3 = %+v, want term 3 command i3-new", e3)
	}
	e4, _ := l.EntryAt(4)
	if e4.Term != 3 || string(e4.Command) != "i4-new" {
		t.Errorf("index 4 = %+v, want term 3 command i4-new", e4)
	}
}

// TestLog_AppendAfter_MatchingPrefixPreserved checks that re-sending
// entries the follower already has (e.g. a retried RPC) is a true no-op:
// nothing is truncated or duplicated when every incoming entry already
// matches by term.
func TestLog_AppendAfter_MatchingPrefixPreserved(t *testing.T) {
	l := NewLog()
	l.Append(LogEntry{Term: 1, Command: Command("i1")})
	l.Append(LogEntry{Term: 1, Command: Command("i2")})
	l.Append(LogEntry{Term: 2, Command: Command("i3")})

	before := append([]LogEntry(nil), l.entries...)

	l.AppendAfter(0, []LogEntry{
		{Term: 1, Command: Command("i1")},
		{Term: 1, Command: Command("i2")},
		{Term: 2, Command: Command("i3")},
	})

	if !reflect.DeepEqual(before, l.entries) {
		t.Fatalf("log mutated on a fully-matching AppendAfter: before=%+v after=%+v", before, l.entries)
	}
}

func TestLog_AppendAfter_ExtendsPastEnd(t *testing.T) {
	l := NewLog()
	l.Append(LogEntry{Term: 1, Command: Command("i1")})

	l.AppendAfter(1, []LogEntry{
		{Term: 1, Command: Command("i2")},
		{Term: 1, Command: Command("i3")},
	})

	if l.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", l.LastIndex())
	}
	e2, _ := l.EntryAt(2)
	e3, _ := l.EntryAt(3)
	if string(e2.Command) != "i2" || string(e3.Command) != "i3" {
		t.Fatalf("unexpected entries: %+v, %+v", e2, e3)
	}
}

func TestLog_Range(t *testing.T) {
	l := NewLog()
	for i := 1; i <= 5; i++ {
		l.Append(LogEntry{Term: 1})
	}
	got := l.Range(2, 4)
	if len(got) != 3 || got[0].Index != 2 || got[2].Index != 4 {
		t.Fatalf("Range(2,4) = %+v", got)
	}
	if got := l.Range(6, 10); got != nil {
		t.Fatalf("Range past the end should be nil, got %+v", got)
	}
}

// TestLogIsUpToDate covers every combination the phase spec calls out
// explicitly: a later term always wins regardless of length; equal terms
// fall back to comparing length; an equal length with an equal term is
// up-to-date (>=, not >).
func TestLogIsUpToDate(t *testing.T) {
	tests := []struct {
		name                                       string
		candIndex, candTerm, voterIndex, voterTerm uint64
		want                                       bool
	}{
		{"higher last term wins despite shorter log", 1, 5, 10, 3, true},
		{"same term, longer candidate log", 10, 3, 5, 3, true},
		{"same term, same length", 5, 3, 5, 3, true},
		{"older last term loses despite longer log", 100, 2, 1, 3, false},
		{"same term, shorter candidate log", 3, 3, 5, 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := logIsUpToDate(tt.candIndex, tt.candTerm, tt.voterIndex, tt.voterTerm)
			if got != tt.want {
				t.Errorf("logIsUpToDate(cand=%d/%d, voter=%d/%d) = %v, want %v",
					tt.candIndex, tt.candTerm, tt.voterIndex, tt.voterTerm, got, tt.want)
			}
		})
	}
}
