package memtable

import "testing"

func TestLenAndBytes_TrackLiveEntriesIncrementally(t *testing.T) {
	m := New()
	if m.Len() != 0 || m.Bytes() != 0 {
		t.Fatalf("empty MemTable: Len()=%d Bytes()=%d, want 0, 0", m.Len(), m.Bytes())
	}

	mustPut(t, m, "a", "1")   // key+value = 1+1 = 2 bytes
	mustPut(t, m, "bb", "22") // 2+2 = 4 bytes
	if got := m.Len(); got != 2 {
		t.Fatalf("Len() after 2 puts = %d, want 2", got)
	}
	if got := m.Bytes(); got != 6 {
		t.Fatalf("Bytes() after 2 puts = %d, want 6", got)
	}

	// Updating an existing key changes only the byte count, not the count.
	mustPut(t, m, "a", "123") // value grows from 1 to 3 bytes: +2
	if got := m.Len(); got != 2 {
		t.Fatalf("Len() after update = %d, want 2", got)
	}
	if got := m.Bytes(); got != 8 {
		t.Fatalf("Bytes() after update = %d, want 8", got)
	}

	// Deleting a live key decrements both.
	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() after delete = %d, want 1", got)
	}
	if got := m.Bytes(); got != 4 {
		t.Fatalf("Bytes() after delete = %d, want 4", got)
	}

	// Deleting an already-deleted (or never-present) key is a no-op for
	// both counters.
	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete (again): %v", err)
	}
	if err := m.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("Delete (never existed): %v", err)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() after redundant deletes = %d, want 1", got)
	}
	if got := m.Bytes(); got != 4 {
		t.Fatalf("Bytes() after redundant deletes = %d, want 4", got)
	}

	// Reviving a deleted key via Put counts it as live again.
	mustPut(t, m, "a", "9")
	if got := m.Len(); got != 2 {
		t.Fatalf("Len() after resurrecting a deleted key = %d, want 2", got)
	}
	if got := m.Bytes(); got != 6 {
		t.Fatalf("Bytes() after resurrecting a deleted key = %d, want 6", got)
	}
}
