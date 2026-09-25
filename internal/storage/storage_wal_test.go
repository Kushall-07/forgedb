package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

// These tests exercise durability across real process/instance
// boundaries: each opens a MemStore against a real temporary directory,
// closes it, and opens a second MemStore against the same directory to
// verify the WAL on disk reconstructs the expected state. They do not
// simulate power loss or hardware failure -- see the torn-tail and
// corruption tests in the wal package, and docs/storage/phase2-wal-recovery.md
// for what these tests do and do not prove.

func TestStorePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	mustStorePut(t, s1, "a", "one")
	mustStorePut(t, s1, "b", "two")
	if err := s1.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen): %v", err)
	}
	defer s2.Close()

	if _, err := s2.Get([]byte("a")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(a) after reopen: got %v, want ErrKeyNotFound", err)
	}
	got, err := s2.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get(b) after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("two")) {
		t.Fatalf("Get(b) after reopen: got %q, want %q", got, "two")
	}
}

func TestStoreReopenAfterUpdate(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	mustStorePut(t, s1, "a", "value1")
	mustStorePut(t, s1, "a", "value2")
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen): %v", err)
	}
	defer s2.Close()

	got, err := s2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("value2")) {
		t.Fatalf("Get after reopen: got %q, want %q", got, "value2")
	}
}

func TestStoreReopenAfterDeleteThenPutResurrects(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	mustStorePut(t, s1, "a", "value1")
	if err := s1.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustStorePut(t, s1, "a", "value2")
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen): %v", err)
	}
	defer s2.Close()

	got, err := s2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("value2")) {
		t.Fatalf("Get after reopen: got %q, want %q", got, "value2")
	}
}

func TestStoreEmptyValueSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	mustStorePut(t, s1, "a", "")
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen): %v", err)
	}
	defer s2.Close()

	got, err := s2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get after reopen: got %q, want empty value", got)
	}
}

func TestStoreCreatesMissingDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")

	s, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore with missing nested dir: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestStoreThirdReopenAppliesAllHistory(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	mustStorePut(t, s1, "a", "1")
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen 1): %v", err)
	}
	mustStorePut(t, s2, "b", "2")
	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s3, err := NewMemStore(dir)
	if err != nil {
		t.Fatalf("NewMemStore (reopen 2): %v", err)
	}
	defer s3.Close()

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := s3.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q): got %q, want %q", key, got, want)
		}
	}
}
