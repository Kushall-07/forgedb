package memtable

import (
	"bytes"
	"errors"
	"testing"
)

func TestPutGet(t *testing.T) {
	m := New()
	if err := m.Put([]byte("a"), []byte("value1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, found := m.Get([]byte("a"))
	if !found {
		t.Fatalf("Get: expected key to be found")
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Get: got %q, want %q", value, "value1")
	}
}

func TestPutUpdateGet(t *testing.T) {
	m := New()
	mustPut(t, m, "a", "value1")
	mustPut(t, m, "a", "value2")

	value, found := m.Get([]byte("a"))
	if !found {
		t.Fatalf("Get: expected key to be found")
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Get: got %q, want %q", value, "value2")
	}
}

func TestRepeatedUpdatesKeepSingleNode(t *testing.T) {
	m := New()
	for i := 0; i < 5; i++ {
		mustPut(t, m, "a", "v")
	}
	mustPut(t, m, "b", "v")

	count := 0
	for x := m.list.header.forward[0]; x != nil; x = x.forward[0] {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 distinct nodes after repeated updates, got %d", count)
	}
}

func TestGetMissingKey(t *testing.T) {
	m := New()
	_, found := m.Get([]byte("missing"))
	if found {
		t.Fatalf("Get: expected missing key to not be found")
	}
}

func TestPutDeleteGet(t *testing.T) {
	m := New()
	mustPut(t, m, "a", "value")

	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found := m.Get([]byte("a"))
	if found {
		t.Fatalf("Get: expected deleted key to not be found")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	m := New()
	if err := m.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete on missing key should not error, got %v", err)
	}
	_, found := m.Get([]byte("missing"))
	if found {
		t.Fatalf("Get: expected key to remain not found")
	}
}

func TestDeleteLeavesTombstoneNode(t *testing.T) {
	m := New()
	mustPut(t, m, "a", "value")
	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	node, ok := m.list.search([]byte("a"))
	if !ok {
		t.Fatalf("expected tombstone node to remain linked in the skip list")
	}
	if !node.deleted {
		t.Fatalf("expected node to be marked deleted")
	}
}

func TestPutAfterDeleteResurrectsKey(t *testing.T) {
	m := New()
	mustPut(t, m, "a", "value1")
	if err := m.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustPut(t, m, "a", "value2")

	value, found := m.Get([]byte("a"))
	if !found {
		t.Fatalf("Get: expected key to be found after re-Put")
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Get: got %q, want %q", value, "value2")
	}
}

func TestMultipleKeysIndependent(t *testing.T) {
	m := New()
	mustPut(t, m, "key1", "v1")
	mustPut(t, m, "key2", "v2")
	mustPut(t, m, "key3", "v3")

	if err := m.Delete([]byte("key2")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	v1, found := m.Get([]byte("key1"))
	if !found || !bytes.Equal(v1, []byte("v1")) {
		t.Fatalf("key1: got %q, found=%v", v1, found)
	}
	if _, found := m.Get([]byte("key2")); found {
		t.Fatalf("key2: expected deleted key to not be found")
	}
	v3, found := m.Get([]byte("key3"))
	if !found || !bytes.Equal(v3, []byte("v3")) {
		t.Fatalf("key3: got %q, found=%v", v3, found)
	}
}

func TestEmptyAndNilKeyRejected(t *testing.T) {
	m := New()

	if err := m.Put([]byte(""), []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put empty key: got %v, want ErrEmptyKey", err)
	}
	if err := m.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put nil key: got %v, want ErrEmptyKey", err)
	}
	if err := m.Delete([]byte("")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete empty key: got %v, want ErrEmptyKey", err)
	}
	if _, found := m.Get([]byte("")); found {
		t.Fatalf("Get empty key: expected not found")
	}
}

func TestEmptyValueIsStoredAndDistinctFromMissing(t *testing.T) {
	m := New()
	if err := m.Put([]byte("a"), []byte("")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value, found := m.Get([]byte("a"))
	if !found {
		t.Fatalf("Get: expected empty-value key to be found")
	}
	if len(value) != 0 {
		t.Fatalf("Get: got %q, want empty value", value)
	}
}

func TestValueOwnershipOnPut(t *testing.T) {
	m := New()
	value := []byte("hello")
	mustPutBytes(t, m, []byte("key"), value)

	value[0] = 'X'

	stored, found := m.Get([]byte("key"))
	if !found {
		t.Fatalf("Get: expected key to be found")
	}
	if !bytes.Equal(stored, []byte("hello")) {
		t.Fatalf("mutating caller's slice after Put affected stored value: got %q", stored)
	}
}

func TestValueOwnershipOnGet(t *testing.T) {
	m := New()
	mustPut(t, m, "key", "hello")

	got, found := m.Get([]byte("key"))
	if !found {
		t.Fatalf("Get: expected key to be found")
	}
	got[0] = 'X'

	gotAgain, found := m.Get([]byte("key"))
	if !found {
		t.Fatalf("Get: expected key to be found")
	}
	if !bytes.Equal(gotAgain, []byte("hello")) {
		t.Fatalf("mutating returned slice affected stored value: got %q", gotAgain)
	}
}

func TestKeyOwnershipOnPut(t *testing.T) {
	m := New()
	key := []byte("key")
	mustPutBytes(t, m, key, []byte("value"))

	key[0] = 'X'

	_, found := m.Get([]byte("key"))
	if !found {
		t.Fatalf("mutating caller's key slice after Put corrupted stored key")
	}
}

func TestOrderingIsSortedByKey(t *testing.T) {
	m := New()
	keys := []string{"delta", "alpha", "charlie", "echo", "bravo"}
	for _, k := range keys {
		mustPut(t, m, k, "v")
	}

	var prev []byte
	count := 0
	for x := m.list.header.forward[0]; x != nil; x = x.forward[0] {
		if prev != nil && bytes.Compare(prev, x.key) >= 0 {
			t.Fatalf("skip list not strictly ordered: %q before %q", prev, x.key)
		}
		prev = x.key
		count++
	}
	if count != len(keys) {
		t.Fatalf("expected %d nodes, got %d", len(keys), count)
	}
}

func mustPut(t *testing.T, m *MemTable, key, value string) {
	t.Helper()
	if err := m.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustPutBytes(t *testing.T, m *MemTable, key, value []byte) {
	t.Helper()
	if err := m.Put(key, value); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}
