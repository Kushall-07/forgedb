package storage

import (
	"bytes"
	"errors"
	"testing"
)

func newTestStore(t *testing.T) Store {
	t.Helper()
	s, err := NewMemStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	return s
}

func TestStorePutGet(t *testing.T) {
	s := newTestStore(t)

	if err := s.Put([]byte("a"), []byte("value1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("value1")) {
		t.Fatalf("Get: got %q, want %q", got, "value1")
	}
}

func TestStorePutUpdateGet(t *testing.T) {
	s := newTestStore(t)

	mustStorePut(t, s, "a", "value1")
	mustStorePut(t, s, "a", "value2")

	got, err := s.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("value2")) {
		t.Fatalf("Get: got %q, want %q", got, "value2")
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Get([]byte("missing"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get missing key: got %v, want ErrKeyNotFound", err)
	}
}

func TestStorePutDeleteGet(t *testing.T) {
	s := newTestStore(t)

	mustStorePut(t, s, "a", "value")
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get([]byte("a"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get after Delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestStoreDeleteMissingKeyIsNotAnError(t *testing.T) {
	s := newTestStore(t)

	if err := s.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete missing key: got %v, want nil", err)
	}
}

func TestStoreMultipleKeysIndependent(t *testing.T) {
	s := newTestStore(t)

	mustStorePut(t, s, "key1", "v1")
	mustStorePut(t, s, "key2", "v2")
	mustStorePut(t, s, "key3", "v3")

	if err := s.Delete([]byte("key2")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	v1, err := s.Get([]byte("key1"))
	if err != nil || !bytes.Equal(v1, []byte("v1")) {
		t.Fatalf("key1: got %q, err=%v", v1, err)
	}
	if _, err := s.Get([]byte("key2")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("key2: got %v, want ErrKeyNotFound", err)
	}
	v3, err := s.Get([]byte("key3"))
	if err != nil || !bytes.Equal(v3, []byte("v3")) {
		t.Fatalf("key3: got %q, err=%v", v3, err)
	}
}

func TestStoreEmptyAndNilKeyRejected(t *testing.T) {
	s := newTestStore(t)

	if err := s.Put([]byte(""), []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put empty key: got %v, want ErrEmptyKey", err)
	}
	if err := s.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put nil key: got %v, want ErrEmptyKey", err)
	}
	if _, err := s.Get([]byte("")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Get empty key: got %v, want ErrEmptyKey", err)
	}
	if err := s.Delete([]byte("")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete empty key: got %v, want ErrEmptyKey", err)
	}
}

func TestStoreEmptyValueIsStoredAndDistinctFromMissing(t *testing.T) {
	s := newTestStore(t)

	if err := s.Put([]byte("a"), []byte("")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get: got %q, want empty value", got)
	}
}

func TestStoreValueOwnershipOnPut(t *testing.T) {
	s := newTestStore(t)

	value := []byte("hello")
	if err := s.Put([]byte("key"), value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	value[0] = 'X'

	got, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("mutating caller's slice after Put affected stored value: got %q", got)
	}
}

func TestStoreValueOwnershipOnGet(t *testing.T) {
	s := newTestStore(t)

	mustStorePut(t, s, "key", "hello")

	got, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got[0] = 'X'

	gotAgain, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(gotAgain, []byte("hello")) {
		t.Fatalf("mutating returned slice affected stored value: got %q", gotAgain)
	}
}

func mustStorePut(t *testing.T, s Store, key, value string) {
	t.Helper()
	if err := s.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}
