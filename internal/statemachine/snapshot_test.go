package statemachine

import (
	"errors"
	"testing"

	"github.com/Kushall-07/forgedb/internal/storage"
)

// --- Basic capture/restore round trip ---------------------------------------

func TestKVStateMachine_Snapshot_EmptyState_RoundTrips(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if _, err := restored.store.Get([]byte("anything")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get on restored-empty state = %v, want ErrKeyNotFound", err)
	}
}

func TestKVStateMachine_Snapshot_PutSurvivesRestore(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply(Put): %v", err)
	}
	if _, err := sm.Apply(NewPutCommand("c1", 2, []byte("y"), []byte("20"))); err != nil {
		t.Fatalf("Apply(Put): %v", err)
	}

	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	v, err := restored.store.Get([]byte("x"))
	if err != nil || string(v) != "10" {
		t.Fatalf("Get(x) after restore = %q, %v, want 10", v, err)
	}
	v, err = restored.store.Get([]byte("y"))
	if err != nil || string(v) != "20" {
		t.Fatalf("Get(y) after restore = %q, %v, want 20", v, err)
	}
}

func TestKVStateMachine_Snapshot_DeleteSurvivesRestore(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply(Put): %v", err)
	}
	if _, err := sm.Apply(NewDeleteCommand("c1", 2, []byte("x"))); err != nil {
		t.Fatalf("Apply(Delete): %v", err)
	}

	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if _, err := restored.store.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(x) after restoring a deleted key = %v, want ErrKeyNotFound", err)
	}
}

// --- Restore reconciles storage (deletes stale keys the snapshot omits) ----

func TestKVStateMachine_Restore_DeletesKeysNotInSnapshot(t *testing.T) {
	// Simulate a lagging follower: it already applied "stale" on its own,
	// but the incoming snapshot (from further ahead on the leader, where
	// "stale" was since deleted) does not mention it at all.
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("stale"), []byte("old"))); err != nil {
		t.Fatalf("Apply(Put stale): %v", err)
	}

	source, _ := newTestStateMachine(t)
	if _, err := source.Apply(NewPutCommand("c1", 1, []byte("fresh"), []byte("new"))); err != nil {
		t.Fatalf("Apply(Put fresh): %v", err)
	}
	data, err := source.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if err := sm.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if _, err := sm.store.Get([]byte("stale")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(stale) after restore = %v, want ErrKeyNotFound (not present in the snapshot)", err)
	}
	v, err := sm.store.Get([]byte("fresh"))
	if err != nil || string(v) != "new" {
		t.Fatalf("Get(fresh) after restore = %q, %v, want new", v, err)
	}
}

// --- Deduplication state survives snapshot/restore --------------------------

func TestKVStateMachine_Snapshot_DuplicateRequestBehaviorSurvivesRestore(t *testing.T) {
	sm, cs := newTestStateMachine(t)
	cmd := NewPutCommand("c1", 1, []byte("x"), []byte("10"))
	if _, err := sm.Apply(cmd); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, restoredCS := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	putsAfterRestore := restoredCS.puts // RestoreSnapshot itself legitimately Puts the snapshot's own KV data

	// Re-applying the identical (ClientID, RequestID, Op, Key, Value) must
	// be recognized as a replay -- it must not touch storage again.
	res, err := restored.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply(duplicate) after restore: %v", err)
	}
	if !res.Replayed {
		t.Fatalf("Result.Replayed = false after restore, want true (dedup state must survive)")
	}
	if got := restoredCS.puts; got != putsAfterRestore {
		t.Fatalf("real Put calls after restore+replay = %d, want unchanged %d", got, putsAfterRestore)
	}
	_ = cs // original store, unused beyond the sanity of the setup above
}

func TestKVStateMachine_Snapshot_ConflictingRequestBehaviorSurvivesRestore(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	// Same (ClientID, RequestID) but a different command -- must still be
	// recognized as a conflict after restore, not silently re-executed.
	res, err := restored.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("999")))
	if err != nil {
		t.Fatalf("Apply(conflicting): %v", err)
	}
	if !errors.Is(res.Err, ErrRequestIDConflict) {
		t.Fatalf("Result.Err = %v, want ErrRequestIDConflict", res.Err)
	}
}

func TestKVStateMachine_Snapshot_StaleRequestBehaviorSurvivesRestore(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 5, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	res, err := restored.Apply(NewPutCommand("c1", 2, []byte("x"), []byte("stale")))
	if err != nil {
		t.Fatalf("Apply(stale): %v", err)
	}
	if !errors.Is(res.Err, ErrStaleRequest) {
		t.Fatalf("Result.Err = %v, want ErrStaleRequest", res.Err)
	}
}

func TestKVStateMachine_Snapshot_MultipleClientsDedupStateSurvives(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	for i, client := range []string{"alice", "bob", "carol"} {
		if _, err := sm.Apply(NewPutCommand(client, uint64(i+1), []byte(client), []byte("v"))); err != nil {
			t.Fatalf("Apply(%s): %v", client, err)
		}
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, restoredCS := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	putsAfterRestore := restoredCS.puts
	for i, client := range []string{"alice", "bob", "carol"} {
		res, err := restored.Apply(NewPutCommand(client, uint64(i+1), []byte(client), []byte("v")))
		if err != nil {
			t.Fatalf("Apply(%s) replay: %v", client, err)
		}
		if !res.Replayed {
			t.Fatalf("client %s: Replayed = false, want true", client)
		}
	}
	if got := restoredCS.puts; got != putsAfterRestore {
		t.Fatalf("real Put calls after restore+replay of 3 clients = %d, want unchanged %d", got, putsAfterRestore)
	}
}

// --- Deterministic encoding ---------------------------------------------------

func TestKVStateMachine_Snapshot_DeterministicEncoding(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	for i, client := range []string{"zed", "alice", "mike"} {
		if _, err := sm.Apply(NewPutCommand(client, uint64(i+1), []byte(client), []byte("v"))); err != nil {
			t.Fatalf("Apply(%s): %v", client, err)
		}
	}
	a, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot (1st): %v", err)
	}
	b, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot (2nd): %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("two CreateSnapshot calls against identical state produced different bytes (map iteration order leaking into encoding?)")
	}
}

// --- Large state within configured limits -------------------------------------

func TestKVStateMachine_Snapshot_LargeState_RoundTrips(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	const n = 500
	for i := 0; i < n; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		if _, err := sm.Apply(NewPutCommand("bulk-client", uint64(i)+1, key, []byte("value"))); err != nil {
			t.Fatalf("Apply(%d): %v", i, err)
		}
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	for i := 0; i < n; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		v, err := restored.store.Get(key)
		if err != nil || string(v) != "value" {
			t.Fatalf("Get(%d) after restoring large state = %q, %v", i, v, err)
		}
	}
}

// --- Corruption detection -------------------------------------------------------

func TestKVStateMachine_RestoreSnapshot_CorruptData_Rejected(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if err := sm.RestoreSnapshot([]byte("not a valid snapshot")); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("RestoreSnapshot(garbage) = %v, want ErrInvalidCommand", err)
	}
}

func TestKVStateMachine_RestoreSnapshot_TruncatedData_Rejected(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(data[:len(data)/2]); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("RestoreSnapshot(truncated) = %v, want ErrInvalidCommand", err)
	}
}

func TestKVStateMachine_RestoreSnapshot_ChecksumMismatch_Rejected(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data, err := sm.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	mutated := append([]byte(nil), data...)
	mutated[len(mutated)-20] ^= 0xFF

	restored, _ := newTestStateMachine(t)
	if err := restored.RestoreSnapshot(mutated); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("RestoreSnapshot(mutated) = %v, want ErrInvalidCommand (checksum mismatch)", err)
	}
}

func TestKVStateMachine_RestoreSnapshot_RestoreFailure_LeavesStorageUntouched(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if err := sm.RestoreSnapshot([]byte("garbage")); err == nil {
		t.Fatalf("RestoreSnapshot(garbage): want error, got nil")
	}
	v, err := sm.store.Get([]byte("x"))
	if err != nil || string(v) != "10" {
		t.Fatalf("Get(x) after rejected restore = %q, %v, want unchanged 10", v, err)
	}
}
