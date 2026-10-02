package raft

import (
	"testing"
)

// --- HandleInstallSnapshot: basic behavior ---------------------------------

func TestHandleInstallSnapshot_Basic_PersistsAndUpdatesBoundary(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("state"),
	})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot: %+v, want Success", reply)
	}
	if got := follower.SnapshotIndex(); got != 10 {
		t.Fatalf("SnapshotIndex = %d, want 10", got)
	}
	if got := follower.CommitIndex(); got != 10 {
		t.Fatalf("CommitIndex after install = %d, want 10", got)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || snap.LastIncludedIndex != 10 || string(snap.Data) != "state" {
		t.Fatalf("PendingSnapshot = %+v, %v, want index 10 / state", snap, ok)
	}
	// LastApplied must not have been silently advanced -- only the state
	// machine, via ConfirmSnapshotRestored, is entitled to do that.
	if got := follower.LastApplied(); got != 0 {
		t.Fatalf("LastApplied after install = %d, want unchanged 0 (not yet confirmed restored)", got)
	}
}

func TestHandleInstallSnapshot_LeaderCatchesUpFarBehindFollower(t *testing.T) {
	tr := NewInMemoryTransport()
	leader := mustNewNode(t, Options{ID: "leader", Peers: []string{"f", "other"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1})
	tr.Register("leader", leader)
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader", "other"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("f", follower)
	other := mustNewNode(t, Options{ID: "other", Peers: []string{"leader", "f"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("other", other)

	electLeader(t, leader, 6)

	// Commit several entries while the follower is partitioned away (the
	// leader + "other" still form a majority of 3), then snapshot and
	// discard the log -- the follower will have nothing that overlaps the
	// leader's retained log at all.
	tr.Partition("f")
	for i := 0; i < 5; i++ {
		if _, _, err := leader.Propose(Command("x")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()

	for i := uint64(1); i <= 5; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(5, []byte("leader-state-through-5")); err != nil {
		t.Fatalf("CreateSnapshot(5): %v", err)
	}

	tr.Heal("f")
	// Next heartbeat round: the leader sees follower nextIndex (1) <=
	// snapshotBoundary (5) and must send InstallSnapshot instead of a
	// doomed AppendEntries.
	leader.Tick()
	leader.Drain()

	if got := follower.SnapshotIndex(); got != 5 {
		t.Fatalf("follower SnapshotIndex after catch-up = %d, want 5", got)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || string(snap.Data) != "leader-state-through-5" {
		t.Fatalf("follower PendingSnapshot = %+v, %v, want leader-state-through-5", snap, ok)
	}

	leader.mu.Lock()
	nextIdx := leader.nextIndex["f"]
	matchIdx := leader.matchIndex["f"]
	leader.mu.Unlock()
	if nextIdx != 6 {
		t.Fatalf("leader nextIndex[f] after successful install = %d, want 6", nextIdx)
	}
	if matchIdx != 5 {
		t.Fatalf("leader matchIndex[f] after successful install = %d, want 5", matchIdx)
	}

	// The cluster must now be able to continue committing new entries
	// normally through ordinary AppendEntries.
	if _, _, err := leader.Propose(Command("after-snapshot")); err != nil {
		t.Fatalf("Propose after snapshot catch-up: %v", err)
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != 6 {
		t.Fatalf("CommitIndex after post-snapshot proposal = %d, want 6", got)
	}
	if got := follower.LastLogIndex(); got != 6 {
		t.Fatalf("follower LastLogIndex after catch-up = %d, want 6", got)
	}
}

// --- Matching / conflicting suffix behavior --------------------------------

func TestHandleInstallSnapshot_MatchingSuffixPreserved(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})
	follower.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{
			{Term: 1, Command: Command("a")},
			{Term: 1, Command: Command("b")},
			{Term: 1, Command: Command("c")},
		},
	})

	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("state-through-2"),
	})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot: %+v", reply)
	}
	if got := follower.LastLogIndex(); got != 3 {
		t.Fatalf("LastLogIndex after matching-suffix install = %d, want 3 (entry 3 preserved)", got)
	}
	follower.mu.Lock()
	e3, ok := follower.log.EntryAt(3)
	follower.mu.Unlock()
	if !ok || string(e3.Command) != "c" {
		t.Fatalf("EntryAt(3) = %+v, %v, want preserved command c", e3, ok)
	}
}

func TestHandleInstallSnapshot_ConflictingSuffixDiscarded(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})
	follower.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{
			{Term: 1, Command: Command("a")},
			{Term: 1, Command: Command("b-old")},
		},
	})

	// Leader's snapshot says index 2 is actually term 3 (from a different,
	// later history than what this follower has).
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 3, LeaderID: "leader", LastIncludedIndex: 2, LastIncludedTerm: 3, Data: []byte("state"),
	})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot: %+v", reply)
	}
	if got := follower.LastLogIndex(); got != 2 {
		t.Fatalf("LastLogIndex after conflicting install = %d, want 2 (no stale suffix)", got)
	}
	follower.mu.Lock()
	lastTerm := follower.log.LastTerm()
	follower.mu.Unlock()
	if lastTerm != 3 {
		t.Fatalf("LastLogTerm after conflicting install = %d, want 3", lastTerm)
	}
}

// --- Term handling ----------------------------------------------------------

func TestHandleInstallSnapshot_HigherTerm_StepsDown(t *testing.T) {
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Transport: tr, ElectionTickMin: 1, ElectionTickMax: 1})
	tr.Register("n", node)
	node.Tick()
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}

	reply := node.HandleInstallSnapshot(InstallSnapshotArgs{Term: 99, LeaderID: "a", LastIncludedIndex: 1, LastIncludedTerm: 99, Data: []byte("x")})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot with higher term: %+v, want Success", reply)
	}
	if term, role, leaderID := node.State(); term != 99 || role != Follower || leaderID != "a" {
		t.Fatalf("state after higher-term install = term %d role %s leader %q, want 99 Follower a", term, role, leaderID)
	}
}

func TestHandleInstallSnapshot_StaleTerm_Rejected(t *testing.T) {
	persister := NewMemoryPersister()
	if err := persister.SaveState(PersistentState{CurrentTerm: 5}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	node, err := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	reply := node.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "a", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("x")})
	if reply.Success {
		t.Fatalf("HandleInstallSnapshot with stale term: %+v, want rejected", reply)
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if got := node.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after stale-term install = %d, want unchanged 0", got)
	}
}

// --- Stale/duplicate snapshot ------------------------------------------------

func TestHandleInstallSnapshot_StaleOrDuplicate_TrivialSuccess(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})
	first := follower.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("v1")})
	if !first.Success {
		t.Fatalf("first install: %+v", first)
	}

	// An older/duplicate snapshot at or behind the current boundary must
	// not regress anything.
	second := follower.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("v1-dup")})
	if !second.Success {
		t.Fatalf("duplicate install: %+v, want trivial success", second)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || string(snap.Data) != "v1" {
		t.Fatalf("PendingSnapshot after duplicate install = %+v, %v, want unchanged v1", snap, ok)
	}

	older := follower.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte("stale")})
	if !older.Success {
		t.Fatalf("stale install: %+v, want trivial success", older)
	}
	if got := follower.SnapshotIndex(); got != 10 {
		t.Fatalf("SnapshotIndex after stale install = %d, want unchanged 10", got)
	}
}

// --- Corrupt / failed persistence -------------------------------------------

func TestHandleInstallSnapshot_PersistenceFailure_RejectsWithoutCorruptingState(t *testing.T) {
	persister := NewMemoryPersister()
	follower, err := NewNode(Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	persister.FailNextSnapshotSave()
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("x")})
	if reply.Success {
		t.Fatalf("HandleInstallSnapshot despite a simulated persistence failure: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after failed install = %d, want unchanged 0", got)
	}
	if _, ok := follower.PendingSnapshot(); ok {
		t.Fatalf("PendingSnapshot set despite a failed persist")
	}

	// Retry succeeds once persistence recovers.
	retry := follower.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("x")})
	if !retry.Success {
		t.Fatalf("retried install after persistence recovered: %+v", retry)
	}
}

// --- ReadIndex interaction with a lagging-behind-snapshot peer --------------

func TestReadIndex_SkipsPeerBehindSnapshotBoundary_StillSucceedsWithOtherMajority(t *testing.T) {
	tr := NewInMemoryTransport()
	leader := mustNewNode(t, Options{ID: "leader", Peers: []string{"f1", "f2"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("leader", leader)
	f1 := mustNewNode(t, Options{ID: "f1", Peers: []string{"leader", "f2"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("f1", f1)
	f2 := mustNewNode(t, Options{ID: "f2", Peers: []string{"leader", "f1"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("f2", f2)

	electLeader(t, leader, 6)

	tr.Partition("f2")
	for i := 0; i < 3; i++ {
		if _, _, err := leader.Propose(Command("x")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(3, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}
	// f2 stays partitioned and far behind (nextIndex still 1, <=
	// snapshotBoundary 3) -- ReadIndex must still succeed via leader+f1.
	if _, err := leader.ReadIndex(); err != nil {
		t.Fatalf("ReadIndex with one peer behind the snapshot boundary (but a healthy majority otherwise): %v", err)
	}
}
