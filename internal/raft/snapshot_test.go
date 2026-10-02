package raft

import (
	"errors"
	"testing"
)

// --- CreateSnapshot validation ------------------------------------------

func TestCreateSnapshot_RejectsZeroIndex(t *testing.T) {
	leader := commitUpTo(t, 3)
	if err := leader.MarkApplied(3); err != nil {
		t.Fatalf("MarkApplied: %v", err)
	}
	if err := leader.CreateSnapshot(0, []byte("x")); !errors.Is(err, ErrInvalidSnapshotIndex) {
		t.Fatalf("CreateSnapshot(0) = %v, want ErrInvalidSnapshotIndex", err)
	}
}

func TestCreateSnapshot_RejectsBeyondLastApplied(t *testing.T) {
	leader := commitUpTo(t, 3)
	if err := leader.MarkApplied(1); err != nil {
		t.Fatalf("MarkApplied(1): %v", err)
	}
	if err := leader.CreateSnapshot(2, []byte("x")); !errors.Is(err, ErrInvalidSnapshotIndex) {
		t.Fatalf("CreateSnapshot(2) with lastApplied=1 = %v, want ErrInvalidSnapshotIndex", err)
	}
}

func TestCreateSnapshot_RejectsAtOrBelowExistingBoundary(t *testing.T) {
	leader := commitUpTo(t, 5)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(2, []byte("v1")); err != nil {
		t.Fatalf("CreateSnapshot(2): %v", err)
	}
	if err := leader.CreateSnapshot(2, []byte("v2")); !errors.Is(err, ErrInvalidSnapshotIndex) {
		t.Fatalf("CreateSnapshot(2) again (at existing boundary) = %v, want ErrInvalidSnapshotIndex", err)
	}
	if err := leader.CreateSnapshot(1, []byte("v3")); !errors.Is(err, ErrInvalidSnapshotIndex) {
		t.Fatalf("CreateSnapshot(1) (below existing boundary) = %v, want ErrInvalidSnapshotIndex", err)
	}
}

func TestCreateSnapshot_Success_RecordsBoundaryAndCompactsLog(t *testing.T) {
	leader := commitUpTo(t, 5)
	for i := uint64(1); i <= 4; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(4, []byte("snapshot-data")); err != nil {
		t.Fatalf("CreateSnapshot(4): %v", err)
	}
	if got := leader.SnapshotIndex(); got != 4 {
		t.Fatalf("SnapshotIndex = %d, want 4", got)
	}
	leader.mu.Lock()
	lastIdx := leader.log.LastIndex()
	_, hasEntry1 := leader.log.EntryAt(1)
	leader.mu.Unlock()
	if lastIdx != 5 {
		t.Fatalf("LastIndex after snapshot at 4 = %d, want unchanged 5", lastIdx)
	}
	if hasEntry1 {
		t.Fatalf("entry 1 should have been compacted away")
	}
}

// --- Persistence of the snapshot boundary/log compaction ------------------

func TestCreateSnapshot_PersistsSnapshotBeforeCompactingLog(t *testing.T) {
	persister := NewMemoryPersister()
	node := commitUpToWithPersister(t, 3, persister)
	for i := uint64(1); i <= 3; i++ {
		if err := node.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}

	if err := node.CreateSnapshot(3, []byte("state-through-3")); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}

	snap, err := persister.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.LastIncludedIndex != 3 || string(snap.Data) != "state-through-3" {
		t.Fatalf("persisted snapshot = %+v, want index 3 / state-through-3", snap)
	}

	state, err := persister.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if len(state.Log) != 0 {
		t.Fatalf("persisted log after full compaction = %+v, want empty", state.Log)
	}
}

func TestCreateSnapshot_SnapshotSaveFailure_LeavesLogUntouched(t *testing.T) {
	persister := NewMemoryPersister()
	leader := commitUpToWithPersister(t, 3, persister)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}

	persister.FailNextSnapshotSave()
	if err := leader.CreateSnapshot(2, []byte("x")); err == nil {
		t.Fatalf("CreateSnapshot with a simulated snapshot-save failure: want error, got nil")
	}
	if got := leader.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after failed CreateSnapshot = %d, want unchanged 0", got)
	}
	leader.mu.Lock()
	_, hasEntry1 := leader.log.EntryAt(1)
	leader.mu.Unlock()
	if !hasEntry1 {
		t.Fatalf("log was compacted despite a failed snapshot save")
	}

	// Once persistence recovers, the same request must succeed normally.
	if err := leader.CreateSnapshot(2, []byte("x")); err != nil {
		t.Fatalf("retried CreateSnapshot after persistence recovered: %v", err)
	}
}

func TestCreateSnapshot_LogSaveFailure_RollsBackCompactionButKeepsSnapshot(t *testing.T) {
	persister := NewMemoryPersister()
	leader := commitUpToWithPersister(t, 3, persister)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}

	persister.FailNextSave()
	if err := leader.CreateSnapshot(2, []byte("x")); err == nil {
		t.Fatalf("CreateSnapshot with a simulated log-save failure: want error, got nil")
	}

	// The snapshot itself is durable (SaveSnapshot ran first and
	// succeeded)...
	snap, err := persister.LoadSnapshot()
	if err != nil || snap.LastIncludedIndex != 2 {
		t.Fatalf("LoadSnapshot after rolled-back compaction = %+v, %v, want index 2 persisted", snap, err)
	}
	// ...but the in-memory log must be rolled back to match what is
	// actually durable (SaveState never succeeded), not silently ahead of
	// it.
	leader.mu.Lock()
	boundary := leader.log.entries[0].Index
	_, hasEntry1 := leader.log.EntryAt(1)
	leader.mu.Unlock()
	if boundary != 0 || !hasEntry1 {
		t.Fatalf("in-memory log was not rolled back: boundary=%d hasEntry1=%v", boundary, hasEntry1)
	}
}

// --- Restart reconciliation of a snapshot + persisted log -----------------

func TestNewNode_RestartWithSnapshotOnly_ReconstructsFlooredLog(t *testing.T) {
	persister := NewMemoryPersister()
	leader := commitUpToWithPersister(t, 3, persister)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(3, []byte("full-state")); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}
	leader.Stop()

	restarted, err := NewNode(Options{ID: "node0", Peers: leader.peers, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := restarted.SnapshotIndex(); got != 3 {
		t.Fatalf("SnapshotIndex after restart = %d, want 3", got)
	}
	if got := restarted.LastLogIndex(); got != 3 {
		t.Fatalf("LastLogIndex after restart (no suffix beyond the snapshot) = %d, want 3", got)
	}
	snap, ok := restarted.PendingSnapshot()
	if !ok || string(snap.Data) != "full-state" {
		t.Fatalf("PendingSnapshot after restart = %+v, %v, want full-state pending", snap, ok)
	}
}

func TestNewNode_RestartWithSnapshotAndSuffix_DropsOverlapNoGap(t *testing.T) {
	persister := NewMemoryPersister()
	leader := commitUpToWithPersister(t, 3, persister)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	if err := leader.CreateSnapshot(2, []byte("state-through-2")); err != nil {
		t.Fatalf("CreateSnapshot(2): %v", err)
	}
	// Propose more entries after the snapshot.
	if _, _, err := leader.Propose(Command("after-snapshot")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	leader.Stop()

	restarted, err := NewNode(Options{ID: "node0", Peers: leader.peers, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := restarted.SnapshotIndex(); got != 2 {
		t.Fatalf("SnapshotIndex after restart = %d, want 2", got)
	}
	if got := restarted.LastLogIndex(); got != 4 {
		t.Fatalf("LastLogIndex after restart = %d, want 4 (suffix after the snapshot retained)", got)
	}
	e4, ok := restarted.log.EntryAt(4)
	if !ok || string(e4.Command) != "after-snapshot" {
		t.Fatalf("EntryAt(4) after restart = %+v, %v, want after-snapshot", e4, ok)
	}
}

// --- RequestVote / AppendEntries correctness after compaction --------------

func TestRequestVote_WorksAfterCompaction(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	persister := NewMemoryPersister()
	if err := persister.SaveState(PersistentState{CurrentTerm: 1, Log: l.entries[1:]}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := persister.SaveSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("x")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	voter, err := NewNode(Options{ID: "voter", Peers: []string{"cand"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// A candidate whose log matches voter's logical last position (5/1)
	// must be granted a vote.
	reply := voter.HandleRequestVote(RequestVoteArgs{Term: 2, CandidateID: "cand", LastLogIndex: 5, LastLogTerm: 1})
	if !reply.VoteGranted {
		t.Fatalf("vote denied for a candidate at least as up to date: %+v", reply)
	}
}

func TestRequestVote_RejectsStaleCandidateAfterCompaction(t *testing.T) {
	l := buildLog(5)
	if err := l.compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	persister := NewMemoryPersister()
	if err := persister.SaveState(PersistentState{CurrentTerm: 1, Log: l.entries[1:]}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := persister.SaveSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("x")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	voter, err := NewNode(Options{ID: "voter", Peers: []string{"cand"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// A candidate whose log is behind the voter's logical last position
	// (even behind the snapshot boundary itself) must be denied.
	reply := voter.HandleRequestVote(RequestVoteArgs{Term: 2, CandidateID: "cand", LastLogIndex: 2, LastLogTerm: 1})
	if reply.VoteGranted {
		t.Fatalf("vote granted for a stale candidate behind the snapshot boundary: %+v", reply)
	}
}

func TestAppendEntries_WorksExactlyAtSnapshotBoundary(t *testing.T) {
	persister := NewMemoryPersister()
	if err := persister.SaveSnapshot(Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 3, Data: []byte("x")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	follower, err := NewNode(Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	reply := follower.HandleAppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: "leader", PrevLogIndex: 5, PrevLogTerm: 3,
		Entries: []LogEntry{{Term: 3, Command: Command("after")}},
	})
	if !reply.Success {
		t.Fatalf("AppendEntries at the snapshot boundary rejected: %+v", reply)
	}
	if follower.LastLogIndex() != 6 {
		t.Fatalf("LastLogIndex = %d, want 6", follower.LastLogIndex())
	}
}

func TestAppendEntries_ConflictResolutionAfterCompaction(t *testing.T) {
	persister := NewMemoryPersister()
	if err := persister.SaveState(PersistentState{CurrentTerm: 2, Log: []LogEntry{
		{Index: 4, Term: 1},
		{Index: 5, Term: 2}, // stale entry at index 5
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := persister.SaveSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("x")}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	follower, err := NewNode(Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	reply := follower.HandleAppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: "leader", PrevLogIndex: 4, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Command: Command("C")}},
	})
	if !reply.Success {
		t.Fatalf("conflict-resolving AppendEntries after compaction rejected: %+v", reply)
	}
	e5, ok := follower.log.EntryAt(5)
	if !ok || e5.Term != 3 || string(e5.Command) != "C" {
		t.Fatalf("index 5 after conflict resolution = %+v, %v, want term 3 / C", e5, ok)
	}

	// Never truncates below the snapshot boundary.
	if _, ok := follower.log.EntryAt(3); ok {
		t.Fatalf("EntryAt(3) should still report false: it is covered by the snapshot, never a real retained entry")
	}
}

// --- Helpers ---------------------------------------------------------------

// commitUpToWithPersister is commitUpTo (see apply_test.go), but gives
// nodes[0] (the leader) a specific Persister instead of a fresh
// discardPersister, so a test can inspect what actually got persisted.
func commitUpToWithPersister(t *testing.T, n int, persister Persister) *Node {
	t.Helper()
	tr := NewInMemoryTransport()
	leader, err := NewNode(Options{ID: "node0", Peers: []string{"node1", "node2"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5, Persister: persister})
	if err != nil {
		t.Fatalf("NewNode(leader): %v", err)
	}
	tr.Register("node0", leader)
	follower1 := mustNewNode(t, Options{ID: "node1", Peers: []string{"node0", "node2"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("node1", follower1)
	follower2 := mustNewNode(t, Options{ID: "node2", Peers: []string{"node0", "node1"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("node2", follower2)

	electLeader(t, leader, 6)
	for i := 0; i < n; i++ {
		if _, _, err := leader.Propose(Command("x")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != uint64(n) {
		t.Fatalf("leader CommitIndex after %d proposals = %d, want %d", n, got, n)
	}
	return leader
}
