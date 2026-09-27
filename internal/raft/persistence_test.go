package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// withValidChecksum appends a correctly computed CRC-32C checksum to body,
// producing a byte slice that passes decodeState's checksum check and
// therefore exercises whatever length/bounds validation comes after it --
// as opposed to failing earlier at the checksum mismatch, which a
// maliciously (rather than accidentally) corrupted file wouldn't.
func withValidChecksum(body []byte) []byte {
	checksum := crc32.Checksum(body, crcTable)
	out := make([]byte, len(body)+4)
	copy(out, body)
	binary.LittleEndian.PutUint32(out[len(body):], checksum)
	return out
}

// --- Fresh startup ---------------------------------------------------------

func TestPersistence_FreshStartup_NoStateFile(t *testing.T) {
	dir := t.TempDir()
	p := NewFilePersister(filepath.Join(dir, "raft-state"))

	node, err := NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: p})
	if err != nil {
		t.Fatalf("NewNode with no existing state file: %v", err)
	}
	term, role, _ := node.State()
	if term != 0 || role != Follower {
		t.Fatalf("fresh node state = term %d role %s, want term 0 Follower", term, role)
	}
	if node.LastLogIndex() != 0 {
		t.Fatalf("fresh node LastLogIndex = %d, want 0", node.LastLogIndex())
	}
}

func TestPersistence_FreshStartup_MemoryPersister(t *testing.T) {
	node, err := NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: NewMemoryPersister()})
	if err != nil {
		t.Fatalf("NewNode with fresh MemoryPersister: %v", err)
	}
	if term, role, _ := node.State(); term != 0 || role != Follower {
		t.Fatalf("fresh node state = term %d role %s, want term 0 Follower", term, role)
	}
}

// --- Term persistence -------------------------------------------------------

func TestPersistence_Term_SurvivesRestart(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"a"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	// Bump the term via a RequestVote from a higher-term peer.
	node.HandleRequestVote(RequestVoteArgs{Term: 7, CandidateID: "a"})
	if term, _, _ := node.State(); term != 7 {
		t.Fatalf("term after RequestVote = %d, want 7", term)
	}
	node.Stop()

	restarted, err := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if term, role, _ := restarted.State(); term != 7 {
		t.Fatalf("term after restart = %d, want 7", term)
	} else if role != Follower {
		t.Fatalf("role after restart = %s, want Follower", role)
	}
}

// --- Vote persistence --------------------------------------------------------

func TestPersistence_Vote_PreventsSecondVoteAfterRestart(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	voter := mustNewNode(t, Options{ID: "voter", Peers: []string{"a", "b"}, Transport: tr, Persister: persister})
	tr.Register("voter", voter)

	reply := voter.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "a"})
	if !reply.VoteGranted {
		t.Fatalf("first vote denied: %+v", reply)
	}
	voter.Stop()

	restarted, err := NewNode(Options{ID: "voter", Peers: []string{"a", "b"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	second := restarted.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "b"})
	if second.VoteGranted {
		t.Fatalf("restarted node granted a second vote in term 3 to a different candidate: %+v", second)
	}

	// The original candidate can still be re-confirmed (idempotent retry).
	again := restarted.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "a"})
	if !again.VoteGranted {
		t.Fatalf("restarted node denied a re-request from the already-voted-for candidate: %+v", again)
	}
}

// --- Log persistence ---------------------------------------------------------

func TestPersistence_Log_ExactlyRestored(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Transport: tr, ElectionTickMin: 2, ElectionTickMax: 2, Persister: persister})
	tr.Register("n", node)

	node.Tick() // becomes candidate
	node.Tick() // single-node cluster: elects itself leader
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}

	if _, _, err := node.Propose(Command("cmd-a")); err != nil {
		t.Fatalf("Propose cmd-a: %v", err)
	}
	if _, _, err := node.Propose(Command("cmd-b")); err != nil {
		t.Fatalf("Propose cmd-b: %v", err)
	}
	node.Drain()
	wantLastIndex := node.LastLogIndex()
	node.Stop()

	restarted, err := NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if restarted.LastLogIndex() != wantLastIndex {
		t.Fatalf("restarted LastLogIndex = %d, want %d", restarted.LastLogIndex(), wantLastIndex)
	}

	restarted.mu.Lock()
	e1, ok1 := restarted.log.EntryAt(1)
	e2, ok2 := restarted.log.EntryAt(2)
	restarted.mu.Unlock()
	if !ok1 || string(e1.Command) != "cmd-a" {
		t.Fatalf("restored entry 1 = %+v, %v, want cmd-a", e1, ok1)
	}
	if !ok2 || string(e2.Command) != "cmd-b" {
		t.Fatalf("restored entry 2 = %+v, %v, want cmd-b", e2, ok2)
	}
}

// --- Combined state -----------------------------------------------------------

func TestPersistence_CombinedTermVoteLog_AllRestored(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"a"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	node.HandleAppendEntries(AppendEntriesArgs{
		Term: 4, LeaderID: "a",
		Entries: []LogEntry{
			{Term: 4, Command: Command("x")},
			{Term: 4, Command: Command("y")},
		},
	})
	// LastLogIndex/LastLogTerm must be at least as up-to-date as this
	// node's own log (index 2, term 4) for the vote to be granted --
	// otherwise logIsUpToDate correctly refuses it regardless of votedFor.
	reply := node.HandleRequestVote(RequestVoteArgs{Term: 4, CandidateID: "someone-else", LastLogIndex: 2, LastLogTerm: 4})
	if !reply.VoteGranted {
		t.Fatalf("vote denied, want granted (votedFor was still empty and the candidate's log is up to date): %+v", reply)
	}
	node.Stop()

	restarted, err := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	term, role, _ := restarted.State()
	if term != 4 {
		t.Fatalf("restored term = %d, want 4", term)
	}
	if role != Follower {
		t.Fatalf("restored role = %s, want Follower", role)
	}
	if restarted.votedFor != "someone-else" {
		t.Fatalf("restored votedFor = %q, want %q", restarted.votedFor, "someone-else")
	}
	if restarted.LastLogIndex() != 2 {
		t.Fatalf("restored LastLogIndex = %d, want 2", restarted.LastLogIndex())
	}
}

// --- Restart election behavior -------------------------------------------------

func TestPersistence_RestartedNode_StartsFollowerAndCanElect(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Transport: tr, ElectionTickMin: 2, ElectionTickMax: 2, Persister: persister})
	tr.Register("n", node)

	node.Tick()
	node.Tick()
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("node did not elect itself leader before restart")
	}
	node.Stop()

	tr2 := NewInMemoryTransport()
	restarted, err := NewNode(Options{ID: "n", Transport: tr2, ElectionTickMin: 2, ElectionTickMax: 2, Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	tr2.Register("n", restarted)

	if restarted.IsLeader() {
		t.Fatalf("restarted node resumed leadership; a restart must never restore an old leader role")
	}
	if _, role, _ := restarted.State(); role != Follower {
		t.Fatalf("restarted node role = %s, want Follower", role)
	}

	// It must still be able to win a new election.
	restarted.Tick()
	restarted.Tick()
	restarted.Drain()
	if !restarted.IsLeader() {
		t.Fatalf("restarted node could not win a new election")
	}
}

// --- Restart with existing log: participates correctly in AppendEntries -------

func TestPersistence_RestartedFollower_AcceptsMatchingAppendEntries(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"leader"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{{Term: 1, Command: Command("a")}, {Term: 1, Command: Command("b")}},
	})
	node.Stop()

	restarted, err := NewNode(Options{ID: "n", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	// The leader continues from where the follower's log actually is.
	reply := restarted.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "leader", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 1, Command: Command("c")}},
	})
	if !reply.Success {
		t.Fatalf("AppendEntries after restart rejected: %+v", reply)
	}
	if restarted.LastLogIndex() != 3 {
		t.Fatalf("LastLogIndex after post-restart append = %d, want 3", restarted.LastLogIndex())
	}
}

// --- Conflict after restart -----------------------------------------------------

func TestPersistence_ConflictAfterRestart_ResolvedAndPersisted(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"leader"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	// Persist a log with a stale entry at index 3 (term 2), as if from an
	// old leader: A(1) B(1) X(2).
	node.HandleAppendEntries(AppendEntriesArgs{
		Term: 2, LeaderID: "leader",
		Entries: []LogEntry{
			{Term: 1, Command: Command("A")},
			{Term: 1, Command: Command("B")},
			{Term: 2, Command: Command("X")},
		},
	})
	node.Stop()

	restarted, err := NewNode(Options{ID: "n", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	// The new leader's log at index 3 is actually C at term 3: the
	// existing entry X must be discarded and replaced, exactly as
	// Log.AppendAfter's conflict rule requires (see log_test.go).
	reply := restarted.HandleAppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: "leader", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Command: Command("C")}},
	})
	if !reply.Success {
		t.Fatalf("conflict-resolving AppendEntries after restart rejected: %+v", reply)
	}

	restarted.mu.Lock()
	e3, ok := restarted.log.EntryAt(3)
	restarted.mu.Unlock()
	if !ok || e3.Term != 3 || string(e3.Command) != "C" {
		t.Fatalf("index 3 after conflict resolution = %+v, %v, want term 3 / C", e3, ok)
	}

	// And the resolved log must itself now be durable: a second restart
	// must see C, not X, at index 3.
	restarted.Stop()
	twiceRestarted, err := NewNode(Options{ID: "n", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("second restart: %v", err)
	}
	twiceRestarted.mu.Lock()
	e3Again, ok := twiceRestarted.log.EntryAt(3)
	twiceRestarted.mu.Unlock()
	if !ok || e3Again.Term != 3 || string(e3Again.Command) != "C" {
		t.Fatalf("index 3 after second restart = %+v, %v, want term 3 / C", e3Again, ok)
	}
}

// --- Missing state -----------------------------------------------------------

func TestPersistence_MissingStateFile_StartsFresh(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist", "raft-state")
	node, err := NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: NewFilePersister(path)})
	if err != nil {
		t.Fatalf("NewNode with missing state file should succeed, got: %v", err)
	}
	if term, role, _ := node.State(); term != 0 || role != Follower {
		t.Fatalf("fresh node state = term %d role %s, want term 0 Follower", term, role)
	}
}

// --- Corrupt state -------------------------------------------------------------

func TestPersistence_CorruptState_StartupFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	p := NewFilePersister(path)
	if err := p.SaveState(PersistentState{CurrentTerm: 5, VotedFor: "a"}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corrupted := append([]byte(nil), data...)
	corrupted[len(corrupted)-1] ^= 0xFF // flip a bit in the checksum
	if err := os.WriteFile(path, corrupted, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: NewFilePersister(path)})
	if err == nil {
		t.Fatalf("NewNode with corrupt state file succeeded, want an explicit failure")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewNode error = %v, want it to wrap ErrCorrupt", err)
	}
}

func TestPersistence_BadMagic_StartupFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x00}, 64), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := NewFilePersister(path).LoadState()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("LoadState with bad magic = %v, want ErrCorrupt", err)
	}
}

// --- Truncated state -----------------------------------------------------------

func TestPersistence_TruncatedState_StartupFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	p := NewFilePersister(path)
	if err := p.SaveState(PersistentState{
		CurrentTerm: 2,
		VotedFor:    "a",
		Log:         []LogEntry{{Index: 1, Term: 1, Command: Command("hello")}},
	}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	truncated := data[:len(data)/2]
	if err := os.WriteFile(path, truncated, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = NewNode(Options{ID: "n", Transport: NewInMemoryTransport(), Persister: NewFilePersister(path)})
	if err == nil {
		t.Fatalf("NewNode with truncated state file succeeded, want an explicit failure")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewNode error = %v, want it to wrap ErrCorrupt", err)
	}
}

// --- Checksum corruption ---------------------------------------------------------

func TestPersistence_ChecksumMismatch_Detected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	p := NewFilePersister(path)
	if err := p.SaveState(PersistentState{CurrentTerm: 9, VotedFor: "x"}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Flip a bit in the middle of the body (the term field) without
	// touching the checksum bytes at the end.
	mutated := append([]byte(nil), data...)
	mutated[15] ^= 0x01
	if err := os.WriteFile(path, mutated, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = NewFilePersister(path).LoadState()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("LoadState after body mutation = %v, want ErrCorrupt (checksum mismatch)", err)
	}
}

// --- Invalid lengths -------------------------------------------------------------

func TestPersistence_MaliciousLogLength_RejectedWithoutHugeAllocation(t *testing.T) {
	// A hand-crafted header claiming an enormous log entry count. If
	// decodeState didn't validate this before allocating, this test would
	// hang or OOM instead of failing fast.
	var body []byte
	body = append(body, raftMagic[:]...)
	body = append(body, 1, 0, 0, 0)             // version
	body = append(body, 0, 0, 0, 0, 0, 0, 0, 0) // currentTerm = 0
	body = append(body, 0, 0, 0, 0)             // votedForLen = 0
	body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // numEntries = huge

	_, err := decodeState(withValidChecksum(body))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeState with huge entry count = %v, want ErrCorrupt", err)
	}
}

func TestPersistence_MaliciousVotedForLength_Rejected(t *testing.T) {
	var body []byte
	body = append(body, raftMagic[:]...)
	body = append(body, 1, 0, 0, 0)
	body = append(body, 0, 0, 0, 0, 0, 0, 0, 0)
	body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // votedForLen = huge

	_, err := decodeState(withValidChecksum(body))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeState with huge votedFor length = %v, want ErrCorrupt", err)
	}
}

func TestPersistence_MaliciousCommandLength_Rejected(t *testing.T) {
	state := PersistentState{
		CurrentTerm: 1,
		Log:         []LogEntry{{Index: 1, Term: 1, Command: Command("ok")}},
	}
	data, err := encodeState(state)
	if err != nil {
		t.Fatalf("encodeState: %v", err)
	}
	// The command length for the single entry sits right after
	// index(8)+term(8) within the entry, itself right after the header +
	// votedFor + numEntries fields (votedFor is empty here).
	cmdLenOff := raftHeaderSize + 0 /* votedFor */ + 4 /* numEntries */ + 8 + 8
	mutated := append([]byte(nil), data...)
	mutated[cmdLenOff] = 0xFF
	mutated[cmdLenOff+1] = 0xFF
	mutated[cmdLenOff+2] = 0xFF
	mutated[cmdLenOff+3] = 0xFF

	_, err = decodeState(mutated)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeState with huge command length = %v, want ErrCorrupt", err)
	}
}

// --- Persistence failure ---------------------------------------------------------

func TestPersistence_SaveFailure_VoteNotGranted(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"a"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	persister.FailNextSave()
	reply := node.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "a"})
	if reply.VoteGranted {
		t.Fatalf("vote was reported granted despite a persistence failure: %+v", reply)
	}
	if node.votedFor != "" {
		t.Fatalf("votedFor = %q after a failed persist, want unchanged empty", node.votedFor)
	}

	// Once persistence recovers, the same request must succeed normally.
	retry := node.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "a"})
	if !retry.VoteGranted {
		t.Fatalf("retried vote after persistence recovered was denied: %+v", retry)
	}
}

func TestPersistence_SaveFailure_TermStepDownRolledBack(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"a"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	persister.FailNextSave()
	reply := node.HandleRequestVote(RequestVoteArgs{Term: 9, CandidateID: "a"})
	if reply.Term != 0 {
		t.Fatalf("reply.Term = %d after a failed term persist, want unchanged 0", reply.Term)
	}
	if term, role, _ := node.State(); term != 0 || role != Follower {
		t.Fatalf("state after failed term persist = term %d role %s, want term 0 Follower (rolled back)", term, role)
	}
}

func TestPersistence_SaveFailure_ProposeReturnsErrorAndRollsBack(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Transport: tr, ElectionTickMin: 1, ElectionTickMax: 1, Persister: persister})
	tr.Register("n", node)
	node.Tick()
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}

	beforeLen := node.LastLogIndex()
	persister.FailNextSave()
	_, _, err := node.Propose(Command("cmd"))
	if err == nil {
		t.Fatalf("Propose succeeded despite a persistence failure")
	}
	if node.LastLogIndex() != beforeLen {
		t.Fatalf("log grew despite a failed persist: LastLogIndex = %d, want %d", node.LastLogIndex(), beforeLen)
	}
}

func TestPersistence_SaveFailure_AppendEntriesRolledBack(t *testing.T) {
	persister := NewMemoryPersister()
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "n", Peers: []string{"leader"}, Transport: tr, Persister: persister})
	tr.Register("n", node)

	persister.FailNextSave()
	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		Entries: []LogEntry{{Term: 1, Command: Command("a")}},
	})
	if reply.Success {
		t.Fatalf("AppendEntries reported success despite a persistence failure: %+v", reply)
	}
	if node.LastLogIndex() != 0 {
		t.Fatalf("log mutated despite a failed persist: LastLogIndex = %d, want 0", node.LastLogIndex())
	}
}

// --- Atomic replacement -----------------------------------------------------------

func TestPersistence_AtomicReplacement_FailedEncodeKeepsOldState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	p := NewFilePersister(path)

	good := PersistentState{CurrentTerm: 3, VotedFor: "a", Log: []LogEntry{{Index: 1, Term: 1, Command: Command("ok")}}}
	if err := p.SaveState(good); err != nil {
		t.Fatalf("SaveState(good): %v", err)
	}

	oversized := PersistentState{CurrentTerm: 4, Log: []LogEntry{{Index: 1, Term: 1, Command: make(Command, maxCommandSize+1)}}}
	if err := p.SaveState(oversized); err == nil {
		t.Fatalf("SaveState(oversized) unexpectedly succeeded")
	}

	got, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState after failed save: %v", err)
	}
	if got.CurrentTerm != 3 || got.VotedFor != "a" {
		t.Fatalf("state after failed save = %+v, want the previous good state preserved", got)
	}
}

func TestPersistence_AtomicReplacement_StrayTempFileIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")
	p := NewFilePersister(path)

	good := PersistentState{CurrentTerm: 5, VotedFor: "b"}
	if err := p.SaveState(good); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Simulate a crash between "write the temp file" and "rename it into
	// place": a stray, incomplete .tmp file is left behind, but the real
	// path was never touched.
	if err := os.WriteFile(path+".tmp", []byte("garbage, never renamed"), 0o644); err != nil {
		t.Fatalf("WriteFile(.tmp): %v", err)
	}

	got, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState with a stray .tmp file present: %v", err)
	}
	if got.CurrentTerm != 5 || got.VotedFor != "b" {
		t.Fatalf("state with a stray .tmp file present = %+v, want the last real save preserved", got)
	}
}

// --- Repeated restart --------------------------------------------------------------

func TestPersistence_RepeatedRestart_StateAccumulatesCorrectly(t *testing.T) {
	persister := NewMemoryPersister()
	var wantTerm uint64
	var wantLastIndex uint64

	for round := 0; round < 5; round++ {
		tr := NewInMemoryTransport()
		node, err := NewNode(Options{ID: "n", Transport: tr, ElectionTickMin: 1, ElectionTickMax: 1, Persister: persister})
		if err != nil {
			t.Fatalf("round %d: NewNode: %v", round, err)
		}
		tr.Register("n", node)

		if term, role, _ := node.State(); term != wantTerm {
			t.Fatalf("round %d: term on restart = %d, want %d", round, term, wantTerm)
		} else if role != Follower {
			t.Fatalf("round %d: role on restart = %s, want Follower", round, role)
		}
		if node.LastLogIndex() != wantLastIndex {
			t.Fatalf("round %d: LastLogIndex on restart = %d, want %d", round, node.LastLogIndex(), wantLastIndex)
		}

		node.Tick() // single-node cluster: wins its own election, term++
		node.Drain()
		if !node.IsLeader() {
			t.Fatalf("round %d: did not become leader", round)
		}
		wantTerm++

		if _, _, err := node.Propose(Command("cmd")); err != nil {
			t.Fatalf("round %d: Propose: %v", round, err)
		}
		node.Drain()
		wantLastIndex++

		node.Stop()
	}
}

// --- gofmt/govet hygiene: exercise MemoryPersister deep-copy isolation -------------

func TestMemoryPersister_SaveStateIsolatesCallerSlice(t *testing.T) {
	p := NewMemoryPersister()
	log := []LogEntry{{Index: 1, Term: 1, Command: Command("a")}}
	if err := p.SaveState(PersistentState{Log: log}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	log[0].Command[0] = 'z' // mutate the caller's own copy after saving

	got, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if string(got.Log[0].Command) != "a" {
		t.Fatalf("saved state was aliased to the caller's slice: got %q, want %q", got.Log[0].Command, "a")
	}
}
