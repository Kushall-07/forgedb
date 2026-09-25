package raft

import (
	"bytes"
	"testing"
)

// --- 10. Heartbeats -------------------------------------------------------

func TestNode_Heartbeats_PreventElection(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 4, 4
		o.HeartbeatTick = 1
	})
	leader := nodes[0]
	electLeader(t, leader, 4)
	leaderTerm, _, _ := leader.State()

	followers := nodes[1:]
	for round := 0; round < 20; round++ {
		leader.Tick() // HeartbeatTick=1: sends every tick
		leader.Drain()
		for _, f := range followers {
			f.Tick() // 1 << ElectionTick=4, never times out on its own
		}
	}

	for _, f := range followers {
		term, role, leaderID := f.State()
		if role != Follower {
			t.Errorf("follower %s role = %s after sustained heartbeats, want Follower", f.ID(), role)
		}
		if term != leaderTerm {
			t.Errorf("follower %s term = %d, want unchanged leader term %d", f.ID(), term, leaderTerm)
		}
		if leaderID != leader.ID() {
			t.Errorf("follower %s leaderID = %q, want %q", f.ID(), leaderID, leader.ID())
		}
	}
}

// --- 11 / 21. Leader step-down --------------------------------------------

func TestNode_LeaderStepsDown_OnHigherTermAppendEntries(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	reply := leader.HandleAppendEntries(AppendEntriesArgs{Term: 99, LeaderID: "someone-else"})
	if !reply.Success {
		t.Fatalf("higher-term AppendEntries was rejected: %+v", reply)
	}

	term, role, leaderID := leader.State()
	if role != Follower {
		t.Fatalf("role after higher-term AppendEntries = %s, want Follower", role)
	}
	if term != 99 {
		t.Fatalf("term after higher-term AppendEntries = %d, want 99", term)
	}
	if leaderID != "someone-else" {
		t.Fatalf("leaderID = %q, want %q", leaderID, "someone-else")
	}
	if leader.IsLeader() {
		t.Fatalf("node still reports itself as leader")
	}
}

// TestNode_OldLeaderStepsDown_AfterPartitionHeals is a fuller, end-to-end
// version of the same invariant: a real leader (nodeA) is partitioned
// away, the remaining majority elects a new leader (nodeB) at a higher
// term, the partition heals, and nodeA must step down the moment nodeB's
// next heartbeat reaches it.
func TestNode_OldLeaderStepsDown_AfterPartitionHeals(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
		o.HeartbeatTick = 1
	})
	nodeA, nodeB := nodes[0], nodes[1]

	electLeader(t, nodeA, 5)
	termA, _, _ := nodeA.State()

	tr.Partition(nodeA.ID())

	// nodeB and nodeC are still a majority of the 3-node cluster (2 of
	// 3) and can elect a new leader among themselves even with nodeA cut
	// off.
	electLeader(t, nodeB, 5)
	termB, _, _ := nodeB.State()
	if termB <= termA {
		t.Fatalf("new leader's term %d did not exceed old leader's term %d", termB, termA)
	}

	tr.Heal(nodeA.ID())

	// nodeB's next heartbeat should reach the now-reachable nodeA and
	// force it to step down.
	nodeB.Tick()
	nodeB.Drain()

	term, role, leaderID := nodeA.State()
	if role != Follower {
		t.Fatalf("old leader role after partition heals = %s, want Follower", role)
	}
	if term != termB {
		t.Fatalf("old leader term after stepping down = %d, want %d", term, termB)
	}
	if leaderID != nodeB.ID() {
		t.Fatalf("old leader's leaderID = %q, want %q", leaderID, nodeB.ID())
	}
}

// --- 12. AppendEntries term rejection --------------------------------------

func TestNode_AppendEntries_StaleTermRejected(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)
	node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "a"}) // bump term to 5

	reply := node.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "stale-leader"})
	if reply.Success {
		t.Fatalf("stale-term AppendEntries was accepted: %+v", reply)
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if _, role, leaderID := node.State(); role != Follower || leaderID != "" {
		t.Fatalf("stale AppendEntries changed leader state: role=%s leaderID=%q", role, leaderID)
	}
}

// --- 13. prevLogIndex mismatch ----------------------------------------------

func TestNode_AppendEntries_PrevLogIndexMismatch(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "a",
		PrevLogIndex: 5, PrevLogTerm: 1, // way past the empty log
	})
	if reply.Success {
		t.Fatalf("AppendEntries with out-of-range PrevLogIndex was accepted: %+v", reply)
	}
	if node.LastLogIndex() != 0 {
		t.Fatalf("log was mutated despite a rejected AppendEntries: LastLogIndex=%d", node.LastLogIndex())
	}
}

// --- 14. prevLogTerm mismatch -----------------------------------------------

func TestNode_AppendEntries_PrevLogTermMismatch(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)

	ok := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Command: Command("i1")}},
	})
	if !ok.Success {
		t.Fatalf("initial append failed: %+v", ok)
	}

	// Now claim PrevLogIndex=1 has term 2, but the follower's index 1 is
	// actually term 1.
	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 2,
		Entries: []LogEntry{{Term: 1, Command: Command("i2")}},
	})
	if reply.Success {
		t.Fatalf("AppendEntries with mismatched PrevLogTerm was accepted: %+v", reply)
	}
	if node.LastLogIndex() != 1 {
		t.Fatalf("log was mutated despite a rejected AppendEntries: LastLogIndex=%d", node.LastLogIndex())
	}
}

// --- 15 / 16. Conflict truncation and matching-prefix preservation, via
// the Node/RPC surface rather than the Log directly (log_test.go already
// exercises Log.AppendAfter in isolation).

func TestNode_AppendEntries_ConflictTruncation(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)

	node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 1, Command: Command("i1")},
			{Term: 1, Command: Command("i2")},
			{Term: 2, Command: Command("i3-old")},
		},
	})

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: "a", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 3, Command: Command("i3-new")}},
	})
	if !reply.Success {
		t.Fatalf("conflict-resolving AppendEntries rejected: %+v", reply)
	}
	if node.LastLogIndex() != 3 {
		t.Fatalf("LastLogIndex = %d, want 3", node.LastLogIndex())
	}

	node.mu.Lock()
	e3, _ := node.log.EntryAt(3)
	node.mu.Unlock()
	if e3.Term != 3 || string(e3.Command) != "i3-new" {
		t.Fatalf("index 3 = %+v, want term 3 / i3-new", e3)
	}
}

func TestNode_AppendEntries_MatchingPrefixPreserved(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)

	entries := []LogEntry{
		{Term: 1, Command: Command("i1")},
		{Term: 1, Command: Command("i2")},
	}
	node.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: entries})

	// A retried/duplicate RPC re-sending the same entries must not
	// disturb the log.
	reply := node.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: entries})
	if !reply.Success {
		t.Fatalf("re-sent AppendEntries rejected: %+v", reply)
	}
	if node.LastLogIndex() != 2 {
		t.Fatalf("LastLogIndex = %d, want 2 (no duplication)", node.LastLogIndex())
	}
}

// --- 17. New entry replication ----------------------------------------------

func TestNode_Replication_ReachesFollowers(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	idx, term, err := leader.Propose(Command("set x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	for _, f := range nodes[1:] {
		if f.LastLogIndex() != idx {
			t.Fatalf("follower %s LastLogIndex = %d, want %d", f.ID(), f.LastLogIndex(), idx)
		}
		f.mu.Lock()
		e, ok := f.log.EntryAt(idx)
		f.mu.Unlock()
		if !ok || e.Term != term || !bytes.Equal(e.Command, Command("set x=1")) {
			t.Fatalf("follower %s entry at %d = %+v, %v", f.ID(), idx, e, ok)
		}
	}
}

// --- 18. Majority commit -----------------------------------------------------

func TestNode_MajorityCommit(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	idx, _, err := leader.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	if got := leader.CommitIndex(); got != idx {
		t.Fatalf("leader CommitIndex = %d, want %d (majority replicated)", got, idx)
	}
	entries := leader.CommittedEntries(1)
	if len(entries) != 1 || !bytes.Equal(entries[0].Command, Command("cmd")) {
		t.Fatalf("CommittedEntries = %+v", entries)
	}
}

// --- 19 / 25. Minority cannot commit / message loss --------------------------

func TestNode_PartitionedLeader_CannotFalselyCommit(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	// Cut the leader off from both followers: it is now a minority of
	// one out of three, which can never reach the majority of two
	// required to commit anything.
	tr.Partition(leader.ID())

	_, _, err := leader.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose on an isolated leader should still be accepted locally: %v", err)
	}
	leader.Drain()

	if got := leader.CommitIndex(); got != 0 {
		t.Fatalf("CommitIndex = %d, want 0 (no majority reachable)", got)
	}
}

func TestNode_OneFollowerPartitioned_StillCommitsViaRemainingMajority(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	// Only one follower is dropped; leader + the other follower is still
	// a majority of the 3-node cluster and must be able to commit.
	tr.Partition(nodes[2].ID())

	idx, _, err := leader.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	if got := leader.CommitIndex(); got != idx {
		t.Fatalf("CommitIndex = %d, want %d (leader + one follower is a majority of 3)", got, idx)
	}
}

// --- 20. Five-node majority ---------------------------------------------------

func TestNode_FiveNodeMajority(t *testing.T) {
	tr, nodes := newTestCluster(t, 5, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	t.Run("majority of three reachable commits", func(t *testing.T) {
		// leader + 2 of 4 followers = 3 of 5, the majority.
		tr.Partition(nodes[3].ID())
		tr.Partition(nodes[4].ID())
		defer func() {
			tr.Heal(nodes[3].ID())
			tr.Heal(nodes[4].ID())
		}()

		idx, _, err := leader.Propose(Command("cmd1"))
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		leader.Drain()

		if got := leader.CommitIndex(); got != idx {
			t.Fatalf("CommitIndex = %d, want %d", got, idx)
		}
	})

	t.Run("minority of two reachable cannot commit", func(t *testing.T) {
		committedBefore := leader.CommitIndex()
		tr.Partition(nodes[2].ID())
		tr.Partition(nodes[3].ID())
		tr.Partition(nodes[4].ID())
		defer func() {
			tr.Heal(nodes[2].ID())
			tr.Heal(nodes[3].ID())
			tr.Heal(nodes[4].ID())
		}()

		_, _, err := leader.Propose(Command("cmd2"))
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		leader.Drain()

		if got := leader.CommitIndex(); got != committedBefore {
			t.Fatalf("CommitIndex advanced to %d with only a minority (leader + 1) reachable", got)
		}
	})
}

// --- 22. Propose on follower ---------------------------------------------------

func TestNode_ProposeOnFollower_ReturnsNotLeader(t *testing.T) {
	_, nodes := newTestCluster(t, 3, nil)
	_, _, err := nodes[0].Propose(Command("cmd"))
	if err != ErrNotLeader {
		t.Fatalf("Propose on a Follower: err = %v, want ErrNotLeader", err)
	}
	if nodes[0].LastLogIndex() != 0 {
		t.Fatalf("Propose on a Follower mutated the log: LastLogIndex = %d", nodes[0].LastLogIndex())
	}
}

// --- 23. Commit/apply boundary --------------------------------------------------

func TestNode_CommitApplyBoundary(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	// Isolate one follower so the second proposed entry cannot reach a
	// majority (leader + the isolated follower would be a majority, but
	// isolating one of two followers still leaves leader + the other
	// follower as a majority -- so isolate leader from replication
	// entirely for the *second* entry by partitioning both followers).
	idxCommitted, _, err := leader.Propose(Command("committed"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if leader.CommitIndex() != idxCommitted {
		t.Fatalf("first entry did not commit: CommitIndex=%d", leader.CommitIndex())
	}

	tr.Partition(nodes[1].ID())
	tr.Partition(nodes[2].ID())
	idxUncommitted, _, err := leader.Propose(Command("uncommitted"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	if leader.CommitIndex() != idxCommitted {
		t.Fatalf("CommitIndex advanced past the last majority-replicated entry: %d", leader.CommitIndex())
	}

	got := leader.CommittedEntries(1)
	if len(got) != 1 || !bytes.Equal(got[0].Command, Command("committed")) {
		t.Fatalf("CommittedEntries = %+v, want exactly the committed entry", got)
	}
	// The uncommitted entry must not be exposed across the apply
	// boundary even though it exists in the leader's own log.
	if leader.LastLogIndex() <= idxCommitted {
		t.Fatalf("test setup broken: uncommitted entry wasn't appended (LastLogIndex=%d)", leader.LastLogIndex())
	}
	for _, e := range got {
		if e.Index > leader.CommitIndex() {
			t.Fatalf("CommittedEntries leaked an entry past CommitIndex: %+v", e)
		}
	}
	_ = idxUncommitted
}

// --- 24. Term monotonicity stress ------------------------------------------------

func TestNode_TermMonotonicity_Stress(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 3, 6
	})
	lastTerm := make([]uint64, len(nodes))

	for round := 0; round < 200; round++ {
		switch round % 15 {
		case 0:
			tr.Partition(nodes[(round/15)%len(nodes)].ID())
		case 7:
			tr.Heal(nodes[(round/15)%len(nodes)].ID())
		}

		tickAll(nodes)
		drainAll(nodes)

		for i, n := range nodes {
			term, _, _ := n.State()
			if term < lastTerm[i] {
				t.Fatalf("round %d: node %s term decreased %d -> %d", round, n.ID(), lastTerm[i], term)
			}
			lastTerm[i] = term
		}
	}

	for _, n := range nodes {
		tr.Heal(n.ID())
	}
}
