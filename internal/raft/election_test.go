package raft

import (
	"math/rand"
	"testing"
)

// --- 1. Initial state -------------------------------------------------

func TestNode_InitialState_AllFollowers(t *testing.T) {
	_, nodes := newTestCluster(t, 3, nil)
	for _, n := range nodes {
		term, role, leader := n.State()
		if role != Follower {
			t.Errorf("node %s role = %s, want Follower", n.ID(), role)
		}
		if term != 0 {
			t.Errorf("node %s term = %d, want 0", n.ID(), term)
		}
		if leader != "" {
			t.Errorf("node %s leaderID = %q, want empty", n.ID(), leader)
		}
	}
}

// --- 2. Candidate transition ------------------------------------------

func TestNode_ElectionTimeout_BecomesCandidate(t *testing.T) {
	tr := NewInMemoryTransport()
	// Isolate the node so no votes can ever arrive; this isolates the
	// Follower->Candidate transition itself from the rest of the
	// election, which is covered separately below.
	opts := Options{
		ID: "solo", Peers: []string{"ghost"}, Transport: tr,
		ElectionTickMin: 3, ElectionTickMax: 3,
	}
	node := NewNode(opts)
	tr.Register("solo", node)

	for i := 0; i < 2; i++ {
		node.Tick()
	}
	if _, role, _ := node.State(); role != Follower {
		t.Fatalf("role after 2 sub-threshold ticks = %s, want Follower", role)
	}

	node.Tick() // 3rd tick crosses the fixed timeout
	node.Drain()

	term, role, _ := node.State()
	if role != Candidate {
		t.Fatalf("role after election timeout = %s, want Candidate", role)
	}
	if term != 1 {
		t.Fatalf("term after election timeout = %d, want 1", term)
	}
}

// --- 3. Self vote -------------------------------------------------------

func TestNode_Candidate_VotesForSelf(t *testing.T) {
	tr := NewInMemoryTransport()
	opts := Options{
		ID: "solo", Peers: []string{"ghost"}, Transport: tr,
		ElectionTickMin: 1, ElectionTickMax: 1,
	}
	node := NewNode(opts)
	tr.Register("solo", node)

	node.Tick()
	node.Drain()

	node.mu.Lock()
	votedFor := node.votedFor
	votesReceived := node.votesReceived["solo"]
	node.mu.Unlock()

	if votedFor != "solo" {
		t.Errorf("votedFor = %q, want %q", votedFor, "solo")
	}
	if !votesReceived {
		t.Errorf("candidate did not count its own vote")
	}
}

// --- 4. Majority election -----------------------------------------------

func TestNode_MajorityVotes_BecomesLeader(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 10, 20
	})

	electLeader(t, nodes[0], 20)

	term, _, _ := nodes[0].State()
	if term != 1 {
		t.Fatalf("leader term = %d, want 1", term)
	}
	count, _ := countLeaders(nodes)
	if count != 1 {
		t.Fatalf("leader count = %d, want 1", count)
	}
}

func TestNode_SingleNodeCluster_ElectsSelfImmediately(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "solo", Transport: tr, ElectionTickMin: 2, ElectionTickMax: 2})
	tr.Register("solo", node)

	node.Tick()
	node.Tick()
	node.Drain()

	if !node.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}
}

// --- 5. Election failure --------------------------------------------------

func TestNode_ElectionFailure_NoMajority(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})

	// Isolate node0 so its RequestVote RPCs can never be delivered: it
	// can never gather a majority, and must not become leader.
	tr.Partition(nodes[0].ID())

	for i := 0; i < 5; i++ {
		nodes[0].Tick()
	}
	nodes[0].Drain()

	term, role, _ := nodes[0].State()
	if role != Candidate {
		t.Fatalf("role = %s, want Candidate (no majority reachable)", role)
	}
	if term != 1 {
		t.Fatalf("term = %d, want 1", term)
	}
	if nodes[0].IsLeader() {
		t.Fatalf("isolated candidate incorrectly became leader")
	}
}

// --- 6. One vote per term ------------------------------------------------

func TestNode_OneVotePerTerm(t *testing.T) {
	tr := NewInMemoryTransport()
	voter := NewNode(Options{ID: "voter", Peers: []string{"a", "b"}, Transport: tr})
	tr.Register("voter", voter)

	first := voter.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "a"})
	if !first.VoteGranted {
		t.Fatalf("first vote request denied: %+v", first)
	}

	second := voter.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "b"})
	if second.VoteGranted {
		t.Fatalf("second candidate in same term was granted a vote: %+v", second)
	}

	// The same candidate asking again in the same term is fine (e.g. a
	// retried RPC) -- it is not a *second* vote.
	again := voter.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "a"})
	if !again.VoteGranted {
		t.Fatalf("re-request from the already-voted-for candidate was denied: %+v", again)
	}
}

// --- 7. Higher-term RequestVote ------------------------------------------

func TestNode_HigherTermRequestVote_StepsDownAndUpdatesTerm(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr, ElectionTickMin: 1, ElectionTickMax: 1})
	tr.Register("n", node)

	// Make it a candidate at term 1 first.
	node.Tick()
	node.Drain()
	if term, role, _ := node.State(); role != Candidate || term != 1 {
		t.Fatalf("precondition failed: term=%d role=%s", term, role)
	}

	reply := node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "a"})
	term, role, _ := node.State()
	if role != Follower {
		t.Fatalf("role after higher-term RequestVote = %s, want Follower", role)
	}
	if term != 5 {
		t.Fatalf("term after higher-term RequestVote = %d, want 5", term)
	}
	if !reply.VoteGranted {
		t.Fatalf("vote should be granted once stepped down to the new term: %+v", reply)
	}
}

// --- 8. Stale RequestVote --------------------------------------------------

func TestNode_StaleRequestVote_Rejected(t *testing.T) {
	tr := NewInMemoryTransport()
	node := NewNode(Options{ID: "n", Peers: []string{"a"}, Transport: tr})
	tr.Register("n", node)

	// Bump the node's term up first (simulating it having seen a later
	// term already), then send an older-term RequestVote.
	node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "someone-else"})

	reply := node.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "a"})
	if reply.VoteGranted {
		t.Fatalf("stale-term RequestVote was granted: %+v", reply)
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 (node's current term)", reply.Term)
	}

	term, role, _ := node.State()
	if term != 5 || role != Follower {
		t.Fatalf("node state changed by a stale RequestVote: term=%d role=%s", term, role)
	}
}

// --- Majority calculation --------------------------------------------------

func TestNode_MajorityCalculation(t *testing.T) {
	tests := []struct {
		peers int // len(n.peers), i.e. cluster size - 1
		want  int
	}{
		{0, 1}, // 1-node cluster
		{2, 2}, // 3-node cluster
		{4, 3}, // 5-node cluster
		{1, 2}, // 2-node cluster (edge case, not otherwise used)
		{6, 4}, // 7-node cluster
	}
	for _, tt := range tests {
		peers := make([]string, tt.peers)
		for i := range peers {
			peers[i] = "p"
		}
		n := NewNode(Options{ID: "n", Peers: peers, Transport: NewInMemoryTransport(), Rand: rand.New(rand.NewSource(1))})
		if got := n.majority(); got != tt.want {
			t.Errorf("majority() with %d peers (cluster size %d) = %d, want %d", tt.peers, tt.peers+1, got, tt.want)
		}
	}
}
