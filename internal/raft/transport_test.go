package raft

import "testing"

// fakeHandler is a minimal RPCHandler for exercising InMemoryTransport in
// isolation, without needing a full Node.
type fakeHandler struct {
	voteReply    RequestVoteReply
	appendReply  AppendEntriesReply
	installReply InstallSnapshotReply
}

func (f *fakeHandler) HandleRequestVote(RequestVoteArgs) RequestVoteReply { return f.voteReply }
func (f *fakeHandler) HandleAppendEntries(AppendEntriesArgs) AppendEntriesReply {
	return f.appendReply
}
func (f *fakeHandler) HandleInstallSnapshot(InstallSnapshotArgs) InstallSnapshotReply {
	return f.installReply
}

func TestInMemoryTransport_DeliversToRegisteredHandler(t *testing.T) {
	tr := NewInMemoryTransport()
	tr.Register("b", &fakeHandler{voteReply: RequestVoteReply{Term: 7, VoteGranted: true}})

	reply, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a", Term: 7})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reply.VoteGranted || reply.Term != 7 {
		t.Fatalf("reply = %+v, want VoteGranted=true Term=7", reply)
	}
}

func TestInMemoryTransport_UnknownTarget(t *testing.T) {
	tr := NewInMemoryTransport()
	_, err := tr.SendRequestVote("ghost", RequestVoteArgs{CandidateID: "a"})
	if err != ErrPeerUnreachable {
		t.Fatalf("err = %v, want ErrPeerUnreachable", err)
	}
}

func TestInMemoryTransport_Partition_BlocksBothDirections(t *testing.T) {
	tr := NewInMemoryTransport()
	tr.Register("a", &fakeHandler{})
	tr.Register("b", &fakeHandler{voteReply: RequestVoteReply{VoteGranted: true}})

	tr.Partition("b")

	// b cannot receive.
	if _, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a"}); err != ErrPeerUnreachable {
		t.Fatalf("send to partitioned node: err = %v, want ErrPeerUnreachable", err)
	}
	// b cannot send (identified as the RPC's origin).
	if _, err := tr.SendRequestVote("a", RequestVoteArgs{CandidateID: "b"}); err != ErrPeerUnreachable {
		t.Fatalf("send from partitioned node: err = %v, want ErrPeerUnreachable", err)
	}
}

func TestInMemoryTransport_Heal_RestoresDelivery(t *testing.T) {
	tr := NewInMemoryTransport()
	tr.Register("a", &fakeHandler{})
	tr.Register("b", &fakeHandler{voteReply: RequestVoteReply{VoteGranted: true}})

	tr.Partition("b")
	if _, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a"}); err == nil {
		t.Fatalf("expected send to partitioned node to fail")
	}

	tr.Heal("b")
	reply, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a"})
	if err != nil {
		t.Fatalf("unexpected error after heal: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("reply after heal = %+v, want VoteGranted=true", reply)
	}
}

func TestInMemoryTransport_PartitionGroups_SplitsIntoSides(t *testing.T) {
	tr := NewInMemoryTransport()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		tr.Register(id, &fakeHandler{voteReply: RequestVoteReply{VoteGranted: true}})
	}

	// "A B C | D E": within each side, communication works; across the
	// split, it fails in both directions -- including between d and e,
	// which a pair of single-node Partition calls could not express (see
	// PartitionGroups's doc comment).
	tr.PartitionGroups([]string{"a", "b", "c"}, []string{"d", "e"})

	if _, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a"}); err != nil {
		t.Fatalf("a -> b (same side): err = %v, want nil", err)
	}
	if _, err := tr.SendRequestVote("e", RequestVoteArgs{CandidateID: "d"}); err != nil {
		t.Fatalf("d -> e (same side): err = %v, want nil", err)
	}
	if _, err := tr.SendRequestVote("d", RequestVoteArgs{CandidateID: "a"}); err != ErrPeerUnreachable {
		t.Fatalf("a -> d (across split): err = %v, want ErrPeerUnreachable", err)
	}
	if _, err := tr.SendRequestVote("a", RequestVoteArgs{CandidateID: "e"}); err != ErrPeerUnreachable {
		t.Fatalf("e -> a (across split): err = %v, want ErrPeerUnreachable", err)
	}

	tr.HealPartitions()
	if _, err := tr.SendRequestVote("d", RequestVoteArgs{CandidateID: "a"}); err != nil {
		t.Fatalf("a -> d after HealPartitions: err = %v, want nil", err)
	}
}

func TestInMemoryTransport_PartitionGroups_NodeLeftOutRemainsReachable(t *testing.T) {
	tr := NewInMemoryTransport()
	for _, id := range []string{"a", "b", "c"} {
		tr.Register(id, &fakeHandler{voteReply: RequestVoteReply{VoteGranted: true}})
	}

	// c is not mentioned in either group: it remains reachable from (and
	// to) both a and b even while they are split from each other.
	tr.PartitionGroups([]string{"a"}, []string{"b"})

	if _, err := tr.SendRequestVote("c", RequestVoteArgs{CandidateID: "a"}); err != nil {
		t.Fatalf("a -> c (c unmentioned): err = %v, want nil", err)
	}
	if _, err := tr.SendRequestVote("c", RequestVoteArgs{CandidateID: "b"}); err != nil {
		t.Fatalf("b -> c (c unmentioned): err = %v, want nil", err)
	}
	if _, err := tr.SendRequestVote("b", RequestVoteArgs{CandidateID: "a"}); err != ErrPeerUnreachable {
		t.Fatalf("a -> b (across split): err = %v, want ErrPeerUnreachable", err)
	}
}
