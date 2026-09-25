package raft

import "testing"

// fakeHandler is a minimal RPCHandler for exercising InMemoryTransport in
// isolation, without needing a full Node.
type fakeHandler struct {
	voteReply   RequestVoteReply
	appendReply AppendEntriesReply
}

func (f *fakeHandler) HandleRequestVote(RequestVoteArgs) RequestVoteReply { return f.voteReply }
func (f *fakeHandler) HandleAppendEntries(AppendEntriesArgs) AppendEntriesReply {
	return f.appendReply
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
