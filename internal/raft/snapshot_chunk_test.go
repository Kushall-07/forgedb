package raft

import (
	"hash/crc32"
	"path/filepath"
	"testing"
)

// This file covers Phase 19's chunked InstallSnapshot transfer (see
// docs/deployment/phase19-chunked-snapshot-transfer.md): splitting a
// snapshot's payload across multiple bounded InstallSnapshotArgs, each
// one a separate chunk, instead of requiring the entire payload in one
// RPC. Every test here either drives a real leader->follower transfer
// through the existing Tick/Propose/Drain machinery with a small
// Options.SnapshotChunkSize (so a tiny test snapshot still spans several
// chunks deterministically), or calls HandleInstallSnapshot directly with
// hand-built chunk args to exercise a specific validation rule in
// isolation. installsnapshot_test.go's existing, unmodified tests already
// cover every non-chunked (Chunked: false) behavior this phase leaves
// untouched.

// chunkChecksum is the same CRC-32C computation sendInstallSnapshot uses
// for each chunk's InstallSnapshotArgs.Checksum.
func chunkChecksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// --- Leader-driven multi-chunk transfer, end to end -------------------------

func TestChunkedInstallSnapshot_LeaderSendsMultipleChunks_FollowerReconstructsExactly(t *testing.T) {
	tr := NewInMemoryTransport()
	leader := mustNewNode(t, Options{
		ID: "leader", Peers: []string{"f", "other"}, Transport: tr,
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		SnapshotChunkSize: 4, // small enough that a realistic payload spans many chunks
	})
	tr.Register("leader", leader)
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader", "other"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("f", follower)
	other := mustNewNode(t, Options{ID: "other", Peers: []string{"leader", "f"}, Transport: tr, ElectionTickMin: 5, ElectionTickMax: 5})
	tr.Register("other", other)

	electLeader(t, leader, 6)

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

	// 35 bytes at a 4-byte chunk size is 8 full chunks plus a short final
	// one -- enough to exercise several chunk boundaries, not just one.
	payload := []byte("the quick brown fox jumps over!!!!!")
	if len(payload)%4 == 0 {
		t.Fatalf("test payload is %d bytes, an exact multiple of the chunk size -- want a short final chunk too", len(payload))
	}
	if err := leader.CreateSnapshot(5, payload); err != nil {
		t.Fatalf("CreateSnapshot(5): %v", err)
	}

	tr.Heal("f")
	leader.Tick()
	leader.Drain()

	if got := follower.SnapshotIndex(); got != 5 {
		t.Fatalf("follower SnapshotIndex after chunked catch-up = %d, want 5", got)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok {
		t.Fatalf("follower has no PendingSnapshot after chunked catch-up")
	}
	if string(snap.Data) != string(payload) {
		t.Fatalf("follower reconstructed snapshot data = %q, want %q", snap.Data, payload)
	}

	leader.mu.Lock()
	nextIdx := leader.nextIndex["f"]
	matchIdx := leader.matchIndex["f"]
	leader.mu.Unlock()
	if nextIdx != 6 {
		t.Fatalf("leader nextIndex[f] after chunked install = %d, want 6", nextIdx)
	}
	if matchIdx != 5 {
		t.Fatalf("leader matchIndex[f] after chunked install = %d, want 5", matchIdx)
	}

	// The cluster must continue committing normally afterward.
	if _, _, err := leader.Propose(Command("after-chunked-snapshot")); err != nil {
		t.Fatalf("Propose after chunked catch-up: %v", err)
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != 6 {
		t.Fatalf("CommitIndex after post-chunked-snapshot proposal = %d, want 6", got)
	}
}

func TestChunkedInstallSnapshot_EmptyPayload_SingleFinalChunk(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: true, TotalSize: 0, Data: nil, Checksum: chunkChecksum(nil),
	})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot(empty, chunked): %+v, want Success", reply)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || snap.LastIncludedIndex != 10 || len(snap.Data) != 0 {
		t.Fatalf("PendingSnapshot = %+v, %v, want empty data at index 10", snap, ok)
	}
}

// --- Partial transfer safety -------------------------------------------------

func TestChunkedInstallSnapshot_IncompleteTransfer_InstallsNothing(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	chunk1 := []byte("hello-")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 12, Data: chunk1, Checksum: chunkChecksum(chunk1),
	})
	if !reply.Success {
		t.Fatalf("first (non-final) chunk: %+v, want Success", reply)
	}

	// The transfer never completes. Nothing durable must have changed.
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after incomplete transfer = %d, want unchanged 0", got)
	}
	if _, ok := follower.PendingSnapshot(); ok {
		t.Fatalf("PendingSnapshot set despite an incomplete transfer")
	}
	if got := follower.CommitIndex(); got != 0 {
		t.Fatalf("CommitIndex after incomplete transfer = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_ExistingSnapshotSurvivesFailedIncomingTransfer(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	// Install a first, valid snapshot via a complete chunked transfer.
	good := []byte("good-state")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 5, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: true, TotalSize: uint64(len(good)), Data: good, Checksum: chunkChecksum(good),
	})
	if !reply.Success {
		t.Fatalf("initial install: %+v", reply)
	}

	// Start a newer transfer but corrupt its second chunk.
	part1 := []byte("newer-")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 12, Data: part1, Checksum: chunkChecksum(part1),
	})
	if !reply.Success {
		t.Fatalf("newer transfer's first chunk: %+v, want Success", reply)
	}
	part2 := []byte("bad!!")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: uint64(len(part1)), Final: true, TotalSize: 12, Data: part2,
		Checksum: chunkChecksum([]byte("WRONG")), // corrupted on the wire
	})
	if reply.Success {
		t.Fatalf("corrupted final chunk: %+v, want rejected", reply)
	}

	// The original, fully-installed snapshot must be completely untouched.
	if got := follower.SnapshotIndex(); got != 5 {
		t.Fatalf("SnapshotIndex after failed incoming transfer = %d, want unchanged 5", got)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || string(snap.Data) != "good-state" {
		t.Fatalf("PendingSnapshot after failed incoming transfer = %+v, %v, want unchanged good-state", snap, ok)
	}
}

func TestChunkedInstallSnapshot_RestartDoesNotSeeIncompleteTransfer(t *testing.T) {
	dir := t.TempDir()
	persister := NewFilePersister(filepath.Join(dir, "raft-state"))

	node, err := NewNode(Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	chunk1 := []byte("partial-")
	reply := node.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 20, Data: chunk1, Checksum: chunkChecksum(chunk1),
	})
	if !reply.Success {
		t.Fatalf("first chunk: %+v, want Success", reply)
	}

	// Simulate a crash and restart: a brand new Node against the exact
	// same (real, file-backed) persister. The in-progress transfer lived
	// only in the old Node's memory and was never written anywhere.
	restarted, err := NewNode(Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport(), Persister: persister})
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}

	if got := restarted.SnapshotIndex(); got != 0 {
		t.Fatalf("restarted node SnapshotIndex = %d, want 0 (no complete snapshot was ever persisted)", got)
	}
	if _, ok := restarted.PendingSnapshot(); ok {
		t.Fatalf("restarted node has a PendingSnapshot, want none")
	}

	// A fresh transfer for the same snapshot, starting over at offset 0,
	// must work normally.
	full := []byte("complete-state-after-restart")
	reply = restarted.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: true, TotalSize: uint64(len(full)), Data: full, Checksum: chunkChecksum(full),
	})
	if !reply.Success {
		t.Fatalf("fresh install after restart: %+v, want Success", reply)
	}
	if got := restarted.SnapshotIndex(); got != 10 {
		t.Fatalf("SnapshotIndex after fresh post-restart install = %d, want 10", got)
	}
}

// --- Chunk validation ---------------------------------------------------------

func TestChunkedInstallSnapshot_CorruptedChunk_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	data := []byte("some-bytes")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: true, TotalSize: uint64(len(data)), Data: data,
		Checksum: chunkChecksum(data) ^ 0xFFFFFFFF, // deliberately wrong
	})
	if reply.Success {
		t.Fatalf("corrupted chunk: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after corrupted chunk = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_MissingChunkGap_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	first := []byte("aaaa")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 16, Data: first, Checksum: chunkChecksum(first),
	})
	if !reply.Success {
		t.Fatalf("first chunk: %+v, want Success", reply)
	}

	// Skip straight to offset 12 instead of the expected offset 4 -- a
	// gap in the stream.
	skipped := []byte("dddd")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 12, Final: true, TotalSize: 16, Data: skipped, Checksum: chunkChecksum(skipped),
	})
	if reply.Success {
		t.Fatalf("chunk at unexpected gapped offset: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after gapped chunk = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_DuplicateChunk_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	first := []byte("aaaa")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 12, Data: first, Checksum: chunkChecksum(first),
	})
	if !reply.Success {
		t.Fatalf("first chunk: %+v, want Success", reply)
	}
	second := []byte("bbbb")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 4, Final: false, TotalSize: 12, Data: second, Checksum: chunkChecksum(second),
	})
	if !reply.Success {
		t.Fatalf("second chunk: %+v, want Success", reply)
	}

	// Resend the already-consumed offset-4 chunk again (a duplicate the
	// leader might produce after, e.g., a reply timeout).
	dup := []byte("bbbb")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 4, Final: false, TotalSize: 12, Data: dup, Checksum: chunkChecksum(dup),
	})
	if reply.Success {
		t.Fatalf("duplicate chunk: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after duplicate chunk = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_CompletionWithoutExpectedData_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	short := []byte("short")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		// Final is set, but TotalSize declares more than Data actually
		// carries -- a short transfer.
		Chunked: true, Offset: 0, Final: true, TotalSize: 50, Data: short, Checksum: chunkChecksum(short),
	})
	if reply.Success {
		t.Fatalf("short final chunk: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after short final chunk = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_OverflowBeyondDeclaredTotal_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	oversized := []byte("this-is-way-too-much-data")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 5, Data: oversized, Checksum: chunkChecksum(oversized),
	})
	if reply.Success {
		t.Fatalf("chunk exceeding declared TotalSize: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after oversized chunk = %d, want unchanged 0", got)
	}
}

func TestChunkedInstallSnapshot_UnknownTransferNonZeroOffset_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	// No transfer has ever started (no offset-0 chunk), so a chunk at a
	// non-zero offset cannot be resumed against anything.
	chunk := []byte("mid-stream")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 8, Final: false, TotalSize: 32, Data: chunk, Checksum: chunkChecksum(chunk),
	})
	if reply.Success {
		t.Fatalf("resuming an unknown transfer: %+v, want rejected", reply)
	}
}

// --- Newer snapshot superseding an incomplete older one ---------------------

func TestChunkedInstallSnapshot_NewerSnapshotSupersedesIncompleteOlderTransfer(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	// Start, but never finish, a transfer for an older snapshot boundary.
	older := []byte("stale-")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 12, Data: older, Checksum: chunkChecksum(older),
	})
	if !reply.Success {
		t.Fatalf("older transfer's first chunk: %+v, want Success", reply)
	}

	// The leader has since moved on to a newer snapshot entirely (e.g. it
	// compacted further while this follower was still catching up) and
	// restarts the whole transfer at offset 0 for a higher boundary.
	newer := []byte("fresh-complete-state")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 20, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: true, TotalSize: uint64(len(newer)), Data: newer, Checksum: chunkChecksum(newer),
	})
	if !reply.Success {
		t.Fatalf("newer transfer superseding incomplete older one: %+v, want Success", reply)
	}

	if got := follower.SnapshotIndex(); got != 20 {
		t.Fatalf("SnapshotIndex after newer transfer = %d, want 20", got)
	}
	snap, ok := follower.PendingSnapshot()
	if !ok || string(snap.Data) != "fresh-complete-state" {
		t.Fatalf("PendingSnapshot after newer transfer = %+v, %v, want fresh-complete-state uncontaminated by the older partial buffer", snap, ok)
	}
}

func TestChunkedInstallSnapshot_StaleIdentityMidOlderTransfer_Rejected(t *testing.T) {
	follower := mustNewNode(t, Options{ID: "f", Peers: []string{"leader"}, Transport: NewInMemoryTransport()})

	// Start a transfer for boundary 20.
	first := []byte("aaaa")
	reply := follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 20, LastIncludedTerm: 1,
		Chunked: true, Offset: 0, Final: false, TotalSize: 8, Data: first, Checksum: chunkChecksum(first),
	})
	if !reply.Success {
		t.Fatalf("first chunk: %+v, want Success", reply)
	}

	// A continuation chunk (non-zero offset) for a different, older
	// identity must not be accepted into the in-progress boundary-20
	// buffer.
	foreign := []byte("bbbb")
	reply = follower.HandleInstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 15, LastIncludedTerm: 1,
		Chunked: true, Offset: 4, Final: true, TotalSize: 8, Data: foreign, Checksum: chunkChecksum(foreign),
	})
	if reply.Success {
		t.Fatalf("foreign-identity continuation chunk: %+v, want rejected", reply)
	}
	if got := follower.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after foreign-identity chunk = %d, want unchanged 0", got)
	}
}
