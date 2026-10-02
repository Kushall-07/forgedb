// Package dbnode is Phase 8's composition root: it wires one ForgeDB
// node's Raft consensus (internal/raft), state machine (internal/statemachine),
// and persistent KV storage (internal/storage) into a single replicated
// write path, without changing what any of those three packages already
// do. See docs/raft/phase8-raft-storage-integration.md for the full design.
//
// The end-to-end path a committed write follows is:
//
//	Client command
//	      |
//	      v
//	     Raft            (internal/raft: ordering, replication, commit)
//	      |
//	 majority commit
//	      |
//	      v
//	   Applier           (internal/statemachine: apply-in-order boundary)
//	      |
//	      v
//	 KVStateMachine       (internal/statemachine: dedup + Put/Delete calls)
//	      |
//	      v
//	  storage.Store       (internal/storage: WAL -> MemTable)
//
// Node owns no consensus or storage logic of its own -- every method here
// either constructs one of those three packages' existing types or
// delegates directly to them. This package exists purely so a caller (a
// test, or eventually a real server) has one object to open, propose
// through, and close, instead of wiring the three packages together by
// hand every time.
package dbnode

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// raftStateFileName is the file name FilePersister uses within a Node's
// RaftDir. See Config.RaftDir.
const raftStateFileName = "raft-state"

// ErrNotLeader is returned by Propose when this Node is not currently the
// Raft leader -- a direct re-export of raft.ErrNotLeader, so callers of
// this package never need to import internal/raft themselves just to
// check the error. Phase 8, like Phase 5, does not implement client
// redirection to the real leader.
var ErrNotLeader = raft.ErrNotLeader

// ErrReadUnavailable is returned by ConsistentGet when this Node cannot
// currently establish the quorum confirmation a linearizable read
// requires -- a direct re-export of raft.ErrReadBarrierUnavailable, for
// the same reason ErrNotLeader re-exports raft.ErrNotLeader above. It
// covers both an isolated leader (on the minority side of a partition)
// and a leader that loses leadership while the confirmation round is in
// flight -- see (*raft.Node).ReadIndex and
// docs/raft/phase8.5-read-consistency.md.
//
// This is always distinguishable from a key simply not being present:
// ConsistentGet returns storage.ErrKeyNotFound, never ErrReadUnavailable
// or ErrNotLeader, once a read barrier has actually been established --
// see ConsistentGet's doc comment.
var ErrReadUnavailable = raft.ErrReadBarrierUnavailable

// registrar is implemented by transports that support pre-registering a
// handler under a node ID -- in practice, *raft.InMemoryTransport. Open
// uses it, when available, to register this Node's raft.Node with the
// transport automatically, so a caller configuring a cluster of Nodes
// never needs a separate manual Register call per node. A future real
// network transport that has no such registration step simply does not
// implement this interface, and Open silently skips it.
type registrar interface {
	Register(id string, h raft.RPCHandler)
}

// Config configures a new Node. Every Node in a simulated cluster must be
// given distinct RaftDir and KVDir values (see docs/raft/phase8-raft-storage-integration.md
// section on per-node storage directories) -- Open does not itself detect
// or prevent two Nodes sharing a directory.
type Config struct {
	// ID and Peers configure the underlying raft.Node exactly as
	// raft.Options.ID/Peers do: ID must be unique within the cluster and
	// Peers must list every other node's ID, excluding ID itself.
	ID    string
	Peers []string

	// Transport delivers this node's outgoing RPCs, exactly as
	// raft.Options.Transport. Phase 8 continues to use only the existing
	// in-memory transport (see the package doc); Required.
	Transport raft.Transport

	// RaftDir is the directory this Node's Raft persistence (currentTerm,
	// votedFor, and the log -- see raft.FilePersister) is stored in.
	// Required.
	RaftDir string

	// KVDir is the data directory this Node's KV storage (WAL and
	// MemTable -- see storage.NewMemStore) is stored in. Required, and
	// must never be the same directory as RaftDir or as another Node's
	// KVDir/RaftDir: see the package doc.
	KVDir string

	// ElectionTickMin, ElectionTickMax, HeartbeatTick, and Rand are passed
	// straight through to raft.Options; see there for defaults and
	// meaning. Tests that need fast, deterministic elections typically set
	// ElectionTickMin == ElectionTickMax to a small value.
	ElectionTickMin int
	ElectionTickMax int
	HeartbeatTick   int
	Rand            *rand.Rand

	// WrapStore, if non-nil, wraps the freshly opened storage.Store before
	// it is handed to the state machine. Production code has no reason to
	// set this; it exists solely as a test seam for injecting storage
	// faults at the composition-root level (see node_test.go's
	// TestNode_StorageFault_StopsWithoutAdvancingLastApplied) without
	// giving this package a second, parallel storage implementation of its
	// own.
	WrapStore func(storage.Store) storage.Store
}

// Node is one ForgeDB node: a raft.Node, a storage.Store, a
// statemachine.KVStateMachine, and a statemachine.Applier, opened against
// the same Config and closed together. See the package doc for the write
// path these four pieces form.
type Node struct {
	id      string
	raft    *raft.Node
	store   storage.Store
	sm      *statemachine.KVStateMachine
	applier *statemachine.Applier
}

// Open constructs a Node from cfg: it opens KV storage at cfg.KVDir
// (replaying its WAL, per storage.NewMemStore), opens Raft persistence at
// cfg.RaftDir (restoring currentTerm/votedFor/log, per raft.NewNode), and
// constructs the state machine and applier on top of them. It does not
// start any background goroutine (see Run) and does not itself elect a
// leader or apply anything -- a caller drives that via Tick/Drain (tests)
// or Run (production), exactly as internal/raft and internal/statemachine
// already do on their own.
//
// If Raft persistence reports previously-saved state, that state is
// restored; if it reports corruption, or if KV storage fails to open or
// replay its WAL, Open returns an error and leaves nothing running.
func Open(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("dbnode: Config.ID must not be empty")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("dbnode: Config.Transport must not be nil")
	}
	if cfg.RaftDir == "" {
		return nil, fmt.Errorf("dbnode: Config.RaftDir must not be empty")
	}
	if cfg.KVDir == "" {
		return nil, fmt.Errorf("dbnode: Config.KVDir must not be empty")
	}

	store, err := storage.NewMemStore(cfg.KVDir)
	if err != nil {
		return nil, fmt.Errorf("dbnode: open KV storage: %w", err)
	}
	var backingStore storage.Store = store
	if cfg.WrapStore != nil {
		backingStore = cfg.WrapStore(store)
	}

	persister := raft.NewFilePersister(filepath.Join(cfg.RaftDir, raftStateFileName))
	raftNode, err := raft.NewNode(raft.Options{
		ID:              cfg.ID,
		Peers:           cfg.Peers,
		Transport:       cfg.Transport,
		ElectionTickMin: cfg.ElectionTickMin,
		ElectionTickMax: cfg.ElectionTickMax,
		HeartbeatTick:   cfg.HeartbeatTick,
		Rand:            cfg.Rand,
		Persister:       persister,
	})
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("dbnode: open raft persistence: %w", err)
	}

	if reg, ok := cfg.Transport.(registrar); ok {
		reg.Register(cfg.ID, raftNode)
	}

	sm := statemachine.NewKVStateMachine(backingStore)
	applier := statemachine.NewApplier(raftNode, sm)

	return &Node{id: cfg.ID, raft: raftNode, store: store, sm: sm, applier: applier}, nil
}

// ID returns the node's own ID.
func (n *Node) ID() string { return n.id }

// Raft returns the underlying raft.Node, for tests and callers that need
// direct access to Raft-level state (State, CommitIndex, LastLogIndex,
// and so on) beyond what Node itself exposes.
func (n *Node) Raft() *raft.Node { return n.raft }

// Store returns the underlying storage.Store, so a test can inspect KV
// state directly (see docs/raft/phase8-raft-storage-integration.md's note
// on reads: Phase 8 does not add a read-consistency protocol, so
// inspecting local storage directly is the intended way to verify
// integration-test outcomes).
func (n *Node) Store() storage.Store { return n.store }

// IsLeader reports whether this node currently believes itself to be the
// Raft leader.
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// Propose encodes cmd and appends it to the leader's Raft log, exactly as
// (*raft.Node).Propose, returning ErrNotLeader if this node is not
// currently the leader. Propose does not wait for cmd to commit, let alone
// apply -- see Tick/Drain/ApplyAvailable (tests) or Run (production) for
// how committed entries actually reach storage.
func (n *Node) Propose(cmd statemachine.Command) (index uint64, term uint64, err error) {
	encoded, err := cmd.Encode()
	if err != nil {
		return 0, 0, fmt.Errorf("dbnode: encode command: %w", err)
	}
	return n.raft.Propose(encoded)
}

// Tick advances this node's Raft node by one logical tick -- see
// (*raft.Node).Tick. It exists so deterministic tests can drive a cluster
// of Nodes without reaching into Raft() for every call.
func (n *Node) Tick() { n.raft.Tick() }

// Drain blocks until every RPC this node's Raft node has sent (and every
// further send that triggered) has completed -- see (*raft.Node).Drain.
func (n *Node) Drain() { n.raft.Drain() }

// ApplyAvailable applies every currently committed entry this node has not
// yet applied to its state machine and storage, in order -- see
// (*Applier).ApplyAvailable. Deterministic tests call this directly (after
// Tick/Drain has settled a round of Raft activity) instead of relying on
// Run's background goroutine and a polling loop.
func (n *Node) ApplyAvailable() (applied int, err error) { return n.applier.ApplyAvailable() }

// ConsistentGet performs a linearizable read of key. See
// docs/raft/phase8.5-read-consistency.md for the full design; in outline,
// it follows three steps, in order:
//
//  1. Establish a read barrier through Raft ((*raft.Node).ReadIndex):
//     this node must currently be leader, and a majority of the cluster
//     must freshly confirm that leadership is still current right now,
//     not merely at some earlier instant. This returns ErrNotLeader (not
//     leader) or ErrReadUnavailable (leader, but quorum could not be
//     confirmed -- e.g. a minority partition, or a leadership change
//     during the confirmation round) without reading anything.
//  2. Wait for this node's own Applier to apply every entry through that
//     barrier (waitForApplied): committed is not the same as applied
//     (see internal/raft's commitIndex/lastApplied distinction), and
//     storage must never be read ahead of the state machine's own
//     progress through the log.
//  3. Only then read key from local storage.
//
// ctx governs step 2's wait and must not be nil; context.Background()
// waits indefinitely (appropriate for a test driving ApplyAvailable
// manually, since the wait then never actually blocks -- see the package
// tests), while a production caller would typically pass a
// request-scoped deadline instead. If ctx is cancelled or times out
// before the wait completes, ConsistentGet returns a wrapped ctx.Err()
// without ever reading storage.
//
// A successful barrier still distinguishes a missing key
// (storage.ErrKeyNotFound, returned exactly as Store.Get already defines
// it) from every failure above -- see ErrReadUnavailable's doc comment.
// ConsistentGet never serves a read from a follower's local storage; it
// is leader-only by construction, via step 1.
func (n *Node) ConsistentGet(ctx context.Context, key []byte) ([]byte, error) {
	readIndex, err := n.raft.ReadIndex()
	if err != nil {
		return nil, err
	}
	if err := n.waitForApplied(ctx, readIndex); err != nil {
		return nil, fmt.Errorf("dbnode: wait for state machine to apply through read index %d: %w", readIndex, err)
	}
	return n.store.Get(key)
}

// waitForApplied blocks until n.raft.LastApplied() >= target, or until ctx
// is done. It is event-driven rather than a busy-loop: it waits on
// n.raft.AppliedCh() (notified by every (*raft.Node).MarkApplied call) and
// re-checks LastApplied each time it wakes, exactly the same
// wait-then-recheck pattern CommitCh already establishes for commit
// notifications -- a coalesced or missed notification is never a problem
// because LastApplied, not the channel, is the authoritative check.
func (n *Node) waitForApplied(ctx context.Context, target uint64) error {
	for {
		if n.raft.LastApplied() >= target {
			return nil
		}
		select {
		case <-n.raft.AppliedCh():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Run starts this node's Raft node and Applier as background goroutines --
// see (*raft.Node).Run and (*Applier).Run -- for production use. Tests
// generally call Tick/Drain/ApplyAvailable directly instead.
func (n *Node) Run(tickInterval time.Duration) {
	n.raft.Run(tickInterval)
	n.applier.Run()
}

// Close shuts this node down cleanly:
//
//  1. stop the Raft node's background ticking (if Run started it) and
//     drain its in-flight RPC goroutines, so no further entry can commit
//     and no further outbound RPC is in flight;
//  2. stop the Applier (if Run started it) and, per (*Applier).Stop's
//     contract, block until any in-flight ApplyAvailable call -- and
//     therefore any in-flight storage write -- has actually finished;
//  3. only then close KV storage.
//
// This ordering is what prevents a shutdown race between the applier
// still writing to storage and storage being closed out from under it
// (see docs/raft/phase8-raft-storage-integration.md's lifecycle section).
// Close is safe to call even if Run was never called. It does not delete
// or otherwise touch this node's on-disk directories -- a later Open with
// the same Config reopens exactly what Close left behind.
func (n *Node) Close() error {
	n.raft.Stop()
	n.raft.Drain()
	n.applier.Stop()
	return n.store.Close()
}
