// Package chaos is ForgeDB's Phase 10 chaos/failure-testing harness. It
// builds on the real internal/dbnode composition (real raft.Node, real
// raft.FilePersister, real storage.MemStore -- never a parallel fake
// implementation of any of them) and the real internal/raft.InMemoryTransport,
// adding only what is needed to deliberately break a running cluster in
// deterministic, reproducible ways and then check that ForgeDB's safety
// guarantees from Phases 5-9 actually held throughout.
//
// Cluster is the harness's single entry point: it opens N dbnode.Nodes
// wired through a shared InMemoryTransport, and exposes crash/restart,
// partition/heal, deterministic ticking, proposing, consistent reads, and
// bounded wait helpers on top of them, plus an EventLog and an
// InvariantTracker that together make every scenario's full action
// sequence and safety-invariant history available for diagnosis the
// moment something goes wrong. See docs/chaos/phase10-chaos-testing.md
// for the full design and docs/raft/phase5-raft-consensus.md through
// phase9-snapshots.md for the mechanisms being tested, none of which this
// package redesigns.
//
// Deterministic scheduling only: every method here that advances the
// cluster does so via Tick/Drain/ApplyAvailable, never time.Sleep, so a
// scenario's outcome depends only on the sequence of calls a test makes,
// not on real time. See Advance.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// Default election/heartbeat tick counts for a Cluster.
//
// ElectionTickMin/Max are deliberately *not* set equal to each other, the
// way internal/dbnode's own test convention does (see
// internal/dbnode/cluster_test.go's testElectionTick): that convention
// only ever drives elections by calling electLeader on one specific,
// chosen node, so it needs no spread at all. A chaos scenario also needs
// the opposite: after CrashNode kills the current leader, the surviving
// followers must elect a new one *naturally*, by timing out on their own
// -- see WaitForLeader -- and with zero spread, every follower would
// share the identical deterministic timeout and repeat the same split
// vote forever. A real spread, combined with each node getting its own
// deterministic (seeded by its index, like internal/raft/cluster_test.go's
// newTestCluster) *rand.Rand, keeps elections both reproducible and able
// to resolve naturally within a bounded number of rounds.
const (
	DefaultElectionTickMin = 5
	DefaultElectionTickMax = 10
	DefaultHeartbeatTick   = 1
)

// Config configures a new Cluster.
type Config struct {
	// NumNodes is the number of nodes to open, named "node0".."node{N-1}".
	// Required, must be positive.
	NumNodes int

	// BaseDir is the root directory under which each node gets its own
	// "<BaseDir>/<id>/raft" and "<BaseDir>/<id>/kv" subdirectories.
	// Required; a test typically passes t.TempDir(). Using real,
	// per-node-distinct directories (never a shared or in-memory fake) is
	// what makes CrashNode/RestartNode exercise genuine on-disk recovery
	// instead of merely resetting an in-memory struct -- see
	// RestartNode's doc comment.
	BaseDir string

	// ElectionTickMin, ElectionTickMax, and HeartbeatTick configure every
	// node's raft.Options fields of the same name. Default to
	// DefaultElectionTickMin/Max/DefaultHeartbeatTick when zero.
	ElectionTickMin int
	ElectionTickMax int
	HeartbeatTick   int
}

type nodeDirs struct {
	raftDir string
	kvDir   string
}

// Cluster is a chaos harness's live cluster: N dbnode.Nodes wired through
// a shared raft.InMemoryTransport, each backed by its own real,
// persistent RaftDir/KVDir. It is not safe for concurrent use by multiple
// goroutines (chaos scenarios are deliberately single-threaded and
// deterministic -- see the package doc); a Cluster is meant to be driven
// by exactly one scenario at a time.
type Cluster struct {
	cfg       Config
	ids       []string
	transport *raft.InMemoryTransport

	nodeIndex   map[string]int
	nodes       map[string]*dbnode.Node
	dirs        map[string]nodeDirs
	crashed     map[string]bool
	partitioned map[string]bool
	groups      [][]string

	persisters map[string]*faultyPersister
	stores     map[string]*faultyStore

	Log *EventLog
	Inv *InvariantTracker
}

// NewCluster opens a brand new Cluster of cfg.NumNodes nodes, electing no
// leader and applying nothing -- a scenario drives all of that itself via
// Advance/ElectLeader/Propose. scenario names the scenario for EventLog's
// diagnostics (see Dump); seed is the scenario's randomness seed, or 0
// for a purely deterministic scenario that uses none.
func NewCluster(scenario string, seed int64, cfg Config) (*Cluster, error) {
	if cfg.NumNodes <= 0 {
		return nil, fmt.Errorf("chaos: Config.NumNodes must be positive")
	}
	if cfg.BaseDir == "" {
		return nil, fmt.Errorf("chaos: Config.BaseDir must not be empty")
	}
	if cfg.ElectionTickMin == 0 {
		cfg.ElectionTickMin = DefaultElectionTickMin
	}
	if cfg.ElectionTickMax == 0 {
		cfg.ElectionTickMax = DefaultElectionTickMax
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = DefaultHeartbeatTick
	}

	ids := make([]string, cfg.NumNodes)
	nodeIndex := make(map[string]int, cfg.NumNodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
		nodeIndex[ids[i]] = i
	}

	c := &Cluster{
		cfg:         cfg,
		ids:         ids,
		nodeIndex:   nodeIndex,
		transport:   raft.NewInMemoryTransport(),
		nodes:       make(map[string]*dbnode.Node),
		dirs:        make(map[string]nodeDirs),
		crashed:     make(map[string]bool),
		partitioned: make(map[string]bool),
		persisters:  make(map[string]*faultyPersister),
		stores:      make(map[string]*faultyStore),
		Log:         newEventLog(scenario, seed),
		Inv:         newInvariantTracker(),
	}

	for _, id := range ids {
		c.dirs[id] = nodeDirs{
			raftDir: filepath.Join(cfg.BaseDir, id, "raft"),
			kvDir:   filepath.Join(cfg.BaseDir, id, "kv"),
		}
		if err := c.openNode(id); err != nil {
			return nil, err
		}
	}
	c.Observe("StartCluster")
	return c, nil
}

// peersOf returns every ID in c.ids except self, preserving order.
func (c *Cluster) peersOf(self string) []string {
	peers := make([]string, 0, len(c.ids)-1)
	for _, id := range c.ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// openNode constructs and opens a fresh dbnode.Node for id against its
// existing (possibly brand new, possibly already-populated-by-a-prior-
// incarnation) RaftDir/KVDir, wiring in fresh fault-injection wrappers.
// Used by both NewCluster (first open) and RestartNode (reopen after a
// crash).
func (c *Cluster) openNode(id string) error {
	fp := &faultyPersister{}
	fs := &faultyStore{}
	cfg := dbnode.Config{
		ID:              id,
		Peers:           c.peersOf(id),
		Transport:       c.transport,
		RaftDir:         c.dirs[id].raftDir,
		KVDir:           c.dirs[id].kvDir,
		ElectionTickMin: c.cfg.ElectionTickMin,
		ElectionTickMax: c.cfg.ElectionTickMax,
		HeartbeatTick:   c.cfg.HeartbeatTick,
		// Seeded by this node's fixed index (not by the current time),
		// exactly as internal/raft/cluster_test.go's newTestCluster does,
		// so a given scenario's election outcomes are reproducible across
		// runs while still spreading timeouts enough that a natural,
		// untargeted election (see WaitForLeader) does not deadlock in a
		// repeated split vote.
		Rand: rand.New(rand.NewSource(int64(c.nodeIndex[id]) + 1)),
		WrapPersister: func(p raft.Persister) raft.Persister {
			fp.inner = p
			return fp
		},
		WrapStore: func(s storage.Store) storage.Store {
			fs.Store = s
			return fs
		},
	}
	n, err := dbnode.Open(cfg)
	if err != nil {
		return fmt.Errorf("chaos: open node %s: %w", id, err)
	}
	c.nodes[id] = n
	c.persisters[id] = fp
	c.stores[id] = fs
	c.crashed[id] = false
	return nil
}

// IDs returns every node ID in the cluster, "node0".."node{N-1}", in
// order, regardless of whether it is currently crashed.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// LiveIDs returns every node ID that is not currently crashed.
func (c *Cluster) LiveIDs() []string {
	live := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		if !c.crashed[id] {
			live = append(live, id)
		}
	}
	return live
}

// IsCrashed reports whether id is currently crashed (see CrashNode).
func (c *Cluster) IsCrashed(id string) bool { return c.crashed[id] }

// Node returns the live dbnode.Node for id, or nil if id is currently
// crashed or unknown. Tests that need lower-level access (Raft(), Store(),
// SnapshotIndex(), ...) beyond what Cluster itself exposes use this.
func (c *Cluster) Node(id string) *dbnode.Node {
	if c.crashed[id] {
		return nil
	}
	return c.nodes[id]
}

// Transport returns the cluster's shared InMemoryTransport, for a
// scenario that needs single-node Partition/Heal directly rather than
// through Cluster's own Partition/Heal passthroughs.
func (c *Cluster) Transport() *raft.InMemoryTransport { return c.transport }

// Close closes every currently-live node. It does not delete any
// on-disk directory.
func (c *Cluster) Close() {
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil && !c.crashed[id] {
			n.Close()
		}
	}
}

// --- Crash / restart ---------------------------------------------------

// CrashNode simulates node id's process dying: the node stops
// participating in Raft and stops serving any client call, but its
// on-disk RaftDir/KVDir are untouched (see RestartNode) -- exactly
// docs/chaos/phase10-chaos-testing.md's failure model for a node crash.
//
// It is implemented as a single-node Partition (see
// raft.InMemoryTransport.Partition), which cuts id off from every other
// node in both directions -- the observable effect of a crash from the
// rest of the cluster's point of view -- followed by a real Close of the
// dbnode.Node, which stops its background goroutines and flushes/closes
// its storage exactly as a graceful shutdown would. It returns an error
// if id is unknown or already crashed.
func (c *Cluster) CrashNode(id string) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("chaos: unknown node %s", id)
	}
	if c.crashed[id] {
		return fmt.Errorf("chaos: node %s is already crashed", id)
	}

	c.transport.Partition(id)
	c.crashed[id] = true
	c.partitioned[id] = true
	err := n.Close()
	c.Observe("Crash(" + id + ")")
	if err != nil {
		return fmt.Errorf("chaos: close crashed node %s: %w", id, err)
	}
	return nil
}

// RestartNode simulates node id's process restarting: it closes nothing
// further (CrashNode already did) and instead constructs a brand new
// dbnode.Node from exactly the same RaftDir/KVDir id was using before it
// crashed -- so this is real on-disk recovery (currentTerm, votedFor, the
// Raft log, any snapshot, and the KV WAL all actually reload from disk),
// never merely resetting an in-memory struct -- and heals the Partition
// CrashNode established, so the restarted node can rejoin the cluster's
// network immediately. It returns an error if id is unknown or is not
// currently crashed.
//
// The restarted node always comes back as a Follower with CommitIndex and
// LastApplied reset to 0 (Raft's documented volatile-state-always-resets
// behavior -- see docs/raft/phase6-raft-persistence.md); its term,
// persisted log, and any persisted snapshot are exactly what survived.
// RestartNode resets this cluster's InvariantTracker baseline for id's
// commit/applied monotonicity accordingly (see InvariantTracker's doc
// comment) so that expected reset is never mistaken for a real
// regression.
func (c *Cluster) RestartNode(id string) error {
	if !c.crashed[id] {
		return fmt.Errorf("chaos: node %s is not crashed", id)
	}

	c.transport.Heal(id)
	delete(c.partitioned, id)
	c.Inv.resetVolatile(id)
	if err := c.openNode(id); err != nil {
		return err
	}
	c.Observe("Restart(" + id + ")")
	return nil
}

// --- Network partition ---------------------------------------------------

// Partition isolates id from every other node, in both directions,
// without crashing it: id keeps running (ticking, applying, serving
// local reads) but cannot send or receive any Raft RPC until Heal(id).
// See raft.InMemoryTransport.Partition.
func (c *Cluster) Partition(id string) {
	c.transport.Partition(id)
	c.partitioned[id] = true
	c.Observe("Partition(" + id + ")")
}

// Heal reverses a prior Partition(id).
func (c *Cluster) Heal(id string) {
	c.transport.Heal(id)
	delete(c.partitioned, id)
	c.Observe("Heal(" + id + ")")
}

// PartitionGroups splits the cluster into disjoint groups -- e.g.
// PartitionGroups([]string{"node0","node1","node2"}, []string{"node3","node4"})
// for "A B C | D E" -- via raft.InMemoryTransport.PartitionGroups.
func (c *Cluster) PartitionGroups(groups ...[]string) {
	c.transport.PartitionGroups(groups...)
	c.groups = groups
	c.Observe(fmt.Sprintf("PartitionGroups(%v)", groups))
}

// HealPartitions reverses a prior PartitionGroups call.
func (c *Cluster) HealPartitions() {
	c.transport.HealPartitions()
	c.groups = nil
	c.Observe("HealPartitions")
}

// --- Deterministic scheduling -------------------------------------------

// TickAll calls Tick once on every live node. It does not drain or
// observe by itself -- see Advance for the usual combined step.
func (c *Cluster) TickAll() {
	for _, id := range c.LiveIDs() {
		c.nodes[id].Tick()
	}
}

// DrainAll calls Drain on every live node, blocking until every RPC
// goroutine any of them spawned (directly or transitively) during the
// most recent TickAll/Propose/ElectLeader has completed.
func (c *Cluster) DrainAll() {
	for _, id := range c.LiveIDs() {
		c.nodes[id].Drain()
	}
}

// ApplyAllAvailable calls ApplyAvailable on every live node once,
// returning any per-node errors keyed by node ID (e.g. an injected
// storage fault -- see faultyStore). Unlike internal/dbnode's own test
// convention (applyAllAvailable, which fails the test immediately), this
// never stops early: a chaos scenario deliberately injecting a fault on
// one node still wants every other node to make progress in the same
// round.
func (c *Cluster) ApplyAllAvailable() map[string]error {
	errs := make(map[string]error)
	for _, id := range c.LiveIDs() {
		if _, err := c.nodes[id].ApplyAvailable(); err != nil {
			errs[id] = err
		}
	}
	return errs
}

// Advance runs rounds logical steps of the cluster's deterministic
// scheduler: each round ticks every live node, drains, applies every
// currently-available committed entry, and records an Observe step. This
// is the harness's sole timing primitive -- no step here, or anywhere
// else in this package, ever calls time.Sleep (see the package doc).
//
// It mirrors internal/dbnode's own settleCommit test convention (tick,
// drain, tick, drain -- see internal/dbnode/cluster_test.go) but folds in
// ApplyAvailable and invariant observation, and is parameterized so a
// scenario can run as many rounds as a given Wait* helper needs.
func (c *Cluster) Advance(rounds int) {
	for i := 0; i < rounds; i++ {
		c.TickAll()
		c.DrainAll()
		c.ApplyAllAvailable()
		c.Observe("Tick")
	}
}

// Settle is Advance(2): one extra round beyond a single Tick/Drain is
// what lets every node's own CommitIndex (not just the leader's) catch
// up to a just-committed entry -- see settleCommit's doc comment in
// internal/dbnode/cluster_test.go for exactly why one round alone is
// never enough.
func (c *Cluster) Settle() { c.Advance(2) }

// ElectLeader deterministically elects id as leader: it ticks only id
// (never the rest of the cluster, so no other node's election timeout
// can fire first) enough times to guarantee its own randomized election
// timeout has elapsed, drains it, and returns an error if id did not
// become leader. This is internal/dbnode's own electLeader test
// convention (see internal/dbnode/cluster_test.go), exposed as a Cluster
// method so a scenario can control exactly which node leads without
// relying on timing races.
func (c *Cluster) ElectLeader(id string) error {
	n := c.Node(id)
	if n == nil {
		return fmt.Errorf("chaos: node %s is not live", id)
	}
	for i := 0; i < c.cfg.ElectionTickMax+1; i++ {
		n.Tick()
	}
	n.Drain()
	c.Observe("ElectLeader(" + id + ")")
	if !n.IsLeader() {
		return fmt.Errorf("chaos: node %s did not become leader", id)
	}
	return nil
}

// --- Client operations ---------------------------------------------------

// Propose proposes cmd through node id (which must currently believe
// itself leader) and drains it immediately afterward, mirroring
// internal/dbnode's own proposeOrFatal test convention minus the
// t.Fatalf -- a scenario decides for itself how to react to an error
// (e.g. ErrNotLeader is the expected outcome against a stale leader; see
// Scenario E).
func (c *Cluster) Propose(id string, cmd statemachine.Command) (index, term uint64, err error) {
	n := c.Node(id)
	if n == nil {
		return 0, 0, fmt.Errorf("chaos: node %s is not live", id)
	}
	index, term, err = n.Propose(cmd)
	n.Drain()
	c.Observe(fmt.Sprintf("Propose(%s, %s %q)", id, cmd.Op, cmd.Key))
	return index, term, err
}

// ConsistentGet performs a linearizable read of key against node id via
// (*dbnode.Node).ConsistentGet, using context.Background() (a chaos
// scenario drives application deterministically via Advance, so the wait
// inside ConsistentGet never actually blocks on real time).
func (c *Cluster) ConsistentGet(id string, key []byte) ([]byte, error) {
	n := c.Node(id)
	if n == nil {
		return nil, fmt.Errorf("chaos: node %s is not live", id)
	}
	v, err := n.ConsistentGet(context.Background(), key)
	c.Observe(fmt.Sprintf("ConsistentGet(%s, %q)", id, key))
	return v, err
}

// CreateSnapshot captures node id's state machine as a new Raft snapshot
// through index -- see (*dbnode.Node).CreateSnapshot.
func (c *Cluster) CreateSnapshot(id string, index uint64) error {
	n := c.Node(id)
	if n == nil {
		return fmt.Errorf("chaos: node %s is not live", id)
	}
	err := n.CreateSnapshot(index)
	c.Observe(fmt.Sprintf("CreateSnapshot(%s, %d)", id, index))
	return err
}

// --- Fault injection -----------------------------------------------------

// FailNextPersist arranges for node id's next Raft SaveState call (term,
// vote, or log write) to fail, reproducing raft.MemoryPersister's own
// FailNextSave fault injection against the real FilePersister a dbnode.Node
// actually uses. It returns an error if id is unknown.
func (c *Cluster) FailNextPersist(id string) error {
	fp, ok := c.persisters[id]
	if !ok {
		return fmt.Errorf("chaos: unknown node %s", id)
	}
	fp.FailNextSave()
	return nil
}

// FailNextSnapshotPersist arranges for node id's next Raft SaveSnapshot
// call to fail.
func (c *Cluster) FailNextSnapshotPersist(id string) error {
	fp, ok := c.persisters[id]
	if !ok {
		return fmt.Errorf("chaos: unknown node %s", id)
	}
	fp.FailNextSnapshotSave()
	return nil
}

// FailNextPut arranges for node id's next KV storage Put call (a
// committed PUT reaching the apply boundary) to fail.
func (c *Cluster) FailNextPut(id string) error {
	fs, ok := c.stores[id]
	if !ok {
		return fmt.Errorf("chaos: unknown node %s", id)
	}
	fs.FailNextPut()
	return nil
}

// FailNextDelete arranges for node id's next KV storage Delete call to
// fail.
func (c *Cluster) FailNextDelete(id string) error {
	fs, ok := c.stores[id]
	if !ok {
		return fmt.Errorf("chaos: unknown node %s", id)
	}
	fs.FailNextDelete()
	return nil
}

// PutCount returns how many times node id's real underlying KV storage
// has actually executed a Put -- as distinct from how many PUT commands
// have been proposed or applied, which a replayed duplicate request
// (see Propose and docs/raft/phase7-state-machine.md's deduplication)
// must never let this number double-count. See faultyStore.
func (c *Cluster) PutCount(id string) (int, error) {
	fs, ok := c.stores[id]
	if !ok {
		return 0, fmt.Errorf("chaos: unknown node %s", id)
	}
	return fs.PutCount(), nil
}

// --- Logical state and convergence ---------------------------------------

// LogicalState returns every live key/value pair currently stored on node
// id, as a plain map -- the "logical abstraction" docs/chaos/phase10-chaos-testing.md
// calls for comparing nodes by, deliberately ignoring Raft log
// length, snapshot boundary, or WAL layout differences that do not change
// what the node logically represents (see Cluster.AssertConverged).
func (c *Cluster) LogicalState(id string) (map[string]string, error) {
	n := c.Node(id)
	if n == nil {
		return nil, fmt.Errorf("chaos: node %s is not live", id)
	}
	entries, err := n.Store().Snapshot()
	if err != nil {
		return nil, fmt.Errorf("chaos: snapshot node %s storage: %w", id, err)
	}
	state := make(map[string]string, len(entries))
	for _, e := range entries {
		state[string(e.Key)] = string(e.Value)
	}
	return state, nil
}
