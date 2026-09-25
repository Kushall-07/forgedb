// Package raft implements a standalone Raft consensus module for
// ForgeDB: the mechanism by which a cluster of nodes agrees on a single,
// ordered log of commands, tolerating the failure of a minority of
// nodes.
//
// This package is deliberately self-contained. It knows about terms,
// votes, log entries, leaders, and quorums; it knows nothing about
// MemTable, the WAL, SSTables, the Manifest, compaction, or any other
// part of ForgeDB's storage engine (internal/storage), and nothing about
// HTTP, gRPC, or any client-facing API. A Node's job ends the moment an
// entry is known to be committed -- it exposes committed entries (see
// CommitIndex and CommittedEntries) for a future state machine to
// consume, but it never applies a command to anything itself. See
// docs/raft/phase5-raft-consensus.md for the full design and the
// reasoning behind this boundary.
package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Role is a Raft node's current role in the consensus protocol. A node
// is always in exactly one role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// String returns a human-readable name for r, used in logging and test
// failure messages.
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ErrNotLeader is returned by Propose when called on a node that is not
// currently the leader. Phase 5 does not implement client redirection: a
// caller that receives ErrNotLeader is responsible for finding the
// current leader itself (a future phase's concern).
var ErrNotLeader = errors.New("raft: not leader")

// Default tick counts for election and heartbeat timing, used when an
// Options value leaves them at zero. These are counts of logical Tick
// calls, not real durations -- see Options and Tick.
const (
	DefaultElectionTickMin = 10
	DefaultElectionTickMax = 20
	DefaultHeartbeatTick   = 2
)

// Options configures a new Node.
type Options struct {
	// ID uniquely identifies this node within the cluster.
	ID string

	// Peers lists the IDs of every other node in the cluster -- it must
	// not include ID itself. Cluster membership is fixed for the
	// lifetime of a Node; Phase 5 does not implement reconfiguration.
	Peers []string

	// Transport delivers this node's outgoing RPCs to its peers and is
	// where the node itself must be registered (see
	// InMemoryTransport.Register) so peers can deliver RPCs back to it.
	// Required.
	Transport Transport

	// ElectionTickMin and ElectionTickMax bound the randomized election
	// timeout, in logical ticks (see Tick): each time a node needs a new
	// election timeout (on startup, and whenever it resets one), it
	// picks a value uniformly from [ElectionTickMin, ElectionTickMax].
	// Randomization within this range is what keeps split votes rare in
	// a real cluster. Both default to DefaultElectionTickMin /
	// DefaultElectionTickMax when left zero.
	ElectionTickMin int
	ElectionTickMax int

	// HeartbeatTick is how many ticks a leader waits between heartbeat
	// (empty AppendEntries) rounds. It must be well below
	// ElectionTickMin, or followers will time out between heartbeats.
	// Defaults to DefaultHeartbeatTick when zero.
	HeartbeatTick int

	// Rand supplies randomness for election timeouts. It defaults to a
	// new source seeded from the current time; tests that need
	// reproducible timeouts can supply their own.
	Rand *rand.Rand
}

// Node is a single participant in Raft consensus: it tracks the state
// described in the Raft paper (current term, vote, log, commit index,
// role, and, while leader, per-follower replication progress), drives
// elections and heartbeats via Tick, handles incoming RPCs via
// HandleRequestVote / HandleAppendEntries, and accepts new commands from
// a client via Propose.
//
// Node's state is entirely in memory; Phase 6 will add persistence for
// currentTerm, votedFor, and the log, which must survive a restart for
// Raft's safety guarantees to hold across crashes. Without that, a
// restarted Node in Phase 5 has no memory of a previous run -- it is only
// safe to use a Node for the duration of a single process's simulated
// cluster.
//
// A Node is safe for concurrent use. All RPC handling, election timeouts,
// heartbeats, replication, and Propose calls are synchronized through a
// single internal mutex; outgoing RPCs are always sent from a background
// goroutine so the mutex is never held while waiting on a (potentially
// slow, or in a real transport, blocking) network call. See Drain for how
// tests observe when a burst of such background work has settled.
type Node struct {
	mu sync.Mutex

	id    string
	peers []string

	// Persistent state (conceptually -- Phase 5 keeps it in memory only;
	// see the Node doc comment).
	currentTerm uint64
	votedFor    string
	log         *Log

	// Volatile state.
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    string

	// Candidate-only: which peers have granted a vote in the election
	// for currentTerm. Reset every time a new election starts.
	votesReceived map[string]bool

	// Leader-only replication state.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Election/heartbeat timing, in logical ticks (see Tick).
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	electionTickMin int
	electionTickMax int
	heartbeatTick   int

	transport Transport
	rnd       *rand.Rand

	// commitCh receives a non-blocking notification every time
	// commitIndex advances, so a caller (or, eventually, a state
	// machine) can wait for new committed entries instead of polling.
	// It is never required reading: CommitIndex/CommittedEntries are
	// always authoritative even if a notification was dropped because
	// nothing was listening.
	commitCh chan struct{}

	// inflight tracks this node's currently outstanding outbound RPC
	// goroutines (including any further sends they themselves trigger,
	// such as a candidate that wins an election immediately
	// broadcasting AppendEntries -- see trackRPC). Drain waits on it.
	inflight sync.WaitGroup

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewNode constructs a Node from opts. The node starts as a Follower with
// an empty log and term 0, and is not yet registered with its transport
// or running any background ticking -- see Options.Transport, Register,
// and Run.
func NewNode(opts Options) *Node {
	electionMin := opts.ElectionTickMin
	if electionMin == 0 {
		electionMin = DefaultElectionTickMin
	}
	electionMax := opts.ElectionTickMax
	if electionMax == 0 {
		electionMax = DefaultElectionTickMax
	}
	heartbeatTick := opts.HeartbeatTick
	if heartbeatTick == 0 {
		heartbeatTick = DefaultHeartbeatTick
	}
	rnd := opts.Rand
	if rnd == nil {
		rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	n := &Node{
		id:              opts.ID,
		peers:           append([]string(nil), opts.Peers...),
		log:             NewLog(),
		role:            Follower,
		transport:       opts.Transport,
		electionTickMin: electionMin,
		electionTickMax: electionMax,
		heartbeatTick:   heartbeatTick,
		rnd:             rnd,
		commitCh:        make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
	n.resetElectionTimerLocked()
	return n
}

// ID returns the node's own ID.
func (n *Node) ID() string { return n.id }

// State returns a consistent snapshot of the node's current term, role,
// and known leader ID (empty if the node does not currently know a
// leader -- e.g. mid-election).
func (n *Node) State() (term uint64, role Role, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role, n.leaderID
}

// IsLeader reports whether the node currently believes itself to be the
// leader.
func (n *Node) IsLeader() bool {
	_, role, _ := n.State()
	return role == Leader
}

// majority returns the number of votes (or acknowledging replicas)
// needed for a quorum of the cluster, computed as floor(N/2)+1 where N is
// the total cluster size (len(n.peers)+1, since n.peers excludes the node
// itself). It is never hard-coded for a particular cluster size.
func (n *Node) majority() int {
	total := len(n.peers) + 1
	return total/2 + 1
}

// resetElectionTimerLocked picks a fresh randomized election timeout and
// clears the elapsed-tick counter. Called whenever the node has reason to
// believe the current leader (or, while a candidate, the current
// election) is still alive: on startup, after granting a vote, after
// accepting a valid AppendEntries, and when starting a new election
// itself. n.mu must be held.
func (n *Node) resetElectionTimerLocked() {
	n.electionElapsed = 0
	spread := n.electionTickMax - n.electionTickMin
	if spread <= 0 {
		n.electionTimeout = n.electionTickMin
	} else {
		n.electionTimeout = n.electionTickMin + n.rnd.Intn(spread+1)
	}
}

// becomeFollowerLocked steps the node down to Follower at term. It is
// called whenever the node observes an RPC (request or reply) carrying a
// term higher than its own -- the single mechanism by which term
// monotonicity is enforced and a stale leader or candidate is forced to
// stand down (Raft's "leader authority" and "term monotonicity"
// invariants). n.mu must be held.
func (n *Node) becomeFollowerLocked(term uint64) {
	n.currentTerm = term
	n.votedFor = ""
	n.role = Follower
	n.leaderID = ""
	n.votesReceived = nil
	n.resetElectionTimerLocked()
}

// trackRPC runs fn in a new goroutine, registering it with n.inflight so
// Drain can wait for it (and for anything it transitively triggers, such
// as a candidate that wins an election immediately starting to send
// AppendEntries -- as long as that further send is itself issued via
// trackRPC before fn returns, which every send path in this package
// does, it is registered before n.inflight's count could reach zero).
// This is the only place Node spawns a goroutine to talk to the network,
// keeping every actual Transport call outside of n.mu.
func (n *Node) trackRPC(fn func()) {
	n.inflight.Add(1)
	go func() {
		defer n.inflight.Done()
		fn()
	}()
}

// Drain blocks until every RPC this node has sent -- and every further
// send that processing a reply triggered -- has completed. Production use
// of Node does not need it: replication and elections make progress on
// their own over successive Ticks. It exists for deterministic tests,
// which otherwise have no way to know when a burst of background
// send-and-process-reply goroutines triggered by a Tick or a Propose call
// has finished; see the package tests for the standard tick-then-drain
// pattern.
func (n *Node) Drain() {
	n.inflight.Wait()
}

// Tick advances the node's internal logical clock by one step. It is the
// sole timing primitive Node depends on: a Follower or Candidate that
// accumulates ElectionTick* ticks without seeing a valid heartbeat or
// vote request starts (or restarts) an election, and a Leader that
// accumulates HeartbeatTick ticks sends a new round of AppendEntries
// (heartbeats) to every peer.
//
// Ticks carry no notion of wall-clock duration themselves -- Run drives
// them from a real timer for production use, but tests are expected to
// call Tick directly, which makes election and heartbeat timing fully
// deterministic and independent of real time (see the package doc and
// Drain).
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppendEntriesLocked()
		}
	default: // Follower, Candidate
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.startElectionLocked()
		}
	}
}

// Run starts a background goroutine that calls Tick once every
// tickInterval until Stop is called. It is the production entry point for
// driving a Node's timing off the real clock; it is optional; tests
// generally call Tick directly instead (see Tick).
func (n *Node) Run(tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.Tick()
			case <-n.stopCh:
				return
			}
		}
	}()
}

// Stop halts the background goroutine started by Run, if any. It is safe
// to call more than once, and safe to call even if Run was never called.
// Stop does not wait for in-flight RPCs; use Drain for that.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}
