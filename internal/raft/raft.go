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
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Kushall-07/forgedb/internal/metrics"
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

	// Persister makes currentTerm, votedFor, and the log durable across a
	// restart -- see Persister and docs/raft/phase6-raft-persistence.md.
	// It defaults to a no-op in-memory-only persister when left nil,
	// which reproduces Phase 5's original behavior exactly: a Node that
	// forgets everything if the process restarts.
	Persister Persister
}

// Node is a single participant in Raft consensus: it tracks the state
// described in the Raft paper (current term, vote, log, commit index,
// role, and, while leader, per-follower replication progress), drives
// elections and heartbeats via Tick, handles incoming RPCs via
// HandleRequestVote / HandleAppendEntries, and accepts new commands from
// a client via Propose.
//
// currentTerm, votedFor, and the log are durably persisted via
// Options.Persister every time they change (see persistLocked and its
// call sites), so a Node constructed with a real Persister (FilePersister
// or a MemoryPersister shared across two NewNode calls) survives a
// restart without forgetting them -- see
// docs/raft/phase6-raft-persistence.md for exactly what is and isn't
// persisted and why. commitIndex, lastApplied, role, timers, and
// leader-only replication state remain volatile by design and are always
// reset by NewNode; a restarted Node always starts as a Follower.
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

	// persister durably saves currentTerm, votedFor, and log every time
	// one of them changes (see persistLocked). It is never nil -- NewNode
	// defaults it to discardPersister{} when Options.Persister is left
	// unset.
	persister Persister

	// commitCh receives a non-blocking notification every time
	// commitIndex advances, so a caller (or, eventually, a state
	// machine) can wait for new committed entries instead of polling.
	// It is never required reading: CommitIndex/CommittedEntries are
	// always authoritative even if a notification was dropped because
	// nothing was listening.
	commitCh chan struct{}

	// appliedCh mirrors commitCh for lastApplied: it receives a
	// non-blocking notification every time MarkApplied advances
	// lastApplied, so a caller establishing a read barrier (see
	// ReadIndex in read.go and docs/raft/phase8.5-read-consistency.md)
	// can wait, event-driven, for the state machine to catch up to a
	// required index instead of polling. Like commitCh, it is never
	// required reading: LastApplied is always authoritative.
	appliedCh chan struct{}

	// inflight tracks this node's currently outstanding outbound RPC
	// goroutines (including any further sends they themselves trigger,
	// such as a candidate that wins an election immediately
	// broadcasting AppendEntries -- see trackRPC). Drain waits on it.
	inflight sync.WaitGroup

	stopCh   chan struct{}
	stopOnce sync.Once

	// snapshotData is the opaque state-machine payload of this node's
	// most recent snapshot (see Snapshot, persist.go), kept in memory so
	// a leader can hand it straight to a lagging peer via InstallSnapshot
	// without re-reading the persister on every send. It is nil exactly
	// when n.log.entries[0].Index == 0 (no snapshot has ever been created
	// or installed); the two are always kept in sync (see CreateSnapshot
	// and HandleInstallSnapshot in snapshot.go).
	snapshotData []byte

	// pendingSnapshot is set whenever this node has a snapshot (just
	// installed via InstallSnapshot, or loaded from the persister at
	// startup) whose data the caller's state machine has not yet
	// confirmed restoring -- see PendingSnapshot and
	// ConfirmSnapshotRestored in snapshot.go, and
	// docs/raft/phase9-snapshots.md. It is nil once the state machine has
	// caught up. This is never set by CreateSnapshot itself: that path's
	// state machine is the snapshot's own source, already in exactly that
	// state, with nothing to restore.
	pendingSnapshot *Snapshot
}

// NewNode constructs a Node from opts. If opts.Persister has previously
// saved state (from an earlier Node instance -- simulating an earlier
// process that has since crashed or been stopped), that currentTerm,
// votedFor, and log are restored; otherwise the node starts fresh, with
// an empty log and term 0. Either way, every volatile field (commitIndex,
// lastApplied, role, timers, and leader-only replication state) starts at
// its zero value and the node always starts as a Follower -- a restart
// never resumes an old leadership, in-flight replication, or claim about
// what was committed (see docs/raft/phase6-raft-persistence.md).
//
// NewNode returns an error if opts.Persister reports that a previously
// saved state exists but is corrupt (see ErrCorrupt): this is
// deliberately not the same as "no state", and Phase 6 treats it as a
// startup failure rather than silently discarding the corrupt state and
// proceeding as a fresh node, which could otherwise let this node forget
// a vote or log entries it had already durably promised to remember.
//
// A successfully constructed Node is not yet registered with its
// transport or running any background ticking -- see Options.Transport,
// Register, and Run.
func NewNode(opts Options) (*Node, error) {
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
		rnd = rand.New(rand.NewSource(randomSeed()))
	}
	persister := opts.Persister
	if persister == nil {
		persister = discardPersister{}
	}

	n := &Node{
		id:              opts.ID,
		peers:           append([]string(nil), opts.Peers...),
		role:            Follower,
		transport:       opts.Transport,
		electionTickMin: electionMin,
		electionTickMax: electionMax,
		heartbeatTick:   heartbeatTick,
		rnd:             rnd,
		persister:       persister,
		commitCh:        make(chan struct{}, 1),
		appliedCh:       make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}

	state, stateErr := persister.LoadState()
	hasState := stateErr == nil
	if !hasState && !errors.Is(stateErr, ErrNoState) {
		return nil, fmt.Errorf("raft: load persisted state: %w", stateErr)
	}
	if hasState {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
	}

	snap, snapErr := persister.LoadSnapshot()
	hasSnapshot := snapErr == nil
	if !hasSnapshot && !errors.Is(snapErr, ErrNoSnapshot) {
		return nil, fmt.Errorf("raft: load persisted snapshot: %w", snapErr)
	}

	logEntries := []LogEntry(nil)
	if hasState {
		logEntries = state.Log
	}
	var floorIndex, floorTerm uint64
	if hasSnapshot {
		floorIndex, floorTerm = snap.LastIncludedIndex, snap.LastIncludedTerm

		// A crash between SaveSnapshot succeeding and the following,
		// smaller SaveState succeeding can leave the persisted log still
		// containing entries at or below the new snapshot boundary (see
		// docs/raft/phase9-snapshots.md's crash-window analysis) --
		// harmless, since the snapshot already covers them, as long as
		// they are dropped here rather than re-applied.
		var suffix []LogEntry
		for _, e := range logEntries {
			if e.Index > floorIndex {
				suffix = append(suffix, e)
			}
		}
		if len(suffix) > 0 && suffix[0].Index != floorIndex+1 {
			return nil, fmt.Errorf("raft: persisted log has a gap after snapshot boundary %d (first retained entry is %d): %w", floorIndex, suffix[0].Index, ErrCorrupt)
		}
		logEntries = suffix

		n.pendingSnapshot = &Snapshot{LastIncludedIndex: snap.LastIncludedIndex, LastIncludedTerm: snap.LastIncludedTerm, Data: snap.Data}
		n.snapshotData = snap.Data
	}
	n.log = newLogFromEntries(floorIndex, floorTerm, logEntries)

	n.resetElectionTimerLocked()
	return n, nil
}

// persistLocked durably saves the node's current currentTerm, votedFor,
// and log via n.persister. It must be called with n.mu held, and only
// after those three fields already reflect the state to be saved.
//
// Every call site (becomeFollowerLocked, startElectionLocked,
// HandleRequestVote, HandleAppendEntries, Propose) snapshots the
// affected field(s) before mutating them and restores that snapshot if
// persistLocked returns an error, so a failed persist never leaves this
// node's in-memory state ahead of what is actually durable on disk --
// there is no window in which this node could act on (grant a
// conflicting vote, report an entry as replicated, become a candidate
// in) a term, vote, or log entry that a crash immediately afterward would
// cause it to forget.
func (n *Node) persistLocked() error {
	logCopy := append([]LogEntry(nil), n.log.entries[1:]...) // exclude the sentinel/snapshot boundary
	return n.persister.SaveState(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         logCopy,
	})
}

// newLogFromEntries reconstructs a Log from a persisted, no-sentinel
// entry slice, prepending a sentinel at (baseIndex, baseTerm) -- (0, 0)
// for a node that has never created or installed a snapshot, or the
// snapshot's own (LastIncludedIndex, LastIncludedTerm) otherwise (see
// Log's doc comment and docs/raft/phase9-snapshots.md).
func newLogFromEntries(baseIndex, baseTerm uint64, entries []LogEntry) *Log {
	l := &Log{entries: make([]LogEntry, 0, len(entries)+1)}
	l.entries = append(l.entries, LogEntry{Index: baseIndex, Term: baseTerm})
	l.entries = append(l.entries, entries...)
	return l
}

// randomSeed returns a seed for Options.Rand's default source, drawn from
// the OS's cryptographic entropy pool rather than time.Now().UnixNano().
//
// This matters for a reason that only shows up once Raft runs as real,
// independent OS processes (Phase 14): on a coarse-resolution system
// clock -- observed in practice on Windows, whose clock tick is
// typically ~15ms -- two Node values default-constructed at nearly the
// same wall-clock instant (either genuinely independent processes
// started together, as a Docker Compose cluster does, or two NewNode
// calls made in quick succession within the same process, as tests that
// leave Options.Rand unset do) can read the identical UnixNano() value
// and therefore seed byte-for-byte identical pseudo-random sequences.
// Every subsequent "randomized" election timeout (resetElectionTimerLocked)
// then comes out identical on both nodes, in lockstep, forever -- a
// permanent, deterministic split vote that no number of retries resolves,
// since nothing ever perturbs it. A cryptographically-seeded source makes
// two concurrent calls draw independent seeds regardless of clock
// resolution, which is all that is needed here: Options.Rand's own
// documented purpose (randomizing election timeouts so split votes are
// rare) is otherwise completely unaffected, and a caller that supplies
// its own Options.Rand (every existing deterministic test) is never
// affected by this at all.
func randomSeed() int64 {
	var buf [8]byte
	if _, err := cryptorand.Read(buf[:]); err == nil {
		return int64(binary.LittleEndian.Uint64(buf[:]))
	}
	// cryptorand.Read failing at all is practically unreachable on every
	// supported platform; falling back to the previous time-based seed
	// is still strictly better than leaving rnd nil.
	return time.Now().UnixNano()
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
//
// The new term (and the reset vote that goes with it -- a new term means
// this node has not voted in it yet) is persisted before any of the rest
// of the step-down happens. If persistence fails, the term/vote change is
// rolled back and an error is returned: the node behaves as though it
// never saw evidence of the higher term at all, rather than risk acting
// on (or telling an RPC caller about) a term it cannot durably remember.
// The caller -- another node -- will simply see this RPC rejected or
// retried, exactly as if the message had been dropped.
func (n *Node) becomeFollowerLocked(term uint64) error {
	oldTerm, oldVotedFor := n.currentTerm, n.votedFor
	wasCandidate := n.role == Candidate
	n.currentTerm = term
	n.votedFor = ""
	if err := n.persistLocked(); err != nil {
		n.currentTerm, n.votedFor = oldTerm, oldVotedFor
		metrics.RecordError("raft")
		return err
	}
	n.role = Follower
	n.leaderID = ""
	n.votesReceived = nil
	n.resetElectionTimerLocked()

	metrics.RaftHigherTermStepsDownTotal.Inc()
	if wasCandidate {
		metrics.RaftElectionsLostTotal.Inc()
	}
	logEvent(n.id).Info(eventStepDown, "term", term, "reason", "higher_term")
	return nil
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
