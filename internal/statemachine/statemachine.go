// Package statemachine implements ForgeDB's Phase 7 state machine: the
// component that sits between Raft (internal/raft) and ForgeDB's storage
// engine (internal/storage), applying committed Raft log entries to
// storage in order and deduplicating retried client requests. See
// docs/raft/phase7-state-machine.md for the full design.
//
// This package keeps the same boundary Phase 5/6 established: Raft
// decides what is committed and in what order; StateMachine decides what
// applying a committed command means (a storage mutation, a replayed
// duplicate, or a resolved error); Store decides how the resulting
// key/value state is physically persisted. Nothing here makes a consensus
// decision, and nothing in internal/raft or internal/storage knows this
// package exists.
package statemachine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/Kushall-07/forgedb/internal/storage"
)

// StateMachine applies committed Commands to database state. A caller
// (see Applier) must guarantee two things Apply itself does not
// re-verify: that command actually corresponds to a committed Raft log
// entry (never an entry Raft has not yet marked committed), and that
// successive Apply calls happen in strict Raft log order, one at a time,
// never concurrently. Given those two guarantees, Apply is deterministic:
// the same sequence of Commands, applied to the same starting state,
// always produces the same sequence of Results and the same final state,
// on every replica.
type StateMachine interface {
	// Apply executes cmd and returns its deterministic Result.
	//
	// Apply's own error return is reserved for a storage-layer fault this
	// replica could not durably execute (e.g. a disk I/O error) -- an
	// unresolved outcome. When Apply returns a non-nil error, the caller
	// must not treat cmd's log entry as applied (see (*raft.Node).MarkApplied)
	// and should retry later once the fault clears; Result is meaningless
	// in that case.
	//
	// Every other outcome -- a successful Put/Delete, a replayed
	// duplicate, ErrRequestIDConflict, ErrStaleRequest, or a deterministic
	// storage validation failure such as storage.ErrEmptyKey -- is fully
	// resolved: Apply returns a nil error together with a Result whose Err
	// field carries the failure, if any. A resolved outcome still counts
	// as applied even when it reports failure, because every replica
	// computes it identically and there is nothing further to retry.
	Apply(cmd Command) (Result, error)
}

// dedupEntry is the per-client deduplication record KVStateMachine keeps:
// the last (RequestID, command shape) it processed for one ClientID, and
// the Result that request produced. Only the *last* request per client is
// retained -- see docs/raft/phase7-state-machine.md's "Deduplication
// table" section for why this bounded, one-entry-per-client shape is
// intentional (and snapshot-friendly, unlike an ever-growing history of
// every request ever seen).
type dedupEntry struct {
	requestID uint64
	op        Op
	key       []byte
	value     []byte
	result    Result
}

// matches reports whether cmd is the identical logical request that
// produced e -- same Op, Key, and Value -- as opposed to a different
// command that happens to reuse e's RequestID (see ErrRequestIDConflict).
func (e dedupEntry) matches(cmd Command) bool {
	return e.op == cmd.Op && bytes.Equal(e.key, cmd.Key) && bytes.Equal(e.value, cmd.Value)
}

// KVStateMachine is the Phase 7 StateMachine: it applies OpPut/OpDelete
// Commands to an internal/storage.Store, deduplicating by (ClientID,
// RequestID). It implements StateMachine.
//
// KVStateMachine is safe for concurrent use, but see Apply's doc comment
// on the Applier's responsibility to still serialize calls in log order
// -- KVStateMachine's own lock only prevents corruption of its dedup
// table, it does not by itself provide the ordering guarantee Raft
// requires.
type KVStateMachine struct {
	mu    sync.Mutex
	store storage.Store
	dedup map[string]dedupEntry
}

// NewKVStateMachine returns a KVStateMachine that applies commands against
// store. store is used exactly as internal/storage already implements it
// -- KVStateMachine introduces no second key/value implementation and no
// storage-format knowledge of its own.
func NewKVStateMachine(store storage.Store) *KVStateMachine {
	return &KVStateMachine{store: store, dedup: make(map[string]dedupEntry)}
}

var _ StateMachine = (*KVStateMachine)(nil)

// Apply implements StateMachine. See the StateMachine interface doc
// comment for the exact contract, and docs/raft/phase7-state-machine.md
// for the full reasoning behind every branch below.
func (sm *KVStateMachine) Apply(cmd Command) (Result, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if prev, ok := sm.dedup[cmd.ClientID]; ok {
		switch {
		case cmd.RequestID == prev.requestID:
			if !prev.matches(cmd) {
				return Result{Err: ErrRequestIDConflict}, nil
			}
			res := prev.result
			res.Replayed = true
			return res, nil
		case cmd.RequestID < prev.requestID:
			return Result{Err: ErrStaleRequest}, nil
		}
		// cmd.RequestID > prev.requestID: a genuinely new request from an
		// already-known client -- fall through to execute it normally.
	}

	res, err := sm.execute(cmd)
	if err != nil {
		return Result{}, err
	}

	sm.dedup[cmd.ClientID] = dedupEntry{
		requestID: cmd.RequestID,
		op:        cmd.Op,
		key:       append([]byte(nil), cmd.Key...),
		value:     append([]byte(nil), cmd.Value...),
		result:    res,
	}
	return res, nil
}

// execute runs cmd against storage exactly once, with no dedup
// bookkeeping of its own. Its error return follows Apply's contract: nil
// for every deterministic, resolved outcome (including a storage
// validation failure such as storage.ErrEmptyKey, folded into Result.Err),
// non-nil only for a genuine storage-layer fault Apply must not record as
// applied.
func (sm *KVStateMachine) execute(cmd Command) (Result, error) {
	switch cmd.Op {
	case OpPut:
		if err := sm.store.Put(cmd.Key, cmd.Value); err != nil {
			if errors.Is(err, storage.ErrEmptyKey) {
				return Result{Err: err}, nil
			}
			return Result{}, fmt.Errorf("statemachine: put: %w", err)
		}
		return Result{Applied: true, Key: cmd.Key, Value: cmd.Value}, nil

	case OpDelete:
		if err := sm.store.Delete(cmd.Key); err != nil {
			if errors.Is(err, storage.ErrEmptyKey) {
				return Result{Err: err}, nil
			}
			return Result{}, fmt.Errorf("statemachine: delete: %w", err)
		}
		return Result{Applied: true, Key: cmd.Key}, nil

	default:
		return Result{}, fmt.Errorf("statemachine: unknown op %s: %w", cmd.Op, ErrInvalidCommand)
	}
}
