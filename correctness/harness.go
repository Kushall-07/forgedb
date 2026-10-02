package correctness

import (
	"errors"

	"github.com/Kushall-07/forgedb/chaos"
	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// Harness is Phase 11's bounded, deterministic concurrent-client-history
// runner (§32/§33 of the Phase 11 brief), built directly on top of the
// real Phase 10 chaos.Cluster -- never a second, parallel cluster
// implementation (§47). It records every call it makes through
// (*chaos.Cluster).Propose/ConsistentGet (and nothing else -- no local
// storage.Store.Get, per §31) into a History that Check can then
// evaluate.
//
// chaos.Cluster documents itself as "not safe for concurrent use by
// multiple goroutines" (chaos/cluster.go): every chaos scenario is a
// single sequential narrative. Harness deliberately respects that
// constraint rather than fighting it with goroutines and locks (§33 asks
// for "deliberate and bounded" concurrency, not uncontrolled goroutines).
// Instead, it splits every operation into two explicit, separately
// timestamped steps a test script drives by hand:
//
//  1. Invoke records the operation's start (a new tick on the shared
//     Clock) and returns a PendingOp -- no call against the cluster
//     happens yet.
//  2. Resolve (or ResolveInterrupted) performs the actual, real,
//     synchronous call against the cluster, determines its Outcome, and
//     records the operation's completion (another tick) into the
//     History.
//
// A test script creates genuine overlap deterministically simply by
// calling Invoke for two different operations before calling Resolve on
// either one: e.g. Invoke(A); Invoke(B); Resolve(A); Resolve(B) produces
// A.Invoke < B.Invoke < A.Complete < B.Complete, which is a real overlap
// by Operation's own definition, even though the two underlying cluster
// calls still happen one at a time, in-process, never concurrently. This
// is the harness's one deliberate simplification, called out explicitly
// here rather than left as a silent assumption (§39/§40): the *history*
// this produces is a faithful stand-in for what two genuinely concurrent
// clients would observe (same invoke/complete ordering, same results),
// even though no two goroutines are ever actually racing inside
// chaos.Cluster.
type Harness struct {
	cluster *chaos.Cluster
	clock   Clock
	nextID  int
	ops     []Operation
}

// NewHarness returns a Harness recording operations against c.
func NewHarness(c *chaos.Cluster) *Harness {
	return &Harness{cluster: c}
}

// History returns every operation recorded so far, as a fresh copy.
func (h *Harness) History() History {
	return History{Ops: append([]Operation(nil), h.ops...)}
}

// PendingOp is an operation that has been Invoked but not yet Resolved.
type PendingOp struct {
	id        int
	clientID  string
	requestID uint64
	kind      OpKind
	node      string
	key       string
	value     string
	invoke    int
}

// Invoke records a new operation's invocation against node (the target
// for Propose, for a Put/Delete, or for ConsistentGet, for a Get) and
// returns a handle to resolve later. See the type doc comment for why
// invocation and resolution are split, and ClientID/RequestID's meaning
// in statemachine.Command's own doc comment -- a genuine client retry of
// the same logical request is modeled as a second Invoke/Resolve pair
// sharing the identical (clientID, requestID) (§42/§57).
func (h *Harness) Invoke(clientID string, requestID uint64, kind OpKind, node, key, value string) *PendingOp {
	h.nextID++
	return &PendingOp{
		id:        h.nextID,
		clientID:  clientID,
		requestID: requestID,
		kind:      kind,
		node:      node,
		key:       key,
		value:     value,
		invoke:    h.clock.Next(),
	}
}

// Resolve performs the real call p's Invoke described, determines its
// Outcome, appends the finished Operation to h's History, and returns
// it.
//
// For a Put/Delete, Resolve proposes the command against p.node and then
// waits (via the real chaos.Cluster.WaitForApplied -- never a fabricated
// wait) up to maxRounds Advance rounds for it to actually commit and
// apply. An immediate Propose error (e.g. dbnode.ErrNotLeader) is a
// resolved, definitive OutcomeFailed. Exhausting maxRounds without
// confirmation is OutcomeIncomplete, not OutcomeFailed: the proposal was
// accepted into the log, so it may yet commit, and the harness has no
// way to know (§39/§40) -- this is the ordinary, non-adversarial path's
// only source of Incomplete results; see ResolveInterrupted for the
// deliberate version.
//
// For a Get, Resolve calls the real (*chaos.Cluster).ConsistentGet --
// never a local storage.Store.Get (§31) -- against p.node. A nil error
// is a successful read that found a value; storage.ErrKeyNotFound is
// an equally successful read that definitively did not (§8: "not found"
// is a resolved answer, not a failure); dbnode.ErrNotLeader and
// dbnode.ErrReadUnavailable (and any other error) are OutcomeFailed.
// maxRounds is unused for a Get -- ConsistentGet's own ReadIndex round
// trip is already synchronous and self-contained (see
// docs/raft/phase8.5-read-consistency.md); there is nothing here to
// bound-wait for.
func (h *Harness) Resolve(p *PendingOp, maxRounds int) Operation {
	op := Operation{
		ID:        p.id,
		ClientID:  p.clientID,
		RequestID: p.requestID,
		Kind:      p.kind,
		Node:      p.node,
		Key:       p.key,
		Value:     p.value,
		Invoke:    p.invoke,
	}

	switch p.kind {
	case OpPut, OpDelete:
		h.resolveWrite(p, &op, maxRounds)
	case OpGet:
		h.resolveGet(p, &op)
	}

	tick := h.clock.Next()
	if op.Outcome != OutcomeIncomplete {
		op.Complete = tick
	}
	h.ops = append(h.ops, op)
	return op
}

// ResolveInterrupted is Resolve's deliberate-failure counterpart (§25/§26/
// §39): it proposes p's command exactly as Resolve would, but -- the
// moment the proposal is accepted into the log (no error from Propose
// itself) -- calls interrupt (e.g. a closure that crashes or partitions
// p.node) and immediately records the operation as OutcomeIncomplete,
// without ever waiting for or checking whether it actually committed.
// This is the harness's explicit model of "the client's request was
// sent, and then the client lost contact before any response, confirmed
// or not, could be received" -- distinct from an ordinary Resolve
// timeout only in that the interruption is deliberate and immediate
// rather than a bounded wait exhausting its budget.
//
// If Propose itself fails immediately (e.g. this node was already not
// leader), that is still a resolved, definitive OutcomeFailed -- the
// client did receive an answer before interrupt ever ran -- and
// interrupt is not called at all.
//
// ResolveInterrupted is intended for Put/Delete; calling it for a Get
// records OutcomeIncomplete unconditionally without touching the
// cluster at all, since ConsistentGet has no equivalent "accepted but
// not yet confirmed" intermediate state to interrupt (see Resolve's doc
// comment) -- deliberate, not a silent gap: a test wanting an
// unavailable-read case should use Resolve against a minority-partitioned
// node instead (see docs/correctness/phase11-linearizability.md).
func (h *Harness) ResolveInterrupted(p *PendingOp, interrupt func()) Operation {
	op := Operation{
		ID:        p.id,
		ClientID:  p.clientID,
		RequestID: p.requestID,
		Kind:      p.kind,
		Node:      p.node,
		Key:       p.key,
		Value:     p.value,
		Invoke:    p.invoke,
	}

	switch p.kind {
	case OpPut, OpDelete:
		cmd := writeCommand(p)
		if _, _, err := h.cluster.Propose(p.node, cmd); err != nil {
			op.Outcome = OutcomeFailed
			op.Err = err
		} else {
			interrupt()
			op.Outcome = OutcomeIncomplete
		}
	default:
		op.Outcome = OutcomeIncomplete
	}

	tick := h.clock.Next()
	if op.Outcome != OutcomeIncomplete {
		op.Complete = tick
	}
	h.ops = append(h.ops, op)
	return op
}

func writeCommand(p *PendingOp) statemachine.Command {
	if p.kind == OpPut {
		return statemachine.NewPutCommand(p.clientID, p.requestID, []byte(p.key), []byte(p.value))
	}
	return statemachine.NewDeleteCommand(p.clientID, p.requestID, []byte(p.key))
}

func (h *Harness) resolveWrite(p *PendingOp, op *Operation, maxRounds int) {
	index, _, err := h.cluster.Propose(p.node, writeCommand(p))
	if err != nil {
		op.Outcome = OutcomeFailed
		op.Err = err
		return
	}
	if err := h.cluster.WaitForApplied(p.node, index, maxRounds); err != nil {
		op.Outcome = OutcomeIncomplete
		return
	}
	op.Outcome = OutcomeOK
}

func (h *Harness) resolveGet(p *PendingOp, op *Operation) {
	val, err := h.cluster.ConsistentGet(p.node, []byte(p.key))
	switch {
	case err == nil:
		op.Outcome = OutcomeOK
		op.Found = true
		op.Result = string(val)
	case errors.Is(err, storage.ErrKeyNotFound):
		op.Outcome = OutcomeOK
		op.Found = false
	case errors.Is(err, dbnode.ErrNotLeader), errors.Is(err, dbnode.ErrReadUnavailable):
		// A follower, or a minority-partitioned/stale leader that could
		// not confirm quorum -- the §9/§31 "do not treat this as a
		// successful read returning an invalid value" cases.
		op.Outcome = OutcomeFailed
		op.Err = err
	default:
		// Any other error from ConsistentGet is still a resolved,
		// definitive failure as far as this harness is concerned -- there
		// is no third "unknown" Outcome.
		op.Outcome = OutcomeFailed
		op.Err = err
	}
}

// --- One-shot convenience wrappers for the common non-overlapping case --

// Put invokes and immediately resolves a single Put -- the usual shape
// for a simple sequential test (§18-§22) that does not need an explicit
// overlap window.
func (h *Harness) Put(node, clientID string, requestID uint64, key, value string, maxRounds int) Operation {
	return h.Resolve(h.Invoke(clientID, requestID, OpPut, node, key, value), maxRounds)
}

// Delete invokes and immediately resolves a single Delete.
func (h *Harness) Delete(node, clientID string, requestID uint64, key string, maxRounds int) Operation {
	return h.Resolve(h.Invoke(clientID, requestID, OpDelete, node, key, ""), maxRounds)
}

// Get invokes and immediately resolves a single ConsistentGet.
func (h *Harness) Get(node, clientID string, requestID uint64, key string) Operation {
	return h.Resolve(h.Invoke(clientID, requestID, OpGet, node, key, ""), 0)
}
