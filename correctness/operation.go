// Package correctness is ForgeDB's Phase 11 correctness-testing layer: it
// asks a different question than Phase 10's chaos harness (chaos/) did.
// Phase 10 asks "does the system survive failures?" (safety invariants,
// convergence). Phase 11 asks "are the externally observed operations
// correct?" -- specifically, does the recorded history of client-visible
// PUT/DELETE/ConsistentGet calls against ForgeDB's public KV API satisfy
// linearizability?
//
// This package builds an operation-history model (this file), a reference
// sequential KV specification (model.go), a bounded backtracking
// linearizability checker (checker.go), violation reporting (result.go),
// bounded history minimization (reducer.go), history
// serialization/replay (replay.go), and a deterministic client harness
// built on top of the real chaos.Cluster (harness.go) -- never a second,
// parallel cluster implementation; see docs/correctness/phase11-linearizability.md
// for the full design.
//
// Nothing in this package changes internal/raft, internal/statemachine,
// internal/storage, internal/dbnode, or chaos: it only observes their
// existing public behavior (chaos.Cluster.Propose, .ConsistentGet, and the
// fault-injection methods Phase 10 already built) and checks it against an
// independent specification.
package correctness

import "fmt"

// OpKind identifies which public KV operation an Operation represents.
// Phase 11's correctness target is ForgeDB's linearizable-read API
// specifically: Get here always means (*dbnode.Node).ConsistentGet, never
// a direct local storage.Store.Get -- see docs/raft/phase8.5-read-consistency.md
// and this package's doc on why a non-linearizable local read must never
// be fed into this checker (§31 of the Phase 11 brief).
type OpKind int

const (
	OpGet OpKind = iota
	OpPut
	OpDelete
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	default:
		return fmt.Sprintf("OpKind(%d)", int(k))
	}
}

// Outcome classifies how an Operation's invocation was observed to
// resolve. This three-way split is the §8/§9/§39 distinction the Phase 11
// brief insists on, and the checker (checker.go) treats each case
// completely differently:
//
//   - OutcomeOK: the operation definitely, successfully took effect (a
//     committed-and-applied Put/Delete) or definitely observed a
//     consistent value (a ConsistentGet that returned a value or a
//     definitive "not found"). Only OutcomeOK operations are fed into the
//     sequential specification search -- see Check.
//   - OutcomeFailed: the operation definitely did NOT take effect, and the
//     caller received a clear, resolved answer saying so (ErrNotLeader,
//     ErrReadUnavailable, a request-id conflict, and so on). A failed
//     operation imposes no state-transition requirement at all; it is
//     excluded from the linearization entirely (§9/§11).
//   - OutcomeIncomplete: the operation was invoked but no resolved answer
//     was ever observed (e.g. the node crashed, or a client-visible retry
//     budget was exhausted, before a definite success or failure could be
//     confirmed). Like OutcomeFailed, an incomplete operation imposes no
//     state-transition requirement and is excluded from the linearization
//     -- but unlike OutcomeFailed, this is not a claim that the operation
//     didn't happen; it genuinely might have. See §39/§40.
type Outcome int

const (
	OutcomeIncomplete Outcome = iota
	OutcomeFailed
	OutcomeOK
)

func (o Outcome) String() string {
	switch o {
	case OutcomeIncomplete:
		return "Incomplete"
	case OutcomeFailed:
		return "Failed"
	case OutcomeOK:
		return "OK"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// Operation is one recorded client-visible call against ForgeDB's public
// KV API: a PUT, a DELETE, or a ConsistentGet. It is the unit a History is
// built from, and the unit Check reasons about -- never raw Raft log
// entries, never internal apply calls.
//
// ID must be unique within a History (see History.Validate) -- it is an
// arbitrary caller-assigned identifier, not an ordering signal; Invoke and
// Complete are what carry ordering (see their doc comments below).
type Operation struct {
	ID        int
	ClientID  string
	RequestID uint64
	Kind      OpKind
	Node      string // the ForgeDB node this call was issued against
	Key       string
	Value     string // Put's argument; meaningless for Get/Delete

	// Invoke is this operation's position in a single global, strictly
	// monotonic logical sequence shared by every operation in a History
	// (see Clock) -- never a wall-clock timestamp (§7 of the Phase 11
	// brief: tests must stay deterministic). It marks the point at which
	// the client's call was issued and is always meaningful, regardless of
	// Outcome.
	Invoke int

	// Complete is this operation's position in the same global logical
	// sequence, marking the point at which a resolved answer (success or
	// failure) was observed. It is only meaningful when Outcome !=
	// OutcomeIncomplete, and must then satisfy Invoke < Complete (an
	// operation cannot complete before, or at the same logical instant as,
	// it was invoked) -- see History.Validate.
	Complete int

	Outcome Outcome

	// Found and Result are meaningful only for Kind == OpGet and Outcome
	// == OutcomeOK: Found reports whether the key existed at the moment
	// this read was observed to resolve, and Result carries its value
	// when Found is true. They mirror storage.Store.Get's own
	// ErrKeyNotFound-vs-value distinction (see model.go) -- a "not found"
	// read is still a fully successful, fully resolved operation, not a
	// failure (§8/§31).
	Found  bool
	Result string

	// Err is meaningful only when Outcome == OutcomeFailed: the resolved
	// error the caller observed (e.g. dbnode.ErrNotLeader,
	// dbnode.ErrReadUnavailable). It is never set for OutcomeOK or
	// OutcomeIncomplete.
	Err error
}

// successfulWrite reports whether op is a completed, successful Put or
// Delete -- the two kinds that mutate the reference model's state (see
// model.go).
func (op Operation) successfulWrite() bool {
	return op.Outcome == OutcomeOK && (op.Kind == OpPut || op.Kind == OpDelete)
}

// History is a recorded sequence of Operations -- the unit Check
// evaluates. Order within Ops carries no meaning by itself; all ordering
// information is in each Operation's own Invoke/Complete fields (see
// Operation's doc comment). A History is not required to be sorted.
type History struct {
	Ops []Operation
}

// Validate rejects a malformed History before any linearizability
// reasoning is attempted -- kept entirely separate from Check (§38 of the
// Phase 11 brief: "keep history validation separate from linearizability
// validation"). It checks only structural well-formedness: it does not,
// and cannot, determine whether a structurally valid History is actually
// linearizable -- that is Check's job.
//
// Validate rejects:
//   - a duplicate Operation.ID;
//   - an unknown OpKind or Outcome;
//   - Complete <= Invoke for any completed (non-Incomplete) operation;
//   - a completed operation (OutcomeOK or OutcomeFailed) that doesn't
//     carry the fields its Outcome requires: OutcomeFailed must carry a
//     non-nil Err; OutcomeOK must carry nil Err; a Get with OutcomeOK and
//     Found == false must carry an empty Result (a "not found" read never
//     has a value to echo).
func (h History) Validate() error {
	seen := make(map[int]bool, len(h.Ops))
	for _, op := range h.Ops {
		if seen[op.ID] {
			return fmt.Errorf("correctness: duplicate operation id %d", op.ID)
		}
		seen[op.ID] = true

		switch op.Kind {
		case OpGet, OpPut, OpDelete:
		default:
			return fmt.Errorf("correctness: operation %d: unknown kind %v", op.ID, op.Kind)
		}

		switch op.Outcome {
		case OutcomeIncomplete:
			if op.Err != nil {
				return fmt.Errorf("correctness: operation %d: Incomplete must not carry Err", op.ID)
			}
		case OutcomeFailed:
			if op.Complete <= op.Invoke {
				return fmt.Errorf("correctness: operation %d: Complete (%d) must be greater than Invoke (%d)", op.ID, op.Complete, op.Invoke)
			}
			if op.Err == nil {
				return fmt.Errorf("correctness: operation %d: Failed must carry a non-nil Err", op.ID)
			}
		case OutcomeOK:
			if op.Complete <= op.Invoke {
				return fmt.Errorf("correctness: operation %d: Complete (%d) must be greater than Invoke (%d)", op.ID, op.Complete, op.Invoke)
			}
			if op.Err != nil {
				return fmt.Errorf("correctness: operation %d: OK must not carry Err", op.ID)
			}
			if op.Kind == OpGet && !op.Found && op.Result != "" {
				return fmt.Errorf("correctness: operation %d: Get reported not-found but carries a non-empty Result", op.ID)
			}
			if op.Kind != OpGet && op.Found {
				return fmt.Errorf("correctness: operation %d: Found is only meaningful for Get", op.ID)
			}
		default:
			return fmt.Errorf("correctness: operation %d: unknown outcome %v", op.ID, op.Outcome)
		}
	}
	return nil
}

// Completed returns every operation in h whose Outcome is OutcomeOK --
// the set Check actually attempts to linearize (see checker.go). Order of
// the result is unspecified.
func (h History) Completed() []Operation {
	out := make([]Operation, 0, len(h.Ops))
	for _, op := range h.Ops {
		if op.Outcome == OutcomeOK {
			out = append(out, op)
		}
	}
	return out
}

// Clock is a single global, strictly monotonic logical sequence shared by
// every Operation a test or harness produces, used to assign Invoke/
// Complete values. It deliberately has nothing to do with wall-clock time
// (§7): the only property Check relies on is that two Operations' Invoke/
// Complete values are comparable as plain integers, with a lower value
// always meaning "happened first" in whatever deterministic sequence of
// events produced the History.
type Clock struct{ next int }

// Next advances the clock and returns the new value. The first call
// returns 1 (0 is reserved as "never set"), so a zero-valued Operation
// field is always distinguishable from a real tick.
func (c *Clock) Next() int {
	c.next++
	return c.next
}
