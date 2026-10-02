package correctness

import (
	"fmt"
	"math/rand"

	"github.com/Kushall-07/forgedb/chaos"
)

// This file implements §34/§35 of the Phase 11 brief: a bounded, seeded
// randomized linearizability test, added only after the deterministic
// histories in integration_test.go already pass, never in place of them
// -- exactly the relationship chaos/random.go already has to chaos's own
// deterministic scenarios (see docs/chaos/phase10-chaos-testing.md §13).
//
// RandomCorrectnessOptions.Seed alone determines the entire run
// (operation choices, client/key selection, and which bounded faults
// fire) -- the same seed always reproduces the identical script
// deterministically, exactly like chaos.NewRandomPlan.

// RandomCorrectnessOptions configures RunRandomCorrectness.
type RandomCorrectnessOptions struct {
	// Seed determines every random choice this run makes. Required for
	// reproducibility; there is no other source of randomness anywhere in
	// RunRandomCorrectness.
	Seed int64

	// NumOps bounds how many operations are invoked in total (§35: "10-30
	// operations" is the brief's own suggested range for a checker this
	// simple). Required, must be positive.
	NumOps int

	// MaxPending bounds how many invoked-but-not-yet-resolved operations
	// may exist at once -- the knob that controls how much genuine
	// overlap a run can produce (see Harness's doc comment). 0 means 3.
	MaxPending int

	// MaxApplyRounds bounds how many Advance rounds a Put/Delete's
	// Resolve will wait for its proposal to commit before concluding
	// OutcomeIncomplete (see Harness.Resolve). 0 means 20.
	MaxApplyRounds int

	// Keys is the fixed key set operations are drawn from. A small set
	// (§54/§55: "multiple keys... to ensure the checker doesn't
	// accidentally serialize unrelated operations unnecessarily") makes
	// both genuine key contention (several clients touching the same
	// key) and genuine independence (unrelated keys that must be allowed
	// to commute) likely within a short run. Empty means {"k0","k1","k2"}.
	Keys []string

	// InjectFaults, if true, lets the run occasionally crash+restart or
	// partition+heal a single non-leader node between steps (bounded to
	// at most 2 such events per run) -- connecting this randomized
	// correctness test to Phase 10's failure model (§25/§47), without
	// turning it into a second chaos harness: every fault used here is
	// one chaos.Cluster already implements.
	InjectFaults bool
}

// RunRandomCorrectness runs a bounded, seeded randomized script of
// Put/Delete/Get operations (and, if requested, bounded faults) against
// c through a fresh Harness, and returns the resulting History. It never
// calls Check itself -- the caller decides what to do with the result
// (see random_test.go), printing c.Dump() alongside any failure for full
// reproducibility from the seed alone, exactly as chaos's own randomized
// scenario does.
//
// Unlike chaos.RunRandomScenario, this does not attempt to drive the
// cluster to final convergence afterward (§10 of docs/chaos/phase10-chaos-testing.md's
// notion of convergence has nothing to add here): Phase 11's correctness
// claim is about the recorded operation history itself, via Check, never
// about final KV state equality (§37: "do not make the checker too
// trusting... final state equality is insufficient").
func RunRandomCorrectness(c *chaos.Cluster, opts RandomCorrectnessOptions) (History, error) {
	if opts.NumOps <= 0 {
		return History{}, fmt.Errorf("correctness: RandomCorrectnessOptions.NumOps must be positive")
	}
	maxPending := opts.MaxPending
	if maxPending <= 0 {
		maxPending = 3
	}
	maxApplyRounds := opts.MaxApplyRounds
	if maxApplyRounds <= 0 {
		maxApplyRounds = 20
	}
	keys := opts.Keys
	if len(keys) == 0 {
		keys = []string{"k0", "k1", "k2"}
	}

	rnd := rand.New(rand.NewSource(opts.Seed))
	h := NewHarness(c)

	var pending []*PendingOp
	invoked := 0
	clientSeq := 0
	faultsUsed := 0
	const maxFaults = 2

	// A hard safety cap on total loop iterations, independent of NumOps/
	// MaxPending, so a pathological configuration (e.g. no leader ever
	// elected) cannot hang a test -- it would instead leave some
	// operations permanently pending, which the final drain loop below
	// still resolves (deterministically, as OutcomeIncomplete if nothing
	// else) before returning.
	maxSteps := (opts.NumOps + maxPending) * 20
	for step := 0; step < maxSteps && (invoked < opts.NumOps || len(pending) > 0); step++ {
		c.Advance(1)

		if opts.InjectFaults && faultsUsed < maxFaults && rnd.Intn(10) == 0 {
			if injectRandomFault(c, rnd) {
				faultsUsed++
			}
		}

		canInvoke := invoked < opts.NumOps && len(pending) < maxPending
		canResolve := len(pending) > 0

		switch {
		case canInvoke && (!canResolve || rnd.Intn(2) == 0):
			clientSeq++
			kind := randomKind(rnd)
			key := keys[rnd.Intn(len(keys))]
			node, ok := pickNode(c, rnd, kind)
			if !ok {
				continue // no eligible node this round; try again next step
			}
			var value string
			if kind == OpPut {
				value = fmt.Sprintf("v%d", rnd.Intn(1000))
			}
			clientID := fmt.Sprintf("rand-client-%d", clientSeq)
			p := h.Invoke(clientID, uint64(clientSeq), kind, node, key, value)
			pending = append(pending, p)
			invoked++

		case canResolve:
			idx := rnd.Intn(len(pending))
			p := pending[idx]
			pending = append(pending[:idx], pending[idx+1:]...)
			h.Resolve(p, maxApplyRounds)
		}
	}

	// Drain whatever is still pending (deterministically, in invocation
	// order) so the returned History never silently drops an invoked
	// operation -- every Invoke must be matched by exactly one Resolve.
	for _, p := range pending {
		h.Resolve(p, maxApplyRounds)
	}

	return h.History(), nil
}

func randomKind(rnd *rand.Rand) OpKind {
	switch rnd.Intn(3) {
	case 0:
		return OpPut
	case 1:
		return OpDelete
	default:
		return OpGet
	}
}

// pickNode chooses the target node for a new operation: Put/Delete must
// go to a node that currently believes itself leader (a real client
// would learn this the same way -- by having a prior response tell it,
// or by being redirected; this harness, like chaos.RunRandomScenario,
// simply checks CurrentLeader directly), while a Get is deliberately
// sent to any live node, leader or not, so an unavailable-read failure
// (§9/§31) is a normal, expected outcome this generator can produce, not
// something it avoids.
func pickNode(c *chaos.Cluster, rnd *rand.Rand, kind OpKind) (string, bool) {
	if kind == OpGet {
		live := c.LiveIDs()
		if len(live) == 0 {
			return "", false
		}
		return live[rnd.Intn(len(live))], true
	}
	leader, ok := c.CurrentLeader()
	return leader, ok
}

// injectRandomFault fires exactly one bounded fault (§25) against a
// randomly chosen node, never the only node in a single-node cluster, and
// reports whether it actually did anything (a chosen action that isn't
// currently valid, e.g. restarting a node that isn't crashed, is treated
// as a no-op -- it does not count against the caller's fault budget).
func injectRandomFault(c *chaos.Cluster, rnd *rand.Rand) bool {
	ids := c.IDs()
	if len(ids) < 2 {
		return false
	}
	id := ids[rnd.Intn(len(ids))]
	switch rnd.Intn(2) {
	case 0:
		if !c.IsCrashed(id) {
			if err := c.CrashNode(id); err == nil {
				// Immediately restart it a few rounds later so the
				// cluster can keep making the progress later Invoke/
				// Resolve steps in this same run depend on; a
				// permanently-crashed node would otherwise starve the
				// "Put/Delete needs a leader" check for the rest of the
				// run.
				c.Advance(3)
				c.RestartNode(id)
				return true
			}
		}
	case 1:
		if !c.IsCrashed(id) {
			c.Partition(id)
			c.Advance(3)
			c.Heal(id)
			return true
		}
	}
	return false
}
