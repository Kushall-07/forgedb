package chaos

import (
	"fmt"
	"math/rand"

	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// This file implements docs/chaos/phase10-chaos-testing.md's randomized
// chaos scenario: bounded, seeded, and reproducible, added only on top
// of the deterministic scenarios (scenario_*_test.go), never in place of
// them -- see section 25/37 of the Phase 10 brief this package
// implements. It deliberately does not attempt anything like a general
// linearizability history checker; it only drives the harness's existing
// faults through a bounded number of random steps and checks the exact
// same InvariantTracker + convergence properties every deterministic
// scenario already checks.

// RandomPlan is a bounded, reproducible sequence of actions for
// RunRandomScenario, generated from a single seed. Keeping generation
// (NewRandomPlan) and execution (RunRandomScenario) separate means a
// failure can be reproduced by re-running the exact same *RandomPlan --
// printed in full by the EventLog on failure -- without needing the
// original seed at all, and a test can also log the seed alongside it
// for a human to regenerate the same plan independently.
type RandomPlan struct {
	Seed  int64
	Steps []RandomStep
}

// RandomStep is one action in a RandomPlan. Kind selects which fields are
// meaningful; see NewRandomPlan for the exact action set.
type RandomStep struct {
	Kind  RandomKind
	Node  string // target node for every kind below
	Key   string // Put/Delete/Get
	Value string // Put
}

// RandomKind enumerates every action a randomized scenario may take.
// Deliberately a small, closed set -- section 25 of the Phase 10 brief
// lists exactly these.
type RandomKind int

const (
	RandomPut RandomKind = iota
	RandomDelete
	RandomGet
	RandomCrash
	RandomRestart
	RandomPartition
	RandomHeal
	RandomSnapshot
)

func (k RandomKind) String() string {
	switch k {
	case RandomPut:
		return "Put"
	case RandomDelete:
		return "Delete"
	case RandomGet:
		return "Get"
	case RandomCrash:
		return "Crash"
	case RandomRestart:
		return "Restart"
	case RandomPartition:
		return "Partition"
	case RandomHeal:
		return "Heal"
	case RandomSnapshot:
		return "Snapshot"
	default:
		return "Unknown"
	}
}

// NewRandomPlan generates a bounded plan of exactly numSteps steps for a
// cluster of the given node IDs, using a *rand.Rand seeded from seed --
// the same seed always produces the exact same plan, which is what makes
// a failure reproducible from the seed alone (see RandomPlan).
func NewRandomPlan(seed int64, ids []string, numSteps int) *RandomPlan {
	rnd := rand.New(rand.NewSource(seed))
	plan := &RandomPlan{Seed: seed, Steps: make([]RandomStep, 0, numSteps)}

	kinds := []RandomKind{RandomPut, RandomDelete, RandomGet, RandomCrash, RandomRestart, RandomPartition, RandomHeal, RandomSnapshot}
	for i := 0; i < numSteps; i++ {
		kind := kinds[rnd.Intn(len(kinds))]
		node := ids[rnd.Intn(len(ids))]
		step := RandomStep{Kind: kind, Node: node}
		switch kind {
		case RandomPut:
			step.Key = fmt.Sprintf("k%d", rnd.Intn(5))
			step.Value = fmt.Sprintf("v%d", rnd.Intn(1000))
		case RandomDelete, RandomGet:
			step.Key = fmt.Sprintf("k%d", rnd.Intn(5))
		}
		plan.Steps = append(plan.Steps, step)
	}
	return plan
}

// RunRandomScenario executes plan against c, one step at a time: a
// Put/Delete is proposed only against whichever node currently believes
// itself leader (any other target is skipped as a no-op step, logged as
// such, exactly like a real client that doesn't yet know who the leader
// is); Crash/Restart/Partition/Heal/Snapshot apply directly to plan's
// target node, each skipped as a no-op if it is not currently valid
// (e.g. RandomRestart on a node that is not crashed). Every step
// advances the deterministic scheduler by one round first (see
// Cluster.Advance), so the plan always makes some real progress between
// actions instead of queuing up actions a static cluster could never
// actually process.
//
// It never calls t.Fatalf itself -- the caller (see random_test.go)
// decides what to assert once the whole plan has run, printing
// Cluster.Dump (which, since EventLog.Seed is this plan's seed, is
// enough on its own to reproduce the exact same plan and replay it).
func RunRandomScenario(c *Cluster, plan *RandomPlan) {
	for i, step := range plan.Steps {
		c.Advance(1)
		runRandomStep(c, step, i)
	}
}

func runRandomStep(c *Cluster, step RandomStep, i int) {
	switch step.Kind {
	case RandomPut:
		if leader, ok := c.CurrentLeader(); ok {
			clientID := fmt.Sprintf("rand-client-%d", i)
			cmd := statemachine.NewPutCommand(clientID, uint64(i), []byte(step.Key), []byte(step.Value))
			c.Propose(leader, cmd)
		} else {
			c.Observe(fmt.Sprintf("Skip(Put %s: no leader)", step.Key))
		}
	case RandomDelete:
		if leader, ok := c.CurrentLeader(); ok {
			clientID := fmt.Sprintf("rand-client-%d", i)
			cmd := statemachine.NewDeleteCommand(clientID, uint64(i), []byte(step.Key))
			c.Propose(leader, cmd)
		} else {
			c.Observe(fmt.Sprintf("Skip(Delete %s: no leader)", step.Key))
		}
	case RandomGet:
		if n := c.Node(step.Node); n != nil {
			c.ConsistentGet(step.Node, []byte(step.Key))
		} else {
			c.Observe(fmt.Sprintf("Skip(Get on %s: not live)", step.Node))
		}
	case RandomCrash:
		if !c.IsCrashed(step.Node) {
			c.CrashNode(step.Node)
		} else {
			c.Observe("Skip(Crash: already crashed " + step.Node + ")")
		}
	case RandomRestart:
		if c.IsCrashed(step.Node) {
			c.RestartNode(step.Node)
		} else {
			c.Observe("Skip(Restart: not crashed " + step.Node + ")")
		}
	case RandomPartition:
		c.Partition(step.Node)
	case RandomHeal:
		c.Heal(step.Node)
	case RandomSnapshot:
		if n := c.Node(step.Node); n != nil {
			applied := n.Raft().LastApplied()
			if applied > n.SnapshotIndex() {
				c.CreateSnapshot(step.Node, applied)
			} else {
				c.Observe("Skip(Snapshot: nothing new to snapshot on " + step.Node + ")")
			}
		} else {
			c.Observe("Skip(Snapshot on " + step.Node + ": not live)")
		}
	}
}
