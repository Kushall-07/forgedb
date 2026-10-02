package correctness

import (
	"os"
	"strconv"
	"testing"

	"github.com/Kushall-07/forgedb/chaos"
)

// TestRandomCorrectness_BoundedSeededScenario is §34/§35 of the Phase 11
// brief: a bounded, seeded randomized linearizability test, run only
// after every deterministic history in integration_test.go already
// passes. The default seed (42, mirroring chaos/random_test.go's own
// convention) is overridable via the CORRECTNESS_SEED environment
// variable for local reproduction of a specific failing run -- exactly
// like chaos's own CHAOS_SEED.
func TestRandomCorrectness_BoundedSeededScenario(t *testing.T) {
	seed := int64(42)
	if v := os.Getenv("CORRECTNESS_SEED"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("CORRECTNESS_SEED=%q: %v", v, err)
		}
		seed = parsed
	}

	c, err := chaos.NewCluster(t.Name(), seed, chaos.Config{NumNodes: 5, BaseDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(c.Close)

	if err := c.ElectLeader("node0"); err != nil {
		t.Fatalf("ElectLeader: %v\n\n%s", err, c.Dump())
	}

	h, err := RunRandomCorrectness(c, RandomCorrectnessOptions{
		Seed:         seed,
		NumOps:       25,
		MaxPending:   3,
		InjectFaults: true,
	})
	if err != nil {
		t.Fatalf("RunRandomCorrectness: %v", err)
	}

	if err := h.Validate(); err != nil {
		t.Fatalf("recorded history failed Validate (seed=%d): %v\n\n%s", seed, err, c.Dump())
	}

	result := Check(h, CheckOptions{})
	switch result.Status {
	case StatusValid:
		// expected
	case StatusInconclusive:
		t.Logf("Check was INCONCLUSIVE (seed=%d, states explored=%d, budget=%d) -- not a failure, but consider raising CheckOptions.MaxStates for this seed",
			seed, result.StatesExplored, result.Budget)
	case StatusViolation:
		t.Fatalf("LINEARIZABILITY VIOLATION (seed=%d) -- reproduce with CORRECTNESS_SEED=%d\n\n%s\n\nchaos log:\n%s",
			seed, seed, result.Report(h), c.Dump())
	}
}

// TestRandomCorrectness_MultipleSeeds runs a handful of additional fixed
// seeds at a smaller size, as extra coverage beyond the one default seed
// above, without turning this into an unbounded fuzz run (§35: keep the
// default randomized test small).
func TestRandomCorrectness_MultipleSeeds(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 7, 99} {
		t.Run("seed_"+strconv.FormatInt(seed, 10), func(t *testing.T) {
			c, err := chaos.NewCluster(t.Name(), seed, chaos.Config{NumNodes: 3, BaseDir: t.TempDir()})
			if err != nil {
				t.Fatalf("NewCluster: %v", err)
			}
			t.Cleanup(c.Close)
			if err := c.ElectLeader("node0"); err != nil {
				t.Fatalf("ElectLeader: %v\n\n%s", err, c.Dump())
			}

			h, err := RunRandomCorrectness(c, RandomCorrectnessOptions{
				Seed:       seed,
				NumOps:     15,
				MaxPending: 2,
			})
			if err != nil {
				t.Fatalf("RunRandomCorrectness: %v", err)
			}
			if err := h.Validate(); err != nil {
				t.Fatalf("recorded history failed Validate (seed=%d): %v\n\n%s", seed, err, c.Dump())
			}

			result := Check(h, CheckOptions{})
			if result.Status == StatusViolation {
				t.Fatalf("LINEARIZABILITY VIOLATION (seed=%d)\n\n%s\n\nchaos log:\n%s", seed, result.Report(h), c.Dump())
			}
		})
	}
}
