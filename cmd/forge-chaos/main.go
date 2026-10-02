// Command forge-chaos is a small, manually-run executable wrapper around
// the chaos package's bounded randomized scenario (see
// chaos.NewRandomPlan/RunRandomScenario), for exploring chaos outcomes
// interactively or in a longer CI job than `go test`'s default run is
// meant for -- the actual safety net is `go test ./chaos/...`, run as
// part of the ordinary test suite; this command exists purely so a seed
// and step count can be supplied from the command line instead of edited
// into a test file. See docs/chaos/phase10-chaos-testing.md.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/Kushall-07/forgedb/chaos"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

func main() {
	seed := flag.Int64("seed", rand.Int63(), "random seed for the chaos plan (reported in output; pass it back via -seed to reproduce a failure exactly)")
	nodes := flag.Int("nodes", 5, "number of nodes in the cluster")
	steps := flag.Int("steps", 200, "number of randomized steps to run")
	convergeRounds := flag.Int("converge-rounds", 100, "bounded rounds to wait for post-run convergence")
	flag.Parse()

	fmt.Printf("forge-chaos: seed=%d nodes=%d steps=%d\n", *seed, *nodes, *steps)

	baseDir, err := os.MkdirTemp("", "forge-chaos-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-chaos: create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir)

	c, err := chaos.NewCluster("forge-chaos", *seed, chaos.Config{NumNodes: *nodes, BaseDir: baseDir})
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-chaos: NewCluster: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	plan := chaos.NewRandomPlan(*seed, c.IDs(), *steps)
	chaos.RunRandomScenario(c, plan)

	for _, id := range c.IDs() {
		c.Heal(id)
	}
	c.HealPartitions()
	for _, id := range c.IDs() {
		if c.IsCrashed(id) {
			if err := c.RestartNode(id); err != nil {
				fail(c, fmt.Errorf("final RestartNode(%s): %w", id, err))
			}
		}
	}

	leader, err := c.WaitForLeader(*convergeRounds)
	if err != nil {
		fail(c, err)
	}
	// See chaos/random_test.go's identical step for why this is required
	// before convergence can be expected: Raft's current-term commit
	// rule.
	closingMarker := statemachine.NewPutCommand("forge-chaos-closing-marker", 1, []byte("closing-marker"), []byte("done"))
	if _, _, err := c.Propose(leader, closingMarker); err != nil {
		fail(c, fmt.Errorf("closing Propose(%s): %w", leader, err))
	}
	c.Settle()

	if err := c.WaitForConvergence(*convergeRounds); err != nil {
		fail(c, err)
	}
	if err := c.AssertInvariants(); err != nil {
		fail(c, err)
	}

	fmt.Println("forge-chaos: PASS -- cluster converged, no invariant violations")
}

func fail(c *chaos.Cluster, err error) {
	fmt.Fprintln(os.Stderr, "forge-chaos: FAIL:", err)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, c.Dump())
	os.Exit(1)
}
