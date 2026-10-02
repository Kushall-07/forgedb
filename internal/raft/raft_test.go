package raft

import "testing"

// TestRandomSeed_ProducesDistinctValues guards against the regression
// Phase 14's real multi-process deployment exposed: NewNode's previous
// default Rand source (rand.NewSource(time.Now().UnixNano())) could seed
// two Node values constructed at nearly the same wall-clock instant with
// the identical value on a coarse-resolution system clock, producing
// byte-for-byte identical "randomized" election timeouts and therefore a
// permanent, deterministic split vote -- observed in practice when three
// independently started OS processes on Windows never elected a leader
// at all. randomSeed (cryptographically seeded) must not reproduce that:
// calling it many times back to back, with no time delay between calls,
// must not collide.
func TestRandomSeed_ProducesDistinctValues(t *testing.T) {
	const n = 64
	seen := make(map[int64]bool, n)
	for i := 0; i < n; i++ {
		s := randomSeed()
		if seen[s] {
			t.Fatalf("randomSeed() produced a duplicate value %d across %d back-to-back calls", s, n)
		}
		seen[s] = true
	}
}

// TestNewNode_DefaultRand_TwoNodesDoNotLockstep is a direct regression
// test for the split-vote scenario itself: two Node values opened back
// to back with Options.Rand left nil (exactly what cmd/forgedb and any
// other caller that does not override it gets) must not pick the same
// randomized election timeout every single time -- see
// resetElectionTimerLocked. This can never be asserted as "never equal
// once," since two independent random draws from the same bounded range
// can legitimately coincide sometimes; the regression this guards
// against is them being correlated on *every* draw (the lockstep
// failure mode), so this checks that at least one of several repeated
// elections differs across two freshly-seeded nodes.
func TestNewNode_DefaultRand_TwoNodesDoNotLockstep(t *testing.T) {
	newNode := func(id string, peers []string) *Node {
		n, err := NewNode(Options{
			ID:              id,
			Peers:           peers,
			Transport:       NewInMemoryTransport(),
			ElectionTickMin: 1000,
			ElectionTickMax: 2000,
		})
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		return n
	}

	allEqual := true
	for i := 0; i < 20; i++ {
		a := newNode("a", []string{"b"})
		b := newNode("b", []string{"a"})
		if a.electionTimeout != b.electionTimeout {
			allEqual = false
			break
		}
	}
	if allEqual {
		t.Fatal("two independently-constructed nodes' default-seeded election timeouts were identical in every one of 20 trials -- the lockstep split-vote regression this test guards against")
	}
}
