// Package endtoend holds Phase 13's end-to-end workload benchmarks: the
// top of ForgeDB's stack, exercised exactly as a real client would --
// client command -> dbnode.Node.Propose/ConsistentGet -> Raft commit ->
// Applier -> KVStateMachine -> storage.Store (WAL + MemTable) -- via the
// real, already-built multi-node harness in package chaos
// (chaos.Cluster), never a parallel fake cluster. See
// benchmark/README.md for how these numbers relate to the lower-level,
// single-layer benchmarks in internal/raft, internal/statemachine,
// internal/storage, and internal/dbnode.
//
// Every cluster here uses 3 or 5 nodes, never 1: a literal one-node
// (zero-peer) Raft cluster never advances its own CommitIndex through
// Propose alone in this codebase (see internal/raft/apply_test.go's
// commitUpTo comment and internal/dbnode/node_test.go's section A), so a
// single-node "committed write" benchmark would have nothing to measure.
//
// chaos.Cluster is explicitly documented as not safe for concurrent use
// by multiple goroutines (see chaos/cluster.go's package doc: "meant to
// be driven by exactly one scenario at a time"), so every benchmark here
// models one sequential client, not concurrent load -- concurrency
// scaling is covered instead at the layers that are safe for it
// (internal/storage's BenchmarkStoreConcurrentPut, internal/raft's
// BenchmarkRaftProposeConcurrent, internal/storage/memtable's
// BenchmarkMemTableConcurrentMixed).
package endtoend

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/chaos"
	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

func init() { logging.SetLevel(slog.LevelError) }

// benchKeyspace bounds every workload below to a fixed number of distinct
// keys, deliberately kept small: raft.FilePersister (see
// internal/raft/file_persister.go) rewrites and fsyncs this node's entire
// Raft log on every committed Propose, so preloading (or growing the
// keyspace across) a large number of keys would make setup/log-growth
// cost dominate the measured workload instead of the workload itself --
// see internal/dbnode/bench_test.go's identical note.
const benchKeyspace = 100

const leaderID = "node0"

func benchKey(i int) []byte { return []byte(fmt.Sprintf("bench-key-%06d", i)) }

// newBenchCluster opens a chaos.Cluster of n nodes under b.TempDir(),
// elects node0 leader, and preloads benchKeyspace keys through a
// committed, applied PUT each -- every workload below starts from this
// identical warm state so results across workloads are comparable (see
// benchmark/README.md's "isolation" section on starting state).
func newBenchCluster(b *testing.B, n int) *chaos.Cluster {
	b.Helper()
	c, err := chaos.NewCluster(b.Name(), 1, chaos.Config{NumNodes: n, BaseDir: b.TempDir()})
	if err != nil {
		b.Fatalf("chaos.NewCluster: %v", err)
	}
	b.Cleanup(c.Close)

	if err := c.ElectLeader(leaderID); err != nil {
		b.Fatalf("ElectLeader: %v", err)
	}
	for i := 0; i < benchKeyspace; i++ {
		if _, _, err := c.Propose(leaderID, statemachine.NewPutCommand("preload", uint64(i+1), benchKey(i), []byte("value"))); err != nil {
			b.Fatalf("preload Propose: %v", err)
		}
	}
	c.Settle()
	c.ApplyAllAvailable()
	return c
}

// --- Key distributions -------------------------------------------------------
//
// Three distinct key-access patterns, each documented per
// benchmark/README.md's rule against silently mixing distributions:
//
//   - uniform:    every key in [0, benchKeyspace) equally likely.
//   - hotset:     80% of accesses land on the smallest 20% of keys,
//     modeling a small set of frequently-accessed records among a larger
//     dataset.
//   - sequential: strictly increasing key index, wrapping at
//     benchKeyspace -- models an append-heavy or scan-like access
//     pattern rather than random lookups.

type keyDistribution func(r *rand.Rand, i int) int

func uniformKeys(r *rand.Rand, i int) int { return r.Intn(benchKeyspace) }

func hotsetKeys(r *rand.Rand, i int) int {
	hotSize := benchKeyspace / 5 // smallest 20% of the keyspace is "hot"
	if r.Intn(100) < 80 {
		return r.Intn(hotSize)
	}
	return hotSize + r.Intn(benchKeyspace-hotSize)
}

func sequentialKeys(r *rand.Rand, i int) int { return i % benchKeyspace }

// --- Latency sampling ---------------------------------------------------------

// maxLatencySamples bounds how many individual operation latencies a
// workload retains for percentile reporting, regardless of b.N -- see
// benchmark/README.md's rule against unbounded latency sample retention
// (Go's own ns/op already averages over all of b.N; this adds p50/p95/p99
// without an unbounded memory cost).
const maxLatencySamples = 20_000

type latencyRecorder struct {
	samples []time.Duration
	seen    int
}

func newLatencyRecorder() *latencyRecorder { return &latencyRecorder{} }

func (r *latencyRecorder) record(d time.Duration) {
	r.seen++
	if len(r.samples) < maxLatencySamples {
		r.samples = append(r.samples, d)
		return
	}
	// Reservoir-style replacement once the cap is hit, so a long run's
	// later samples still have a chance to displace earlier ones instead
	// of the recorded set being frozen to only the first maxLatencySamples
	// operations.
	if j := rand.Intn(r.seen); j < len(r.samples) {
		r.samples[j] = d
	}
}

// report adds p50/p95/p99 latency (in milliseconds) as custom benchmark
// metrics -- Go's own ns/op is a mean, which the task's methodology
// explicitly warns must never be read as a percentile (see
// benchmark/README.md's "latency" section).
func (r *latencyRecorder) report(b *testing.B) {
	if len(r.samples) == 0 {
		return
	}
	sorted := append([]time.Duration(nil), r.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		return float64(sorted[idx]) / float64(time.Millisecond)
	}
	b.ReportMetric(pct(0.50), "p50-ms")
	b.ReportMetric(pct(0.95), "p95-ms")
	b.ReportMetric(pct(0.99), "p99-ms")
}

// --- Workload mixes, by cluster size -----------------------------------------

// runWorkload drives b.N operations against newBenchCluster's warm
// cluster: writePercent% committed PUTs (through full commit + apply) and
// (100-writePercent)% linearizable ConsistentGet reads, using keyDist to
// choose each operation's key. Every operation's own wall-clock latency
// is recorded for percentile reporting.
func runWorkload(b *testing.B, clusterSize, writePercent int, keyDist keyDistribution) {
	c := newBenchCluster(b, clusterSize)
	r := rand.New(rand.NewSource(42))
	lat := newLatencyRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opStart := time.Now()
		key := benchKey(keyDist(r, i))
		if r.Intn(100) < writePercent {
			if _, _, err := c.Propose(leaderID, statemachine.NewPutCommand(fmt.Sprintf("client-%d", i), 1, key, []byte("value"))); err != nil {
				b.Fatalf("Propose: %v", err)
			}
			c.Settle()
			c.ApplyAllAvailable()
		} else {
			if _, err := c.ConsistentGet(leaderID, key); err != nil {
				b.Fatalf("ConsistentGet: %v", err)
			}
		}
		lat.record(time.Since(opStart))
	}
	reportThroughput(b, lat)
}

// reportThroughput derives ops/sec from b.Elapsed() -- the Go testing
// framework's own timer, which already correctly excludes everything
// before ResetTimer and anything between a StopTimer/StartTimer pair --
// rather than a second, independently measured wall-clock span, which on
// this environment's clock resolution could read back as exactly zero for
// a handful of very fast iterations (e.g. an all-ConsistentGet sample at
// a small -benchtime=Nx) and produce a bogus +Inf metric.
func reportThroughput(b *testing.B, lat *latencyRecorder) {
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "logical-ops/sec")
	}
	lat.report(b)
}

// BenchmarkWorkloadMix covers the five workload profiles from
// benchmark/README.md's matrix (100% writes, 100% reads, 50/50, 90/10
// read-heavy, 10/90 write-heavy) across 3- and 5-node clusters, using a
// uniform key distribution -- see BenchmarkKeyDistribution for how
// distribution choice alone affects results at a fixed mix.
func BenchmarkWorkloadMix(b *testing.B) {
	mixes := []struct {
		name         string
		writePercent int
	}{
		{"Writes100", 100},
		{"ReadHeavy90_10", 10},
		{"Balanced50_50", 50},
		{"WriteHeavy10_90", 90},
		{"Reads100", 0},
	}
	for _, clusterSize := range []int{3, 5} {
		for _, m := range mixes {
			b.Run(fmt.Sprintf("nodes=%d/%s", clusterSize, m.name), func(b *testing.B) {
				runWorkload(b, clusterSize, m.writePercent, uniformKeys)
			})
		}
	}
}

// BenchmarkKeyDistribution holds the workload mix fixed (50/50) and
// cluster size fixed (3 nodes) while varying the key-access distribution,
// isolating the effect of key locality alone.
func BenchmarkKeyDistribution(b *testing.B) {
	dists := []struct {
		name string
		fn   keyDistribution
	}{
		{"Uniform", uniformKeys},
		{"HotSet80_20", hotsetKeys},
		{"Sequential", sequentialKeys},
	}
	for _, d := range dists {
		b.Run(d.name, func(b *testing.B) {
			runWorkload(b, 3, 50, d.fn)
		})
	}
}

// --- Value size scaling -------------------------------------------------------

// runValueSizeWorkload is runWorkload's analogue for the value-size
// matrix: a balanced 50/50 mix against a 3-node cluster, with every PUT
// using a size-byte value instead of the small fixed value runWorkload
// uses. Reads (ConsistentGet) are unaffected by write value size on their
// own request shape, but share the same already-written data.
func runValueSizeWorkload(b *testing.B, size int) {
	c := newBenchCluster(b, 3)
	val := make([]byte, size)
	rand.New(rand.NewSource(7)).Read(val)
	r := rand.New(rand.NewSource(42))
	lat := newLatencyRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opStart := time.Now()
		key := benchKey(uniformKeys(r, i))
		if r.Intn(100) < 50 {
			if _, _, err := c.Propose(leaderID, statemachine.NewPutCommand(fmt.Sprintf("client-%d", i), 1, key, val)); err != nil {
				b.Fatalf("Propose: %v", err)
			}
			c.Settle()
			c.ApplyAllAvailable()
		} else {
			if _, err := c.ConsistentGet(leaderID, key); err != nil {
				b.Fatalf("ConsistentGet: %v", err)
			}
		}
		lat.record(time.Since(opStart))
	}
	reportThroughput(b, lat)
}

// BenchmarkValueSize measures how write value size affects end-to-end
// latency/throughput for a balanced workload on a 3-node cluster. Kept to
// three representative sizes (not the full six-point matrix the
// lower-level storage/WAL benchmarks use) to avoid combinatorial
// benchmark explosion at this, the most expensive layer to exercise --
// see benchmark/README.md.
func BenchmarkValueSize(b *testing.B) {
	for _, vs := range []struct {
		name string
		n    int
	}{
		{"256B", 256},
		{"4KiB", 4096},
		{"64KiB", 65536},
	} {
		b.Run(vs.name, func(b *testing.B) {
			runValueSizeWorkload(b, vs.n)
		})
	}
}
