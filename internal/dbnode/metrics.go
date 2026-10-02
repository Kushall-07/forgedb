package dbnode

import (
	"sync"
	"sync/atomic"

	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// currentNode is the Node whose live state the pull-based gauges below
// currently report. In a real deployment exactly one Node is ever open
// per process, so this is simply "this process's node." A test binary
// that opens many Nodes in the same process (as this package's own
// tests, and internal/raft's, do) instead keeps redirecting these
// gauges at whichever Node most recently called Open -- see Open/Close
// -- rather than every Open attempting a second, panicking registration
// of the same global metric name (see metrics.Registry.register).
var currentNode atomic.Pointer[Node]

var metricsOnce sync.Once

// registerMetricsOnce wires this process's live Raft and storage state
// into the global metrics.Default registry, exactly once per process.
// Every gauge here is pull-based: it reads currentNode's already-cheap,
// already-synchronized accessors (raft.Node.Status/PeerStatuses,
// storage.MemStore.Stats) at scrape time, never on a schedule and never
// while any Raft or storage lock is held -- see
// docs/observability/phase12-observability.md.
func registerMetricsOnce() {
	metricsOnce.Do(func() {
		metrics.Default.NewGaugeFunc("forgedb_raft_term", "This node's current Raft term.", func() float64 {
			n := currentNode.Load()
			if n == nil {
				return 0
			}
			return float64(n.raft.Status().Term)
		})
		metrics.Default.NewGaugeVecFunc("forgedb_raft_role", "1 for this node's current role, 0 for the others.", "role", func() map[string]float64 {
			out := map[string]float64{"leader": 0, "candidate": 0, "follower": 0}
			n := currentNode.Load()
			if n == nil {
				return out
			}
			role := n.raft.Status().Role
			for k := range out {
				if k == toLowerRole(role) {
					out[k] = 1
				}
			}
			return out
		})
		metrics.Default.NewGaugeFunc("forgedb_raft_commit_index", "This node's current Raft commit index.", func() float64 {
			return statusFloat(func(s raft.Status) uint64 { return s.CommitIndex })
		})
		metrics.Default.NewGaugeFunc("forgedb_raft_last_applied", "This node's current Raft last-applied index.", func() float64 {
			return statusFloat(func(s raft.Status) uint64 { return s.LastApplied })
		})
		metrics.Default.NewGaugeFunc("forgedb_raft_log_entries", "Entries currently retained in this node's Raft log, past its snapshot boundary.", func() float64 {
			return statusFloat(func(s raft.Status) uint64 {
				if s.LastLogIndex <= s.SnapshotIndex {
					return 0
				}
				return s.LastLogIndex - s.SnapshotIndex
			})
		})
		metrics.Default.NewGaugeFunc("forgedb_raft_snapshot_index", "The index of this node's most recent Raft snapshot (0 if none).", func() float64 {
			return statusFloat(func(s raft.Status) uint64 { return s.SnapshotIndex })
		})
		metrics.Default.NewGaugeVecFunc("forgedb_raft_follower_lag_entries", "Entries a follower's matchIndex is behind this leader's last log index.", "peer_id", func() map[string]float64 {
			n := currentNode.Load()
			if n == nil {
				return nil
			}
			out := make(map[string]float64)
			for _, p := range n.raft.PeerStatuses() {
				out[p.PeerID] = float64(p.Lag)
			}
			return out
		})

		metrics.Default.NewGaugeFunc("forgedb_storage_memtable_entries", "Live entries currently held in this node's MemTable.", func() float64 {
			stats, ok := storageStats()
			if !ok {
				return 0
			}
			return float64(stats.MemTableEntries)
		})
		metrics.Default.NewGaugeFunc("forgedb_storage_memtable_bytes", "Approximate live key+value bytes currently held in this node's MemTable.", func() float64 {
			stats, ok := storageStats()
			if !ok {
				return 0
			}
			return float64(stats.MemTableBytes)
		})
	})
}

func statusFloat(get func(raft.Status) uint64) float64 {
	n := currentNode.Load()
	if n == nil {
		return 0
	}
	return float64(get(n.raft.Status()))
}

// storageStats returns the current Node's storage.MemStore.Stats, and
// false if no Node is current or its Store is not a *storage.MemStore
// (e.g. a test-only WrapStore decorator) -- production Nodes always
// construct a MemStore (see Open), so this only ever returns false in a
// test that deliberately replaces storage with something else.
func storageStats() (storage.Stats, bool) {
	n := currentNode.Load()
	if n == nil {
		return storage.Stats{}, false
	}
	ms, ok := n.store.(*storage.MemStore)
	if !ok {
		return storage.Stats{}, false
	}
	return ms.Stats(), true
}

func toLowerRole(role string) string {
	switch role {
	case "Leader":
		return "leader"
	case "Candidate":
		return "candidate"
	default:
		return "follower"
	}
}
