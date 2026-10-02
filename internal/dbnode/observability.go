package dbnode

import (
	"time"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// startedAt is recorded once at package init, used only to compute
// Status.ProcessUptimeSeconds -- a convenience echo of the same value
// the forgedb_node_uptime_seconds metric already exposes, so a /cluster
// JSON response doesn't need a second round trip to /metrics just to
// show how long this process has been running.
var startedAt = time.Now()

// Status is the full diagnostic snapshot internal/api's /cluster
// endpoint renders: this node's identity, its Raft-level state (see
// raft.Status), its peers' replication progress (leader-only), and its
// storage engine's current in-memory size. It is read-only and cheap --
// every field comes from an already-synchronized, O(1) accessor -- and
// never mutates anything (see docs/observability/phase12-observability.md's
// "diagnostics are read-only" rule).
type Status struct {
	NodeID            string            `json:"node_id"`
	ProcessUptimeSecs float64           `json:"process_uptime_seconds"`
	Raft              raft.Status       `json:"raft"`
	Peers             []raft.PeerStatus `json:"peers,omitempty"`
	Storage           StorageStatus     `json:"storage"`
}

// StorageStatus is the storage-engine portion of Status -- see
// storage.MemStore.Stats.
type StorageStatus struct {
	MemTableEntries int   `json:"memtable_entries"`
	MemTableBytes   int64 `json:"memtable_bytes"`
}

// Status returns a full diagnostic snapshot of this node. See the
// Status type doc comment.
func (n *Node) Status() Status {
	st := Status{
		NodeID:            n.id,
		ProcessUptimeSecs: time.Since(startedAt).Seconds(),
		Raft:              n.raft.Status(),
		Peers:             n.raft.PeerStatuses(),
	}
	if ms, ok := n.store.(*storage.MemStore); ok {
		stats := ms.Stats()
		st.Storage = StorageStatus{MemTableEntries: stats.MemTableEntries, MemTableBytes: stats.MemTableBytes}
	}
	return st
}
