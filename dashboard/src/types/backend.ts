/**
 * Raw shapes returned by the real ForgeDB HTTP API, exactly as
 * internal/api/server.go and internal/dbnode/observability.go encode
 * them -- including the Go field-name casing on the unexported-tag
 * structs (raft.Status, raft.PeerStatus), which do NOT use snake_case
 * because they carry no `json:"..."` tags. Verified against a running
 * node:
 *
 *   GET /health  -> {"status":"alive","node_id":"node-1"}
 *   GET /cluster -> {"node_id":"node-1","process_uptime_seconds":1.4,
 *                    "raft":{"NodeID":"node-1","Role":"Follower","Term":0,
 *                    "LeaderID":"","CommitIndex":0,"LastApplied":0,
 *                    "LastLogIndex":0,"LastLogTerm":0,"SnapshotIndex":0,
 *                    "SnapshotTerm":0,"Peers":null},
 *                    "storage":{"memtable_entries":0,"memtable_bytes":0}}
 *
 * These types describe the backend's wire format only. Dashboard-side
 * code never consumes this shape directly outside services/forgedbApi.ts
 * -- it is mapped into the dashboard's own types/cluster.ts shapes (or
 * into LiveNodeSnapshot below, for fields that type has no slot for).
 */

export interface BackendHealthResponse {
  status: string;
  node_id: string;
}

export interface BackendPeerStatus {
  PeerID: string;
  NextIndex: number;
  MatchIndex: number;
  Lag: number;
}

export interface BackendRaftStatus {
  NodeID: string;
  Role: string;
  Term: number;
  LeaderID: string;
  CommitIndex: number;
  LastApplied: number;
  LastLogIndex: number;
  LastLogTerm: number;
  SnapshotIndex: number;
  SnapshotTerm: number;
  Peers: string[] | null;
}

export interface BackendStorageStatus {
  memtable_entries: number;
  memtable_bytes: number;
}

export interface BackendClusterStatus {
  node_id: string;
  process_uptime_seconds: number;
  raft: BackendRaftStatus;
  peers?: BackendPeerStatus[];
  storage: BackendStorageStatus;
}

/**
 * The queried node's own state, mapped from BackendClusterStatus but kept
 * separate from types/cluster.ts's NodeState/ClusterState: GET /cluster
 * is node-local (see internal/api/server.go's handleCluster), so this is
 * one node's self-reported view, not a federated multi-node record, and
 * it carries fields (role as the backend's own string, snapshot term,
 * process uptime) that NodeState has no slot for and that this phase
 * must not force into that type by inventing values.
 */
export interface LiveNodeSnapshot {
  nodeId: string;
  role: string;
  leaderId: string;
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastLogIndex: number;
  lastLogTerm: number;
  snapshotIndex: number;
  snapshotTerm: number;
  memtableEntries: number;
  memtableBytes: number;
  processUptimeSeconds: number;
}
