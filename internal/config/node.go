package config

// Node describes one member of a ForgeDB cluster's static roster: its
// ID, its client-facing HTTP address, and its node-to-node gRPC address.
// Config.Peers is a list of these -- Phase 14 uses it both to build
// internal/raft's peer-ID list and internal/transport's gRPC dial-address
// map (excluding this process's own entry from each), and, on the API
// side, to resolve a known leader's ID to an HTTP address a client can be
// redirected to (see internal/api's not-leader response).
type Node struct {
	ID   string
	HTTP string
	GRPC string
}
