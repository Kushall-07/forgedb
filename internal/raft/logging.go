package raft

import (
	"log/slog"

	"github.com/Kushall-07/forgedb/internal/logging"
)

// Event names this package logs under -- aliases of the shared catalog
// in internal/logging/events.go, kept local so call sites in this
// package read as eventElectionStarted rather than the longer
// logging.EventRaftElectionStarted.
const (
	eventElectionStarted   = logging.EventRaftElectionStarted
	eventElectionWon       = logging.EventRaftElectionWon
	eventLeaderChanged     = logging.EventRaftLeaderChanged
	eventStepDown          = logging.EventRaftStepDown
	eventAppendSuccess     = logging.EventRaftAppendSuccess
	eventAppendFailure     = logging.EventRaftAppendFailure
	eventCommitAdvance     = logging.EventRaftCommitAdvance
	eventSnapshotCreated   = logging.EventRaftSnapshotCreated
	eventSnapshotInstalled = logging.EventRaftSnapshotInstalled
	eventSnapshotFailed    = logging.EventRaftSnapshotFailed
)

// logEvent returns a logger tagging every record with this node's ID and
// component=raft, used at every call site below instead of repeating
// those two fields by hand. See docs/observability/phase12-observability.md's
// logging section: append.success/commit.advance are logged at Debug
// (they happen on every heartbeat/commit and would otherwise flood
// normal-operation logs -- see Tick/broadcastAppendEntriesLocked's
// per-heartbeat frequency), everything else here is a rare, meaningful
// state transition and is logged at Info.
func logEvent(nodeID string) *slog.Logger {
	return logging.With("node_id", nodeID, "component", "raft")
}
