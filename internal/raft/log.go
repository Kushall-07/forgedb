package raft

// Log is a node's in-memory Raft log. It is 1-indexed: entries[0] is a
// fixed sentinel with Index 0 and Term 0, standing for "the position
// immediately before the log begins." This lets index-0 be treated as an
// ordinary, always-matching log position rather than a special case --
// notably, a leader's very first AppendEntries to a fresh follower uses
// PrevLogIndex 0, PrevLogTerm 0, and TermAt(0) naturally returns (0,
// true), satisfying the consistency check with no extra branching.
//
// Log is not safe for concurrent use; callers (Node) are responsible for
// their own synchronization. Phase 5 keeps the log entirely in memory --
// persistence and crash recovery for Raft state are Phase 6 work.
type Log struct {
	entries []LogEntry
}

// NewLog returns an empty Log, containing only the index-0 sentinel.
func NewLog() *Log {
	return &Log{entries: []LogEntry{{Index: 0, Term: 0}}}
}

// LastIndex returns the index of the last entry in the log (0 if the log
// is empty apart from the sentinel).
func (l *Log) LastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

// LastTerm returns the term of the last entry in the log (0 if the log is
// empty apart from the sentinel).
func (l *Log) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at index, and false if index is
// not present in the log (including indexes beyond the end of the log).
// Index 0 always returns (0, true), by the sentinel described on Log.
func (l *Log) TermAt(index uint64) (uint64, bool) {
	if index >= uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index].Term, true
}

// EntryAt returns the entry at index, and false if index is 0 (the
// sentinel is not a real entry) or beyond the end of the log.
func (l *Log) EntryAt(index uint64) (LogEntry, bool) {
	if index == 0 || index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// Append adds e to the end of the log, assigning it the next index, and
// returns that index. It is used by a leader appending a newly proposed
// command to its own log.
func (l *Log) Append(e LogEntry) uint64 {
	e.Index = uint64(len(l.entries))
	l.entries = append(l.entries, e)
	return e.Index
}

// Slice returns a copy of the log entries from index from (inclusive) to
// the end of the log, or nil if from is beyond the end of the log. The
// returned slice is a fresh copy safe for the caller to retain; the
// LogEntry values themselves (including their Command byte slices) are
// shared with the log and must be treated as read-only.
func (l *Log) Slice(from uint64) []LogEntry {
	if from == 0 {
		from = 1
	}
	if from >= uint64(len(l.entries)) {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-int(from))
	copy(out, l.entries[from:])
	return out
}

// Range returns a copy of the log entries in the inclusive range [from,
// to]. It clamps to the log's actual bounds and returns nil if the range
// is empty or entirely out of bounds.
func (l *Log) Range(from, to uint64) []LogEntry {
	if from == 0 {
		from = 1
	}
	if to >= uint64(len(l.entries)) {
		to = uint64(len(l.entries)) - 1
	}
	if from > to {
		return nil
	}
	out := make([]LogEntry, to-from+1)
	copy(out, l.entries[from:to+1])
	return out
}

// AppendAfter implements the follower side of AppendEntries log
// replication: newEntries are the entries a leader sent immediately after
// prevIndex (which the caller must already have verified matches the
// leader's PrevLogTerm via TermAt(prevIndex), per the AppendEntries
// consistency check -- AppendAfter itself does not re-check prevIndex).
//
// For each new entry, in order:
//   - if the log already has an entry at that index with the same term,
//     it is left untouched (the matching prefix is never truncated, even
//     though the leader may be re-sending entries the follower already
//     has -- this happens naturally on a retried or duplicate RPC);
//   - otherwise (a genuine conflict, where an existing entry at that
//     index has a different term, or the log does not yet reach that
//     index), the log is truncated starting at that index and every
//     remaining new entry, including this one, is appended.
//
// This is exactly the Raft log-matching / conflict-resolution rule: a
// follower never blindly appends duplicate entries, and never discards
// entries that already agree with the leader.
func (l *Log) AppendAfter(prevIndex uint64, newEntries []LogEntry) {
	for i, e := range newEntries {
		idx := prevIndex + 1 + uint64(i)
		if idx < uint64(len(l.entries)) {
			if l.entries[idx].Term == e.Term {
				continue
			}
			l.entries = l.entries[:idx]
		}
		l.appendFrom(idx, newEntries[i:])
		return
	}
}

// appendFrom appends entries to the log starting at index startIdx,
// which must be exactly len(l.entries) (i.e. immediately after the
// current end of the log) when called.
func (l *Log) appendFrom(startIdx uint64, entries []LogEntry) {
	for i, e := range entries {
		e.Index = startIdx + uint64(i)
		l.entries = append(l.entries, e)
	}
}

// logIsUpToDate implements the Raft §5.4.1 log-freshness comparison used
// by the RequestVote voting rule: a candidate's log is at least as
// up-to-date as a voter's if the candidate's last entry has a later term,
// or, when the terms are equal, if the candidate's log is at least as
// long. Term is compared first and takes priority over length -- a
// shorter log with a later term is still more up-to-date than a longer
// log stuck at an older term, since only entries from the current
// leader's term (or later) can represent the true, agreed-upon history.
func logIsUpToDate(candidateLastIndex, candidateLastTerm, voterLastIndex, voterLastTerm uint64) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}
