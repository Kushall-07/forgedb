package raft

import "fmt"

// Log is a node's in-memory Raft log. It is 1-indexed: entries[0] is a
// sentinel standing for "the position immediately before the log begins."
// Before any snapshot has ever been created (see snapshot.go and
// docs/raft/phase9-snapshots.md), that position is always (Index: 0, Term:
// 0) -- this is Phase 5's original design, and every method below behaves
// exactly as it always has for a Log that has never been compacted.
//
// Once a snapshot exists, entries[0] instead stands for that snapshot's
// own boundary: (Index: lastIncludedIndex, Term: lastIncludedTerm). This
// is a direct generalization of the original sentinel, not a different
// mechanism -- "the position immediately before the log begins" is
// exactly what a snapshot boundary *is* once a prefix has been discarded.
// Every method that used to special-case "index/offset 0" now compares
// against entries[0].Index instead, and behaves identically to before when
// that value happens to be 0.
//
// Physical slice offset and logical Raft index are therefore no longer
// the same number once compaction has happened: logical index i lives at
// physical offset i - entries[0].Index. Callers of this type (Node) only
// ever deal in logical indexes; the offset arithmetic is entirely
// internal to this file.
//
// Log is not safe for concurrent use; callers (Node) are responsible for
// their own synchronization.
type Log struct {
	entries []LogEntry
}

// NewLog returns an empty Log, containing only the index-0 sentinel (no
// snapshot has ever been created).
func NewLog() *Log {
	return &Log{entries: []LogEntry{{Index: 0, Term: 0}}}
}

// LastIndex returns the index of the last entry in the log, or the
// snapshot boundary's own index if no entries have been retained past it.
func (l *Log) LastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// LastTerm returns the term of the last entry in the log, or the snapshot
// boundary's own term if no entries have been retained past it.
func (l *Log) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at index, and false if index is
// not present in this log -- either because it has already been
// compacted away (index below entries[0].Index) or because it is beyond
// the end of the log. Index == entries[0].Index (the sentinel/snapshot
// boundary) always returns (entries[0].Term, true): this is what lets
// AppendEntries's consistency check and RequestVote's up-to-date check
// work correctly right at the compaction boundary with no retained entry
// there at all (see docs/raft/phase9-snapshots.md).
func (l *Log) TermAt(index uint64) (uint64, bool) {
	base := l.entries[0].Index
	if index < base {
		return 0, false
	}
	offset := index - base
	if offset >= uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[offset].Term, true
}

// EntryAt returns the entry at index, and false if index is the sentinel
// (the compaction boundary is not a real retained entry, exactly as index
// 0 never was before any compaction existed) or beyond the end of the
// log.
func (l *Log) EntryAt(index uint64) (LogEntry, bool) {
	base := l.entries[0].Index
	if index <= base {
		return LogEntry{}, false
	}
	offset := index - base
	if offset >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[offset], true
}

// Append adds e to the end of the log, assigning it the next index, and
// returns that index. It is used by a leader appending a newly proposed
// command to its own log.
func (l *Log) Append(e LogEntry) uint64 {
	e.Index = l.entries[0].Index + uint64(len(l.entries))
	l.entries = append(l.entries, e)
	return e.Index
}

// Slice returns a copy of the log entries from index from (inclusive) to
// the end of the log, or nil if from is beyond the end of the log. from
// at or below the current compaction boundary is clamped up to just past
// it (there is nothing earlier left to return). The returned slice is a
// fresh copy safe for the caller to retain; the LogEntry values themselves
// (including their Command byte slices) are shared with the log and must
// be treated as read-only.
func (l *Log) Slice(from uint64) []LogEntry {
	base := l.entries[0].Index
	if from <= base {
		from = base + 1
	}
	offset := from - base
	if offset >= uint64(len(l.entries)) {
		return nil
	}
	out := make([]LogEntry, uint64(len(l.entries))-offset)
	copy(out, l.entries[offset:])
	return out
}

// Range returns a copy of the log entries in the inclusive range [from,
// to]. It clamps to the log's actual retained bounds (from up to just past
// the compaction boundary, to down to the last real entry) and returns nil
// if the resulting range is empty.
func (l *Log) Range(from, to uint64) []LogEntry {
	base := l.entries[0].Index
	if from <= base {
		from = base + 1
	}
	lastIdx := l.LastIndex()
	if to > lastIdx {
		to = lastIdx
	}
	if from > to {
		return nil
	}
	fromOff := from - base
	toOff := to - base
	out := make([]LogEntry, toOff-fromOff+1)
	copy(out, l.entries[fromOff:toOff+1])
	return out
}

// AppendAfter implements the follower side of AppendEntries log
// replication: newEntries are the entries a leader sent immediately after
// prevIndex (which the caller must already have verified matches the
// leader's PrevLogTerm via TermAt(prevIndex) -- and therefore that
// prevIndex is at or above the current compaction boundary -- per the
// AppendEntries consistency check; AppendAfter itself does not re-check
// prevIndex).
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
// entries that already agree with the leader. It also never discards
// anything at or before the compaction boundary: prevIndex is always
// already at or above it, so no new entry's index can be at or below it
// either.
//
// AppendAfter reports whether it actually mutated the log (truncated a
// conflicting suffix and/or appended new entries), as opposed to being a
// true no-op because every incoming entry already matched. Phase 6 uses
// this to decide whether an AppendEntries call needs to durably persist
// the log at all -- a pure heartbeat, or a retried RPC resending entries
// the follower already has, needs no disk write.
func (l *Log) AppendAfter(prevIndex uint64, newEntries []LogEntry) bool {
	base := l.entries[0].Index
	for i, e := range newEntries {
		idx := prevIndex + 1 + uint64(i)
		offset := idx - base
		if offset < uint64(len(l.entries)) {
			if l.entries[offset].Term == e.Term {
				continue
			}
			l.entries = l.entries[:offset]
		}
		l.appendFrom(idx, newEntries[i:])
		return true
	}
	return false
}

// appendFrom appends entries to the log starting at index startIdx,
// which must be exactly l.entries[0].Index + len(l.entries) (i.e.
// immediately after the current end of the log) when called.
func (l *Log) appendFrom(startIdx uint64, entries []LogEntry) {
	for i, e := range entries {
		e.Index = startIdx + uint64(i)
		l.entries = append(l.entries, e)
	}
}

// compact discards every entry at or before index, replacing them with a
// new sentinel at (index, term), where term is index's own current term
// in this log (looked up via TermAt). It is used by (*Node).CreateSnapshot,
// where index is always this node's *own* already-applied log position --
// there is never a conflict to resolve, unlike installSnapshotBoundary
// below, which a follower uses to adopt a boundary a *leader* sent it.
//
// compact returns an error, mutating nothing, if index is not currently
// present in this log (including already being at or below the existing
// compaction boundary, or beyond LastIndex()). CreateSnapshot already
// validates this independently before calling compact; this is defense in
// depth, not the primary check.
func (l *Log) compact(index uint64) error {
	term, ok := l.TermAt(index)
	if !ok {
		return fmt.Errorf("raft: log: cannot compact at index %d: not present in this log", index)
	}
	base := l.entries[0].Index
	offset := index - base
	newEntries := make([]LogEntry, 0, uint64(len(l.entries))-offset)
	newEntries = append(newEntries, LogEntry{Index: index, Term: term})
	newEntries = append(newEntries, l.entries[offset+1:]...)
	l.entries = newEntries
	return nil
}

// installSnapshotBoundary resets this log's compaction boundary to
// (lastIncludedIndex, lastIncludedTerm), as a follower does when it
// accepts InstallSnapshot from the leader (see HandleInstallSnapshot).
// Unlike compact, the leader's snapshot boundary may disagree with (or
// simply extend past) whatever this log currently holds at that index:
//
//   - if this log already has an entry at lastIncludedIndex with term
//     lastIncludedTerm (which includes the boundary already being exactly
//     this log's own current sentinel), everything after it already
//     agrees with the leader up to that point and is preserved untouched
//     -- the "matching suffix" case (see docs/raft/phase9-snapshots.md);
//   - otherwise (a real conflict at that index, or this log doesn't reach
//     that index at all), every existing entry is discarded: the leader's
//     snapshot boundary becomes this log's entire authoritative history
//     and it starts empty immediately after it.
//
// installSnapshotBoundary must only be called with lastIncludedIndex
// strictly greater than this log's current entries[0].Index -- moving the
// boundary backward (a stale or duplicate InstallSnapshot RPC) is
// HandleInstallSnapshot's responsibility to detect and reject before ever
// calling this method.
func (l *Log) installSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) {
	if term, ok := l.TermAt(lastIncludedIndex); ok && term == lastIncludedTerm {
		base := l.entries[0].Index
		offset := lastIncludedIndex - base
		newEntries := make([]LogEntry, 0, uint64(len(l.entries))-offset)
		newEntries = append(newEntries, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})
		newEntries = append(newEntries, l.entries[offset+1:]...)
		l.entries = newEntries
		return
	}
	l.entries = []LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
}

// logIsUpToDate implements the Raft §5.4.1 log-freshness comparison used
// by the RequestVote voting rule: a candidate's log is at least as
// up-to-date as a voter's if the candidate's last entry has a later term,
// or, when the terms are equal, if the candidate's log is at least as
// long. Term is compared first and takes priority over length -- a
// shorter log with a later term is still more up-to-date than a longer
// log stuck at an older term, since only entries from the current
// leader's term (or later) can represent the true, agreed-upon history.
// candidateLastIndex/candidateLastTerm and voterLastIndex/voterLastTerm
// are each a log's logical "last position" -- LastIndex()/LastTerm() --
// which, after compaction, may be the snapshot boundary itself rather
// than a physically retained entry; this function does not need to know
// the difference.
func logIsUpToDate(candidateLastIndex, candidateLastTerm, voterLastIndex, voterLastTerm uint64) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}
