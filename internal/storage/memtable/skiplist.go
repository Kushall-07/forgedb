package memtable

import (
	"bytes"
	"math/rand"
)

// maxLevel bounds how many forward pointers a node may have. It is large
// enough for an in-memory MemTable's expected key volume without making
// nodes needlessly wide.
const maxLevel = 16

// p is the probability used to decide how many levels a newly inserted
// node participates in. 0.25 is a standard, well-analyzed choice for skip
// lists (Pugh, 1990).
const p = 0.25

// skipListNode is a single entry in the skip list. deleted marks the node
// as a tombstone: the key is logically absent even though the node itself
// remains linked, mirroring how deletes will eventually be represented
// once entries can also live in immutable MemTables and SSTables.
type skipListNode struct {
	key     []byte
	value   []byte
	deleted bool
	forward []*skipListNode
}

// skipList is an ordered, singly-linked skip list keyed by byte slice
// comparison. It is not safe for concurrent use on its own; callers
// (the MemTable) are responsible for synchronization.
type skipList struct {
	header *skipListNode
	level  int
}

func newSkipList() *skipList {
	return &skipList{
		header: &skipListNode{forward: make([]*skipListNode, maxLevel)},
		level:  1,
	}
}

func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.Float64() < p {
		lvl++
	}
	return lvl
}

// search returns the node for key, if present, regardless of whether it is
// a tombstone. Callers decide how to interpret a tombstone hit.
func (s *skipList) search(key []byte) (*skipListNode, bool) {
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x, true
	}
	return nil, false
}

// upsert inserts a new node for key, or updates the existing node's value
// and tombstone state in place if key is already present. key and value
// are stored as given, so callers must pass copies they own.
//
// It reports existed (whether key was already present, tombstone or
// not), wasDeleted (the prior node's tombstone state, meaningless if
// existed is false), and oldValueLen (the prior node's value length,
// meaningless if existed is false) -- exactly what a caller (MemTable)
// needs to maintain an incremental live-entry/byte count without ever
// having to re-scan the whole list (see MemTable.Len/Bytes and
// docs/observability/phase12-observability.md's rule against scanning
// the database on every metrics scrape).
func (s *skipList) upsert(key, value []byte, deleted bool) (existed, wasDeleted bool, oldValueLen int) {
	update := make([]*skipListNode, maxLevel)
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && bytes.Equal(x.key, key) {
		existed, wasDeleted, oldValueLen = true, x.deleted, len(x.value)
		x.value = value
		x.deleted = deleted
		return existed, wasDeleted, oldValueLen
	}

	newLevel := randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.header
		}
		s.level = newLevel
	}

	node := &skipListNode{key: key, value: value, deleted: deleted, forward: make([]*skipListNode, newLevel)}
	for i := 0; i < newLevel; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
	return false, false, 0
}

// forEach calls fn once for every node in the list, in ascending key
// order, by walking the level-0 forward chain (which always threads
// through every node regardless of its own height). It includes
// tombstones; callers that only want live entries (see MemTable.All)
// filter them out themselves.
func (s *skipList) forEach(fn func(node *skipListNode)) {
	for x := s.header.forward[0]; x != nil; x = x.forward[0] {
		fn(x)
	}
}
