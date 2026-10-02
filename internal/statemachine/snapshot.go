package statemachine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/Kushall-07/forgedb/internal/storage"
)

// Snapshotter is implemented by a StateMachine that supports capturing its
// entire current state as an opaque, serialized snapshot and later
// restoring it wholesale -- the state-machine side of Phase 9's Raft
// snapshotting (see internal/raft.Node.CreateSnapshot/PendingSnapshot and
// docs/raft/phase9-snapshots.md). internal/raft never calls these methods
// itself and never interprets the bytes they produce or consume; only
// (*Applier).CreateSnapshot and (*Applier).ApplyAvailable do, via a type
// assertion -- a StateMachine that does not implement Snapshotter (e.g. a
// minimal test fake) simply cannot be snapshotted, failing clearly instead
// of silently losing state.
type Snapshotter interface {
	// CreateSnapshot serializes this state machine's entire current state
	// into opaque bytes suitable for later being passed to RestoreSnapshot
	// (by this instance, a freshly restarted one, or a different replica
	// entirely). It must capture everything required to resume producing
	// identical results afterward -- for KVStateMachine, that includes the
	// deduplication table, not just key/value data (see
	// docs/raft/phase9-snapshots.md).
	CreateSnapshot() ([]byte, error)

	// RestoreSnapshot replaces this state machine's entire current state
	// with what data (previously produced by CreateSnapshot) describes,
	// discarding whatever state existed before the call. It returns an
	// error, changing nothing durable that can be helped, if data fails
	// structural validation.
	RestoreSnapshot(data []byte) error
}

var _ Snapshotter = (*KVStateMachine)(nil)

// This file defines KVStateMachine's own snapshot wire format, used by
// CreateSnapshot/RestoreSnapshot. It follows the same conventions as
// internal/raft/format.go (magic + version + explicit little-endian
// fields + a trailing CRC-32C checksum over everything before it):
//
//	magic (8 bytes "ForgeSM1")
//	formatVersion (u32 LE)
//	numKV (u32 LE)
//	  [ keyLen (u32 LE) | key | valueLen (u32 LE) | value ] * numKV
//	numDedup (u32 LE)
//	  [ clientIDLen (u32 LE) | clientID
//	    | requestID (u64 LE)
//	    | op (u8)
//	    | keyLen (u32 LE) | key
//	    | valueLen (u32 LE) | value
//	    | resultApplied (u8) | resultReplayed (u8)
//	    | resultKeyLen (u32 LE) | resultKey
//	    | resultValueLen (u32 LE) | resultValue
//	    | resultErrCode (u8) ] * numDedup
//	checksum (u32 LE, CRC-32C over every preceding byte)
//
// KV entries are encoded in the sorted-by-key order Store.Snapshot already
// returns them in; dedup entries are sorted by ClientID before encoding
// (Go map iteration order is randomized, and a snapshot's bytes must be
// deterministic for a given logical state). resultErrCode encodes
// Result.Err as one of a small, closed set of known sentinel values (see
// errCode*) -- every error KVStateMachine.Apply can ever place in a
// resolved Result.Err is one of these, by construction (see execute and
// Apply).
var smSnapshotMagic = [8]byte{'F', 'o', 'r', 'g', 'e', 'S', 'M', '1'}

const smSnapshotFormatVersion1 = 1

// Bounds checked before any length is used to size an allocation, the
// same defense-in-depth convention every other on-disk/wire format in
// this codebase follows.
const (
	smMaxEntries       = 1 << 20 // ~1,000,000 live keys
	smMaxDedupEntries  = 1 << 20 // ~1,000,000 distinct clients
	smMaxKeyLen        = 1 << 20 // 1 MiB, matching command.go's maxKeyLen
	smMaxValueLen      = 1 << 20 // 1 MiB, matching command.go's maxValueLen
	smMaxClientIDLen   = 4096    // matching command.go's maxClientIDLen
	smMaxSnapshotBytes = 1 << 29 // 512 MiB total, checked before decoding
)

// resultErrCode is the closed, bounded set of errors a resolved
// dedupEntry.result.Err can ever hold (see KVStateMachine.execute and
// Apply) -- the only values CreateSnapshot/RestoreSnapshot ever need to
// round-trip.
type resultErrCode byte

const (
	errCodeNone resultErrCode = iota
	errCodeRequestIDConflict
	errCodeStaleRequest
	errCodeEmptyKey
)

func encodeResultErr(err error) (byte, error) {
	switch err {
	case nil:
		return byte(errCodeNone), nil
	case ErrRequestIDConflict:
		return byte(errCodeRequestIDConflict), nil
	case ErrStaleRequest:
		return byte(errCodeStaleRequest), nil
	case storage.ErrEmptyKey:
		return byte(errCodeEmptyKey), nil
	default:
		return 0, fmt.Errorf("statemachine: snapshot: cannot encode unrecognized dedup result error: %v", err)
	}
}

func decodeResultErr(code byte) (error, error) {
	switch resultErrCode(code) {
	case errCodeNone:
		return nil, nil
	case errCodeRequestIDConflict:
		return ErrRequestIDConflict, nil
	case errCodeStaleRequest:
		return ErrStaleRequest, nil
	case errCodeEmptyKey:
		return storage.ErrEmptyKey, nil
	default:
		return nil, fmt.Errorf("statemachine: snapshot: unknown result error code %d: %w", code, ErrInvalidCommand)
	}
}

// CreateSnapshot implements Snapshotter. It captures every live key/value
// pair (via sm.store.Snapshot) and the entire deduplication table, holding
// sm.mu for the whole call so the result is a single, atomic point-in-time
// view -- no Apply call can be interleaved partway through.
func (sm *KVStateMachine) CreateSnapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	kv, err := sm.store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("statemachine: snapshot storage: %w", err)
	}
	if len(kv) > smMaxEntries {
		return nil, fmt.Errorf("statemachine: snapshot: %d live keys exceeds maximum of %d", len(kv), smMaxEntries)
	}

	type dedupRow struct {
		clientID string
		e        dedupEntry
	}
	rows := make([]dedupRow, 0, len(sm.dedup))
	for clientID, e := range sm.dedup {
		rows = append(rows, dedupRow{clientID: clientID, e: e})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].clientID < rows[j].clientID })
	if len(rows) > smMaxDedupEntries {
		return nil, fmt.Errorf("statemachine: snapshot: %d dedup entries exceeds maximum of %d", len(rows), smMaxDedupEntries)
	}

	size := 8 + 4 + 4 // magic + version + numKV
	for _, e := range kv {
		if len(e.Key) > smMaxKeyLen || len(e.Value) > smMaxValueLen {
			return nil, fmt.Errorf("statemachine: snapshot: key/value exceeds bounds")
		}
		size += 4 + len(e.Key) + 4 + len(e.Value)
	}
	size += 4 // numDedup
	for _, r := range rows {
		if len(r.clientID) > smMaxClientIDLen || len(r.e.key) > smMaxKeyLen || len(r.e.value) > smMaxValueLen ||
			len(r.e.result.Key) > smMaxKeyLen || len(r.e.result.Value) > smMaxValueLen {
			return nil, fmt.Errorf("statemachine: snapshot: dedup entry exceeds bounds")
		}
		size += 4 + len(r.clientID) + 8 + 1 + 4 + len(r.e.key) + 4 + len(r.e.value) + 1 + 1 + 4 + len(r.e.result.Key) + 4 + len(r.e.result.Value) + 1
	}
	size += 4 // checksum

	buf := make([]byte, size)
	off := 0
	copy(buf[off:off+8], smSnapshotMagic[:])
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], smSnapshotFormatVersion1)
	off += 4

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(kv)))
	off += 4
	for _, e := range kv {
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Key)))
		off += 4
		copy(buf[off:], e.Key)
		off += len(e.Key)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Value)))
		off += 4
		copy(buf[off:], e.Value)
		off += len(e.Value)
	}

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(rows)))
	off += 4
	for _, r := range rows {
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.clientID)))
		off += 4
		copy(buf[off:], r.clientID)
		off += len(r.clientID)

		binary.LittleEndian.PutUint64(buf[off:], r.e.requestID)
		off += 8

		buf[off] = byte(r.e.op)
		off++

		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.e.key)))
		off += 4
		copy(buf[off:], r.e.key)
		off += len(r.e.key)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.e.value)))
		off += 4
		copy(buf[off:], r.e.value)
		off += len(r.e.value)

		if r.e.result.Applied {
			buf[off] = 1
		}
		off++
		if r.e.result.Replayed {
			buf[off] = 1
		}
		off++

		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.e.result.Key)))
		off += 4
		copy(buf[off:], r.e.result.Key)
		off += len(r.e.result.Key)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.e.result.Value)))
		off += 4
		copy(buf[off:], r.e.result.Value)
		off += len(r.e.result.Value)

		errCode, err := encodeResultErr(r.e.result.Err)
		if err != nil {
			return nil, err
		}
		buf[off] = errCode
		off++
	}

	checksum := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], checksum)
	off += 4

	return buf[:off], nil
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// RestoreSnapshot implements Snapshotter. It replaces this state machine's
// entire logical state -- both storage's key/value content and the
// deduplication table -- with exactly what data describes, holding sm.mu
// for the whole call.
//
// Storage is reconciled, not merely written to: every key data describes
// is Put with its snapshot value, and every key currently in storage that
// data does *not* mention is explicitly Deleted. This matters specifically
// for InstallSnapshot on a follower that had already applied some history
// beyond what this snapshot represents being asked to roll forward past a
// point where a key it once held was since deleted on the leader -- a
// plain "Put everything the snapshot has" would leave that stale key
// behind forever. See docs/raft/phase9-snapshots.md.
func (sm *KVStateMachine) RestoreSnapshot(data []byte) error {
	kv, dedup, err := decodeKVStateMachineSnapshot(data)
	if err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	current, err := sm.store.Snapshot()
	if err != nil {
		return fmt.Errorf("statemachine: restore snapshot: read current storage state: %w", err)
	}
	want := make(map[string][]byte, len(kv))
	for _, e := range kv {
		want[string(e.Key)] = e.Value
	}
	for _, e := range current {
		if _, ok := want[string(e.Key)]; !ok {
			if err := sm.store.Delete(e.Key); err != nil {
				return fmt.Errorf("statemachine: restore snapshot: delete stale key: %w", err)
			}
		}
	}
	for _, e := range kv {
		if err := sm.store.Put(e.Key, e.Value); err != nil {
			return fmt.Errorf("statemachine: restore snapshot: put key: %w", err)
		}
	}

	sm.dedup = dedup
	return nil
}

// decodeKVStateMachineSnapshot parses and fully validates data, previously
// produced by CreateSnapshot, returning a wrapped ErrInvalidCommand for
// any structural problem: bad magic, unsupported version, truncated or
// oversized fields, an unrecognized result error code, or a checksum
// mismatch. Every length is validated before it is used to slice data or
// size an allocation.
func decodeKVStateMachineSnapshot(data []byte) ([]storage.Entry, map[string]dedupEntry, error) {
	if len(data) > smMaxSnapshotBytes {
		return nil, nil, fmt.Errorf("statemachine: snapshot of %d bytes exceeds maximum of %d: %w", len(data), smMaxSnapshotBytes, ErrInvalidCommand)
	}
	if len(data) < 8+4+4 {
		return nil, nil, fmt.Errorf("statemachine: truncated snapshot header (%d bytes): %w", len(data), ErrInvalidCommand)
	}
	if !bytes.Equal(data[0:8], smSnapshotMagic[:]) {
		return nil, nil, fmt.Errorf("statemachine: snapshot: bad magic: %w", ErrInvalidCommand)
	}
	version := binary.LittleEndian.Uint32(data[8:12])
	if version != smSnapshotFormatVersion1 {
		return nil, nil, fmt.Errorf("statemachine: snapshot: unsupported format version %d: %w", version, ErrInvalidCommand)
	}
	if len(data) < 16+4 { // +4 trailing checksum
		return nil, nil, fmt.Errorf("statemachine: truncated snapshot (%d bytes): %w", len(data), ErrInvalidCommand)
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	body := data[:len(data)-4]
	if crc32.Checksum(body, crcTable) != checksum {
		return nil, nil, fmt.Errorf("statemachine: snapshot: checksum mismatch: %w", ErrInvalidCommand)
	}

	off := 12
	readBytes := func(maxLen int) ([]byte, error) {
		if len(body)-off < 4 {
			return nil, fmt.Errorf("statemachine: snapshot: truncated length field: %w", ErrInvalidCommand)
		}
		n := binary.LittleEndian.Uint32(body[off:])
		off += 4
		if int(n) > maxLen {
			return nil, fmt.Errorf("statemachine: snapshot: field length %d exceeds maximum of %d: %w", n, maxLen, ErrInvalidCommand)
		}
		if len(body)-off < int(n) {
			return nil, fmt.Errorf("statemachine: snapshot: truncated field (want %d bytes): %w", n, ErrInvalidCommand)
		}
		var out []byte
		if n > 0 {
			out = append([]byte(nil), body[off:off+int(n)]...)
		}
		off += int(n)
		return out, nil
	}

	if len(body)-off < 4 {
		return nil, nil, fmt.Errorf("statemachine: snapshot: truncated numKV: %w", ErrInvalidCommand)
	}
	numKV := binary.LittleEndian.Uint32(body[off:])
	off += 4
	if numKV > smMaxEntries {
		return nil, nil, fmt.Errorf("statemachine: snapshot: numKV %d exceeds maximum of %d: %w", numKV, smMaxEntries, ErrInvalidCommand)
	}
	kv := make([]storage.Entry, 0, numKV)
	for i := uint32(0); i < numKV; i++ {
		key, err := readBytes(smMaxKeyLen)
		if err != nil {
			return nil, nil, err
		}
		value, err := readBytes(smMaxValueLen)
		if err != nil {
			return nil, nil, err
		}
		kv = append(kv, storage.Entry{Key: key, Value: value})
	}

	if len(body)-off < 4 {
		return nil, nil, fmt.Errorf("statemachine: snapshot: truncated numDedup: %w", ErrInvalidCommand)
	}
	numDedup := binary.LittleEndian.Uint32(body[off:])
	off += 4
	if numDedup > smMaxDedupEntries {
		return nil, nil, fmt.Errorf("statemachine: snapshot: numDedup %d exceeds maximum of %d: %w", numDedup, smMaxDedupEntries, ErrInvalidCommand)
	}
	dedup := make(map[string]dedupEntry, numDedup)
	for i := uint32(0); i < numDedup; i++ {
		clientID, err := readBytes(smMaxClientIDLen)
		if err != nil {
			return nil, nil, err
		}
		if len(body)-off < 8+1 {
			return nil, nil, fmt.Errorf("statemachine: snapshot: truncated dedup entry %d: %w", i, ErrInvalidCommand)
		}
		requestID := binary.LittleEndian.Uint64(body[off:])
		off += 8
		op := Op(body[off])
		off++
		if op != OpPut && op != OpDelete {
			return nil, nil, fmt.Errorf("statemachine: snapshot: dedup entry %d: unknown op %d: %w", i, op, ErrInvalidCommand)
		}
		key, err := readBytes(smMaxKeyLen)
		if err != nil {
			return nil, nil, err
		}
		value, err := readBytes(smMaxValueLen)
		if err != nil {
			return nil, nil, err
		}
		if len(body)-off < 2 {
			return nil, nil, fmt.Errorf("statemachine: snapshot: truncated dedup entry %d result flags: %w", i, ErrInvalidCommand)
		}
		applied := body[off] != 0
		off++
		replayed := body[off] != 0
		off++
		resultKey, err := readBytes(smMaxKeyLen)
		if err != nil {
			return nil, nil, err
		}
		resultValue, err := readBytes(smMaxValueLen)
		if err != nil {
			return nil, nil, err
		}
		if len(body)-off < 1 {
			return nil, nil, fmt.Errorf("statemachine: snapshot: truncated dedup entry %d result error code: %w", i, ErrInvalidCommand)
		}
		resultErr, err := decodeResultErr(body[off])
		if err != nil {
			return nil, nil, err
		}
		off++

		dedup[string(clientID)] = dedupEntry{
			requestID: requestID,
			op:        op,
			key:       key,
			value:     value,
			result: Result{
				Applied:  applied,
				Replayed: replayed,
				Key:      resultKey,
				Value:    resultValue,
				Err:      resultErr,
			},
		}
	}

	if off != len(body) {
		return nil, nil, fmt.Errorf("statemachine: snapshot: trailing bytes after dedup entries: %w", ErrInvalidCommand)
	}

	return kv, dedup, nil
}
