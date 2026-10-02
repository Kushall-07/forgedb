package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// This file defines Phase 6's on-disk representation of PersistentState,
// used by FilePersister. It follows the same conventions as
// internal/storage/manifest (magic + version + explicit little-endian
// fields + a trailing CRC-32C checksum over everything before it): the
// format is a full snapshot of the current state, not an append-only edit
// log, so recovery only ever has one file to read and no history to
// replay. See docs/raft/phase6-raft-persistence.md for the full field
// layout and the reasoning behind every bound below.
//
//	magic (8 bytes "ForgeRP1")
//	formatVersion (u32 LE)
//	currentTerm   (u64 LE)
//	votedForLen   (u32 LE) | votedFor bytes
//	numEntries    (u32 LE)
//	  [ index (u64 LE) | term (u64 LE) | commandLen (u32 LE) | command bytes ] * numEntries
//	checksum (u32 LE, CRC-32C over every preceding byte)
var raftMagic = [8]byte{'F', 'o', 'r', 'g', 'e', 'R', 'P', '1'}

const raftFormatVersion1 = 1

// Every length read back from disk is validated against one of these
// bounds before it is ever used to size an allocation, so a corrupted
// length field can never trigger an unbounded allocation attempt (see
// decodeState). They are generous for a Raft node's needs -- votedFor is
// just a peer ID, and Phase 6 does not yet bound how large a client
// command may be beyond a sane per-entry ceiling -- without being
// unbounded.
const (
	maxVotedForLen = 4096    // a node ID
	maxLogEntries  = 1 << 20 // ~1,000,000 entries
	maxCommandSize = 1 << 20 // 1 MiB per log entry's command
	maxStateSize   = 1 << 30 // 1 GiB total file size, checked before decoding
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// raftHeaderSize is the fixed size, in bytes, of the header before
// votedFor's variable-length bytes: magic + version + currentTerm +
// votedForLen.
const raftHeaderSize = 8 + 4 + 8 + 4

// encodeState serializes state into its complete on-disk representation,
// ready to be written atomically via atomicfile.Write. It validates every
// length against the bounds above before allocating the output buffer, so
// a caller that (through a bug elsewhere) tries to persist an oversized
// command gets an explicit error instead of a runaway allocation.
func encodeState(state PersistentState) ([]byte, error) {
	if len(state.VotedFor) > maxVotedForLen {
		return nil, fmt.Errorf("raft: votedFor of %d bytes exceeds maximum of %d", len(state.VotedFor), maxVotedForLen)
	}
	if len(state.Log) > maxLogEntries {
		return nil, fmt.Errorf("raft: log of %d entries exceeds maximum of %d", len(state.Log), maxLogEntries)
	}

	size := raftHeaderSize + len(state.VotedFor) + 4 // + numEntries
	for _, e := range state.Log {
		if len(e.Command) > maxCommandSize {
			return nil, fmt.Errorf("raft: log entry %d command of %d bytes exceeds maximum of %d", e.Index, len(e.Command), maxCommandSize)
		}
		size += 8 + 8 + 4 + len(e.Command)
	}
	size += 4 // checksum

	buf := make([]byte, size)
	off := 0
	copy(buf[off:off+8], raftMagic[:])
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], raftFormatVersion1)
	off += 4
	binary.LittleEndian.PutUint64(buf[off:], state.CurrentTerm)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(state.VotedFor)))
	off += 4
	copy(buf[off:], state.VotedFor)
	off += len(state.VotedFor)

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(state.Log)))
	off += 4
	for _, e := range state.Log {
		binary.LittleEndian.PutUint64(buf[off:], e.Index)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.Term)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Command)))
		off += 4
		copy(buf[off:], e.Command)
		off += len(e.Command)
	}

	checksum := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], checksum)
	off += 4

	return buf[:off], nil
}

// decodeState parses and fully validates data, previously produced by
// encodeState, returning ErrCorrupt (wrapped with detail) for any
// structural problem: a bad magic number, an unsupported version, a
// truncated header or entry, a length that exceeds the bounds above, or a
// checksum mismatch. Every length is checked before it is used to size an
// allocation or slice a buffer.
func decodeState(data []byte) (PersistentState, error) {
	if len(data) > maxStateSize {
		return PersistentState{}, fmt.Errorf("raft: persisted state of %d bytes exceeds maximum of %d: %w", len(data), maxStateSize, ErrCorrupt)
	}
	if len(data) < raftHeaderSize {
		return PersistentState{}, fmt.Errorf("raft: truncated header (%d bytes): %w", len(data), ErrCorrupt)
	}
	if !bytes.Equal(data[0:8], raftMagic[:]) {
		return PersistentState{}, fmt.Errorf("raft: bad magic: %w", ErrCorrupt)
	}
	version := binary.LittleEndian.Uint32(data[8:12])
	if version != raftFormatVersion1 {
		return PersistentState{}, fmt.Errorf("raft: unsupported format version %d: %w", version, ErrCorrupt)
	}
	if len(data) < raftHeaderSize+4 { // +4 for the trailing checksum
		return PersistentState{}, fmt.Errorf("raft: truncated file (%d bytes): %w", len(data), ErrCorrupt)
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	body := data[:len(data)-4]
	if crc32.Checksum(body, crcTable) != checksum {
		return PersistentState{}, fmt.Errorf("raft: checksum mismatch: %w", ErrCorrupt)
	}

	off := 12
	currentTerm := binary.LittleEndian.Uint64(body[off : off+8])
	off += 8

	votedForLen := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	if votedForLen > maxVotedForLen {
		return PersistentState{}, fmt.Errorf("raft: votedFor length %d exceeds maximum of %d: %w", votedForLen, maxVotedForLen, ErrCorrupt)
	}
	if len(body)-off < int(votedForLen) {
		return PersistentState{}, fmt.Errorf("raft: truncated votedFor: %w", ErrCorrupt)
	}
	votedFor := string(body[off : off+int(votedForLen)])
	off += int(votedForLen)

	if len(body)-off < 4 {
		return PersistentState{}, fmt.Errorf("raft: truncated log entry count: %w", ErrCorrupt)
	}
	numEntries := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	if numEntries > maxLogEntries {
		return PersistentState{}, fmt.Errorf("raft: log entry count %d exceeds maximum of %d: %w", numEntries, maxLogEntries, ErrCorrupt)
	}

	entries := make([]LogEntry, 0, numEntries)
	var prevTerm uint64
	var firstIndex uint64
	for i := uint32(0); i < numEntries; i++ {
		if len(body)-off < 8+8+4 {
			return PersistentState{}, fmt.Errorf("raft: truncated log entry %d: %w", i, ErrCorrupt)
		}
		index := binary.LittleEndian.Uint64(body[off : off+8])
		off += 8
		term := binary.LittleEndian.Uint64(body[off : off+8])
		off += 8
		cmdLen := binary.LittleEndian.Uint32(body[off : off+4])
		off += 4
		if cmdLen > maxCommandSize {
			return PersistentState{}, fmt.Errorf("raft: log entry %d command length %d exceeds maximum of %d: %w", i, cmdLen, maxCommandSize, ErrCorrupt)
		}
		if len(body)-off < int(cmdLen) {
			return PersistentState{}, fmt.Errorf("raft: truncated log entry %d command: %w", i, ErrCorrupt)
		}

		// The log is stored in strictly sequential index order (see Log),
		// but the *first* entry's index is no longer always 1 once Phase
		// 9 log compaction has discarded a prefix -- it must be whatever
		// index immediately follows the current snapshot boundary (see
		// docs/raft/phase9-snapshots.md). Every entry after the first
		// must still follow it by exactly 1, index must never be 0 (that
		// is the sentinel's reserved position, never a real persisted
		// entry), and a well-formed Raft log's term must never decrease
		// from one entry to the next. Any violation means the file was
		// not produced by encodeState (or was corrupted after it was).
		if i == 0 {
			if index == 0 {
				return PersistentState{}, fmt.Errorf("raft: log entry 0 has index 0, which is reserved for the sentinel: %w", ErrCorrupt)
			}
			firstIndex = index
		} else if wantIndex := firstIndex + uint64(i); index != wantIndex {
			return PersistentState{}, fmt.Errorf("raft: log entry %d has index %d, want %d: %w", i, index, wantIndex, ErrCorrupt)
		}
		if term < prevTerm {
			return PersistentState{}, fmt.Errorf("raft: log entry %d term %d is less than preceding entry's term %d: %w", i, term, prevTerm, ErrCorrupt)
		}
		prevTerm = term

		var command Command
		if cmdLen > 0 {
			command = append(Command(nil), body[off:off+int(cmdLen)]...)
		}
		off += int(cmdLen)

		entries = append(entries, LogEntry{Index: index, Term: term, Command: command})
	}
	if off != len(body) {
		return PersistentState{}, fmt.Errorf("raft: trailing bytes after log entries: %w", ErrCorrupt)
	}
	if numEntries > 0 && prevTerm > currentTerm {
		return PersistentState{}, fmt.Errorf("raft: last log entry term %d exceeds currentTerm %d: %w", prevTerm, currentTerm, ErrCorrupt)
	}

	return PersistentState{CurrentTerm: currentTerm, VotedFor: votedFor, Log: entries}, nil
}

// This section defines Phase 9's on-disk representation of Snapshot, used
// by FilePersister.SaveSnapshot/LoadSnapshot. It follows exactly the same
// conventions as PersistentState above (magic + version + explicit
// little-endian fields + a trailing CRC-32C checksum over everything
// before it) and for the same reasons: a full snapshot of current state,
// not an edit log, so recovery only ever has one file to read. See
// docs/raft/phase9-snapshots.md.
//
//	magic (8 bytes "ForgeSS1")
//	formatVersion (u32 LE)
//	lastIncludedIndex (u64 LE)
//	lastIncludedTerm  (u64 LE)
//	dataLen (u32 LE) | data bytes
//	checksum (u32 LE, CRC-32C over every preceding byte)
var snapshotMagic = [8]byte{'F', 'o', 'r', 'g', 'e', 'S', 'S', '1'}

const snapshotFormatVersion1 = 1

// maxSnapshotDataSize bounds the opaque state-machine payload a snapshot
// may carry, checked before it is ever used to size an allocation (see
// decodeSnapshot). 256 MiB is generous for the KV state Phase 9's
// in-memory MemTable-backed storage engine can hold in any realistic test
// or demo scenario, without being unbounded.
const (
	maxSnapshotDataSize = 1 << 28 // 256 MiB
	maxSnapshotFileSize = 1 << 29 // 512 MiB total file size, checked before decoding
)

// snapshotHeaderSize is the fixed size, in bytes, of encodeSnapshot's
// output before Data's variable-length bytes: magic + version +
// lastIncludedIndex + lastIncludedTerm + dataLen.
const snapshotHeaderSize = 8 + 4 + 8 + 8 + 4

// encodeSnapshot serializes snap into its complete on-disk representation,
// ready to be written atomically via atomicfile.Write. It validates Data's
// length against maxSnapshotDataSize before allocating the output buffer.
func encodeSnapshot(snap Snapshot) ([]byte, error) {
	if len(snap.Data) > maxSnapshotDataSize {
		return nil, fmt.Errorf("raft: snapshot data of %d bytes exceeds maximum of %d", len(snap.Data), maxSnapshotDataSize)
	}

	size := snapshotHeaderSize + len(snap.Data) + 4 // + checksum
	buf := make([]byte, size)
	off := 0
	copy(buf[off:off+8], snapshotMagic[:])
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], snapshotFormatVersion1)
	off += 4
	binary.LittleEndian.PutUint64(buf[off:], snap.LastIncludedIndex)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], snap.LastIncludedTerm)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(snap.Data)))
	off += 4
	copy(buf[off:], snap.Data)
	off += len(snap.Data)

	checksum := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], checksum)
	off += 4

	return buf[:off], nil
}

// decodeSnapshot parses and fully validates data, previously produced by
// encodeSnapshot, returning ErrCorrupt (wrapped with detail) for any
// structural problem: a bad magic number, an unsupported version, a
// truncated header or payload, a length exceeding maxSnapshotDataSize, or
// a checksum mismatch. The data length is validated before it is ever
// used to size an allocation or slice a buffer, so a corrupted or
// maliciously crafted length can never trigger an unbounded allocation or
// out-of-range read.
func decodeSnapshot(data []byte) (Snapshot, error) {
	if len(data) > maxSnapshotFileSize {
		return Snapshot{}, fmt.Errorf("raft: persisted snapshot of %d bytes exceeds maximum of %d: %w", len(data), maxSnapshotFileSize, ErrCorrupt)
	}
	if len(data) < snapshotHeaderSize+4 { // +4 for the trailing checksum
		return Snapshot{}, fmt.Errorf("raft: truncated snapshot (%d bytes): %w", len(data), ErrCorrupt)
	}
	if !bytes.Equal(data[0:8], snapshotMagic[:]) {
		return Snapshot{}, fmt.Errorf("raft: snapshot: bad magic: %w", ErrCorrupt)
	}
	version := binary.LittleEndian.Uint32(data[8:12])
	if version != snapshotFormatVersion1 {
		return Snapshot{}, fmt.Errorf("raft: snapshot: unsupported format version %d: %w", version, ErrCorrupt)
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	body := data[:len(data)-4]
	if crc32.Checksum(body, crcTable) != checksum {
		return Snapshot{}, fmt.Errorf("raft: snapshot: checksum mismatch: %w", ErrCorrupt)
	}

	off := 12
	lastIncludedIndex := binary.LittleEndian.Uint64(body[off : off+8])
	off += 8
	lastIncludedTerm := binary.LittleEndian.Uint64(body[off : off+8])
	off += 8

	if len(body)-off < 4 {
		return Snapshot{}, fmt.Errorf("raft: snapshot: truncated data length: %w", ErrCorrupt)
	}
	dataLen := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	if dataLen > maxSnapshotDataSize {
		return Snapshot{}, fmt.Errorf("raft: snapshot: data length %d exceeds maximum of %d: %w", dataLen, maxSnapshotDataSize, ErrCorrupt)
	}
	if uint64(len(body)-off) != uint64(dataLen) {
		return Snapshot{}, fmt.Errorf("raft: snapshot: truncated or trailing bytes after data (declared %d, have %d): %w", dataLen, len(body)-off, ErrCorrupt)
	}

	var payload []byte
	if dataLen > 0 {
		payload = append([]byte(nil), body[off:off+int(dataLen)]...)
	}

	return Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: payload}, nil
}
