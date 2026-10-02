package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestSnapshotFormat_EncodeDecode_RoundTrip(t *testing.T) {
	snap := Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 7, Data: []byte("some opaque state machine bytes")}
	data, err := encodeSnapshot(snap)
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(data)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if got.LastIncludedIndex != 42 || got.LastIncludedTerm != 7 || string(got.Data) != string(snap.Data) {
		t.Fatalf("round trip = %+v, want %+v", got, snap)
	}
}

func TestSnapshotFormat_EmptyData_RoundTrip(t *testing.T) {
	snap := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: nil}
	data, err := encodeSnapshot(snap)
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	got, err := decodeSnapshot(data)
	if err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	if len(got.Data) != 0 {
		t.Fatalf("Data = %v, want empty", got.Data)
	}
}

func TestSnapshotFormat_BadMagic_Rejected(t *testing.T) {
	data, err := encodeSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	data[0] ^= 0xFF
	if _, err := decodeSnapshot(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeSnapshot with bad magic = %v, want ErrCorrupt", err)
	}
}

func TestSnapshotFormat_UnsupportedVersion_Rejected(t *testing.T) {
	data, err := encodeSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	binary.LittleEndian.PutUint32(data[8:12], 999)
	if _, err := decodeSnapshot(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeSnapshot with unsupported version = %v, want ErrCorrupt", err)
	}
}

func TestSnapshotFormat_Truncated_Rejected(t *testing.T) {
	data, err := encodeSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("hello world")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	truncated := data[:len(data)/2]
	if _, err := decodeSnapshot(truncated); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeSnapshot truncated = %v, want ErrCorrupt", err)
	}
}

func TestSnapshotFormat_ChecksumMismatch_Rejected(t *testing.T) {
	data, err := encodeSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2, Data: []byte("payload")})
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	data[20] ^= 0x01 // mutate a data byte, not the trailing checksum
	if _, err := decodeSnapshot(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeSnapshot with mutated body = %v, want ErrCorrupt", err)
	}
}

func TestSnapshotFormat_OversizedData_RejectedAtEncode(t *testing.T) {
	_, err := encodeSnapshot(Snapshot{Data: make([]byte, maxSnapshotDataSize+1)})
	if err == nil {
		t.Fatalf("encodeSnapshot with oversized data: want error, got nil")
	}
}

func TestSnapshotFormat_MaliciousDataLength_RejectedWithoutHugeAllocation(t *testing.T) {
	var body []byte
	body = append(body, snapshotMagic[:]...)
	body = append(body, 1, 0, 0, 0)             // version
	body = append(body, make([]byte, 8)...)     // lastIncludedIndex
	body = append(body, make([]byte, 8)...)     // lastIncludedTerm
	body = append(body, 0xFF, 0xFF, 0xFF, 0xFF) // dataLen = huge

	checksum := crc32.Checksum(body, crcTable)
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, checksum)
	data := append(body, out...)

	if _, err := decodeSnapshot(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeSnapshot with huge data length = %v, want ErrCorrupt", err)
	}
}
