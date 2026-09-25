// Package wal implements ForgeDB's write-ahead log: a durable,
// append-only, binary log of Put/Delete Records. It knows nothing about
// MemTable, Raft, replication, or networking. It is a self-contained,
// reusable storage primitive: a Store implementation appends every
// mutation here and syncs it before applying the mutation in memory, and
// rebuilds its in-memory state by replaying this log on restart.
package wal

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrCorrupt indicates the WAL contains a complete, fully-framed record
// whose checksum does not match its contents, or a header with a value
// that can never be valid (an unknown operation type, or a length outside
// the bounds encodeRecord enforces). It is distinct from a torn tail (see
// Replay): a torn tail is silently truncated, but ErrCorrupt is always
// returned to the caller rather than skipped, since silently discarding a
// corrupt record in the middle of the log could hide data loss.
var ErrCorrupt = errors.New("wal: corrupt record")

// WAL is an append-only, durable log of Records backed by a single file.
// A WAL is safe for concurrent use.
type WAL struct {
	mu sync.Mutex
	f  *os.File
}

// Open opens the WAL file at path for appending and replay, creating it
// (and any missing parent directories) if it does not already exist. It
// never truncates an existing file. Open does not read or replay any
// existing content; call Replay explicitly to do that.
func Open(path string) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("wal: create directory %s: %w", dir, err)
		}
	}
	// O_APPEND is deliberately not used here: on Windows, Go only grants
	// FILE_APPEND_DATA (not FILE_WRITE_DATA) access for it, which is
	// enough to write but not enough to Truncate -- and Replay must be
	// able to truncate a torn tail. Instead, Replay explicitly seeks the
	// file to the end of the last valid record before returning, and
	// every subsequent Append writes at that (single-writer, mutex-
	// serialized) file position.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &WAL{f: f}, nil
}

// Replay reads every record from the beginning of the WAL, in the order
// they were appended, invoking fn once per valid record with records
// still in append order. It must be called, if at all, before any Append
// on this WAL, and at most once.
//
// Replay distinguishes three cases when it cannot produce a normal
// record:
//
//   - Clean end of file at a record boundary: Replay returns nil. This is
//     the ordinary case when nothing is torn.
//   - A torn tail: the file ends partway through what would be the next
//     record (an incomplete header, or a header whose declared key/value
//     bytes are not all present). This is the expected shape of damage
//     from a crash during the write of a record, since a record is
//     written as a single []byte in one Write call whose bytes can still
//     be split across the write/fsync boundary by the OS or disk.
//     Replay's policy is to truncate the file at the last complete record
//     and return nil: the torn, never-synced record is discarded, which
//     is safe because Append+Sync (the durability boundary; see
//     MemStore) never reported success for it.
//   - Corruption: a complete, fully-framed record (every declared byte
//     present) whose checksum does not match, or a header with an
//     invalid operation type or an out-of-bounds length. Replay stops
//     immediately and returns an error wrapping ErrCorrupt. It never
//     skips a corrupt record and continues past it, since doing so could
//     silently resurrect stale state or drop data.
//
// If fn returns an error, Replay stops and returns it.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek to start for replay: %w", err)
	}
	r := bufio.NewReader(w.f)

	var offset int64
	headerBuf := make([]byte, headerSize)

	for {
		n, err := io.ReadFull(r, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) && n == 0 {
				// Clean end of file at a record boundary. Reposition the
				// file for subsequent Appends: bufio may have read ahead
				// of what ReadFull consumed logically, so w.f's cursor
				// cannot be trusted to already sit at offset.
				return w.seekTo(offset)
			}
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return w.truncateTornTail(offset)
			}
			return fmt.Errorf("wal: read record header at offset %d: %w", offset, err)
		}

		hdr, herr := decodeHeader(headerBuf)
		if herr != nil {
			return fmt.Errorf("wal: invalid header at offset %d: %v: %w", offset, herr, ErrCorrupt)
		}

		payload := make([]byte, int(hdr.keyLen)+int(hdr.valLen))
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return w.truncateTornTail(offset)
			}
			return fmt.Errorf("wal: read record payload at offset %d: %w", offset, err)
		}

		body := make([]byte, len(headerBuf)-4+len(payload))
		copy(body, headerBuf[4:])
		copy(body[len(headerBuf)-4:], payload)
		if crc32.Checksum(body, crcTable) != hdr.checksum {
			return fmt.Errorf("wal: checksum mismatch at offset %d: %w", offset, ErrCorrupt)
		}

		rec := Record{
			Type:  hdr.opType,
			Key:   payload[:hdr.keyLen],
			Value: payload[hdr.keyLen:],
		}
		if err := fn(rec); err != nil {
			return fmt.Errorf("wal: apply record at offset %d: %w", offset, err)
		}

		offset += int64(headerSize) + int64(len(payload))
	}
}

// truncateTornTail truncates the WAL file to offset bytes, discarding an
// incomplete final record so the file ends exactly at the last complete,
// valid record, then positions the file for subsequent Appends.
func (w *WAL) truncateTornTail(offset int64) error {
	if err := w.f.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate torn tail at offset %d: %w", offset, err)
	}
	return w.seekTo(offset)
}

// seekTo positions the file at offset, where the next Append will write.
func (w *WAL) seekTo(offset int64) error {
	if _, err := w.f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek to offset %d: %w", offset, err)
	}
	return nil
}

// Append encodes rec and writes it to the WAL. Append alone does not
// guarantee the record survives a crash: the write may still be sitting
// in the operating system's page cache rather than on stable storage.
// Call Sync to establish that durability boundary.
func (w *WAL) Append(rec Record) error {
	data, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(data); err != nil {
		return fmt.Errorf("wal: append: %w", err)
	}
	return nil
}

// Sync flushes the WAL file to stable storage (fsync). Only after Sync
// returns nil are the records Appended before it guaranteed to survive a
// process crash or OS restart; Append by itself only guarantees the bytes
// were handed to the operating system, which may still be buffering them
// in memory.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// Close closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}
	return nil
}
