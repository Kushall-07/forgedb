package statemachine

import (
	"encoding/binary"
	"fmt"

	"github.com/Kushall-07/forgedb/internal/raft"
)

// Op identifies which mutation a Command represents.
type Op uint8

const (
	// OpPut writes Command.Value at Command.Key, inserting or overwriting
	// whatever was there before.
	OpPut Op = iota + 1

	// OpDelete marks Command.Key as logically absent. Command.Value is
	// unused and always empty for a Delete.
	OpDelete
)

// String returns a human-readable name for op, used in error messages and
// test failures.
func (op Op) String() string {
	switch op {
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	default:
		return fmt.Sprintf("Op(%d)", uint8(op))
	}
}

// Command is ForgeDB's concrete Raft payload: a client mutation (PUT or
// DELETE) tagged with the logical client request identity
// (ClientID, RequestID) the state machine uses for deduplication -- see
// docs/raft/phase7-state-machine.md. It is what Encode/DecodeCommand
// convert to and from the opaque raft.Command bytes that Raft itself
// replicates without ever interpreting.
//
// (ClientID, RequestID) together identify one logical client request.
// RequestID alone is not sufficient -- two different clients may
// independently choose the same RequestID -- and a well-behaved client is
// expected to number its own requests with strictly increasing RequestID
// values over its lifetime, the same assumption the original Raft paper's
// client-interaction section makes.
type Command struct {
	ClientID  string
	RequestID uint64
	Op        Op
	Key       []byte
	Value     []byte // only meaningful for OpPut
}

// NewPutCommand returns a Command that writes value at key on behalf of
// (clientID, requestID). key and value are copied; the caller's slices may
// be reused or mutated afterward.
func NewPutCommand(clientID string, requestID uint64, key, value []byte) Command {
	return Command{
		ClientID:  clientID,
		RequestID: requestID,
		Op:        OpPut,
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
	}
}

// NewDeleteCommand returns a Command that deletes key on behalf of
// (clientID, requestID). key is copied; the caller's slice may be reused
// or mutated afterward.
func NewDeleteCommand(clientID string, requestID uint64, key []byte) Command {
	return Command{
		ClientID:  clientID,
		RequestID: requestID,
		Op:        OpDelete,
		Key:       append([]byte(nil), key...),
	}
}

// Bounds on Encode's individual fields, checked before allocating the
// output buffer and re-checked by Decode before it is ever used to size a
// slice -- the same defense-in-depth convention internal/raft/format.go
// and internal/storage's on-disk formats already follow. They are
// generous for ForgeDB's needs without being unbounded; a command this
// large would in any case already be rejected by internal/raft's own
// maxCommandSize (1 MiB per log entry) once Encode's output becomes a
// raft.LogEntry.Command.
const (
	maxClientIDLen = 4096    // matches internal/raft's maxVotedForLen for a peer/client identifier
	maxKeyLen      = 1 << 20 // 1 MiB
	maxValueLen    = 1 << 20 // 1 MiB
)

// commandHeaderLen is the fixed size, in bytes, of Encode's output before
// the variable-length ClientID, Key, and Value fields: op + clientIDLen +
// requestID + keyLen + valueLen.
const commandHeaderLen = 1 + 4 + 8 + 4 + 4

// Encode serializes c into the opaque byte payload a raft.Command carries,
// ready to be passed to (*raft.Node).Propose. The wire format is:
//
//	op          (1 byte)
//	clientIDLen (u32 LE) | clientID bytes
//	requestID   (u64 LE)
//	keyLen      (u32 LE) | key bytes
//	valueLen    (u32 LE) | value bytes  (0 for OpDelete)
//
// Encode validates c.Op and every length bound before allocating the
// output buffer, returning a wrapped ErrInvalidCommand instead of
// producing bytes DecodeCommand could not read back.
func (c Command) Encode() (raft.Command, error) {
	if c.Op != OpPut && c.Op != OpDelete {
		return nil, fmt.Errorf("statemachine: unknown op %s: %w", c.Op, ErrInvalidCommand)
	}
	if len(c.ClientID) > maxClientIDLen {
		return nil, fmt.Errorf("statemachine: client id of %d bytes exceeds maximum of %d: %w", len(c.ClientID), maxClientIDLen, ErrInvalidCommand)
	}
	if len(c.Key) > maxKeyLen {
		return nil, fmt.Errorf("statemachine: key of %d bytes exceeds maximum of %d: %w", len(c.Key), maxKeyLen, ErrInvalidCommand)
	}
	if len(c.Value) > maxValueLen {
		return nil, fmt.Errorf("statemachine: value of %d bytes exceeds maximum of %d: %w", len(c.Value), maxValueLen, ErrInvalidCommand)
	}

	size := commandHeaderLen + len(c.ClientID) + len(c.Key) + len(c.Value)
	buf := make([]byte, size)
	off := 0

	buf[off] = byte(c.Op)
	off++
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(c.ClientID)))
	off += 4
	copy(buf[off:], c.ClientID)
	off += len(c.ClientID)
	binary.LittleEndian.PutUint64(buf[off:], c.RequestID)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(c.Key)))
	off += 4
	copy(buf[off:], c.Key)
	off += len(c.Key)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(c.Value)))
	off += 4
	copy(buf[off:], c.Value)
	off += len(c.Value)

	return raft.Command(buf), nil
}

// DecodeCommand parses a raft.Command previously produced by
// Command.Encode, returning a wrapped ErrInvalidCommand for any
// structural problem: an unknown op, a truncated header or field, a
// length exceeding Encode's own bounds, or trailing bytes left over after
// decoding everything the header claims is present (strict EOF). Every
// length is validated before it is used to slice data, so a malformed or
// corrupted payload can never trigger an out-of-range read.
func DecodeCommand(raw raft.Command) (Command, error) {
	data := []byte(raw)
	if len(data) < commandHeaderLen {
		return Command{}, fmt.Errorf("statemachine: truncated command header (%d bytes): %w", len(data), ErrInvalidCommand)
	}

	off := 0
	op := Op(data[off])
	off++
	if op != OpPut && op != OpDelete {
		return Command{}, fmt.Errorf("statemachine: unknown op %d: %w", data[0], ErrInvalidCommand)
	}

	clientIDLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if clientIDLen > maxClientIDLen || uint64(len(data)-off) < uint64(clientIDLen)+8+4 {
		return Command{}, fmt.Errorf("statemachine: truncated or oversized client id: %w", ErrInvalidCommand)
	}
	clientID := string(data[off : off+int(clientIDLen)])
	off += int(clientIDLen)

	requestID := binary.LittleEndian.Uint64(data[off:])
	off += 8

	keyLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if keyLen > maxKeyLen || uint64(len(data)-off) < uint64(keyLen)+4 {
		return Command{}, fmt.Errorf("statemachine: truncated or oversized key: %w", ErrInvalidCommand)
	}
	var key []byte
	if keyLen > 0 {
		key = append([]byte(nil), data[off:off+int(keyLen)]...)
	}
	off += int(keyLen)

	valueLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if valueLen > maxValueLen || uint64(len(data)-off) != uint64(valueLen) {
		return Command{}, fmt.Errorf("statemachine: truncated, oversized, or trailing bytes after value: %w", ErrInvalidCommand)
	}
	var value []byte
	if valueLen > 0 {
		value = append([]byte(nil), data[off:off+int(valueLen)]...)
	}

	return Command{ClientID: clientID, RequestID: requestID, Op: op, Key: key, Value: value}, nil
}
