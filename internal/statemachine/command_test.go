package statemachine

import (
	"bytes"
	"errors"
	"testing"
)

func TestCommand_EncodeDecode_Put_RoundTrips(t *testing.T) {
	cmd := NewPutCommand("client-a", 42, []byte("key1"), []byte("value1"))

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.ClientID != cmd.ClientID || got.RequestID != cmd.RequestID || got.Op != cmd.Op {
		t.Fatalf("DecodeCommand = %+v, want %+v", got, cmd)
	}
	if !bytes.Equal(got.Key, cmd.Key) || !bytes.Equal(got.Value, cmd.Value) {
		t.Fatalf("DecodeCommand key/value = %q/%q, want %q/%q", got.Key, got.Value, cmd.Key, cmd.Value)
	}
}

func TestCommand_EncodeDecode_Delete_RoundTrips(t *testing.T) {
	cmd := NewDeleteCommand("client-b", 7, []byte("key1"))

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Op != OpDelete || got.ClientID != "client-b" || got.RequestID != 7 {
		t.Fatalf("DecodeCommand = %+v", got)
	}
	if !bytes.Equal(got.Key, []byte("key1")) {
		t.Fatalf("DecodeCommand key = %q, want key1", got.Key)
	}
	if len(got.Value) != 0 {
		t.Fatalf("DecodeCommand value = %q, want empty", got.Value)
	}
}

func TestCommand_EncodeDecode_EmptyValue(t *testing.T) {
	cmd := NewPutCommand("client-a", 1, []byte("key"), nil)

	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if len(got.Value) != 0 {
		t.Fatalf("DecodeCommand value = %q, want empty", got.Value)
	}
}

func TestCommand_Encode_UnknownOpRejected(t *testing.T) {
	cmd := Command{ClientID: "c", RequestID: 1, Op: Op(99), Key: []byte("k")}
	if _, err := cmd.Encode(); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("Encode with unknown op: got %v, want ErrInvalidCommand", err)
	}
}

func TestDecodeCommand_TruncatedHeaderRejected(t *testing.T) {
	if _, err := DecodeCommand([]byte{1, 2, 3}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("DecodeCommand on truncated bytes: got %v, want ErrInvalidCommand", err)
	}
}

func TestDecodeCommand_UnknownOpRejected(t *testing.T) {
	cmd := NewPutCommand("c", 1, []byte("k"), []byte("v"))
	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	corrupted := append([]byte(nil), encoded...)
	corrupted[0] = 99 // overwrite the op byte with an unrecognized value

	if _, err := DecodeCommand(corrupted); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("DecodeCommand with corrupted op byte: got %v, want ErrInvalidCommand", err)
	}
}

func TestDecodeCommand_TrailingBytesRejected(t *testing.T) {
	cmd := NewPutCommand("c", 1, []byte("k"), []byte("v"))
	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	withTrailingGarbage := append(append([]byte(nil), encoded...), 0xFF)

	if _, err := DecodeCommand(withTrailingGarbage); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("DecodeCommand with trailing bytes: got %v, want ErrInvalidCommand", err)
	}
}
