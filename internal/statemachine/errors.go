package statemachine

import "errors"

// ErrInvalidCommand is returned by Encode/DecodeCommand when a Command
// (or its encoded bytes) fails structural validation -- an unknown Op, a
// truncated or oversized field, or trailing bytes. See Command.Encode and
// DecodeCommand.
var ErrInvalidCommand = errors.New("statemachine: invalid command")

// ErrRequestIDConflict is returned, via Result.Err, when a client submits
// (ClientID, RequestID) that this state machine has already applied for a
// different command -- e.g. RequestID 42 previously meant "PUT x=10" and
// now means "PUT x=999". This is never treated as a valid duplicate: the
// original dedup record is left untouched, and the command is not applied
// a second time. See docs/raft/phase7-state-machine.md.
var ErrRequestIDConflict = errors.New("statemachine: request id conflict: client already used this request id for a different command")

// ErrStaleRequest is returned, via Result.Err, when a client's RequestID
// for ClientID is lower than the highest RequestID already recorded for
// that client -- e.g. request 10 is replayed after request 11 has already
// been applied. Because a well-behaved client numbers its requests with
// strictly increasing RequestID values, a lower RequestID arriving after a
// higher one has already been recorded can only be a stale retry of
// something already superseded; it is never re-executed, regardless of
// whether this particular state machine instance still remembers applying
// it (see [KVStateMachine], which retains only the *last* request per
// client, not full history). See docs/raft/phase7-state-machine.md for the
// full reasoning.
var ErrStaleRequest = errors.New("statemachine: stale request: an older request id was replayed after a newer one from the same client")
