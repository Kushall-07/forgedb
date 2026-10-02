package correctness

import (
	"encoding/json"
	"fmt"
	"os"
)

// This file implements §43/§44 of the Phase 11 brief: a failed History
// must be representable as data, so it can be saved and replayed without
// re-deriving it. It deliberately uses plain JSON (§43: "a large custom
// serialization framework" is explicitly out of scope) and is the
// counterpart, for a single concrete History, to what EventLog.Seed
// already gives chaos's own randomized scenario (see
// docs/chaos/phase10-chaos-testing.md §13) -- replaying a randomized
// correctness run (random.go) should normally just mean re-running
// RunRandomCorrectness with the same seed, which reproduces the identical
// script deterministically; SaveHistory/LoadHistory exist for the
// narrower case of wanting the exact recorded operation history itself
// as a standalone artifact (e.g. to attach to a bug report, or to replay
// purely through the checker without re-running a live cluster at all).

// jsonOperation mirrors Operation but with Err as a plain string, since
// the error interface itself cannot round-trip through JSON.
type jsonOperation struct {
	ID        int    `json:"id"`
	ClientID  string `json:"client_id"`
	RequestID uint64 `json:"request_id"`
	Kind      string `json:"kind"`
	Node      string `json:"node"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Invoke    int    `json:"invoke"`
	Complete  int    `json:"complete,omitempty"`
	Outcome   string `json:"outcome"`
	Found     bool   `json:"found,omitempty"`
	Result    string `json:"result,omitempty"`
	Err       string `json:"err,omitempty"`
}

func kindToString(k OpKind) string {
	return k.String()
}

func kindFromString(s string) (OpKind, error) {
	switch s {
	case "Get":
		return OpGet, nil
	case "Put":
		return OpPut, nil
	case "Delete":
		return OpDelete, nil
	default:
		return 0, fmt.Errorf("correctness: unknown operation kind %q", s)
	}
}

func outcomeToString(o Outcome) string {
	return o.String()
}

func outcomeFromString(s string) (Outcome, error) {
	switch s {
	case "Incomplete":
		return OutcomeIncomplete, nil
	case "Failed":
		return OutcomeFailed, nil
	case "OK":
		return OutcomeOK, nil
	default:
		return 0, fmt.Errorf("correctness: unknown outcome %q", s)
	}
}

// replayError is the concrete error type a deserialized Operation's Err
// field gets, when it was non-empty. It only ever carries the original
// error's message (errors.Is against the original sentinel will not
// match a replayed History) -- good enough for diagnostics and reporting,
// which is replay's only purpose; see the package doc.
type replayError string

func (e replayError) Error() string { return string(e) }

func toJSONHistory(h History) []jsonOperation {
	out := make([]jsonOperation, 0, len(h.Ops))
	for _, op := range h.Ops {
		j := jsonOperation{
			ID:        op.ID,
			ClientID:  op.ClientID,
			RequestID: op.RequestID,
			Kind:      kindToString(op.Kind),
			Node:      op.Node,
			Key:       op.Key,
			Value:     op.Value,
			Invoke:    op.Invoke,
			Outcome:   outcomeToString(op.Outcome),
			Found:     op.Found,
			Result:    op.Result,
		}
		if op.Outcome != OutcomeIncomplete {
			j.Complete = op.Complete
		}
		if op.Err != nil {
			j.Err = op.Err.Error()
		}
		out = append(out, j)
	}
	return out
}

func fromJSONHistory(in []jsonOperation) (History, error) {
	ops := make([]Operation, 0, len(in))
	for _, j := range in {
		kind, err := kindFromString(j.Kind)
		if err != nil {
			return History{}, err
		}
		outcome, err := outcomeFromString(j.Outcome)
		if err != nil {
			return History{}, err
		}
		op := Operation{
			ID:        j.ID,
			ClientID:  j.ClientID,
			RequestID: j.RequestID,
			Kind:      kind,
			Node:      j.Node,
			Key:       j.Key,
			Value:     j.Value,
			Invoke:    j.Invoke,
			Complete:  j.Complete,
			Outcome:   outcome,
			Found:     j.Found,
			Result:    j.Result,
		}
		if j.Err != "" {
			op.Err = replayError(j.Err)
		}
		ops = append(ops, op)
	}
	return History{Ops: ops}, nil
}

// SaveHistory writes h to path as indented JSON, for a failed history to
// be attached to a bug report or re-loaded later via LoadHistory without
// re-running a live cluster at all.
func SaveHistory(h History, path string) error {
	data, err := json.MarshalIndent(toJSONHistory(h), "", "  ")
	if err != nil {
		return fmt.Errorf("correctness: marshal history: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("correctness: write history: %w", err)
	}
	return nil
}

// LoadHistory reads a History previously written by SaveHistory. Every
// Operation.Err it reconstructs is a replayError carrying only the
// original error's message -- see replayError's doc comment -- which is
// sufficient for Check (which only inspects Err's presence/absence, never
// its identity) and for Report.
func LoadHistory(path string) (History, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return History{}, fmt.Errorf("correctness: read history: %w", err)
	}
	var in []jsonOperation
	if err := json.Unmarshal(data, &in); err != nil {
		return History{}, fmt.Errorf("correctness: unmarshal history: %w", err)
	}
	return fromJSONHistory(in)
}
