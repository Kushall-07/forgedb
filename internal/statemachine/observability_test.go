package statemachine

import (
	"testing"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

func TestMetrics_ApplySuccess_PutAndGet(t *testing.T) {
	sm, _ := newTestStateMachine(t)

	commandsBefore := metrics.StateMachineCommandsTotal.Value()
	successBefore := metrics.StateMachineApplySuccessTotal.Value()

	cmd := NewPutCommand("client-1", 1, []byte("k"), []byte("v"))
	res, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !res.Applied {
		t.Fatalf("Result.Applied = false, want true")
	}

	if got := metrics.StateMachineCommandsTotal.Value(); got != commandsBefore+1 {
		t.Errorf("StateMachineCommandsTotal = %d, want %d", got, commandsBefore+1)
	}
	if got := metrics.StateMachineApplySuccessTotal.Value(); got != successBefore+1 {
		t.Errorf("StateMachineApplySuccessTotal = %d, want %d", got, successBefore+1)
	}
}

func TestMetrics_DedupHit(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	cmd := NewPutCommand("client-1", 1, []byte("k"), []byte("v"))
	if _, err := sm.Apply(cmd); err != nil {
		t.Fatalf("Apply (first): %v", err)
	}

	hitsBefore := metrics.StateMachineDedupHitsTotal.Value()
	res, err := sm.Apply(cmd) // identical retry
	if err != nil {
		t.Fatalf("Apply (retry): %v", err)
	}
	if !res.Replayed {
		t.Fatalf("Result.Replayed = false on an identical retry, want true")
	}
	if got := metrics.StateMachineDedupHitsTotal.Value(); got != hitsBefore+1 {
		t.Errorf("StateMachineDedupHitsTotal = %d, want %d", got, hitsBefore+1)
	}
}

func TestMetrics_DedupConflict(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	first := NewPutCommand("client-1", 1, []byte("k"), []byte("v1"))
	if _, err := sm.Apply(first); err != nil {
		t.Fatalf("Apply (first): %v", err)
	}

	conflictsBefore := metrics.StateMachineDedupConflictsTotal.Value()
	conflicting := NewPutCommand("client-1", 1, []byte("k"), []byte("v2")) // same RequestID, different Value
	res, err := sm.Apply(conflicting)
	if err != nil {
		t.Fatalf("Apply (conflicting): %v", err)
	}
	if res.Err != ErrRequestIDConflict {
		t.Fatalf("Result.Err = %v, want ErrRequestIDConflict", res.Err)
	}
	if got := metrics.StateMachineDedupConflictsTotal.Value(); got != conflictsBefore+1 {
		t.Errorf("StateMachineDedupConflictsTotal = %d, want %d", got, conflictsBefore+1)
	}
}

func TestMetrics_ApplyFailure_StorageFault(t *testing.T) {
	sm, cs := newTestStateMachine(t)
	cs.FailNext()

	failureBefore := metrics.StateMachineApplyFailureTotal.Value()
	cmd := NewPutCommand("client-1", 1, []byte("k"), []byte("v"))
	if _, err := sm.Apply(cmd); err == nil {
		t.Fatalf("Apply with a simulated storage fault returned nil error")
	}
	if got := metrics.StateMachineApplyFailureTotal.Value(); got != failureBefore+1 {
		t.Errorf("StateMachineApplyFailureTotal = %d, want %d", got, failureBefore+1)
	}
}
