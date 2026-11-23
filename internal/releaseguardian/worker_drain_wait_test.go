package releaseguardian

import (
	"context"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestPersistedWorkerDrainResumesOnlySameCandidate(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(t, testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now), otherDigest)
	s.Bundle.Release = declarativerelease.PlanRelease{ComponentID: s.Key.Component, Delivery: &declarativerelease.Delivery{Writer: "guardian", Group: s.Key.Group}, Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{}}}
	s.Bundle.Prepared.ConfigSHA, s.Bundle.Prepared.PlanDigest = s.Record.ConfigSHA, testDigest
	store := &fakeStore{snapshot: s}
	exec := &fakeExecutor{rollout: &ExecutionReceipt{Status: "failed-no-write", Reason: "inactive-worker-before-write", RecordDigest: s.Record.RecordDigest, ReceiptDigest: testDigest, WorkerDrainPlanDigest: testDigest}}
	c, err := NewController(ModeWrite, store, exec)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	if err := c.Reconcile(context.Background(), s.Key); err != nil {
		t.Fatal(err)
	}
	wait := store.status
	s.PreviousStatus = &wait
	if !WaitingForWorkerDrain(s) || store.status.State != StateRolloutPending || exec.rollouts != 1 || exec.rollbacks != 0 {
		t.Fatalf("wait=%+v", wait)
	}
	store.snapshot = s
	// A new controller process must recover the wait from stored state.
	c, err = NewController(ModeWrite, store, exec)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	if err := c.Reconcile(context.Background(), s.Key); err != nil || exec.rollouts != 1 {
		t.Fatalf("extra rollout before readiness interval, %v", err)
	}
	stale := s
	stale.Bundle.Prepared.PlanDigest = otherDigest
	if WaitingForWorkerDrain(stale) {
		t.Fatal("wait was reused for a changed plan")
	}
	now = now.Add(time.Minute)
	store.snapshot.Health = testHealth(HealthHealthy, HealthHealthy, HealthHealthy, now)
	exec.rollout = &ExecutionReceipt{Status: "verified", RecordDigest: s.Record.RecordDigest, ReceiptDigest: otherDigest}
	if err := c.Reconcile(context.Background(), s.Key); err != nil || exec.rollouts != 2 || store.status.State != StateVerifying || store.status.WorkerDrainPlanDigest != "" {
		t.Fatalf("failed to resume: %+v, %v", store.status, err)
	}
}

func TestWorkerDrainReceiptMustProveNoWritesAndExactPlan(t *testing.T) {
	configureInClusterExecutorFixture(t)
	s := processSnapshot(t)
	s.Bundle.Prepared.PlanDigest = testDigest
	s.Bundle.Release = declarativerelease.PlanRelease{Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{}}, Delivery: &declarativerelease.Delivery{Writer: "guardian", Group: s.Key.Group}}
	wait := declarativerelease.WorkerDrainWait(s.Bundle.Prepared, declarativerelease.ErrInactiveWorkerDraining)
	for _, tc := range []struct {
		name   string
		change func(*declarativerelease.ExecutionResult)
	}{
		{"known wait", nil},
		{"partial forward", func(r *declarativerelease.ExecutionResult) { r.ForwardApplyCount = 1 }},
		{"rollback", func(r *declarativerelease.ExecutionResult) { r.LKGApplyCount = 1 }},
		{"other plan", func(r *declarativerelease.ExecutionResult) { r.ExecutionPlanDigest = otherDigest }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := wait
			if tc.change != nil {
				tc.change(&result)
				result = resealProcessResult(t, result)
			}
			binary := writeExecutorFixture(t, "execute", s.Record.RecordDigest, result)
			executor, err := NewProcessExecutor(binary, "pod-uid")
			if err != nil {
				t.Fatal(err)
			}
			received, err := executor.Rollout(context.Background(), s)
			if (err != nil) != (tc.change != nil) || err == nil && received.WorkerDrainPlanDigest != testDigest {
				t.Fatalf("receipt=%+v error=%v", received, err)
			}
		})
	}
}
