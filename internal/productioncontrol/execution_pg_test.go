package productioncontrol

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

type testExecutionEvidence struct {
	enrollment string
	terminal   string
	fenced     bool
}

func (v *testExecutionEvidence) VerifyBaseline(_ context.Context, b Baseline) error {
	if b.EvidenceDigest != v.enrollment {
		return errors.New("resource is not independently enrolled")
	}
	return nil
}

func (v *testExecutionEvidence) VerifyTerminal(_ context.Context, _ Claim, _ Intent, r ExecutionResult) error {
	if !v.fenced || r.EvidenceDigest != v.terminal {
		return errors.New("no conclusive terminal evidence")
	}
	return nil
}

func testExecutionLifecycle(t *testing.T, ctx context.Context, j, restarted *Journal) {
	t.Helper()
	intent, payload, private, trusted, now := approvalFixture(t)
	j1, err := NewJournal(j.db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	j2, err := NewJournal(restarted.db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	j1.now = func() time.Time { return now }
	j2.now = j1.now
	intent.RequestID = "dispatch-a"
	intent.RollbackDigest = intent.CurrentDigest
	baseline := Baseline{intent.scope(), Version{intent.Generation, intent.CurrentDigest}, digest([]byte("test enrollment"))}
	evidence := &testExecutionEvidence{terminal: digest([]byte("test terminal proof"))}
	w1, err := NewExecutionJournal(j1, evidence)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := NewExecutionJournal(j2, evidence)
	if err != nil {
		t.Fatal(err)
	}
	signAndRecord := func(intent Intent) (string, string) {
		t.Helper()
		record, err := j1.RecordIntent(ctx, "operator-a", intent, payload)
		if err != nil {
			t.Fatal(err)
		}
		approval, err := SignApproval(intent, "owner-a", private, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		approved, err := j1.RecordApproval(ctx, approval)
		if err != nil {
			t.Fatal(err)
		}
		return record.Digest, approved.Digest
	}
	first, firstApproval := signAndRecord(intent)
	if _, _, err := w1.ReserveExecution(ctx, "worker-a", first, firstApproval); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("reservation without verified resource gave %v", err)
	}
	if _, err := w1.AnchorResource(ctx, "observer-a", baseline); err == nil {
		t.Fatal("anchored a resource without provider evidence")
	}
	evidence.enrollment = baseline.EvidenceDigest
	if inserted, err := w1.AnchorResource(ctx, "observer-a", baseline); err != nil || !inserted {
		t.Fatalf("anchor: inserted=%v err=%v", inserted, err)
	}
	j2.now = func() time.Time { return now.Add(time.Hour) }
	if _, _, err := w2.ReserveExecution(ctx, "worker-b", first, firstApproval); err == nil {
		t.Fatal("dispatch accepted an expired stored approval")
	}
	j2.now = j1.now
	other := intent
	other.RequestID = "dispatch-b"
	second, secondApproval := signAndRecord(other)
	type attempt struct {
		w        *ExecutionJournal
		digest   string
		approval string
	}
	attempts := []attempt{{w1, first, firstApproval}, {w2, second, secondApproval}}
	won := make(chan Claim, len(attempts))
	var wg sync.WaitGroup
	for _, a := range attempts {
		wg.Go(func() {
			claim, created, err := a.w.ReserveExecution(ctx, "worker-a", a.digest, a.approval)
			if errors.Is(err, ErrResourceBusy) {
				return
			}
			if err != nil || !created {
				t.Errorf("competing claim: created=%v err=%v", created, err)
				return
			}
			won <- claim
		})
	}
	wg.Wait()
	close(won)
	if n := len(won); n != 1 {
		t.Fatalf("resource had %d competing writers authorized", n)
	}
	claim := <-won
	loser, loserApproval := first, firstApproval
	if claim.IntentDigest == first {
		loser, loserApproval = second, secondApproval
	}
	handle, result, err := w2.Execution(ctx, baseline.Scope, claim.IntentDigest)
	if err != nil || handle != claim || result != nil || claim.Fence != 1 {
		t.Fatalf("restart did not read the pending claim: %+v result=%+v err=%v", handle, result, err)
	}
	if err := w2.VerifyOpenClaim(ctx, claim); err != nil {
		t.Fatalf("current approved pending claim was rejected: %v", err)
	}
	stale := claim
	stale.Fence++
	if err := w2.VerifyOpenClaim(ctx, stale); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("an unrelated fence was accepted for a provider write: %v", err)
	}
	j2.now = func() time.Time { return now.Add(48 * time.Hour) }
	if err := w2.VerifyOpenClaim(ctx, claim); err == nil {
		t.Fatal("expired pending claim was accepted for a new provider write")
	}
	replay, created, err := w2.ReserveExecution(ctx, "worker-b", claim.IntentDigest, claim.ApprovalDigest)
	if err != nil || created || replay != claim {
		t.Fatalf("restart reissued or lost authority: %+v created=%v err=%v", replay, created, err)
	}
	if _, _, err := w2.ReserveExecution(ctx, "worker-b", loser, loserApproval); !errors.Is(err, ErrResourceBusy) {
		t.Fatalf("pending claim expired or permitted a second writer: %v", err)
	}
	j2.now = j1.now
	completed := ExecutionResult{Applied, Version{intent.Generation + 1, intent.TargetDigest}, evidence.terminal}
	if _, err := w2.CompleteExecution(ctx, "worker-b", claim, completed); err == nil {
		t.Fatal("unfenced observation was accepted as terminal")
	}
	pending, err := w2.Resource(ctx, baseline.Scope)
	if err != nil || pending.LastVerified != baseline.Verified || pending.Pending != claim.IntentDigest {
		t.Fatalf("unknown result changed current state: %+v err=%v", pending, err)
	}
	evidence.fenced = true
	if _, err := w2.CompleteExecution(ctx, "worker-b", stale, completed); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale fence should be rejected, got %v", err)
	}
	if inserted, err := w2.CompleteExecution(ctx, "worker-b", claim, completed); err != nil || !inserted {
		t.Fatalf("verified application not persisted: inserted=%v err=%v", inserted, err)
	}
	if err := w2.VerifyOpenClaim(ctx, claim); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("terminal claim was accepted for another provider write: %v", err)
	}
	if inserted, err := w1.CompleteExecution(ctx, "worker-a", claim, completed); err != nil || inserted {
		t.Fatalf("same terminal result was reinserted: inserted=%v err=%v", inserted, err)
	}
	conflicting := completed
	conflicting.EvidenceDigest = baseline.EvidenceDigest
	if _, err := w1.CompleteExecution(ctx, "worker-a", claim, conflicting); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("execution result was replaced: %v", err)
	}
	if _, _, err := w2.ReserveExecution(ctx, "worker-b", loser, loserApproval); !errors.Is(err, ErrResourceDrift) {
		t.Fatalf("old predecessor still actionable: %v", err)
	}
	intent.RequestID = "dispatch-successor"
	intent.Generation = completed.Verified.Generation
	intent.CurrentDigest = completed.Verified.Digest
	intent.RollbackDigest = completed.Verified.Digest
	intent.TargetDigest = digest([]byte("next target"))
	next, nextApproval := signAndRecord(intent)
	nextClaim, created, err := w2.ReserveExecution(ctx, "worker-b", next, nextApproval)
	if err != nil || !created || nextClaim.Fence != claim.Fence+1 {
		t.Fatalf("successor fence: %+v created=%v err=%v", nextClaim, created, err)
	}
	pending, err = w2.Resource(ctx, baseline.Scope)
	if err != nil || pending.LastVerified != completed.Verified || pending.Pending != next {
		t.Fatalf("successor lost last verified target: %+v err=%v", pending, err)
	}
	restored := ExecutionResult{Restored, Version{intent.Generation + 2, intent.RollbackDigest}, evidence.terminal}
	if _, err := w2.CompleteExecution(ctx, "worker-b", nextClaim, restored); err != nil {
		t.Fatal(err)
	}
	settled, err := w1.Resource(ctx, baseline.Scope)
	if err != nil || settled.LastVerified != restored.Verified || settled.Pending != "" {
		t.Fatalf("verified rollback did not settle scope: %+v err=%v", settled, err)
	}
}
