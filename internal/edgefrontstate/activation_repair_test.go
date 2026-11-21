package edgefrontstate

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func seedPartialRepair(t *testing.T) (string, ActivationRepairCASRequest, ActivationState, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	apply := func(operation string, expected uint64, from, to string, rollbackOf uint64, at time.Time) ActivationState {
		t.Helper()
		request := activationRequest(operation, expected, from, to, "bundle-"+to, rollbackOf)
		request.WorkerSourceCommit, request.WorkerImageDigest = strings.Repeat(to, 40), "sha256:"+strings.Repeat(to, 64)
		receipt, err := ApplyActivationCAS(path, request, at)
		if err != nil {
			t.Fatal(err)
		}
		return receipt.Current
	}
	prior := apply(ActivationOperationInit, 0, "a", "a", 0, now)
	current := apply(ActivationOperationPromote, 1, "a", "b", 0, now.Add(time.Second))
	identity := func(state ActivationState) ActivationRepairRelease {
		return ActivationRepairRelease{Slot: state.ActiveSlot, BundleGeneration: state.BundleGeneration,
			WorkerSourceCommit: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest}
	}
	return path, ActivationRepairCASRequest{JournalDigest: "sha256:" + strings.Repeat("c", 64), CommittedGeneration: 2,
		Committed: identity(current), Previous: identity(prior), Expected: current}, current, now.Add(2 * time.Second)
}

func applyTestRepairAt(path string, request ActivationRepairCASRequest, at time.Time) (ActivationReceipt, error) {
	return applyActivationRepairCAS(path, request, at.Add(10*time.Second), func() time.Time { return at })
}

func TestActivationRepairRecordsSameTargetOnKeptAndRolledBackMembers(t *testing.T) {
	stablePath, stable, current, now := seedPartialRepair(t)
	rolledBackPath, repair, _, _ := seedPartialRepair(t)
	rollback := activationRequest(ActivationOperationRollback, 2, "b", "a", "bundle-a", 2)
	rollback.WorkerSourceCommit, rollback.WorkerImageDigest = repair.Previous.WorkerSourceCommit, repair.Previous.WorkerImageDigest
	rolledBack, err := ApplyActivationCAS(rolledBackPath, rollback, now)
	if err != nil {
		t.Fatal(err)
	}
	repair.Expected = rolledBack.Current
	if _, err := applyTestRepairAt(rolledBackPath, repair, now); err == nil {
		t.Fatal("divergent node switched before preparation")
	}
	prepare := activationRequest(ActivationOperationPromote, 3, "a", "b", "bundle-b", 0)
	prepare.WorkerSourceCommit, prepare.WorkerImageDigest = repair.Committed.WorkerSourceCommit, repair.Committed.WorkerImageDigest
	p, err := PreparePublication(rolledBackPath, prepare, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestRepairAt(rolledBackPath, repair, p.ExpiresAt.Add(-5*time.Second)); err == nil {
		t.Fatal("expired or near-expiry preparation was accepted")
	}
	read, _, err := ReadActivationState(rolledBackPath)
	if err != nil || read != rolledBack.Current {
		t.Fatalf("failed repair changed the file: %+v, error=%v", read, err)
	}
	kept, err := applyTestRepairAt(stablePath, stable, now)
	if err != nil || !stable.Committed.matches(kept.Current) || kept.Previous == nil || *kept.Previous != current {
		t.Fatalf("same-slot rebase changed serving identity: %+v, error=%v", kept, err)
	}
	recovered, err := applyTestRepairAt(rolledBackPath, repair, now.Add(time.Second))
	if err != nil || recovered.Previous == nil || *recovered.Previous != rolledBack.Current ||
		kept.Current.Generation != 4 || !stable.MatchesActivation(kept.Current) || !repair.MatchesActivation(recovered.Current) ||
		activationWithoutTime(kept.Current) != activationWithoutTime(recovered.Current) ||
		kept.StateDigest == "" || recovered.StateDigest == "" {
		t.Fatalf("different group target: kept=%+v repaired=%+v err=%v", kept, recovered, err)
	}
	if _, err := applyTestRepairAt(rolledBackPath, repair, now.Add(2*time.Second)); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("replay did not preserve the newer generation: %v", err)
	}
	wrongJournal := repair
	wrongJournal.JournalDigest = "sha256:" + strings.Repeat("e", 64)
	if wrongJournal.MatchesActivation(recovered.Current) {
		t.Fatal("a different journal recognized the successful write")
	}
	if _, err := ApplyActivationCAS(stablePath, rollback, now); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("legacy stale CAS was not fenced: %v", err)
	}
	rollback.ExpectedGeneration, rollback.RollbackOfGeneration = 4, 4
	if _, err := ApplyActivationCAS(stablePath, rollback, now.Add(time.Second)); err != nil {
		t.Fatalf("ordinary v1 CAS could not continue after convergence: %v", err)
	}
}

func TestActivationRepairCASMatchesCompletePredecessorAndFencesConcurrentWriters(t *testing.T) {
	path, input, current, now := seedPartialRepair(t)
	incorrect := input
	incorrect.Expected.Reason = "another otherwise valid audit marker"
	if _, err := applyTestRepairAt(path, incorrect, now); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("changed predecessor did not conflict: %v", err)
	}
	incorrect = input
	incorrect.Committed.BundleGeneration = "unproven-bundle"
	if _, err := applyTestRepairAt(path, incorrect, now); err == nil {
		t.Fatal("same-slot repair would change the serving bundle")
	}
	got, _, err := ReadActivationState(path)
	if err != nil || got != current {
		t.Fatalf("rejected update reached disk: %+v, error=%v", got, err)
	}
	const writers = 8
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Go(func() {
			_, err := applyTestRepairAt(path, input, now)
			results <- err
		})
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrActivationCASConflict) {
			t.Fatal(err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d of %d writers, want exactly one", accepted, writers)
	}
}

func TestActivationRepairDeadlineSurvivesContentionAndDelayBeforeReplace(t *testing.T) {
	path, request, current, now := seedPartialRepair(t)
	lock, err := acquireActivationLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestRepairAt(path, request, now); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("repair did not immediately reject a busy lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	for _, deadline := range []time.Time{now, now.Add(time.Minute)} {
		if _, err := applyActivationRepairCAS(path, request, deadline, func() time.Time { return now }); err == nil {
			t.Fatal("invalid write deadline was accepted")
		}
	}
	deadline := now.Add(5 * time.Second)
	calls := 0
	if _, err := applyActivationRepairCAS(path, request, deadline, func() time.Time {
		calls++
		if calls == 3 {
			return deadline
		}
		return now
	}); err == nil || calls != 3 {
		t.Fatalf("deadline missed just before atomic replace: checks=%d err=%v", calls, err)
	}
	if read, _, err := ReadActivationState(path); err != nil || read != current {
		t.Fatalf("expired write reached disk: state=%+v err=%v", read, err)
	}
}

func TestActivationRepairPreparationHasBoundedExactLocalWrite(t *testing.T) {
	path, request, _, now := seedPartialRepair(t)
	deadline := now.Add(5 * time.Second)
	clock := func() time.Time { return now }
	if _, err := prepareActivationRepairPublication(path, request, deadline, clock); err == nil {
		t.Fatal("prepared a serving Worker")
	}
	rollback := activationRequest(ActivationOperationRollback, 2, "b", "a", "bundle-a", 2)
	rollback.WorkerSourceCommit, rollback.WorkerImageDigest = request.Previous.WorkerSourceCommit, request.Previous.WorkerImageDigest
	phase, err := ApplyActivationCAS(path, rollback, now)
	if err != nil {
		t.Fatal(err)
	}
	request.Expected = phase.Current
	changed := request
	changed.Expected.Reason = "not the journal predecessor"
	if _, err := prepareActivationRepairPublication(path, changed, deadline, clock); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("preparation did not fence complete predecessor: %v", err)
	}
	lock, err := acquireActivationLock(path)
	if err != nil {
		t.Fatal(err)
	}
	_, busyErr := prepareActivationRepairPublication(path, request, deadline, clock)
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(busyErr, ErrActivationCASConflict) {
		t.Fatalf("preparation did not reject lock contention: %v", busyErr)
	}
	checks := 0
	if _, err := prepareActivationRepairPublication(path, request, deadline, func() time.Time {
		checks++
		if checks == 3 {
			return deadline
		}
		return now
	}); err == nil || checks != 3 {
		t.Fatalf("preparation did not check deadline before atomic replacement: checks=%d err=%v", checks, err)
	}
	if _, found, err := ReadPublicationPreparation(path); err != nil || found {
		t.Fatalf("expired preparation reached disk: found=%v err=%v", found, err)
	}
	first, err := prepareActivationRepairPublication(path, request, deadline, clock)
	if err != nil || first.TargetSlot != "b" || first.ExpiresAt.Sub(first.CreatedAt) != PublicationPreparationLifetime {
		t.Fatalf("bad preparation: %+v, err=%v", first, err)
	}
	second, err := prepareActivationRepairPublication(path, request, deadline, func() time.Time { return now.Add(time.Second) })
	if err != nil || second != first {
		t.Fatalf("preparation replay changed the original expiration: %+v, err=%v", second, err)
	}
	if state, found, err := ReadActivationState(path); err != nil || !found || state != request.Expected {
		t.Fatalf("preparation changed the serving activation: %+v, err=%v", state, err)
	}
}

func activationRequest(operation string, expectedGeneration uint64, expectedSlot, targetSlot, bundle string, rollbackOf uint64) ActivationCASRequest {
	return ActivationCASRequest{
		GroupID: "edge-group-fixture", ExpectedGeneration: expectedGeneration, ExpectedSlot: expectedSlot, TargetSlot: targetSlot,
		BundleGeneration: bundle, WorkerSourceCommit: strings.Repeat("a", 40), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64),
		Operation: operation, RollbackOfGeneration: rollbackOf, Reason: "edge group activation test",
	}
}
