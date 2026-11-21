package edgegroupfront

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
	if _, err := applyActivationRepairCAS(rolledBackPath, repair, func() time.Time { return now }); err == nil {
		t.Fatal("divergent node switched before preparation")
	}
	prepare := activationRequest(ActivationOperationPromote, 3, "a", "b", "bundle-b", 0)
	prepare.WorkerSourceCommit, prepare.WorkerImageDigest = repair.Committed.WorkerSourceCommit, repair.Committed.WorkerImageDigest
	p, err := PreparePublication(rolledBackPath, prepare, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyActivationRepairCAS(rolledBackPath, repair, func() time.Time { return p.ExpiresAt.Add(-5 * time.Second) }); err == nil {
		t.Fatal("expired or near-expiry preparation was accepted")
	}
	read, _, err := ReadActivationState(rolledBackPath)
	if err != nil || read != rolledBack.Current {
		t.Fatalf("failed repair changed the file: %+v, error=%v", read, err)
	}
	kept, err := applyActivationRepairCAS(stablePath, stable, func() time.Time { return now })
	if err != nil || !stable.Committed.matches(kept.Current) || kept.Previous == nil || *kept.Previous != current {
		t.Fatalf("same-slot rebase changed serving identity: %+v, error=%v", kept, err)
	}
	recovered, err := applyActivationRepairCAS(rolledBackPath, repair, func() time.Time { return now.Add(time.Second) })
	if err != nil || recovered.Previous == nil || *recovered.Previous != rolledBack.Current ||
		kept.Current.Generation != 4 || !stable.MatchesActivation(kept.Current) || !repair.MatchesActivation(recovered.Current) ||
		activationWithoutTime(kept.Current) != activationWithoutTime(recovered.Current) ||
		kept.StateDigest == "" || recovered.StateDigest == "" {
		t.Fatalf("different group target: kept=%+v repaired=%+v err=%v", kept, recovered, err)
	}
	if _, err := applyActivationRepairCAS(rolledBackPath, repair, func() time.Time { return now.Add(2 * time.Second) }); !errors.Is(err, ErrActivationCASConflict) {
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
	if _, err := applyActivationRepairCAS(path, incorrect, func() time.Time { return now }); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("changed predecessor did not conflict: %v", err)
	}
	incorrect = input
	incorrect.Committed.BundleGeneration = "unproven-bundle"
	if _, err := applyActivationRepairCAS(path, incorrect, func() time.Time { return now }); err == nil {
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
			_, err := applyActivationRepairCAS(path, input, func() time.Time { return now })
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
