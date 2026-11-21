package edgegroupfront

import (
	"errors"
	"math"
	"strings"
	"time"
)

// ActivationRepairRelease is an immutable Worker and publication identity from
// the committed group authority, not a discovered or arbitrary target.
type ActivationRepairRelease struct {
	Slot               string
	BundleGeneration   string
	WorkerSourceCommit string
	WorkerImageDigest  string
}

// ActivationRepairCASRequest applies one node of a durable, group-locked
// repair. Callers must have independently verified the release, the exact
// roster and running Worker, and the journal. A digest is not authorization.
type ActivationRepairCASRequest struct {
	JournalDigest       string
	Expected            ActivationState
	CommittedGeneration uint64
	Committed           ActivationRepairRelease
	Previous            ActivationRepairRelease
}

func ApplyActivationRepairCAS(path string, request ActivationRepairCASRequest) (ActivationReceipt, error) {
	return applyActivationRepairCAS(path, request, time.Now)
}

func applyActivationRepairCAS(path string, request ActivationRepairCASRequest, clock func() time.Time) (ActivationReceipt, error) {
	if err := validateActivationPath(path); err != nil {
		return ActivationReceipt{}, err
	}
	if err := request.validate(); err != nil {
		return ActivationReceipt{}, err
	}
	lock, err := acquireActivationLock(path)
	if err != nil {
		return ActivationReceipt{}, err
	}
	defer lock.Close()
	current, exists, err := ReadActivationState(path)
	if err != nil || !exists {
		return ActivationReceipt{}, errors.Join(ErrActivationCASConflict, err)
	}
	if !sameRepairActivation(current, request.Expected) {
		return ActivationReceipt{}, ErrActivationCASConflict
	}
	// Take the clock after acquiring the lock so waiting for an unrelated
	// writer cannot consume the preparation TTL and then reuse stale time.
	now := clock().UTC()
	if now.IsZero() || now.Before(current.UpdatedAt) {
		return ActivationReceipt{}, errors.New("Front repair clock precedes the exact predecessor")
	}
	if current.ActiveSlot != request.Committed.Slot {
		prepared, found, err := ReadPublicationPreparation(path)
		if err != nil || !found || !prepared.Matches(current, request.Committed.Slot, request.Committed.WorkerSourceCommit, now) ||
			prepared.BundleGeneration != request.Committed.BundleGeneration || prepared.WorkerImageDigest != request.Committed.WorkerImageDigest ||
			!now.Add(5*time.Second).Before(prepared.ExpiresAt) {
			return ActivationReceipt{}, errors.New("Front repair lacks exact unexpired publication preparation")
		}
	}
	next := request.target()
	next.UpdatedAt = now
	return commitActivationState(path, current, true, next)
}

func (request ActivationRepairCASRequest) validate() error {
	if validateActivationState(request.Expected) != nil || !activationDigestPattern.MatchString(request.JournalDigest) ||
		request.CommittedGeneration < 2 || request.CommittedGeneration > math.MaxUint64-3 ||
		!request.Committed.valid() || !request.Previous.valid() || request.Committed.Slot == request.Previous.Slot {
		return errors.New("Front repair request has invalid release or journal identity")
	}
	state := request.Expected
	if state.Generation == request.CommittedGeneration && request.Committed.matches(state) {
		switch state.Operation {
		case ActivationOperationInit:
			if state.PreviousSlot == "" {
				return nil
			}
		case ActivationOperationPromote:
			if state.PreviousSlot == request.Previous.Slot {
				return nil
			}
		case ActivationOperationRollback:
			if state.PreviousSlot == request.Previous.Slot && state.RollbackOfGeneration == state.Generation-1 {
				return nil
			}
		}
	}
	if state.Generation == request.CommittedGeneration+1 && state.Operation == ActivationOperationRollback &&
		state.RollbackOfGeneration == request.CommittedGeneration && state.PreviousSlot == request.Committed.Slot &&
		state.ActiveSlot == request.Previous.Slot && state.WorkerSourceCommit == request.Previous.WorkerSourceCommit &&
		state.WorkerImageDigest == request.Previous.WorkerImageDigest &&
		(state.BundleGeneration == request.Previous.BundleGeneration || state.BundleGeneration == request.Committed.BundleGeneration) {
		return nil
	}
	return errors.New("Front repair predecessor is not committed or its exact one-step rollback")
}

func (release ActivationRepairRelease) valid() bool {
	return normalizeSlot(release.Slot) != "" && normalizeSlot(release.Slot) == release.Slot &&
		activationCommitPattern.MatchString(release.WorkerSourceCommit) && activationDigestPattern.MatchString(release.WorkerImageDigest) &&
		len(release.BundleGeneration) <= 255 && release.BundleGeneration != "" && strings.TrimSpace(release.BundleGeneration) == release.BundleGeneration
}

func (release ActivationRepairRelease) matches(state ActivationState) bool {
	return release.Slot == state.ActiveSlot && release.BundleGeneration == state.BundleGeneration &&
		release.WorkerSourceCommit == state.WorkerSourceCommit && release.WorkerImageDigest == state.WorkerImageDigest
}

func sameRepairActivation(left, right ActivationState) bool {
	return left.UpdatedAt.Equal(right.UpdatedAt) && activationWithoutTime(left) == activationWithoutTime(right)
}

func activationWithoutTime(state ActivationState) ActivationState {
	state.UpdatedAt = time.Time{}
	return state
}

func (request ActivationRepairCASRequest) target() ActivationState {
	// Preserve the existing v1 format. For a member already on the committed
	// release, no serving field changes, even though its fence advances by two.
	return ActivationState{
		Schema: ActivationStateSchemaV1, GroupID: request.Expected.GroupID, Generation: request.CommittedGeneration + 2,
		ActiveSlot: request.Committed.Slot, PreviousSlot: request.Previous.Slot, BundleGeneration: request.Committed.BundleGeneration,
		WorkerSourceCommit: request.Committed.WorkerSourceCommit, WorkerImageDigest: request.Committed.WorkerImageDigest,
		Authority: ActivationAuthority, Operation: ActivationOperationPromote, Reason: "reconcile partial rollback using journal " + request.JournalDigest,
	}
}

// MatchesActivation is a read-only postcondition for resolving a lost CAS
// reply. It cannot mistake an unrelated promotion at the new generation for
// this journal's write. Callers must still observe the live Front and Worker.
func (request ActivationRepairCASRequest) MatchesActivation(state ActivationState) bool {
	if request.validate() != nil || validateActivationState(state) != nil || state.UpdatedAt.Before(request.Expected.UpdatedAt) {
		return false
	}
	return activationWithoutTime(state) == request.target()
}
