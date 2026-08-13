package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type authorityDecisionStore interface {
	LoadCandidate(context.Context, string) (CandidateAuthority, types.UID, string, error)
	LoadCurrent(context.Context, string) (CurrentAuthority, types.UID, string, error)
	LoadCandidateCanaryResult(context.Context, CandidateAuthority, string, time.Time) (CandidateCanaryResult, error)
	LoadLatestCandidateCanaryResult(context.Context, CandidateAuthority, time.Time) (CandidateCanaryResult, error)
	PutCandidate(context.Context, CandidateAuthority, types.UID, string) (types.UID, string, error)
	SwitchCurrent(context.Context, CurrentAuthority, types.UID, string) (types.UID, string, error)
}

func (controller *AuthorityController) Reconcile(ctx context.Context, groupID string) (AuthorityTransitionReceipt, bool, error) {
	if controller == nil || !groupPattern.MatchString(groupID) {
		return AuthorityTransitionReceipt{}, false, errors.New("authority reconcile group is invalid")
	}
	candidate, _, _, err := controller.store.LoadCandidate(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, false, err
	}
	if candidate.State == CandidateAuthorityRejected {
		return AuthorityTransitionReceipt{}, false, nil
	}
	current, _, _, err := controller.store.LoadCurrent(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, false, err
	}
	if candidate.State == CandidateAuthorityVerified && current.CurrentRecordDigest == candidate.RecordDigest && current.CurrentWorkerSlot == candidate.WorkerSlot {
		return AuthorityTransitionReceipt{}, false, nil
	}
	resultDigest := candidate.CanaryResultDigest
	if candidate.State == CandidateAuthorityLoaded {
		result, err := controller.store.LoadLatestCandidateCanaryResult(ctx, candidate, controller.now().UTC())
		if errors.Is(err, ErrCandidateCanaryUnavailable) {
			return AuthorityTransitionReceipt{}, false, nil
		}
		if err != nil {
			return AuthorityTransitionReceipt{}, false, err
		}
		resultDigest = result.ResultDigest
	}
	receipt, err := controller.VerifyAndSwitch(ctx, groupID, resultDigest)
	return receipt, err == nil, err
}

type AuthorityController struct {
	store     authorityDecisionStore
	verifiers map[string]CandidateCanaryVerifier
	now       func() time.Time
}

type CandidateCanaryVerifier struct {
	KeyID string
	Key   []byte
}

func NewAuthorityController(store authorityDecisionStore, verifiers map[string]CandidateCanaryVerifier) (*AuthorityController, error) {
	if store == nil || len(verifiers) == 0 {
		return nil, errors.New("authority controller configuration is invalid")
	}
	values := make(map[string]CandidateCanaryVerifier, len(verifiers))
	for group, verifier := range verifiers {
		if !groupPattern.MatchString(group) || !componentPattern.MatchString(verifier.KeyID) || len(verifier.Key) != ed25519.PublicKeySize {
			return nil, errors.New("authority controller signing key is invalid")
		}
		verifier.Key = append([]byte(nil), verifier.Key...)
		values[group] = verifier
	}
	return &AuthorityController{store: store, verifiers: values, now: time.Now}, nil
}

func (controller *AuthorityController) VerifyAndSwitch(ctx context.Context, groupID, resultDigest string) (AuthorityTransitionReceipt, error) {
	if controller == nil || !digestPattern.MatchString(resultDigest) {
		return AuthorityTransitionReceipt{}, errors.New("authority switch request is invalid")
	}
	verifier, exists := controller.verifiers[groupID]
	if !exists {
		return AuthorityTransitionReceipt{}, errors.New("authority switch request is invalid")
	}
	candidate, candidateUID, candidateRV, err := controller.store.LoadCandidate(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	if !candidate.HasPromotionWitness() {
		return AuthorityTransitionReceipt{}, errors.New("candidate promotion witness is unavailable")
	}
	result, err := controller.store.LoadCandidateCanaryResult(ctx, candidate, resultDigest, controller.now().UTC())
	if err != nil || result.KeyID != verifier.KeyID || result.VerifySignature(verifier.Key) != nil {
		return AuthorityTransitionReceipt{}, errors.New("candidate canary attestation is invalid")
	}
	if result.RouteState != HealthHealthy || result.DependencyState != HealthHealthy {
		if candidate.State != CandidateAuthorityLoaded {
			return AuthorityTransitionReceipt{}, errors.New("terminal candidate cannot be rejected again")
		}
		rejected := candidate
		rejected.State, rejected.Generation, rejected.CanaryResultDigest = CandidateAuthorityRejected, candidate.Generation+1, result.ResultDigest
		if _, _, err = controller.store.PutCandidate(ctx, rejected, candidateUID, candidateRV); err != nil {
			return AuthorityTransitionReceipt{}, err
		}
		current, _, _, err := controller.store.LoadCurrent(ctx, groupID)
		if err != nil {
			return AuthorityTransitionReceipt{}, err
		}
		return (AuthorityTransitionReceipt{
			GroupID: groupID, Action: AuthorityCandidateRejected, CandidateDigest: candidate.RecordDigest,
			CanaryResultDigest: result.ResultDigest, Before: current, After: current,
			ObservedAt: controller.now().UTC().Format(time.RFC3339Nano),
		}).Seal()
	}
	if candidate.State == CandidateAuthorityLoaded {
		verified := candidate
		verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, result.ResultDigest
		if _, _, err := controller.store.PutCandidate(ctx, verified, candidateUID, candidateRV); err != nil {
			return AuthorityTransitionReceipt{}, err
		}
		candidate = verified
	} else if candidate.State != CandidateAuthorityVerified || candidate.CanaryResultDigest != result.ResultDigest {
		return AuthorityTransitionReceipt{}, errors.New("candidate terminal state does not bind the canary result")
	}
	current, currentUID, currentRV, err := controller.store.LoadCurrent(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	if current.CurrentRecordDigest == candidate.RecordDigest || current.CurrentWorkerSlot == candidate.WorkerSlot {
		return AuthorityTransitionReceipt{}, errors.New("candidate is not an inactive distinct authority")
	}
	next := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: candidate.RecordDigest, CurrentWorkerSlot: candidate.WorkerSlot,
		PreviousRecordDigest: current.CurrentRecordDigest, PreviousWorkerSlot: current.CurrentWorkerSlot,
		AuthorityEpoch: current.AuthorityEpoch + 1,
	}
	if _, _, err := controller.store.SwitchCurrent(ctx, next, currentUID, currentRV); err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	return (AuthorityTransitionReceipt{
		GroupID: groupID, Action: AuthorityCurrentSwitched, CandidateDigest: candidate.RecordDigest,
		CanaryResultDigest: result.ResultDigest, Before: current, After: next,
		ObservedAt: controller.now().UTC().Format(time.RFC3339Nano),
	}).Seal()
}

func (controller *AuthorityController) Revert(ctx context.Context, groupID, failedRecordDigest, canaryResultDigest string) (AuthorityTransitionReceipt, error) {
	if controller == nil || !groupPattern.MatchString(groupID) || !digestPattern.MatchString(failedRecordDigest) || !digestPattern.MatchString(canaryResultDigest) {
		return AuthorityTransitionReceipt{}, errors.New("authority revert request is invalid")
	}
	current, uid, rv, err := controller.store.LoadCurrent(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	if current.CurrentRecordDigest != failedRecordDigest || current.PreviousRecordDigest == "" || current.PreviousWorkerSlot == "" {
		return AuthorityTransitionReceipt{}, errors.New("authority revert does not bind the exact current and LKG")
	}
	reverted := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: current.PreviousRecordDigest, CurrentWorkerSlot: current.PreviousWorkerSlot,
		PreviousRecordDigest: current.CurrentRecordDigest, PreviousWorkerSlot: current.CurrentWorkerSlot,
		AuthorityEpoch: current.AuthorityEpoch + 1,
	}
	if _, _, err := controller.store.SwitchCurrent(ctx, reverted, uid, rv); err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	return (AuthorityTransitionReceipt{
		GroupID: groupID, Action: AuthorityCurrentReverted, CandidateDigest: failedRecordDigest,
		CanaryResultDigest: canaryResultDigest, Before: current, After: reverted,
		ObservedAt: controller.now().UTC().Format(time.RFC3339Nano),
	}).Seal()
}
