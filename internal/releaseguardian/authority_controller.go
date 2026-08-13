package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type authorityDecisionStore interface {
	LoadCandidate(context.Context, string) (CandidateAuthority, types.UID, string, error)
	LoadCurrent(context.Context, string) (CurrentAuthority, types.UID, string, error)
	LoadBaselineReceipt(context.Context, string) (AuthorityBaselineReceipt, error)
	LoadTransitionJournal(context.Context, string) (AuthorityTransitionJournal, bool, error)
	LoadCandidateCanaryResult(context.Context, CandidateAuthority, string, time.Time) (CandidateCanaryResult, error)
	LoadLatestCandidateCanaryResult(context.Context, CandidateAuthority, time.Time) (CandidateCanaryResult, error)
	PutCandidate(context.Context, CandidateAuthority, types.UID, string) (types.UID, string, error)
	SwitchCurrent(context.Context, CurrentAuthority, types.UID, string) (types.UID, string, error)
	CreateTransitionJournal(context.Context, AuthorityTransitionJournal) error
	UpdateTransitionJournal(context.Context, AuthorityTransitionJournal, AuthorityTransitionJournal) error
	DeleteTransitionJournal(context.Context, AuthorityTransitionJournal) error
}

// authorityCompensationSettler is deliberately optional. Production
// activators can prove that an activated external transaction was fully
// compensated before its immutable journal is retired. The controller never
// infers compensation from CurrentAuthority alone.
type authorityCompensationSettler interface {
	CompensationSettled(context.Context, AuthorityTransitionJournal) (bool, error)
}

type authorityPrewriteCASClassifier interface {
	IsPrewriteCASChanged(error) bool
}

type authorityPreparedSettler interface {
	PreparedSettled(context.Context, AuthorityTransitionJournal) (bool, error)
}

func (controller *AuthorityController) finalizeTransitionJournal(ctx context.Context, journal AuthorityTransitionJournal) error {
	if finalizer, ok := controller.activators[journal.GroupID].(interface{ Finalize(context.Context) error }); ok {
		if err := finalizer.Finalize(ctx); err != nil {
			return err
		}
	}
	return controller.store.DeleteTransitionJournal(ctx, journal)
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
	journal, journalExists, err := controller.store.LoadTransitionJournal(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, false, err
	}
	if !candidate.HasWorkerReleaseIdentity() {
		if !journalExists {
			return AuthorityTransitionReceipt{}, false, nil
		}
		if journal.Phase != AuthorityTransitionPrepared || journal.Candidate != candidate || journal.Before != current {
			return AuthorityTransitionReceipt{}, false, errors.New("legacy candidate transition cannot be safely settled")
		}
		settler, ok := controller.activators[groupID].(authorityPreparedSettler)
		if !ok {
			return AuthorityTransitionReceipt{}, false, errors.New("legacy candidate transition has no settlement observer")
		}
		settled, settleErr := settler.PreparedSettled(ctx, journal)
		if settleErr != nil || !settled {
			return AuthorityTransitionReceipt{}, false, settleErr
		}
		return AuthorityTransitionReceipt{}, false, controller.store.DeleteTransitionJournal(ctx, journal)
	}
	if journalExists {
		receipt, changed, resumeErr := controller.resumeTransition(ctx, current, journal)
		if resumeErr != nil && controller.isPrewriteCASChanged(groupID, resumeErr) {
			if settleErr := controller.settlePrewriteCAS(ctx, groupID); settleErr != nil {
				return AuthorityTransitionReceipt{}, false, settleErr
			}
			return AuthorityTransitionReceipt{}, false, nil
		}
		return receipt, changed, resumeErr
	}
	if candidate.State == CandidateAuthorityVerified {
		// A verified candidate can only enter production while its immutable
		// transition journal exists. With no journal it is either already
		// current or a settled failed attempt awaiting importer replacement.
		// Never reconstruct a production transaction from an expired canary.
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
	if err != nil && controller.isPrewriteCASChanged(groupID, err) {
		if settleErr := controller.settlePrewriteCAS(ctx, groupID); settleErr != nil {
			return AuthorityTransitionReceipt{}, false, settleErr
		}
		return AuthorityTransitionReceipt{}, false, nil
	}
	return receipt, err == nil, err
}

func (controller *AuthorityController) isPrewriteCASChanged(groupID string, err error) bool {
	classifier, ok := controller.activators[groupID].(authorityPrewriteCASClassifier)
	return ok && classifier.IsPrewriteCASChanged(err)
}

func (controller *AuthorityController) settlePrewriteCAS(ctx context.Context, groupID string) error {
	journal, exists, loadErr := controller.store.LoadTransitionJournal(ctx, groupID)
	settledCandidate, _, _, candidateErr := controller.store.LoadCandidate(ctx, groupID)
	if loadErr != nil || candidateErr != nil || !exists || journal.Phase != AuthorityTransitionPrepared || journal.Candidate != settledCandidate {
		return errors.New("stale authority prewrite journal is unavailable")
	}
	return controller.store.DeleteTransitionJournal(ctx, journal)
}

func (controller *AuthorityController) resumeTransition(ctx context.Context, current CurrentAuthority, journal AuthorityTransitionJournal) (AuthorityTransitionReceipt, bool, error) {
	if journal.Validate() != nil || journal.Before.GroupID != current.GroupID {
		return AuthorityTransitionReceipt{}, false, errors.New("authority transition journal cannot be resumed")
	}
	if current.CurrentRecordDigest == journal.Candidate.RecordDigest && current.CurrentWorkerSlot == journal.Candidate.WorkerSlot {
		if journal.Phase != AuthorityTransitionActivated || journal.Activation == nil ||
			current.CurrentFrontGeneration != journal.Activation.TargetGeneration || current.CurrentBundleGeneration != journal.Activation.TargetBundleGeneration {
			return AuthorityTransitionReceipt{}, false, errors.New("completed authority transition does not match its journal")
		}
		if err := controller.finalizeTransitionJournal(ctx, journal); err != nil {
			return AuthorityTransitionReceipt{}, false, err
		}
		return AuthorityTransitionReceipt{}, false, nil
	}
	if current != journal.Before {
		return AuthorityTransitionReceipt{}, false, errors.New("authority transition predecessor changed while journal was active")
	}
	if journal.Phase == AuthorityTransitionActivated {
		if settler, ok := controller.activators[journal.GroupID].(authorityCompensationSettler); ok {
			settled, settleErr := settler.CompensationSettled(ctx, journal)
			if settleErr != nil {
				return AuthorityTransitionReceipt{}, false, fmt.Errorf("reconcile compensated authority transition: %w", settleErr)
			}
			if settled {
				if err := controller.finalizeTransitionJournal(ctx, journal); err != nil {
					return AuthorityTransitionReceipt{}, false, err
				}
				return AuthorityTransitionReceipt{}, false, nil
			}
		}
	}
	// Prepared and activated journals resume the same immutable candidate and
	// canary. The production activator's idempotent Control replay and exact
	// Front state observation settle any zero/partial/full external mutation.
	receipt, err := controller.verifyAndSwitch(ctx, journal.GroupID, journal.CanaryResultDigest, &journal)
	return receipt, err == nil, err
}

type AuthorityController struct {
	store      authorityDecisionStore
	verifiers  map[string]CandidateCanaryVerifier
	activators map[string]FrontAuthorityActivator
	observers  map[string]AuthorityHealthObserver
	failures   map[string]int
	healthMu   sync.Mutex
	now        func() time.Time
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

func NewAuthorityControllerWithActivators(store authorityDecisionStore, verifiers map[string]CandidateCanaryVerifier, activators map[string]FrontAuthorityActivator) (*AuthorityController, error) {
	controller, err := NewAuthorityController(store, verifiers)
	if err != nil {
		return nil, err
	}
	values := make(map[string]FrontAuthorityActivator, len(activators))
	for group, activator := range activators {
		if !groupPattern.MatchString(group) || activator == nil {
			return nil, errors.New("authority activator configuration is invalid")
		}
		values[group] = activator
	}
	controller.activators = values
	return controller, nil
}

func (controller *AuthorityController) SetHealthObservers(observers map[string]AuthorityHealthObserver) error {
	if controller == nil {
		return errors.New("authority controller is nil")
	}
	values := make(map[string]AuthorityHealthObserver, len(observers))
	for group, observer := range observers {
		if !groupPattern.MatchString(group) || observer == nil || controller.activators[group] == nil {
			return errors.New("authority health observer configuration is invalid")
		}
		values[group] = observer
	}
	controller.observers, controller.failures = values, map[string]int{}
	return nil
}

// ObserveAndRevert is event/timer driven after an authority switch. Three
// consecutive candidate-only failures are required; any healthy current or
// unhealthy LKG resets the counter and cannot trigger a rollback.
func (controller *AuthorityController) ObserveAndRevert(ctx context.Context, groupID string) (AuthorityTransitionReceipt, bool, error) {
	if controller == nil || !groupPattern.MatchString(groupID) {
		return AuthorityTransitionReceipt{}, false, errors.New("authority health group is invalid")
	}
	observer := controller.observers[groupID]
	if observer == nil {
		return AuthorityTransitionReceipt{}, false, nil
	}
	current, _, _, err := controller.store.LoadCurrent(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, false, err
	}
	if current.PreviousRecordDigest == "" || current.PreviousWorkerSlot == "" {
		controller.healthMu.Lock()
		controller.failures[groupID] = 0
		controller.healthMu.Unlock()
		return AuthorityTransitionReceipt{}, false, nil
	}
	currentHealthy, lkgHealthy, evidenceDigest, err := observer.ObserveCurrentAndLKG(ctx, current)
	if err != nil || !digestPattern.MatchString(evidenceDigest) {
		controller.healthMu.Lock()
		controller.failures[groupID] = 0
		controller.healthMu.Unlock()
		return AuthorityTransitionReceipt{}, false, err
	}
	controller.healthMu.Lock()
	if currentHealthy || !lkgHealthy {
		controller.failures[groupID] = 0
		controller.healthMu.Unlock()
		return AuthorityTransitionReceipt{}, false, nil
	}
	controller.failures[groupID]++
	if controller.failures[groupID] < 3 {
		controller.healthMu.Unlock()
		return AuthorityTransitionReceipt{}, false, nil
	}
	controller.failures[groupID] = 0
	controller.healthMu.Unlock()
	receipt, err := controller.Revert(ctx, groupID, current.CurrentRecordDigest, evidenceDigest)
	return receipt, err == nil, err
}

func (controller *AuthorityController) VerifyAndSwitch(ctx context.Context, groupID, resultDigest string) (AuthorityTransitionReceipt, error) {
	return controller.verifyAndSwitch(ctx, groupID, resultDigest, nil)
}

func (controller *AuthorityController) verifyAndSwitch(ctx context.Context, groupID, resultDigest string, resume *AuthorityTransitionJournal) (AuthorityTransitionReceipt, error) {
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
	if !candidate.HasWorkerReleaseIdentity() {
		return AuthorityTransitionReceipt{}, errors.New("candidate Worker release identity is unavailable")
	}
	verificationTime := controller.now().UTC()
	if candidate.State == CandidateAuthorityVerified || resume != nil {
		verificationTime = time.Time{}
	}
	result, err := controller.store.LoadCandidateCanaryResult(ctx, candidate, resultDigest, verificationTime)
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
	if candidate.State != CandidateAuthorityLoaded && (candidate.State != CandidateAuthorityVerified || candidate.CanaryResultDigest != result.ResultDigest) {
		return AuthorityTransitionReceipt{}, errors.New("candidate terminal state does not bind the canary result")
	}
	current, currentUID, currentRV, err := controller.store.LoadCurrent(ctx, groupID)
	if err != nil {
		return AuthorityTransitionReceipt{}, err
	}
	if current.CurrentRecordDigest == candidate.RecordDigest || current.CurrentWorkerSlot == candidate.WorkerSlot {
		return AuthorityTransitionReceipt{}, errors.New("candidate is not an inactive distinct authority")
	}
	baseline, err := controller.store.LoadBaselineReceipt(ctx, groupID)
	if err != nil || baseline.ReceiptDigest != current.BaselineReceiptDigest {
		return AuthorityTransitionReceipt{}, errors.New("current Front baseline is unavailable")
	}
	currentFrontGeneration, currentBundleGeneration := current.CurrentFrontGeneration, current.CurrentBundleGeneration
	currentWorkerSourceSHA, currentWorkerImageDigest := current.CurrentWorkerSourceSHA, current.CurrentWorkerImageDigest
	if currentFrontGeneration == 0 {
		if len(baseline.Nodes) == 0 {
			return AuthorityTransitionReceipt{}, errors.New("current Front baseline is empty")
		}
		first := baseline.Nodes[0]
		currentFrontGeneration, currentBundleGeneration = first.ActivationGeneration, first.BundleGeneration
		currentWorkerSourceSHA, currentWorkerImageDigest = first.WorkerSourceSHA, first.WorkerImageDigest
		for _, node := range baseline.Nodes[1:] {
			if node.ActivationGeneration != currentFrontGeneration || node.BundleGeneration != currentBundleGeneration ||
				node.WorkerSourceSHA != currentWorkerSourceSHA || node.WorkerImageDigest != currentWorkerImageDigest {
				return AuthorityTransitionReceipt{}, errors.New("current Front baseline is mixed")
			}
		}
	}
	if resume != nil && resume.Phase == AuthorityTransitionActivated && resume.Activation != nil {
		activation := *resume.Activation
		if activation.GroupID != groupID || activation.PreviousSlot != current.CurrentWorkerSlot ||
			activation.PreviousBundleGeneration != currentBundleGeneration ||
			activation.PreviousWorkerSourceSHA != currentWorkerSourceSHA || activation.PreviousWorkerImageDigest != currentWorkerImageDigest ||
			activation.PreviousGeneration < currentFrontGeneration {
			return AuthorityTransitionReceipt{}, errors.New("authority transition activation predecessor is invalid")
		}
		currentFrontGeneration = activation.PreviousGeneration
	}
	verifiedCandidate := candidate
	if verifiedCandidate.State == CandidateAuthorityLoaded {
		verifiedCandidate.State, verifiedCandidate.Generation, verifiedCandidate.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, result.ResultDigest
	}
	var journal AuthorityTransitionJournal
	if resume == nil {
		journal, err = (AuthorityTransitionJournal{GroupID: groupID, Phase: AuthorityTransitionPrepared,
			CurrentUID: string(currentUID), CurrentRV: currentRV, Before: current, Candidate: verifiedCandidate,
			CanaryResultDigest: result.ResultDigest, PreviousNodes: append([]AuthorityBaselineNodeWitness(nil), baseline.Nodes...),
			CreatedAt: controller.now().UTC().Format(time.RFC3339Nano)}).Seal()
		if err != nil {
			return AuthorityTransitionReceipt{}, err
		}
		if err := controller.store.CreateTransitionJournal(ctx, journal); err != nil {
			return AuthorityTransitionReceipt{}, err
		}
		if candidate.State == CandidateAuthorityLoaded {
			if _, _, err := controller.store.PutCandidate(ctx, verifiedCandidate, candidateUID, candidateRV); err != nil {
				_ = controller.store.DeleteTransitionJournal(context.WithoutCancel(ctx), journal)
				return AuthorityTransitionReceipt{}, err
			}
			candidate = verifiedCandidate
		}
	} else {
		journal = *resume
		if candidate.State == CandidateAuthorityLoaded && journal.Candidate == verifiedCandidate {
			if _, _, err := controller.store.PutCandidate(ctx, verifiedCandidate, candidateUID, candidateRV); err != nil {
				return AuthorityTransitionReceipt{}, err
			}
			candidate = verifiedCandidate
		}
		if journal.Validate() != nil || journal.GroupID != groupID || journal.CurrentUID != string(currentUID) || journal.CurrentRV != currentRV ||
			journal.Before != current || journal.Candidate != candidate || journal.CanaryResultDigest != result.ResultDigest {
			return AuthorityTransitionReceipt{}, errors.New("authority transition resume witness changed")
		}
	}
	next := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: candidate.RecordDigest, CurrentWorkerSlot: candidate.WorkerSlot,
		PreviousRecordDigest: current.CurrentRecordDigest, PreviousWorkerSlot: current.CurrentWorkerSlot,
		AuthorityEpoch: current.AuthorityEpoch + 1, BaselineReceiptDigest: current.BaselineReceiptDigest,
	}
	activator, exists := controller.activators[groupID]
	if !exists {
		return AuthorityTransitionReceipt{}, errors.New("authority group has no production activator")
	}
	transaction, err := activator.BeginPromote(ctx, FrontAuthorityTarget{
		GroupID: groupID, TargetSlot: candidate.WorkerSlot, CandidateBundleGeneration: candidate.BundleGeneration,
		ServingGeneration: candidate.ServingGeneration,
		AuthoritySequence: candidate.AuthoritySequence, PublicationSequence: candidate.CurrentPublicationSequence,
		RecoveryEpoch: candidate.CurrentRecoveryEpoch, PublishedBundleDigest: candidate.CurrentBundleDigest,
		PreviousServingGeneration: candidate.CurrentServingGeneration, CandidateEpoch: candidate.CandidateEpoch,
		PreviousSlot: current.CurrentWorkerSlot, PreviousFrontGeneration: currentFrontGeneration, PreviousBundleGeneration: currentBundleGeneration,
		PreviousWorkerSourceSHA: currentWorkerSourceSHA, PreviousWorkerImageDigest: currentWorkerImageDigest,
		WorkerSourceSHA: result.WorkerSourceSHA, WorkerImageDigest: result.WorkerImageDigest,
		WorkerCohortDigest: result.WorkerCohortDigest, CandidateRecordDigest: candidate.RecordDigest,
		CanaryResultDigest: result.ResultDigest, PreviousNodes: append([]AuthorityBaselineNodeWitness(nil), baseline.Nodes...),
	})
	if err != nil {
		return AuthorityTransitionReceipt{}, fmt.Errorf("activate verified group authority: %w", err)
	}
	activation := transaction.Receipt()
	if journal.Phase == AuthorityTransitionPrepared {
		activatedJournal := journal
		activatedJournal.Phase, activatedJournal.Activation = AuthorityTransitionActivated, &activation
		activatedJournal, err = activatedJournal.Seal()
		if err != nil || controller.store.UpdateTransitionJournal(ctx, journal, activatedJournal) != nil {
			if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
				return AuthorityTransitionReceipt{}, errors.Join(errors.New("persist authority activation witness"), rollbackErr)
			}
			_ = controller.store.DeleteTransitionJournal(context.WithoutCancel(ctx), journal)
			return AuthorityTransitionReceipt{}, errors.New("persist authority activation witness")
		}
		journal = activatedJournal
	} else if journal.Activation == nil || *journal.Activation != activation {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(errors.New("resumed authority activation changed"), rollbackErr)
		}
		return AuthorityTransitionReceipt{}, errors.New("resumed authority activation changed")
	}
	next.CurrentFrontGeneration, next.CurrentBundleGeneration = activation.TargetGeneration, activation.TargetBundleGeneration
	next.CurrentWorkerSourceSHA, next.CurrentWorkerImageDigest = activation.TargetWorkerSourceSHA, activation.TargetWorkerImageDigest
	next.PreviousFrontGeneration, next.PreviousBundleGeneration = activation.PreviousGeneration, activation.PreviousBundleGeneration
	next.PreviousWorkerSourceSHA, next.PreviousWorkerImageDigest = activation.PreviousWorkerSourceSHA, activation.PreviousWorkerImageDigest
	if next.Validate() != nil {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(errors.New("authority activation receipt is invalid"), rollbackErr)
		}
		return AuthorityTransitionReceipt{}, errors.New("authority activation receipt is invalid")
	}
	if _, _, err := controller.store.SwitchCurrent(ctx, next, currentUID, currentRV); err != nil {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(err, fmt.Errorf("authority compensation is unknown: %w", rollbackErr))
		}
		return AuthorityTransitionReceipt{}, err
	}
	if err := transaction.Commit(context.WithoutCancel(ctx)); err != nil {
		return AuthorityTransitionReceipt{}, fmt.Errorf("finalize authority transaction: %w", err)
	}
	if err := controller.store.DeleteTransitionJournal(context.WithoutCancel(ctx), journal); err != nil {
		return AuthorityTransitionReceipt{}, fmt.Errorf("delete completed authority transaction journal: %w", err)
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
		CurrentFrontGeneration: current.PreviousFrontGeneration, CurrentBundleGeneration: current.PreviousBundleGeneration,
		CurrentWorkerSourceSHA: current.PreviousWorkerSourceSHA, CurrentWorkerImageDigest: current.PreviousWorkerImageDigest,
		PreviousRecordDigest: current.CurrentRecordDigest, PreviousWorkerSlot: current.CurrentWorkerSlot,
		PreviousFrontGeneration: current.CurrentFrontGeneration, PreviousBundleGeneration: current.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: current.CurrentWorkerImageDigest,
		AuthorityEpoch: current.AuthorityEpoch + 1, BaselineReceiptDigest: current.BaselineReceiptDigest,
	}
	activator, exists := controller.activators[groupID]
	if !exists {
		return AuthorityTransitionReceipt{}, errors.New("authority group has no production activator")
	}
	transaction, err := activator.BeginRestore(ctx, current)
	if err != nil {
		return AuthorityTransitionReceipt{}, fmt.Errorf("restore group authority: %w", err)
	}
	restore := transaction.Receipt()
	if restore.GroupID != groupID || restore.PreviousSlot != current.CurrentWorkerSlot || restore.TargetSlot != current.PreviousWorkerSlot ||
		restore.PreviousGeneration < current.CurrentFrontGeneration || (restore.PreviousGeneration-current.CurrentFrontGeneration)%2 != 0 ||
		restore.TargetGeneration != restore.PreviousGeneration+1 || restore.PreviousBundleGeneration != current.CurrentBundleGeneration ||
		!restoredBundleGenerationMatches(restore.TargetBundleGeneration, current.PreviousBundleGeneration) ||
		restore.PreviousWorkerSourceSHA != current.CurrentWorkerSourceSHA || restore.PreviousWorkerImageDigest != current.CurrentWorkerImageDigest ||
		restore.TargetWorkerSourceSHA != current.PreviousWorkerSourceSHA || restore.TargetWorkerImageDigest != current.PreviousWorkerImageDigest {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(errors.New("authority restore receipt is invalid"), rollbackErr)
		}
		return AuthorityTransitionReceipt{}, errors.New("authority restore receipt is invalid")
	}
	reverted.CurrentFrontGeneration, reverted.CurrentBundleGeneration = restore.TargetGeneration, restore.TargetBundleGeneration
	reverted.CurrentWorkerSourceSHA, reverted.CurrentWorkerImageDigest = restore.TargetWorkerSourceSHA, restore.TargetWorkerImageDigest
	reverted.PreviousFrontGeneration, reverted.PreviousBundleGeneration = restore.PreviousGeneration, restore.PreviousBundleGeneration
	reverted.PreviousWorkerSourceSHA, reverted.PreviousWorkerImageDigest = restore.PreviousWorkerSourceSHA, restore.PreviousWorkerImageDigest
	if reverted.Validate() != nil {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(errors.New("authority restored pointer is invalid"), rollbackErr)
		}
		return AuthorityTransitionReceipt{}, errors.New("authority restored pointer is invalid")
	}
	if _, _, err := controller.store.SwitchCurrent(ctx, reverted, uid, rv); err != nil {
		if rollbackErr := transaction.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
			return AuthorityTransitionReceipt{}, errors.Join(err, fmt.Errorf("authority revert compensation is unknown: %w", rollbackErr))
		}
		return AuthorityTransitionReceipt{}, err
	}
	if err := transaction.Commit(context.WithoutCancel(ctx)); err != nil {
		return AuthorityTransitionReceipt{}, fmt.Errorf("finalize authority revert: %w", err)
	}
	return (AuthorityTransitionReceipt{
		GroupID: groupID, Action: AuthorityCurrentReverted, CandidateDigest: failedRecordDigest,
		CanaryResultDigest: canaryResultDigest, Before: current, After: reverted,
		ObservedAt: controller.now().UTC().Format(time.RFC3339Nano),
	}).Seal()
}

func restoredBundleGenerationMatches(observed, previous string) bool {
	if strings.TrimSpace(observed) == strings.TrimSpace(previous) && strings.TrimSpace(observed) != "" {
		return true
	}
	observedBase, observedPublication, observedRecovery, observedOK := splitAuthorityBundleGeneration(observed)
	previousBase, previousPublication, previousRecovery, previousOK := splitAuthorityBundleGeneration(previous)
	return observedOK && previousOK && observedBase == previousBase && observedPublication > previousPublication && observedRecovery > previousRecovery
}

func splitAuthorityBundleGeneration(value string) (string, uint64, uint64, bool) {
	value = strings.TrimSpace(value)
	separator := strings.LastIndex(value, ".p")
	recoverySeparator := strings.LastIndex(value, ".r")
	if separator < 1 || recoverySeparator <= separator+2 || recoverySeparator+2 >= len(value) {
		return "", 0, 0, false
	}
	publication, publicationErr := strconv.ParseUint(value[separator+2:recoverySeparator], 10, 64)
	recovery, recoveryErr := strconv.ParseUint(value[recoverySeparator+2:], 10, 64)
	return value[:separator], publication, recovery, publicationErr == nil && recoveryErr == nil && publication > 0
}
