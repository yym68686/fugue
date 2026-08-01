package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const edgeRemediationHeartbeatMaxAge = 60 * time.Second

func (s *Store) AdvanceEdgeRemediation(advance model.EdgeRemediationAdvance) (model.EdgeActivationState, error) {
	advance, err := normalizeEdgeRemediationAdvance(advance)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	if s.usingDatabase() {
		return s.pgAdvanceEdgeRemediation(advance)
	}
	var out model.EdgeActivationState
	err = s.withLockedState(true, func(state *model.State) error {
		if state.EdgeActivation == nil {
			return ErrEdgeInstanceFencingNotReady
		}
		current, err := normalizeStoredEdgeActivation(*state.EdgeActivation)
		if err != nil {
			return err
		}
		if current.Generation != advance.ExpectedActivationGeneration {
			return ErrConflict
		}
		now := time.Now().UTC()
		next, err := buildNextEdgeRemediation(current, advance, state.EdgeNodeInstances, state.EdgeActiveEpochs, now)
		if err != nil {
			return err
		}
		state.EdgeActivation = &next
		out = next
		return nil
	})
	return out, err
}

func buildNextEdgeRemediation(current model.EdgeActivationState, advance model.EdgeRemediationAdvance, instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, now time.Time) (model.EdgeActivationState, error) {
	if current.RouteAuthority != model.EdgeRouteAuthorityActiveEpoch || (current.Phase != model.EdgeActivationPhaseActive && current.Phase != model.EdgeActivationPhaseEnforced) || current.PlanDigest == "" || current.ReleaseID == "" {
		return model.EdgeActivationState{}, ErrConflict
	}
	if advance.ToPhase != model.EdgeRemediationPhaseRollbackPending {
		if err := verifyActiveEpochsForRemediation(instances, epochs, now); err != nil {
			return model.EdgeActivationState{}, err
		}
	}
	latestSequence := uint64(0)
	if current.Remediation != nil {
		latestSequence = current.Remediation.Sequence
	}
	if advance.ExpectedActionSequence != latestSequence {
		return model.EdgeActivationState{}, ErrConflict
	}
	next := current
	switch advance.ToPhase {
	case model.EdgeRemediationPhasePrepared:
		if current.Remediation != nil && current.Remediation.Phase != model.EdgeRemediationPhaseVerified {
			return model.EdgeActivationState{}, ErrConflict
		}
		if err := verifyInactiveRemediationTarget(advance.Target, instances, epochs); err != nil {
			return model.EdgeActivationState{}, err
		}
		if current.Remediation != nil && current.Remediation.Nonce == advance.Nonce {
			return model.EdgeActivationState{}, ErrConflict
		}
		for _, previous := range current.RemediationHistory {
			if previous.Nonce == advance.Nonce {
				return model.EdgeActivationState{}, ErrConflict
			}
		}
		if current.Remediation != nil {
			next.RemediationHistory = append(next.RemediationHistory, *current.Remediation)
			if len(next.RemediationHistory) > 64 {
				next.RemediationHistory = append([]model.EdgeRemediationAction(nil), next.RemediationHistory[len(next.RemediationHistory)-64:]...)
			}
		}
		next.Remediation = &model.EdgeRemediationAction{
			Schema: model.EdgeRemediationSchemaV1, Sequence: latestSequence + 1, Phase: model.EdgeRemediationPhasePrepared,
			Nonce: advance.Nonce, ReleaseFence: advance.ReleaseFence, AuthorizationDigest: advance.AuthorizationDigest,
			AuthorizationKeyID: advance.AuthorizationKeyID, AuthorizationKeyGeneration: advance.AuthorizationKeyGeneration, AuthorizationRunnerObservedSecretUID: advance.AuthorizationRunnerObservedSecretUID, AuthorizationRunnerObservedSecretVersion: advance.AuthorizationRunnerObservedSecretVersion,
			ActivationGeneration: current.Generation, PlanDigest: current.PlanDigest, ReleaseID: current.ReleaseID,
			ActiveEvidenceDigest: advance.ActiveEvidenceDigest, SyntheticDigest: advance.SyntheticDigest, KubernetesDigest: advance.KubernetesDigest,
			Target: advance.Target, Actor: advance.Actor, CreatedAt: now, UpdatedAt: now,
		}
	case model.EdgeRemediationPhaseCommitted, model.EdgeRemediationPhaseVerified, model.EdgeRemediationPhaseRollbackPending:
		wantCurrent := model.EdgeRemediationPhasePrepared
		if advance.ToPhase == model.EdgeRemediationPhaseVerified || advance.ToPhase == model.EdgeRemediationPhaseRollbackPending {
			wantCurrent = model.EdgeRemediationPhaseCommitted
		}
		if current.Remediation == nil || current.Remediation.Phase != wantCurrent || current.Remediation.Nonce != advance.Nonce || current.Remediation.ReleaseFence != advance.ReleaseFence || current.Remediation.Target != advance.Target || current.Remediation.ActiveEvidenceDigest != advance.ActiveEvidenceDigest || current.Remediation.SyntheticDigest != advance.SyntheticDigest || current.Remediation.KubernetesDigest != advance.KubernetesDigest {
			return model.EdgeActivationState{}, ErrConflict
		}
		committed := *current.Remediation
		committed.Phase = advance.ToPhase
		committed.AuthorizationDigest = advance.AuthorizationDigest
		committed.AuthorizationKeyID = advance.AuthorizationKeyID
		committed.AuthorizationKeyGeneration = advance.AuthorizationKeyGeneration
		committed.AuthorizationRunnerObservedSecretUID = advance.AuthorizationRunnerObservedSecretUID
		committed.AuthorizationRunnerObservedSecretVersion = advance.AuthorizationRunnerObservedSecretVersion
		committed.UpdatedAt = now
		next.Remediation = &committed
	default:
		return model.EdgeActivationState{}, ErrInvalidInput
	}
	next.Generation++
	next.UpdatedAt = now
	return next, nil
}

func verifyActiveEpochsForRemediation(instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, now time.Time) error {
	if len(epochs) == 0 {
		return ErrEdgeInstanceFencingNotReady
	}
	for _, epoch := range epochs {
		eligible := 0
		for _, instance := range instances {
			if !edgeInstanceMatchesActiveEpoch(instance, epoch) {
				continue
			}
			if instance.LastHeartbeatAt.IsZero() || instance.LastHeartbeatAt.After(now.Add(5*time.Second)) || now.Sub(instance.LastHeartbeatAt) > edgeRemediationHeartbeatMaxAge || !instance.EffectiveHealthy || instance.ConsecutiveHealthy < edgeInstanceHealthyObservationsRequired || edgeInstanceHardFailure(instance) || instance.Node.Draining || model.NormalizeEdgeTLSStatus(instance.Node.TLSStatus) != model.EdgeTLSStatusReady {
				continue
			}
			eligible++
		}
		if epoch.MinHealthyInstances <= 0 || eligible < epoch.MinHealthyInstances {
			return fmt.Errorf("%w: active epoch is not fresh and healthy", ErrEdgeInstanceFencingNotReady)
		}
	}
	return nil
}

func verifyInactiveRemediationTarget(target model.EdgeRemediationTarget, instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch) error {
	var groupEpoch *model.EdgeActiveEpoch
	for index := range epochs {
		if epochs[index].EdgeGroupID == target.EdgeGroupID {
			if groupEpoch != nil {
				return ErrConflict
			}
			copy := epochs[index]
			groupEpoch = &copy
		}
	}
	if groupEpoch == nil || (target.Slot == groupEpoch.Slot && target.ReleaseEpoch == groupEpoch.ReleaseEpoch) {
		return fmt.Errorf("%w: remediation target is active or has no active peer", ErrConflict)
	}
	if target.FailureClass == model.EdgeRemediationReasonCrashLoop {
		return nil
	}
	for _, instance := range instances {
		if instance.EdgeID == target.EdgeID && instance.EdgeGroupID == target.EdgeGroupID && instance.Slot == target.Slot && instance.InstanceUID == target.InstanceUID && instance.ReleaseEpoch == target.ReleaseEpoch && instance.FailureClass == target.FailureClass && edgeInstanceHardFailure(instance) {
			return nil
		}
	}
	return fmt.Errorf("%w: inactive hard-failure evidence is absent", ErrEdgeInstanceFencingNotReady)
}

func normalizeEdgeRemediationAdvance(advance model.EdgeRemediationAdvance) (model.EdgeRemediationAdvance, error) {
	advance.ToPhase = strings.TrimSpace(strings.ToLower(advance.ToPhase))
	advance.ActiveEvidenceDigest = normalizeEdgeEvidenceDigest(advance.ActiveEvidenceDigest)
	advance.SyntheticDigest = normalizeEdgeEvidenceDigest(advance.SyntheticDigest)
	advance.KubernetesDigest = normalizeEdgeEvidenceDigest(advance.KubernetesDigest)
	advance.ReleaseFence = normalizeEdgeInstanceToken(advance.ReleaseFence, 512)
	advance.Nonce = normalizeEdgeEvidenceDigest(advance.Nonce)
	advance.AuthorizationDigest = normalizeEdgeEvidenceDigest(advance.AuthorizationDigest)
	advance.AuthorizationKeyID = normalizeEdgeInstanceToken(advance.AuthorizationKeyID, 128)
	advance.AuthorizationKeyGeneration = normalizeEdgeInstanceToken(advance.AuthorizationKeyGeneration, 128)
	advance.AuthorizationRunnerObservedSecretUID = normalizeEdgeInstanceToken(advance.AuthorizationRunnerObservedSecretUID, 128)
	advance.AuthorizationRunnerObservedSecretVersion = normalizeEdgeInstanceToken(advance.AuthorizationRunnerObservedSecretVersion, 128)
	advance.Actor = strings.TrimSpace(advance.Actor)
	target := &advance.Target
	target.EdgeID = normalizeEdgeID(target.EdgeID)
	target.EdgeGroupID = normalizeEdgeGroupID(target.EdgeGroupID)
	target.Slot = normalizeEdgeSlot(target.Slot, false)
	target.InstanceUID = normalizeEdgeInstanceToken(target.InstanceUID, 128)
	target.ReleaseEpoch = normalizeEdgeInstanceToken(target.ReleaseEpoch, 256)
	target.DaemonSetName = normalizeEdgeInstanceToken(target.DaemonSetName, 253)
	target.DaemonSetUID = normalizeEdgeInstanceToken(target.DaemonSetUID, 128)
	target.DaemonSetVersion = normalizeEdgeInstanceToken(target.DaemonSetVersion, 128)
	target.FailureClass = strings.TrimSpace(strings.ToLower(target.FailureClass))
	if advance.ExpectedActivationGeneration == 0 || advance.Actor == "" || advance.ReleaseFence == "" || advance.Nonce == "" || advance.AuthorizationDigest == "" || advance.AuthorizationKeyID == "" || advance.AuthorizationKeyGeneration == "" || advance.AuthorizationRunnerObservedSecretUID == "" || advance.AuthorizationRunnerObservedSecretVersion == "" || advance.ActiveEvidenceDigest == "" || advance.SyntheticDigest == "" || advance.KubernetesDigest == "" || (advance.ToPhase != model.EdgeRemediationPhasePrepared && advance.ToPhase != model.EdgeRemediationPhaseCommitted && advance.ToPhase != model.EdgeRemediationPhaseVerified && advance.ToPhase != model.EdgeRemediationPhaseRollbackPending) || target.EdgeID == "" || target.EdgeGroupID == "" || target.Slot == "" || target.InstanceUID == "" || target.ReleaseEpoch == "" || target.DaemonSetName == "" || target.DaemonSetUID == "" || target.DaemonSetVersion == "" || !validEdgeRemediationFailure(target.FailureClass) {
		return model.EdgeRemediationAdvance{}, ErrInvalidInput
	}
	return advance, nil
}

func validEdgeRemediationFailure(value string) bool {
	switch value {
	case model.EdgeInstanceFailureSignatureInvalid, model.EdgeInstanceFailureMaxStaleExceeded, model.EdgeInstanceFailureIdentityDrift, model.EdgeRemediationReasonCrashLoop:
		return true
	default:
		return false
	}
}

func sortedRemediationTargets(items []model.EdgeRemediationTarget) {
	sort.Slice(items, func(i, j int) bool { return items[i].DaemonSetName < items[j].DaemonSetName })
}
