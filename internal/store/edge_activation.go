package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const edgeFlatWriteFenceMetaKey = "edge_instance_flat_write_fence"

func defaultEdgeActivationState(now time.Time) model.EdgeActivationState {
	return model.EdgeActivationState{
		Schema:         model.EdgeActivationSchemaV1,
		Phase:          model.EdgeActivationPhaseLegacyAuthoritative,
		RouteAuthority: model.EdgeRouteAuthorityLegacy,
		Generation:     1,
		CreatedAt:      now.UTC(),
		UpdatedAt:      now.UTC(),
	}
}

func (s *Store) GetEdgeActivationState() (model.EdgeActivationState, error) {
	if s.usingDatabase() {
		return s.pgGetEdgeActivationState()
	}
	var out model.EdgeActivationState
	err := s.withLockedState(false, func(state *model.State) error {
		if state.EdgeActivation == nil {
			return ErrEdgeInstanceFencingNotReady
		}
		var err error
		out, err = normalizeStoredEdgeActivation(*state.EdgeActivation)
		return err
	})
	return out, err
}

func (s *Store) AdvanceEdgeActivation(advance model.EdgeActivationAdvance) (model.EdgeActivationState, error) {
	advance, err := normalizeEdgeActivationAdvance(advance)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	if s.usingDatabase() {
		return s.pgAdvanceEdgeActivation(advance)
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
		if current.Generation != advance.ExpectedGeneration {
			return ErrConflict
		}
		now := time.Now().UTC()
		next, err := buildNextEdgeActivation(current, advance, state.EdgeNodeInstances, state.EdgeActiveEpochs, now)
		if err != nil {
			return err
		}
		if advance.ToPhase == model.EdgeActivationPhaseActive {
			next.PreviousActiveEpochs = append([]model.EdgeActiveEpoch(nil), state.EdgeActiveEpochs...)
			state.EdgeActiveEpochs = append([]model.EdgeActiveEpoch(nil), current.CandidateEpochs...)
		}
		if advance.ToPhase == model.EdgeActivationActionRollback && current.Rollback != nil {
			state.EdgeActiveEpochs = append([]model.EdgeActiveEpoch(nil), current.Rollback.ActiveEpochs...)
		}
		state.EdgeActivation = &next
		out = next
		return nil
	})
	return out, err
}

func buildNextEdgeActivation(current model.EdgeActivationState, advance model.EdgeActivationAdvance, instances []model.EdgeNodeInstance, currentEpochs []model.EdgeActiveEpoch, now time.Time) (model.EdgeActivationState, error) {
	if current.Remediation != nil && current.Remediation.Phase != model.EdgeRemediationPhaseVerified {
		return model.EdgeActivationState{}, ErrConflict
	}
	if !allowedEdgeActivationTransition(current.Phase, advance.ToPhase) {
		return model.EdgeActivationState{}, ErrConflict
	}
	for _, receipt := range current.Receipts {
		if receipt.PhaseNonce == advance.PhaseNonce {
			return model.EdgeActivationState{}, ErrConflict
		}
	}
	if advance.ToPhase == model.EdgeActivationActionRollback {
		if current.Rollback == nil || advance.EvidenceDigest == "" || advance.PlanDigest != current.PlanDigest || advance.ReleaseID != current.ReleaseID || advance.ReleaseRecordUID != current.ReleaseRecordUID || advance.ReleaseRecordVersion != current.ReleaseRecordVersion || advance.ReleaseRecordDigest != current.ReleaseRecordDigest {
			return model.EdgeActivationState{}, ErrConflict
		}
		snapshot := current.Rollback
		next := current
		next.Phase = snapshot.Phase
		next.RouteAuthority = snapshot.RouteAuthority
		next.PlanDigest = snapshot.PlanDigest
		next.ReleaseID = snapshot.ReleaseID
		next.ReleaseRecordUID = snapshot.ReleaseRecordUID
		next.ReleaseRecordVersion = snapshot.ReleaseRecordVersion
		next.ReleaseRecordDigest = snapshot.ReleaseRecordDigest
		next.ExpectedInstances = append([]model.EdgeExpectedInstance(nil), snapshot.ExpectedInstances...)
		next.CandidateEpochs = nil
		next.PreviousActiveEpochs = nil
		next.PreviousAuthority = ""
		next.LegacySnapshotDigest = snapshot.LegacySnapshotDigest
		next.APIReplicaGeneration = snapshot.APIReplicaGeneration
		next.SoakStartedAt = snapshot.SoakStartedAt
		next.Rollback = nil
		next.Generation++
		next.UpdatedAt = now
		next.Receipts = append(next.Receipts, model.EdgeActivationReceipt{Sequence: next.Generation, FromPhase: current.Phase, ToPhase: next.Phase, PlanDigest: advance.PlanDigest, EvidenceDigest: advance.EvidenceDigest, ReleaseID: advance.ReleaseID, Actor: advance.Actor, ReleaseFence: advance.ReleaseFence, PhaseNonce: advance.PhaseNonce, Authorization: advance.AuthorizationDigest, KeyID: advance.AuthorizationKeyID, KeyGeneration: advance.AuthorizationKeyGeneration, RunnerObservedSecretUID: advance.AuthorizationRunnerObservedSecretUID, RunnerObservedSecretVersion: advance.AuthorizationRunnerObservedSecretVersion, RecordedAt: now})
		return next, nil
	}
	if advance.ToPhase != model.EdgeActivationPhaseLegacyAuthoritative {
		if advance.PlanDigest == "" || advance.EvidenceDigest == "" || advance.ReleaseID == "" || advance.ReleaseRecordUID == "" || advance.ReleaseRecordVersion == "" || advance.ReleaseRecordDigest == "" || advance.LegacySnapshotDigest == "" {
			return model.EdgeActivationState{}, ErrInvalidInput
		}
	}
	if current.Phase != model.EdgeActivationPhaseLegacyAuthoritative && advance.ToPhase != model.EdgeActivationPhaseLegacyAuthoritative && advance.ToPhase != model.EdgeActivationPhaseShadow {
		if advance.PlanDigest != current.PlanDigest || advance.ReleaseID != current.ReleaseID || advance.ReleaseRecordUID != current.ReleaseRecordUID || advance.ReleaseRecordVersion != current.ReleaseRecordVersion || advance.ReleaseRecordDigest != current.ReleaseRecordDigest || advance.LegacySnapshotDigest != current.LegacySnapshotDigest {
			return model.EdgeActivationState{}, ErrConflict
		}
	}
	if advance.ToPhase == model.EdgeActivationPhaseFenced {
		if err := verifyEdgeFenceCandidates(advance.ExpectedInstances, advance.ActiveEpochs, instances); err != nil {
			return model.EdgeActivationState{}, err
		}
	}
	if advance.ToPhase == model.EdgeActivationPhaseActive {
		if strings.TrimSpace(advance.APIReplicaGeneration) == "" || !sameExpectedEdgeInstances(current.ExpectedInstances, advance.ExpectedInstances) || len(current.CandidateEpochs) == 0 {
			return model.EdgeActivationState{}, ErrConflict
		}
	}
	if advance.ToPhase == model.EdgeActivationPhaseEnforced && current.SoakStartedAt == nil {
		return model.EdgeActivationState{}, ErrConflict
	}

	next := current
	next.Phase = advance.ToPhase
	next.Generation++
	if advance.ToPhase == model.EdgeActivationPhaseLegacyAuthoritative {
		next.RouteAuthority = model.EdgeRouteAuthorityLegacy
		next.PlanDigest = ""
		next.ReleaseID = ""
		next.ReleaseRecordUID = ""
		next.ReleaseRecordVersion = ""
		next.ReleaseRecordDigest = ""
		next.ExpectedInstances = nil
		next.CandidateEpochs = nil
		next.APIReplicaGeneration = ""
		next.SoakStartedAt = nil
	} else {
		if advance.ToPhase == model.EdgeActivationPhaseShadow {
			next.Rollback = &model.EdgeActivationSnapshot{Phase: current.Phase, RouteAuthority: current.RouteAuthority, PlanDigest: current.PlanDigest, ReleaseID: current.ReleaseID, ReleaseRecordUID: current.ReleaseRecordUID, ReleaseRecordVersion: current.ReleaseRecordVersion, ReleaseRecordDigest: current.ReleaseRecordDigest, ExpectedInstances: append([]model.EdgeExpectedInstance(nil), current.ExpectedInstances...), ActiveEpochs: append([]model.EdgeActiveEpoch(nil), currentEpochs...), LegacySnapshotDigest: current.LegacySnapshotDigest, APIReplicaGeneration: current.APIReplicaGeneration, SoakStartedAt: current.SoakStartedAt}
		}
		next.PlanDigest = advance.PlanDigest
		next.ReleaseID = advance.ReleaseID
		next.ReleaseRecordUID = advance.ReleaseRecordUID
		next.ReleaseRecordVersion = advance.ReleaseRecordVersion
		next.ReleaseRecordDigest = advance.ReleaseRecordDigest
		next.LegacySnapshotDigest = advance.LegacySnapshotDigest
		if len(advance.ExpectedInstances) > 0 {
			next.ExpectedInstances = append([]model.EdgeExpectedInstance(nil), advance.ExpectedInstances...)
		}
		if advance.ToPhase == model.EdgeActivationPhaseFenced {
			next.CandidateEpochs = append([]model.EdgeActiveEpoch(nil), advance.ActiveEpochs...)
		}
		if advance.APIReplicaGeneration != "" {
			next.APIReplicaGeneration = advance.APIReplicaGeneration
		}
		if advance.ToPhase == model.EdgeActivationPhaseActive {
			next.PreviousAuthority = current.RouteAuthority
			next.RouteAuthority = model.EdgeRouteAuthorityActiveEpoch
			next.CandidateEpochs = nil
			copy := now
			next.SoakStartedAt = &copy
		}
	}
	next.UpdatedAt = now
	next.Receipts = append(next.Receipts, model.EdgeActivationReceipt{
		Sequence:                    next.Generation,
		FromPhase:                   current.Phase,
		ToPhase:                     next.Phase,
		PlanDigest:                  advance.PlanDigest,
		EvidenceDigest:              advance.EvidenceDigest,
		ReleaseID:                   advance.ReleaseID,
		Actor:                       advance.Actor,
		ReleaseFence:                advance.ReleaseFence,
		PhaseNonce:                  advance.PhaseNonce,
		Authorization:               advance.AuthorizationDigest,
		KeyID:                       advance.AuthorizationKeyID,
		KeyGeneration:               advance.AuthorizationKeyGeneration,
		RunnerObservedSecretUID:     advance.AuthorizationRunnerObservedSecretUID,
		RunnerObservedSecretVersion: advance.AuthorizationRunnerObservedSecretVersion,
		RecordedAt:                  now,
	})
	return next, nil
}

func allowedEdgeActivationTransition(from, to string) bool {
	if to == model.EdgeActivationActionRollback {
		return from == model.EdgeActivationPhaseShadow || from == model.EdgeActivationPhaseFenced || from == model.EdgeActivationPhaseActive
	}
	if to == model.EdgeActivationPhaseLegacyAuthoritative {
		return from == model.EdgeActivationPhaseShadow || from == model.EdgeActivationPhaseFenced || from == model.EdgeActivationPhaseActive || from == model.EdgeActivationPhaseEnforced
	}
	switch from {
	case model.EdgeActivationPhaseLegacyAuthoritative:
		return to == model.EdgeActivationPhaseShadow
	case model.EdgeActivationPhaseShadow:
		return to == model.EdgeActivationPhaseFenced
	case model.EdgeActivationPhaseFenced:
		return to == model.EdgeActivationPhaseActive
	case model.EdgeActivationPhaseActive:
		return to == model.EdgeActivationPhaseEnforced
	case model.EdgeActivationPhaseEnforced:
		return to == model.EdgeActivationPhaseShadow
	default:
		return false
	}
}

func verifyEdgeFenceCandidates(expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch, instances []model.EdgeNodeInstance) error {
	if len(expected) == 0 || len(epochs) == 0 {
		return ErrInvalidInput
	}
	epochByGroup := make(map[string]model.EdgeActiveEpoch, len(epochs))
	for _, raw := range epochs {
		epoch, err := normalizeEdgeActiveEpoch(raw)
		if err != nil {
			return err
		}
		if _, ok := epochByGroup[epoch.EdgeGroupID]; ok {
			return ErrConflict
		}
		epochByGroup[epoch.EdgeGroupID] = epoch
	}
	instanceByKey := make(map[string]model.EdgeNodeInstance, len(instances))
	for _, instance := range instances {
		instanceByKey[edgeNodeInstanceKey(instance)] = instance
	}
	groups := map[string]int{}
	for _, item := range expected {
		needle := model.EdgeNodeInstance{EdgeID: item.EdgeID, EdgeGroupID: item.EdgeGroupID, Slot: item.Slot, InstanceUID: item.InstanceUID, ReleaseEpoch: item.ReleaseEpoch}
		key := edgeNodeInstanceKey(needle)
		instance, ok := instanceByKey[key]
		if !ok {
			return fmt.Errorf("%w: expected edge instance is missing", ErrEdgeInstanceFencingNotReady)
		}
		epoch, ok := epochByGroup[item.EdgeGroupID]
		if !ok || !edgeInstanceMatchesActiveEpoch(instance, epoch) || !instance.EffectiveHealthy || instance.ConsecutiveHealthy < edgeInstanceHealthyObservationsRequired || edgeInstanceHardFailure(instance) || instance.Node.Draining || model.NormalizeEdgeTLSStatus(instance.Node.TLSStatus) != model.EdgeTLSStatusReady {
			return fmt.Errorf("%w: expected edge instance is not eligible", ErrEdgeInstanceFencingNotReady)
		}
		groups[item.EdgeGroupID]++
	}
	for groupID, epoch := range epochByGroup {
		if groups[groupID] < epoch.MinHealthyInstances {
			return fmt.Errorf("%w: active epoch has insufficient expected instances", ErrEdgeInstanceFencingNotReady)
		}
	}
	return nil
}

func normalizeEdgeActivationAdvance(advance model.EdgeActivationAdvance) (model.EdgeActivationAdvance, error) {
	advance.ToPhase = strings.TrimSpace(strings.ToLower(advance.ToPhase))
	advance.PlanDigest = normalizeEdgeEvidenceDigest(advance.PlanDigest)
	advance.EvidenceDigest = normalizeEdgeEvidenceDigest(advance.EvidenceDigest)
	advance.ReleaseRecordDigest = normalizeEdgeEvidenceDigest(advance.ReleaseRecordDigest)
	advance.LegacySnapshotDigest = normalizeEdgeEvidenceDigest(advance.LegacySnapshotDigest)
	advance.ReleaseID = normalizeEdgeInstanceToken(advance.ReleaseID, 256)
	advance.ReleaseRecordUID = normalizeEdgeInstanceToken(advance.ReleaseRecordUID, 128)
	advance.ReleaseRecordVersion = normalizeEdgeInstanceToken(advance.ReleaseRecordVersion, 128)
	advance.APIReplicaGeneration = normalizeEdgeInstanceToken(advance.APIReplicaGeneration, 256)
	advance.Actor = strings.TrimSpace(advance.Actor)
	advance.ReleaseFence = normalizeEdgeInstanceToken(advance.ReleaseFence, 512)
	advance.PhaseNonce = normalizeEdgeEvidenceDigest(advance.PhaseNonce)
	advance.AuthorizationDigest = normalizeEdgeEvidenceDigest(advance.AuthorizationDigest)
	advance.AuthorizationKeyID = normalizeEdgeInstanceToken(advance.AuthorizationKeyID, 128)
	advance.AuthorizationKeyGeneration = normalizeEdgeInstanceToken(advance.AuthorizationKeyGeneration, 128)
	advance.AuthorizationRunnerObservedSecretUID = normalizeEdgeInstanceToken(advance.AuthorizationRunnerObservedSecretUID, 128)
	advance.AuthorizationRunnerObservedSecretVersion = normalizeEdgeInstanceToken(advance.AuthorizationRunnerObservedSecretVersion, 128)
	if advance.ExpectedGeneration == 0 || advance.Actor == "" || advance.ReleaseFence == "" || advance.PhaseNonce == "" || advance.AuthorizationDigest == "" || advance.AuthorizationKeyID == "" || advance.AuthorizationKeyGeneration == "" || advance.AuthorizationRunnerObservedSecretUID == "" || advance.AuthorizationRunnerObservedSecretVersion == "" || (!validEdgeActivationPhase(advance.ToPhase) && advance.ToPhase != model.EdgeActivationActionRollback) {
		return model.EdgeActivationAdvance{}, ErrInvalidInput
	}
	for index := range advance.ExpectedInstances {
		item := &advance.ExpectedInstances[index]
		item.EdgeID = normalizeEdgeID(item.EdgeID)
		item.EdgeGroupID = normalizeEdgeGroupID(item.EdgeGroupID)
		item.Slot = normalizeEdgeSlot(item.Slot, false)
		item.InstanceUID = normalizeEdgeInstanceToken(item.InstanceUID, 128)
		item.ReleaseEpoch = normalizeEdgeInstanceToken(item.ReleaseEpoch, 256)
		if item.EdgeID == "" || item.EdgeGroupID == "" || item.Slot == "" || item.InstanceUID == "" || item.ReleaseEpoch == "" {
			return model.EdgeActivationAdvance{}, ErrInvalidInput
		}
	}
	sortExpectedEdgeInstances(advance.ExpectedInstances)
	for index := 1; index < len(advance.ExpectedInstances); index++ {
		if edgeExpectedInstanceKey(advance.ExpectedInstances[index-1]) == edgeExpectedInstanceKey(advance.ExpectedInstances[index]) {
			return model.EdgeActivationAdvance{}, ErrConflict
		}
	}
	return advance, nil
}

func normalizeStoredEdgeActivation(state model.EdgeActivationState) (model.EdgeActivationState, error) {
	if state.Schema != model.EdgeActivationSchemaV1 || !validEdgeActivationPhase(state.Phase) || (state.RouteAuthority != model.EdgeRouteAuthorityLegacy && state.RouteAuthority != model.EdgeRouteAuthorityActiveEpoch) || state.Generation == 0 || state.CreatedAt.IsZero() || state.UpdatedAt.IsZero() {
		return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
	}
	if (state.Phase == model.EdgeActivationPhaseActive || state.Phase == model.EdgeActivationPhaseEnforced) != (state.RouteAuthority == model.EdgeRouteAuthorityActiveEpoch) {
		return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
	}
	seenNonces := map[string]struct{}{}
	lastSequence := uint64(0)
	for _, receipt := range state.Receipts {
		if receipt.Sequence <= lastSequence || receipt.Sequence > state.Generation || normalizeEdgeEvidenceDigest(receipt.PlanDigest) == "" || normalizeEdgeEvidenceDigest(receipt.EvidenceDigest) == "" || normalizeEdgeEvidenceDigest(receipt.PhaseNonce) == "" || normalizeEdgeEvidenceDigest(receipt.Authorization) == "" || normalizeEdgeInstanceToken(receipt.ReleaseFence, 512) == "" || normalizeEdgeInstanceToken(receipt.KeyID, 128) == "" || normalizeEdgeInstanceToken(receipt.KeyGeneration, 128) == "" || normalizeEdgeInstanceToken(receipt.RunnerObservedSecretUID, 128) == "" || normalizeEdgeInstanceToken(receipt.RunnerObservedSecretVersion, 128) == "" {
			return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
		}
		if _, exists := seenNonces[receipt.PhaseNonce]; exists {
			return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
		}
		seenNonces[receipt.PhaseNonce] = struct{}{}
		lastSequence = receipt.Sequence
	}
	if state.Remediation != nil {
		action := state.Remediation
		if action.Schema != model.EdgeRemediationSchemaV1 || action.Sequence == 0 || (action.Phase != model.EdgeRemediationPhasePrepared && action.Phase != model.EdgeRemediationPhaseCommitted && action.Phase != model.EdgeRemediationPhaseVerified && action.Phase != model.EdgeRemediationPhaseRollbackPending) || normalizeEdgeEvidenceDigest(action.Nonce) == "" || normalizeEdgeEvidenceDigest(action.AuthorizationDigest) == "" || action.PlanDigest != state.PlanDigest || action.ReleaseID != state.ReleaseID || action.CreatedAt.IsZero() || action.UpdatedAt.IsZero() {
			return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
		}
	}
	sortExpectedEdgeInstances(state.ExpectedInstances)
	return state, nil
}

func validEdgeActivationPhase(phase string) bool {
	switch phase {
	case model.EdgeActivationPhaseLegacyAuthoritative, model.EdgeActivationPhaseShadow, model.EdgeActivationPhaseFenced, model.EdgeActivationPhaseActive, model.EdgeActivationPhaseEnforced:
		return true
	default:
		return false
	}
}

func edgeActivationUsesInstanceRoutes(state model.EdgeActivationState) bool {
	return state.RouteAuthority == model.EdgeRouteAuthorityActiveEpoch
}

func normalizeEdgeEvidenceDigest(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return ""
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:")); err != nil {
		return ""
	}
	return value
}

func edgeExpectedInstanceKey(item model.EdgeExpectedInstance) string {
	return strings.Join([]string{item.EdgeID, item.EdgeGroupID, item.Slot, item.InstanceUID, item.ReleaseEpoch}, "\x00")
}

func sortExpectedEdgeInstances(items []model.EdgeExpectedInstance) {
	sort.Slice(items, func(i, j int) bool { return edgeExpectedInstanceKey(items[i]) < edgeExpectedInstanceKey(items[j]) })
}

func sameExpectedEdgeInstances(left, right []model.EdgeExpectedInstance) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if edgeExpectedInstanceKey(left[index]) != edgeExpectedInstanceKey(right[index]) {
			return false
		}
	}
	return true
}
