package store

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func validateEdgeExclusionClearMaterial(activation model.EdgeActivationState, instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, edgeIDs, edgeGroupIDs []string, now time.Time) error {
	if activation.RouteAuthority != model.EdgeRouteAuthorityActiveEpoch || activation.Phase != model.EdgeActivationPhaseEnforced {
		return fmt.Errorf("%w: edge exclusion clear requires fully enforced active-epoch route authority with no release transition", ErrConflict)
	}
	if activation.Remediation != nil && activation.Remediation.Phase != model.EdgeRemediationPhaseVerified {
		return fmt.Errorf("%w: edge exclusion clear conflicts with active remediation", ErrConflict)
	}
	epochByGroup := make(map[string]model.EdgeActiveEpoch, len(epochs))
	for _, epoch := range epochs {
		groupID := normalizeEdgeGroupID(epoch.EdgeGroupID)
		if groupID == "" {
			return fmt.Errorf("%w: edge exclusion clear active epoch identity is incomplete", ErrConflict)
		}
		if _, exists := epochByGroup[groupID]; exists {
			return fmt.Errorf("%w: edge exclusion clear active epoch identity is ambiguous", ErrConflict)
		}
		epochByGroup[groupID] = epoch
	}
	requestedEdges := edgeExclusionIDSet(edgeIDs, false)
	requestedGroups := edgeExclusionIDSet(edgeGroupIDs, true)
	matchedEdges := make(map[string]struct{}, len(requestedEdges))
	matchedGroups := make(map[string]int, len(requestedGroups))
	for _, instance := range instances {
		groupID := normalizeEdgeGroupID(instance.EdgeGroupID)
		epoch, ok := epochByGroup[groupID]
		if !ok || instance.Slot != epoch.Slot || instance.ReleaseEpoch != epoch.ReleaseEpoch || !edgeExclusionInstanceClearEligible(instance, now) {
			continue
		}
		edgeID := normalizeEdgeID(instance.EdgeID)
		if _, ok := requestedEdges[edgeID]; ok {
			matchedEdges[edgeID] = struct{}{}
		}
		if _, ok := requestedGroups[groupID]; ok {
			matchedGroups[groupID]++
		}
	}
	for edgeID := range requestedEdges {
		if _, ok := matchedEdges[edgeID]; !ok {
			return fmt.Errorf("%w: edge exclusion clear lacks fresh TLS-ready active identity for edge %s", ErrConflict, edgeID)
		}
	}
	for groupID := range requestedGroups {
		epoch, ok := epochByGroup[groupID]
		minimum := 1
		if ok && epoch.MinHealthyInstances > minimum {
			minimum = epoch.MinHealthyInstances
		}
		if !ok || matchedGroups[groupID] < minimum {
			return fmt.Errorf("%w: edge exclusion clear lacks fresh healthy active epoch for group %s", ErrConflict, groupID)
		}
	}
	return nil
}

func edgeExclusionInstanceClearEligible(instance model.EdgeNodeInstance, now time.Time) bool {
	return instance.EffectiveHealthy && instance.ConsecutiveHealthy >= 2 && strings.TrimSpace(instance.FailureClass) == "" && !instance.Node.Draining && model.NormalizeEdgeTLSStatus(instance.Node.TLSStatus) == model.EdgeTLSStatusReady && !instance.LastHeartbeatAt.IsZero() && !instance.LastHeartbeatAt.After(now.Add(5*time.Second)) && now.Sub(instance.LastHeartbeatAt) <= time.Minute
}

func edgeExclusionIDSet(values []string, group bool) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if group {
			value = normalizeEdgeGroupID(value)
		} else {
			value = normalizeEdgeID(value)
		}
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func removedEdgeExclusionIDs(before, after []string, group bool) []string {
	remaining := edgeExclusionIDSet(after, group)
	removed := make([]string, 0)
	for value := range edgeExclusionIDSet(before, group) {
		if _, ok := remaining[value]; !ok {
			removed = append(removed, value)
		}
	}
	return removed
}

func validateFileEdgeExclusionClearState(state *model.State, edgeIDs, edgeGroupIDs []string, now time.Time) error {
	if state == nil || state.EdgeActivation == nil {
		return fmt.Errorf("%w: edge exclusion clear activation state is missing", ErrConflict)
	}
	if err := verifyEdgeActivationReadiness(state); err != nil {
		return err
	}
	return validateEdgeExclusionClearMaterial(*state.EdgeActivation, state.EdgeNodeInstances, state.EdgeActiveEpochs, edgeIDs, edgeGroupIDs, now)
}

// CheckEdgeRoutePolicyClearEvidence returns a point-in-time read-only view. The
// mutation paths repeat this check under the same durable transaction as CAS.
func (s *Store) CheckEdgeRoutePolicyClearEvidence(hostname string, edgeIDs, edgeGroupIDs []string) error {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	if hostname == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCheckEdgeRoutePolicyClearEvidence(hostname, edgeIDs, edgeGroupIDs)
	}
	return s.withLockedState(false, func(state *model.State) error {
		if findEdgeRoutePolicy(state, hostname) < 0 {
			return ErrNotFound
		}
		return validateFileEdgeExclusionClearState(state, edgeIDs, edgeGroupIDs, time.Now().UTC())
	})
}
