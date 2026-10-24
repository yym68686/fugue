package api

import (
	"context"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func observeEdgeNodeInventory(node model.EdgeNode, now time.Time) *model.EdgeNodeInventoryObservation {
	observation := &model.EdgeNodeInventoryObservation{
		EvaluatedAt: now.UTC(), ObservedAt: latestNodeActivity(node.LastHeartbeatAt, node.LastSeenAt),
		AdmissionAllowsDNS: edgeNodeDNSEligible(node), RouteStatePresent: edgeNodeHasRouteState(node),
		TLSStatePresent: edgeNodeTLSReadyForDNS(node), RouteProofStatus: "not_evaluated",
	}
	if observation.ObservedAt != nil {
		until := observation.ObservedAt.Add(platformNodeHeartbeatStaleAfter)
		observation.FreshUntil = &until
		observation.HeartbeatFresh = !observation.ObservedAt.After(now) && edgeNodeHeartbeatFresh(node, now)
	}
	switch {
	case observation.ObservedAt == nil:
		observation.Reason = "observations_missing"
	case observation.ObservedAt.After(now):
		observation.Reason = "observation_from_future"
	case !observation.HeartbeatFresh:
		observation.Reason = "heartbeat_stale"
	case node.Draining || edgeNodeEffectiveCanaryState(node) == model.EdgeCanaryStateDrained:
		observation.Reason = "draining"
	case !observation.AdmissionAllowsDNS:
		observation.Reason = "admission_rejected"
	case !edgeNodeRouteServingCapable(node, now):
		observation.Reason = "node_unhealthy"
	case !observation.RouteStatePresent:
		observation.Reason = "route_state_missing"
	case !observation.TLSStatePresent:
		observation.Reason = "tls_state_missing"
	default:
		observation.Eligible = true
		observation.Reason = "inventory_checks_passed"
	}
	return observation
}

func (s *Server) observeGlobalDNSAdmission(now time.Time) (model.EdgeDNSAdmissionIntent, []platformproducer.OfflineEdge) {
	result := model.EdgeDNSAdmissionIntent{EvaluatedAt: now.UTC(), Scope: "global", State: "unavailable", ServingEvidence: "not_evaluated"}
	policy, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil {
		return result, nil
	}
	if !found {
		result.State = "not_configured"
		return result, nil
	}
	trusted := func(artifact model.PlatformArtifact) bool {
		return artifact.Status == model.PlatformArtifactStatusValidated && s.store.VerifyPlatformArtifactIntegrity(artifact) == nil && platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass
	}
	producer, err := platformproducer.Decode(policy)
	if err != nil || !trusted(policy) || producer.TargetScope != "global" || producer.Mode != "serving" || release.ArtifactID != policy.ID || release.Generation != policy.Generation || release.FencingToken <= 0 || release.Status != model.PlatformArtifactReleaseStatusActive {
		return result, nil
	}
	result.ProducerPolicyID, result.ProducerReleaseID, result.ProducerFencingToken = policy.ID, release.ID, release.FencingToken
	input, err := s.store.GetPlatformArtifact(producer.DNSPolicyArtifactID)
	if err != nil || !trusted(input) || input.ContentHash != producer.DNSPolicyDigest {
		return result, nil
	}
	static, err := s.loadStaticPlatformIntent(producer.StaticIntentArtifactID, producer.StaticIntentDigest)
	if err != nil {
		return result, nil
	}
	declaration, err := platformproducer.DecodeProjectionPolicy(input, static.Consumers, producer.HostedZoneTemplates)
	if err != nil {
		return result, nil
	}
	result.InputArtifactID, result.InputDigest, result.State = input.ID, input.ContentHash, "allowed"
	return result, declaration.OfflineEdges
}

func dnsAdmissionForNode(node model.EdgeNode, state model.EdgeDNSAdmissionIntent, holds []platformproducer.OfflineEdge) *model.EdgeDNSAdmissionIntent {
	if state.State != "allowed" {
		return &state
	}
	for _, hold := range holds {
		if hold.EdgeID != node.ID {
			continue
		}
		state.State, state.Reason, state.ExpectedRecoveryDate = "suspended", hold.Reason, hold.ExpectedRecoveryDate
		if hold.EdgeGroupID != node.EdgeGroupID || hold.PublicIPv4 != node.PublicIPv4 {
			state.State = "identity_conflict"
		}
		break
	}
	return &state
}

func (s *Server) observeEdgeNode(node model.EdgeNode, now time.Time) *model.EdgeNodeInventoryObservation {
	observation := observeEdgeNodeInventory(node, now)
	intent, holds := s.observeGlobalDNSAdmission(now)
	observation.DNSAdmissionIntent = dnsAdmissionForNode(node, intent, holds)
	s.observeOfflineRecovery(node, observation.DNSAdmissionIntent, holds, now)
	return observation
}

func (s *Server) observeOfflineRecovery(node model.EdgeNode, intent *model.EdgeDNSAdmissionIntent, holds []platformproducer.OfflineEdge, now time.Time) {
	if intent.State != "suspended" {
		return
	}
	for _, hold := range holds {
		if hold.EdgeID != node.ID || hold.Recovery == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		o, err := s.readOfflineRecovery(ctx, hold)
		cancel()
		intent.RecoveryState = "probing"
		if err != nil {
			intent.RecoveryState = "unavailable"
			return
		}
		if !o.LastAttempt.IsZero() {
			intent.RecoveryLastProbeAt = &o.LastAttempt
		}
		intent.RecoveryLastResult = o.LastResult
		if o.Capacity != nil {
			intent.RecoveryCapacityBytesPerSecond = o.Capacity.BytesPerSecond
			intent.RecoveryCapacityElapsedMilliseconds = o.Capacity.ElapsedMilliseconds
			intent.RecoveryCapacityEvidenceDigest = o.Capacity.ReportDigest
		}
		if o.Ready(hold, now) {
			intent.State, intent.Reason, intent.RecoveryState = "allowed", "", "ready"
		}
		return
	}
}
