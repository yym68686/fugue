package api

import (
	"time"

	"fugue/internal/model"
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
