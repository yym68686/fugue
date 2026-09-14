// Package routebinding materializes route behavior without reading runtime state.
package routebinding

import (
	"slices"
	"strings"

	"fugue/internal/model"
)

// FromIntent is shared by Edge Control and readiness observers. Selection,
// health, publication generations and signing remain the caller's responsibility.
func FromIntent(intent model.EdgeRouteIntent, groupID string) model.EdgeRouteBinding {
	status := strings.TrimSpace(intent.OriginStatus)
	if status == "" {
		status = model.EdgeRouteStatusActive
	}
	return model.EdgeRouteBinding{
		Hostname: strings.TrimSuffix(strings.ToLower(strings.TrimSpace(intent.Hostname)), "."), PathPrefix: model.NormalizeAppRoutePathPrefix(intent.PathPrefix), RouteKind: strings.TrimSpace(intent.RouteKind),
		AppID: strings.TrimSpace(intent.AppID), TenantID: strings.TrimSpace(intent.TenantID), RuntimeID: strings.TrimSpace(intent.RuntimeID),
		RuntimeType: strings.TrimSpace(intent.RuntimeType), RuntimeEdgeGroup: strings.TrimSpace(intent.RuntimeEdgeGroupID),
		RuntimeEdgeGroupID: strings.TrimSpace(intent.RuntimeEdgeGroupID), RuntimeClusterNode: strings.TrimSpace(intent.RuntimeClusterNode),
		SelectedEdgeGroup: groupID, EdgeGroupID: groupID,
		ExcludedEdgeIDs: identitySet(intent.ExcludedEdgeIDs), ExcludedEdgeGroupIDs: identitySet(intent.ExcludedEdgeGroupIDs),
		ExclusionReason: strings.TrimSpace(intent.ExclusionReason), ExclusionExpiresAt: intent.ExclusionExpiresAt,
		MinHealthyEdgeNodes: intent.MinHealthyEdgeNodes,
		RoutePolicy:         model.NormalizeEdgeRoutePolicy(intent.RoutePolicy),
		UpstreamKind:        strings.TrimSpace(intent.UpstreamKind), UpstreamScope: strings.TrimSpace(intent.UpstreamScope),
		UpstreamURL: strings.TrimSpace(intent.UpstreamURL), Upstreams: append([]model.EdgeRouteUpstream(nil), intent.Upstreams...), ServicePort: intent.ServicePort,
		TLSPolicy: strings.TrimSpace(intent.TLSPolicy), CachePolicyID: strings.TrimSpace(intent.CachePolicyID),
		CacheNamespace: strings.TrimSpace(intent.CacheNamespace), DeploymentGeneration: strings.TrimSpace(intent.DeploymentGeneration),
		RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(intent.RequestBodyPolicies), Streaming: intent.Streaming,
		Status: status, StatusReason: strings.TrimSpace(intent.OriginStatusReason), CreatedAt: intent.CreatedAt, UpdatedAt: intent.UpdatedAt,
	}
}

func identitySet(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if v := strings.ToLower(strings.TrimSpace(value)); v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	return slices.Compact(out)
}
