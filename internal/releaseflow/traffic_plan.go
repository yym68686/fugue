package releaseflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/model"
)

type TrafficPlanResult struct {
	Binding      model.EdgeRouteBinding
	StatusReason string
}

func AppReleaseByID(releases []model.AppRelease) map[string]model.AppRelease {
	out := make(map[string]model.AppRelease, len(releases))
	for _, release := range releases {
		if id := strings.TrimSpace(release.ID); id != "" {
			out[id] = release
		}
	}
	return out
}

func AppTrafficPolicyByApp(policies []model.AppTrafficPolicy) map[string]model.AppTrafficPolicy {
	out := make(map[string]model.AppTrafficPolicy, len(policies))
	for _, policy := range policies {
		if appID := strings.TrimSpace(policy.AppID); appID != "" {
			out[appID] = policy
		}
	}
	return out
}

func ApplyAppReleaseTraffic(binding model.EdgeRouteBinding, policies map[string]model.AppTrafficPolicy, releases map[string]model.AppRelease) model.EdgeRouteBinding {
	return PlanAppReleaseTraffic(binding, policies, releases).Binding
}

func PlanAppReleaseTraffic(binding model.EdgeRouteBinding, policies map[string]model.AppTrafficPolicy, releases map[string]model.AppRelease) TrafficPlanResult {
	result := TrafficPlanResult{Binding: binding}
	policy, ok := policies[strings.TrimSpace(binding.AppID)]
	if !ok || strings.TrimSpace(policy.StableReleaseID) == "" {
		return result
	}
	if !strings.EqualFold(strings.TrimSpace(binding.Status), model.EdgeRouteStatusActive) || strings.TrimSpace(binding.UpstreamURL) == "" {
		return result
	}
	stableRelease, ok := releases[strings.TrimSpace(policy.StableReleaseID)]
	if !ok || strings.TrimSpace(stableRelease.AppID) != strings.TrimSpace(binding.AppID) {
		return result
	}
	stableWeight := policy.StableWeight
	candidateWeight := policy.CandidateWeight
	if policy.Mode == model.AppTrafficModeSingle || policy.Mode == model.AppTrafficModePaused {
		stableWeight = 100
		candidateWeight = 0
	}

	upstreams := []model.EdgeRouteUpstream{}
	stableURL := firstNonEmpty(stableRelease.UpstreamURL, binding.UpstreamURL)
	if stableWeight > 0 || candidateWeight == 0 {
		upstreams = append(upstreams, model.EdgeRouteUpstream{
			Role:                 model.AppReleaseRoleStable,
			ReleaseID:            stableRelease.ID,
			Weight:               stableWeight,
			UpstreamKind:         firstNonEmpty(binding.UpstreamKind, model.EdgeRouteUpstreamKindKubernetesService),
			UpstreamScope:        binding.UpstreamScope,
			UpstreamURL:          stableURL,
			ServicePort:          binding.ServicePort,
			RuntimeID:            firstNonEmpty(stableRelease.RuntimeID, binding.RuntimeID),
			DeploymentGeneration: firstNonEmpty(stableRelease.ResolvedImageRef, stableRelease.SourceRef, binding.DeploymentGeneration),
			Status:               model.EdgeRouteStatusActive,
		})
	}

	if candidateID := strings.TrimSpace(policy.CandidateReleaseID); candidateID != "" {
		if candidate, ok := releases[candidateID]; ok && strings.TrimSpace(candidate.AppID) == strings.TrimSpace(binding.AppID) {
			candidateStatus := model.EdgeRouteStatusActive
			statusReason := strings.TrimSpace(candidate.StatusReason)
			if !AppReleaseCanReceiveEdgeTraffic(candidate) || strings.TrimSpace(candidate.UpstreamURL) == "" {
				candidateStatus = model.EdgeRouteStatusUnavailable
				if statusReason == "" {
					statusReason = "candidate release is not ready for edge traffic"
				}
				result.StatusReason = statusReason
				candidateWeight = 0
				if stableWeight < 100 {
					stableWeight = 100
				}
			}
			upstreams = append(upstreams, model.EdgeRouteUpstream{
				Role:                 model.AppReleaseRoleCandidate,
				ReleaseID:            candidate.ID,
				Weight:               candidateWeight,
				UpstreamKind:         firstNonEmpty(binding.UpstreamKind, model.EdgeRouteUpstreamKindKubernetesService),
				UpstreamScope:        binding.UpstreamScope,
				UpstreamURL:          candidate.UpstreamURL,
				ServicePort:          binding.ServicePort,
				RuntimeID:            firstNonEmpty(candidate.RuntimeID, binding.RuntimeID),
				DeploymentGeneration: firstNonEmpty(candidate.ResolvedImageRef, candidate.SourceRef),
				Status:               candidateStatus,
				StatusReason:         statusReason,
			})
		}
	}
	if len(upstreams) == 0 {
		return result
	}
	if upstreams[0].Role == model.AppReleaseRoleStable {
		upstreams[0].Weight = stableWeight
	}
	binding.Upstreams = upstreams
	binding.RouteGeneration = EdgeRouteGeneration(binding)
	result.Binding = binding
	return result
}

func AppReleaseCanReceiveEdgeTraffic(release model.AppRelease) bool {
	switch strings.TrimSpace(release.Status) {
	case model.AppReleaseStatusReady, model.AppReleaseStatusServing:
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func EdgeRouteGeneration(binding model.EdgeRouteBinding) string {
	payload, _ := json.Marshal(edgeRouteVersionMaterialFromBinding(binding))
	sum := sha256.Sum256(payload)
	return "routegen_" + hex.EncodeToString(sum[:])[:16]
}

type edgeRouteVersionMaterial struct {
	Hostname             string                    `json:"hostname"`
	PathPrefix           string                    `json:"path_prefix,omitempty"`
	RouteKind            string                    `json:"route_kind"`
	AppID                string                    `json:"app_id"`
	TenantID             string                    `json:"tenant_id"`
	RuntimeID            string                    `json:"runtime_id"`
	RuntimeType          string                    `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID   string                    `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                    `json:"runtime_cluster_node,omitempty"`
	RuntimeEdgeGroup     string                    `json:"runtime_edge_group,omitempty"`
	SelectedEdgeGroup    string                    `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string                    `json:"edge_group_id"`
	FallbackEdgeGroupID  string                    `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string                    `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                  `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                  `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                    `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time                `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int                       `json:"min_healthy_edge_nodes,omitempty"`
	HealthyEdgeNodeCount int                       `json:"healthy_edge_node_count,omitempty"`
	EdgeRedundancyStatus string                    `json:"edge_redundancy_status,omitempty"`
	EdgeRedundancyReason string                    `json:"edge_redundancy_reason,omitempty"`
	RoutePolicy          string                    `json:"route_policy"`
	SelectionReason      string                    `json:"selection_reason,omitempty"`
	FallbackReason       string                    `json:"fallback_reason,omitempty"`
	UpstreamKind         string                    `json:"upstream_kind"`
	UpstreamScope        string                    `json:"upstream_scope,omitempty"`
	UpstreamURL          string                    `json:"upstream_url,omitempty"`
	Upstreams            []model.EdgeRouteUpstream `json:"upstreams,omitempty"`
	ServicePort          int                       `json:"service_port"`
	TLSPolicy            string                    `json:"tls_policy"`
	CachePolicyID        string                    `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                    `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                    `json:"deployment_generation,omitempty"`
	Streaming            bool                      `json:"streaming"`
	Status               string                    `json:"status"`
	StatusReason         string                    `json:"status_reason,omitempty"`
}

func edgeRouteVersionMaterialFromBinding(binding model.EdgeRouteBinding) edgeRouteVersionMaterial {
	return edgeRouteVersionMaterial{
		Hostname:             binding.Hostname,
		PathPrefix:           model.NormalizeAppRoutePathPrefix(binding.PathPrefix),
		RouteKind:            binding.RouteKind,
		AppID:                binding.AppID,
		TenantID:             binding.TenantID,
		RuntimeID:            binding.RuntimeID,
		RuntimeType:          binding.RuntimeType,
		RuntimeEdgeGroupID:   binding.RuntimeEdgeGroupID,
		RuntimeClusterNode:   binding.RuntimeClusterNode,
		RuntimeEdgeGroup:     binding.RuntimeEdgeGroup,
		SelectedEdgeGroup:    binding.SelectedEdgeGroup,
		EdgeGroupID:          binding.EdgeGroupID,
		FallbackEdgeGroupID:  binding.FallbackEdgeGroupID,
		PolicyEdgeGroupID:    binding.PolicyEdgeGroupID,
		ExcludedEdgeIDs:      append([]string(nil), binding.ExcludedEdgeIDs...),
		ExcludedEdgeGroupIDs: append([]string(nil), binding.ExcludedEdgeGroupIDs...),
		ExclusionReason:      binding.ExclusionReason,
		ExclusionExpiresAt:   binding.ExclusionExpiresAt,
		MinHealthyEdgeNodes:  binding.MinHealthyEdgeNodes,
		HealthyEdgeNodeCount: binding.HealthyEdgeNodeCount,
		EdgeRedundancyStatus: binding.EdgeRedundancyStatus,
		EdgeRedundancyReason: binding.EdgeRedundancyReason,
		RoutePolicy:          binding.RoutePolicy,
		SelectionReason:      binding.SelectionReason,
		FallbackReason:       binding.FallbackReason,
		UpstreamKind:         binding.UpstreamKind,
		UpstreamScope:        binding.UpstreamScope,
		UpstreamURL:          binding.UpstreamURL,
		Upstreams:            append([]model.EdgeRouteUpstream(nil), binding.Upstreams...),
		ServicePort:          binding.ServicePort,
		TLSPolicy:            binding.TLSPolicy,
		CachePolicyID:        binding.CachePolicyID,
		CacheNamespace:       binding.CacheNamespace,
		DeploymentGeneration: binding.DeploymentGeneration,
		Streaming:            binding.Streaming,
		Status:               binding.Status,
		StatusReason:         binding.StatusReason,
	}
}
