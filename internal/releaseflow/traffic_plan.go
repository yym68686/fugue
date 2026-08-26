package releaseflow

import (
	"strings"

	"fugue/internal/model"
	"fugue/internal/trafficepoch"
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
	binding.RouteGeneration = trafficepoch.EdgeRouteGeneration(binding)
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
