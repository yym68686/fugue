package platformconfig

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// ApplyTrafficPolicyConstraints resolves only fixed input facts. Unavailable
// candidates may fall back only when policy explicitly permits it and the
// stable observation is fresh and active. Unknown evidence never authorizes
// traffic, and no part of intent or policy is mutated.
func ApplyTrafficPolicyConstraints(routes []CompiledRoute, policy PolicySnapshot, snapshot RuntimeSnapshot) ([]CompiledRoute, error) {
	if err := validatePolicyRules(policy); err != nil {
		return nil, err
	}
	if len(snapshot.Releases) > 10000 {
		return nil, fmt.Errorf("runtime snapshot has too many releases")
	}
	byID := make(map[string]ReleaseObservation, len(snapshot.Releases))
	for _, release := range snapshot.Releases {
		if release.ID == "" || release.AppID == "" || byID[release.ID].ID != "" {
			return nil, fmt.Errorf("release observation identity is invalid")
		}
		for _, value := range []string{release.ID, release.AppID, release.TenantID, release.RuntimeID, release.UpstreamURL, release.DeploymentGeneration} {
			if value != strings.TrimSpace(value) {
				return nil, fmt.Errorf("release observation identity is not canonical")
			}
		}
		if snapshot.CapturedAt == nil || snapshot.CapturedAt.IsZero() || release.ObservedAt.IsZero() || release.ObservedAt.After(*snapshot.CapturedAt) || snapshot.CapturedAt.Sub(release.ObservedAt).Seconds() > float64(policy.MaxStaleSeconds) {
			return nil, fmt.Errorf("release observation requires fresh evidence at fixed captured_at")
		}
		switch release.Status {
		case model.EdgeRouteStatusActive, model.EdgeRouteStatusUnavailable, model.EdgeRouteStatusDisabled:
		default:
			return nil, fmt.Errorf("release observation status is invalid")
		}
		if release.UpstreamURL != "" {
			if err := ValidateUpstreamIntents([]UpstreamIntent{{UpstreamURL: release.UpstreamURL, Weight: 100}}); err != nil {
				return nil, err
			}
		}
		byID[release.ID] = release
	}
	byApp := make(map[string]TrafficPolicyConstraint, len(policy.TrafficConstraints))
	for _, rule := range policy.TrafficConstraints {
		byApp[rule.AppID] = rule
	}
	matched := make(map[string]bool, len(byApp))
	out := append([]CompiledRoute(nil), routes...)
	for i := range out {
		route := &out[i]
		rule, exists := byApp[route.AppID]
		if !exists {
			continue
		}
		if rule.TenantID != route.TenantID {
			return nil, fmt.Errorf("traffic policy tenant does not match route")
		}
		if len(route.Upstreams) > 0 {
			return nil, fmt.Errorf("route upstreams and traffic policy cannot both own release weights")
		}
		matched[rule.AppID] = true
		stableWeight, candidateWeight := rule.StableWeight, rule.CandidateWeight
		if rule.Mode == model.AppTrafficModeSingle || rule.Mode == model.AppTrafficModePaused {
			stableWeight, candidateWeight = 100, 0
		}
		if candidateWeight > 0 && (rule.StickyHeader != "" || rule.StickyCookie != "") {
			return nil, fmt.Errorf("sticky release routing requires consumer support; compilation refused")
		}
		resolve := func(id string) (ReleaseObservation, error) {
			release, ok := byID[id]
			if !ok || id == "" {
				return ReleaseObservation{}, fmt.Errorf("referenced release observation is missing")
			}
			if release.AppID != route.AppID || release.TenantID != route.TenantID {
				return ReleaseObservation{}, fmt.Errorf("release observation owner does not match route")
			}
			return release, nil
		}
		var candidate ReleaseObservation
		if candidateWeight > 0 {
			var err error
			candidate, err = resolve(rule.CandidateReleaseID)
			if err != nil {
				return nil, err
			}
			if candidate.Status != model.EdgeRouteStatusActive || candidate.UpstreamURL == "" {
				if rule.UnavailableCandidate != "stable" {
					return nil, fmt.Errorf("candidate unavailable and stable fallback is not authorized")
				}
				stableWeight, candidateWeight = 100, 0
			}
		}
		var stable ReleaseObservation
		if stableWeight > 0 {
			var err error
			stable, err = resolve(rule.StableReleaseID)
			if err != nil {
				return nil, err
			}
			if stable.Status != model.EdgeRouteStatusActive || stable.UpstreamURL == "" {
				return nil, fmt.Errorf("stable release is not ready for traffic")
			}
		}
		// Intent disable and origin/policy failure always win over healthy releases.
		if !route.Enabled || (route.Status != "" && route.Status != model.EdgeRouteStatusActive) || (route.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy)) {
			route.Upstreams = nil
			continue
		}
		upstreams := make([]UpstreamIntent, 0, 2)
		for _, target := range []struct {
			release ReleaseObservation
			role    string
			weight  int
		}{{stable, model.AppReleaseRoleStable, stableWeight}, {candidate, model.AppReleaseRoleCandidate, candidateWeight}} {
			if target.weight <= 0 {
				continue
			}
			upstreams = append(upstreams, UpstreamIntent{Role: target.role, ReleaseID: target.release.ID, Weight: target.weight, UpstreamKind: route.UpstreamKind, UpstreamScope: route.UpstreamScope, UpstreamURL: target.release.UpstreamURL, ServicePort: route.ServicePort, RuntimeID: target.release.RuntimeID, DeploymentGeneration: target.release.DeploymentGeneration})
		}
		if err := ValidateUpstreamIntents(upstreams); err != nil {
			return nil, err
		}
		route.Upstreams = upstreams
	}
	if len(matched) != len(byApp) {
		return nil, fmt.Errorf("traffic policy references an app outside the intent")
	}
	return out, nil
}
