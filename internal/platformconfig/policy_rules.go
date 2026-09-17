package platformconfig

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

type RoutePolicyConstraint struct {
	ID                   string     `json:"id"`
	Hostname             string     `json:"hostname"`
	AppID                string     `json:"app_id,omitempty"`
	TenantID             string     `json:"tenant_id,omitempty"`
	MatchScope           string     `json:"match_scope,omitempty"`
	EdgeGroupID          string     `json:"edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string   `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string   `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string     `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time `json:"exclusion_expires_at,omitempty"`
	ExclusionOwnerDigest string     `json:"exclusion_owner_digest,omitempty"`
	ExclusionGeneration  uint64     `json:"exclusion_generation,omitempty"`
	ExclusionFence       string     `json:"exclusion_fence,omitempty"`
	MinHealthyEdgeNodes  int        `json:"min_healthy_edge_nodes,omitempty"`
	RoutePolicy          string     `json:"route_policy"`
	Enabled              bool       `json:"enabled"`
}

type TrafficPolicyConstraint struct {
	TenantID             string `json:"tenant_id,omitempty"`
	UnavailableCandidate string `json:"unavailable_candidate,omitempty"`
	ID                   string `json:"id"`
	AppID                string `json:"app_id"`
	Mode                 string `json:"mode"`
	StableReleaseID      string `json:"stable_release_id,omitempty"`
	CandidateReleaseID   string `json:"candidate_release_id,omitempty"`
	StableWeight         int    `json:"stable_weight"`
	CandidateWeight      int    `json:"candidate_weight"`
	StickyHeader         string `json:"sticky_header,omitempty"`
	StickyCookie         string `json:"sticky_cookie,omitempty"`
}

func ProjectPolicySnapshot(base PolicySnapshot, routePolicies []model.EdgeRoutePolicy, trafficPolicies []model.AppTrafficPolicy, generation string) (PolicySnapshot, error) {
	out := NormalizePolicySnapshot(base)
	out.Generation = strings.TrimSpace(generation)
	out.RouteConstraints = nil
	out.TrafficConstraints = nil
	for _, p := range routePolicies {
		// Legacy hostname policies also match sibling apps in the same tenant.
		// Preserve that desired scope explicitly; generic app-scoped rules retain
		// their existing behavior and never inherit this migration default.
		matchScope := ""
		if strings.TrimSpace(p.TenantID) != "" {
			matchScope = "tenant_hostname"
		}
		out.RouteConstraints = append(out.RouteConstraints, RoutePolicyConstraint{ID: p.ID, Hostname: strings.Trim(strings.ToLower(strings.TrimSpace(p.Hostname)), "."), AppID: p.AppID, TenantID: p.TenantID, MatchScope: matchScope, EdgeGroupID: p.EdgeGroupID, ExcludedEdgeIDs: append([]string(nil), p.ExcludedEdgeIDs...), ExcludedEdgeGroupIDs: append([]string(nil), p.ExcludedEdgeGroupIDs...), ExclusionReason: p.ExclusionReason, ExclusionExpiresAt: p.ExclusionExpiresAt, ExclusionOwnerDigest: p.ExclusionOwnerDigest, ExclusionGeneration: p.ExclusionGeneration, ExclusionFence: p.ExclusionFence, MinHealthyEdgeNodes: p.MinHealthyEdgeNodes, RoutePolicy: p.RoutePolicy, Enabled: p.Enabled})
	}
	for _, p := range trafficPolicies {
		out.TrafficConstraints = append(out.TrafficConstraints, TrafficPolicyConstraint{TenantID: p.TenantID, UnavailableCandidate: "stable", ID: p.ID, AppID: p.AppID, Mode: p.Mode, StableReleaseID: p.StableReleaseID, CandidateReleaseID: p.CandidateReleaseID, StableWeight: p.StableWeight, CandidateWeight: p.CandidateWeight, StickyHeader: p.StickyHeader, StickyCookie: p.StickyCookie})
	}
	out = NormalizePolicySnapshot(out)
	if err := ValidatePolicySnapshot(out); err != nil {
		return PolicySnapshot{}, err
	}
	return out, nil
}

func validatePolicyRules(in PolicySnapshot) error {
	if len(in.RouteConstraints) > 4096 || len(in.TrafficConstraints) > 4096 {
		return fmt.Errorf("policy rule count exceeds 4096")
	}
	routes := map[string]bool{}
	for _, rule := range in.RouteConstraints {
		if rule.ID == "" || rule.Hostname == "" || rule.ID != strings.TrimSpace(rule.ID) || rule.Hostname != strings.Trim(strings.ToLower(strings.TrimSpace(rule.Hostname)), ".") || routes[rule.Hostname] || rule.RoutePolicy == "" || model.NormalizeEdgeRoutePolicy(rule.RoutePolicy) == "" || rule.MinHealthyEdgeNodes < 0 || rule.MinHealthyEdgeNodes > 10000 {
			return fmt.Errorf("route policy constraint is invalid")
		}
		if rule.AppID != strings.TrimSpace(rule.AppID) || rule.TenantID != strings.TrimSpace(rule.TenantID) ||
			(rule.MatchScope != "" && rule.MatchScope != "app" && rule.MatchScope != "tenant_hostname") ||
			(rule.MatchScope == "tenant_hostname" && rule.TenantID == "") {
			return fmt.Errorf("route policy match scope is invalid")
		}
		if rule.ExclusionExpiresAt != nil && rule.ExclusionExpiresAt.IsZero() {
			return fmt.Errorf("exclusion expiry is invalid")
		}
		if rule.ExclusionOwnerDigest != strings.TrimSpace(rule.ExclusionOwnerDigest) || rule.ExclusionFence != strings.TrimSpace(rule.ExclusionFence) {
			return fmt.Errorf("exclusion authorization metadata is not canonical")
		}
		if len(rule.ExcludedEdgeIDs) > 4096 || len(rule.ExcludedEdgeGroupIDs) > 4096 {
			return fmt.Errorf("exclusion list is too large")
		}
		for _, id := range append(append([]string{}, rule.ExcludedEdgeIDs...), rule.ExcludedEdgeGroupIDs...) {
			if id == "" || id != strings.TrimSpace(id) {
				return fmt.Errorf("exclusion identity is invalid")
			}
		}
		routes[rule.Hostname] = true
	}
	apps := map[string]bool{}
	for _, rule := range in.TrafficConstraints {
		if rule.ID == "" || rule.AppID == "" || rule.ID != strings.TrimSpace(rule.ID) || rule.AppID != strings.TrimSpace(rule.AppID) || rule.TenantID != strings.TrimSpace(rule.TenantID) || apps[rule.AppID] {
			return fmt.Errorf("traffic policy constraint identity is invalid")
		}
		if rule.UnavailableCandidate != "" && rule.UnavailableCandidate != "reject" && rule.UnavailableCandidate != "stable" {
			return fmt.Errorf("unavailable candidate policy is invalid")
		}
		if rule.StableReleaseID != "" && rule.StableReleaseID == rule.CandidateReleaseID {
			return fmt.Errorf("stable and candidate release identities must differ")
		}
		apps[rule.AppID] = true
		switch rule.Mode {
		case model.AppTrafficModeSingle, model.AppTrafficModeCanary, model.AppTrafficModeWeighted, model.AppTrafficModePaused:
		default:
			return fmt.Errorf("traffic policy mode is invalid")
		}
		if rule.StableWeight < 0 || rule.CandidateWeight < 0 || rule.StableWeight > 100 || rule.CandidateWeight > 100 || rule.StableWeight+rule.CandidateWeight != 100 {
			return fmt.Errorf("traffic policy weights must total 100")
		}
	}
	return nil
}

func ApplyRoutePolicyConstraints(routes []CompiledRoute, policy PolicySnapshot, capturedAt *time.Time) ([]CompiledRoute, error) {
	if err := validatePolicyRules(policy); err != nil {
		return nil, err
	}
	routes = append([]CompiledRoute(nil), routes...)
	for i := range routes {
		routes[i].Upstreams = append([]UpstreamIntent(nil), routes[i].Upstreams...)
		routes[i].RequestBodyPolicies = model.CloneEdgeRequestBodyPolicies(routes[i].RequestBodyPolicies)
	}
	byHost := make(map[string]RoutePolicyConstraint, len(policy.RouteConstraints))
	for _, rule := range policy.RouteConstraints {
		byHost[strings.ToLower(strings.Trim(strings.TrimSpace(rule.Hostname), "."))] = rule
	}
	matched := map[string]bool{}
	for i := range routes {
		rule, ok := byHost[strings.ToLower(strings.Trim(strings.TrimSpace(routes[i].Hostname), "."))]
		if !ok {
			continue
		}
		if rule.TenantID != "" && rule.TenantID != routes[i].TenantID {
			return nil, fmt.Errorf("route policy owner does not match intent")
		}
		if rule.MatchScope != "tenant_hostname" && rule.AppID != "" && rule.AppID != routes[i].AppID {
			continue
		}
		matched[rule.Hostname] = true
		lifecycle, err := routeConstraintExclusionLifecycle(rule, capturedAt)
		if err != nil {
			return nil, err
		}
		routes[i].ExclusionLifecycle = lifecycle
		routes[i].MinHealthyEdgeNodes = rule.MinHealthyEdgeNodes
		routes[i].ExcludedEdgeIDs = append([]string(nil), rule.ExcludedEdgeIDs...)
		routes[i].ExcludedEdgeGroupIDs = append([]string(nil), rule.ExcludedEdgeGroupIDs...)
		if rule.EdgeGroupID != "" {
			if routes[i].EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned && routes[i].EdgeGroupID != rule.EdgeGroupID {
				return nil, fmt.Errorf("DNS policy group conflicts with route intent group")
			}
			// Policy drains DNS selection, not Host serving. Keep the intent's
			// executor scope intact and bind this derived DNS constraint into
			// the artifact and fixed placement input digest.
			routes[i].DNSPlacementEdgeGroupID = rule.EdgeGroupID
		}
		routes[i].ExclusionReason, routes[i].ExclusionExpiresAt = rule.ExclusionReason, rule.ExclusionExpiresAt
		routes[i].RoutePolicy = model.NormalizeEdgeRoutePolicy(rule.RoutePolicy)
		if !rule.Enabled || !model.EdgeRoutePolicyAllowsTraffic(routes[i].RoutePolicy) {
			routes[i].Enabled = false
			routes[i].Upstreams = nil
			routes[i].Status = model.EdgeRouteStatusDisabled
		}
	}
	if len(matched) != len(byHost) {
		return nil, fmt.Errorf("route policy references a hostname outside the intent")
	}
	return routes, nil
}

func routeConstraintExclusionLifecycle(rule RoutePolicyConstraint, capturedAt *time.Time) (string, error) {
	policy := model.EdgeRoutePolicy{ExcludedEdgeIDs: rule.ExcludedEdgeIDs, ExcludedEdgeGroupIDs: rule.ExcludedEdgeGroupIDs,
		ExclusionExpiresAt: rule.ExclusionExpiresAt, ExclusionOwnerDigest: rule.ExclusionOwnerDigest,
		ExclusionGeneration: rule.ExclusionGeneration, ExclusionFence: rule.ExclusionFence}
	var at time.Time
	if capturedAt != nil {
		at = *capturedAt
	}
	if model.EdgeRoutePolicyHasExclusions(policy) && rule.ExclusionOwnerDigest != "" && rule.ExclusionGeneration > 0 && rule.ExclusionFence != "" &&
		rule.ExclusionExpiresAt != nil && at.IsZero() {
		return "", fmt.Errorf("versioned expiring exclusion requires fixed runtime captured_at")
	}
	// The shared lifecycle rule retains legacy/expired holds. Classification
	// never mutates the exclusion set, policy or its authorization metadata.
	return model.EdgeRoutePolicyExclusionLifecycleAt(policy, at), nil
}
