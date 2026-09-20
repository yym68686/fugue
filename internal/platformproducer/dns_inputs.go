package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// ProjectionPolicyInput is the supported producer subset of PolicySnapshot.
// Unused fields are rejected rather than silently ignored.
type ProjectionPolicyInput struct {
	DNSQueryPolicy           *platformconfig.DNSQueryPolicy            `json:"dns_query_policy,omitempty"`
	MinimumHealthyEdges      *int                                      `json:"minimum_healthy_edges,omitempty"`
	MaxStaleSeconds          *int                                      `json:"max_stale_seconds,omitempty"`
	RouteConstraints         *[]platformconfig.RoutePolicyConstraint   `json:"route_constraints,omitempty"`
	DNSRouteStateConstraints *[]platformconfig.DNSRouteStateConstraint `json:"dns_route_state_constraints,omitempty"`
	SchemaVersion            string                                    `json:"schema_version"`
	Generation               string                                    `json:"generation"`
	Scope                    string                                    `json:"scope"`
	Authorities              []platformconfig.DNSAuthorityPolicy       `json:"dns_authorities"`
	Clients                  []platformconfig.DNSClientPolicy          `json:"dns_client_policies"`
	DNSReadiness             *platformconfig.ReadinessProbePolicy      `json:"dns_readiness"`
	TLSReadiness             *platformconfig.ReadinessProbePolicy      `json:"tls_readiness"`
	Cohorts                  []platformconfig.TrafficRolloutCohort     `json:"traffic_rollout_cohorts"`
}

func DecodeProjectionPolicy(a model.PlatformArtifact, consumers []platformconfig.DNSConsumerIntent, templates []HostedZoneTemplate) (ProjectionPolicyInput, error) {
	var p ProjectionPolicyInput
	fail := func() (ProjectionPolicyInput, error) {
		return p, fmt.Errorf("projection policy identity, defaults or complete ownership invalid")
	}
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return p, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil {
		return fail()
	}
	for _, key := range []string{"minimum_healthy_edges", "max_stale_seconds", "route_constraints", "dns_route_state_constraints"} {
		if value, present := a.Content[key]; present && value == nil {
			return fail()
		}
	}
	if value, present := a.Content["dns_query_policy"]; present && value == nil {
		return fail()
	}
	if platformconfig.ValidateDNSQueryPolicy(p.DNSQueryPolicy) != nil {
		return fail()
	}
	if _, _, err := p.RouteDefaults(); err != nil {
		return fail()
	}
	if a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || a.ScopeKey != "global" || p.Scope != "global" || p.Generation != a.Generation || p.SchemaVersion != platformconfig.SchemaVersion || len(consumers) == 0 || platformconfig.ValidateDNSConsumers(consumers) != nil || len(p.Authorities) == 0 || len(p.Clients) != len(consumers) || p.DNSReadiness == nil || p.TLSReadiness == nil || len(p.Cohorts) == 0 {
		return fail()
	}
	if platformconfig.ValidateDNSAuthorityOwnership(p.Authorities, consumers) != nil || platformconfig.ValidateDNSClientPolicyOwnership(p.Clients, consumers) != nil || platformconfig.ValidateReadinessProbePolicy(p.DNSReadiness) != nil || platformconfig.ValidateReadinessProbePolicy(p.TLSReadiness) != nil || platformconfig.ValidateTrafficRolloutCohorts(p.Cohorts) != nil {
		return fail()
	}
	if len(templates) > 0 {
		if len(templates) != len(consumers) {
			return fail()
		}
		seen := map[string]bool{}
		for _, t := range templates {
			found := false
			for _, a := range p.Authorities {
				found = found || (a.NodeID == t.NodeID && a.Zone == t.TemplateZone)
			}
			if !found || seen[t.NodeID] {
				return fail()
			}
			seen[t.NodeID] = true
		}
	}
	return p, nil
}

// RouteDefaults has no ambient defaults. The legacy source may omit the entire
// group; any explicit group must be complete and validated before it is used.
func (p ProjectionPolicyInput) RouteDefaults() (platformconfig.PolicySnapshot, bool, error) {
	fail := func() (platformconfig.PolicySnapshot, bool, error) {
		return platformconfig.PolicySnapshot{}, false, fmt.Errorf("route projection defaults incomplete or invalid")
	}
	if p.MinimumHealthyEdges == nil && p.MaxStaleSeconds == nil && p.RouteConstraints == nil && p.DNSRouteStateConstraints == nil {
		return platformconfig.PolicySnapshot{}, false, nil
	}
	if p.MinimumHealthyEdges == nil || p.MaxStaleSeconds == nil || p.RouteConstraints == nil || p.DNSRouteStateConstraints == nil || *p.MinimumHealthyEdges < 1 || *p.MinimumHealthyEdges > 10000 || *p.MaxStaleSeconds < 1 || *p.MaxStaleSeconds > 604800 {
		return fail()
	}
	out := platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: p.Generation, MinimumHealthyEdges: *p.MinimumHealthyEdges, MaxStaleSeconds: *p.MaxStaleSeconds, RouteConstraints: *p.RouteConstraints, DNSRouteStateConstraints: *p.DNSRouteStateConstraints}
	if err := platformconfig.ValidatePolicySnapshot(out); err != nil {
		return fail()
	}
	return platformconfig.NormalizePolicySnapshot(out), true, nil
}
