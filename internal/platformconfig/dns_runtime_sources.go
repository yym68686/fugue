package platformconfig

import (
	"encoding/json"
	"fmt"
	"slices"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

// DNSRuntimeSourcePlan derives requirements from an authenticated source. The
// caller must verify the source snapshot first. DNS retains ownership of its
// records, physical candidates and constraints; a source supplies route behavior
// and may further restrict eligibility. Nothing here grants positive readiness.
func DNSRuntimeSourcePlan(source CellDNSPlanSource, policy PolicySnapshot, pub model.PlatformDNSRouteSourcePublication, cell string) (*DNSReadinessPlan, *model.TrafficReleaseBinding, error) {
	aliases, err := RouteAuthorityAliases(source.Intent)
	if err != nil {
		return nil, nil, err
	}
	group := cell
	if pub.Parent.ScopeKey == GlobalScopeKey {
		group = ""
		for old, neutral := range aliases {
			if neutral == cell {
				group = old
			}
		}
		if group == "" {
			return nil, nil, fmt.Errorf("routing source has no authorized physical alias")
		}
	} else if pub.Parent.ScopeKey != AuthorityCellScope(cell) {
		return nil, nil, fmt.Errorf("routing source scope differs")
	}
	projection, err := ProjectRouteArtifact(pub.Route)
	if err != nil {
		return nil, nil, err
	}
	lineage := LineageFromArtifact(pub.Parent)
	b := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: pub.Parent.ID, ReleaseSetDigest: pub.Parent.ContentHash, ReleaseSetGeneration: pub.Parent.Generation, RouteArtifactID: pub.Route.ID, RouteArtifactDigest: pub.Route.ContentHash, RouteArtifactGeneration: pub.Route.Generation, RouteArtifactSequence: pub.Route.GenerationSequence, ReleaseID: pub.Release.ID, ReleaseChannel: pub.Release.ReleaseChannel, FencingToken: pub.Release.FencingToken, ScopeKey: pub.Parent.ScopeKey, IntentDigest: lineage.IntentDigest, PolicyDigest: lineage.PolicyDigest, InputSnapshotDigest: lineage.InputSnapshotDigest, CompilerVersion: lineage.CompilerVersion, ProjectionDigest: trafficbinding.ProjectionDigest(projection), CanaryRuleRef: pub.Release.CanaryRuleRef}
	if b.ReleaseChannel == "gray" {
		b.EdgeGroupIDs, err = ResolveTrafficCanary(pub.Parent, b.CanaryRuleRef)
	}
	if err != nil {
		return nil, nil, err
	}
	if b.ReleaseChannel == "gray" && !slices.Contains(b.EdgeGroupIDs, group) {
		return &DNSReadinessPlan{}, b, nil
	}
	if err = trafficbinding.ValidateGroup(b, group, true); err != nil {
		return nil, nil, err
	}
	snapshot := RuntimeSnapshot{CapturedAt: source.CapturedAt}
	topology, err := TrafficConsumersFromRelease(pub.Parent)
	if err != nil {
		return nil, nil, err
	}
	for _, endpoint := range source.Endpoints {
		if endpoint.EdgeGroupID != cell {
			continue
		}
		// A new routing publication cannot enroll a new physical DNS candidate.
		if pub.Parent.ScopeKey != GlobalScopeKey && (topology == nil || topology.AuthorityCellID != cell || !slices.Contains(topology.EdgeNodeIDs, endpoint.EdgeID)) {
			continue
		}
		snapshot.DNSEdgeEndpoints = append(snapshot.DNSEdgeEndpoints, endpoint)
	}
	route := pub.Route
	if group != cell {
		raw, _ := json.Marshal(route.Content)
		route.Content = nil
		if json.Unmarshal(raw, &route.Content) != nil {
			return nil, nil, fmt.Errorf("invalid source content")
		}
		normalizeDNSRuntimeAliases(route.Content, aliases)
	}
	ref := CellRoutePublicationReference{AuthorityCellID: cell, ReleaseSetID: pub.Parent.ID, ReleaseSetDigest: pub.Parent.ContentHash, ReleaseID: pub.Release.ID, ReleaseChannel: pub.Release.ReleaseChannel, FencingToken: pub.Release.FencingToken, CanaryRuleRef: pub.Release.CanaryRuleRef, RouteArtifactID: pub.Route.ID, RouteArtifactDigest: pub.Route.ContentHash, TLSArtifactID: pub.TLS.ID, TLSArtifactDigest: pub.TLS.ContentHash}
	plan, err := compileCellDNSReadinessProjection(source.Intent, []CellRoutePublicationInput{{Reference: ref, Parent: pub.Parent, Route: route, TLS: pub.TLS}}, snapshot, policy, nil, true)
	if err != nil {
		return nil, nil, err
	}
	routes := map[string]model.EdgeRouteIntent{}
	for _, r := range projection.Routes {
		routes[r.Hostname+"\x00"+model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = r
	}
	ids := map[string]string{}
	for i := range plan.Probes {
		p := &plan.Probes[i]
		old := p.ID
		// Derive the proof digest from the original signed route, before aliasing.
		r, ok := routes[p.Hostname+"\x00"+p.Path]
		if !ok {
			return nil, nil, fmt.Errorf("runtime dependency absent")
		}
		digest, e := routeproof.Digest(routebinding.FromIntent(r, group))
		if e != nil {
			return nil, nil, e
		}
		p.RouteDigest = digest
		if group != cell {
			p.PreviousAuthority = &DNSPreviousAuthority{PublicationDigest: p.CellPublicationDigest, EdgeGroupID: group, RouteDigest: digest}
		}
		p.ID, err = DNSReadinessProbeID(*p)
		if err != nil {
			return nil, nil, err
		}
		ids[old] = p.ID
	}
	for i := range plan.Records {
		for j := range plan.Records[i].Targets {
			t := &plan.Records[i].Targets[j]
			t.RequireSinglePublication = true
			for k, id := range t.ProbeIDs {
				t.ProbeIDs[k] = ids[id]
			}
			slices.Sort(t.ProbeIDs)
		}
	}
	return plan, b, ValidateDNSReadinessPlan(plan, policy.DNSReadiness)
}

func normalizeDNSRuntimeAliases(value any, aliases map[string]string) {
	switch v := value.(type) {
	case map[string]any:
		for k, item := range v {
			switch k {
			case "edge_group_id", "dns_placement_edge_group_id":
				if s, ok := item.(string); ok && aliases[s] != "" {
					v[k] = aliases[s]
				}
			case "excluded_edge_group_ids":
				if rows, ok := item.([]any); ok {
					for i, item := range rows {
						if s, ok := item.(string); ok && aliases[s] != "" {
							rows[i] = aliases[s]
						}
					}
				}
			default:
				normalizeDNSRuntimeAliases(item, aliases)
			}
		}
	case []any:
		for _, item := range v {
			normalizeDNSRuntimeAliases(item, aliases)
		}
	}
}

// RestrictDNSRuntimeSourcePlan applies current hard policy to an older positive
// publication without mistaking an unready candidate's runtime state for policy.
// A failed candidate cannot erase LKG route behavior; explicit exclusions and
// disabled policy still take effect, and quorum can only increase.
func RestrictDNSRuntimeSourcePlan(plan *DNSReadinessPlan, source CellDNSPlanSource, pub model.PlatformDNSRouteSourcePublication, selected []model.PlatformDNSRouteSourcePublication) error {
	restrictions, err := PrepareDNSRuntimeSourceRestrictions(source, selected)
	if err != nil {
		return err
	}
	return restrictions.Restrict(plan, pub)
}

type dnsRuntimeSourcePolicy struct {
	minimum int
	rules   map[string]RoutePolicyConstraint
}

// DNSRuntimeSourceRestrictions is a request-local typed view of current hard
// policy. It contains no readiness or authorization verdict. Prepare once per
// observed scope, then apply to every publication/cell, including retained LKG.
type DNSRuntimeSourceRestrictions struct {
	aliases map[string]string
	records map[string]DNSIntent
	current []dnsRuntimeSourcePolicy
}

func PrepareDNSRuntimeSourceRestrictions(source CellDNSPlanSource, selected []model.PlatformDNSRouteSourcePublication) (*DNSRuntimeSourceRestrictions, error) {
	aliases, err := RouteAuthorityAliases(source.Intent)
	if err != nil {
		return nil, err
	}
	out := &DNSRuntimeSourceRestrictions{aliases: aliases, records: map[string]DNSIntent{}}
	for _, r := range source.Intent.DNS {
		out.records[r.Hostname] = r
	}
	for _, current := range selected {
		var policy PolicySnapshot
		raw, err := json.Marshal(current.Route.Content["policy"])
		if err != nil || json.Unmarshal(raw, &policy) != nil {
			return nil, fmt.Errorf("current route constraints unavailable")
		}
		rules := map[string]RoutePolicyConstraint{}
		for _, rule := range policy.RouteConstraints {
			rules[rule.Hostname] = rule
		}
		out.current = append(out.current, dnsRuntimeSourcePolicy{minimum: policy.MinimumHealthyEdges, rules: rules})
	}
	return out, nil
}

func (restriction *DNSRuntimeSourceRestrictions) Restrict(plan *DNSReadinessPlan, pub model.PlatformDNSRouteSourcePublication) error {
	owners, err := restriction.ForPublication(pub)
	if err != nil {
		return err
	}
	return owners.Restrict(plan)
}

type DNSRuntimePublicationRestrictions struct {
	current *DNSRuntimeSourceRestrictions
	byHost  map[string][]CompiledRoute
}

// ForPublication decodes owner fields once, independent of the number of cells.
func (restriction *DNSRuntimeSourceRestrictions) ForPublication(pub model.PlatformDNSRouteSourcePublication) (*DNSRuntimePublicationRestrictions, error) {
	var routes []CompiledRoute
	raw, err := json.Marshal(pub.Route.Content["routes"])
	if err != nil || json.Unmarshal(raw, &routes) != nil {
		return nil, fmt.Errorf("routing source owners invalid")
	}
	byHost := map[string][]CompiledRoute{}
	for _, route := range routes {
		byHost[route.Hostname] = append(byHost[route.Hostname], route)
	}
	return &DNSRuntimePublicationRestrictions{current: restriction, byHost: byHost}, nil
}

func (owners *DNSRuntimePublicationRestrictions) Restrict(plan *DNSReadinessPlan) error {
	restriction, byHost := owners.current, owners.byHost
	aliases, records := restriction.aliases, restriction.records
	for _, current := range restriction.current {
		rules := current.rules
		for i := range plan.Records {
			r := &plan.Records[i]
			r.MinimumHealthyEdges = max(r.MinimumHealthyEdges, current.minimum)
			targets := r.Targets[:0]
			for _, target := range r.Targets {
				allowed := true
				for _, host := range DNSPlacementHostnames(records[r.Hostname]) {
					rule, exists := rules[host]
					if !exists {
						continue
					}
					for _, owner := range byHost[host] {
						if rule.TenantID != "" && rule.TenantID != owner.TenantID {
							return fmt.Errorf("current route constraint owner differs")
						}
						if rule.MatchScope != "tenant_hostname" && rule.AppID != "" && rule.AppID != owner.AppID {
							continue
						}
						r.MinimumHealthyEdges = max(r.MinimumHealthyEdges, rule.MinHealthyEdgeNodes)
						group := rule.EdgeGroupID
						if aliases[group] != "" {
							group = aliases[group]
						}
						if !rule.Enabled || !model.EdgeRoutePolicyAllowsTraffic(model.NormalizeEdgeRoutePolicy(rule.RoutePolicy)) || slices.Contains(rule.ExcludedEdgeIDs, target.EdgeID) || group != "" && group != target.EdgeGroupID {
							allowed = false
						}
						for _, excluded := range rule.ExcludedEdgeGroupIDs {
							if aliases[excluded] != "" {
								excluded = aliases[excluded]
							}
							if excluded == target.EdgeGroupID {
								allowed = false
							}
						}
					}
				}
				if allowed {
					targets = append(targets, target)
				}
			}
			r.Targets = targets
		}
	}
	used := map[string]bool{}
	for _, r := range plan.Records {
		for _, t := range r.Targets {
			for _, id := range t.ProbeIDs {
				used[id] = true
			}
		}
	}
	probes := plan.Probes[:0]
	for _, p := range plan.Probes {
		if used[p.ID] {
			probes = append(probes, p)
		}
	}
	plan.Probes = probes
	return nil
}
