package platformconfig

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

func validateCellDNSIntent(in PlatformIntent) error {
	if len(in.Routes) != 0 || len(in.TLS) != 0 || len(in.CachePolicies) != 0 || in.ApplicationDomains != nil || len(in.DNSConsumers) == 0 || ValidateDNSConsumers(in.DNSConsumers) != nil || in.EdgeTopology == nil || in.EdgeTopology.Validate() != nil || len(in.CellRoutePublications) < 1 || len(in.CellRoutePublications) > 16 {
		return fmt.Errorf("DNS-only intent requires independent consumers, explicit topology and exact Cell references, without route/TLS configuration")
	}
	cells := map[string]bool{}
	for _, ref := range in.CellRoutePublications {
		if ref.Validate() != nil || cells[ref.AuthorityCellID] || ref.AuthorityCellID == in.AuthorityCellID {
			return fmt.Errorf("DNS routing Cell reference invalid or duplicated")
		}
		cells[ref.AuthorityCellID] = true
	}
	if len(cells) != len(in.EdgeTopology.Cells) {
		return fmt.Errorf("DNS topology and referenced Cells differ")
	}
	for _, c := range in.EdgeTopology.Cells {
		if !cells[c.ID] || c.LegacyGroupID != "" {
			return fmt.Errorf("DNS topology must use exact neutral routing Cells")
		}
	}
	return nil
}

func validateCellDNSInputs(intent PlatformIntent, publications []CellRoutePublicationInput, snapshot RuntimeSnapshot) error {
	if intent.PublicationRole != PublicationRoleCellDNS {
		if len(publications) != 0 {
			return fmt.Errorf("Cell publication inputs require DNS-only authority")
		}
		return nil
	}
	if len(snapshot.Origins) != 0 || len(snapshot.Releases) != 0 || len(snapshot.TLSDomains) != 0 || len(snapshot.DNSPlacements) != 0 || len(publications) != len(intent.CellRoutePublications) {
		return fmt.Errorf("DNS input must retain exact Cell publications without mutable route/TLS inputs")
	}
	refs := map[string]CellRoutePublicationReference{}
	for _, r := range intent.CellRoutePublications {
		refs[r.AuthorityCellID] = r
	}
	members := map[string]string{}
	for _, p := range publications {
		if err := ValidateCellRoutePublication(p); err != nil {
			return err
		}
		if ref, ok := refs[p.Reference.AuthorityCellID]; !ok || ref != p.Reference {
			return fmt.Errorf("DNS input differs from pinned Cell reference")
		}
		delete(refs, p.Reference.AuthorityCellID)
		topology, _ := TrafficConsumersFromRelease(p.Parent)
		for _, node := range topology.EdgeNodeIDs {
			if _, exists := members[node]; exists {
				return fmt.Errorf("physical Edge belongs to multiple referenced Cells")
			}
			members[node] = topology.AuthorityCellID
		}
	}
	declared := map[string]string{}
	for _, edge := range intent.EdgeTopology.Edges {
		if members[edge.ID] != edge.AuthorityCellID {
			return fmt.Errorf("DNS topology Edge lacks signed routing membership")
		}
		declared[edge.ID] = edge.AuthorityCellID
	}
	for _, endpoint := range snapshot.DNSEdgeEndpoints {
		if declared[endpoint.EdgeID] != endpoint.EdgeGroupID || declared[endpoint.EdgeID] == "" {
			return fmt.Errorf("DNS endpoint lacks declared neutral topology")
		}
	}
	return nil
}

func cellDNSConstraintAllows(c EdgeSelectionConstraint, edge edgetopology.Edge) bool {
	return slices.ContainsFunc(edge.ServingPoolIDs, func(id string) bool { return slices.Contains(c.AllowedPoolIDs, id) }) &&
		!slices.ContainsFunc(c.RequiredCapabilities, func(id string) bool { return !slices.Contains(edge.Capabilities, id) }) &&
		(len(c.AllowedCountries) == 0 || slices.Contains(c.AllowedCountries, edge.Labels["country"]))
}

// Each Cell supplies its own route projection and hard route constraints.
// Combining these immutable inputs never recompiles a Cell's routes under DNS
// policy. Quorum counts physical Edges once, including across IP families.
func compileCellDNSReadiness(intent PlatformIntent, publications []CellRoutePublicationInput, snapshot RuntimeSnapshot, policy PolicySnapshot) (*DNSReadinessPlan, error) {
	if err := validateCellDNSInputs(intent, publications, snapshot); err != nil {
		return nil, err
	}
	if policy.DNSReadiness == nil || ValidateDNSReadinessPolicy(policy.DNSReadiness) != nil || validateDNSEdgeEndpoints(snapshot.DNSEdgeEndpoints, snapshot.CapturedAt) != nil {
		return nil, fmt.Errorf("DNS Cell readiness policy or endpoint observations invalid")
	}
	edges := map[string]edgetopology.Edge{}
	for _, edge := range intent.EdgeTopology.Edges {
		edges[edge.ID] = edge
	}
	constraints := map[string]EdgeSelectionConstraint{}
	for _, c := range policy.EdgeSelectionConstraints {
		constraints[c.Hostname] = c
	}
	usedConstraints := map[string]bool{}
	probes := map[string]DNSReadinessProbe{}
	records := map[string]DNSReadinessRecord{}
	for _, pub := range publications {
		projection, err := ProjectRouteArtifact(pub.Route)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Routes []CompiledRoute `json:"routes"`
			Policy PolicySnapshot  `json:"policy"`
		}
		raw, _ := json.Marshal(pub.Route.Content)
		if json.Unmarshal(raw, &payload) != nil {
			return nil, fmt.Errorf("Cell compiled routes unavailable")
		}
		projectedByHost := map[string][]model.EdgeRouteIntent{}
		for _, route := range projection.Routes {
			projectedByHost[route.Hostname] = append(projectedByHost[route.Hostname], route)
		}
		byHost := map[string][]CompiledRoute{}
		for _, route := range payload.Routes {
			byHost[route.Hostname] = append(byHost[route.Hostname], route)
		}
		refDigest, _ := Digest(pub.Reference)
		for _, record := range intent.DNS {
			if DNSPlacementOptions(record) == nil {
				continue
			}
			complete, owners := true, []CompiledRoute{}
			requiredProjection := projection
			requiredProjection.Routes = nil
			for _, host := range DNSPlacementHostnames(record) {
				complete = complete && len(byHost[host]) > 0
				owners = append(owners, byHost[host]...)
				requiredProjection.Routes = append(requiredProjection.Routes, projectedByHost[host]...)
			}
			if !complete {
				continue
			}
			if err := ValidateDNSRouteOwners(record, owners); err != nil {
				return nil, err
			}
			rules := []EdgeSelectionConstraint{}
			minimum, cells, freshness := max(policy.MinimumHealthyEdges, payload.Policy.MinimumHealthyEdges), 0, policy.DNSReadiness.FactFreshnessSeconds
			for _, route := range requiredProjection.Routes {
				minimum = max(minimum, route.MinHealthyEdgeNodes)
			}
			domains := map[string]int{}
			for _, host := range DNSPlacementHostnames(record) {
				if c, ok := constraints[host]; ok {
					usedConstraints[host] = true
					for _, route := range byHost[host] {
						if route.TenantID != c.TenantID || c.OwnerKind == "platform" && (!PlatformServiceRouteKind(route.Kind) || route.AppID != "") {
							return nil, fmt.Errorf("DNS Edge selection owner differs from Cell route")
						}
					}
					rules = append(rules, c)
					minimum, cells, freshness = max(minimum, c.MinCandidates), max(cells, c.MinDistinctCells), min(freshness, c.FactMaxAgeSeconds)
					for dimension, n := range c.MinDistinctDomains {
						domains[dimension] = max(domains[dimension], n)
					}
				}
			}
			local := snapshot
			local.DNSEdgeEndpoints = nil
			for _, endpoint := range snapshot.DNSEdgeEndpoints {
				if endpoint.EdgeGroupID != pub.Reference.AuthorityCellID {
					continue
				}
				edge := edges[endpoint.EdgeID]
				allowed := true
				for _, c := range rules {
					allowed = allowed && cellDNSConstraintAllows(c, edge)
				}
				for dimension := range domains {
					allowed = allowed && edge.FailureDomains[dimension] != ""
				}
				if allowed {
					local.DNSEdgeEndpoints = append(local.DNSEdgeEndpoints, endpoint)
				}
			}
			one := intent
			one.DNS = []DNSIntent{record}
			plan, err := compileDNSReadinessProjection(one, owners, local, policy, requiredProjection)
			if err != nil {
				return nil, err
			}
			ids := map[string]string{}
			for _, probe := range plan.Probes {
				oldID := probe.ID
				probe.CellPublicationDigest, probe.FactMaxAgeSeconds = refDigest, freshness
				probe.ID, err = DNSReadinessProbeID(probe)
				if err != nil {
					return nil, err
				}
				ids[oldID], probes[probe.ID] = probe.ID, probe
			}
			for _, planned := range plan.Records {
				merged, exists := records[planned.Hostname]
				if !exists {
					merged = DNSReadinessRecord{Hostname: planned.Hostname, RequireDualStack: planned.RequireDualStack, Targets: []DNSReadinessTarget{}, MinDistinctDomains: map[string]int{}}
				}
				merged.MinimumHealthyEdges = max(merged.MinimumHealthyEdges, planned.MinimumHealthyEdges, minimum)
				merged.MinDistinctCells = max(merged.MinDistinctCells, cells)
				for d, n := range domains {
					merged.MinDistinctDomains[d] = max(merged.MinDistinctDomains[d], n)
				}
				for _, target := range planned.Targets {
					target.FailureDomains = map[string]string{}
					for d := range domains {
						target.FailureDomains[d] = edges[target.EdgeID].FailureDomains[d]
					}
					for i, id := range target.ProbeIDs {
						target.ProbeIDs[i] = ids[id]
					}
					sort.Strings(target.ProbeIDs)
					merged.Targets = append(merged.Targets, target)
				}
				records[planned.Hostname] = merged
			}
		}
	}
	if len(usedConstraints) != len(constraints) {
		return nil, fmt.Errorf("DNS Edge selection constraint lacks a referenced route")
	}
	out := &DNSReadinessPlan{Probes: []DNSReadinessProbe{}, Records: []DNSReadinessRecord{}}
	for _, p := range probes {
		out.Probes = append(out.Probes, p)
	}
	for _, r := range intent.DNS {
		if DNSPlacementOptions(r) != nil && (r.Status == "" || r.Status == model.EdgeRouteStatusActive) {
			if _, ok := records[r.Hostname]; !ok {
				return nil, fmt.Errorf("DNS record %s has no complete referenced Cell routes", r.Hostname)
			}
		}
	}
	for _, r := range records {
		sort.Slice(r.Targets, func(i, j int) bool {
			return r.Targets[i].EdgeID+"\x00"+r.Targets[i].Address < r.Targets[j].EdgeID+"\x00"+r.Targets[j].Address
		})
		if !DNSReadinessQuorum(r, func(DNSReadinessTarget) bool { return true }) {
			return nil, fmt.Errorf("DNS record %s cannot meet physical Edge and failure domain minimums", r.Hostname)
		}
		out.Records = append(out.Records, r)
	}
	sort.Slice(out.Probes, func(i, j int) bool { return out.Probes[i].ID < out.Probes[j].ID })
	sort.Slice(out.Records, func(i, j int) bool { return out.Records[i].Hostname < out.Records[j].Hostname })
	return out, ValidateDNSReadinessPlan(out, policy.DNSReadiness)
}

// CellDNSPlanSource retains configuration and endpoint observations needed to
// independently reproduce every dependency, exclusion and quorum requirement.
type CellDNSPlanSource struct {
	Intent     PlatformIntent    `json:"intent"`
	Endpoints  []DNSEdgeEndpoint `json:"endpoints"`
	CapturedAt *time.Time        `json:"captured_at"`
}

func ValidateDNSCellPlan(artifact model.PlatformArtifact) error {
	pubs, err := DecodeCellRoutePublications(artifact)
	if err != nil {
		return err
	}
	var payload struct {
		Plan   *DNSReadinessPlan  `json:"readiness_plan"`
		Source *CellDNSPlanSource `json:"cell_dns_source"`
		Policy PolicySnapshot     `json:"policy"`
	}
	raw, _ := json.Marshal(artifact.Content)
	if json.Unmarshal(raw, &payload) != nil {
		return fmt.Errorf("invalid DNS readiness content")
	}
	if len(pubs) == 0 {
		if payload.Source != nil {
			return fmt.Errorf("DNS Cell source in ordinary publication")
		}
		if payload.Plan != nil {
			for _, p := range payload.Plan.Probes {
				if p.CellPublicationDigest != "" {
					return fmt.Errorf("foreign Cell proof in ordinary DNS")
				}
			}
		}
		return nil
	}
	if payload.Plan == nil || payload.Source == nil || payload.Source.Intent.PublicationRole != PublicationRoleCellDNS || ValidatePlatformIntent(payload.Source.Intent) != nil || ValidatePolicySnapshot(payload.Policy) != nil {
		return fmt.Errorf("DNS Cell requirements or source missing")
	}
	intentDigest, _ := Digest(NormalizePlatformIntent(payload.Source.Intent))
	policyDigest, _ := Digest(payload.Policy)
	if intentDigest != artifact.Metadata["intent_digest"] || policyDigest != artifact.Metadata["policy_digest"] {
		return fmt.Errorf("DNS Cell source differs from signed lineage")
	}
	if _, err := CompileTrafficConsumerTopology(payload.Source.Intent, payload.Policy); err != nil {
		return err
	}
	want, err := compileCellDNSReadiness(payload.Source.Intent, pubs, RuntimeSnapshot{DNSEdgeEndpoints: payload.Source.Endpoints, CapturedAt: payload.Source.CapturedAt}, payload.Policy)
	if err != nil {
		return err
	}
	wantDigest, _ := Digest(want)
	actualDigest, _ := Digest(payload.Plan)
	if wantDigest != actualDigest {
		return fmt.Errorf("DNS Cell requirements differ from referenced route/TLS publications")
	}
	return nil
}

func DNSReadinessFactMaxAge(p DNSReadinessProbe, policy *DNSReadinessPolicy) int {
	if p.FactMaxAgeSeconds > 0 {
		return min(p.FactMaxAgeSeconds, policy.FactFreshnessSeconds)
	}
	return policy.FactFreshnessSeconds
}

// DNSReadinessQuorum is shared by answer generation, local health and remote
// fact validation, so none of them can count duplicate IPs as independent Edges.
func DNSReadinessQuorum(r DNSReadinessRecord, ready func(DNSReadinessTarget) bool) bool {
	for _, family := range []string{"", "A", "AAAA"} {
		if family != "" && !r.RequireDualStack {
			continue
		}
		edges, cells := map[string]bool{}, map[string]bool{}
		domains := map[string]map[string]bool{}
		for d := range r.MinDistinctDomains {
			domains[d] = map[string]bool{}
		}
		for _, target := range r.Targets {
			if family != "" && target.Family != family || !ready(target) {
				continue
			}
			edges[target.EdgeID], cells[target.EdgeGroupID] = true, true
			for d := range domains {
				if value := target.FailureDomains[d]; value != "" {
					domains[d][value] = true
				}
			}
		}
		if len(edges) < r.MinimumHealthyEdges || len(cells) < r.MinDistinctCells {
			return false
		}
		for d, n := range r.MinDistinctDomains {
			if len(domains[d]) < n {
				return false
			}
		}
	}
	return true
}
