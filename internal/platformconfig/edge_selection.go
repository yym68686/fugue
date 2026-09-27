package platformconfig

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

var edgeSelectionID = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
var edgeSelectionHostname = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)

func PlatformServiceRouteKind(kind string) bool {
	return kind == model.EdgeRouteKindPlatform || kind == model.EdgeRouteKindPlatformRoute ||
		(strings.HasPrefix(kind, "control-plane-") && len(kind) <= 128 && edgeSelectionID.MatchString(kind))
}

// EdgeSelectionConstraint is signed policy for one public hostname. It does
// not choose an Edge or change the currently published DNS answer.
type EdgeSelectionConstraint struct {
	OwnerKind            string         `json:"owner_kind,omitempty"`
	TenantID             string         `json:"tenant_id"`
	Hostname             string         `json:"hostname"`
	AllowedPoolIDs       []string       `json:"allowed_pool_ids"`
	RequiredCapabilities []string       `json:"required_capabilities"`
	AllowedCountries     []string       `json:"allowed_countries,omitempty"`
	MinCandidates        int            `json:"min_candidates"`
	MinDistinctCells     int            `json:"min_distinct_cells,omitempty"`
	MinDistinctDomains   map[string]int `json:"min_distinct_domains,omitempty"`
	FactMaxAgeSeconds    int            `json:"fact_max_age_seconds"`
}

func cloneEdgeDomainMinimums(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func validateEdgeSelectionConstraints(constraints []EdgeSelectionConstraint) error {
	if len(constraints) > 4096 {
		return fmt.Errorf("too many Edge selection constraints")
	}
	seen := make(map[string]bool, len(constraints))
	for _, c := range constraints {
		if !edgetopology.ValidGrantOwner(c.OwnerKind, c.TenantID) ||
			len(c.Hostname) < 3 || len(c.Hostname) > 253 || !edgeSelectionHostname.MatchString(c.Hostname) || strings.Contains(c.Hostname, "..") || seen[c.Hostname] ||
			c.MinCandidates < 1 || c.MinCandidates > 100 || c.MinDistinctCells < 0 || c.MinDistinctCells > 100 ||
			c.FactMaxAgeSeconds < 1 || c.FactMaxAgeSeconds > 600 {
			return fmt.Errorf("Edge selection identity or bounds invalid")
		}
		for _, label := range strings.Split(c.Hostname, ".") {
			if len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return fmt.Errorf("Edge selection hostname invalid")
			}
		}
		seen[c.Hostname] = true
		for _, list := range [][]string{c.AllowedPoolIDs, c.RequiredCapabilities, c.AllowedCountries} {
			if len(list) > 100 {
				return fmt.Errorf("Edge selection list exceeds bound")
			}
			for i, value := range list {
				if len(value) > 128 || !edgeSelectionID.MatchString(value) || i > 0 && list[i-1] >= value {
					return fmt.Errorf("Edge selection list must be canonical and unique")
				}
			}
		}
		if len(c.AllowedPoolIDs) == 0 || len(c.RequiredCapabilities) == 0 {
			return fmt.Errorf("Edge selection requires pools and capabilities")
		}
		for dimension, minimum := range c.MinDistinctDomains {
			if len(dimension) > 128 || !edgeSelectionID.MatchString(dimension) || dimension == "country" || dimension == "region" || minimum < 1 || minimum > 100 {
				return fmt.Errorf("Edge selection risk diversity invalid")
			}
		}
	}
	return nil
}

// CompileEdgeSelectionGrants derives shadow grants from the same compiled
// routes and DNS constraints that the signed ReleaseSet will publish. Current
// runtime evidence must still be verified independently before any selection.
func CompileEdgeSelectionGrants(intent PlatformIntent, policy PolicySnapshot, routes []CompiledRoute, projected model.EdgeRouteIntentSnapshot) ([]edgetopology.RouteGrant, error) {
	if len(policy.EdgeSelectionConstraints) == 0 {
		return nil, nil
	}
	if intent.EdgeTopology == nil || policy.DNSReadiness == nil {
		return nil, fmt.Errorf("Edge selection requires topology and DNS readiness policy")
	}
	if err := intent.EdgeTopology.Validate(); err != nil {
		return nil, err
	}
	if err := validateEdgeSelectionConstraints(policy.EdgeSelectionConstraints); err != nil {
		return nil, err
	}
	byHost := make(map[string][]model.EdgeRouteIntent)
	compiledByHost := make(map[string][]CompiledRoute)
	for _, route := range projected.Routes {
		byHost[route.Hostname] = append(byHost[route.Hostname], route)
	}
	for _, route := range routes {
		compiledByHost[route.Hostname] = append(compiledByHost[route.Hostname], route)
	}
	grants := make([]edgetopology.RouteGrant, 0, len(policy.EdgeSelectionConstraints))
	for _, c := range policy.EdgeSelectionConstraints {
		hostRoutes := byHost[c.Hostname]
		if len(hostRoutes) == 0 || len(hostRoutes) != len(compiledByHost[c.Hostname]) || c.FactMaxAgeSeconds > policy.DNSReadiness.FactFreshnessSeconds || c.MinCandidates < policy.MinimumHealthyEdges {
			return nil, fmt.Errorf("Edge selection hostname %q lacks complete route or freshness policy", c.Hostname)
		}
		for _, route := range hostRoutes {
			if route.TenantID != c.TenantID || c.OwnerKind == "platform" && (!PlatformServiceRouteKind(route.RouteKind) || route.AppID != "") || route.OriginStatus != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) || c.MinCandidates < route.MinHealthyEdgeNodes {
				return nil, fmt.Errorf("Edge selection hostname %q has a foreign or inactive path", c.Hostname)
			}
		}
		records := []DNSIntent{}
		for _, record := range intent.DNS {
			if DNSPlacementOptions(record) != nil && slices.Contains(DNSPlacementHostnames(record), c.Hostname) {
				if !slices.Equal(DNSPlacementHostnames(record), []string{c.Hostname}) {
					return nil, fmt.Errorf("Edge selection hostname %q requires separate grants for shared DNS dependencies", c.Hostname)
				}
				owners := []CompiledRoute{}
				for _, hostname := range DNSPlacementHostnames(record) {
					owners = append(owners, compiledByHost[hostname]...)
				}
				if len(owners) == 0 || record.Status != "" && record.Status != model.EdgeRouteStatusActive {
					return nil, fmt.Errorf("Edge selection hostname %q has an inactive DNS dependency", c.Hostname)
				}
				for _, owner := range owners {
					if !DNSRouteStateAllowed(record, owner, policy) || owner.TenantID != c.TenantID {
						return nil, fmt.Errorf("Edge selection hostname %q has a foreign or inactive DNS dependency", c.Hostname)
					}
				}
				records = append(records, record)
			}
		}
		if len(records) == 0 {
			return nil, fmt.Errorf("Edge selection hostname %q has no public DNS dependency", c.Hostname)
		}
		grant := edgetopology.RouteGrant{
			OwnerKind: c.OwnerKind, TenantID: c.TenantID, Hostname: c.Hostname, AllowedPoolIDs: append([]string(nil), c.AllowedPoolIDs...),
			RequiredCapabilities: append([]string(nil), c.RequiredCapabilities...), AllowedCountries: append([]string(nil), c.AllowedCountries...),
			MinCandidates: c.MinCandidates, MinDistinctCells: c.MinDistinctCells,
			MinDistinctDomains: cloneEdgeDomainMinimums(c.MinDistinctDomains), FactMaxAgeSeconds: c.FactMaxAgeSeconds,
			RequiredRouteDigestsByCell: make(map[string][]string),
		}
		cells := make(map[string]string, len(intent.EdgeTopology.Cells))
		for _, cell := range intent.EdgeTopology.Cells {
			cells[cell.ID] = cell.ServingGroupID()
		}
		eligible := 0
		domainValues := make(map[string]map[string]bool, len(c.MinDistinctDomains))
		for dimension := range c.MinDistinctDomains {
			domainValues[dimension] = make(map[string]bool)
		}
		for _, edge := range intent.EdgeTopology.Edges {
			groupID := cells[edge.AuthorityCellID]
			if !slices.ContainsFunc(edge.ServingPoolIDs, func(id string) bool { return slices.Contains(c.AllowedPoolIDs, id) }) ||
				slices.ContainsFunc(c.RequiredCapabilities, func(id string) bool { return !slices.Contains(edge.Capabilities, id) }) ||
				len(c.AllowedCountries) > 0 && !slices.Contains(c.AllowedCountries, edge.Labels["country"]) {
				continue
			}
			allowed := true
			for _, record := range records {
				owners := []CompiledRoute{}
				for _, hostname := range DNSPlacementHostnames(record) {
					owners = append(owners, compiledByHost[hostname]...)
				}
				allowed = allowed && DNSPlacementAllowsEdge(record, owners, edge.ID, groupID)
			}
			if !allowed {
				grant.ExcludedEdgeIDs = append(grant.ExcludedEdgeIDs, edge.ID)
				continue
			}
			missingDomain := false
			for dimension := range c.MinDistinctDomains {
				if edge.FailureDomains[dimension] == "" {
					missingDomain = true
					break
				}
			}
			if missingDomain {
				continue
			}
			eligible++
			for dimension := range c.MinDistinctDomains {
				if value := edge.FailureDomains[dimension]; value != "" {
					domainValues[dimension][value] = true
				}
			}
			if _, exists := grant.RequiredRouteDigestsByCell[edge.AuthorityCellID]; exists {
				continue
			}
			for _, route := range hostRoutes {
				digest, err := routeproof.Digest(routebinding.FromIntent(route, groupID))
				if err != nil {
					return nil, err
				}
				grant.RequiredRouteDigestsByCell[edge.AuthorityCellID] = append(grant.RequiredRouteDigestsByCell[edge.AuthorityCellID], digest)
			}
			sort.Strings(grant.RequiredRouteDigestsByCell[edge.AuthorityCellID])
		}
		if len(grant.RequiredRouteDigestsByCell) == 0 {
			return nil, fmt.Errorf("Edge selection hostname %q has no statically eligible cell", c.Hostname)
		}
		if eligible < c.MinCandidates || len(grant.RequiredRouteDigestsByCell) < c.MinDistinctCells {
			return nil, fmt.Errorf("Edge selection hostname %q cannot meet candidate or cell minimum", c.Hostname)
		}
		for dimension, minimum := range c.MinDistinctDomains {
			if len(domainValues[dimension]) < minimum {
				return nil, fmt.Errorf("Edge selection hostname %q cannot meet %s diversity", c.Hostname, dimension)
			}
		}
		sort.Strings(grant.ExcludedEdgeIDs)
		if err := grant.Validate(*intent.EdgeTopology); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, nil
}
