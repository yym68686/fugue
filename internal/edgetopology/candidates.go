package edgetopology

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

var routeDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var hostnamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)

// RouteGrant is the policy projection for one tenant and hostname. Its caller
// must verify the signed source artifact before using this compiler for serving.
type RouteGrant struct {
	TenantID                   string              `json:"tenant_id"`
	Hostname                   string              `json:"hostname"`
	RequiredRouteDigestsByCell map[string][]string `json:"required_route_digests_by_cell"`
	AllowedPoolIDs             []string            `json:"allowed_pool_ids"`
	AllowedCountries           []string            `json:"allowed_countries,omitempty"`
	RequiredCapabilities       []string            `json:"required_capabilities"`
	ExcludedEdgeIDs            []string            `json:"excluded_edge_ids,omitempty"`
	MinCandidates              int                 `json:"min_candidates"`
	MinDistinctCells           int                 `json:"min_distinct_cells,omitempty"`
	MinDistinctDomains         map[string]int      `json:"min_distinct_domains,omitempty"`
	FactMaxAgeSeconds          int                 `json:"fact_max_age_seconds"`
}

// RouteFact is an observation of the exact loaded route and TLS hostname on
// one Edge. Its caller must verify the observation's identity and provenance.
type RouteFact struct {
	EdgeID            string    `json:"edge_id"`
	LegacyGroupID     string    `json:"legacy_group_id"`
	ReadyRouteDigests []string  `json:"ready_route_digests"`
	TLSHostname       string    `json:"tls_hostname"`
	ObservedAt        time.Time `json:"observed_at"`
	ValidUntil        time.Time `json:"valid_until"`
	Healthy           bool      `json:"healthy"`
	RouteReady        bool      `json:"route_ready"`
	TLSReady          bool      `json:"tls_ready"`
	CapacityAvailable bool      `json:"capacity_available"`
	Draining          bool      `json:"draining"`
	Quarantined       bool      `json:"quarantined"`
}

type Candidate struct {
	EdgeID          string            `json:"edge_id"`
	AuthorityCellID string            `json:"authority_cell_id"`
	ServingPoolIDs  []string          `json:"serving_pool_ids"`
	FailureDomains  map[string]string `json:"failure_domains"`
}

func (grant RouteGrant) Validate(intent Intent) error {
	if grant.TenantID == "" || len(grant.TenantID) > 128 || grant.TenantID != strings.TrimSpace(grant.TenantID) || strings.ContainsAny(grant.TenantID, "\r\n") ||
		len(grant.Hostname) > 253 || !hostnamePattern.MatchString(grant.Hostname) || strings.Contains(grant.Hostname, "..") ||
		grant.MinCandidates < 1 || grant.MinCandidates > 100 ||
		grant.MinDistinctCells < 0 || grant.MinDistinctCells > 100 || grant.FactMaxAgeSeconds < 1 || grant.FactMaxAgeSeconds > 3600 {
		return fmt.Errorf("route grant identity or bounds are invalid")
	}
	for _, label := range strings.Split(grant.Hostname, ".") {
		if len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return fmt.Errorf("route grant hostname is invalid")
		}
	}
	if len(grant.AllowedPoolIDs) == 0 || len(grant.RequiredCapabilities) == 0 || len(grant.RequiredRouteDigestsByCell) == 0 || len(grant.RequiredRouteDigestsByCell) > len(intent.Cells) {
		return fmt.Errorf("route grant requires pools, capabilities, and cell-specific route proofs")
	}
	knownCells := make(map[string]struct{}, len(intent.Cells))
	for _, cell := range intent.Cells {
		knownCells[cell.ID] = struct{}{}
	}
	for cellID, digests := range grant.RequiredRouteDigestsByCell {
		if _, ok := knownCells[cellID]; !ok || len(digests) == 0 || len(digests) > 4096 {
			return fmt.Errorf("route grant has invalid route proofs for cell %q", cellID)
		}
		for i, digest := range digests {
			if !routeDigestPattern.MatchString(digest) || (i > 0 && digests[i-1] >= digest) {
				return fmt.Errorf("route grant digests must be valid, ordered, and unique")
			}
		}
	}
	knownPools := make(map[string]struct{}, len(intent.Pools))
	for _, pool := range intent.Pools {
		knownPools[pool.ID] = struct{}{}
	}
	for _, list := range [][]string{grant.AllowedPoolIDs, grant.RequiredCapabilities, grant.ExcludedEdgeIDs, grant.AllowedCountries} {
		if len(list) > 10000 {
			return fmt.Errorf("route grant list is too large")
		}
		for i, value := range list {
			if !validID(value) || (i > 0 && list[i-1] >= value) {
				return fmt.Errorf("route grant lists must be ordered and unique")
			}
		}
	}
	for _, poolID := range grant.AllowedPoolIDs {
		if _, ok := knownPools[poolID]; !ok {
			return fmt.Errorf("route grant references unknown pool %q", poolID)
		}
	}
	for dimension, minimum := range grant.MinDistinctDomains {
		if !validID(dimension) || dimension == "country" || dimension == "region" || minimum < 1 || minimum > 100 {
			return fmt.Errorf("route grant risk diversity is invalid")
		}
	}
	return nil
}

// EligibleCandidates applies hard authorization and current serving evidence.
// It never ranks by geography or latency. Missing or insufficient evidence
// fails closed, preserving the existing independently published LKG.
func (intent Intent) EligibleCandidates(grant RouteGrant, facts []RouteFact, now time.Time) ([]Candidate, error) {
	if err := intent.Validate(); err != nil {
		return nil, err
	}
	if err := grant.Validate(intent); err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, fmt.Errorf("candidate evaluation requires an explicit clock")
	}
	cells := make(map[string]string, len(intent.Cells))
	for _, cell := range intent.Cells {
		cells[cell.ID] = cell.ServingGroupID()
	}
	edges := make(map[string]Edge, len(intent.Edges))
	for _, edge := range intent.Edges {
		edges[edge.ID] = edge
	}
	seenFacts := make(map[string]struct{}, len(facts))
	result := make([]Candidate, 0, len(facts))
	for _, fact := range facts {
		if _, duplicate := seenFacts[fact.EdgeID]; duplicate {
			return nil, fmt.Errorf("duplicate Edge route fact %q", fact.EdgeID)
		}
		seenFacts[fact.EdgeID] = struct{}{}
		edge, known := edges[fact.EdgeID]
		requiredDigests, cellAllowed := grant.RequiredRouteDigestsByCell[edge.AuthorityCellID]
		if !known || cells[edge.AuthorityCellID] != fact.LegacyGroupID ||
			!cellAllowed ||
			!hasAny(edge.ServingPoolIDs, grant.AllowedPoolIDs) ||
			slices.Contains(grant.ExcludedEdgeIDs, edge.ID) ||
			!hasAll(edge.Capabilities, grant.RequiredCapabilities) ||
			(len(grant.AllowedCountries) > 0 && !slices.Contains(grant.AllowedCountries, edge.Labels["country"])) ||
			!routeProofsComplete(fact.ReadyRouteDigests, requiredDigests) || fact.TLSHostname != grant.Hostname ||
			!fact.Healthy || !fact.RouteReady || !fact.TLSReady || !fact.CapacityAvailable ||
			fact.Draining || fact.Quarantined || fact.ObservedAt.After(now) ||
			now.Sub(fact.ObservedAt) > time.Duration(grant.FactMaxAgeSeconds)*time.Second ||
			!fact.ValidUntil.After(now) || !fact.ValidUntil.After(fact.ObservedAt) {
			continue
		}
		missingRiskDimension := false
		for dimension := range grant.MinDistinctDomains {
			if edge.FailureDomains[dimension] == "" {
				missingRiskDimension = true
				break
			}
		}
		if missingRiskDimension {
			continue
		}
		result = append(result, Candidate{
			EdgeID: edge.ID, AuthorityCellID: edge.AuthorityCellID,
			ServingPoolIDs: append([]string(nil), edge.ServingPoolIDs...),
			FailureDomains: cloneDomains(edge.FailureDomains),
		})
	}
	if len(result) < grant.MinCandidates {
		return nil, fmt.Errorf("route grant has %d eligible edges, needs %d", len(result), grant.MinCandidates)
	}
	cellsSeen := make(map[string]struct{})
	for _, candidate := range result {
		cellsSeen[candidate.AuthorityCellID] = struct{}{}
	}
	if len(cellsSeen) < grant.MinDistinctCells {
		return nil, fmt.Errorf("route grant has %d authority cells, needs %d", len(cellsSeen), grant.MinDistinctCells)
	}
	for dimension, minimum := range grant.MinDistinctDomains {
		values := make(map[string]struct{})
		for _, candidate := range result {
			if value := candidate.FailureDomains[dimension]; value != "" {
				values[value] = struct{}{}
			}
		}
		if len(values) < minimum {
			return nil, fmt.Errorf("route grant has %d distinct %s domains, needs %d", len(values), dimension, minimum)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].EdgeID < result[j].EdgeID })
	return result, nil
}

func hasAny(values, wanted []string) bool {
	for _, value := range wanted {
		if slices.Contains(values, value) {
			return true
		}
	}
	return false
}

func hasAll(values, wanted []string) bool {
	for _, value := range wanted {
		if !slices.Contains(values, value) {
			return false
		}
	}
	return true
}

func routeProofsComplete(ready, required []string) bool {
	if len(ready) < len(required) || len(ready) > 4096 {
		return false
	}
	for i, digest := range ready {
		if !routeDigestPattern.MatchString(digest) || (i > 0 && ready[i-1] >= digest) {
			return false
		}
	}
	for _, digest := range required {
		index := sort.SearchStrings(ready, digest)
		if index == len(ready) || ready[index] != digest {
			return false
		}
	}
	return true
}

func cloneDomains(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
