package edgetopology

import (
	"strings"
	"testing"
	"time"
)

func candidateFixture(t *testing.T) (Intent, RouteGrant, []RouteFact, time.Time) {
	t.Helper()
	intent := productionIntent(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	grant := RouteGrant{
		TenantID: "tenant_example", Hostname: "app.example.test",
		RequiredRouteDigests: []string{"sha256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("b", 64)},
		AllowedPoolIDs:       []string{"pool-public"}, RequiredCapabilities: []string{"http", "tls"},
		MinCandidates: 2, MinDistinctCells: 2, MinDistinctDomains: map[string]int{"host": 2},
		FactMaxAgeSeconds: 60,
	}
	facts := make([]RouteFact, 0, len(intent.Edges))
	cells := make(map[string]string, len(intent.Cells))
	for _, cell := range intent.Cells {
		cells[cell.ID] = cell.ServingGroupID()
	}
	for _, edge := range intent.Edges {
		facts = append(facts, RouteFact{
			EdgeID: edge.ID, LegacyGroupID: cells[edge.AuthorityCellID], ReadyRouteDigests: append([]string(nil), grant.RequiredRouteDigests...),
			TLSHostname: grant.Hostname, ObservedAt: now.Add(-10 * time.Second), ValidUntil: now.Add(time.Minute),
			Healthy: true, RouteReady: true, TLSReady: true, CapacityAvailable: true,
		})
	}
	return intent, grant, facts, now
}

func TestEligibleCandidatesCrossCellWithoutCountryPin(t *testing.T) {
	intent, grant, facts, now := candidateFixture(t)
	got, err := intent.EligibleCandidates(grant, facts, now)
	if err != nil || len(got) != 3 || got[0].EdgeID != "bwg" || got[2].EdgeID != "vps-84c8f0a9" {
		t.Fatalf("unexpected cross-cell candidates: %+v, %v", got, err)
	}
	got[0].FailureDomains["host"] = "changed"
	if intent.Edges[0].FailureDomains["host"] == "changed" {
		t.Fatal("candidate mutated topology intent")
	}
}

func TestEligibleCandidatesRespectEveryHardGate(t *testing.T) {
	tests := map[string]func(*RouteGrant, []RouteFact){
		"missing path proof": func(_ *RouteGrant, facts []RouteFact) {
			facts[0].ReadyRouteDigests = facts[0].ReadyRouteDigests[:1]
			facts[2].ReadyRouteDigests = facts[2].ReadyRouteDigests[:1]
		},
		"TLS hostname": func(_ *RouteGrant, facts []RouteFact) {
			facts[0].TLSHostname = "other.test"
			facts[2].TLSHostname = "other.test"
		},
		"health": func(_ *RouteGrant, facts []RouteFact) { facts[0].Healthy = false; facts[2].Healthy = false },
		"capacity": func(_ *RouteGrant, facts []RouteFact) {
			facts[0].CapacityAvailable = false
			facts[2].CapacityAvailable = false
		},
		"quarantine":        func(_ *RouteGrant, facts []RouteFact) { facts[0].Quarantined = true; facts[2].Quarantined = true },
		"draining":          func(_ *RouteGrant, facts []RouteFact) { facts[0].Draining = true; facts[2].Draining = true },
		"group mismatch":    func(_ *RouteGrant, facts []RouteFact) { facts[2].LegacyGroupID = "edge-group-country-us" },
		"expired":           func(_ *RouteGrant, facts []RouteFact) { facts[2].ValidUntil = facts[2].ObservedAt.Add(time.Second) },
		"country residency": func(grant *RouteGrant, _ []RouteFact) { grant.AllowedCountries = []string{"us"} },
		"exclusion":         func(grant *RouteGrant, _ []RouteFact) { grant.ExcludedEdgeIDs = []string{"vps-84c8f0a9"} },
		"pool":              func(grant *RouteGrant, _ []RouteFact) { grant.AllowedPoolIDs = []string{"pool-other"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			intent, grant, facts, now := candidateFixture(t)
			mutate(&grant, facts)
			if got, err := intent.EligibleCandidates(grant, facts, now); err == nil || got != nil {
				t.Fatalf("hard gate %s admitted candidates %+v: %v", name, got, err)
			}
		})
	}
}

func TestEligibleCandidatesRejectStaleAndDuplicateFacts(t *testing.T) {
	intent, grant, facts, now := candidateFixture(t)
	facts[2].ObservedAt = now.Add(-time.Minute - time.Second)
	if got, err := intent.EligibleCandidates(grant, facts, now); err == nil || got != nil {
		t.Fatalf("stale cross-cell proof was accepted: %+v, %v", got, err)
	}
	_, grant, facts, now = candidateFixture(t)
	facts = append(facts, facts[0])
	if got, err := intent.EligibleCandidates(grant, facts, now); err == nil || got != nil {
		t.Fatalf("ambiguous duplicate Edge fact was accepted: %+v, %v", got, err)
	}
}

func TestRouteGrantRequiresExplicitResidencyAndRiskPolicy(t *testing.T) {
	intent, grant, facts, now := candidateFixture(t)
	grant.MinDistinctDomains = map[string]int{"provider": 2}
	if got, err := intent.EligibleCandidates(grant, facts, now); err == nil || got != nil {
		t.Fatalf("missing provider evidence was accepted: %+v, %v", got, err)
	}
	_, grant, facts, now = candidateFixture(t)
	grant.MinDistinctDomains = map[string]int{"country": 2}
	if got, err := intent.EligibleCandidates(grant, facts, now); err == nil || got != nil {
		t.Fatalf("country was accepted as a failure domain: %+v, %v", got, err)
	}
}
