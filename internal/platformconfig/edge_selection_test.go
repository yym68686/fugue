package platformconfig

import (
	"encoding/json"
	"slices"
	"testing"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

func edgeSelectionFixture() CompileRequest {
	r := dnsReadinessFixture()
	r.Intent.EdgeTopology = &edgetopology.Intent{
		SchemaVersion: edgetopology.SchemaVersion,
		Cells: []edgetopology.AuthorityCell{
			{ID: "cell-a", LegacyGroupID: "edge-group-a"},
			{ID: "cell-b", LegacyGroupID: "edge-group-b"},
		},
		Pools: []edgetopology.ServingPool{{ID: "pool-public"}},
		Edges: []edgetopology.Edge{
			{ID: "edge-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-a"}, Labels: map[string]string{"country": "de"}},
			{ID: "edge-b", AuthorityCellID: "cell-b", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-b"}, Labels: map[string]string{"country": "us"}},
		},
	}
	r.Policy.EdgeSelectionConstraints = []EdgeSelectionConstraint{{
		TenantID: "tenant-a", Hostname: "app.example.test", AllowedPoolIDs: []string{"pool-public"},
		RequiredCapabilities: []string{"http", "tls"}, MinCandidates: 2,
		MinDistinctCells: 2, MinDistinctDomains: map[string]int{"host": 2}, FactMaxAgeSeconds: 60,
	}}
	r.Intent.Routes = append(r.Intent.Routes, RouteIntent{
		Hostname: "app.example.test", PathPrefix: "/api", AppID: "app-a", TenantID: "tenant-a",
		UpstreamURL: "http://origin:8080", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled,
	})
	rebindPlacement(&r)
	return r
}

func TestEdgeSelectionGrantsBindEveryPathToEachCell(t *testing.T) {
	r := edgeSelectionFixture()
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(compiled.RouteArtifact.Content["edge_selection_grants"])
	if err != nil {
		t.Fatal(err)
	}
	var grants []edgetopology.RouteGrant
	if err := json.Unmarshal(raw, &grants); err != nil {
		t.Fatal(err)
	}
	if len(grants) != 1 || grants[0].TenantID != "tenant-a" || len(grants[0].RequiredRouteDigestsByCell) != 2 {
		t.Fatalf("missing signed Edge grants: %+v", grants)
	}
	projected, err := ProjectRouteArtifact(compiled.RouteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range r.Intent.EdgeTopology.Cells {
		digests := grants[0].RequiredRouteDigestsByCell[cell.ID]
		if len(digests) != 2 {
			t.Fatalf("cell %s lost a route path: %+v", cell.ID, digests)
		}
		for _, route := range projected.Routes {
			digest, err := routeproof.Digest(routebinding.FromIntent(route, cell.ServingGroupID()))
			if err != nil || !slices.Contains(digests, digest) {
				t.Fatalf("cell %s proof differs from executor: %v", cell.ID, err)
			}
		}
	}
	if slices.Equal(grants[0].RequiredRouteDigestsByCell["cell-a"], grants[0].RequiredRouteDigestsByCell["cell-b"]) {
		t.Fatal("serving group was omitted from route proofs")
	}
	if compiled.PolicyArtifact.Content["edge_selection_constraints"] == nil {
		t.Fatal("signed policy lost selection constraints")
	}
	baseline := dnsReadinessFixture()
	rebindPlacement(&baseline)
	old, err := Compile(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if _, added := old.RouteArtifact.Content["edge_selection_grants"]; added {
		t.Fatal("legacy route artifact gained an implicit selection grant")
	}
}

func TestEdgeSelectionGrantRejectsForeignAndIneligibleDependencies(t *testing.T) {
	tests := map[string]func(*CompileRequest){
		"missing topology":   func(r *CompileRequest) { r.Intent.EdgeTopology = nil },
		"foreign path":       func(r *CompileRequest) { r.Intent.Routes[1].TenantID = "tenant-b" },
		"inactive path":      func(r *CompileRequest) { r.Intent.Routes[1].Enabled = false },
		"unknown pool":       func(r *CompileRequest) { r.Policy.EdgeSelectionConstraints[0].AllowedPoolIDs = []string{"pool-other"} },
		"missing capability": func(r *CompileRequest) { r.Intent.EdgeTopology.Edges[1].Capabilities = []string{"http"} },
		"DNS group pin":      func(r *CompileRequest) { r.Intent.DNS[0].EdgeGroupID = "edge-group-a" },
		"stronger route minimum": func(r *CompileRequest) {
			r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "route", Hostname: "app.example.test", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 3}}
		},
		"missing host domain": func(r *CompileRequest) {
			r.Intent.EdgeTopology.Edges[1].FailureDomains = map[string]string{"provider": "provider-b"}
		},
		"stale fact policy": func(r *CompileRequest) { r.Policy.EdgeSelectionConstraints[0].FactMaxAgeSeconds = 121 },
		"country as risk domain": func(r *CompileRequest) {
			r.Policy.EdgeSelectionConstraints[0].MinDistinctDomains = map[string]int{"country": 2}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			r := edgeSelectionFixture()
			mutate(&r)
			rebindPlacement(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid Edge selection produced a grant")
			}
		})
	}
}

func TestEdgeSelectionGrantRespectsExistingDNSGroupPin(t *testing.T) {
	r := edgeSelectionFixture()
	r.Intent.DNS[0].EdgeGroupID = "edge-group-a"
	r.Policy.MinimumHealthyEdges = 1
	r.Policy.EdgeSelectionConstraints[0].MinCandidates = 1
	r.Policy.EdgeSelectionConstraints[0].MinDistinctCells = 1
	r.Policy.EdgeSelectionConstraints[0].MinDistinctDomains = map[string]int{"host": 1}
	rebindPlacement(&r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(compiled.RouteArtifact.Content["edge_selection_grants"])
	if err != nil {
		t.Fatal(err)
	}
	var grants []edgetopology.RouteGrant
	if err := json.Unmarshal(raw, &grants); err != nil {
		t.Fatal(err)
	}
	if len(grants) != 1 || len(grants[0].RequiredRouteDigestsByCell) != 1 || len(grants[0].RequiredRouteDigestsByCell["cell-a"]) != 2 || !slices.Equal(grants[0].ExcludedEdgeIDs, []string{"edge-b"}) {
		t.Fatalf("DNS group pin was widened by Edge selection: %+v", grants)
	}
}

func TestEdgeSelectionPolicyNormalizationDoesNotAliasCaller(t *testing.T) {
	r := edgeSelectionFixture()
	original := r.Policy.EdgeSelectionConstraints[0]
	normalized := NormalizePolicySnapshot(r.Policy)
	normalized.EdgeSelectionConstraints[0].AllowedPoolIDs[0] = "pool-changed"
	normalized.EdgeSelectionConstraints[0].MinDistinctDomains["host"] = 3
	if original.AllowedPoolIDs[0] != "pool-public" || original.MinDistinctDomains["host"] != 2 {
		t.Fatal("normalizing Edge selection policy mutated caller")
	}
}

func TestPlatformServiceGrantCannotAuthorizeTenantOrAppRoutes(t *testing.T) {
	fixture := func() CompileRequest {
		r := edgeSelectionFixture()
		r.Policy.EdgeSelectionConstraints[0].OwnerKind = "platform"
		r.Policy.EdgeSelectionConstraints[0].TenantID = ""
		for i := range r.Intent.Routes {
			r.Intent.Routes[i].Kind = model.EdgeRouteKindPlatform
			r.Intent.Routes[i].TenantID = ""
			r.Intent.Routes[i].AppID = ""
		}
		r.Intent.DNS[0].TenantID = ""
		r.Intent.DNS[0].AppID = ""
		r.Intent.DNS[0].Type = "FUGUE_ROUTE"
		r.Intent.DNS[0].Values = []string{}
		r.Intent.DNS[0].Application = nil
		r.Intent.DNS[0].Route = &DNSRouteIntent{Hostnames: []string{"app.example.test"}, DNSApplicationIntent: DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}
		rebindPlacement(&r)
		return r
	}
	r := fixture()
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(compiled.RouteArtifact.Content["edge_selection_grants"])
	var grants []edgetopology.RouteGrant
	if json.Unmarshal(raw, &grants) != nil || len(grants) != 1 || grants[0].OwnerKind != "platform" || grants[0].TenantID != "" {
		t.Fatal("platform service owner was not preserved")
	}
	for name, mutate := range map[string]func(*CompileRequest){
		"tenant owner":       func(r *CompileRequest) { r.Intent.Routes[0].TenantID = "tenant-a" },
		"app owner":          func(r *CompileRequest) { r.Intent.Routes[0].AppID = "app-a" },
		"non-platform route": func(r *CompileRequest) { r.Intent.Routes[0].Kind = model.EdgeRouteKindPlatformDomain },
		"implicit platform":  func(r *CompileRequest) { r.Policy.EdgeSelectionConstraints[0].OwnerKind = "" },
		"unknown owner kind": func(r *CompileRequest) { r.Policy.EdgeSelectionConstraints[0].OwnerKind = "shared" },
	} {
		t.Run(name, func(t *testing.T) {
			r := fixture()
			mutate(&r)
			rebindPlacement(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid platform owner accepted")
			}
		})
	}
}
