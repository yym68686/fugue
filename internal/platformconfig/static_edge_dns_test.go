package platformconfig

import (
	"encoding/json"
	"fugue/internal/model"
	"reflect"
	"strings"
	"testing"
)

func staticDNSCompileFixture() CompileRequest {
	r := availabilityCompileFixture()
	r.Policy.DNSPlacementMode = DNSPlacementConsumerReadiness
	r.Policy.MinimumHealthyEdges = 1
	base := consumerPlacementFixture()
	r.Policy.TLSReadiness, r.Policy.DNSAuthorities, r.Policy.DNSClientPolicies, r.Policy.TrafficRolloutCohorts = base.Policy.TLSReadiness, base.Policy.DNSAuthorities, base.Policy.DNSClientPolicies, base.Policy.TrafficRolloutCohorts
	availability := &r.Policy.DNSQueryPolicy.AvailabilityRoutes[0].Policy
	availability.OrderedEdgeIDs = []string{"static-a", "edge-b", "edge-a"}
	r.Policy.StaticEdgeRoutes = []StaticEdgeDNSRoute{{AppID: r.Intent.Routes[0].AppID, TenantID: r.Intent.Routes[0].TenantID, Hostname: r.Intent.Routes[0].Hostname, Routing: model.AppStaticEdgeRouting{Version: model.StaticEdgeDNSVersion, Availability: *model.CloneDNSAvailabilityPolicy(availability), Candidates: []model.StaticEdgeDNSCandidate{{RegistrationID: "registration-a", EdgeID: "static-a", Addresses: []string{"1.1.1.1"}, ProbePath: "/health", RouteDigest: "sha256:" + strings.Repeat("c", 64), StatusCode: 200}}}}}
	r.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs = []string{"static-a", "edge-b", "edge-a"}
	r.RuntimeSnapshot.DNSSelections[0].PhysicalSelection = nil
	r.RuntimeSnapshot.DNSSelections[0].Candidates = append(r.RuntimeSnapshot.DNSSelections[0].Candidates, DNSSelectionCandidate{IP: "1.1.1.1", EdgeID: "static-a", EdgeGroupID: StaticEdgeDNSGroup("static-a")})
	rebindPlacement(&r)
	r.RuntimeSnapshot.DNSPlacements = nil
	return r
}

func TestStaticDNSCompileAddsIndependentAuthorityWithoutReadiness(t *testing.T) {
	r := staticDNSCompileFixture()
	before, _ := json.Marshal(r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(compiled.DNSArtifact.Content["readiness_plan"])
	var plan DNSReadinessPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	independent := 0
	for _, p := range plan.Probes {
		if p.StaticEdge != nil {
			independent++
			if p.CellPublicationDigest != "" || p.PreviousAuthority != nil || p.Path != "/health" || p.RouteDigest != r.Policy.StaticEdgeRoutes[0].Routing.Candidates[0].RouteDigest {
				t.Fatal("static authority became managed authority", p)
			}
		}
	}
	if independent != 1 {
		t.Fatal("missing independent static readiness", plan)
	}
	views := compiledQueryViews(t, compiled)
	if len(views[0].Records[0].Candidates) != 3 {
		t.Fatal("static candidate not authorized")
	}
	for _, candidate := range views[0].Records[0].Candidates {
		if candidate.Healthy || candidate.TLSReady || candidate.RouteReady {
			t.Fatal("invented readiness")
		}
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated input")
	}
	clone := NormalizePolicySnapshot(r.Policy)
	clone.StaticEdgeRoutes[0].Routing.Candidates[0].Addresses[0] = "8.8.8.8"
	if reflect.DeepEqual(clone.StaticEdgeRoutes, r.Policy.StaticEdgeRoutes) {
		t.Fatal("normalization aliases policy")
	}
}

func TestStaticDNSRejectsSharedHostsAndAuthorityConfusion(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"different app":    func(r *CompileRequest) { r.Policy.StaticEdgeRoutes[0].AppID = "another-app" },
		"different tenant": func(r *CompileRequest) { r.Policy.StaticEdgeRoutes[0].TenantID = "another-tenant" },
		"shared path": func(r *CompileRequest) {
			route := r.Intent.Routes[0]
			route.PathPrefix = "/other"
			r.Intent.Routes = append(r.Intent.Routes, route)
		},
		"private address": func(r *CompileRequest) {
			r.Policy.StaticEdgeRoutes[0].Routing.Candidates[0].Addresses = []string{"10.0.0.1"}
		},
		"managed address": func(r *CompileRequest) {
			r.Policy.StaticEdgeRoutes[0].Routing.Candidates[0].Addresses = []string{"93.184.216.34"}
		},
		"cell publication":                  func(r *CompileRequest) { r.Policy.PublicationRole = PublicationRoleCellDNS },
		"placement without consumer probes": func(r *CompileRequest) { r.Policy.DNSPlacementMode = "" },
	} {
		t.Run(name, func(t *testing.T) {
			r := staticDNSCompileFixture()
			mutate(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("unsafe static DNS authority accepted")
			}
		})
	}
}
