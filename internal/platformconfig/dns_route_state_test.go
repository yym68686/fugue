package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func errorPageDNSFixture() CompileRequest {
	r := placementFixture()
	r.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
	r.Intent.Routes[0].Enabled = false
	r.Policy.DNSRouteStateConstraints = []DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, InactiveBehavior: "serve_error_page"}}
	for i := range r.RuntimeSnapshot.DNSPlacements[0].Candidates {
		r.RuntimeSnapshot.DNSPlacements[0].Candidates[i].InactiveRoutesVerified = true
	}
	rebindPlacement(&r)
	return r
}

func TestInactiveDNSRequiresExplicitPolicyAndExactStateEvidence(t *testing.T) {
	r := errorPageDNSFixture()
	before, _ := json.Marshal(r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if records := flattenedRecords(t, compiled); len(records) != 2 || len(records[0].ValueExpirations) != 2 {
		t.Fatal(records)
	}
	r.CreatedAt = time.Now().Add(24 * time.Hour)
	replayed, err := Compile(r)
	if err != nil || !reflect.DeepEqual(compiled.DNSArtifact.Content, replayed.DNSArtifact.Content) {
		t.Fatal("wall clock changed output", err)
	}
	r.CreatedAt = time.Time{}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated input")
	}
	r.Policy.DNSRouteStateConstraints = nil
	rebindPlacement(&r)
	omitted, err := Compile(r)
	if err != nil || len(flattenedRecords(t, omitted)) != 0 {
		t.Fatal("implicit policy restored inactive DNS", err)
	}
	if !reflect.DeepEqual(compiled.RouteArtifact.Content["routes"], omitted.RouteArtifact.Content["routes"]) {
		t.Fatal("DNS policy changed route or upstream behavior")
	}
	for name, mutate := range map[string]func(*CompileRequest){
		"missing state proof": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].InactiveRoutesVerified = false
		},
		"TLS failed":   func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].TLSReady = false },
		"route failed": func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].RouteReady = false },
		"unhealthy":    func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].Healthy = false },
		"expired": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ValidUntil = *r.RuntimeSnapshot.CapturedAt
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := errorPageDNSFixture()
			mutate(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("unsafe inactive DNS accepted")
			}
		})
	}
}

func TestDNSRouteStatePolicyIsBoundedAndCannotEnableDisallowedRoutes(t *testing.T) {
	r := errorPageDNSFixture()
	p := r.Policy
	for _, rules := range [][]DNSRouteStateConstraint{
		{{RecordKind: "unknown", InactiveBehavior: "serve_error_page"}},
		{{RecordKind: "custom-domain-target", InactiveBehavior: "execute_origin"}},
		{{RecordKind: "custom-domain-target", InactiveBehavior: "omit"}, {RecordKind: "custom-domain-target", InactiveBehavior: "serve_error_page"}},
	} {
		p.DNSRouteStateConstraints = rules
		if ValidatePolicySnapshot(p) == nil {
			t.Fatal("invalid policy accepted", rules)
		}
	}
	p = r.Policy
	for _, route := range []CompiledRoute{
		{RouteIntent: RouteIntent{Enabled: false, Status: "active", RoutePolicy: "route_a_only"}},
		{RouteIntent: RouteIntent{Enabled: false, Status: "disabled", RoutePolicy: "route_a_only"}},
		{RouteIntent: RouteIntent{Enabled: false, Status: "unknown", RoutePolicy: "edge_enabled"}},
	} {
		if DNSRouteStateAllowed(r.Intent.DNS[0], route, p) {
			t.Fatal("policy bypassed route state", route)
		}
	}
	for _, status := range []string{"disabled", "unavailable"} {
		if !DNSRouteStateAllowed(r.Intent.DNS[0], CompiledRoute{RouteIntent: RouteIntent{Enabled: false, Status: status, RoutePolicy: "edge_enabled"}}, p) {
			t.Fatal("explicit error page rejected", status)
		}
	}
}
