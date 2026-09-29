package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func cellRouteCompileFixture(t *testing.T) CompileRequest {
	t.Helper()
	r := declaredCellCompileFixture(t)
	r.Intent.PublicationRole, r.Policy.PublicationRole = PublicationRoleCellRoutes, PublicationRoleCellRoutes
	r.Intent.DNSConsumers, r.RuntimeSnapshot.DNSConsumers = nil, nil
	r.Policy.TLSReadiness = &ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxProbes: 4096, MaxConcurrency: 8}
	r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "dns-constraint", Hostname: "app.example.test", EdgeGroupID: "cell-other", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 2}}
	topology, err := TrafficConsumerTopologyFromIntent(r.Intent)
	if err != nil {
		t.Fatal(err)
	}
	r.Policy.ConsumerTopologyDigest, _ = Digest(topology)
	return r
}

func TestCellRoutePublicationOwnsOnlyRouteTLSAndPreservesDNSConstraints(t *testing.T) {
	r := cellRouteCompileFixture(t)
	before, _ := json.Marshal(r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	parent := BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "tls"}, time.Now())
	parent.ScopeKey = parent.Scope.Key
	kinds, err := ValidateReleaseComposition(parent)
	if err != nil || !reflect.DeepEqual(kinds, []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}) || compiled.DNSArtifact.ID != "" {
		t.Fatal("route-only publication invented DNS authority", kinds, err)
	}
	for _, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.TLSArtifact} {
		child.ScopeKey = child.Scope.Key
		if ValidateTrafficConsumerTopologyProjection(parent, child) != nil {
			t.Fatal("child lost exact route membership")
		}
	}
	projection, err := ProjectRouteArtifact(compiled.RouteArtifact)
	var payload struct {
		Routes []CompiledRoute `json:"routes"`
	}
	raw, _ := json.Marshal(compiled.RouteArtifact.Content)
	if json.Unmarshal(raw, &payload) != nil || len(payload.Routes) != 1 || payload.Routes[0].DNSPlacementEdgeGroupID != "cell-other" {
		t.Fatal("DNS constraint lost from signed route artifact")
	}
	if err != nil || len(projection.Routes) != 1 || projection.Routes[0].MinHealthyEdgeNodes != 2 {
		t.Fatal("independent route compilation erased cross-cell DNS constraints", projection, err)
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated configuration")
	}
}

func TestCellRoutePublicationRejectsImplicitRolesAndDNSOwnership(t *testing.T) {
	for _, scenario := range []string{"missing role", "foreign role", "global", "missing TLS", "extra DNS", "DNS intent", "DNS consumer", "DNS policy", "DNS facts", "foreign child role"} {
		t.Run(scenario, func(t *testing.T) {
			r := cellRouteCompileFixture(t)
			switch scenario {
			case "global":
				r.Intent.Scope, r.Policy.Scope = "global", "global"
			case "DNS intent":
				r.Intent.DNS = []DNSIntent{{Hostname: "test.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60}}
			case "DNS consumer":
				r.Intent.DNSConsumers = declaredCellCompileFixture(t).Intent.DNSConsumers
			case "DNS policy":
				r.Policy.DNSQueryPolicy = &DNSQueryPolicy{}
			case "DNS facts":
				r.RuntimeSnapshot.DNSConsumers = declaredCellCompileFixture(t).RuntimeSnapshot.DNSConsumers
			}
			compiled, err := Compile(r)
			if scenario == "global" || scenario == "DNS intent" || scenario == "DNS consumer" || scenario == "DNS policy" || scenario == "DNS facts" {
				if err == nil {
					t.Fatal("invalid role input compiled")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			parent := BuildReleaseSetArtifact(compiled.ReleaseSet, []string{"route", "tls"}, time.Now())
			parent.ScopeKey = parent.Scope.Key
			switch scenario {
			case "missing role":
				delete(parent.Content, "publication_role")
			case "foreign role":
				parent.Content["publication_role"] = "other"
			case "missing TLS":
				parent.Content["artifact_ids"] = []string{"route"}
				parent.Content["artifact_kinds"] = []string{model.PlatformArtifactKindEdgeRouteBundle}
			case "extra DNS":
				parent.Content["artifact_ids"] = []string{"route", "dns", "tls"}
				parent.Content["artifact_kinds"] = PublicationArtifactKinds("")
			case "foreign child role":
				child := compiled.RouteArtifact
				child.ScopeKey = child.Scope.Key
				delete(child.Content["policy"].(map[string]any), "publication_role")
				if ValidateTrafficConsumerTopologyProjection(parent, child) == nil {
					t.Fatal("child role lost")
				}
				return
			}
			if _, err := ValidateReleaseComposition(parent); err == nil {
				t.Fatal("invalid composition accepted")
			}
		})
	}
}
