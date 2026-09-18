package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

func dnsReadinessFixture() CompileRequest {
	r := placementFixture()
	r.Policy.DNSReadiness = &DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	r.RuntimeSnapshot.DNSEdgeEndpoints = []DNSEdgeEndpoint{
		{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", ObservedAt: *r.RuntimeSnapshot.CapturedAt, A: []string{"93.184.216.34"}, AAAA: []string{"2606:4700:4700::1111"}},
		{EdgeID: "edge-b", EdgeGroupID: "edge-group-b", ObservedAt: *r.RuntimeSnapshot.CapturedAt, A: []string{"93.184.216.35"}, AAAA: []string{"2606:4700:4700::1001"}},
	}
	r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].EdgeGroupID = "edge-group-a"
	r.RuntimeSnapshot.DNSPlacements[0].Candidates[1].EdgeGroupID = "edge-group-b"
	rebindPlacement(&r)
	return r
}

func TestDNSReadinessPlanMatchesSharedRouteProjectionWithoutRenewingValues(t *testing.T) {
	r := dnsReadinessFixture()
	raw, _ := json.Marshal(r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	var plan DNSReadinessPlan
	b, _ := json.Marshal(compiled.DNSArtifact.Content["readiness_plan"])
	json.Unmarshal(b, &plan)
	if len(plan.Probes) != 4 || len(plan.Records) != 1 || len(plan.Records[0].Targets) != 4 {
		t.Fatalf("missing endpoint requirements: %+v", plan)
	}
	projected, err := ProjectRouteArtifact(compiled.RouteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plan.Probes {
		route := projected.Routes[0]
		digest, err := routeproof.Digest(routebinding.FromIntent(route, p.EdgeGroupID))
		if err != nil || p.RouteDigest != digest || p.Hostname != route.Hostname || p.Path != route.PathPrefix {
			t.Fatal("compiler and executor proof disagree", err)
		}
	}
	records := flattenedRecords(t, compiled)
	later, err := DNSRecordsAt(records, r.RuntimeSnapshot.CapturedAt.Add(time.Minute))
	if err != nil || len(later) != 0 {
		t.Fatal("readiness requirements revived expired DNS values", err)
	}
	after, _ := json.Marshal(r)
	if string(raw) != string(after) {
		t.Fatal("compile changed intent or facts")
	}
	slices.Reverse(r.RuntimeSnapshot.DNSEdgeEndpoints)
	r.CreatedAt = time.Now()
	replay, err := Compile(r)
	if err != nil || !reflect.DeepEqual(compiled.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("readiness plan depends on enumeration or wall clock", err)
	}
	r.RuntimeSnapshot.DNSEdgeEndpoints[0].ObservedAt = r.RuntimeSnapshot.DNSEdgeEndpoints[0].ObservedAt.Add(-time.Hour)
	changed, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Lineage.IntentDigest != changed.Lineage.IntentDigest || compiled.Lineage.PolicyDigest != changed.Lineage.PolicyDigest || compiled.Lineage.InputSnapshotDigest == changed.Lineage.InputSnapshotDigest || !reflect.DeepEqual(compiled.DNSArtifact.Content["readiness_plan"], changed.DNSArtifact.Content["readiness_plan"]) {
		t.Fatal("endpoint observation became readiness or altered stable requirements")
	}
}

func TestDNSReadinessRejectsAmbiguityAndBindsAllDependencyPaths(t *testing.T) {
	r := dnsReadinessFixture()
	route := r.Intent.Routes[0]
	route.PathPrefix = "/api"
	r.Intent.Routes = append(r.Intent.Routes, route)
	routes, err := ResolveRouteOrigins(r.Intent.Routes, r.RuntimeSnapshot, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := CompileDNSReadiness(r.Intent, routes, r.RuntimeSnapshot, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Probes) != 8 {
		t.Fatal("missing shared-host dependency")
	}
	for _, target := range plan.Records[0].Targets {
		if len(target.ProbeIDs) != 2 {
			t.Fatal("partial route dependency authorized")
		}
	}
	for name, mutate := range map[string]func(*CompileRequest){
		"future endpoint": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSEdgeEndpoints[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"duplicate owner": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSEdgeEndpoints[1].A = r.RuntimeSnapshot.DNSEdgeEndpoints[0].A
		},
		"private endpoint": func(r *CompileRequest) { r.RuntimeSnapshot.DNSEdgeEndpoints[0].A = []string{"10.0.0.1"} },
		"budget":           func(r *CompileRequest) { r.Policy.DNSReadiness.MaxProbes = 1 },
		"missing policy":   func(r *CompileRequest) { r.Policy.DNSReadiness = nil },
		"unbounded policy": func(r *CompileRequest) { r.Policy.DNSReadiness.MaxConcurrency = 17 },
	} {
		t.Run(name, func(t *testing.T) {
			r := dnsReadinessFixture()
			mutate(&r)
			if _, err := CompileDNSReadiness(r.Intent, []CompiledRoute{{RouteIntent: r.Intent.Routes[0]}}, r.RuntimeSnapshot, r.Policy); err == nil {
				t.Fatal("invalid readiness input accepted")
			}
		})
	}
	plan.Probes[0].RouteDigest = "sha256:" + strings.Repeat("z", 64)
	plan.Probes[0].ID, _ = DNSReadinessProbeID(plan.Probes[0])
	if err := ValidateDNSReadinessPlan(plan, r.Policy.DNSReadiness); err == nil {
		t.Fatal("invalid proof digest accepted")
	}
}

func TestDNSReadinessRequirementsRetainExplicitInactiveState(t *testing.T) {
	r := dnsReadinessFixture()
	r.Intent.Routes[0].Enabled = false
	r.Intent.Routes[0].Status = model.EdgeRouteStatusDisabled
	r.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
	r.Policy.DNSRouteStateConstraints = []DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, InactiveBehavior: "serve_error_page"}}
	routes := []CompiledRoute{{RouteIntent: r.Intent.Routes[0]}}
	plan, err := CompileDNSReadiness(r.Intent, routes, r.RuntimeSnapshot, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Probes) == 0 {
		t.Fatal("inactive error-page proof missing")
	}
	for _, p := range plan.Probes {
		if p.State != model.EdgeRouteStatusDisabled {
			t.Fatal("inactive route treated as active")
		}
	}
	r.Policy.DNSRouteStateConstraints = nil
	plan, err = CompileDNSReadiness(r.Intent, routes, r.RuntimeSnapshot, r.Policy)
	if err != nil || len(plan.Probes) != 0 {
		t.Fatal("implicit inactive DNS authorization", err)
	}
}
