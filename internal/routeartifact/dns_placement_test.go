package routeartifact

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func dnsGroupCompileFixture(t *testing.T) platformconfig.CompileRequest {
	t.Helper()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	r := platformconfig.CompileRequest{
		Intent:          platformconfig.PlatformIntent{Generation: "dns-scope", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", UpstreamURL: "http://origin:8080", Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", Type: "FUGUE_APP", Values: []string{"app"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}},
		Policy:          platformconfig.PolicySnapshot{Generation: "dns-policy", MinimumHealthyEdges: 1, MaxStaleSeconds: 60, RouteConstraints: []platformconfig.RoutePolicyConstraint{{ID: "group", Hostname: "app.example.test", AppID: "app", TenantID: "tenant", EdgeGroupID: "edge-group-a", ExcludedEdgeIDs: []string{"excluded"}, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}}},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now},
	}
	routes, err := platformconfig.ResolveRouteOrigins(r.Intent.Routes, r.RuntimeSnapshot, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	routes, err = platformconfig.ApplyRoutePolicyConstraints(routes, r.Policy, r.RuntimeSnapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.DNSPlacementInputDigest(r.Intent.DNS[0], routes, r.Policy)
	if err != nil {
		t.Fatal(err)
	}
	candidates := []platformconfig.DNSPlacementCandidate{}
	for i, id := range []string{"allowed", "excluded", "other-group"} {
		group := "edge-group-a"
		if i == 2 {
			group = "edge-group-b"
		}
		candidates = append(candidates, platformconfig.DNSPlacementCandidate{EdgeID: id, EdgeGroupID: group, ServingGeneration: "serving", ObservedAt: now, ValidUntil: now.Add(30 * time.Second), Healthy: true, RouteReady: true, TLSReady: true, A: []string{[]string{"93.184.216.34", "93.184.216.35", "93.184.216.36"}[i]}})
	}
	r.RuntimeSnapshot.DNSPlacements = []platformconfig.DNSPlacementObservation{{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60, Candidates: candidates}}
	return r
}

func TestDNSPolicyRestrictsAnswersWithoutRemovingHostRoutes(t *testing.T) {
	r := dnsGroupCompileFixture(t)
	before, _ := json.Marshal(r)
	compiled, err := platformconfig.Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	var records []platformconfig.DNSIntent
	raw, _ := json.Marshal(compiled.DNSArtifact.Content["records"])
	if err := json.Unmarshal(raw, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !reflect.DeepEqual(records[0].Values, []string{"93.184.216.34"}) || records[0].TTL != 30 {
		t.Fatalf("DNS constraint or lease lost: %+v", records)
	}
	for _, group := range []string{"edge-group-a", "edge-group-b"} {
		bundle, err := MaterializeForGroup(compiled.RouteArtifact, group)
		if err != nil || len(bundle.Routes) != 1 || bundle.Routes[0].Status != model.EdgeRouteStatusActive {
			t.Fatalf("DNS restriction removed Host route from %s: %+v %v", group, bundle, err)
		}
	}
	active, err := platformconfig.DNSRecordsAt(records, r.RuntimeSnapshot.CapturedAt.Add(30*time.Second))
	if err != nil || len(active) != 0 {
		t.Fatal("DNS lease survived expiration", err)
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated input")
	}
	r.CreatedAt = time.Now().Add(24 * time.Hour)
	replay, err := platformconfig.Compile(r)
	if err != nil || !reflect.DeepEqual(compiled.RouteArtifact.Content, replay.RouteArtifact.Content) || !reflect.DeepEqual(compiled.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("fixed snapshot replay changed artifact", err)
	}
}

func TestDNSGroupCompilationStillRequiresBoundPositiveEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"missing":     func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.DNSPlacements = nil },
		"wrong input": func(r *platformconfig.CompileRequest) { r.Intent.Routes[0].UpstreamURL = "http://different" },
		"wrong group": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].EdgeGroupID = "edge-group-b"
		},
		"TLS unready": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].TLSReady = false
		},
		"expired": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ValidUntil = *r.RuntimeSnapshot.CapturedAt
		},
		"conflicting pin": func(r *platformconfig.CompileRequest) {
			r.Intent.Routes[0].EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
			r.Intent.Routes[0].EdgeGroupID = "edge-group-b"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := dnsGroupCompileFixture(t)
			mutate(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("unproven DNS answer accepted")
			}
		})
	}
}
