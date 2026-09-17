package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func placementFixture() CompileRequest {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	intent := applicationDNSFixture()
	intent.Routes[0].RoutePolicy = model.EdgeRoutePolicyEnabled
	intent.DNS[0].Application.TTLPolicy = "min"
	policy := PolicySnapshot{Generation: "placement-policy", MinimumHealthyEdges: 2, MaxStaleSeconds: 120}
	digest, _ := DNSPlacementInputDigest(intent.DNS[0], []CompiledRoute{{RouteIntent: intent.Routes[0]}}, policy)
	return CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &now, DNSPlacements: []DNSPlacementObservation{{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 45, Candidates: []DNSPlacementCandidate{
		{EdgeID: "edge-a", EdgeGroupID: "group-a", ServingGeneration: "serving-a", ObservedAt: now.Add(-10 * time.Second), ValidUntil: now.Add(40 * time.Second), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"93.184.216.34"}, AAAA: []string{"2606:4700:4700::1111"}},
		{EdgeID: "edge-b", EdgeGroupID: "group-b", ServingGeneration: "serving-b", ObservedAt: now.Add(-110 * time.Second), ValidUntil: now.Add(time.Minute), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"93.184.216.35"}, AAAA: []string{"2606:4700:4700::1001"}},
	}}}}}
}
func rebindPlacement(r *CompileRequest) {
	routes, _ := ResolveRouteOrigins(r.Intent.Routes, r.RuntimeSnapshot, r.Policy)
	routes, _ = ApplyRoutePolicyConstraints(routes, r.Policy, r.RuntimeSnapshot.CapturedAt)
	routes, _ = ApplyTrafficPolicyConstraints(routes, r.Policy, r.RuntimeSnapshot)
	r.RuntimeSnapshot.DNSPlacements[0].InputDigest, _ = DNSPlacementInputDigest(r.Intent.DNS[0], routes, r.Policy)
}
func TestDNSPlacementIsDeterministicAndLeasesNeverRenew(t *testing.T) {
	r := placementFixture()
	before, _ := json.Marshal(r)
	result, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	records := flattenedRecords(t, result)
	if len(records) != 2 || records[0].TTL != 10 || len(records[0].Values) != 2 || records[0].Application != nil {
		t.Fatalf("unexpected placement: %+v", records)
	}
	for _, record := range records {
		if len(record.ValueExpirations) != 2 {
			t.Fatal("address lost absolute expiry")
		}
	}
	after, _ := json.Marshal(r)
	if !slices.Equal(before, after) {
		t.Fatal("compiler mutated input")
	}
	r.CreatedAt = time.Now().Add(48 * time.Hour)
	slices.Reverse(r.RuntimeSnapshot.DNSPlacements[0].Candidates)
	replay, err := Compile(r)
	if err != nil || !reflect.DeepEqual(result.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("order/wall clock changed compiled content", err)
	}
	active, err := DNSRecordsAt(records, r.RuntimeSnapshot.CapturedAt.Add(10*time.Second))
	if err != nil || len(active) != 0 {
		t.Fatal("expired placement quorum survived", active, err)
	}
	active, err = DNSRecordsAt(records, r.RuntimeSnapshot.CapturedAt.Add(40*time.Second))
	if err != nil || len(active) != 0 {
		t.Fatal("placement extended by TTL", active, err)
	}
	r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].A = []string{"93.184.216.36"}
	changed, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Lineage.IntentDigest != result.Lineage.IntentDigest || changed.Lineage.PolicyDigest != result.Lineage.PolicyDigest || changed.Lineage.InputSnapshotDigest == result.Lineage.InputSnapshotDigest || reflect.DeepEqual(changed.DNSArtifact.Content, result.DNSArtifact.Content) {
		t.Fatal("facts not separated from configuration")
	}
}
func TestDNSPlacementRejectsUnboundInvalidAndInsufficientEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"missing":        func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements = nil },
		"input changed":  func(r *CompileRequest) { r.Intent.Routes[0].UpstreamURL = "http://new:8080" },
		"policy changed": func(r *CompileRequest) { r.Policy.MaxStaleSeconds = 180 },
		"new path": func(r *CompileRequest) {
			p := r.Intent.Routes[0]
			p.PathPrefix = "/api"
			r.Intent.Routes = append(r.Intent.Routes, p)
		},
		"tenant changed":  func(r *CompileRequest) { r.Intent.Routes[0].TenantID = "other"; r.Intent.DNS[0].TenantID = "other" },
		"missing capture": func(r *CompileRequest) { r.RuntimeSnapshot.CapturedAt = nil },
		"future check": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].CheckedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"future observation": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"zero generation": func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ServingGeneration = "" },
		"duplicate edge":  func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[1].EdgeID = "edge-a" },
		"duplicate IP": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[1].A = []string{"93.184.216.34"}
		},
		"wrong family": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].AAAA = []string{"93.184.216.34"}
		},
		"private IP":              func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].A = []string{"10.0.0.1"} },
		"missing route readiness": func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].RouteReady = false },
		"missing TLS readiness":   func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].TLSReady = false },
		"unhealthy":               func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].Healthy = false },
		"expired": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ValidUntil = *r.RuntimeSnapshot.CapturedAt
		},
		"stale heartbeat": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-121 * time.Second)
		},
		"extra observation": func(r *CompileRequest) {
			f := r.RuntimeSnapshot.DNSPlacements[0]
			f.InputDigest = "sha256:" + strings.Repeat("a", 64)
			r.RuntimeSnapshot.DNSPlacements = append(r.RuntimeSnapshot.DNSPlacements, f)
		},
		"duplicate observation": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements = append(r.RuntimeSnapshot.DNSPlacements, r.RuntimeSnapshot.DNSPlacements[0])
		},
		"no target TTL": func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].TargetTTL = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			r := placementFixture()
			mutate(&r)
			got, err := Compile(r)
			if err == nil || got.DNSArtifact.Content != nil {
				t.Fatal("invalid placement compiled", got.DNSArtifact, err)
			}
		})
	}
}
func TestDNSPlacementHonorsRoutesExclusionsFamiliesAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(*CompileRequest)
		count     int
		wantError bool
	}{
		{"disabled", func(r *CompileRequest) { r.Intent.Routes[0].Enabled = false }, 0, false},
		{"origin unavailable", func(r *CompileRequest) { r.Intent.Routes[0].Status = "unavailable" }, 0, false},
		{"record disabled", func(r *CompileRequest) { r.Intent.DNS[0].Status = "disabled" }, 0, false},
		{"v4", func(r *CompileRequest) { r.Intent.DNS[0].Application.IPv4Policy = "ipv4_only" }, 1, false},
		{"v6", func(r *CompileRequest) { r.Intent.DNS[0].Application.IPv6Policy = "ipv6_only" }, 1, false},
		{"stale permitted", func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSPlacements[0].Status = "stale"
			r.Intent.DNS[0].Application.FallbackPolicy = "stale_if_error"
		}, 2, false},
		{"stale refused", func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements[0].Status = "stale" }, 0, true},
		{"dual minimum", func(r *CompileRequest) {
			r.Intent.DNS[0].Application.IPv4Policy = "dual_stack_required"
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].AAAA = nil
		}, 0, true},
		{"one healthy", func(r *CompileRequest) {
			r.Policy.MinimumHealthyEdges = 1
			r.RuntimeSnapshot.DNSPlacements[0].Candidates[0].Healthy = false
		}, 2, false},
		{"pinned group", func(r *CompileRequest) {
			r.Policy.MinimumHealthyEdges = 1
			r.Intent.Routes[0].EdgeGroupMode = "pinned"
			r.Intent.Routes[0].EdgeGroupID = "group-b"
		}, 2, false},
		{"exclude edge", func(r *CompileRequest) {
			r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "policy", Hostname: r.Intent.Routes[0].Hostname, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, ExcludedEdgeIDs: []string{"edge-a"}}}
		}, 0, true},
		{"per-route threshold", func(r *CompileRequest) {
			r.Policy.RouteConstraints = []RoutePolicyConstraint{{ID: "policy", Hostname: r.Intent.Routes[0].Hostname, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 3}}
		}, 0, true},
		{"multiple paths", func(r *CompileRequest) {
			p := r.Intent.Routes[0]
			p.PathPrefix = "/api"
			r.Intent.Routes = append(r.Intent.Routes, p)
		}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := placementFixture()
			tc.change(&r)
			rebindPlacement(&r)
			got, err := Compile(r)
			if tc.wantError {
				if err == nil {
					t.Fatal("constraint ignored")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			records := flattenedRecords(t, got)
			if len(records) != tc.count {
				t.Fatal("wrong RRsets", records)
			}
		})
	}
}
