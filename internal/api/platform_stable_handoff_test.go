package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func TestAutomaticStableHandoffPreservesPositiveArtifactUntilIndependentReady(t *testing.T) {
	s, request, parent, _ := retainedTrafficAPIFixture(t)
	now := time.Now().UTC()
	request.RuntimeSnapshot.CapturedAt = &now
	request.RuntimeSnapshot.Releases[0].ID = "release-a-next"
	request.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	request.RuntimeSnapshot.Releases[0].StatusReason = "release service-owned endpoint Pods are not ready"
	request.Policy.TrafficConstraints[0].StableReleaseID = "release-a-next"
	projection := platformIntentProjectionResponse{Intent: request.Intent, Policy: request.Policy, RuntimeSnapshot: request.RuntimeSnapshot, CapturedAt: now}
	business := store.RouteBusinessSnapshot{TrafficPolicies: []model.AppTrafficPolicy{{ID: "traffic-a", AppID: "app-a", TenantID: "tenant-a", Mode: "single", StableReleaseID: "release-a-next", StableWeight: 100, UpdatedByType: model.ActorTypeSystem, UpdatedByID: "stable-release-sync"}}}
	before, _ := json.Marshal(projection)
	err := s.guardPendingStableHandoffs(context.Background(), projection, business)
	if err == nil || !strings.Contains(err.Error(), "automatic stable release handoff pending") || !strings.Contains(err.Error(), "endpoint Pods are not ready") {
		t.Fatal("negative publication not blocked", err)
	}
	after, _ := json.Marshal(projection)
	if string(before) != string(after) {
		t.Fatal("gate rewrote serving authority or observations")
	}
	active, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !found || active.ID != parent.ID || active.ContentHash != parent.ContentHash {
		t.Fatal("current signed artifact changed", err)
	}
	// A later fresh observation authorizes the new target normally. No evidence
	// or LKG lifetime is renewed by the failed capture.
	projection.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusActive
	if err := s.guardPendingStableHandoffs(context.Background(), projection, business); err != nil {
		t.Fatal(err)
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(compiled.RouteArtifact.Content)
	if !strings.Contains(string(raw), "release-a-next") || strings.Contains(string(raw), "stable release is not ready") {
		t.Fatal("ready handoff did not compile", string(raw))
	}
	for _, scenario := range []string{"operator selection", "disable", "route authority change", "peer config recovery", "dns recovery", "policy recovery", "expired evidence", "weights", "candidate", "ready", "no old binding"} {
		t.Run(scenario, func(t *testing.T) {
			var p platformIntentProjectionResponse
			_ = json.Unmarshal(before, &p)
			b := business
			b.TrafficPolicies = append([]model.AppTrafficPolicy(nil), business.TrafficPolicies...)
			switch scenario {
			case "operator selection":
				b.TrafficPolicies[0].UpdatedByType = "user"
			case "disable":
				p.Intent.Routes[0].Enabled = false
			case "route authority change":
				p.Intent.Routes[0].UpstreamURL = "http://different:8080"
			case "peer config recovery":
				p.Intent.Routes[1].Enabled = false
			case "dns recovery":
				p.Intent.DNS = []platformconfig.DNSIntent{{Hostname: "recovery.example.test", Type: "A", Values: []string{"192.0.2.30"}, TTL: 60}}
			case "policy recovery":
				p.Policy.MinimumHealthyEdges = 3
			case "expired evidence":
				p.CapturedAt = now.Add(time.Duration(p.Policy.MaxStaleSeconds+1) * time.Second)
			case "weights":
				p.Policy.TrafficConstraints[0].StableWeight = 80
			case "candidate":
				b.TrafficPolicies[0].CandidateReleaseID = "candidate"
			case "ready":
				p.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusActive
			case "no old binding":
				p.Intent.Scope = "another-scope"
			}
			if err := s.guardPendingStableHandoffs(context.Background(), p, b); err != nil {
				t.Fatal("explicit authority or normal isolation blocked", err)
			}
		})
	}
	projection.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	projection.Intent.Generation, projection.Policy.Generation = "new-intent", "new-policy"
	projection.Intent.CreatedAt, projection.Policy.CreatedAt = now, now
	if err := s.guardPendingStableHandoffs(context.Background(), projection, business); err == nil {
		t.Fatal("generation/time metadata incorrectly bypassed handoff guard")
	}
	// Revoked sources must not become an indefinite publication lock. The
	// compiler's ordinary strict handling remains responsible for admission.
	source, err := s.readRetainedTrafficSource(context.Background(), "global")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "revoked", Pass: false}}); err != nil {
		t.Fatal(err)
	}
	if err := s.guardPendingStableHandoffs(context.Background(), projection, business); err != nil {
		t.Fatal("revoked evidence blocked strict configuration recovery", err)
	}
}
