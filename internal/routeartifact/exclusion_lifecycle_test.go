package routeartifact

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCompiledExclusionLifecycleUsesFixedTimeAndNeverClears(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, scenario := range []struct {
		name, want string
		expires    time.Duration
		noExpiry   bool
		legacy     bool
		clear      bool
	}{
		{name: "no exclusions", want: model.EdgeExclusionLifecycleClear, clear: true, noExpiry: true},
		{name: "legacy hold", want: model.EdgeExclusionLifecycleLegacyHold, legacy: true, expires: -time.Hour},
		{name: "active indefinite", want: model.EdgeExclusionLifecycleActive, noExpiry: true},
		{name: "active", want: model.EdgeExclusionLifecycleActive, expires: 48 * time.Hour},
		{name: "expiring day", want: model.EdgeExclusionLifecycleExpiring24H, expires: 24 * time.Hour},
		{name: "expiring hour", want: model.EdgeExclusionLifecycleExpiring1H, expires: time.Hour},
		{name: "exact expiry", want: model.EdgeExclusionLifecycleExpiredHold},
		{name: "expired", want: model.EdgeExclusionLifecycleExpiredHold, expires: -time.Hour},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			expires := now.Add(scenario.expires)
			original := model.EdgeRoutePolicy{ID: "exclusion-policy", Hostname: "route.example.test", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled,
				ExcludedEdgeIDs: []string{"edge-excluded"}, ExcludedEdgeGroupIDs: []string{"edge-group-excluded"}, ExclusionReason: "maintenance",
				ExclusionOwnerDigest: "sha256:" + strings.Repeat("a", 64), ExclusionGeneration: 2, ExclusionFence: "fence-2", ExclusionExpiresAt: &expires}
			if scenario.clear {
				original.ExcludedEdgeIDs, original.ExcludedEdgeGroupIDs = nil, nil
			}
			if scenario.noExpiry {
				original.ExclusionExpiresAt = nil
			}
			if scenario.legacy {
				original.ExclusionFence = ""
			}
			policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{}, []model.EdgeRoutePolicy{original}, nil, "policy-v1")
			if err != nil {
				t.Fatal(err)
			}
			rule := policy.RouteConstraints[0]
			if rule.ExclusionOwnerDigest != original.ExclusionOwnerDigest || rule.ExclusionGeneration != original.ExclusionGeneration || rule.ExclusionFence != original.ExclusionFence {
				t.Fatal("policy projection lost immutable exclusion authorization metadata")
			}
			request := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent-v1", Routes: []platformconfig.RouteIntent{{Hostname: original.Hostname, Enabled: true, UpstreamURL: "http://origin"}}}, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
			before, _ := json.Marshal(request)
			compiled, err := platformconfig.Compile(request)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := Project(compiled.RouteArtifact)
			if err != nil || len(projection.Routes) != 1 {
				t.Fatal("compiled artifact projection failed", err)
			}
			route := projection.Routes[0]
			if route.ExclusionLifecycle != scenario.want || !reflect.DeepEqual(route.ExcludedEdgeIDs, original.ExcludedEdgeIDs) ||
				!reflect.DeepEqual(route.ExcludedEdgeGroupIDs, original.ExcludedEdgeGroupIDs) || route.ExclusionReason != original.ExclusionReason ||
				!reflect.DeepEqual(route.ExclusionExpiresAt, original.ExclusionExpiresAt) {
				t.Fatalf("exclusion lifecycle changed safety or omitted diagnostics: %+v", route)
			}
			after, _ := json.Marshal(request)
			if string(before) != string(after) {
				t.Fatal("compiler mutated intent or policy")
			}
			request.CreatedAt = now.Add(90 * 24 * time.Hour)
			replay, err := platformconfig.Compile(request)
			if err != nil || !reflect.DeepEqual(compiled.RouteArtifact.Content, replay.RouteArtifact.Content) {
				t.Fatal("wall clock changed exclusion evaluation", err)
			}
			request.RuntimeSnapshot.CapturedAt = nil
			_, err = platformconfig.Compile(request)
			needsTime := !scenario.clear && !scenario.legacy && !scenario.noExpiry
			if (err != nil) != needsTime {
				t.Fatalf("fixed captured_at requirement mismatch: %v", err)
			}
			if !needsTime {
				return
			}
			later := now.Add(96 * time.Hour)
			request.RuntimeSnapshot.CapturedAt = &later
			expired, err := platformconfig.Compile(request)
			if err != nil {
				t.Fatal(err)
			}
			lateProjection, err := Project(expired.RouteArtifact)
			if err != nil || lateProjection.Routes[0].ExclusionLifecycle != model.EdgeExclusionLifecycleExpiredHold ||
				len(lateProjection.Routes[0].ExcludedEdgeIDs) != 1 || len(lateProjection.Routes[0].ExcludedEdgeGroupIDs) != 1 {
				t.Fatal("expired authorization cleared exclusion", err)
			}
			if expired.Lineage.PolicyDigest != compiled.Lineage.PolicyDigest || expired.Lineage.IntentDigest != compiled.Lineage.IntentDigest || expired.Lineage.InputSnapshotDigest == compiled.Lineage.InputSnapshotDigest {
				t.Fatal("runtime evaluation time changed desired policy identity")
			}
		})
	}
}
