package api

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCompletedManagedHandoffDuringDNSChange(t *testing.T) {
	for _, test := range []struct {
		name string
		edit string
		keep bool
	}{
		{"pending endpoints after exact automatic sync", "", true},
		{"pending generation after exact automatic sync", "generation", true},
		{"unrelated historical automatic sync", "older_sync", false},
		{"not a deploy", "scale", false},
		{"operator selection", "manual", false},
		{"origin identity failure", "ownership", false},
		{"changed app authority", "cache", false},
		{"insufficient original expiry", "lifetime", false},
		{"new release already ready", "ready", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, p, b, path := handoffPartitionFixture(t)
			ctx := t.Context()
			p.RuntimeSnapshot.Releases[0].ID = "release-next"
			p.RuntimeSnapshot.Releases[0].StatusReason = platformconfig.ManagedHandoffEndpointsReason
			p.RuntimeSnapshot.Origins[0].Status, p.RuntimeSnapshot.Origins[0].StatusReason = model.EdgeRouteStatusActive, ""
			p.Policy.TrafficConstraints[0].StableReleaseID = "release-next"
			op, policy := &b.Operations[0], &b.TrafficPolicies[0]
			completed := p.CapturedAt.Add(-time.Second)
			op.Status, op.CompletedAt = model.OperationStatusCompleted, &completed
			policy.StableReleaseID, policy.UpdatedAt = "release-next", completed
			policy.UpdatedByType, policy.UpdatedByID = model.ActorTypeSystem, "stable-release-sync"
			budget := time.Duration(0)
			switch test.edit {
			case "generation":
				p.RuntimeSnapshot.Releases[0].StatusReason = platformconfig.ManagedHandoffReleaseReason
			case "older_sync":
				policy.UpdatedAt = completed.Add(-time.Second)
			case "scale":
				op.Type = model.OperationTypeScale
			case "manual":
				policy.UpdatedByType = model.ActorTypeAPIKey
			case "ownership":
				p.RuntimeSnapshot.Releases[0].StatusReason = "release deployment ownership differs"
			case "cache":
				p.Intent.Routes[0].CacheNamespace = "operator-defined"
			case "lifetime":
				budget = 24 * time.Hour
			case "ready":
				p.RuntimeSnapshot.Releases[0].Status, p.RuntimeSnapshot.Releases[0].StatusReason = model.EdgeRouteStatusActive, ""
			}
			persistHandoffBusiness(t, path, b)

			if err := s.guardPendingStableHandoffs(ctx, p, b); err != nil {
				t.Fatal("whole-scope guard must allow DNS/peer changes", err)
			}
			if err := s.captureManagedHandoffPartitions(ctx, &p, b, budget); err != nil {
				t.Fatal(err)
			}
			parts := p.RuntimeSnapshot.RetainedTrafficPartitions
			if (len(parts) == 1) != test.keep {
				t.Fatalf("unexpected retained routes (%d): %v", len(parts), p.Issues)
			}
			if !test.keep {
				return
			}
			source, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
			if err != nil {
				t.Fatal(err)
			}
			part := parts[0]
			if !reflect.DeepEqual(part.Routes, source.routes["tenant-a\x00app-a"]) || !part.ExpiresAt.Equal(source.expiry("tenant-a", "app-a")) {
				t.Fatal("retention changed the signed route or renewed the original expiry")
			}
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
			if err != nil {
				t.Fatal(err)
			}
			var routes []platformconfig.CompiledRoute
			raw, _ := json.Marshal(compiled.RouteArtifact.Content["routes"])
			if err = json.Unmarshal(raw, &routes); err != nil {
				t.Fatal(err)
			}
			for _, r := range routes {
				if r.AppID == "app-a" && (r.Status == model.EdgeRouteStatusUnavailable || len(r.Upstreams) != 1 || r.Upstreams[0].ReleaseID != "release-a" || r.CacheNamespace != "app-a_deploy-old") {
					t.Fatal("verified route was replaced or mislabeled", r)
				}
				if r.AppID == "app-b" && r.Enabled {
					t.Fatal("independent peer disable was lost")
				}
			}
			if err = s.validateRetainedTrafficCompilation(ctx, compiled); err != nil {
				t.Fatal("exact automatic handoff failed admission", err)
			}
			policy.UpdatedAt = policy.UpdatedAt.Add(time.Second)
			persistHandoffBusiness(t, path, b)
			if err = s.validateRetainedTrafficCompilation(ctx, compiled); err == nil {
				t.Fatal("stale handoff claim bypassed fresh policy/operation proof")
			}
		})
	}
}
