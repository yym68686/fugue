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
		{"Service still selects an owned previous cohort", "previous_cohort", true},
		{"older PostgreSQL writer restamps policy", "older_writer", true},
		{"unrelated historical automatic sync", "older_sync", false},
		{"not a deploy", "scale", false},
		{"operator selection", "manual", false},
		{"release ownership failure", "ownership", false},
		{"independent origin failure", "origin", false},
		{"changed app authority", "cache", false},
		{"new stable binds a different Service", "service", false},
		{"insufficient original expiry", "lifetime", false},
		{"new release already ready", "ready", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, p, b, db := handoffPartitionFixture(t)
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
			promoted := completed
			b.Releases = []model.AppRelease{{ID: policy.StableReleaseID, AppID: policy.AppID, TenantID: policy.TenantID,
				RuntimeID: b.Apps[0].Spec.RuntimeID, Status: model.AppReleaseStatusServing, Role: model.AppReleaseRoleStable, PromotedAt: &promoted,
				UpstreamURL: p.RuntimeSnapshot.Releases[0].UpstreamURL}}
			budget := time.Duration(0)
			switch test.edit {
			case "generation":
				p.RuntimeSnapshot.Releases[0].StatusReason = platformconfig.ManagedHandoffReleaseReason
			case "previous_cohort":
				p.RuntimeSnapshot.Releases[0].StatusReason = platformconfig.ManagedHandoffPreviousCohortPrefix + "release endpoint identity differs: field=pod_release_key"
			case "older_writer":
				policy.UpdatedAt = completed.Add(30 * time.Millisecond)
			case "older_sync":
				promoted = completed.Add(-time.Second)
			case "scale":
				op.Type = model.OperationTypeScale
			case "manual":
				policy.UpdatedByType = model.ActorTypeAPIKey
			case "ownership":
				p.RuntimeSnapshot.Releases[0].StatusReason = "release deployment ownership differs"
			case "origin":
				p.RuntimeSnapshot.Origins[0].Status, p.RuntimeSnapshot.Origins[0].StatusReason = model.EdgeRouteStatusUnavailable, "service absent"
			case "cache":
				p.Intent.Routes[0].CacheNamespace = "operator-defined"
			case "service":
				b.Releases[0].UpstreamURL = "http://other-service:8080"
			case "lifetime":
				budget = 24 * time.Hour
			case "ready":
				p.RuntimeSnapshot.Releases[0].Status, p.RuntimeSnapshot.Releases[0].StatusReason = model.EdgeRouteStatusActive, ""
			}
			persistHandoffBusiness(t, db, b)

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
			promoted = promoted.Add(-time.Second)
			persistHandoffBusiness(t, db, b)
			if err = s.validateRetainedTrafficCompilation(ctx, compiled); err == nil {
				t.Fatal("stale handoff claim bypassed fresh release and operation proof")
			}
		})
	}
}
