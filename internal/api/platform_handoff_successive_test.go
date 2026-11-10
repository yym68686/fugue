package api

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestManagedHandoffAfterAutomaticStableSyncRetainsVerifiedPath(t *testing.T) {
	s, p, b, path := handoffPartitionFixture(t)
	// A second deploy starts before the first completed deploy's automatic
	// stable selection reaches a verified full traffic publication. The last
	// verified route still names release-a under deploy-old.
	p.RuntimeSnapshot.Releases[0].ID = "release-between"
	p.Policy.TrafficConstraints[0].StableReleaseID = "release-between"
	b.TrafficPolicies[0].StableReleaseID = "release-between"
	b.TrafficPolicies[0].UpdatedByType = model.ActorTypeSystem
	b.TrafficPolicies[0].UpdatedByID = "stable-release-sync"
	persistHandoffBusiness(t, path, b)
	ctx := context.Background()
	source, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(p)
	if err = s.captureManagedHandoffPartitions(ctx, &p, b); err != nil {
		t.Fatal(err)
	}
	if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 1 {
		t.Fatal("successive automatic deploy withdrew the verified path", p.Issues)
	}
	part := p.RuntimeSnapshot.RetainedTrafficPartitions[0]
	if !reflect.DeepEqual(part.Routes, source.routes["tenant-a\x00app-a"]) || !part.ExpiresAt.Equal(source.expiry("tenant-a", "app-a")) {
		t.Fatal("successive handoff changed route or renewed evidence")
	}
	if p.RuntimeSnapshot.Releases[0].Status != model.EdgeRouteStatusUnavailable || p.RuntimeSnapshot.Origins[0].Status != model.EdgeRouteStatusUnavailable {
		t.Fatal("handoff invented readiness")
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
		if r.AppID == "app-a" && (r.Status == model.EdgeRouteStatusUnavailable || len(r.Upstreams) != 1 || r.Upstreams[0].ReleaseID != "release-a" || r.DeploymentGeneration != "deploy-old") {
			t.Fatal("exact earlier verified path lost", r)
		}
		if r.AppID == "app-b" && r.Enabled {
			t.Fatal("peer configuration recovery blocked")
		}
	}
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err != nil {
		t.Fatal("successive handoff refused independent admission", err)
	}
	stored, err := s.materializePlatformCompilation(ctx, compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal("successive handoff failed materialization", err)
	}
	// A complete proof read failure may defer capture, but cannot authorize
	// copying even this correctly identified positive source.
	var retry platformIntentProjectionResponse
	_ = json.Unmarshal(before, &retry)
	err = s.captureManagedHandoffPartitionsWithSource(ctx, &retry, b, func() (retainedTrafficSource, error) { return source, context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) || len(retry.RuntimeSnapshot.RetainedTrafficPartitions) != 0 {
		t.Fatal("successive handoff swallowed proof failure or reused partial proof", err)
	}
	// Automatic selection is a durable admission condition, not trust in the
	// capture's claimed handoff. A later operator edit revokes this exception.
	b.TrafficPolicies[0].UpdatedByType = "user"
	persistHandoffBusiness(t, path, b)
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err == nil {
		t.Fatal("operator edit after capture retained automatic selection authority")
	}
	b.TrafficPolicies[0].UpdatedByType = model.ActorTypeSystem
	persistHandoffBusiness(t, path, b)
	// Retention itself can become the next verified full publication. Its
	// policy names the intermediate stable while its exact route still names
	// the earlier release; another capture must preserve that provenance.
	for _, channel := range []string{"shadow", "gray", "full"} {
		req := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel}
		if channel == "gray" {
			req.CanaryRuleRef = "cohort=complete"
		}
		_, release, _, _, err := s.store.ReleasePlatformArtifact(stored.ReleaseArtifact.ID, req, platformProducerPrincipal())
		if err != nil {
			t.Fatal(err)
		}
		reportServingProducerAPI(t, s, stored.ReleaseArtifact, release)
		if channel == "full" {
			_, _, _, _, err = s.store.VerifyPlatformArtifactReleaseLKG(release.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: release.FencingToken, Reason: "synthetic successive handoff", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic"}}}, platformProducerPrincipal())
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var repeated platformIntentProjectionResponse
	_ = json.Unmarshal(before, &repeated)
	if err = s.captureManagedHandoffPartitions(ctx, &repeated, b); err != nil {
		t.Fatal(err)
	}
	if len(repeated.RuntimeSnapshot.RetainedTrafficPartitions) != 1 || !reflect.DeepEqual(repeated.RuntimeSnapshot.RetainedTrafficPartitions[0].Routes, part.Routes) || !repeated.RuntimeSnapshot.RetainedTrafficPartitions[0].ExpiresAt.Equal(part.ExpiresAt) {
		t.Fatal("repeated successive handoff lost exact route or renewed expiry")
	}
	compiled, err = platformconfig.Compile(platformconfig.CompileRequest{Intent: repeated.Intent, Policy: repeated.Policy, RuntimeSnapshot: repeated.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err != nil {
		t.Fatal("repeated handoff refused admission", err)
	}
}

func TestSuccessiveManagedHandoffRequiresAutomaticSelectionAndExactAuthority(t *testing.T) {
	for _, scenario := range []string{"operator", "other system actor", "policy differs", "disabled", "upstream", "cache", "runtime", "tls", "weights", "candidate", "finished", "failed", "unrelated origin"} {
		t.Run(scenario, func(t *testing.T) {
			s, p, b, _ := handoffPartitionFixture(t)
			p.RuntimeSnapshot.Releases[0].ID = "release-between"
			p.Policy.TrafficConstraints[0].StableReleaseID = "release-between"
			b.TrafficPolicies[0].StableReleaseID = "release-between"
			b.TrafficPolicies[0].UpdatedByType = model.ActorTypeSystem
			b.TrafficPolicies[0].UpdatedByID = "stable-release-sync"
			switch scenario {
			case "operator":
				b.TrafficPolicies[0].UpdatedByType = "user"
			case "other system actor":
				b.TrafficPolicies[0].UpdatedByID = "other"
			case "policy differs":
				p.Policy.TrafficConstraints[0].StableReleaseID = "selected"
			case "disabled":
				p.Intent.Routes[0].Enabled = false
			case "upstream":
				p.Intent.Routes[0].UpstreamURL = "http://changed:8080"
			case "cache":
				p.Intent.CachePolicies[0].TTLSeconds++
			case "runtime":
				p.Intent.Routes[0].RuntimeID = "other-runtime"
			case "tls":
				p.Intent.Routes[0].TLSPolicy = "other"
			case "weights":
				p.Policy.TrafficConstraints[0].StableWeight = 90
			case "candidate":
				b.TrafficPolicies[0].CandidateReleaseID = "candidate"
			case "finished":
				b.Operations[0].Status = model.OperationStatusCompleted
			case "failed":
				b.Operations[0].Status = model.OperationStatusFailed
			case "unrelated origin":
				p.RuntimeSnapshot.Origins[0].StatusReason = "service identity differs"
			}
			if err := s.captureManagedHandoffPartitions(context.Background(), &p, b); err != nil {
				t.Fatal(err)
			}
			// Other negative origins may carry a partition but must still close
			// at compilation; a handoff never overrides unrelated runtime failure.
			if scenario == "unrelated origin" {
				compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
				if err != nil {
					t.Fatal(err)
				}
				var routes []platformconfig.CompiledRoute
				raw, _ := json.Marshal(compiled.RouteArtifact.Content["routes"])
				_ = json.Unmarshal(raw, &routes)
				for _, r := range routes {
					if r.AppID == "app-a" && (r.Status != model.EdgeRouteStatusUnavailable || len(r.Upstreams) > 0) {
						t.Fatal("unrelated failure bypassed")
					}
				}
			} else if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 0 {
				t.Fatal("explicit authority or invalid deployment retained")
			}
		})
	}
}
