package api

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func handoffPartitionFixture(t *testing.T) (*Server, platformIntentProjectionResponse, store.RouteBusinessSnapshot, string) {
	t.Helper()
	s, r, _, path := retainedTrafficAPIFixture(t, func(r *platformconfig.CompileRequest) {
		route := &r.Intent.Routes[0]
		route.DeploymentGeneration, route.CachePolicyID, route.CacheNamespace = "deploy-old", "static", "app-a_deploy-old"
		r.Intent.CachePolicies = defaultEdgeCachePolicies()
		route.CachePolicyID = r.Intent.CachePolicies[0].ID
		route.OriginRef, route.RuntimeID = "origin-a", model.DefaultManagedRuntimeID
		r.RuntimeSnapshot.Origins = []platformconfig.OriginObservation{{Ref: "origin-a", RuntimeID: model.DefaultManagedRuntimeID, RuntimeType: model.RuntimeTypeManagedShared, Status: model.EdgeRouteStatusActive, ObservedAt: *r.RuntimeSnapshot.CapturedAt}}
		r.RuntimeSnapshot.Releases[0].RuntimeID = model.DefaultManagedRuntimeID
	})
	now := time.Now().UTC()
	r.RuntimeSnapshot.CapturedAt = &now
	r.Intent.Generation, r.Policy.Generation = "intent-new", "policy-new"
	r.Intent.Routes[0].DeploymentGeneration, r.Intent.Routes[0].CacheNamespace = "deploy-next", "app-a_deploy-next"
	r.RuntimeSnapshot.Releases[0].Status, r.RuntimeSnapshot.Releases[0].StatusReason = model.EdgeRouteStatusUnavailable, inPlaceDeployReadinessReason
	r.RuntimeSnapshot.Origins[0].Status, r.RuntimeSnapshot.Origins[0].StatusReason = model.EdgeRouteStatusUnavailable, "runtime invariant violation: current_image_mismatch"
	// A code handoff must not block independently authorized peer/DNS recovery.
	r.Intent.Routes[1].Enabled = false
	r.Intent.DNS = []platformconfig.DNSIntent{{Hostname: "recovery.example.test", Type: "A", Values: []string{"192.0.2.30"}, TTL: 60}}
	b := store.RouteBusinessSnapshot{
		Apps:            []model.App{{ID: "app-a", TenantID: "tenant-a", Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID}, Status: model.AppStatus{Phase: "deploying", LastOperationID: "deploy-next"}}},
		TrafficPolicies: []model.AppTrafficPolicy{{ID: "traffic-a", AppID: "app-a", TenantID: "tenant-a", Mode: "single", StableReleaseID: "release-a", StableWeight: 100}},
	}
	persistHandoffBusiness(t, path, b)
	return s, platformIntentProjectionResponse{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot, CapturedAt: now}, b, path
}

func persistHandoffBusiness(t *testing.T, path string, b store.RouteBusinessSnapshot) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state model.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	state.Apps = b.Apps
	state.AppTrafficPolicies = b.TrafficPolicies
	raw, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestManagedHandoffPartitionPreservesPathAcrossOperationCacheAndDNSChurn(t *testing.T) {
	s, p, b, _ := handoffPartitionFixture(t)
	ctx := context.Background()
	before, _ := json.Marshal(p)
	if err := s.guardPendingStableHandoffs(ctx, p, b); err != nil {
		t.Fatal("handoff blocked peer/DNS configuration recovery", err)
	}
	s.captureManagedHandoffPartitions(ctx, &p, b)
	s.captureRetainedTrafficPartitions(ctx, &p)
	if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 1 {
		t.Fatal("operation/cache churn withdrew previously verified serving path", p.Issues)
	}
	part := p.RuntimeSnapshot.RetainedTrafficPartitions[0]
	if p.RuntimeSnapshot.Releases[0].Status != model.EdgeRouteStatusUnavailable || p.RuntimeSnapshot.Origins[0].Status != model.EdgeRouteStatusUnavailable {
		t.Fatal("retention invented a healthy origin/release")
	}
	if err := projectReadyReleaseOrigins(&p, []releaseRuntimeReadiness{{ReleaseID: "release-a", Ready: false}}); err != nil {
		t.Fatal(err)
	}
	if p.RuntimeSnapshot.Origins[0].Status != model.EdgeRouteStatusUnavailable {
		t.Fatal("origin projection invented readiness")
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(compiled.RouteArtifact.Content["routes"])
	if err := json.Unmarshal(raw, &routes); err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if route.AppID == "app-a" && (len(route.Upstreams) != 1 || route.Status == model.EdgeRouteStatusUnavailable || route.DeploymentGeneration != "deploy-old" || route.CacheNamespace != "app-a_deploy-old") {
			t.Fatal("verified path not retained exactly", route)
		}
		if route.AppID == "app-b" && route.Enabled {
			t.Fatal("peer disable lost")
		}
	}
	if !part.ExpiresAt.After(p.CapturedAt) {
		t.Fatal("invalid expiry")
	}
	var original platformIntentProjectionResponse
	_ = json.Unmarshal(before, &original)
	a, _ := json.Marshal(p.Intent)
	z, _ := json.Marshal(original.Intent)
	if string(a) != string(z) {
		t.Fatal("desired configuration was rewritten")
	}
	source, err := s.readRetainedTrafficSource(ctx, "global")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(part.Routes, source.routes["tenant-a\x00app-a"]) || !part.ExpiresAt.Equal(source.expiry("tenant-a", "app-a")) {
		t.Fatal("retention changed source route or renewed old evidence")
	}
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err != nil {
		t.Fatal("valid handoff refused admission", err)
	}
	stored, err := s.materializePlatformCompilation(ctx, compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal("valid handoff materialization failed", err)
	}
	// Ordinary shadow/gray/full convergence can advance DNS/peer inputs while
	// keeping this exact old path. Repeated captures cannot renew its expiry.
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
			_, _, _, _, err = s.store.VerifyPlatformArtifactReleaseLKG(release.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: release.FencingToken, Reason: "synthetic handoff convergence", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic"}}}, platformProducerPrincipal())
			if err != nil {
				t.Fatal("verify", channel, err)
			}
		}
	}
	var next platformIntentProjectionResponse
	_ = json.Unmarshal(before, &next)
	s.captureManagedHandoffPartitions(ctx, &next, b)
	if len(next.RuntimeSnapshot.RetainedTrafficPartitions) != 1 || !next.RuntimeSnapshot.RetainedTrafficPartitions[0].ExpiresAt.Equal(part.ExpiresAt) || !reflect.DeepEqual(next.RuntimeSnapshot.RetainedTrafficPartitions[0].Routes, part.Routes) {
		t.Fatal("renewal extended evidence or lost exact route")
	}
	// A fresh independently ready successor leaves the continuity branch and
	// uses the desired epoch normally, with no carried old binding or facts.
	next.RuntimeSnapshot.RetainedTrafficPartitions = nil
	next.RuntimeSnapshot.Facts = nil
	next.RuntimeSnapshot.Releases[0].ID = "release-next"
	next.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusActive
	next.RuntimeSnapshot.Releases[0].StatusReason = ""
	next.Policy.TrafficConstraints[0].StableReleaseID = "release-next"
	if err = projectReadyReleaseOrigins(&next, []releaseRuntimeReadiness{{ReleaseID: "release-next", Ready: true}}); err != nil {
		t.Fatal(err)
	}
	ready, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: next.Intent, Policy: next.Policy, RuntimeSnapshot: next.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(ready.RouteArtifact.Content["routes"])
	_ = json.Unmarshal(raw, &routes)
	for _, route := range routes {
		if route.AppID == "app-a" && (len(route.Upstreams) != 1 || route.Upstreams[0].ReleaseID != "release-next" || route.DeploymentGeneration != "deploy-next" || route.Status == model.EdgeRouteStatusUnavailable) {
			t.Fatal("ready successor stayed on old binding")
		}
	}
}

func TestManagedHandoffPartitionAuthorityAndOriginBoundaries(t *testing.T) {
	s, base, b, _ := handoffPartitionFixture(t)
	raw, _ := json.Marshal(base)
	for _, scenario := range []string{"disabled", "maintenance", "upstream", "cache policy", "custom cache namespace", "wrong operation epoch", "runtime", "tenant", "tls", "weights", "stable selection", "no operation", "finished", "failed", "unmanaged", "expired", "insufficient budget", "other origin failure", "other release failure", "ready"} {
		t.Run(scenario, func(t *testing.T) {
			var p platformIntentProjectionResponse
			_ = json.Unmarshal(raw, &p)
			business := b
			business.Apps = append([]model.App(nil), b.Apps...)
			business.TrafficPolicies = append([]model.AppTrafficPolicy(nil), b.TrafficPolicies...)
			budget := time.Duration(0)
			switch scenario {
			case "disabled":
				p.Intent.Routes[0].Enabled = false
			case "maintenance":
				p.Intent.Routes[0].Status = model.EdgeRouteStatusDisabled
			case "upstream":
				p.Intent.Routes[0].UpstreamURL = "http://other:8080"
			case "cache policy":
				p.Intent.CachePolicies[0].TTLSeconds++
			case "custom cache namespace":
				p.Intent.Routes[0].CacheNamespace = "explicit-cache"
			case "wrong operation epoch":
				p.Intent.Routes[0].DeploymentGeneration = "another-operation"
			case "runtime":
				p.Intent.Routes[0].RuntimeID = "another-runtime"
			case "tenant":
				business.Apps[0].TenantID = "other-tenant"
			case "tls":
				p.Intent.Routes[0].TLSPolicy = "another-policy"
			case "weights":
				p.Policy.TrafficConstraints[0].StableWeight = 90
			case "stable selection":
				p.Policy.TrafficConstraints[0].StableReleaseID = "selected"
			case "no operation":
				business.Apps[0].Status.LastOperationID = ""
			case "finished":
				business.Apps[0].Status.Phase = "deployed"
			case "failed":
				business.Apps[0].Status.LastFailedOperation = &model.AppOperationFailure{ID: "deploy-next"}
			case "unmanaged":
				business.Apps[0].Spec.RuntimeID = "external"
			case "expired":
				p.CapturedAt = p.CapturedAt.Add(2 * time.Hour)
			case "insufficient budget":
				budget = 2 * time.Hour
			case "other release failure":
				p.RuntimeSnapshot.Releases[0].StatusReason = "release service ownership differs"
			case "ready":
				p.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusActive
			case "other origin failure":
				p.RuntimeSnapshot.Origins[0].StatusReason = "origin independently unavailable"
			}
			s.captureManagedHandoffPartitions(context.Background(), &p, business, budget)
			if scenario == "other origin failure" {
				compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
				if err != nil {
					t.Fatal(err)
				}
				var routes []platformconfig.CompiledRoute
				data, _ := json.Marshal(compiled.RouteArtifact.Content["routes"])
				_ = json.Unmarshal(data, &routes)
				for _, r := range routes {
					if r.AppID == "app-a" && (r.Status != model.EdgeRouteStatusUnavailable || len(r.Upstreams) > 0) {
						t.Fatal("unrelated negative origin bypassed")
					}
				}
			} else if len(p.RuntimeSnapshot.RetainedTrafficPartitions) > 0 {
				t.Fatal("explicit authority or invalid handoff retained", scenario)
			}
		})
	}
}

func TestManagedHandoffAdmissionRechecksDurableOperationAndExactSource(t *testing.T) {
	s, p, b, path := handoffPartitionFixture(t)
	s.captureManagedHandoffPartitions(context.Background(), &p, b)
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validateRetainedTrafficCompilation(context.Background(), compiled); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"finished", "replaced operation", "failed", "explicit selection", "forged route", "extended expiry", "forged input digest", "forged fact", "revoked source"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(compiled)
			var copy platformconfig.CompileResult
			_ = json.Unmarshal(raw, &copy)
			business := b
			business.Apps = append([]model.App(nil), b.Apps...)
			business.TrafficPolicies = append([]model.AppTrafficPolicy(nil), b.TrafficPolicies...)
			switch scenario {
			case "finished":
				business.Apps[0].Status.Phase = "deployed"
			case "replaced operation":
				business.Apps[0].Status.LastOperationID = "new-operation"
			case "failed":
				business.Apps[0].Status.LastFailedOperation = &model.AppOperationFailure{ID: "deploy-next"}
			case "explicit selection":
				business.TrafficPolicies[0].StableReleaseID = "selected"
			case "forged route":
				copy.InputSnapshot.RetainedTrafficPartitions[0].Routes[0].UpstreamURL = "http://forged:8080"
			case "extended expiry":
				copy.InputSnapshot.RetainedTrafficPartitions[0].ExpiresAt = copy.InputSnapshot.RetainedTrafficPartitions[0].ExpiresAt.Add(time.Second)
			case "forged input digest":
				copy.InputSnapshot.RetainedTrafficPartitions[0].InputDigest = "sha256:forged"
			case "forged fact":
				copy.InputSnapshot.Facts[platformconfig.ManagedServingHandoffsFact] = []platformconfig.ManagedServingHandoff{{TenantID: "tenant-a", AppID: "app-a", ReleaseID: "release-a", OperationID: "forged"}}
			case "revoked source":
				source, e := s.readRetainedTrafficSource(context.Background(), "global")
				if e != nil {
					t.Fatal(e)
				}
				_, e = s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "revoked", Pass: false}})
				if e != nil {
					t.Fatal(e)
				}
				defer func() {
					_, _ = s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "restore", Pass: true}})
				}()
			}
			persistHandoffBusiness(t, path, business)
			defer persistHandoffBusiness(t, path, b)
			if e := s.validateRetainedTrafficCompilation(context.Background(), copy); e == nil {
				t.Fatal("untrusted handoff admitted", scenario)
			}
		})
	}
}
