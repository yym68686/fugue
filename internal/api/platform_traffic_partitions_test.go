package api

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func retainedTrafficAPIFixture(t *testing.T) (*Server, platformconfig.CompileRequest, model.PlatformArtifact, string) {
	t.Helper()
	path := t.TempDir() + "/state.json"
	st := store.New(path)
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, auth.New(st, ""), nil, ServerConfig{BundleSigningKey: "synthetic-partition-signing", BundleSigningKeyID: "key"})
	s.clusterNodeInventoryCache = newExpiringResponseCache[[]clusterNodeSnapshot](time.Hour)
	s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{node: model.ClusterNode{Name: "edge-a"}, labels: map[string]string{runtimepkg.EdgeRoleLabelKey: runtimepkg.NodeRoleLabelValue}}, {node: model.ClusterNode{Name: "dns-a"}, labels: map[string]string{runtimepkg.DNSRoleLabelKey: runtimepkg.NodeRoleLabelValue}}})
	if _, _, err := st.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	_, consumers, nodes := pinnedDNSFixture()
	if _, err := st.UpdateDNSHeartbeat(nodes[0]); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	r := platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "intent-old", Routes: []platformconfig.RouteIntent{
			{AppID: "app-a", TenantID: "tenant-a", Hostname: "a.example.test", Enabled: true, UpstreamURL: "http://a:8080", ServicePort: 8080},
			{AppID: "app-b", TenantID: "tenant-a", Hostname: "b.example.test", Enabled: true, UpstreamURL: "http://b:8080", ServicePort: 8080},
		}},
		Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: "policy-old", MaxStaleSeconds: 3600, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{"edge-group-a"}}}, TrafficConstraints: []platformconfig.TrafficPolicyConstraint{
			{ID: "traffic-a", AppID: "app-a", TenantID: "tenant-a", Mode: "single", StableReleaseID: "release-a", StableWeight: 100},
			{ID: "traffic-b", AppID: "app-b", TenantID: "tenant-a", Mode: "single", StableReleaseID: "release-b", StableWeight: 100},
		}},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{IsolateTrafficAvailability: true, CapturedAt: &now, Releases: []platformconfig.ReleaseObservation{
			{ID: "release-a", AppID: "app-a", TenantID: "tenant-a", ObservedAt: now, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://a:8080"},
			{ID: "release-b", AppID: "app-b", TenantID: "tenant-a", ObservedAt: now, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://b:8080"},
		}},
	}
	r.Intent.DNSConsumers = consumers
	r.RuntimeSnapshot.DNSConsumers = []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", ObservedAt: now, A: []string{"8.8.8.8"}}}
	compiled, err := platformconfig.Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	response, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal(err)
	}
	parent := response.ReleaseArtifact
	for _, channel := range []string{"shadow", "gray", "full"} {
		request := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel}
		if channel == "gray" {
			request.CanaryRuleRef = "cohort=complete"
		}
		_, release, _, _, err := st.ReleasePlatformArtifact(parent.ID, request, platformProducerPrincipal())
		if err != nil {
			t.Fatal(channel, err)
		}
		reportServingProducerAPI(t, s, parent, release)
		if channel != "shadow" {
			_, _, _, _, err = st.VerifyPlatformArtifactReleaseLKG(release.ID, model.PlatformArtifactVerifyLKGRequest{FencingToken: release.FencingToken, AllowInitialLKG: channel == "gray", Reason: "explicit synthetic bootstrap", Evidence: model.PlatformArtifactVerificationEvidence{ConsumerConvergence: true, LocalProbe: true, PlatformEvidence: true, WatchWindow: true, BaselineMonotonic: true, DatabaseRollbackCompatible: true, EvidenceRefs: []string{"synthetic-bootstrap"}}}, platformProducerPrincipal())
			if err != nil {
				t.Fatal("verify", channel, err)
			}
		}
	}
	return s, r, parent, path
}

func TestRetainedTrafficHostnameProjectionPreservesMultiHostAndSharedPaths(t *testing.T) {
	now := time.Now().UTC()
	r := platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "intent", Routes: []platformconfig.RouteIntent{
			{TenantID: "tenant-a", AppID: "app-a", Hostname: "shared.example.test", PathPrefix: "/a", Enabled: true, UpstreamURL: "http://a:8080"},
			{TenantID: "tenant-a", AppID: "app-a", Hostname: "alias.example.test", Enabled: true, UpstreamURL: "http://a:8080"},
			{TenantID: "tenant-a", AppID: "app-b", Hostname: "shared.example.test", PathPrefix: "/b", Enabled: true, UpstreamURL: "http://b:8080"},
		}},
		Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: "policy", MaxStaleSeconds: 60, TrafficConstraints: []platformconfig.TrafficPolicyConstraint{
			{ID: "traffic-a", TenantID: "tenant-a", AppID: "app-a", Mode: "single", StableReleaseID: "release-a", StableWeight: 100},
			{ID: "traffic-b", TenantID: "tenant-a", AppID: "app-b", Mode: "single", StableReleaseID: "release-b", StableWeight: 100},
		}},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{IsolateTrafficAvailability: true, CapturedAt: &now, Releases: []platformconfig.ReleaseObservation{
			{ID: "release-a", TenantID: "tenant-a", AppID: "app-a", Status: model.EdgeRouteStatusActive, ObservedAt: now, UpstreamURL: "http://a:8080"},
			{ID: "release-b", TenantID: "tenant-a", AppID: "app-b", Status: model.EdgeRouteStatusActive, ObservedAt: now, UpstreamURL: "http://b:8080"},
		}},
	}
	old, err := platformconfig.Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(old.RouteArtifact.Content["routes"])
	if err := json.Unmarshal(raw, &routes); err != nil {
		t.Fatal(err)
	}
	previous := []platformconfig.CompiledRoute{}
	for _, route := range routes {
		if route.AppID == "app-a" {
			previous = append(previous, route)
		}
	}
	digest, err := platformconfig.TrafficPartitionInputDigest(r.Intent, r.Policy, "tenant-a", "app-a")
	if err != nil {
		t.Fatal(err)
	}
	r.RuntimeSnapshot.RetainedTrafficPartitions = []platformconfig.RetainedTrafficPartition{{TenantID: "tenant-a", AppID: "app-a", InputDigest: digest, Source: model.PlatformPublicationPrecondition{ArtifactID: "old-parent", ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "old-full", FencingToken: 1}, RouteArtifactID: "old-routes", RouteArtifactDigest: "sha256:" + strings.Repeat("b", 64), ObservedAt: now, ExpiresAt: now.Add(time.Minute), Routes: platformconfig.NormalizeTrafficPartitionRoutes(previous)}}
	r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	r.RuntimeSnapshot.Releases[1].UpstreamURL = "http://b-new:8080"
	p := platformIntentProjectionResponse{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot, CapturedAt: now}
	before, _ := json.Marshal(p)
	for host, count := range map[string]int{"shared.example.test": 2, "alias.example.test": 1} {
		got, _, err := placementHostnameRoutesFromSnapshot(p, p.Intent, p.Policy, host, func([]platformconfig.CompiledRoute) (model.EdgeRouteIntentSnapshot, error) {
			return model.EdgeRouteIntentSnapshot{}, nil
		})
		if err != nil || len(got) != count {
			t.Fatal(host, err, len(got))
		}
		for _, route := range got {
			if route.Hostname != host || len(route.Upstreams) != 1 {
				t.Fatal("hostname partition lost binding", route)
			}
			if route.AppID == "app-b" && route.Upstreams[0].UpstreamURL != "http://b-new:8080" {
				t.Fatal("healthy shared-host peer blocked")
			}
		}
	}
	after, _ := json.Marshal(p)
	if string(before) != string(after) {
		t.Fatal("hostname projection mutated complete partition")
	}
}

func TestRetainedTrafficCaptureRequiresSignedVerifiedSourceAndExactContent(t *testing.T) {
	s, r, _, path := retainedTrafficAPIFixture(t)
	ctx := context.Background()
	source, err := s.readRetainedTrafficSource(ctx, "global")
	if err != nil {
		t.Fatal(err)
	}
	r.Intent.Generation = "intent-new"
	r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	r.RuntimeSnapshot.Releases[0].StatusReason = "endpoint identity differs: field=pod_release_key"
	r.RuntimeSnapshot.Releases[1].UpstreamURL = "http://b-new:8080"
	projection := platformIntentProjectionResponse{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot, CapturedAt: *r.RuntimeSnapshot.CapturedAt}
	s.captureRetainedTrafficPartitions(ctx, &projection, 5*time.Minute)
	if len(projection.RuntimeSnapshot.RetainedTrafficPartitions) != 1 || projection.RuntimeSnapshot.Releases[0].Status != model.EdgeRouteStatusUnavailable {
		t.Fatal("capture lost exact partition or invented ready workload", projection.Issues)
	}
	part := projection.RuntimeSnapshot.RetainedTrafficPartitions[0]
	if !reflect.DeepEqual(part.Routes, source.routes["tenant-a\x00app-a"]) || part.ExpiresAt != source.expiry("tenant-a", "app-a") {
		t.Fatal("source or original expiry changed")
	}
	r.RuntimeSnapshot = projection.RuntimeSnapshot
	compiled, err := platformconfig.Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err != nil {
		t.Fatal(err)
	}
	if _, err = s.materializePlatformCompilation(ctx, compiled, platformProducerPrincipal(), nil); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"forged route", "extended expiry", "new source fence", "revoked source", "missing consumer receipts"} {
		t.Run(scenario, func(t *testing.T) {
			// Deep copy so adversarial mutations never alter another scenario.
			raw, _ := json.Marshal(compiled)
			var bad platformconfig.CompileResult
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "forged route":
				bad.InputSnapshot.RetainedTrafficPartitions[0].Routes[0].UpstreamURL = "http://forged:8080"
			case "extended expiry":
				bad.InputSnapshot.RetainedTrafficPartitions[0].ExpiresAt = source.expiry("tenant-a", "app-a").Add(time.Second)
			case "new source fence":
				bad.InputSnapshot.RetainedTrafficPartitions[0].Source.FencingToken++
			case "revoked source":
				if _, err := s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "revoked", Pass: false}}); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "restore", Pass: true}})
				}()
			case "missing consumer receipts":
				// Corrupt only this disposable file store, not membership policy.
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var state model.State
				if err := json.Unmarshal(data, &state); err != nil {
					t.Fatal(err)
				}
				state.PlatformConsumerInstances = nil
				data, err = json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.validateRetainedTrafficCompilation(ctx, bad); err == nil {
				t.Fatal("untrusted retention materialized")
			}
		})
	}
}

func TestRetainedTrafficExpiryCannotBeRenewedOrOutliveRolloutBudget(t *testing.T) {
	s, r, _, _ := retainedTrafficAPIFixture(t)
	source, err := s.readRetainedTrafficSource(context.Background(), "global")
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC().Add(10 * time.Second)
	source.snapshot.RetainedTrafficPartitions = []platformconfig.RetainedTrafficPartition{{TenantID: "tenant-a", AppID: "app-a", ExpiresAt: cutoff}}
	if !source.expiry("tenant-a", "app-a").Equal(cutoff) {
		t.Fatal("chained capture renewed prior evidence")
	}
	r.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
	p := platformIntentProjectionResponse{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot, CapturedAt: *r.RuntimeSnapshot.CapturedAt}
	s.captureRetainedTrafficPartitions(context.Background(), &p, 2*time.Hour)
	if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 0 {
		t.Fatal("evidence expires before safe observation windows")
	}
	result, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
	if err != nil {
		t.Fatal("failed partition blocked peer", err)
	}
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(result.RouteArtifact.Content["routes"])
	_ = json.Unmarshal(raw, &routes)
	for _, route := range routes {
		if route.AppID == "app-a" && len(route.Upstreams) > 0 {
			t.Fatal("unproven binding served")
		}
		if route.AppID == "app-b" && len(route.Upstreams) != 1 {
			t.Fatal("healthy peer lost binding")
		}
	}
}
