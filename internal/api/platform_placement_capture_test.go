package api

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseflow"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

func TestPlacementReleaseProofMatchesLegacyEdgeControlOutput(t *testing.T) {
	for _, minimum := range []int{1, 3} {
		t.Run(fmt.Sprintf("minimum=%d", minimum), func(t *testing.T) {
			now := time.Now().UTC()
			const host, group = "weighted.example.test", "edge-group-test-a"
			binding := model.EdgeRouteBinding{Hostname: host, PathPrefix: "/", RouteKind: "platform", AppID: "app-a", TenantID: "tenant-a", RuntimeID: "runtime-a", UpstreamURL: "http://origin:8080", UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, ServicePort: 8080, TLSPolicy: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled, Streaming: true, Status: model.EdgeRouteStatusActive}
			traffic := model.AppTrafficPolicy{ID: "traffic-a", AppID: binding.AppID, TenantID: binding.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: "release-a", StableWeight: 100}
			release := model.AppRelease{ID: "release-a", AppID: binding.AppID, TenantID: binding.TenantID, RuntimeID: binding.RuntimeID, Status: model.AppReleaseStatusServing, UpstreamURL: binding.UpstreamURL, ResolvedImageRef: "registry.example/app:stable"}
			routePolicy := model.EdgeRoutePolicy{ID: "route-policy-a", Hostname: host, AppID: binding.AppID, TenantID: binding.TenantID, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: minimum, ExcludedEdgeIDs: []string{"other-edge"}}
			legacy := releaseflow.ApplyAppReleaseTraffic(binding, map[string]model.AppTrafficPolicy{binding.AppID: traffic}, map[string]model.AppRelease{release.ID: release})
			source := model.EdgeRouteIntentSnapshot{SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, GeneratedAt: now, Routes: []model.EdgeRouteIntent{edgeRouteIntentFromBinding(legacy, routePolicy, now)}}
			source.Generation = edgeRouteIntentSnapshotGeneration(source)
			ledger := edgecontrol.NewMemoryGroupShadowLedger()
			compiler := edgecontrol.GroupShadowCompiler{Inventory: projectionInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
			batch, err := compiler.Reconcile(context.Background(), source, []string{group})
			if err != nil || batch.Succeeded != 1 {
				t.Fatalf("legacy Edge Control compile: %+v %v", batch, err)
			}
			head, found, err := ledger.Head(context.Background(), group)
			if err != nil || !found || head.Bundle == nil || len(head.Bundle.Routes) != 1 {
				t.Fatalf("legacy bundle unavailable: %+v %v", head, err)
			}
			actualDigest, err := routeproof.Digest(head.Bundle.Routes[0])
			if err != nil {
				t.Fatal(err)
			}
			policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{MinimumHealthyEdges: 1, MaxStaleSeconds: 60}, []model.EdgeRoutePolicy{routePolicy}, []model.AppTrafficPolicy{traffic}, "policy-a")
			if err != nil {
				t.Fatal(err)
			}
			streaming := true
			r := platformIntentProjectionResponse{SourceGeneration: "draft-a", CapturedAt: now, Policy: policy, Intent: platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{Generation: "intent-a", Routes: []platformconfig.RouteIntent{{Hostname: host, PathPrefix: "/", Kind: binding.RouteKind, AppID: binding.AppID, TenantID: binding.TenantID, RuntimeID: binding.RuntimeID, UpstreamURL: binding.UpstreamURL, UpstreamKind: binding.UpstreamKind, UpstreamScope: binding.UpstreamScope, ServicePort: binding.ServicePort, TLSPolicy: binding.TLSPolicy, RoutePolicy: binding.RoutePolicy, Enabled: true, Streaming: &streaming}}, DNS: []platformconfig.DNSIntent{{Hostname: host, Type: "FUGUE_APP", AppID: binding.AppID, TenantID: binding.TenantID, Values: []string{binding.AppID}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}})}
			r.RuntimeSnapshot = platformconfig.RuntimeSnapshot{CapturedAt: &r.CapturedAt, Releases: []platformconfig.ReleaseObservation{{ID: release.ID, AppID: release.AppID, TenantID: release.TenantID, RuntimeID: release.RuntimeID, UpstreamURL: release.UpstreamURL, DeploymentGeneration: release.ResolvedImageRef, Status: model.EdgeRouteStatusActive, ObservedAt: now}}}
			_, projected, err := placementHostnameRoutes(r, host)
			if err != nil {
				t.Fatal(err)
			}
			digest, _ := routeproof.Digest(routebinding.FromIntent(projected[0], group))
			if digest != actualDigest {
				t.Fatalf("compiled release projection differs from actual legacy Edge Control output: projected=%+v actual=%+v", projected[0], head.Bundle.Routes[0])
			}
			beforeIntent, beforePolicy := r.Intent, r.Policy
			nodes := []model.EdgeNode{{ID: "edge-test", EdgeGroupID: group, Status: model.EdgeHealthHealthy, Healthy: true, PublicIPv4: "93.184.216.34", RouteBundleVersion: "loaded-bundle", LastHeartbeatAt: &now}}
			captureDNSPlacementFacts(context.Background(), &r, nodes, func(context.Context, string, string, string) (placementRouteProof, error) {
				return placementRouteProof{Digest: actualDigest, EdgeID: "edge-test", GroupID: group, Version: "loaded-bundle", ValidUntil: now.Add(30 * time.Second)}, nil
			})
			if len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 1 {
				t.Fatalf("exact legacy proof not captured: %+v", r.Issues)
			}
			if !reflect.DeepEqual(beforeIntent, r.Intent) || !reflect.DeepEqual(beforePolicy, r.Policy) {
				t.Fatal("projection changed configuration")
			}
			r.RuntimeSnapshot.Releases[0].ObservedAt = now.Add(-time.Hour)
			if _, _, err := placementHostnameRoutes(r, host); err == nil {
				t.Fatal("stale release evidence accepted")
			}
		})
	}
}

func TestSharedHostnamePolicyProofMatchesLegacyEdgeControl(t *testing.T) {
	now := time.Now().UTC()
	expired := now.Add(-time.Hour)
	const host, group = "shared.example.test", "edge-group-test-a"
	legacyPolicy := model.EdgeRoutePolicy{ID: "hostname-policy", Hostname: host, AppID: "app-b", TenantID: "tenant-a", EdgeGroupID: group, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 1, ExcludedEdgeIDs: []string{"other-edge"}, ExclusionExpiresAt: &expired}
	policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{}, []model.EdgeRoutePolicy{legacyPolicy}, nil, "projected-policy")
	if err != nil {
		t.Fatal(err)
	}
	source := model.EdgeRouteIntentSnapshot{SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, GeneratedAt: now}
	draft := platformIntentProjectionResponse{SourceGeneration: "draft", CapturedAt: now, Policy: policy}
	for i, path := range []string{"/", "/v1"} {
		app := []string{"app-a", "app-b"}[i]
		binding := model.EdgeRouteBinding{Hostname: host, PathPrefix: path, RouteKind: "platform", AppID: app, TenantID: "tenant-a", UpstreamURL: "http://origin:8080", UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, ServicePort: 8080, TLSPolicy: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled, Streaming: true, Status: model.EdgeRouteStatusActive}
		source.Routes = append(source.Routes, edgeRouteIntentFromBinding(binding, legacyPolicy, now))
		streaming := true
		draft.Intent.Routes = append(draft.Intent.Routes, platformconfig.RouteIntent{Hostname: host, PathPrefix: path, Kind: binding.RouteKind, AppID: app, TenantID: binding.TenantID, UpstreamURL: binding.UpstreamURL, UpstreamKind: binding.UpstreamKind, UpstreamScope: binding.UpstreamScope, ServicePort: binding.ServicePort, TLSPolicy: binding.TLSPolicy, RoutePolicy: binding.RoutePolicy, Enabled: true, Streaming: &streaming})
	}
	source.Generation = edgeRouteIntentSnapshotGeneration(source)
	ledger := edgecontrol.NewMemoryGroupShadowLedger()
	compiler := edgecontrol.GroupShadowCompiler{Inventory: projectionInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
	batch, err := compiler.Reconcile(context.Background(), source, []string{group})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("legacy compile: %+v %v", batch, err)
	}
	head, found, err := ledger.Head(context.Background(), group)
	if err != nil || !found || head.Bundle == nil || len(head.Bundle.Routes) != 2 {
		t.Fatal("legacy route bundle missing", err)
	}
	_, projected, err := placementHostnameRoutes(draft, host)
	if err != nil || len(projected) != 2 {
		t.Fatal("projected routes missing", err)
	}
	for _, route := range projected {
		want, _ := routeproof.Digest(routebinding.FromIntent(route, group))
		matched := false
		for _, actual := range head.Bundle.Routes {
			if actual.PathPrefix != route.PathPrefix {
				continue
			}
			got, _ := routeproof.Digest(actual)
			if got != want {
				t.Fatalf("policy behavior changed for %s: projected=%+v actual=%+v", route.PathPrefix, route, actual)
			}
			matched = true
		}
		if !matched {
			t.Fatal("missing path", route.PathPrefix)
		}
	}
}

func placementCaptureFixture(t *testing.T) (platformIntentProjectionResponse, []model.EdgeNode, placementRouteProbe) {
	t.Helper()
	now := time.Now().UTC()
	result := platformIntentProjectionResponse{SourceGeneration: "source-a", CapturedAt: now, Policy: platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: "policy-a", MaxStaleSeconds: 60}), Intent: platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{Generation: "intent-a", Routes: []platformconfig.RouteIntent{
		{Hostname: "app.example.test", PathPrefix: "/", Enabled: true, UpstreamURL: "http://origin:8080", AppID: "app-a", TenantID: "tenant-a"},
		{Hostname: "app.example.test", PathPrefix: "/api", Enabled: true, UpstreamURL: "http://api:8080", AppID: "app-a", TenantID: "tenant-a"},
	}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", Type: "FUGUE_APP", AppID: "app-a", TenantID: "tenant-a", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}), Issues: []platformProjectionIssue{{Code: "dns_app_placement_not_projected", Hostname: "app.example.test"}, {Code: "other_migration_pending"}}}
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	heartbeat := now.Add(-20 * time.Second)
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-test-a", Status: model.EdgeHealthHealthy, Healthy: true, PublicIPv4: "93.184.216.34", PublicIPv6: "2606:4700:4700::1111", RouteBundleVersion: "bundle-a", LastHeartbeatAt: &heartbeat}}
	_, projected, err := placementHostnameRoutes(result, "app.example.test")
	if err != nil {
		t.Fatal(err)
	}
	proofs := map[string]string{}
	for _, r := range projected {
		proofs[r.PathPrefix], err = routeproof.Digest(routebinding.FromIntent(r, nodes[0].EdgeGroupID))
		if err != nil {
			t.Fatal(err)
		}
	}
	probe := func(ctx context.Context, host, path, address string) (placementRouteProof, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded collection")
		}
		if host != "app.example.test" || proofs[path] == "" {
			t.Fatal("wrong route target", host, path)
		}
		return placementRouteProof{Digest: proofs[path], Version: "bundle-a", EdgeID: "edge-a", GroupID: "edge-group-test-a", ValidUntil: now.Add(25 * time.Second)}, nil
	}
	return result, nodes, probe
}

func TestPlacementCaptureCompilesOnlyFreshExactProofAndPreservesInputs(t *testing.T) {
	r, nodes, probe := placementCaptureFixture(t)
	intent, policy := r.Intent, r.Policy
	calls := 0
	captureDNSPlacementFacts(context.Background(), &r, nodes, func(ctx context.Context, h, p, a string) (placementRouteProof, error) {
		calls++
		return probe(ctx, h, p, a)
	})
	if calls != 4 || len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 1 {
		t.Fatalf("capture=%+v calls=%d", r, calls)
	}
	fact := r.RuntimeSnapshot.DNSPlacements[0]
	c := fact.Candidates[0]
	if !c.ObservedAt.Equal(*nodes[0].LastHeartbeatAt) || c.ValidUntil.Sub(c.ObservedAt) > 46*time.Second || len(c.A) != 1 || len(c.AAAA) != 1 {
		t.Fatal("leases/addresses changed", c)
	}
	if !reflect.DeepEqual(intent, r.Intent) || !reflect.DeepEqual(policy, r.Policy) {
		t.Fatal("collector mutated configuration")
	}
	if len(r.Issues) != 1 || r.Issues[0].Code != "other_migration_pending" {
		t.Fatal("capture cleared unrelated issue or retained failure", r.Issues)
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.Digest(compiled.DNSArtifact.Content)
	if err != nil || digest == "" {
		t.Fatal("captured facts did not compile")
	}
	again, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot})
	againDigest, _ := platformconfig.Digest(again.DNSArtifact.Content)
	if err != nil || againDigest != digest {
		t.Fatal("capture not replayable", err)
	}
}

func TestPlacementCaptureNeverRenewsBadEvidence(t *testing.T) {
	for name, change := range map[string]func(*model.EdgeNode, *placementRouteProof){
		"digest":     func(n *model.EdgeNode, p *placementRouteProof) { p.Digest = "sha256:wrong" },
		"generation": func(n *model.EdgeNode, p *placementRouteProof) { p.Version = "other" },
		"identity":   func(n *model.EdgeNode, p *placementRouteProof) { p.EdgeID = "other" },
		"group":      func(n *model.EdgeNode, p *placementRouteProof) { p.GroupID = "other" },
		"expiry":     func(n *model.EdgeNode, p *placementRouteProof) { p.ValidUntil = time.Now().Add(-time.Second) },
		"stale heartbeat": func(n *model.EdgeNode, p *placementRouteProof) {
			old := time.Now().Add(-time.Hour)
			n.LastHeartbeatAt = &old
			fresh := time.Now()
			n.LastSeenAt = &fresh
		},
		"future heartbeat": func(n *model.EdgeNode, p *placementRouteProof) {
			future := time.Now().Add(time.Minute)
			n.LastHeartbeatAt = &future
		},
		"missing heartbeat": func(n *model.EdgeNode, p *placementRouteProof) { n.LastHeartbeatAt = nil },
		"unhealthy":         func(n *model.EdgeNode, p *placementRouteProof) { n.Healthy = false },
		"draining":          func(n *model.EdgeNode, p *placementRouteProof) { n.Draining = true },
		"private addresses": func(n *model.EdgeNode, p *placementRouteProof) { n.PublicIPv4 = "10.0.0.1"; n.PublicIPv6 = "::1" },
	} {
		t.Run(name, func(t *testing.T) {
			r, nodes, probe := placementCaptureFixture(t)
			p, err := probeWithDeadline(probe, "/", nodes[0].PublicIPv4)
			if err != nil {
				t.Fatal(err)
			}
			change(&nodes[0], &p)
			captureDNSPlacementFacts(context.Background(), &r, nodes, func(context.Context, string, string, string) (placementRouteProof, error) { return p, nil })
			if len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 0 {
				t.Fatal("invalid evidence captured", r.RuntimeSnapshot.DNSPlacements)
			}
			if len(r.Issues) < 2 {
				t.Fatal("missing diagnostic")
			}
			if _, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot}); err == nil {
				t.Fatal("bad capture compiled")
			}
		})
	}
}

func probeWithDeadline(probe placementRouteProbe, path, ip string) (placementRouteProof, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return probe(ctx, "app.example.test", path, ip)
}

func TestPlacementCaptureChecksEveryPathAndAddress(t *testing.T) {
	r, nodes, probe := placementCaptureFixture(t)
	captureDNSPlacementFacts(context.Background(), &r, nodes, func(ctx context.Context, h, p, a string) (placementRouteProof, error) {
		if p == "/api" && a == nodes[0].PublicIPv6 {
			return placementRouteProof{}, errors.New("TLS failure")
		}
		return probe(ctx, h, p, a)
	})
	c := r.RuntimeSnapshot.DNSPlacements[0].Candidates[0]
	if len(c.A) != 1 || len(c.AAAA) != 0 {
		t.Fatal("one successful path/family authorized another", c)
	}
	r, nodes, probe = placementCaptureFixture(t)
	nodes = append(nodes, nodes[0])
	nodes[1].ID = "edge-b"
	captureDNSPlacementFacts(context.Background(), &r, nodes, probe)
	if len(r.RuntimeSnapshot.DNSPlacements) != 0 {
		t.Fatal("ambiguous address accepted")
	}
	if r.Issues[len(r.Issues)-1].Code != "dns_placement_inventory_ambiguous" {
		t.Fatal(r.Issues)
	}
}

func TestPlacementCaptureDiagnosticsClassifyBoundedProofFailures(t *testing.T) {
	r, nodes, probe := placementCaptureFixture(t)
	failures := map[string]int{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := captureDNSPlacementRecordWithDiagnostics(ctx, r, r.Intent.DNS[0], nodes, func(ctx context.Context, host, path, address string) (placementRouteProof, error) {
		proof, err := probe(ctx, host, path, address)
		if err != nil {
			return proof, err
		}
		proof.Digest = "sha256:wrong"
		return proof, nil
	}, func() *atomic.Int64 { var b atomic.Int64; b.Store(4096); return &b }(), failures)
	if err != nil || len(failures) == 0 {
		t.Fatalf("expected bounded proof diagnostics, err=%v failures=%v", err, failures)
	}
	for key, count := range failures {
		if count < 1 || !strings.Contains(key, ":digest_mismatch") || strings.Contains(key, "http://") {
			t.Fatalf("unexpected diagnostic %q=%d", key, count)
		}
	}
}

func TestPlacementCaptureProbesReferencedHostInsteadOfDNSTarget(t *testing.T) {
	r, nodes, probe := placementCaptureFixture(t)
	options := *r.Intent.DNS[0].Application
	r.Intent.DNS[0].Application = nil
	r.Intent.DNS[0].Route = &platformconfig.DNSRouteIntent{DNSApplicationIntent: options, Hostnames: []string{"app.example.test"}}
	r.Intent.DNS[0].Type, r.Intent.DNS[0].Hostname, r.Intent.DNS[0].Values = "FUGUE_ROUTE", "target.example.test", []string{}
	called := 0
	captureDNSPlacementFacts(context.Background(), &r, nodes, func(ctx context.Context, h, p, a string) (placementRouteProof, error) {
		if h != "app.example.test" {
			t.Fatal("probed DNS alias instead of route hostname", h)
		}
		called++
		return probe(ctx, h, p, a)
	})
	if called != 4 || len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 1 {
		t.Fatal("route alias not captured", r.Issues)
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: r.Intent, Policy: r.Policy, RuntimeSnapshot: r.RuntimeSnapshot})
	if err != nil || compiled.DNSArtifact.Content == nil {
		t.Fatal("captured alias could not compile", err)
	}
}

func TestPlacementCaptureExcludesStaleInventoryAndNeverDropsHostPolicy(t *testing.T) {
	r, nodes, probe := placementCaptureFixture(t)
	stale := nodes[0]
	stale.ID = "stale-edge"
	old := time.Now().Add(-time.Hour)
	stale.LastHeartbeatAt = &old
	nodes = append(nodes, stale)
	captureDNSPlacementFacts(context.Background(), &r, nodes, probe)
	if len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 1 {
		t.Fatal("stale duplicate blocked live inventory", r.Issues)
	}
	r, nodes, probe = placementCaptureFixture(t)
	r.Policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{{ID: "disabled-policy", Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: false}}
	calls := 0
	captureDNSPlacementFacts(context.Background(), &r, nodes, func(ctx context.Context, h, p, a string) (placementRouteProof, error) {
		calls++
		return probe(ctx, h, p, a)
	})
	if calls != 0 || len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 0 {
		t.Fatal("disabled route probed or became ready", r)
	}
	r, nodes, probe = placementCaptureFixture(t)
	r.Intent.Routes[0].OriginRef = "missing-origin"
	r.Intent.Routes[0].RuntimeID = "runtime-a"
	captureDNSPlacementFacts(context.Background(), &r, nodes, probe)
	if len(r.RuntimeSnapshot.DNSPlacements) != 0 {
		t.Fatal("missing origin accepted")
	}
	if r.Issues[len(r.Issues)-1].Code != "dns_placement_route_inputs_invalid" {
		t.Fatal(r.Issues)
	}
}

func TestPlacementCaptureRunsBeyondLegacyProbeLimitWithBoundedConcurrency(t *testing.T) {
	r, nodes, _ := placementCaptureFixture(t)
	template := r.Intent
	r.Intent.Routes, r.Intent.DNS, r.Issues = nil, nil, nil
	for i := 0; i < 200; i++ {
		host := fmt.Sprintf("app-%03d.example.test", i)
		for _, route := range template.Routes {
			route.Hostname = host
			r.Intent.Routes = append(r.Intent.Routes, route)
		}
		record := template.DNS[0]
		record.Hostname = host
		r.Intent.DNS = append(r.Intent.DNS, record)
	}
	_, projected, err := placementHostnameRoutes(r, "app-000.example.test")
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]model.EdgeRouteBinding{}
	for _, route := range projected {
		byPath[route.PathPrefix] = routebinding.FromIntent(route, nodes[0].EdgeGroupID)
	}
	var active, maxActive, calls atomic.Int32
	gate := make(chan struct{})
	started := make(chan struct{}, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		captureDNSPlacementFacts(context.Background(), &r, nodes, func(ctx context.Context, host, path, address string) (placementRouteProof, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
			}
			if calls.Add(1) <= 8 {
				started <- struct{}{}
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return placementRouteProof{}, ctx.Err()
			}
			route := byPath[path]
			route.Hostname = host
			digest, _ := routeproof.Digest(route)
			return placementRouteProof{Digest: digest, Version: nodes[0].RouteBundleVersion, EdgeID: nodes[0].ID, GroupID: nodes[0].EdgeGroupID, ValidUntil: time.Now().Add(time.Minute)}, nil
		})
	}()
	for range 8 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(gate)
			<-done
			t.Fatal("collector did not use eight workers")
		}
	}
	close(gate)
	<-done
	if maxActive.Load() != 8 || active.Load() != 0 || calls.Load() != 800 {
		t.Fatalf("collector bound/workload=%d/%d/%d", maxActive.Load(), active.Load(), calls.Load())
	}
	if len(r.Issues) != 0 || len(r.RuntimeSnapshot.DNSPlacements) != 200 {
		t.Fatal("valid facts incomplete", len(r.RuntimeSnapshot.DNSPlacements), r.Issues)
	}
	for _, fact := range r.RuntimeSnapshot.DNSPlacements {
		if len(fact.Candidates) != 1 {
			t.Fatal("successful proof lost")
		}
	}
}

func TestPlacementCaptureCancellationJoinsWorkers(t *testing.T) {
	r, nodes, _ := placementCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		captureDNSPlacementFacts(ctx, &r, nodes, func(ctx context.Context, host, path, address string) (placementRouteProof, error) {
			close(started)
			<-ctx.Done()
			return placementRouteProof{}, ctx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("probe did not start")
	}
	cancel()
	<-done
	if len(r.RuntimeSnapshot.DNSPlacements) != 1 || len(r.RuntimeSnapshot.DNSPlacements[0].Candidates) != 0 {
		t.Fatal("canceled capture produced positive evidence")
	}
	found := false
	for _, issue := range r.Issues {
		found = found || issue.Code == "dns_placement_capture_limit"
	}
	if !found {
		t.Fatal("missing cancellation diagnostic")
	}
}
