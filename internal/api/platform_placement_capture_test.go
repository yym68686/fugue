package api

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

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
