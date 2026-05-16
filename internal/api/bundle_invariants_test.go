package api

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestEdgeRouteBundleInvariantRejectsEmptyRoutableBundle(t *testing.T) {
	t.Parallel()

	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_empty",
		Generation:  "routegen_empty",
		GeneratedAt: time.Now().UTC(),
		Routes:      nil,
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups: map[string]bool{"edge-group-country-us": true},
		Options:           edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-us"},
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to publish empty route bundle") {
		t.Fatalf("expected empty route bundle invariant failure, got %v", err)
	}
}

func TestEdgeRouteBundleInvariantRejectsEmptyBundleForStaleEdgeGroup(t *testing.T) {
	t.Parallel()

	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_empty",
		Generation:  "routegen_empty",
		GeneratedAt: time.Now().UTC(),
		Routes:      nil,
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups:          map[string]bool{"edge-group-country-de": false},
		ExpectedNonEmptyEdgeGroups: map[string]bool{"edge-group-country-de": true},
		Options:                    edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-de"},
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to publish empty route bundle") {
		t.Fatalf("expected stale edge group empty route bundle invariant failure, got %v", err)
	}
}

func TestEdgeRouteBundleInvariantRejectsNoTrafficBundleForStaleEdgeGroup(t *testing.T) {
	t.Parallel()

	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_no_traffic",
		Generation:  "routegen_no_traffic",
		GeneratedAt: time.Now().UTC(),
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:        "demo.fugue.pro",
				EdgeGroupID:     "edge-group-country-us",
				RoutePolicy:     model.EdgeRoutePolicyRouteAOnly,
				RouteGeneration: "route_demo",
			},
		},
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups:          map[string]bool{"edge-group-country-us": false},
		ExpectedNonEmptyEdgeGroups: map[string]bool{"edge-group-country-us": true},
		Options:                    edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-us"},
	})
	if err == nil || !strings.Contains(err.Error(), "without traffic routes") {
		t.Fatalf("expected no-traffic route bundle invariant failure, got %v", err)
	}
}

func TestEdgeRouteBundleInvariantRejectsAbnormalTrafficDrop(t *testing.T) {
	t.Parallel()

	routes := make([]model.EdgeRouteBinding, 0, 4)
	for _, host := range []string{"a.fugue.pro", "b.fugue.pro", "c.fugue.pro", "d.fugue.pro"} {
		routes = append(routes, model.EdgeRouteBinding{
			Hostname:        host,
			EdgeGroupID:     "edge-group-country-us",
			RoutePolicy:     model.EdgeRoutePolicyEnabled,
			RouteGeneration: "route_" + strings.TrimSuffix(host, ".fugue.pro"),
		})
	}
	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_short",
		Generation:  "routegen_short",
		GeneratedAt: time.Now().UTC(),
		Routes:      routes,
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups:          map[string]bool{"edge-group-country-us": true},
		ExpectedNonEmptyEdgeGroups: map[string]bool{"edge-group-country-us": true},
		ExpectedMinTrafficRoutes:   map[string]int{"edge-group-country-us": 10},
		Options:                    edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-us"},
	})
	if err == nil || !strings.Contains(err.Error(), "abnormal traffic route drop") {
		t.Fatalf("expected abnormal traffic drop invariant failure, got %v", err)
	}
}

func TestEdgeDNSBundleInvariantRejectsMissingProtectedRecord(t *testing.T) {
	t.Parallel()

	protected := []model.EdgeDNSRecord{
		{
			Name:       "fugue.pro",
			Type:       model.EdgeDNSRecordTypeNS,
			Values:     []string{"ns1.fugue.pro"},
			RecordKind: model.EdgeDNSRecordKindProtected,
			Status:     model.EdgeRouteStatusActive,
		},
	}
	bundle := model.EdgeDNSBundle{
		Version:     "dnsgen_missing_protected",
		Generation:  "dnsgen_missing_protected",
		GeneratedAt: time.Now().UTC(),
		Zone:        "fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "d-test.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10"},
				RecordKind: model.EdgeDNSRecordKindProbe,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}
	err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleOptions{Zone: "fugue.pro"}, protected)
	if err == nil || !strings.Contains(err.Error(), "protected fugue.pro NS value is missing") {
		t.Fatalf("expected protected record invariant failure, got %v", err)
	}
}

func TestDNSInventoryHealthyAllowsHistoricalSyncErrors(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	if !dnsInventoryHealthy([]model.DNSNode{{
		ID:               "dns-us-1",
		Status:           model.EdgeHealthHealthy,
		Healthy:          true,
		CacheStatus:      "ready",
		DNSBundleVersion: "dnsgen_recovered",
		CacheLoadErrors:  1,
		BundleSyncErrors: 3,
		LastHeartbeatAt:  &now,
	}}) {
		t.Fatal("historical sync/cache load errors should not block currently healthy DNS inventory after LKG recovery")
	}
	if dnsInventoryHealthy([]model.DNSNode{{
		ID:               "dns-us-1",
		Status:           model.EdgeHealthHealthy,
		Healthy:          true,
		CacheStatus:      "ready",
		CacheWriteErrors: 1,
		LastHeartbeatAt:  &now,
	}}) {
		t.Fatal("cache write errors must still block DNS inventory")
	}
	if dnsInventoryHealthy([]model.DNSNode{{
		ID:               "dns-us-1",
		Status:           model.EdgeHealthHealthy,
		Healthy:          true,
		CacheStatus:      "error",
		DNSBundleVersion: "dnsgen_bad",
		CacheLoadErrors:  1,
		LastHeartbeatAt:  &now,
	}}) {
		t.Fatal("unrecovered cache load errors must block DNS inventory")
	}
}

func TestEdgeRouteHealthyGroupsIgnoreStaleHeartbeat(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	stale := now.Add(-(platformNodeHeartbeatStaleAfter + time.Second))

	storeState := store.New(filepath.Join(t.TempDir(), "store.json"))
	if _, _, err := storeState.CreateEdgeNodeToken(model.EdgeNode{
		ID:                 "edge-de-1",
		EdgeGroupID:        "edge-group-country-de",
		Status:             model.EdgeHealthHealthy,
		Healthy:            true,
		CaddyRouteCount:    3,
		ServingGeneration:  "routegen_previous",
		LKGGeneration:      "routegen_previous",
		LastSeenAt:         &stale,
		LastHeartbeatAt:    &stale,
		RouteBundleVersion: "routegen_previous",
	}); err != nil {
		t.Fatalf("record stale edge node: %v", err)
	}
	healthy, expected, minimum, err := (&Server{store: storeState}).edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if healthy["edge-group-country-de"] {
		t.Fatalf("stale edge heartbeat must not keep group healthy: %v", healthy)
	}
	if !expected["edge-group-country-de"] {
		t.Fatalf("stale edge with previous route state should remain expected non-empty: %v", expected)
	}
	if minimum["edge-group-country-de"] != 0 {
		t.Fatalf("stale edge heartbeat must not preserve a minimum route count, got %v", minimum)
	}
}

func TestEdgeRouteHealthyGroupsIncludeDegradedServingCache(t *testing.T) {
	t.Parallel()

	storeState := store.New(filepath.Join(t.TempDir(), "store.json"))
	if _, _, err := storeState.CreateEdgeNodeToken(model.EdgeNode{
		ID:                 "edge-de-1",
		EdgeGroupID:        "edge-group-country-de",
		Status:             model.EdgeHealthDegraded,
		Healthy:            true,
		CaddyRouteCount:    39,
		ServingGeneration:  "routegen_lkg",
		LKGGeneration:      "routegen_lkg",
		RouteBundleVersion: "routegen_lkg",
	}); err != nil {
		t.Fatalf("create edge node: %v", err)
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                 "edge-de-1",
		EdgeGroupID:        "edge-group-country-de",
		Status:             model.EdgeHealthDegraded,
		Healthy:            true,
		CaddyRouteCount:    39,
		ServingGeneration:  "routegen_lkg",
		LKGGeneration:      "routegen_lkg",
		RouteBundleVersion: "routegen_lkg",
	}); err != nil {
		t.Fatalf("record degraded serving heartbeat: %v", err)
	}
	healthy, expected, minimum, err := (&Server{store: storeState}).edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if !healthy["edge-group-country-de"] {
		t.Fatalf("degraded edge serving LKG should remain route-capable: %v", healthy)
	}
	if !expected["edge-group-country-de"] || minimum["edge-group-country-de"] != 39 {
		t.Fatalf("expected LKG serving metadata to remain visible, expected=%v minimum=%v", expected, minimum)
	}
}
