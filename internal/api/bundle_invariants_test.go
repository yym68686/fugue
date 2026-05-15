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
		BundleSyncErrors: 3,
		LastHeartbeatAt:  &now,
	}}) {
		t.Fatal("historical bundle sync errors should not block currently healthy DNS inventory")
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
	healthy, expected, err := (&Server{store: storeState}).edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if healthy["edge-group-country-de"] {
		t.Fatalf("stale edge heartbeat must not keep group healthy: %v", healthy)
	}
	if !expected["edge-group-country-de"] {
		t.Fatalf("stale edge with previous route state should remain expected non-empty: %v", expected)
	}
}
