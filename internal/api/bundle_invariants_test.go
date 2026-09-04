package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

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
	err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleInvariantInput{
		Options:          edgeDNSBundleOptions{Zone: "fugue.pro"},
		ProtectedRecords: protected,
	})
	if err == nil || !strings.Contains(err.Error(), "protected fugue.pro NS value is missing") {
		t.Fatalf("expected protected record invariant failure, got %v", err)
	}
}

func TestEdgeDNSBundleInvariantRejectsRouteReadyMismatch(t *testing.T) {
	t.Parallel()

	bundle := model.EdgeDNSBundle{
		Version:     "dnsgen_route_ready_mismatch",
		Generation:  "dnsgen_route_ready_mismatch",
		GeneratedAt: time.Now().UTC(),
		Zone:        "fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "d-test.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.20"},
				RecordKind: model.EdgeDNSRecordKindProbe,
				Status:     model.EdgeRouteStatusActive,
			},
			{
				Name:       "demo.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10"},
				RecordKind: model.EdgeDNSRecordKindPlatform,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}
	err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleInvariantInput{
		Options: edgeDNSBundleOptions{Zone: "fugue.pro"},
		AnswerEdgeGroupsByIP: map[string][]string{
			"203.0.113.10": []string{"edge-group-country-us"},
		},
		RouteReadyByHostnameEdgeGroup: map[string]map[string]bool{},
		RecordRouteHostsByName: map[string][]string{
			"demo.fugue.pro": {"demo.fugue.pro"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "without active route for demo.fugue.pro") {
		t.Fatalf("expected route-ready mismatch failure, got %v", err)
	}
}

func TestEdgeDNSBundleInvariantRejectsCustomDomainTargetRouteReadyMismatch(t *testing.T) {
	t.Parallel()

	bundle := model.EdgeDNSBundle{
		Version:     "dnsgen_custom_target_route_ready_mismatch",
		Generation:  "dnsgen_custom_target_route_ready_mismatch",
		GeneratedAt: time.Now().UTC(),
		Zone:        "fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "d-test.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.20"},
				RecordKind: model.EdgeDNSRecordKindProbe,
				Status:     model.EdgeRouteStatusActive,
			},
			{
				Name:       "d-shared.dns.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10"},
				RecordKind: model.EdgeDNSRecordKindCustomDomainTarget,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}
	err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleInvariantInput{
		Options: edgeDNSBundleOptions{Zone: "fugue.pro"},
		AnswerEdgeGroupsByIP: map[string][]string{
			"203.0.113.10": {"edge-group-country-de"},
		},
		RouteReadyByHostnameEdgeGroup: map[string]map[string]bool{
			"shared.example.net": {"edge-group-country-us": true},
		},
		RecordRouteHostsByName: map[string][]string{
			"d-shared.dns.fugue.pro": {"shared.example.net"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "without active route for shared.example.net") {
		t.Fatalf("expected custom-domain target route-ready mismatch failure, got %v", err)
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

	storePath := filepath.Join(t.TempDir(), "store.json")
	storeState := store.New(storePath)
	if err := storeState.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	forceEdgeInstanceHeartbeatTimeForAPITest(t, storePath, stale)
	healthy, _, expected, minimum, err := (&Server{store: storeState}).edgeRouteGroupInventory(context.Background())
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if healthy["edge-group-country-de"] {
		t.Fatalf("stale edge heartbeat must not keep group healthy: %v", healthy)
	}
	if !expected["edge-group-country-de"] {
		t.Fatalf("stale edge with previous route state should remain expected non-empty: %v", expected)
	}
	if minimum["edge-group-country-de"] != 3 {
		t.Fatalf("stale edge LKG route state must preserve a minimum route count, got %v", minimum)
	}
}

func forceEdgeInstanceHeartbeatTimeForAPITest(t *testing.T, path string, heartbeatAt time.Time) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read edge instance fixture: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatalf("decode edge instance fixture: %v", err)
	}
	if len(state.EdgeNodeInstances) == 0 {
		t.Fatal("edge instance fixture is empty")
	}
	for index := range state.EdgeNodeInstances {
		state.EdgeNodeInstances[index].LastHeartbeatAt = heartbeatAt
		state.EdgeNodeInstances[index].UpdatedAt = heartbeatAt
		state.EdgeNodeInstances[index].Node.LastHeartbeatAt = &heartbeatAt
		state.EdgeNodeInstances[index].Node.LastSeenAt = &heartbeatAt
		state.EdgeNodeInstances[index].Node.UpdatedAt = heartbeatAt
	}
	for index := range state.EdgeNodes {
		state.EdgeNodes[index].LastHeartbeatAt = &heartbeatAt
		state.EdgeNodes[index].LastSeenAt = &heartbeatAt
		state.EdgeNodes[index].UpdatedAt = heartbeatAt
	}
	payload, err = json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode edge instance fixture: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write edge instance fixture: %v", err)
	}
}

func TestEdgeRouteHealthyGroupsIncludeDegradedServingCache(t *testing.T) {
	t.Parallel()

	storeState := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := storeState.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
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
	if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, model.EdgeNode{
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
	healthy, _, expected, minimum, err := (&Server{store: storeState}).edgeRouteGroupInventory(context.Background())
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
