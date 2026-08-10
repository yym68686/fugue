package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEligibleDiscoveryEdgeNodesFiltersAndSortsWithoutCountrySelection(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-10 * time.Second)
	stale := now.Add(-2 * time.Minute)
	nodes := []model.EdgeNode{
		{ID: "edge-b", EdgeGroupID: "pool-b", PublicHostname: "b.example", Region: "na", Country: "us", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-b", CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady, LastHeartbeatAt: &fresh},
		{ID: "edge-a", EdgeGroupID: "pool-a", PublicIPv4: "203.0.113.10", Region: "eu", Country: "de", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-a", CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady, LastHeartbeatAt: &fresh},
		{ID: "edge-stale", EdgeGroupID: "pool-c", PublicIPv4: "203.0.113.11", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-c", CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady, LastHeartbeatAt: &stale},
		{ID: "edge-draining", EdgeGroupID: "pool-d", PublicIPv4: "203.0.113.12", Status: model.EdgeHealthHealthy, Healthy: true, Draining: true, RouteBundleVersion: "route-d", CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady, LastHeartbeatAt: &fresh},
	}
	got := eligibleDiscoveryEdgeNodes(nodes, now, nil)
	if len(got) != 2 || got[0].ID != "edge-a" || got[1].ID != "edge-b" {
		t.Fatalf("unexpected candidates: %+v", got)
	}
	if got[0].Country != "de" || got[1].Country != "us" {
		t.Fatalf("country must be preserved only as a label: %+v", got)
	}
}

func TestEligibleDiscoveryEdgeNodesRejectsQuarantine(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-time.Second)
	node := model.EdgeNode{ID: "edge-a", EdgeGroupID: "pool-a", PublicHostname: "a.example", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-a", CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady, LastHeartbeatAt: &fresh}
	quarantine := map[string]model.NodeDeepHealthResult{"edge-a": {QuarantineState: "quarantined"}}
	if got := eligibleDiscoveryEdgeNodes([]model.EdgeNode{node}, now, quarantine); len(got) != 0 {
		t.Fatalf("quarantined edge must not be eligible: %+v", got)
	}
}
