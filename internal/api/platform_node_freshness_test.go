package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestNodeHeartbeatFreshUsesRecentSuccessfulRouteSyncActivity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 14, 23, 5, 0, 0, time.UTC)
	staleHeartbeat := now.Add(-7 * 24 * time.Hour)
	recentRouteSync := now.Add(-30 * time.Second)

	if !nodeHeartbeatFresh(&staleHeartbeat, &recentRouteSync, now) {
		t.Fatal("recent successful route sync must keep a node fresh when heartbeat timestamp is stale")
	}
}

func TestNodeHeartbeatFreshFallsBackToHeartbeatWhenLastSeenMissing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 14, 23, 5, 0, 0, time.UTC)
	recentHeartbeat := now.Add(-30 * time.Second)

	if !nodeHeartbeatFresh(&recentHeartbeat, nil, now) {
		t.Fatal("recent heartbeat must keep a node fresh when last_seen_at is missing")
	}
}

func TestEdgeNodeRouteServingCapableUsesRecentSuccessfulRouteSync(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 14, 23, 5, 0, 0, time.UTC)
	staleHeartbeat := now.Add(-7 * 24 * time.Hour)
	recentRouteSync := now.Add(-30 * time.Second)
	node := model.EdgeNode{
		ID:                 "edge-us-1",
		EdgeGroupID:        "edge-group-country-us",
		Healthy:            true,
		Status:             model.EdgeHealthHealthy,
		CaddyRouteCount:    116,
		LastHeartbeatAt:    &staleHeartbeat,
		LastSeenAt:         &recentRouteSync,
		RouteBundleVersion: "routegen_current",
	}

	if !edgeNodeRouteServingCapable(node, now) {
		t.Fatal("recent successful route sync must keep an otherwise healthy edge route-capable")
	}
}
