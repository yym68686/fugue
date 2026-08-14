package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestNodeHeartbeatFreshUsesNewestActivityEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 14, 23, 5, 0, 0, time.UTC)
	staleHeartbeat := now.Add(-7 * 24 * time.Hour)
	recentRouteSync := now.Add(-30 * time.Second)

	if !nodeHeartbeatFresh(&staleHeartbeat, &recentRouteSync, now) {
		t.Fatal("recent last_seen_at must keep a node fresh when heartbeat timestamp is stale")
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

func TestEdgeNodeRouteServingCapableUsesRecentAuthenticatedActivity(t *testing.T) {
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
		t.Fatal("recent authenticated route sync must keep an otherwise healthy edge route-capable")
	}
}

func TestLatestNodeActivityIgnoresZeroValues(t *testing.T) {
	t.Parallel()

	zero := time.Time{}
	recent := time.Date(2026, time.August, 14, 23, 4, 0, 0, time.UTC)
	got := latestNodeActivity(&zero, &recent)
	if got == nil || !got.Equal(recent) {
		t.Fatalf("expected latest non-zero activity %s, got %v", recent, got)
	}
}
