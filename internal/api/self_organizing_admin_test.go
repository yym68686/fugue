package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRegistryReachabilityCheckFailsWhenRegistryUnavailable(t *testing.T) {
	server := &Server{
		registryPushBase:            "127.0.0.1:1",
		registryPullBase:            "registry.fugue.internal:5000",
		clusterJoinRegistryEndpoint: "127.0.0.1:30500",
	}

	pass, message := server.registryReachabilityCheck(context.Background())
	if pass {
		t.Fatalf("expected unavailable registry to fail, got message %q", message)
	}
	if !strings.Contains(message, "registry unavailable") {
		t.Fatalf("expected unavailable registry message, got %q", message)
	}
}

func TestRegistryReachabilityCheckPassesOnRegistryV2Endpoint(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/" {
			t.Fatalf("expected /v2/ probe path, got %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer probe.Close()

	server := &Server{
		registryPushBase:            probe.URL,
		registryPullBase:            "registry.fugue.internal:5000",
		clusterJoinRegistryEndpoint: "127.0.0.1:30500",
	}
	pass, message := server.registryReachabilityCheck(context.Background())
	if !pass {
		t.Fatalf("expected reachable registry to pass, got %q", message)
	}
}

func TestHeadscaleReachabilityCheckFailsOnBadHealth(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("expected /health probe path, got %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer probe.Close()

	server := &Server{
		clusterJoinMeshProvider:    "tailscale",
		clusterJoinMeshLoginServer: probe.URL,
	}
	pass, message := server.headscaleReachabilityCheck(context.Background())
	if pass {
		t.Fatalf("expected unhealthy headscale to fail, got %q", message)
	}
}

func TestEdgeInventoryHealthyAcceptsDegradedServingLKG(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.EdgeNode{{
		ID:                 "edge-us-1",
		EdgeGroupID:        "edge-group-country-us",
		Status:             model.EdgeHealthDegraded,
		Healthy:            true,
		RouteBundleVersion: "routegen_lkg",
		ServingGeneration:  "routegen_lkg",
		LKGGeneration:      "routegen_lkg",
		CaddyRouteCount:    44,
		CacheStatus:        "stale",
		LastHeartbeatAt:    &now,
	}}
	if !edgeInventoryHealthy(nodes) {
		t.Fatalf("expected degraded edge serving LKG to satisfy autonomy edge inventory")
	}
}
