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

func TestRegistryReachabilityCheckFallsBackToReadyNodeLocalImageCache(t *testing.T) {
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/fugue-system/daemonsets":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"items": [{
					"metadata": {
						"name": "fugue-fugue-image-cache",
						"labels": {
							"app.kubernetes.io/component": "image-cache",
							"app.kubernetes.io/instance": "fugue"
						}
					},
					"status": {
						"desiredNumberScheduled": 3,
						"currentNumberScheduled": 3,
						"numberReady": 3,
						"updatedNumberScheduled": 3,
						"numberAvailable": 3
					}
				}]
			}`))
		default:
			t.Fatalf("unexpected kubernetes path %q", r.URL.String())
		}
	}))
	defer kube.Close()

	server := &Server{
		registryPushBase:            "127.0.0.1:1",
		registryPullBase:            "registry.fugue.internal:5000",
		clusterJoinRegistryEndpoint: "http://127.0.0.1:5000",
		controlPlaneNamespace:       "fugue-system",
		controlPlaneReleaseInstance: "fugue",
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{
				client:      kube.Client(),
				baseURL:     kube.URL,
				bearerToken: "test-token",
			}, nil
		},
	}
	pass, message := server.registryReachabilityCheck(context.Background())
	if !pass {
		t.Fatalf("expected ready node-local image-cache to pass, got %q", message)
	}
	if !strings.Contains(message, "node-local image-cache") {
		t.Fatalf("expected node-local image-cache message, got %q", message)
	}
}

func TestRegistryEndpointIsNodeLocalImageCacheRejectsLegacyNodePort(t *testing.T) {
	if registryEndpointIsNodeLocalImageCache("127.0.0.1:30500", "registry.fugue.internal:5000") {
		t.Fatal("expected legacy local registry NodePort to be rejected")
	}
	if !registryEndpointIsNodeLocalImageCache("http://127.0.0.1:5000", "registry.fugue.internal:5000") {
		t.Fatal("expected loopback endpoint matching registry pull port to be accepted")
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

func TestRouteServingModesKeepPathPrefixRoutesSeparate(t *testing.T) {
	generatedAt := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	routes := []model.EdgeRouteBinding{
		{
			Hostname:          "api.example.com",
			PathPrefix:        "/",
			Status:            model.EdgeRouteStatusActive,
			RoutePolicy:       model.EdgeRoutePolicyEnabled,
			SelectedEdgeGroup: "edge-a",
			RuntimeEdgeGroup:  "edge-a",
			RouteKind:         model.EdgeRouteKindPlatform,
		},
		{
			Hostname:          "api.example.com",
			PathPrefix:        "/v1",
			Status:            model.EdgeRouteStatusActive,
			RoutePolicy:       model.EdgeRoutePolicyEnabled,
			SelectedEdgeGroup: "edge-b",
			RuntimeEdgeGroup:  "edge-b",
			RouteKind:         model.EdgeRouteKindPlatform,
		},
	}

	modes := routeServingModes(routes, generatedAt)
	if len(modes) != 2 {
		t.Fatalf("expected two serving modes, got %+v", modes)
	}
	if modes[0].Hostname != "api.example.com" || modes[0].PathPrefix != "/" {
		t.Fatalf("expected root route first, got %+v", modes[0])
	}
	if modes[1].Hostname != "api.example.com" || modes[1].PathPrefix != "/v1" {
		t.Fatalf("expected /v1 route second, got %+v", modes[1])
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

func TestHeadscaleReachabilityCheckFailsOnUnpinnedHostPathDeployment(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/key":
			if r.URL.Query().Get("v") == "" {
				t.Fatalf("expected headscale key probe to include client version query")
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected headscale probe path %q", r.URL.Path)
		}
	}))
	defer probe.Close()

	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/fugue-system/deployments":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"items": [{
					"metadata": {
						"name": "fugue-fugue-headscale",
						"labels": {
							"app.kubernetes.io/component": "headscale",
							"app.kubernetes.io/instance": "fugue"
						}
					},
					"spec": {
						"replicas": 1,
						"template": {
							"spec": {
								"containers": [{"name": "headscale", "image": "headscale/headscale:0.26.1"}],
								"volumes": [{
									"name": "headscale-data",
									"hostPath": {"path": "/var/lib/fugue/headscale", "type": "DirectoryOrCreate"}
								}]
							}
						}
					},
					"status": {"readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1}
				}]
			}`))
		case "/api/v1/namespaces/fugue-system/pods":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"items": [{
					"metadata": {
						"name": "fugue-fugue-headscale-abc",
						"creationTimestamp": "2026-05-25T00:00:00Z",
						"labels": {
							"app.kubernetes.io/component": "headscale",
							"app.kubernetes.io/instance": "fugue"
						}
					},
					"spec": {
						"nodeName": "runtime-1",
						"containers": [{"name": "headscale", "image": "headscale/headscale:0.26.1"}]
					},
					"status": {
						"phase": "Running",
						"containerStatuses": [{"name": "headscale", "ready": true}]
					}
				}]
			}`))
		default:
			t.Fatalf("unexpected kubernetes path %q", r.URL.String())
		}
	}))
	defer kube.Close()

	server := &Server{
		clusterJoinMeshProvider:     "tailscale",
		clusterJoinMeshLoginServer:  probe.URL,
		controlPlaneNamespace:       "fugue-system",
		controlPlaneReleaseInstance: "fugue",
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{
				client:      kube.Client(),
				baseURL:     kube.URL,
				bearerToken: "test-token",
			}, nil
		},
	}
	pass, message := server.headscaleReachabilityCheck(context.Background())
	if pass {
		t.Fatalf("expected unpinned headscale hostPath to fail, got %q", message)
	}
	if !strings.Contains(message, "hostPath storage is not pinned") {
		t.Fatalf("expected hostPath pinning failure, got %q", message)
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

func TestEdgeInventoryHealthyIgnoresBootstrapPendingEdgeNodes(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.EdgeNode{
		{
			ID:                 "edge-us-1",
			EdgeGroupID:        "edge-group-country-us",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_live",
			ServingGeneration:  "routegen_live",
			LKGGeneration:      "routegen_live",
			CaddyRouteCount:    46,
			CacheStatus:        "ready",
			LastHeartbeatAt:    &now,
		},
		{
			ID:              "edge-hk-1",
			EdgeGroupID:     "edge-group-country-hk",
			Status:          model.EdgeHealthUnknown,
			Healthy:         false,
			CaddyRouteCount: 0,
		},
	}
	if !edgeInventoryHealthy(nodes) {
		t.Fatalf("expected bootstrap pending edge node to be ignored while healthy edge remains serving")
	}
}

func TestEdgeInventoryHealthyIgnoresRouteBootstrapEdgeNodes(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.EdgeNode{
		{
			ID:                 "edge-us-1",
			EdgeGroupID:        "edge-group-country-us",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_live",
			ServingGeneration:  "routegen_live",
			LKGGeneration:      "routegen_live",
			CaddyRouteCount:    46,
			CacheStatus:        "ready",
			LastHeartbeatAt:    &now,
		},
		{
			ID:                 "edge-hk-1",
			EdgeGroupID:        "edge-group-country-hk",
			Status:             model.EdgeHealthDegraded,
			Healthy:            true,
			RouteBundleVersion: "routegen_bootstrap",
			ServingGeneration:  "routegen_bootstrap",
			LKGGeneration:      "routegen_bootstrap",
			CacheStatus:        "stale",
			LastError:          "edge routes returned status 500: bundle bootstrap in progress",
			LastHeartbeatAt:    &now,
		},
	}
	if !edgeInventoryHealthy(nodes) {
		t.Fatalf("expected route bootstrap edge node to be ignored while healthy edge remains serving")
	}
}

func TestActiveInventoryFiltersNodesRetiredByNodePolicy(t *testing.T) {
	now := time.Now().UTC()
	stale := now.Add(-1 * time.Hour)
	policies := []model.ClusterNodePolicyStatus{
		{
			NodeName: "edge-us-1",
			Policy: &model.ClusterNodePolicy{
				EffectiveEdge: true,
				EffectiveDNS:  true,
			},
		},
		{
			NodeName: "fortedrape8",
			Policy: &model.ClusterNodePolicy{
				EffectiveEdge: false,
				EffectiveDNS:  false,
			},
		},
	}

	edgeNodes := []model.EdgeNode{
		{
			ID:                 "edge-us-1",
			EdgeGroupID:        "edge-group-country-us",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_live",
			ServingGeneration:  "routegen_live",
			CaddyRouteCount:    40,
			LastHeartbeatAt:    &now,
		},
		{
			ID:                 "fortedrape8",
			EdgeGroupID:        "edge-group-country-hk",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_old",
			ServingGeneration:  "routegen_old",
			CaddyRouteCount:    40,
			LastHeartbeatAt:    &stale,
		},
	}
	dnsNodes := []model.DNSNode{
		{
			ID:                "edge-us-1",
			EdgeGroupID:       "edge-group-country-us",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_live",
			ServingGeneration: "dnsgen_live",
			LastHeartbeatAt:   &now,
		},
		{
			ID:                "fortedrape8",
			EdgeGroupID:       "edge-group-country-hk",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_old",
			ServingGeneration: "dnsgen_old",
			LastHeartbeatAt:   &stale,
		},
	}

	activeEdges := activeEdgeNodesForPolicy(edgeNodes, policies)
	if len(activeEdges) != 1 || activeEdges[0].ID != "edge-us-1" {
		t.Fatalf("expected only active edge policy node, got %#v", activeEdges)
	}
	if !edgeInventoryHealthy(activeEdges) {
		t.Fatalf("expected active edge inventory to ignore retired stale edge node")
	}

	activeDNS := activeDNSNodesForPolicy(dnsNodes, policies)
	if len(activeDNS) != 1 || activeDNS[0].ID != "edge-us-1" {
		t.Fatalf("expected only active DNS policy node, got %#v", activeDNS)
	}
	if !dnsInventoryHealthy(activeDNS) {
		t.Fatalf("expected active DNS inventory to ignore retired stale DNS node")
	}

	groups := activeEdgeGroupsForInventory([]model.EdgeGroup{
		{ID: "edge-group-country-us"},
		{ID: "edge-group-country-hk"},
	}, activeEdges, activeDNS)
	if len(groups) != 1 || groups[0].ID != "edge-group-country-us" {
		t.Fatalf("expected discovery groups to keep only active inventory groups, got %#v", groups)
	}
}

func TestActiveInventoryFiltersNodesAbsentFromNodePolicyInventory(t *testing.T) {
	now := time.Now().UTC()
	policies := []model.ClusterNodePolicyStatus{
		{
			NodeName: "vps-591f4447",
			Policy: &model.ClusterNodePolicy{
				EffectiveEdge: true,
				EffectiveDNS:  true,
			},
		},
	}

	edgeNodes := []model.EdgeNode{
		{
			ID:                 "vps-591f4447",
			EdgeGroupID:        "edge-group-country-us",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_live",
			ServingGeneration:  "routegen_live",
			CaddyRouteCount:    99,
			LastHeartbeatAt:    &now,
		},
		{
			ID:                 "bwg",
			EdgeGroupID:        "edge-group-country-us",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_stale",
			ServingGeneration:  "routegen_stale",
			CaddyRouteCount:    99,
			LastHeartbeatAt:    &now,
		},
	}
	dnsNodes := []model.DNSNode{
		{
			ID:                "vps-591f4447",
			EdgeGroupID:       "edge-group-country-us",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_live",
			ServingGeneration: "dnsgen_live",
			LastHeartbeatAt:   &now,
		},
		{
			ID:                "bwg",
			EdgeGroupID:       "edge-group-country-us",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_stale",
			ServingGeneration: "dnsgen_stale",
			LastHeartbeatAt:   &now,
		},
	}

	activeEdges := activeEdgeNodesForPolicy(edgeNodes, policies)
	if len(activeEdges) != 1 || activeEdges[0].ID != "vps-591f4447" {
		t.Fatalf("expected absent edge node to be treated as retired, got %#v", activeEdges)
	}
	activeDNS := activeDNSNodesForPolicy(dnsNodes, policies)
	if len(activeDNS) != 1 || activeDNS[0].ID != "vps-591f4447" {
		t.Fatalf("expected absent DNS node to be treated as retired, got %#v", activeDNS)
	}

	rejoinedPolicies := append(append([]model.ClusterNodePolicyStatus(nil), policies...), model.ClusterNodePolicyStatus{
		NodeName: "bwg",
		Policy: &model.ClusterNodePolicy{
			EffectiveEdge: true,
			EffectiveDNS:  true,
		},
	})
	activeEdges = activeEdgeNodesForPolicy(edgeNodes, rejoinedPolicies)
	if len(activeEdges) != 2 {
		t.Fatalf("expected rejoined edge node to be admitted, got %#v", activeEdges)
	}
	activeDNS = activeDNSNodesForPolicy(dnsNodes, rejoinedPolicies)
	if len(activeDNS) != 2 {
		t.Fatalf("expected rejoined DNS node to be admitted, got %#v", activeDNS)
	}
}
