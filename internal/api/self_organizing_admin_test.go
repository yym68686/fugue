package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestPlatformAutonomyControlsDefaultObserveOnlyAndKillSwitch(t *testing.T) {
	t.Setenv("FUGUE_AUTONOMY_MODE", "")
	t.Setenv("FUGUE_AUTONOMY_REPAIR_ENABLED", "")
	t.Setenv("FUGUE_AUTONOMY_QUARANTINE_ENABLED", "")
	t.Setenv("FUGUE_AUTONOMY_DNS_FILTERING_ENABLED", "")
	t.Setenv("FUGUE_AUTONOMY_PEER_OVERLAY_ENABLED", "")
	t.Setenv("FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED", "")
	t.Setenv("FUGUE_AUTONOMY_KILL_SWITCH", "")
	t.Setenv("FUGUE_AUTONOMY_DISABLED_NODES", "")
	t.Setenv("FUGUE_AUTONOMY_DISABLED_SERVICES", "")
	t.Setenv("FUGUE_AUTONOMY_BLAST_RADIUS_CAP", "")
	t.Setenv("FUGUE_AUTONOMY_ROLLBACK_PATH", "")
	controls := platformAutonomyControlsFromEnv()
	if controls.Mode != "observe-only" || controls.BlastRadiusCap == "" || controls.RollbackPath == "" || controls.AutomaticRepairEnabled || controls.QuarantineEnabled || controls.DNSFilteringEnabled || controls.PeerOverlayEnabled || controls.EndpointFallbackEnabled {
		t.Fatalf("expected default observe-only controls, got %+v", controls)
	}

	t.Setenv("FUGUE_AUTONOMY_MODE", "enforced")
	t.Setenv("FUGUE_AUTONOMY_REPAIR_ENABLED", "true")
	t.Setenv("FUGUE_AUTONOMY_QUARANTINE_ENABLED", "true")
	t.Setenv("FUGUE_AUTONOMY_DNS_FILTERING_ENABLED", "true")
	t.Setenv("FUGUE_AUTONOMY_PEER_OVERLAY_ENABLED", "true")
	t.Setenv("FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED", "true")
	t.Setenv("FUGUE_AUTONOMY_KILL_SWITCH", "true")
	t.Setenv("FUGUE_AUTONOMY_DISABLED_NODES", "node-a,node-b node-a")
	t.Setenv("FUGUE_AUTONOMY_DISABLED_SERVICES", "svc-a;svc-b")
	t.Setenv("FUGUE_AUTONOMY_BLAST_RADIUS_CAP", "one-edge")
	t.Setenv("FUGUE_AUTONOMY_ROLLBACK_PATH", "set FUGUE_AUTONOMY_KILL_SWITCH=true")
	controls = platformAutonomyControlsFromEnv()
	if !controls.GlobalKillSwitch || controls.AutomaticRepairEnabled || controls.QuarantineEnabled || controls.DNSFilteringEnabled || controls.PeerOverlayEnabled || controls.EndpointFallbackEnabled {
		t.Fatalf("expected kill switch to disable all autonomy actions, got %+v", controls)
	}
	if len(controls.DisabledNodes) != 2 || controls.DisabledNodes[0] != "node-a" || controls.DisabledNodes[1] != "node-b" {
		t.Fatalf("expected per-node disabled list, got %+v", controls.DisabledNodes)
	}
	if len(controls.DisabledServices) != 2 || controls.BlastRadiusCap != "one-edge" || controls.RollbackPath == "" {
		t.Fatalf("expected scoped safety controls, got %+v", controls)
	}
	_ = os.Getenv("FUGUE_AUTONOMY_MODE")
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

func TestImageCacheDaemonSetAvailabilityHonorsConfiguredMinimum(t *testing.T) {
	tests := []struct {
		name            string
		desired         int32
		ready           int32
		available       int32
		updated         int32
		misscheduled    int32
		minimumReplicas int
		wantPass        bool
		wantMessage     string
	}{
		{name: "fully converged", desired: 3, ready: 3, available: 3, updated: 3, minimumReplicas: 1, wantPass: true, wantMessage: "ready"},
		{name: "one unavailable node remains globally serviceable", desired: 7, ready: 6, available: 6, updated: 7, minimumReplicas: 1, wantPass: true, wantMessage: "partial convergence"},
		{name: "old serving pods remain globally serviceable during rollout", desired: 7, ready: 7, available: 7, updated: 6, minimumReplicas: 1, wantPass: true, wantMessage: "partial convergence"},
		{name: "no available cache fails", desired: 7, ready: 0, available: 0, updated: 7, minimumReplicas: 1, wantPass: false, wantMessage: "below configured minimum"},
		{name: "configured quorum fails closed", desired: 7, ready: 1, available: 1, updated: 7, minimumReplicas: 2, wantPass: false, wantMessage: "below configured minimum"},
		{name: "insufficient scheduled capacity fails closed", desired: 1, ready: 1, available: 1, updated: 1, minimumReplicas: 2, wantPass: false, wantMessage: "scheduled below configured minimum"},
		{name: "misscheduled pod fails closed", desired: 7, ready: 7, available: 7, updated: 7, misscheduled: 1, minimumReplicas: 1, wantPass: false, wantMessage: "misscheduled"},
		{name: "invalid minimum normalizes to one", desired: 1, ready: 1, available: 1, updated: 1, minimumReplicas: 0, wantPass: true, wantMessage: "required=1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pass, message := imageCacheDaemonSetAvailability(test.desired, test.ready, test.available, test.updated, test.misscheduled, test.minimumReplicas)
			if pass != test.wantPass {
				t.Fatalf("expected pass=%t, got pass=%t message=%q", test.wantPass, pass, message)
			}
			if !strings.Contains(message, test.wantMessage) {
				t.Fatalf("expected message containing %q, got %q", test.wantMessage, message)
			}
		})
	}
}

func TestImageStoreMinimumReplicasFromEnv(t *testing.T) {
	t.Setenv("FUGUE_IMAGE_STORE_MIN_REPLICAS", "")
	if got := imageStoreMinimumReplicasFromEnv(); got != 1 {
		t.Fatalf("expected image-store minimum replicas to default to 1, got %d", got)
	}

	t.Setenv("FUGUE_IMAGE_STORE_MIN_REPLICAS", "3")
	if got := imageStoreMinimumReplicasFromEnv(); got != 3 {
		t.Fatalf("expected image-store minimum replicas from env, got %d", got)
	}

	t.Setenv("FUGUE_IMAGE_STORE_MIN_REPLICAS", "invalid")
	if got := imageStoreMinimumReplicasFromEnv(); got != 1 {
		t.Fatalf("expected invalid image-store minimum replicas to fail safe to 1, got %d", got)
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

func TestPlatformAutonomyBlockRolloutOnlyHardFailures(t *testing.T) {
	if platformAutonomyBlockRollout(model.ControlPlaneStoreStatus{}, []model.StoreInvariantCheck{
		{Name: "edge", Pass: false, Message: "active=3 total=5"},
		{Name: "dns", Pass: false, Message: "generation drift"},
		{Name: "node_policy", Pass: false, Message: "policy inventory stale"},
	}) {
		t.Fatalf("expected data-plane degraded checks to stay visible without blocking rollout")
	}
	if !platformAutonomyBlockRollout(model.ControlPlaneStoreStatus{}, []model.StoreInvariantCheck{
		{Name: "registry", Pass: false},
	}) {
		t.Fatalf("expected registry hard failure to block rollout")
	}
	if !platformAutonomyBlockRollout(model.ControlPlaneStoreStatus{BlockRollout: true}, nil) {
		t.Fatalf("expected control-plane store block to block rollout")
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

func TestAutonomyActiveInventoryUsesHeartbeatFreshnessAndFailsClosedWhenEmpty(t *testing.T) {
	now := time.Now().UTC()
	stale := now.Add(-platformNodeHeartbeatStaleAfter - time.Second)
	policies := []model.ClusterNodePolicyStatus{
		{
			NodeName: "fresh-node",
			Policy: &model.ClusterNodePolicy{
				EffectiveEdge: true,
				EffectiveDNS:  true,
			},
		},
		{
			NodeName: "stale-node",
			Policy: &model.ClusterNodePolicy{
				EffectiveEdge: true,
				EffectiveDNS:  true,
			},
		},
	}
	edgeNodes := []model.EdgeNode{
		{
			ID:                 "fresh-node",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_fresh",
			ServingGeneration:  "routegen_fresh",
			CaddyRouteCount:    1,
			LastHeartbeatAt:    &now,
		},
		{
			ID:                 "stale-node",
			Status:             model.EdgeHealthHealthy,
			Healthy:            true,
			RouteBundleVersion: "routegen_stale",
			ServingGeneration:  "routegen_stale",
			CaddyRouteCount:    1,
			LastHeartbeatAt:    &stale,
		},
	}
	dnsNodes := []model.DNSNode{
		{
			ID:                "fresh-node",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_fresh",
			ServingGeneration: "dnsgen_fresh",
			LastHeartbeatAt:   &now,
		},
		{
			ID:                "stale-node",
			Status:            model.EdgeHealthHealthy,
			Healthy:           true,
			DNSBundleVersion:  "dnsgen_stale",
			ServingGeneration: "dnsgen_stale",
			LastHeartbeatAt:   &stale,
		},
	}

	activeEdges := activeEdgeNodesForAutonomy(edgeNodes, policies, now)
	if len(activeEdges) != 1 || activeEdges[0].ID != "fresh-node" {
		t.Fatalf("expected autonomy edge inventory to exclude stale heartbeat, got %#v", activeEdges)
	}
	activeDNS := activeDNSNodesForAutonomy(dnsNodes, policies, now)
	if len(activeDNS) != 1 || activeDNS[0].ID != "fresh-node" {
		t.Fatalf("expected autonomy DNS inventory to exclude stale heartbeat, got %#v", activeDNS)
	}

	onlyStalePolicies := policies[1:]
	if got := activeEdgeNodesForAutonomy(edgeNodes[1:], onlyStalePolicies, now); len(got) != 0 || edgeInventoryHealthy(got) {
		t.Fatalf("expected all-stale edge inventory to be empty and fail closed, got %#v", got)
	}
	if got := activeDNSNodesForAutonomy(dnsNodes[1:], onlyStalePolicies, now); len(got) != 0 || dnsInventoryHealthy(got) {
		t.Fatalf("expected all-stale DNS inventory to be empty and fail closed, got %#v", got)
	}
}
