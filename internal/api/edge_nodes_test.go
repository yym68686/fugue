package api

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeHeartbeatRegistersInventoryAndAdminList(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	heartbeat := httptest.NewRecorder()
	body := map[string]any{
		"edge_id":              "edge-us-1",
		"edge_group_id":        "edge-group-country-us",
		"region":               "us-east",
		"country":              "US",
		"public_ipv4":          "203.0.113.10",
		"mesh_ip":              "100.64.0.10",
		"route_bundle_version": "routegen_first",
		"dns_bundle_version":   "dnsgen_first",
		"caddy_route_count":    3,
		"tls_status":           model.EdgeTLSStatusReady,
		"tls_last_message":     "static platform certificate loaded",
		"tls_ready_at":         time.Now().UTC(),
		"status":               model.EdgeHealthHealthy,
		"healthy":              true,
		"draining":             false,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(heartbeat, req)
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}
	var heartbeatResponse struct {
		Node     model.EdgeNode `json:"node"`
		Accepted bool           `json:"accepted"`
	}
	mustDecodeJSON(t, heartbeat, &heartbeatResponse)
	if !heartbeatResponse.Accepted ||
		heartbeatResponse.Node.ID != "edge-us-1" ||
		heartbeatResponse.Node.EdgeGroupID != "edge-group-country-us" ||
		heartbeatResponse.Node.RouteBundleVersion != "routegen_first" ||
		heartbeatResponse.Node.DNSBundleVersion != "dnsgen_first" ||
		heartbeatResponse.Node.CaddyRouteCount != 3 ||
		heartbeatResponse.Node.TLSStatus != model.EdgeTLSStatusReady ||
		heartbeatResponse.Node.TLSLastMessage != "static platform certificate loaded" ||
		heartbeatResponse.Node.TLSReadyAt == nil ||
		heartbeatResponse.Node.LastHeartbeatAt == nil {
		t.Fatalf("unexpected heartbeat response: %+v", heartbeatResponse)
	}

	list := performJSONRequest(t, server, http.MethodGet, "/v1/edge/nodes", platformAdminKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	var listResponse struct {
		Nodes  []model.EdgeNode  `json:"nodes"`
		Groups []model.EdgeGroup `json:"groups"`
	}
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Nodes) != 1 || listResponse.Nodes[0].ID != "edge-us-1" || listResponse.Nodes[0].TokenHash != "" {
		t.Fatalf("unexpected edge node list: %+v", listResponse.Nodes)
	}
	if len(listResponse.Groups) != 1 ||
		listResponse.Groups[0].ID != "edge-group-country-us" ||
		!listResponse.Groups[0].HasHealthyNodes ||
		listResponse.Groups[0].HealthyNodeCount != 1 {
		t.Fatalf("unexpected edge group summary: %+v", listResponse.Groups)
	}

	get := performJSONRequest(t, server, http.MethodGet, "/v1/edge/nodes/edge-us-1", platformAdminKey, nil)
	if get.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, get.Code, get.Body.String())
	}
	var getResponse struct {
		Node  model.EdgeNode  `json:"node"`
		Group model.EdgeGroup `json:"group"`
	}
	mustDecodeJSON(t, get, &getResponse)
	if getResponse.Node.ID != "edge-us-1" || !getResponse.Group.HasHealthyNodes {
		t.Fatalf("unexpected edge node get response: %+v", getResponse)
	}
}

func TestEdgeHeartbeatStoresPerformanceSamples(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", "", map[string]any{
		"edge_id":       "edge-us-1",
		"edge_group_id": "edge-group-country-us",
		"region":        "us-east",
		"country":       "US",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
		"draining":      false,
		"performance_samples": []map[string]any{
			{
				"id":                      "sample-1",
				"hostname":                "Demo.Fugue.Pro.",
				"client_asn":              "AS123",
				"runtime_region":          "us",
				"ttfb_ms":                 120,
				"upstream_ms":             80,
				"total_ms":                140,
				"status_code":             200,
				"sample_count":            4,
				"cache_hit_count":         3,
				"cache_observation_count": 4,
				"sampled_at":              now,
			},
		},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	samples, err := storeState.ListEdgePerformanceSamples("demo.fugue.pro", time.Time{})
	if err != nil {
		t.Fatalf("list edge performance samples: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected one performance sample, got %+v", samples)
	}
	sample := samples[0]
	if sample.ID != "sample-1" ||
		sample.EdgeID != "edge-us-1" ||
		sample.EdgeGroupID != "edge-group-country-us" ||
		sample.Hostname != "demo.fugue.pro" ||
		sample.ClientCountry != "" ||
		sample.ClientRegion != "" ||
		sample.ClientASN != "as123" ||
		sample.SampleCount != 4 ||
		sample.CacheHitCount != 3 ||
		sample.CacheObservationCount != 4 {
		t.Fatalf("unexpected persisted sample: %+v", sample)
	}
}

func TestGetEdgeNodeQualityAggregatesOnlyRequestedEdge(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	edgeTLSReadyAt := now.Add(-3 * time.Hour)
	edgeHeartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", "", map[string]any{
		"edge_id":              "edge-us-1",
		"edge_group_id":        "edge-group-country-us",
		"region":               "us-east",
		"country":              "US",
		"route_bundle_version": "routegen_us",
		"dns_bundle_version":   "dnsgen_us",
		"caddy_route_count":    7,
		"cache_status":         "ready",
		"tls_status":           model.EdgeTLSStatusReady,
		"tls_last_message":     "all certificates ready",
		"tls_ready_at":         edgeTLSReadyAt,
		"status":               model.EdgeHealthHealthy,
		"healthy":              true,
		"draining":             false,
		"performance_samples": []map[string]any{
			{
				"id":                      "edge-us-sample-1",
				"hostname":                "Demo.Fugue.Pro",
				"path_prefix":             "/",
				"tls_handshake_ms":        10,
				"ttfb_ms":                 120,
				"upstream_ms":             80,
				"total_ms":                140,
				"sample_count":            4,
				"cache_hit_count":         3,
				"cache_observation_count": 4,
				"error_count":             1,
				"sampled_at":              now.Add(-2 * time.Hour),
			},
			{
				"id":                      "edge-us-sample-2",
				"hostname":                "Demo.Fugue.Pro",
				"path_prefix":             "/api",
				"tls_handshake_ms":        20,
				"ttfb_ms":                 200,
				"upstream_ms":             150,
				"total_ms":                240,
				"sample_count":            2,
				"cache_hit_count":         1,
				"cache_observation_count": 2,
				"error_count":             0,
				"sampled_at":              now.Add(-1 * time.Hour),
			},
		},
	})
	if edgeHeartbeat.Code != http.StatusOK {
		t.Fatalf("expected edge heartbeat status %d, got %d body=%s", http.StatusOK, edgeHeartbeat.Code, edgeHeartbeat.Body.String())
	}
	otherEdgeHeartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", "", map[string]any{
		"edge_id":       "edge-de-1",
		"edge_group_id": "edge-group-country-de",
		"region":        "eu-central",
		"country":       "DE",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
		"draining":      false,
		"performance_samples": []map[string]any{
			{
				"id":           "edge-de-sample-1",
				"hostname":     "Demo.Fugue.Pro",
				"ttfb_ms":      999,
				"sample_count": 10,
				"error_count":  10,
				"sampled_at":   now.Add(-30 * time.Minute),
			},
		},
	})
	if otherEdgeHeartbeat.Code != http.StatusOK {
		t.Fatalf("expected other edge heartbeat status %d, got %d body=%s", http.StatusOK, otherEdgeHeartbeat.Code, otherEdgeHeartbeat.Body.String())
	}

	quality := performJSONRequest(t, server, http.MethodGet, "/v1/edge/nodes/edge-us-1/quality?since=6h", platformAdminKey, nil)
	if quality.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, quality.Code, quality.Body.String())
	}
	var response model.EdgeNodeQualityResponse
	mustDecodeJSON(t, quality, &response)
	summary := response.Summary
	if response.Node.ID != "edge-us-1" ||
		summary.EdgeID != "edge-us-1" ||
		summary.EdgeGroupID != "edge-group-country-us" ||
		summary.SampleRecordCount != 2 ||
		summary.RequestCount != 6 ||
		summary.ErrorCount != 1 ||
		summary.CacheHitCount != 4 ||
		summary.CacheObservationCount != 6 ||
		summary.TLSStatus != model.EdgeTLSStatusReady ||
		summary.CacheStatus != "ready" ||
		summary.CaddyRouteCount != 7 ||
		summary.RouteBundleVersion != "routegen_us" ||
		summary.DNSBundleVersion != "dnsgen_us" ||
		summary.LastSampledAt == nil ||
		!summary.LastSampledAt.Equal(now.Add(-1*time.Hour).Truncate(0)) {
		t.Fatalf("unexpected quality summary: %+v response=%+v", summary, response)
	}
	if !edgeQualityFloatClose(summary.ErrorRate, 1.0/6.0) ||
		!edgeQualityFloatClose(summary.CacheHitRate, 4.0/6.0) ||
		!edgeQualityFloatClose(summary.AvgTTFBMS, (120.0*4.0+200.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgTLSHandshakeMS, (10.0*4.0+20.0*2.0)/6.0) {
		t.Fatalf("unexpected quality rates/averages: %+v", summary)
	}
	if len(response.Routes) != 2 {
		t.Fatalf("expected two route summaries, got %+v", response.Routes)
	}
	if response.Routes[0].Hostname != "demo.fugue.pro" ||
		response.Routes[0].PathPrefix != "/" ||
		response.Routes[0].RequestCount != 4 ||
		response.Routes[1].Hostname != "demo.fugue.pro" ||
		response.Routes[1].PathPrefix != "/api" ||
		response.Routes[1].RequestCount != 2 {
		t.Fatalf("unexpected route summaries: %+v", response.Routes)
	}
}

func TestEdgeHeartbeatDiscoversPublicEndpointFromClusterNode(t *testing.T) {
	t.Parallel()

	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{
		node: model.ClusterNode{
			Name:       "vps-591f4447",
			Region:     "north-america",
			InternalIP: "100.64.0.10",
			ExternalIP: "100.64.0.10",
			PublicIP:   "15.204.94.71",
		},
		countryCode: "US",
	}})

	heartbeat := httptest.NewRecorder()
	body := map[string]any{
		"edge_id":              "vps-591f4447",
		"edge_group_id":        "edge-group-country-us",
		"route_bundle_version": "routegen_first",
		"caddy_route_count":    3,
		"status":               model.EdgeHealthHealthy,
		"healthy":              true,
		"draining":             false,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(heartbeat, req)
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}
	var heartbeatResponse struct {
		Node model.EdgeNode `json:"node"`
	}
	mustDecodeJSON(t, heartbeat, &heartbeatResponse)
	if heartbeatResponse.Node.PublicIPv4 != "15.204.94.71" ||
		heartbeatResponse.Node.Region != "north-america" ||
		heartbeatResponse.Node.Country != "us" ||
		heartbeatResponse.Node.MeshIP != "100.64.0.10" {
		t.Fatalf("expected cluster node endpoint discovery, got %+v", heartbeatResponse.Node)
	}
}

func edgeQualityFloatClose(got, want float64) bool {
	return math.Abs(got-want) < 0.000001
}

func TestScopedEdgeTokenRestrictsEdgeGroupAccess(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	_, token, err := storeState.CreateEdgeNodeToken(model.EdgeNode{
		ID:          "edge-us-1",
		EdgeGroupID: "edge-group-country-us",
		Status:      model.EdgeHealthUnknown,
	})
	if err != nil {
		t.Fatalf("create edge node token: %v", err)
	}

	forbiddenRoutes := httptest.NewRecorder()
	forbiddenRoutesReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token="+token+"&edge_group_id=edge-group-country-hk", nil)
	server.Handler().ServeHTTP(forbiddenRoutes, forbiddenRoutesReq)
	if forbiddenRoutes.Code != http.StatusForbidden {
		t.Fatalf("expected scoped token group mismatch to be forbidden, got %d body=%s", forbiddenRoutes.Code, forbiddenRoutes.Body.String())
	}

	allowedRoutes := httptest.NewRecorder()
	allowedRoutesReq := httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token="+token, nil)
	server.Handler().ServeHTTP(allowedRoutes, allowedRoutesReq)
	if allowedRoutes.Code != http.StatusOK {
		t.Fatalf("expected scoped token default route request to succeed, got %d body=%s", allowedRoutes.Code, allowedRoutes.Body.String())
	}
	var bundle model.EdgeRouteBundle
	mustDecodeJSON(t, allowedRoutes, &bundle)
	if bundle.EdgeID != "edge-us-1" || bundle.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected scoped token to fill edge selector, got %+v", bundle)
	}

	sameGroupRoutes := httptest.NewRecorder()
	allowedRoutesReq = httptest.NewRequest(http.MethodGet, "/v1/edge/routes?token="+token+"&edge_id=edge-us-2&edge_group_id=edge-group-country-us", nil)
	server.Handler().ServeHTTP(sameGroupRoutes, allowedRoutesReq)
	if sameGroupRoutes.Code != http.StatusOK {
		t.Fatalf("expected scoped token same-group route request to succeed, got %d body=%s", sameGroupRoutes.Code, sameGroupRoutes.Body.String())
	}
	mustDecodeJSON(t, sameGroupRoutes, &bundle)
	if bundle.EdgeID != "edge-us-2" || bundle.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected scoped token to allow same-group edge selector, got %+v", bundle)
	}

	allowedHeartbeat := httptest.NewRecorder()
	sameGroupBody, _ := json.Marshal(map[string]any{
		"edge_id":       "edge-us-2",
		"edge_group_id": "edge-group-country-us",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
		"draining":      false,
	})
	allowedHeartbeatReq := httptest.NewRequest(http.MethodPost, "/v1/edge/heartbeat?token="+token, bytes.NewReader(sameGroupBody))
	allowedHeartbeatReq.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(allowedHeartbeat, allowedHeartbeatReq)
	if allowedHeartbeat.Code != http.StatusOK {
		t.Fatalf("expected scoped token same-group heartbeat to succeed, got %d body=%s", allowedHeartbeat.Code, allowedHeartbeat.Body.String())
	}

	forbiddenHeartbeat := httptest.NewRecorder()
	mismatchBody, _ := json.Marshal(map[string]any{
		"edge_id":       "edge-hk-1",
		"edge_group_id": "edge-group-country-hk",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
		"draining":      false,
	})
	forbiddenHeartbeatReq := httptest.NewRequest(http.MethodPost, "/v1/edge/heartbeat?token="+token, bytes.NewReader(mismatchBody))
	forbiddenHeartbeatReq.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(forbiddenHeartbeat, forbiddenHeartbeatReq)
	if forbiddenHeartbeat.Code != http.StatusForbidden {
		t.Fatalf("expected scoped token group mismatch to be forbidden, got %d body=%s", forbiddenHeartbeat.Code, forbiddenHeartbeat.Body.String())
	}
}
