package api

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
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
				"method":                  "post",
				"traffic_class":           "large_body_api",
				"client_asn":              "AS123",
				"runtime_region":          "us",
				"ttfb_ms":                 120,
				"upstream_ms":             80,
				"total_ms":                140,
				"status_code":             200,
				"sample_count":            4,
				"cache_hit_count":         3,
				"cache_observation_count": 4,
				"upload_request_count":    4,
				"body_read_block_ms":      320,
				"upload_effective_bps":    131072,
				"max_read_gap_ms":         1500,
				"request_body_bytes":      4096,
				"request_body_read_bytes": 2048,
				"body_incomplete_count":   1,
				"response_egress_bps":     524288,
				"origin_ttfb_ms":          88,
				"active_requests":         2,
				"active_body_buffers":     1,
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
		sample.Method != "POST" ||
		sample.TrafficClass != "large_body_api" ||
		sample.ClientCountry != "" ||
		sample.ClientRegion != "" ||
		sample.ClientASN != "as123" ||
		sample.SampleCount != 4 ||
		sample.CacheHitCount != 3 ||
		sample.CacheObservationCount != 4 ||
		sample.UploadRequestCount != 4 ||
		sample.UploadEffectiveBPS != 131072 ||
		sample.MaxReadGapMS != 1500 ||
		sample.RequestBodyReadBytes != 2048 ||
		sample.BodyIncompleteCount != 1 ||
		sample.ResponseEgressBPS != 524288 ||
		sample.OriginTTFBMS != 88 ||
		sample.ActiveBodyBuffers != 1 {
		t.Fatalf("unexpected persisted sample: %+v", sample)
	}
}

func TestEdgeQualityRankUsesScopedTrafficClassAndServiceExclusion(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	for _, node := range []map[string]any{
		{
			"edge_id":              "edge-us-1",
			"edge_group_id":        "edge-group-country-us",
			"region":               "us-east",
			"country":              "US",
			"public_ipv4":          "203.0.113.10",
			"route_bundle_version": "routegen",
			"caddy_route_count":    4,
			"tls_status":           model.EdgeTLSStatusReady,
			"status":               model.EdgeHealthHealthy,
			"healthy":              true,
		},
		{
			"edge_id":              "edge-de-1",
			"edge_group_id":        "edge-group-country-de",
			"region":               "eu-central",
			"country":              "DE",
			"public_ipv4":          "203.0.113.20",
			"route_bundle_version": "routegen",
			"caddy_route_count":    4,
			"tls_status":           model.EdgeTLSStatusReady,
			"status":               model.EdgeHealthHealthy,
			"healthy":              true,
		},
	} {
		heartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", "", node)
		if heartbeat.Code != http.StatusOK {
			t.Fatalf("expected heartbeat status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
		}
	}
	if _, err := storeState.PutEdgeRoutePolicy(model.EdgeRoutePolicy{
		Hostname:        "api.fugue.pro",
		AppID:           "app-api",
		TenantID:        "tenant-api",
		RoutePolicy:     model.EdgeRoutePolicyEnabled,
		ExcludedEdgeIDs: []string{"edge-de-1"},
		ExclusionReason: "test exclusion",
	}); err != nil {
		t.Fatalf("put edge route policy: %v", err)
	}
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:                   "api-us-large-body-fast",
			EdgeID:               "edge-us-1",
			EdgeGroupID:          "edge-group-country-us",
			Hostname:             "api.fugue.pro",
			PathPrefix:           "/api",
			Method:               "POST",
			TrafficClass:         "large_body_api",
			ClientCountry:        "cn",
			ClientASN:            "as4134",
			DNSPolicy:            "client_scope_header",
			TTFBMS:               110,
			UpstreamMS:           80,
			TotalMS:              140,
			SampleCount:          60,
			UploadEffectiveBPS:   2 * 1024 * 1024,
			MinWindowBPS:         1536 * 1024,
			RequestBodyBytes:     2 * 1024 * 1024,
			RequestBodyReadBytes: 2 * 1024 * 1024,
			BodyReadBlockMS:      40,
			MaxReadGapMS:         100,
			ClientTCPRTTMS:       120,
			ClientTCPRetransRate: 0.01,
			ClientTCPRTORate:     0.0,
			ClientTCPDeliveryBPS: 4 * 1024 * 1024,
			SampledAt:            now.Add(-5 * time.Minute),
		},
		{
			ID:                        "api-de-large-body-slow",
			EdgeID:                    "edge-de-1",
			EdgeGroupID:               "edge-group-country-de",
			Hostname:                  "api.fugue.pro",
			PathPrefix:                "/api",
			Method:                    "POST",
			TrafficClass:              "large_body_api",
			ClientCountry:             "cn",
			ClientASN:                 "as4134",
			DNSPolicy:                 "client_scope_header",
			TTFBMS:                    130,
			UpstreamMS:                90,
			TotalMS:                   180,
			SampleCount:               60,
			UploadEffectiveBPS:        64 * 1024,
			MinWindowBPS:              32 * 1024,
			RequestBodyBytes:          2 * 1024 * 1024,
			RequestBodyReadBytes:      2 * 1024 * 1024,
			BodyReadBlockMS:           1200,
			MaxReadGapMS:              8000,
			BodyIncompleteCount:       3,
			ClientTCPRTTMS:            280,
			ClientTCPRTTVarMS:         90,
			ClientTCPRetransRate:      0.15,
			ClientTCPBytesRetransRate: 0.10,
			ClientTCPRTORate:          0.08,
			ClientTCPDeliveryBPS:      64 * 1024,
			SampledAt:                 now.Add(-5 * time.Minute),
		},
		{
			ID:                   "api-us-huge-body-slow",
			EdgeID:               "edge-us-1",
			EdgeGroupID:          "edge-group-country-us",
			Hostname:             "api.fugue.pro",
			PathPrefix:           "/api",
			Method:               "POST",
			TrafficClass:         "large_body_api",
			ClientCountry:        "cn",
			ClientASN:            "as4134",
			DNSPolicy:            "client_scope_header",
			TTFBMS:               300,
			UpstreamMS:           220,
			TotalMS:              420,
			SampleCount:          60,
			UploadEffectiveBPS:   16 * 1024,
			MinWindowBPS:         8 * 1024,
			RequestBodyBytes:     32 * 1024 * 1024,
			RequestBodyReadBytes: 32 * 1024 * 1024,
			BodyReadBlockMS:      5000,
			MaxReadGapMS:         12000,
			SampledAt:            now.Add(-5 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record performance samples: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/edge/quality-rank/api.fugue.pro?traffic_class=large_body_api&request_size_class=body_1m_16m&method=POST&path_prefix=/api/responses&scope=asn:as4134&window=6h", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected quality rank status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.EdgeQualityRankResponse
	mustDecodeJSON(t, recorder, &response)
	if response.SelectedScope != "asn:as4134" || response.TrafficClass != "large_body_api" || response.RequestSizeClass != "body_1m_16m" || response.Method != "POST" {
		t.Fatalf("unexpected quality rank scope/filter: %+v", response)
	}
	if len(response.Candidates) != 1 || response.Candidates[0].EdgeID != "edge-us-1" || response.Candidates[0].Score <= 0 {
		t.Fatalf("expected non-excluded US edge to rank first, got %+v", response.Candidates)
	}
	if response.Candidates[0].ScoreBreakdown["upload"] <= 0 || response.Candidates[0].Confidence <= 0 {
		t.Fatalf("expected upload/confidence breakdown, got %+v", response.Candidates[0])
	}
	if response.Candidates[0].AvgUploadBPS < 1024*1024 {
		t.Fatalf("expected huge-body slow sample to be filtered out by request_size_class, got %+v", response.Candidates[0])
	}
	if len(response.HardGated) != 1 || response.HardGated[0].EdgeID != "edge-de-1" || !response.HardGated[0].Excluded {
		t.Fatalf("expected service-excluded DE edge in hard gates, got %+v", response.HardGated)
	}
}

func TestEdgeQualityShadowComparisonReportsChangedWinner(t *testing.T) {
	t.Parallel()

	comparison := edgeQualityShadowComparison(model.EdgeRoutePolicy{EdgeGroupID: "edge-group-country-de"}, []model.EdgeQualityRankCandidate{
		{Rank: 1, EdgeID: "edge-us-1", EdgeGroupID: "edge-group-country-us", Score: 100},
		{Rank: 2, EdgeID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Score: 300},
	})
	if comparison == nil ||
		!comparison.Changed ||
		comparison.LegacySelectedEdgeGroupID != "edge-group-country-de" ||
		comparison.QualitySelectedEdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expected changed shadow comparison, got %+v", comparison)
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
				"method":                  "POST",
				"traffic_class":           "large_body_api",
				"tls_handshake_ms":        10,
				"ttfb_ms":                 120,
				"upstream_ms":             80,
				"total_ms":                140,
				"sample_count":            4,
				"cache_hit_count":         3,
				"cache_observation_count": 4,
				"error_count":             1,
				"upload_effective_bps":    128 * 1024,
				"min_window_bps":          96 * 1024,
				"body_read_block_ms":      200,
				"max_read_gap_ms":         1000,
				"body_incomplete_count":   1,
				"response_egress_bps":     512 * 1024,
				"response_write_ms":       8,
				"origin_ttfb_ms":          90,
				"origin_total_ms":         110,
				"active_requests":         2,
				"active_body_buffers":     1,
				"sampled_at":              now.Add(-2 * time.Hour),
			},
			{
				"id":                      "edge-us-sample-2",
				"hostname":                "Demo.Fugue.Pro",
				"path_prefix":             "/api",
				"method":                  "GET",
				"traffic_class":           "html_dynamic",
				"tls_handshake_ms":        20,
				"ttfb_ms":                 200,
				"upstream_ms":             150,
				"total_ms":                240,
				"sample_count":            2,
				"cache_hit_count":         1,
				"cache_observation_count": 2,
				"error_count":             0,
				"upload_effective_bps":    256 * 1024,
				"body_read_block_ms":      80,
				"max_read_gap_ms":         200,
				"response_egress_bps":     768 * 1024,
				"response_write_ms":       4,
				"origin_ttfb_ms":          130,
				"origin_total_ms":         170,
				"active_requests":         1,
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
		!edgeQualityFloatClose(summary.AvgTLSHandshakeMS, (10.0*4.0+20.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgUploadBPS, (128.0*1024.0*4.0+256.0*1024.0*2.0)/6.0) ||
		summary.MinUploadBPS != 96*1024 ||
		!edgeQualityFloatClose(summary.AvgBodyReadMS, (200.0*4.0+80.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgMaxReadGapMS, (1000.0*4.0+200.0*2.0)/6.0) ||
		summary.BodyIncompleteCount != 1 ||
		!edgeQualityFloatClose(summary.AvgResponseEgressBPS, (512.0*1024.0*4.0+768.0*1024.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgOriginTTFBMS, (90.0*4.0+130.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgActiveRequests, (2.0*4.0+1.0*2.0)/6.0) ||
		!edgeQualityFloatClose(summary.AvgActiveBodyBuffers, (1.0*4.0)/6.0) {
		t.Fatalf("unexpected quality rates/averages: %+v", summary)
	}
	if len(response.Routes) != 2 {
		t.Fatalf("expected two route summaries, got %+v", response.Routes)
	}
	if response.Routes[0].Hostname != "demo.fugue.pro" ||
		response.Routes[0].PathPrefix != "/" ||
		response.Routes[0].Method != "POST" ||
		response.Routes[0].TrafficClass != "large_body_api" ||
		response.Routes[0].RequestCount != 4 ||
		response.Routes[0].MinUploadBPS != 96*1024 ||
		response.Routes[1].Hostname != "demo.fugue.pro" ||
		response.Routes[1].PathPrefix != "/api" ||
		response.Routes[1].Method != "GET" ||
		response.Routes[1].TrafficClass != "html_dynamic" ||
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

func TestEdgeNodeDesiredStateAndControlEndpoints(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	_, token, err := storeState.CreateEdgeNodeToken(model.EdgeNode{
		ID:           "edge-jp-1",
		EdgeGroupID:  "edge-group-country-jp",
		WorkloadMode: model.EdgeWorkloadModeDynamic,
		CanaryState:  model.EdgeCanaryStateJoined,
		Region:       "asia",
		Country:      "jp",
		PublicIPv4:   "203.0.113.44",
		Status:       model.EdgeHealthUnknown,
	})
	if err != nil {
		t.Fatalf("create edge token: %v", err)
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:              "edge-jp-1",
		EdgeGroupID:     "edge-group-country-jp",
		WorkloadMode:    model.EdgeWorkloadModeDynamic,
		Region:          "asia",
		Country:         "jp",
		PublicIPv4:      "203.0.113.44",
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		CaddyRouteCount: 2,
		TLSStatus:       model.EdgeTLSStatusReady,
	}); err != nil {
		t.Fatalf("record dynamic edge heartbeat: %v", err)
	}

	desiredRecorder := httptest.NewRecorder()
	desiredReq := httptest.NewRequest(http.MethodGet, "/v1/edge/nodes/edge-jp-1/desired-state?token="+token, nil)
	server.Handler().ServeHTTP(desiredRecorder, desiredReq)
	if desiredRecorder.Code != http.StatusOK {
		t.Fatalf("expected desired-state status %d, got %d body=%s", http.StatusOK, desiredRecorder.Code, desiredRecorder.Body.String())
	}
	var desiredResponse struct {
		DesiredState edgeNodeDesiredStateResponse `json:"desired_state"`
	}
	mustDecodeJSON(t, desiredRecorder, &desiredResponse)
	if desiredResponse.DesiredState.WorkloadMode != model.EdgeWorkloadModeDynamic ||
		desiredResponse.DesiredState.CanaryState != model.EdgeCanaryStateJoined ||
		desiredResponse.DesiredState.DNSEligible ||
		!desiredResponse.DesiredState.RouteReady ||
		!desiredResponse.DesiredState.TLSReady {
		t.Fatalf("unexpected initial desired state: %+v", desiredResponse.DesiredState)
	}

	canary := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/nodes/edge-jp-1/canary", platformAdminKey, map[string]any{
		"state":  model.EdgeCanaryStateCanary,
		"weight": 3,
	})
	if canary.Code != http.StatusOK {
		t.Fatalf("expected canary status %d, got %d body=%s", http.StatusOK, canary.Code, canary.Body.String())
	}
	var canaryResponse struct {
		DesiredState edgeNodeDesiredStateResponse `json:"desired_state"`
	}
	mustDecodeJSON(t, canary, &canaryResponse)
	if canaryResponse.DesiredState.CanaryState != model.EdgeCanaryStateCanary ||
		canaryResponse.DesiredState.CanaryWeight != 3 ||
		!canaryResponse.DesiredState.DNSEligible ||
		canaryResponse.DesiredState.Draining {
		t.Fatalf("unexpected canary desired state: %+v", canaryResponse.DesiredState)
	}

	drain := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/nodes/edge-jp-1/drain", platformAdminKey, nil)
	if drain.Code != http.StatusOK {
		t.Fatalf("expected drain status %d, got %d body=%s", http.StatusOK, drain.Code, drain.Body.String())
	}
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:              "edge-jp-1",
		EdgeGroupID:     "edge-group-country-jp",
		WorkloadMode:    model.EdgeWorkloadModeDynamic,
		Status:          model.EdgeHealthHealthy,
		Healthy:         true,
		Draining:        false,
		CaddyRouteCount: 2,
		TLSStatus:       model.EdgeTLSStatusReady,
	}); err != nil {
		t.Fatalf("heartbeat after drain: %v", err)
	}
	adminDesired := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/nodes/edge-jp-1/desired-state", platformAdminKey, nil)
	if adminDesired.Code != http.StatusOK {
		t.Fatalf("expected admin desired-state status %d, got %d body=%s", http.StatusOK, adminDesired.Code, adminDesired.Body.String())
	}
	var adminDesiredResponse struct {
		DesiredState edgeNodeDesiredStateResponse `json:"desired_state"`
	}
	mustDecodeJSON(t, adminDesired, &adminDesiredResponse)
	if !adminDesiredResponse.DesiredState.Draining ||
		adminDesiredResponse.DesiredState.CanaryState != model.EdgeCanaryStateDrained ||
		adminDesiredResponse.DesiredState.DNSEligible {
		t.Fatalf("expected drained desired state to survive heartbeat, got %+v", adminDesiredResponse.DesiredState)
	}

	undrain := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/nodes/edge-jp-1/undrain", platformAdminKey, nil)
	if undrain.Code != http.StatusOK {
		t.Fatalf("expected undrain status %d, got %d body=%s", http.StatusOK, undrain.Code, undrain.Body.String())
	}
	var undrainResponse struct {
		DesiredState edgeNodeDesiredStateResponse `json:"desired_state"`
	}
	mustDecodeJSON(t, undrain, &undrainResponse)
	if undrainResponse.DesiredState.Draining ||
		undrainResponse.DesiredState.CanaryState != model.EdgeCanaryStateCanary ||
		undrainResponse.DesiredState.CanaryWeight != 1 ||
		!undrainResponse.DesiredState.DNSEligible {
		t.Fatalf("unexpected undrain desired state: %+v", undrainResponse.DesiredState)
	}
}

func TestAdminProbeEdgeNodeFailsClosedWithoutPublicEndpoint(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:           "edge-missing-public",
		EdgeGroupID:  "edge-group-country-jp",
		WorkloadMode: model.EdgeWorkloadModeDynamic,
		Status:       model.EdgeHealthHealthy,
		Healthy:      true,
	}); err != nil {
		t.Fatalf("record edge heartbeat: %v", err)
	}

	probe := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/nodes/edge-missing-public/probe", platformAdminKey, nil)
	if probe.Code != http.StatusOK {
		t.Fatalf("expected probe status %d, got %d body=%s", http.StatusOK, probe.Code, probe.Body.String())
	}
	var response struct {
		Node         model.EdgeNode               `json:"node"`
		DesiredState edgeNodeDesiredStateResponse `json:"desired_state"`
	}
	mustDecodeJSON(t, probe, &response)
	if response.Node.PublicProbeStatus != model.EdgePublicProbeStatusFailing ||
		!strings.Contains(response.Node.PublicProbeLastError, "missing public") ||
		response.DesiredState.DNSEligible {
		t.Fatalf("expected missing-public probe to fail closed, got node=%+v desired=%+v", response.Node, response.DesiredState)
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
