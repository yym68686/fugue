package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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

	forbiddenHeartbeat := httptest.NewRecorder()
	mismatchBody, _ := json.Marshal(map[string]any{
		"edge_id":       "edge-hk-1",
		"edge_group_id": "edge-group-country-us",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
		"draining":      false,
	})
	forbiddenHeartbeatReq := httptest.NewRequest(http.MethodPost, "/v1/edge/heartbeat?token="+token, bytes.NewReader(mismatchBody))
	forbiddenHeartbeatReq.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(forbiddenHeartbeat, forbiddenHeartbeatReq)
	if forbiddenHeartbeat.Code != http.StatusForbidden {
		t.Fatalf("expected scoped token edge mismatch to be forbidden, got %d body=%s", forbiddenHeartbeat.Code, forbiddenHeartbeat.Body.String())
	}
}
