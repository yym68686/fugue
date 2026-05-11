package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSHeartbeatRegistersInventoryAndAdminList(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	heartbeat := httptest.NewRecorder()
	body := map[string]any{
		"dns_node_id":        "dns-us-1",
		"edge_group_id":      "edge-group-country-us",
		"public_ipv4":        "203.0.113.10",
		"mesh_ip":            "100.64.0.10",
		"zone":               "dns.fugue.pro",
		"dns_bundle_version": "dnsgen_first",
		"record_count":       40,
		"cache_status":       "ready",
		"query_count":        12,
		"query_error_count":  1,
		"udp_addr":           ":53",
		"tcp_addr":           ":53",
		"udp_listen":         true,
		"tcp_listen":         true,
		"status":             model.EdgeHealthHealthy,
		"healthy":            true,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/dns/heartbeat?token=edge-secret", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(heartbeat, req)
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}
	var heartbeatResponse struct {
		Node     model.DNSNode `json:"node"`
		Accepted bool          `json:"accepted"`
	}
	mustDecodeJSON(t, heartbeat, &heartbeatResponse)
	if !heartbeatResponse.Accepted ||
		heartbeatResponse.Node.ID != "dns-us-1" ||
		heartbeatResponse.Node.EdgeGroupID != "edge-group-country-us" ||
		heartbeatResponse.Node.Zone != "dns.fugue.pro" ||
		heartbeatResponse.Node.DNSBundleVersion != "dnsgen_first" ||
		heartbeatResponse.Node.RecordCount != 40 ||
		heartbeatResponse.Node.QueryCount != 12 ||
		heartbeatResponse.Node.LastHeartbeatAt == nil {
		t.Fatalf("unexpected heartbeat response: %+v", heartbeatResponse)
	}

	list := performJSONRequest(t, server, http.MethodGet, "/v1/dns/nodes", platformAdminKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	var listResponse struct {
		Nodes []model.DNSNode `json:"nodes"`
	}
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Nodes) != 1 || listResponse.Nodes[0].ID != "dns-us-1" {
		t.Fatalf("unexpected dns node list: %+v", listResponse.Nodes)
	}

	get := performJSONRequest(t, server, http.MethodGet, "/v1/dns/nodes/dns-us-1", platformAdminKey, nil)
	if get.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, get.Code, get.Body.String())
	}
	var getResponse struct {
		Node model.DNSNode `json:"node"`
	}
	mustDecodeJSON(t, get, &getResponse)
	if getResponse.Node.ID != "dns-us-1" || !getResponse.Node.UDPListen || !getResponse.Node.TCPListen {
		t.Fatalf("unexpected dns node get response: %+v", getResponse)
	}
}

func TestDNSDelegationPreflightPassesWithTwoHealthyNodes(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	for _, node := range []model.DNSNode{
		{
			ID:               "dns-us-1",
			EdgeGroupID:      "edge-group-country-us",
			PublicIPv4:       "203.0.113.10",
			Zone:             "dns.fugue.pro",
			Status:           model.EdgeHealthHealthy,
			Healthy:          true,
			DNSBundleVersion: "dnsgen_us",
			RecordCount:      40,
			CacheStatus:      "ready",
			UDPAddr:          ":53",
			TCPAddr:          ":53",
			UDPListen:        true,
			TCPListen:        true,
			LastSeenAt:       &now,
		},
		{
			ID:               "dns-eu-1",
			EdgeGroupID:      "edge-group-country-de",
			PublicIPv4:       "203.0.113.20",
			Zone:             "dns.fugue.pro",
			Status:           model.EdgeHealthHealthy,
			Healthy:          true,
			DNSBundleVersion: "dnsgen_de",
			RecordCount:      40,
			CacheStatus:      "ready",
			UDPAddr:          ":53",
			TCPAddr:          ":53",
			UDPListen:        true,
			TCPListen:        true,
			LastSeenAt:       &now,
		},
	} {
		if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
			t.Fatalf("update dns heartbeat fixture: %v", err)
		}
	}

	kubeServer := newDNSPreflightKubeServer(t, []string{"dns-us-1", "dns-eu-1"})
	defer kubeServer.Close()
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}
	server.dnsDelegationProbe = func(_ context.Context, node model.DNSNode, _, _ string) dnsDelegationProbeResult {
		return dnsDelegationProbeResult{
			UDP53Reachable: true,
			TCP53Reachable: true,
			ProbeAnswers:   []string{node.PublicIPv4},
		}
	}
	server.dnsParentNSLookup = func(_ context.Context, _ string) ([]string, error) {
		return []string{"current-parent.example"}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/dns/delegation/preflight", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.DNSDelegationPreflightResponse
	mustDecodeJSON(t, recorder, &response)
	if !response.Pass || response.HealthyNodeCount != 2 ||
		!strings.Contains(response.DNSBundleVersion, "edge-group-country-us=dnsgen_us") ||
		!strings.Contains(response.DNSBundleVersion, "edge-group-country-de=dnsgen_de") {
		t.Fatalf("expected passing preflight, got %+v", response)
	}
	if len(response.DelegationPlan.PlannedARecords) != 2 || len(response.DelegationPlan.PlannedNSRecords) != 2 {
		t.Fatalf("expected ns1/ns2 plan, got %+v", response.DelegationPlan)
	}
	if !strings.HasPrefix(response.DelegationPlan.PlannedARecords[0].Name, "ns1.") {
		t.Fatalf("expected ns1 A record first, got %+v", response.DelegationPlan.PlannedARecords)
	}
}

func TestDNSDelegationPreflightFailsWhenSameEdgeGroupReportsDifferentBundleVersions(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	for _, node := range []model.DNSNode{
		{
			ID:               "dns-us-1",
			EdgeGroupID:      "edge-group-country-us",
			PublicIPv4:       "203.0.113.10",
			Zone:             "dns.fugue.pro",
			Status:           model.EdgeHealthHealthy,
			Healthy:          true,
			DNSBundleVersion: "dnsgen_old",
			RecordCount:      40,
			CacheStatus:      "ready",
			UDPAddr:          ":53",
			TCPAddr:          ":53",
			UDPListen:        true,
			TCPListen:        true,
			LastSeenAt:       &now,
		},
		{
			ID:               "dns-us-2",
			EdgeGroupID:      "edge-group-country-us",
			PublicIPv4:       "203.0.113.20",
			Zone:             "dns.fugue.pro",
			Status:           model.EdgeHealthHealthy,
			Healthy:          true,
			DNSBundleVersion: "dnsgen_new",
			RecordCount:      40,
			CacheStatus:      "ready",
			UDPAddr:          ":53",
			TCPAddr:          ":53",
			UDPListen:        true,
			TCPListen:        true,
			LastSeenAt:       &now,
		},
	} {
		if _, err := storeState.UpdateDNSHeartbeat(node); err != nil {
			t.Fatalf("update dns heartbeat fixture: %v", err)
		}
	}

	kubeServer := newDNSPreflightKubeServer(t, []string{"dns-us-1", "dns-us-2"})
	defer kubeServer.Close()
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}
	server.dnsDelegationProbe = func(_ context.Context, node model.DNSNode, _, _ string) dnsDelegationProbeResult {
		return dnsDelegationProbeResult{
			UDP53Reachable: true,
			TCP53Reachable: true,
			ProbeAnswers:   []string{node.PublicIPv4},
		}
	}
	server.dnsParentNSLookup = func(_ context.Context, _ string) ([]string, error) {
		return []string{"current-parent.example"}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/dns/delegation/preflight", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.DNSDelegationPreflightResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Pass {
		t.Fatalf("expected preflight to fail on same-group bundle version mismatch, got %+v", response)
	}
	assertDNSPreflightCheck(t, response.Checks, "dns_bundle_version_stable", false)
}

func TestScopedEdgeTokenRestrictsDNSHeartbeat(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	_, token, err := storeState.CreateEdgeNodeToken(model.EdgeNode{
		ID:          "dns-us-1",
		EdgeGroupID: "edge-group-country-us",
		Status:      model.EdgeHealthUnknown,
	})
	if err != nil {
		t.Fatalf("create edge node token: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"dns_node_id":   "dns-eu-1",
		"edge_group_id": "edge-group-country-us",
		"zone":          "dns.fugue.pro",
		"status":        model.EdgeHealthHealthy,
		"healthy":       true,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/dns/heartbeat?token="+token, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected scoped token node mismatch to be forbidden, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func assertDNSPreflightCheck(t *testing.T, checks []model.DNSDelegationPreflightCheck, name string, pass bool) {
	t.Helper()
	for _, check := range checks {
		if check.Name == name {
			if check.Pass != pass {
				t.Fatalf("expected check %s pass=%v, got %+v", name, pass, check)
			}
			return
		}
	}
	t.Fatalf("expected check %s in %+v", name, checks)
}

func newDNSPreflightKubeServer(t *testing.T, nodeNames []string) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods" {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
			return
		}
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/nodes/") && strings.HasSuffix(r.URL.Path, "/proxy/stats/summary") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"), "/proxy/stats/summary"),
					"cpu":      map[string]any{"usageNanoCores": 100_000_000},
					"memory":   map[string]any{"workingSetBytes": 256 * 1024 * 1024},
					"fs": map[string]any{
						"capacityBytes": 20 * 1024 * 1024 * 1024,
						"usedBytes":     2 * 1024 * 1024 * 1024,
					},
				},
			})
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		items := make([]map[string]any, 0, len(nodeNames))
		for _, name := range nodeNames {
			items = append(items, map[string]any{
				"metadata": map[string]any{
					"name":              name,
					"creationTimestamp": "2026-05-11T00:00:00Z",
					"labels": map[string]string{
						"fugue.io/role.dns":    "true",
						"fugue.io/schedulable": "true",
					},
				},
				"spec": map[string]any{},
				"status": map[string]any{
					"addresses": []map[string]string{
						{"type": "InternalIP", "address": "10.0.0.10"},
					},
					"conditions": []map[string]string{
						{"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "ready", "lastTransitionTime": "2026-05-11T00:01:00Z"},
						{"type": "DiskPressure", "status": "False", "reason": "KubeletHasNoDiskPressure", "message": "ok", "lastTransitionTime": "2026-05-11T00:01:00Z"},
					},
				},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
}
