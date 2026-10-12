package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeInventoryObservationSeparatesAdmissionFromFreshness(t *testing.T) {
	now := time.Now().UTC()
	observed := now.Add(-time.Minute)
	node := model.EdgeNode{ID: "edge-a", EdgeGroupID: "group-a", WorkloadMode: model.EdgeWorkloadModeStatic,
		Healthy: true, Status: model.EdgeHealthHealthy, LastHeartbeatAt: &observed, RouteBundleVersion: "routes",
		CaddyRouteCount: 1, TLSStatus: model.EdgeTLSStatusReady}
	for _, scenario := range []string{"healthy", "stale", "future", "missing", "draining", "unhealthy", "routes", "tls"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := node
			reason := "inventory_checks_passed"
			switch scenario {
			case "stale":
				old := now.Add(-2 * time.Minute)
				candidate.LastHeartbeatAt = &old
				reason = "heartbeat_stale"
			case "future":
				future := now.Add(time.Minute)
				candidate.LastHeartbeatAt = &future
				reason = "observation_from_future"
			case "missing":
				candidate.LastHeartbeatAt = nil
				reason = "observations_missing"
			case "draining":
				candidate.Draining = true
				reason = "draining"
			case "unhealthy":
				candidate.Healthy = false
				reason = "node_unhealthy"
			case "routes":
				candidate.RouteBundleVersion = ""
				candidate.CaddyRouteCount = 0
				reason = "route_state_missing"
			case "tls":
				candidate.TLSStatus = model.EdgeTLSStatusError
				reason = "tls_state_missing"
			}
			result := observeEdgeNodeInventory(candidate, now)
			if result.Reason != reason || result.Eligible != (scenario == "healthy") || !result.AdmissionAllowsDNS || result.RouteProofStatus != "not_evaluated" {
				t.Fatal("inventory observation conflated intent and runtime facts", result)
			}
			if candidate.LastHeartbeatAt != nil && !result.FreshUntil.Equal(candidate.LastHeartbeatAt.Add(90*time.Second)) {
				t.Fatal("observation read renewed freshness")
			}
		})
	}
}

func TestStaleEdgeDiagnosticsAreExplicitAndDoNotUpdateStoredHealth(t *testing.T) {
	storeState, server, _, key, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	stored, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{ID: "edge-a", EdgeGroupID: "group-a", Status: model.EdgeHealthHealthy, Healthy: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/edge/nodes?include_stale=true", "/v1/edge/nodes/edge-a", "/v1/admin/edge/nodes/edge-a/desired-state"} {
		response := performJSONRequest(t, server, http.MethodGet, path, key, nil)
		if response.Code != http.StatusOK {
			t.Fatal(path, response.Code, response.Body.String())
		}
		var decoded map[string]any
		mustDecodeJSON(t, response, &decoded)
		var subject map[string]any
		if rows, exists := decoded["nodes"]; exists {
			subject = rows.([]any)[0].(map[string]any)
		} else if desired, exists := decoded["desired_state"]; exists {
			subject = desired.(map[string]any)
		} else {
			subject = decoded["node"].(map[string]any)
		}
		if subject["observation"].(map[string]any)["route_proof_status"] != "not_evaluated" {
			t.Fatal("diagnostic endpoint invented serving proof")
		}
	}
	if response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/nodes?include_stale=invalid", key, nil); response.Code != http.StatusBadRequest {
		t.Fatal("invalid diagnostic selector accepted", response.Code)
	}
	after, _, err := storeState.GetEdgeNode(stored.ID)
	if err != nil || after.Observation != nil || after.Healthy != stored.Healthy || !after.UpdatedAt.Equal(stored.UpdatedAt) {
		t.Fatal("read-only observation changed stored inventory", err)
	}
}
