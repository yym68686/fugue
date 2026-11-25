package api

import (
	"context"
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestTenantSecurityBoundaries(t *testing.T) {
	state, server, _, app := setupAppConfigTestServer(t, model.AppSpec{Image: "ghcr.io/example/demo:1", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared"})
	_, key, err := state.CreateAPIKey(app.TenantID, "workspace", []string{"billing.write", "runtime.attach", "app.read"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path string
		body         map[string]any
	}{
		{"POST", "/v1/billing/top-ups", map[string]any{"amount_cents": 100}},
		{"PATCH", "/v1/billing", map[string]any{"managed_cap": map[string]int{"cpu_millicores": 2000}}},
		{"POST", "/v1/node-keys", map[string]any{"scope": "platform-node"}},
		{"PUT", "/v1/tenants/" + app.TenantID + "/access-hold", map[string]any{"held": false}},
	} {
		res := performJSONRequest(t, server, tc.method, tc.path, key, tc.body)
		if res.Code != http.StatusForbidden {
			t.Fatalf("%s %s: got %d", tc.method, tc.path, res.Code)
		}
	}
	_, nodeSecret, err := state.CreateNodeKey(app.TenantID, "tenant node")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetTenantAccessHold(context.Background(), app.TenantID, true); err != nil {
		t.Fatal(err)
	}
	if res := performJSONRequest(t, server, "GET", "/v1/apps/"+app.ID, key, nil); res.Code != http.StatusUnauthorized {
		t.Fatalf("held credential returned %d", res.Code)
	}
	if _, err := state.AuthenticateNodeKey(nodeSecret); err == nil {
		t.Fatal("held node key accepted")
	}
	if _, _, _, err := state.BootstrapNode(nodeSecret, "worker", "", nil, "", ""); err == nil {
		t.Fatal("held tenant node registration accepted")
	}
	if err := state.SetTenantAccessHold(context.Background(), app.TenantID, false); err != nil {
		t.Fatal(err)
	}
	if res := performJSONRequest(t, server, "GET", "/v1/apps/"+app.ID, key, nil); res.Code != http.StatusOK {
		t.Fatalf("released workspace returned %d", res.Code)
	}
}
