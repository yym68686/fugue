package api

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestAgentHeartbeatPersistsCellSnapshotLabels(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Cell Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	runtimeObj, runtimeKey, err := func() (model.Runtime, string, error) {
		_, runtimeObj, runtimeKey, err := stateStore.BootstrapNode(nodeSecret, "worker-1", "https://old.example.com", map[string]string{"user": "kept"}, "worker-1", "fingerprint-1")
		return runtimeObj, runtimeKey, err
	}()
	if err != nil {
		t.Fatalf("bootstrap node: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/agent/heartbeat", runtimeKey, map[string]any{
		"cell_snapshot": runtime.CellSnapshot{
			RuntimeID:         runtimeObj.ID,
			RuntimeName:       "worker-1",
			MachineName:       "worker-1",
			Endpoint:          "100.64.0.10",
			ObservedPeerCount: 1,
			OutboxPending:     2,
			ObservedAt:        time.Date(2026, 4, 28, 1, 2, 3, 0, time.UTC),
			Mesh: runtime.CellMesh{
				Provider:        "tailscale",
				IP:              "100.64.0.10",
				Hostname:        "fugue-worker-1",
				DiscoverySource: "tailscale",
			},
			Peers: []runtime.CellPeer{{Hostname: "fugue-worker-2", IP: "100.64.0.11", Online: true}},
		},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	updated, err := stateStore.GetRuntime(runtimeObj.ID)
	if err != nil {
		t.Fatalf("get runtime: %v", err)
	}
	if updated.Endpoint != "100.64.0.10" {
		t.Fatalf("expected endpoint to follow cell snapshot, got %q", updated.Endpoint)
	}
	if updated.Labels["user"] != "kept" {
		t.Fatalf("expected user label to be preserved, got %+v", updated.Labels)
	}
	if updated.Labels[runtime.CellRuntimeLabelMeshIP] != "100.64.0.10" {
		t.Fatalf("expected mesh ip label, got %+v", updated.Labels)
	}
	if updated.Labels[runtime.CellRuntimeLabelPeerCount] != "1" {
		t.Fatalf("expected peer count label, got %+v", updated.Labels)
	}
	if updated.Labels[runtime.CellRuntimeLabelObservedPeers] != "1" {
		t.Fatalf("expected observed peer count label, got %+v", updated.Labels)
	}
	if updated.Labels[runtime.CellRuntimeLabelOutboxPending] != "2" {
		t.Fatalf("expected outbox label, got %+v", updated.Labels)
	}
}
