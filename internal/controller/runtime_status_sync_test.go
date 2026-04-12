package controller

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestMarkRuntimeOfflineStaleSyncsManagedOwnedClusterRuntimeStatuses(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	_, readySecret, err := stateStore.CreateNodeKey(tenant.ID, "ready")
	if err != nil {
		t.Fatalf("create ready node key: %v", err)
	}
	_, readyRuntime, err := stateStore.BootstrapClusterNode(readySecret, "cluster-ready", "https://ready.example.com", nil, "cluster-ready", "cluster-ready")
	if err != nil {
		t.Fatalf("bootstrap ready node: %v", err)
	}
	_, notReadySecret, err := stateStore.CreateNodeKey(tenant.ID, "not-ready")
	if err != nil {
		t.Fatalf("create not-ready node key: %v", err)
	}
	_, notReadyRuntime, err := stateStore.BootstrapClusterNode(notReadySecret, "cluster-not-ready", "https://not-ready.example.com", nil, "cluster-not-ready", "cluster-not-ready")
	if err != nil {
		t.Fatalf("bootstrap not-ready node: %v", err)
	}
	_, missingSecret, err := stateStore.CreateNodeKey(tenant.ID, "missing")
	if err != nil {
		t.Fatalf("create missing node key: %v", err)
	}
	_, missingRuntime, err := stateStore.BootstrapClusterNode(missingSecret, "cluster-missing", "https://missing.example.com", nil, "cluster-missing", "cluster-missing")
	if err != nil {
		t.Fatalf("bootstrap missing node: %v", err)
	}
	readyNodeName := readyRuntime.ClusterNodeName
	if readyNodeName == "" {
		readyNodeName = readyRuntime.Name
	}
	notReadyNodeName := notReadyRuntime.ClusterNodeName
	if notReadyNodeName == "" {
		notReadyNodeName = notReadyRuntime.Name
	}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": readyNodeName},
					"status": map[string]any{
						"conditions": []map[string]any{{"type": "Ready", "status": "True"}},
					},
				},
				{
					"metadata": map[string]any{"name": notReadyNodeName},
					"status": map[string]any{
						"conditions": []map[string]any{{"type": "Ready", "status": "False"}},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode nodes: %v", err)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			KubectlApply:        true,
			RuntimeOfflineAfter: time.Hour,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	if err := svc.markRuntimeOfflineStale(); err != nil {
		t.Fatalf("mark runtime offline stale: %v", err)
	}

	readyRuntime, err = stateStore.GetRuntime(readyRuntime.ID)
	if err != nil {
		t.Fatalf("get ready runtime: %v", err)
	}
	if readyRuntime.Status != model.RuntimeStatusActive {
		t.Fatalf("expected ready runtime active, got %q", readyRuntime.Status)
	}
	notReadyRuntime, err = stateStore.GetRuntime(notReadyRuntime.ID)
	if err != nil {
		t.Fatalf("get not-ready runtime: %v", err)
	}
	if notReadyRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected not-ready runtime offline, got %q", notReadyRuntime.Status)
	}
	missingRuntime, err = stateStore.GetRuntime(missingRuntime.ID)
	if err != nil {
		t.Fatalf("get missing runtime: %v", err)
	}
	if missingRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected missing runtime offline, got %q", missingRuntime.Status)
	}
}
