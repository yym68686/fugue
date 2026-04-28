package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestAgentCompletionOutboxReplaysAfterControlPlaneRecovery(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	desiredSpec := model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_external",
	}
	task := AgentTask{
		Operation: model.Operation{
			ID:                "op_deploy_1",
			TenantID:          "tenant_1",
			Type:              model.OperationTypeDeploy,
			Status:            model.OperationStatusWaitingAgent,
			ExecutionMode:     model.ExecutionModeAgent,
			AppID:             "app_1",
			TargetRuntimeID:   "runtime_external",
			AssignedRuntimeID: "runtime_external",
			DesiredSpec:       &desiredSpec,
			CreatedAt:         now,
			UpdatedAt:         now,
		},
		App: model.App{
			ID:        "app_1",
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			Name:      "demo",
			Spec:      desiredSpec,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	completeCalls := 0
	completed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/agent/operations":
			w.Header().Set("Content-Type", "application/json")
			tasks := []AgentTask{task}
			if completed {
				tasks = nil
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"tasks": tasks}); err != nil {
				t.Fatalf("encode tasks: %v", err)
			}
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/agent/operations/"):
			completeCalls++
			if completeCalls == 1 {
				http.Error(w, "control plane unavailable", http.StatusServiceUnavailable)
				return
			}
			completed = true
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"operation": task.Operation}); err != nil {
				t.Fatalf("encode operation: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	cellStore, err := OpenCellStore(filepath.Join(tempDir, "cell-store.json"))
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}
	service := NewAgentService(config.AgentConfig{
		ServerURL:        server.URL,
		RuntimeKey:       "runtime_key",
		RuntimeID:        "runtime_external",
		WorkDir:          filepath.Join(tempDir, "work"),
		CellStorePath:    filepath.Join(tempDir, "cell-store.json"),
		ApplyWithKubectl: false,
	}, log.New(io.Discard, "", 0))
	service.CellStore = cellStore

	if err := service.pollAndProcess(context.Background()); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if completeCalls != 1 {
		t.Fatalf("expected first completion attempt, got %d", completeCalls)
	}
	pending, err := cellStore.CountPendingCompletions()
	if err != nil {
		t.Fatalf("count pending completions: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected one pending completion, got %d", pending)
	}
	cellStoreData, err := os.ReadFile(filepath.Join(tempDir, "cell-store.json"))
	if err != nil {
		t.Fatalf("read cell store file: %v", err)
	}
	if strings.Contains(string(cellStoreData), "nginx:1.27") || strings.Contains(string(cellStoreData), "desired_spec") {
		t.Fatalf("cell store persisted raw desired task payload: %s", string(cellStoreData))
	}

	if err := service.pollAndProcess(context.Background()); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if completeCalls != 1 {
		t.Fatalf("expected pending completion to avoid duplicate local apply before backoff, got %d calls", completeCalls)
	}

	cellStore.mu.Lock()
	if len(cellStore.state.Outbox) != 1 {
		t.Fatalf("expected one outbox event, got %d", len(cellStore.state.Outbox))
	}
	cellStore.state.Outbox[0].NextAttemptAt = time.Now().UTC().Add(-time.Second)
	cellStore.mu.Unlock()

	if err := service.pollAndProcess(context.Background()); err != nil {
		t.Fatalf("third poll: %v", err)
	}
	if completeCalls != 2 {
		t.Fatalf("expected replayed completion after control plane recovery, got %d calls", completeCalls)
	}
	pending, err = cellStore.CountPendingCompletions()
	if err != nil {
		t.Fatalf("count pending completions after replay: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected completion outbox to drain, got %d", pending)
	}
}

func TestAgentCellSnapshotCachesMeshAndRoutes(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	cellStore, err := OpenCellStore(filepath.Join(tempDir, "cell-store.json"))
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}
	service := NewAgentService(config.AgentConfig{
		RuntimeKey:       "runtime_key",
		RuntimeID:        "runtime_external",
		RuntimeName:      "worker-1",
		MachineName:      "worker-1",
		WorkDir:          filepath.Join(tempDir, "work"),
		CellStorePath:    filepath.Join(tempDir, "cell-store.json"),
		ApplyWithKubectl: false,
	}, log.New(io.Discard, "", 0))
	service.CellStore = cellStore
	service.CommandRunner = func(_ context.Context, name string, args ...string) ([]byte, error) {
		command := name + " " + strings.Join(args, " ")
		switch command {
		case "tailscale ip -4":
			return []byte("100.64.0.10\n"), nil
		case "tailscale status --json":
			return []byte(`{
				"Self": {"HostName": "fugue-worker-1", "TailscaleIPs": ["100.64.0.10"]},
				"Peer": {
					"node-key": {"HostName": "fugue-worker-2", "TailscaleIPs": ["100.64.0.11"], "Online": true, "LastSeen": "2026-04-28T00:00:00Z"}
				}
			}`), nil
		default:
			return nil, fmt.Errorf("unexpected command %s", command)
		}
	}

	now := time.Now().UTC()
	desiredSpec := model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_external",
	}
	task := AgentTask{
		Operation: model.Operation{
			ID:                "op_deploy_route",
			TenantID:          "tenant_1",
			Type:              model.OperationTypeDeploy,
			Status:            model.OperationStatusWaitingAgent,
			ExecutionMode:     model.ExecutionModeAgent,
			AppID:             "app_1",
			TargetRuntimeID:   "runtime_external",
			AssignedRuntimeID: "runtime_external",
			DesiredSpec:       &desiredSpec,
			CreatedAt:         now,
			UpdatedAt:         now,
		},
		App: model.App{
			ID:        "app_1",
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			Name:      "demo",
			Route: &model.AppRoute{
				Hostname:    "demo.apps.example.com",
				ServicePort: 8080,
			},
			Spec:      desiredSpec,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	if err := cellStore.RecordDesiredTask(task); err != nil {
		t.Fatalf("record desired task: %v", err)
	}

	snapshot, err := service.RefreshCellSnapshot(context.Background())
	if err != nil {
		t.Fatalf("refresh snapshot: %v", err)
	}
	if snapshot.Mesh.Provider != "tailscale" || snapshot.Mesh.IP != "100.64.0.10" {
		t.Fatalf("expected tailscale mesh snapshot, got %+v", snapshot.Mesh)
	}
	if len(snapshot.Peers) != 1 || snapshot.Peers[0].IP != "100.64.0.11" {
		t.Fatalf("expected one mesh peer, got %+v", snapshot.Peers)
	}
	if snapshot.RouteCount != 1 {
		t.Fatalf("expected one cached route, got %d", snapshot.RouteCount)
	}
	routes, err := cellStore.ListRoutes()
	if err != nil {
		t.Fatalf("list routes: %v", err)
	}
	if len(routes) != 1 || routes[0].Hostname != "demo.apps.example.com" {
		t.Fatalf("expected cached app route, got %+v", routes)
	}
}

func TestCellHTTPHandlerExposesSystemState(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	cellStore, err := OpenCellStore(filepath.Join(tempDir, "cell-store.json"))
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}
	service := NewAgentService(config.AgentConfig{
		RuntimeID:        "runtime_external",
		RuntimeName:      "worker-1",
		WorkDir:          filepath.Join(tempDir, "work"),
		CellStorePath:    filepath.Join(tempDir, "cell-store.json"),
		ApplyWithKubectl: false,
	}, log.New(io.Discard, "", 0))
	service.CellStore = cellStore
	service.CommandRunner = func(_ context.Context, name string, args ...string) ([]byte, error) {
		command := name + " " + strings.Join(args, " ")
		if command == "tailscale ip -4" {
			return []byte("100.64.0.10\n"), nil
		}
		if command == "tailscale status --json" {
			return []byte(`{"Self":{"HostName":"worker-1","TailscaleIPs":["100.64.0.10"]},"Peer":{}}`), nil
		}
		return nil, fmt.Errorf("unexpected command %s", command)
	}

	now := time.Now().UTC()
	desiredSpec := model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_external",
	}
	task := AgentTask{
		Operation: model.Operation{
			ID:                "op_deploy_route",
			TenantID:          "tenant_1",
			Type:              model.OperationTypeDeploy,
			Status:            model.OperationStatusWaitingAgent,
			ExecutionMode:     model.ExecutionModeAgent,
			AppID:             "app_1",
			TargetRuntimeID:   "runtime_external",
			AssignedRuntimeID: "runtime_external",
			DesiredSpec:       &desiredSpec,
			CreatedAt:         now,
			UpdatedAt:         now,
		},
		App: model.App{
			ID:        "app_1",
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			Name:      "demo",
			Route: &model.AppRoute{
				Hostname:    "demo.apps.example.com",
				ServicePort: 8080,
			},
			Spec:      desiredSpec,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	if err := cellStore.RecordDesiredTask(task); err != nil {
		t.Fatalf("record desired task: %v", err)
	}
	if _, err := service.RefreshCellSnapshot(context.Background()); err != nil {
		t.Fatalf("refresh snapshot: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/cell/bundle", nil)
	request.RemoteAddr = "127.0.0.1:10000"
	recorder := httptest.NewRecorder()
	service.cellHTTPHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Snapshot CellSnapshot `json:"snapshot"`
		Routes   []CellRoute  `json:"routes"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode cell bundle: %v", err)
	}
	if payload.Snapshot.Mesh.IP != "100.64.0.10" {
		t.Fatalf("expected mesh ip in snapshot, got %+v", payload.Snapshot.Mesh)
	}
	if len(payload.Routes) != 1 || payload.Routes[0].Hostname != "demo.apps.example.com" {
		t.Fatalf("expected cached route, got %+v", payload.Routes)
	}
	if strings.Contains(recorder.Body.String(), "desired_tasks") {
		t.Fatalf("cell bundle leaked desired task state: %s", recorder.Body.String())
	}
}

func TestRefreshCellPeersStoresPeerSnapshot(t *testing.T) {
	t.Parallel()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/cell/snapshot" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode(CellSnapshot{
			RuntimeID:   "runtime_peer",
			RuntimeName: "worker-2",
			Endpoint:    "100.64.0.11",
			Mesh: CellMesh{
				Provider:        "tailscale",
				IP:              "100.64.0.11",
				Hostname:        "worker-2",
				DiscoverySource: "tailscale",
			},
			ObservedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("encode peer snapshot: %v", err)
		}
	}))
	defer peerServer.Close()

	host, portRaw, err := net.SplitHostPort(strings.TrimPrefix(peerServer.URL, "http://"))
	if err != nil {
		t.Fatalf("split peer server address: %v", err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Fatalf("parse peer server port: %v", err)
	}

	tempDir := t.TempDir()
	cellStore, err := OpenCellStore(filepath.Join(tempDir, "cell-store.json"))
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}
	service := NewAgentService(config.AgentConfig{
		RuntimeID:         "runtime_external",
		WorkDir:           filepath.Join(tempDir, "work"),
		CellStorePath:     filepath.Join(tempDir, "cell-store.json"),
		CellPeerProbe:     true,
		CellPeerProbePort: port,
		ApplyWithKubectl:  false,
	}, log.New(io.Discard, "", 0))
	service.CellStore = cellStore

	err = service.RefreshCellPeers(context.Background(), CellSnapshot{
		Mesh: CellMesh{IP: "100.64.0.10"},
		Peers: []CellPeer{{
			Hostname: "worker-2",
			IP:       host,
			Online:   true,
		}},
	})
	if err != nil {
		t.Fatalf("refresh cell peers: %v", err)
	}
	observations, err := cellStore.ListPeerObservations()
	if err != nil {
		t.Fatalf("list peer observations: %v", err)
	}
	if len(observations) != 1 {
		t.Fatalf("expected one peer observation, got %+v", observations)
	}
	if observations[0].Snapshot == nil || observations[0].Snapshot.RuntimeID != "runtime_peer" {
		t.Fatalf("expected stored peer snapshot, got %+v", observations[0])
	}
	count, err := cellStore.CountPeerObservations()
	if err != nil {
		t.Fatalf("count peer observations: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one observed peer, got %d", count)
	}
}
