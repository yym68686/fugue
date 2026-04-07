package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestCurrentResourceUsageIsAggregatedAcrossNodesForAppsAndBackingServices(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Resource Usage Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  2,
		RuntimeID: tenantSharedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one backing service, got %d", len(app.BackingServices))
	}
	service := app.BackingServices[0]
	namespace := runtime.NamespaceForTenant(tenant.ID)

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "node-a",
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{"type": "Ready", "status": "True"},
							},
						},
					},
					{
						"metadata": map[string]any{
							"name": "node-b",
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{"type": "Ready", "status": "True"},
							},
						},
					},
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "demo-6c8f4f5d4f-a1",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      "demo",
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     app.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "node-a",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":      "demo-6c8f4f5d4f-b2",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      "demo",
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     app.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "node-b",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":      "demo-postgres-7f9d8c6d5d-p1",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:             service.Spec.Postgres.ServiceName,
								runtime.FugueLabelManagedBy:        runtime.FugueLabelManagedByValue,
								runtime.FugueLabelComponent:        "postgres",
								runtime.FugueLabelBackingServiceID: service.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "node-b",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			})
		case "/api/v1/nodes/node-a/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": "node-a",
				},
				"pods": []map[string]any{
					{
						"podRef": map[string]any{
							"name":      "demo-6c8f4f5d4f-a1",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 300_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 256 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 1 * 1024 * 1024 * 1024,
						},
					},
				},
			})
		case "/api/v1/nodes/node-b/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": "node-b",
				},
				"pods": []map[string]any{
					{
						"podRef": map[string]any{
							"name":      "demo-6c8f4f5d4f-b2",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 450_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 384 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 2 * 1024 * 1024 * 1024,
						},
					},
					{
						"podRef": map[string]any{
							"name":      "demo-postgres-7f9d8c6d5d-p1",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 200_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 128 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 4 * 1024 * 1024 * 1024,
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listAppsResponse struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &listAppsResponse)
	if len(listAppsResponse.Apps) != 1 {
		t.Fatalf("expected one app, got %#v", listAppsResponse.Apps)
	}
	assertResourceUsage(t, listAppsResponse.Apps[0].CurrentResourceUsage, 750, 640*1024*1024, 3*1024*1024*1024)
	if len(listAppsResponse.Apps[0].BackingServices) != 1 {
		t.Fatalf("expected one nested backing service, got %#v", listAppsResponse.Apps[0].BackingServices)
	}
	assertResourceUsage(t, listAppsResponse.Apps[0].BackingServices[0].CurrentResourceUsage, 200, 128*1024*1024, 4*1024*1024*1024)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var getAppResponse struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &getAppResponse)
	assertResourceUsage(t, getAppResponse.App.CurrentResourceUsage, 750, 640*1024*1024, 3*1024*1024*1024)
	assertResourceUsage(t, getAppResponse.App.BackingServices[0].CurrentResourceUsage, 200, 128*1024*1024, 4*1024*1024*1024)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/bindings", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var bindingsResponse struct {
		BackingServices []model.BackingService `json:"backing_services"`
	}
	mustDecodeJSON(t, recorder, &bindingsResponse)
	if len(bindingsResponse.BackingServices) != 1 {
		t.Fatalf("expected one binding backing service, got %#v", bindingsResponse.BackingServices)
	}
	assertResourceUsage(t, bindingsResponse.BackingServices[0].CurrentResourceUsage, 200, 128*1024*1024, 4*1024*1024*1024)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/backing-services", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listServicesResponse struct {
		BackingServices []model.BackingService `json:"backing_services"`
	}
	mustDecodeJSON(t, recorder, &listServicesResponse)
	if len(listServicesResponse.BackingServices) != 1 {
		t.Fatalf("expected one backing service, got %#v", listServicesResponse.BackingServices)
	}
	assertResourceUsage(t, listServicesResponse.BackingServices[0].CurrentResourceUsage, 200, 128*1024*1024, 4*1024*1024*1024)
}

func assertResourceUsage(t *testing.T, usage *model.ResourceUsage, cpuMilliCores, memoryBytes, ephemeralStorageBytes int64) {
	t.Helper()

	if usage == nil {
		t.Fatal("expected current_resource_usage to be populated")
	}
	if usage.CPUMilliCores == nil || *usage.CPUMilliCores != cpuMilliCores {
		t.Fatalf("expected cpu_millicores=%d, got %#v", cpuMilliCores, usage)
	}
	if usage.MemoryBytes == nil || *usage.MemoryBytes != memoryBytes {
		t.Fatalf("expected memory_bytes=%d, got %#v", memoryBytes, usage)
	}
	if usage.EphemeralStorageBytes == nil || *usage.EphemeralStorageBytes != ephemeralStorageBytes {
		t.Fatalf("expected ephemeral_storage_bytes=%d, got %#v", ephemeralStorageBytes, usage)
	}
}

func TestListAppsAllowsSkippingOptionalOverlays(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Overlay Skip Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:    "ghcr.io/example/demo:latest",
		Ports:    []int{8080},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	managedStatusCalls := 0
	clusterInventoryCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		managedStatusCalls++
		return nil, errors.New("unexpected managed status lookup")
	}
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		clusterInventoryCalls++
		return nil, errors.New("unexpected cluster inventory lookup")
	}

	recorder := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/apps?include_live_status=false&include_resource_usage=false",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if managedStatusCalls != 0 {
		t.Fatalf("expected managed status overlay to be skipped, got %d calls", managedStatusCalls)
	}
	if clusterInventoryCalls != 0 {
		t.Fatalf("expected resource usage overlay to be skipped, got %d calls", clusterInventoryCalls)
	}

	var response struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Apps) != 1 {
		t.Fatalf("expected one app, got %#v", response.Apps)
	}
	if response.Apps[0].CurrentResourceUsage != nil {
		t.Fatalf("expected current_resource_usage to be omitted when disabled, got %#v", response.Apps[0].CurrentResourceUsage)
	}

	serverTiming := recorder.Header().Get("Server-Timing")
	if !strings.Contains(serverTiming, "store_apps;dur=") {
		t.Fatalf("expected store_apps timing metric, got %q", serverTiming)
	}
	if strings.Contains(serverTiming, "live_status;dur=") {
		t.Fatalf("expected live_status timing metric to be absent, got %q", serverTiming)
	}
	if strings.Contains(serverTiming, "resource_usage;dur=") {
		t.Fatalf("expected resource_usage timing metric to be absent, got %q", serverTiming)
	}
}

func TestListAppsSkipsLiveStatusOverlayByDefault(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Default Overlay Skip Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:    "ghcr.io/example/demo:latest",
		Ports:    []int{8080},
		Replicas: 1,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	managedStatusCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		managedStatusCalls++
		return nil, errors.New("unexpected managed status lookup")
	}

	recorder := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/apps?include_resource_usage=false",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if managedStatusCalls != 0 {
		t.Fatalf("expected managed status overlay to be skipped by default, got %d calls", managedStatusCalls)
	}

	serverTiming := recorder.Header().Get("Server-Timing")
	if strings.Contains(serverTiming, "live_status;dur=") {
		t.Fatalf("expected live_status timing metric to be absent by default, got %q", serverTiming)
	}
}
