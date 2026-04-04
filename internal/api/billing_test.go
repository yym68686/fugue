package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func deployBillingUsageTestApp(
	t *testing.T,
	s *store.Store,
	tenantID,
	projectID,
	name string,
	spec model.AppSpec,
) model.App {
	t.Helper()

	app, err := s.CreateApp(tenantID, projectID, name, "", spec)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	deploySpec := app.Spec
	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenantID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/"+name+".yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	deployed, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get deployed app: %v", err)
	}

	return deployed
}

func TestHandleGetBillingCurrentUsageOnlyCountsInternalClusterWorkloads(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Billing Usage Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	managedOwnedRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create managed-owned runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "billing-viewer", []string{"billing.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	sharedApp := deployBillingUsageTestApp(t, s, tenant.ID, project.ID, "shared-app", model.AppSpec{
		Image:     "ghcr.io/example/shared:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:  "shared",
			User:      "shared",
			Password:  "secret",
			RuntimeID: managedOwnedRuntime.ID,
		},
	})
	if len(sharedApp.BackingServices) != 1 {
		t.Fatalf("expected one backing service for shared app, got %#v", sharedApp.BackingServices)
	}
	sharedService := sharedApp.BackingServices[0]

	byoApp := deployBillingUsageTestApp(t, s, tenant.ID, project.ID, "byo-app", model.AppSpec{
		Image:     "ghcr.io/example/byo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: managedOwnedRuntime.ID,
	})

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
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "shared-app-pod",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      sharedApp.Name,
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     sharedApp.ID,
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
							"name":      "shared-postgres-pod",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:             sharedService.Spec.Postgres.ServiceName,
								runtime.FugueLabelManagedBy:        runtime.FugueLabelManagedByValue,
								runtime.FugueLabelComponent:        "postgres",
								runtime.FugueLabelBackingServiceID: sharedService.ID,
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
							"name":      "byo-app-pod",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      byoApp.Name,
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     byoApp.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "node-a",
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
							"name":      "shared-app-pod",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 100_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 128 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 1 * 1024 * 1024 * 1024,
						},
					},
					{
						"podRef": map[string]any{
							"name":      "shared-postgres-pod",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 200_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 256 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 2 * 1024 * 1024 * 1024,
						},
					},
					{
						"podRef": map[string]any{
							"name":      "byo-app-pod",
							"namespace": namespace,
						},
						"cpu": map[string]any{
							"usageNanoCores": 300_000_000,
						},
						"memory": map[string]any{
							"workingSetBytes": 512 * 1024 * 1024,
						},
						"ephemeral-storage": map[string]any{
							"usedBytes": 3 * 1024 * 1024 * 1024,
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

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/billing", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Billing model.TenantBillingSummary `json:"billing"`
	}
	mustDecodeJSON(t, recorder, &response)
	assertResourceUsage(t, response.Billing.CurrentUsage, 100, 128*1024*1024, 1*1024*1024*1024)
}
