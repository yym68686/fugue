package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestServiceMigrationWaitReportsFinalOperation(t *testing.T) {
	for _, command := range []string{"localize", "move"} {
		for _, outcome := range []string{"completed", "failed", "no-wait"} {
			t.Run(command+"/"+outcome, func(t *testing.T) {
				service := model.BackingService{ID: "svc_fixture", TenantID: "tenant_fixture", Name: "database", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime_target", StorageSize: "4Gi"}}}
				op := model.Operation{ID: "op_fixture", TenantID: service.TenantID, ServiceID: service.ID, Type: model.OperationTypeDatabaseLocalize, Status: model.OperationStatusPending, DesiredSpec: &model.AppSpec{Env: map[string]string{"SECRET_TOKEN": "sensitive-operation-value"}}}
				polls, mutations := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.URL.Path == "/v1/backing-services":
						_ = json.NewEncoder(w).Encode(map[string]any{"backing_services": []model.BackingService{service}})
					case r.URL.Path == "/v1/backing-services/svc_fixture":
						_ = json.NewEncoder(w).Encode(map[string]any{"backing_service": service})
					case r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/localize") || strings.HasSuffix(r.URL.Path, "/migrate")):
						mutations++
						_ = json.NewEncoder(w).Encode(map[string]any{"backing_service": service, "operation": op})
					case r.URL.Path == "/v1/operations/op_fixture":
						polls++
						op.Status = outcome
						if outcome == "failed" {
							op.ErrorMessage = "resize failed"
						}
						service.Spec.Postgres.StorageSize = "10Gi"
						_ = json.NewEncoder(w).Encode(map[string]any{"operation": op})
					default:
						t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
						http.NotFound(w, r)
					}
				}))
				defer server.Close()
				args := []string{"--base-url", server.URL, "--token", "fixture", "--json", "service", command, "database", "--runtime-id", "runtime_target"}
				if outcome == "no-wait" {
					args = append(args, "--wait=false")
				}
				var stdout, stderr bytes.Buffer
				err := runWithStreams(args, &stdout, &stderr)
				if mutations != 1 {
					t.Fatalf("expected exactly one request, got %d", mutations)
				}
				if outcome == "failed" {
					if err == nil || !strings.Contains(err.Error(), "resize failed") {
						t.Fatalf("failed operation not propagated: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				var result struct {
					Operation      model.Operation      `json:"operation"`
					BackingService model.BackingService `json:"backing_service"`
				}
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantSize, wantPolls := "completed", "10Gi", 1
				if outcome == "no-wait" {
					wantStatus, wantSize, wantPolls = "pending", "4Gi", 0
				}
				if result.Operation.Status != wantStatus || result.BackingService.Spec.Postgres.StorageSize != wantSize || polls != wantPolls {
					t.Fatalf("stale or unwanted wait result: %+v polls=%d", result, polls)
				}
				if strings.Contains(stdout.String(), "sensitive-operation-value") {
					t.Fatal("operation secret leaked")
				}
			})
		}
	}
}

func TestAppDatabaseLocalizeWaitReportsFinalOperation(t *testing.T) {
	app := model.App{ID: "app_fixture", Name: "application", Spec: model.AppSpec{Postgres: &model.AppPostgresSpec{StorageSize: "4Gi"}}}
	polled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/apps":
			_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})
		case "/v1/apps/app_fixture":
			if polled {
				app.Spec.Postgres.StorageSize = "10Gi"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"app": app})
		case "/v1/apps/app_fixture/database/localize":
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_fixture", Type: model.OperationTypeDatabaseLocalize, Status: model.OperationStatusPending}})
		case "/v1/operations/op_fixture":
			polled = true
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": model.Operation{ID: "op_fixture", Type: model.OperationTypeDatabaseLocalize, Status: model.OperationStatusCompleted}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "fixture", "--json", "app", "db", "localize", "application", "--storage-size", "10Gi"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Operation model.Operation `json:"operation"`
		App       model.App       `json:"app"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !polled || result.Operation.Status != model.OperationStatusCompleted || result.App.Spec.Postgres.StorageSize != "10Gi" {
		t.Fatalf("stale localization result: %+v", result)
	}
}

func TestRunServiceLocalizeUsesBackingServiceLocalizeEndpoint(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"target-vps","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/localize":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode localize body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"operation":{"id":"op_localize","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-localize","status":"pending","execution_mode":"managed","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"service", "localize", "main-db",
		"--to", "target-vps",
		"--node", "ns101351",
		"--storage-size", "5Gi",
		"--storage-class", "fugue-postgres-rwo",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service localize: %v stderr=%s", err, stderr.String())
	}

	if gotBody["target_runtime_id"] != "runtime_b" || gotBody["target_node_name"] != "ns101351" {
		t.Fatalf("expected localize target runtime/node, got %+v", gotBody)
	}
	if gotBody["storage_size"] != "5Gi" || gotBody["storage_class_name"] != "fugue-postgres-rwo" {
		t.Fatalf("expected localize storage target, got %+v", gotBody)
	}
	var response struct {
		Operation struct {
			ID              string `json:"id"`
			Type            string `json:"type"`
			ServiceID       string `json:"service_id"`
			TargetRuntimeID string `json:"target_runtime_id"`
		} `json:"operation"`
		TargetNodeName string `json:"target_node_name"`
		StorageSize    string `json:"storage_size"`
		StorageClass   string `json:"storage_class"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode stdout JSON: %v body=%s", err, stdout.String())
	}
	if response.Operation.ID != "op_localize" || response.Operation.Type != "database-localize" || response.Operation.ServiceID != "svc_pg" {
		t.Fatalf("unexpected operation response: %+v", response.Operation)
	}
	if response.Operation.TargetRuntimeID != "runtime_b" || response.TargetNodeName != "ns101351" {
		t.Fatalf("unexpected localize response target: %+v", response)
	}
	if response.StorageSize != "5Gi" || response.StorageClass != "fugue-postgres-rwo" {
		t.Fatalf("unexpected localize storage response target: %+v", response)
	}
}

func TestRunServiceMigrateStorageDefaultsToCurrentRuntime(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"apps","slug":"apps","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_a","tenant_id":"tenant_123","name":"source-vps","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","storage_class_name":"fugue-local-rwo"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services/svc_pg":
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","storage_class_name":"fugue-local-rwo"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services/svc_pg/localize":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode localize body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres","storage_class_name":"fugue-local-rwo"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"operation":{"id":"op_storage","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","type":"database-localize","status":"pending","execution_mode":"managed","target_runtime_id":"runtime_a","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"service", "migrate-storage", "main-db",
		"--storage-size", "5Gi",
		"--storage-class", "fugue-postgres-rwo",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service migrate-storage: %v stderr=%s", err, stderr.String())
	}

	if gotBody["target_runtime_id"] != "runtime_a" || gotBody["storage_class_name"] != "fugue-postgres-rwo" || gotBody["storage_size"] != "5Gi" {
		t.Fatalf("expected current runtime storage migration body, got %+v", gotBody)
	}
}
