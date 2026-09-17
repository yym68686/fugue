package api

import (
	"encoding/json"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
	"net/http"
	"path/filepath"
	"testing"
)

func TestDatabaseRecoveryAuthorizationDryRunAndIdempotency(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatal(err)
	}
	rt, _, err := s.CreateRuntime(tenant.ID, "worker", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := s.CreateAPIKey(tenant.ID, "app-writer", []string{"app.write"})
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example.test/app:v1", RuntimeID: rt.ID, Replicas: 1, Postgres: &model.AppPostgresSpec{Database: "demo", RuntimeID: rt.ID, StorageSize: "20Gi", Instances: 1}})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, "bootstrap-test"), nil, ServerConfig{})
	path := "/v1/apps/" + app.ID + "/database/recover"
	rec := performJSONRequest(t, server, http.MethodPost, path, key, map[string]any{"dry_run": false})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("app writer could grow host storage: %d %s", rec.Code, rec.Body.String())
	}
	rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", map[string]any{})
	if rec.Code != http.StatusOK {
		t.Fatalf("default dry run: %d %s", rec.Code, rec.Body.String())
	}
	ops, err := s.ListOperationsByApp(tenant.ID, true, app.ID)
	if err != nil || len(ops) != 0 {
		t.Fatalf("dry run mutated state: %v %v", ops, err)
	}
	var operationID string
	for i := 0; i < 2; i++ {
		rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", map[string]any{"dry_run": false})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("apply: %d %s", rec.Code, rec.Body.String())
		}
		var response struct {
			Operation model.Operation `json:"operation"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Operation.Type != storagerecovery.OperationType {
			t.Fatalf("unexpected operation type %s", response.Operation.Type)
		}
		if i > 0 && response.Operation.ID != operationID {
			t.Fatal("retry created a second recovery")
		}
		operationID = response.Operation.ID
	}
	rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", map[string]any{"dry_run": false, "target_node_name": "different-node"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("changed intent adopted wrong operation: %d", rec.Code)
	}
}
