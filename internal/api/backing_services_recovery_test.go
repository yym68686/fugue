package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
)

func TestIndependentBackingServiceRecoveryRequiresAdminAndExactIntent(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("recovery-tenant")
	if err != nil {
		t.Fatal(err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	project, err := s.CreateProject(tenant.ID, "project", "")
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := s.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	target, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "worker", "", model.AppSpec{Image: "example.test/worker:v1", RuntimeID: source.ID, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "database", "", model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: source.ID, StorageSize: "2Gi", StorageClassName: "source-local", Database: "test", User: "test", Password: "secret", Instances: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "", nil); err != nil {
		t.Fatal(err)
	}
	_, key, err := s.CreateAPIKey(tenant.ID, "app-writer", []string{"app.write"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, "bootstrap-test"), nil, ServerConfig{})
	path := "/v1/backing-services/" + service.ID + "/recover"
	intent := map[string]any{"target_runtime_id": target.ID, "target_node_name": "target-node", "storage_size": "10Gi", "storage_class_name": "target-network"}
	rec := performJSONRequest(t, server, http.MethodPost, path, key, intent)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("app writer can recover service: %d %s", rec.Code, rec.Body.String())
	}
	rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", intent)
	if rec.Code != http.StatusOK {
		t.Fatalf("dry run failed: %d %s", rec.Code, rec.Body.String())
	}
	if ops, err := s.ListOperationsByApp(tenant.ID, true, app.ID); err != nil || len(ops) != 0 {
		t.Fatalf("dry run mutated store: %+v %v", ops, err)
	}
	intent["dry_run"] = false
	var operationID string
	for i := 0; i < 2; i++ {
		rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", intent)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("apply %d: %d %s", i, rec.Code, rec.Body.String())
		}
		var response struct {
			Operation model.Operation `json:"operation"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		op := response.Operation
		if op.Type != storagerecovery.OperationType || op.AppID != app.ID || op.ServiceID != service.ID || op.TargetRuntimeID != target.ID {
			t.Fatalf("wrong recovery operation: %+v", op)
		}
		if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.StorageClassName != "target-network" || op.DesiredSpec.Postgres.StorageSize != "10Gi" {
			t.Fatalf("recovery destination lost: %+v", op.DesiredSpec)
		}
		if i > 0 && op.ID != operationID {
			t.Fatalf("repeat created another operation: %s != %s", op.ID, operationID)
		}
		operationID = op.ID
	}
	intent["storage_class_name"] = "another-target"
	rec = performJSONRequest(t, server, http.MethodPost, path, "bootstrap-test", intent)
	if rec.Code != http.StatusConflict {
		t.Fatalf("changed intent reused operation: %d %s", rec.Code, rec.Body.String())
	}
}
