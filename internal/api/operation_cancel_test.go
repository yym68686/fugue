package api

import (
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestCancelOperationRequiresConfirmationAtAPIAndCancelsPending(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Cancel API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example/demo:latest", Replicas: 1, RuntimeID: runtimeObj.ID})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, TargetRuntimeID: runtimeObj.ID})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "operator", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/operations/"+op.ID+"/cancel", apiKey, map[string]string{"message": "operator requested cancellation"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected cancel status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.Status != model.OperationStatusCanceled {
		t.Fatalf("expected canceled status, got %+v", response.Operation)
	}
	if stored, getErr := s.GetOperation(op.ID); getErr != nil || stored.Status != model.OperationStatusCanceled {
		t.Fatalf("expected canceled operation in store, got %+v err=%v", stored, getErr)
	}
	if _, err := s.GetOperation(op.ID); errors.Is(err, store.ErrNotFound) {
		t.Fatal("operation should remain inspectable after cancellation")
	}
}
