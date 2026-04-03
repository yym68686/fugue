package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPatchAppContinuityQueuesDeploy(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Continuity Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/continuity", apiKey, map[string]any{
		"app_failover": map[string]any{
			"enabled":           true,
			"target_runtime_id": targetRuntime.ID,
		},
		"database_failover": map[string]any{
			"enabled":           true,
			"target_runtime_id": targetRuntime.ID,
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation struct {
			ID string `json:"id"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Failover == nil || op.DesiredSpec.Postgres == nil {
		t.Fatalf("expected desired continuity spec on operation, got %+v", op.DesiredSpec)
	}
	if op.DesiredSpec.Failover.TargetRuntimeID != targetRuntime.ID || !op.DesiredSpec.Failover.Auto {
		t.Fatalf("unexpected app failover desired spec: %+v", op.DesiredSpec.Failover)
	}
	if op.DesiredSpec.Postgres.FailoverTargetRuntimeID != targetRuntime.ID {
		t.Fatalf("unexpected postgres failover target: %+v", op.DesiredSpec.Postgres)
	}
	if op.DesiredSpec.Postgres.Instances != 2 || op.DesiredSpec.Postgres.SynchronousReplicas != 1 {
		t.Fatalf("expected two-instance postgres failover pair, got %+v", op.DesiredSpec.Postgres)
	}
}

func TestPatchAppContinuityReturnsAlreadyCurrent(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Continuity Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-node", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database:                "demo",
			Password:                "secret",
			FailoverTargetRuntimeID: targetRuntime.ID,
		},
		Failover: &model.AppFailoverSpec{
			TargetRuntimeID: targetRuntime.ID,
			Auto:            true,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/continuity", apiKey, map[string]any{
		"app_failover": map[string]any{
			"enabled":           true,
			"target_runtime_id": targetRuntime.ID,
		},
		"database_failover": map[string]any{
			"enabled":           true,
			"target_runtime_id": targetRuntime.ID,
		},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"already_current":true`) {
		t.Fatalf("expected already_current response, got %s", body)
	}
}
