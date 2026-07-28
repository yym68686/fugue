package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAgentFailOperationClosesReleaseAttemptAndIsIdempotent(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Agent API")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example/demo:v1",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	runtimeObj, runtimeKey, err := s.CreateRuntime(tenant.ID, "external", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	desired := app.Spec
	desired.RuntimeID = runtimeObj.ID
	op, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeMigrate,
		AppID:           app.ID,
		TargetRuntimeID: runtimeObj.ID,
		DesiredSpec:     &desired,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil || !found {
		t.Fatalf("claim operation: found=%v err=%v", found, err)
	}
	attempt, err := s.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerManualDeploy,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusDeploying,
	})
	if err != nil {
		t.Fatalf("create release attempt: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	call := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+op.ID+"/fail", strings.NewReader(`{"message":"zero-downtime migrate refused"}`))
		req.Header.Set("Authorization", "Bearer "+runtimeKey)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, req)
		return recorder
	}
	if recorder := call(); recorder.Code != http.StatusOK {
		t.Fatalf("expected first refusal to succeed, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	duplicate := call()
	if duplicate.Code != http.StatusOK {
		t.Fatalf("expected duplicate refusal to be idempotent, got %d body=%s", duplicate.Code, duplicate.Body.String())
	}
	var duplicateResponse struct {
		Claimed bool `json:"claimed"`
	}
	if err := json.NewDecoder(duplicate.Body).Decode(&duplicateResponse); err != nil {
		t.Fatalf("decode duplicate refusal response: %v", err)
	}
	if duplicateResponse.Claimed {
		t.Fatal("duplicate refusal must not win the operation CAS")
	}

	updated, err := s.GetReleaseAttempt(attempt.ID)
	if err != nil {
		t.Fatalf("get release attempt: %v", err)
	}
	if updated.Status != model.ReleaseAttemptStatusFailed || updated.FailureEvidenceID == "" {
		t.Fatalf("expected failed release attempt with evidence, got %+v", updated)
	}
	evidence, err := s.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, PlatformAdmin: true, OperationID: op.ID, Limit: 10})
	if err != nil || len(evidence) != 1 {
		t.Fatalf("expected one failure evidence record, got %d err=%v", len(evidence), err)
	}
}
