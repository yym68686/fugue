package api

import (
	"bytes"
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

func TestScaleAppIfMatchGuardsCommittedSpec(t *testing.T) {
	t.Parallel()
	s, server, _, app := setupAppConfigTestServer(t, model.AppSpec{Image: "ghcr.io/example/demo:latest", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared"})
	_, apiKey, err := s.CreateAPIKey(app.TenantID, "scaler", []string{"app.scale"})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	payload, _ := json.Marshal(map[string]any{"replicas": 2})
	request := func(tag string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/apps/"+app.ID+"/scale", bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("If-Match", tag)
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		return rec
	}
	if got := request(`"` + strings.Repeat("0", 64) + `"`).Code; got != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match status = %d, want 412", got)
	}
	if got := request("bad").Code; got != http.StatusBadRequest {
		t.Fatalf("invalid If-Match status = %d, want 400", got)
	}
	hash := model.AppSpecSHA256(app.Spec)
	if got := request(`"` + hash + `"`).Code; got != http.StatusAccepted {
		t.Fatalf("valid If-Match status = %d, want 202", got)
	}
}

func TestScaleAppRecoversFailedImportedAppDesiredState(t *testing.T) {
	t.Parallel()

	s, server, _, app, recoveredImage, recoveredSource := setupFailedImportedAppRecoveryServer(t)
	_, apiKey, err := s.CreateAPIKey(app.TenantID, "scaler", []string{"app.scale"})
	if err != nil {
		t.Fatalf("create scale api key: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/scale", apiKey, map[string]any{
		"replicas": 2,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.Type != model.OperationTypeDeploy {
		t.Fatalf("expected recovered scale to queue deploy operation, got %q", response.Operation.Type)
	}
	if response.Operation.DesiredSpec == nil {
		t.Fatal("expected recovered scale operation to include desired spec")
	}
	if got := response.Operation.DesiredSpec.Image; got != recoveredImage {
		t.Fatalf("expected recovered image %q, got %q", recoveredImage, got)
	}
	if got := response.Operation.DesiredSpec.Replicas; got != 2 {
		t.Fatalf("expected recovered replicas 2, got %d", got)
	}
	if response.Operation.DesiredSource == nil {
		t.Fatal("expected recovered scale operation to include desired source")
	}
	if got := response.Operation.DesiredSource.ResolvedImageRef; got != recoveredSource.ResolvedImageRef {
		t.Fatalf("expected recovered resolved image ref %q, got %q", recoveredSource.ResolvedImageRef, got)
	}
}

func TestScaleAppWithCurrentImageKeepsScaleOperation(t *testing.T) {
	t.Parallel()

	s, server, _, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	_, apiKey, err := s.CreateAPIKey(app.TenantID, "scaler", []string{"app.scale"})
	if err != nil {
		t.Fatalf("create scale api key: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/scale", apiKey, map[string]any{
		"replicas": 3,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.Type != model.OperationTypeScale {
		t.Fatalf("expected scale operation, got %q", response.Operation.Type)
	}
	if response.Operation.DesiredReplicas == nil || *response.Operation.DesiredReplicas != 3 {
		t.Fatalf("expected desired replicas 3, got %+v", response.Operation.DesiredReplicas)
	}
	if response.Operation.DesiredSpec != nil {
		t.Fatalf("expected normal scale to omit desired spec, got %+v", response.Operation.DesiredSpec)
	}
}

func TestScaleAppRejectsPositiveReplicasWhenDeployBaselineCannotBeRecovered(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Unrecoverable Import Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:1.2.3",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "scaler", []string{"app.scale"})
	if err != nil {
		t.Fatalf("create scale api key: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/scale", apiKey, map[string]any{
		"replicas": 1,
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}
