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

func TestListOperationsReturnsSummariesAndGetOperationReturnsDesiredState(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Operation Summary Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	project, err := s.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:old",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "ghcr.io/example/demo:new"
	desiredSpec.Files = []model.AppFile{{
		Path:    "/app/generated.txt",
		Content: strings.Repeat("x", 4096),
	}}
	desiredSource := model.AppSource{
		Type:     model.AppSourceTypeDockerImage,
		ImageRef: "ghcr.io/example/demo:new",
	}
	op, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	listRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations?app_id="+app.ID, apiKey, nil)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected list status 200, got %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	var listResponse struct {
		Operations []model.Operation `json:"operations"`
	}
	mustDecodeJSON(t, listRecorder, &listResponse)
	if len(listResponse.Operations) != 1 || listResponse.Operations[0].ID != op.ID {
		t.Fatalf("unexpected operation list response: %+v", listResponse.Operations)
	}
	if listResponse.Operations[0].DesiredSpec != nil || listResponse.Operations[0].DesiredSource != nil || listResponse.Operations[0].DesiredOriginSource != nil {
		encoded, _ := json.Marshal(listResponse.Operations[0])
		t.Fatalf("expected list operation to omit desired state, got %s", string(encoded))
	}

	detailListRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations?app_id="+app.ID+"&include_desired=true", apiKey, nil)
	if detailListRecorder.Code != http.StatusOK {
		t.Fatalf("expected detailed list status 200, got %d body=%s", detailListRecorder.Code, detailListRecorder.Body.String())
	}
	var detailListResponse struct {
		Operations []model.Operation `json:"operations"`
	}
	mustDecodeJSON(t, detailListRecorder, &detailListResponse)
	if len(detailListResponse.Operations) != 1 || detailListResponse.Operations[0].DesiredSpec == nil || detailListResponse.Operations[0].DesiredSource == nil {
		t.Fatalf("expected include_desired list to include desired state, got %+v", detailListResponse.Operations)
	}

	getRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID, apiKey, nil)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected get status 200, got %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	var getResponse struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, getRecorder, &getResponse)
	if getResponse.Operation.DesiredSpec == nil || len(getResponse.Operation.DesiredSpec.Files) != 1 {
		t.Fatalf("expected get operation to include desired spec, got %+v", getResponse.Operation)
	}
	if getResponse.Operation.DesiredSource == nil || getResponse.Operation.DesiredSource.ImageRef != desiredSource.ImageRef {
		t.Fatalf("expected get operation to include desired source, got %+v", getResponse.Operation.DesiredSource)
	}
}
