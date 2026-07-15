package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestOperationReadSecurityConfinesProjectsAndDeepRedactsDesiredState(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Operation Security Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	projectA, err := s.CreateProject(tenant.ID, "project-a", "")
	if err != nil {
		t.Fatalf("create project A: %v", err)
	}
	projectB, err := s.CreateProject(tenant.ID, "project-b", "")
	if err != nil {
		t.Fatalf("create project B: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create API key: %v", err)
	}
	appA, err := s.CreateApp(tenant.ID, projectA.ID, "app-a", "", model.AppSpec{Image: "ghcr.io/example/a", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	if err != nil {
		t.Fatalf("create app A: %v", err)
	}
	appB, err := s.CreateApp(tenant.ID, projectB.ID, "app-b", "", model.AppSpec{
		Image:     "ghcr.io/example/b",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database:    "appb",
			User:        "appb",
			Password:    "cross-project-postgres-secret",
			StorageSize: "1Gi",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   750,
				MemoryMebibytes: 1024,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app B: %v", err)
	}
	if len(appB.BackingServices) != 1 {
		t.Fatalf("expected app B managed postgres, got %+v", appB.BackingServices)
	}
	zero := 0
	scaleB, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeScale, AppID: appB.ID, DesiredReplicas: &zero})
	if err != nil {
		t.Fatalf("queue app B disable: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scaleB.ID, "", "disabled for lifecycle operation access test"); err != nil {
		t.Fatalf("complete app B disable: %v", err)
	}
	appB, err = s.GetApp(appB.ID)
	if err != nil {
		t.Fatalf("reload disabled app B: %v", err)
	}
	serviceB, err := s.GetBackingService(appB.BackingServices[0].ID)
	if err != nil {
		t.Fatalf("reload app B managed postgres service: %v", err)
	}
	if serviceB.Spec.Postgres == nil {
		t.Fatalf("app B managed postgres service is missing postgres spec: %+v", serviceB)
	}

	const (
		envSecret        = "operation-env-secret-sentinel"
		fileSecret       = "operation-file-secret-sentinel"
		mountSecret      = "operation-mount-secret-sentinel"
		postgresSecret   = "operation-postgres-secret-sentinel"
		repositorySecret = "operation-repository-secret-sentinel"
		repoQuerySecret  = "operation-repo-query-secret-sentinel"
		originSecret     = "operation-origin-secret-sentinel"
		restartSecret    = "operation-restart-secret-sentinel"
	)
	desiredSpec := appA.Spec
	desiredSpec.Env = map[string]string{"API_TOKEN": envSecret, "LOG_LEVEL": "info"}
	desiredSpec.Files = []model.AppFile{
		{Path: "/app/public.txt", Content: "public-content"},
		{Path: "/run/secret.txt", Content: fileSecret, Secret: true},
	}
	desiredSpec.PersistentStorage = &model.AppPersistentStorageSpec{
		StorageSize: "1Gi",
		ResetToken:  "persistent-reset-secret-sentinel",
		Mounts: []model.AppPersistentStorageMount{{
			Path:        "/data/seed.txt",
			SeedContent: mountSecret,
			Secret:      true,
		}},
	}
	desiredSpec.Postgres = &model.AppPostgresSpec{Database: "app", User: "app", Password: postgresSecret, RuntimeID: "runtime_managed_shared"}
	desiredSpec.RestartToken = restartSecret
	desiredSource := &model.AppSource{
		Type:          model.AppSourceTypeGitHubPrivate,
		RepoURL:       "https://embedded-user:embedded-password@example.com/private.git?token=" + repoQuerySecret,
		RepoAuthToken: repositorySecret,
	}
	opA, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         appA.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: desiredSource,
		DesiredOriginSource: &model.AppSource{
			Type:          model.AppSourceTypeGitHubPrivate,
			RepoURL:       "https://origin-user:origin-password@origin.example.com/private.git?token=" + originSecret,
			RepoAuthToken: originSecret,
		},
	})
	if err != nil {
		t.Fatalf("create operation A: %v", err)
	}
	specB := appB.Spec
	postgresB := *serviceB.Spec.Postgres
	postgresB.Suspended = true
	specB.Postgres = &postgresB
	opB, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDatabaseSuspend,
		AppID:       appB.ID,
		ServiceID:   serviceB.ID,
		DesiredSpec: &specB,
	})
	if err != nil {
		t.Fatalf("create operation B: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	projectPrincipal := model.Principal{TenantID: tenant.ID, ProjectID: projectA.ID}
	if _, err := server.loadAuthorizedOperation(projectPrincipal, opA.ID); err != nil {
		t.Fatalf("same-project operation unexpectedly denied: %v", err)
	}
	if _, err := server.loadAuthorizedOperation(projectPrincipal, opB.ID); !errors.Is(err, errOperationNotVisible) {
		t.Fatalf("cross-project operation must be denied, got %v", err)
	}
	if effective, err := operationListProjectForPrincipal(projectPrincipal, ""); err != nil || effective != projectA.ID {
		t.Fatalf("expected list project confinement to %s, got %q err=%v", projectA.ID, effective, err)
	}
	if _, err := operationListProjectForPrincipal(projectPrincipal, projectB.ID); !errors.Is(err, errOperationNotVisible) {
		t.Fatalf("cross-project list filter must be denied, got %v", err)
	}

	for _, path := range []string{
		"/v1/operations/" + opA.ID,
		"/v1/operations?app_id=" + appA.ID + "&include_desired=true",
	} {
		recorder := performJSONRequest(t, server, http.MethodGet, path, apiKey, nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected operation read %s status 200, got %d body=%s", path, recorder.Code, recorder.Body.String())
		}
		for _, secret := range []string{envSecret, fileSecret, mountSecret, postgresSecret, repositorySecret, repoQuerySecret, originSecret, restartSecret, "embedded-user", "embedded-password", "origin-user", "origin-password", "persistent-reset-secret-sentinel"} {
			if strings.Contains(recorder.Body.String(), secret) {
				t.Fatalf("operation read %s leaked %q: %s", path, secret, recorder.Body.String())
			}
		}
		if !strings.Contains(recorder.Body.String(), "public-content") ||
			!strings.Contains(recorder.Body.String(), "example.com/private.git") ||
			!strings.Contains(recorder.Body.String(), apiRedactedSecretValue) {
			t.Fatalf("operation read must preserve non-sensitive structure and include redaction markers: %s", recorder.Body.String())
		}
	}
}

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

	if _, err := s.SetOperationControllerTiming(op.ID, []model.OperationControllerTimingSegment{
		{Name: "billing_sync", DurationMilliseconds: 1200},
		{Name: "post_deploy_cleanup", DurationMilliseconds: 3400},
	}); err != nil {
		t.Fatalf("set controller timing: %v", err)
	}
	timingRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID, apiKey, nil)
	if timingRecorder.Code != http.StatusOK {
		t.Fatalf("expected timing get status 200, got %d body=%s", timingRecorder.Code, timingRecorder.Body.String())
	}
	var timingResponse struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, timingRecorder, &timingResponse)
	if len(timingResponse.Operation.ControllerTimingSegments) != 2 || timingResponse.Operation.ControllerTimingSegments[0].Name != "billing_sync" {
		t.Fatalf("expected controller timing segments in operation API, got %+v", timingResponse.Operation.ControllerTimingSegments)
	}
}

func TestListOperationsAppliesServerSideFilters(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Filtered Operations Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	project, err := s.CreateProject(tenant.ID, "target", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	otherProject, err := s.CreateProject(tenant.ID, "other", "")
	if err != nil {
		t.Fatalf("create other project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "reader", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "ghcr.io/example/demo", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	otherApp, err := s.CreateApp(tenant.ID, otherProject.ID, "other", "", model.AppSpec{Image: "ghcr.io/example/other", Replicas: 1, RuntimeID: "runtime_managed_shared"})
	if err != nil {
		t.Fatalf("create other app: %v", err)
	}

	appSpec := app.Spec
	older, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &appSpec})
	if err != nil {
		t.Fatalf("create older operation: %v", err)
	}
	replicas := 2
	_, err = s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeScale, AppID: app.ID, DesiredReplicas: &replicas})
	if err != nil {
		t.Fatalf("create type-mismatched operation: %v", err)
	}
	otherSpec := otherApp.Spec
	otherProjectOp, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: otherApp.ID, DesiredSpec: &otherSpec})
	if err != nil {
		t.Fatalf("create other project operation: %v", err)
	}
	if _, err := s.FailOperation(otherProjectOp.ID, "not relevant"); err != nil {
		t.Fatalf("fail other project operation: %v", err)
	}
	newerSpec := app.Spec
	newer, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &newerSpec})
	if err != nil {
		t.Fatalf("create newer operation: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	url := "/v1/operations?tenant_id=" + tenant.ID + "&project_id=" + project.ID + "&type=deploy&status=pending&limit=1"
	recorder := performJSONRequest(t, server, http.MethodGet, url, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Operations []model.Operation `json:"operations"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Operations) != 1 || response.Operations[0].ID != newer.ID {
		t.Fatalf("expected newest matching operation %s, got %+v; older=%s", newer.ID, response.Operations, older.ID)
	}
}
