package controller

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type recordingImporter struct {
	githubReq           *sourceimport.GitHubSourceImportRequest
	githubComposeEnvReq *sourceimport.GitHubComposeServiceEnvRequest
	githubComposeEnv    map[string]string
}

func (r *recordingImporter) ImportGitHubSource(_ context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	req.JobLabels = cloneStringMap(req.JobLabels)
	req.PlacementNodeSelector = cloneStringMap(req.PlacementNodeSelector)
	r.githubReq = &req
	return sourceimport.GitHubSourceImportOutput{
		ImportResult: sourceimport.GitHubImportResult{
			BuildStrategy: model.AppBuildStrategyDockerfile,
			ImageRef:      "registry.push.example/fugue-apps/demo:git-abc123",
			DetectedPort:  8080,
		},
		Source: model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       req.RepoURL,
			RepoBranch:    "main",
			BuildStrategy: model.AppBuildStrategyDockerfile,
		},
	}, nil
}

func (r *recordingImporter) ImportUploadedArchiveSource(context.Context, sourceimport.UploadSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected upload import")
}

func (r *recordingImporter) SuggestGitHubComposeServiceEnv(_ context.Context, req sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error) {
	req.AppHosts = cloneStringMap(req.AppHosts)
	req.ManagedPostgresByOwner = clonePostgresSpecMap(req.ManagedPostgresByOwner)
	r.githubComposeEnvReq = &req
	return cloneStringMap(r.githubComposeEnv), nil
}

func (r *recordingImporter) SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error) {
	return nil, fmt.Errorf("unexpected upload compose env refresh")
}

func TestExecuteManagedImportOperationDoesNotConstrainBuildPlacementByRuntimeLocation(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime("", "internal-cluster-tokyo", model.RuntimeTypeManagedShared, "in-cluster", map[string]string{
		"region":       "ap-northeast-1",
		"country_code": "JP",
	})
	if err != nil {
		t.Fatalf("create shared runtime: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyDockerfile,
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	specCopy := app.Spec
	sourceCopy := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{}
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, app); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.githubReq == nil {
		t.Fatal("expected importer to receive github request")
	}

	if importer.githubReq.PlacementNodeSelector != nil {
		t.Fatalf("expected import build placement selector to be nil, got %v", importer.githubReq.PlacementNodeSelector)
	}
}

func TestExecuteManagedImportOperationRefreshesComposeEnvWithoutOverwritingCustomValues(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	primaryApp, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-api", "", model.AppSpec{
		Env: map[string]string{
			"KEEP": "custom-value",
		},
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			ServiceName: "demo-api-postgres",
			Database:    "demo",
			User:        "demo",
			Password:    "secret",
		},
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyBuildpacks,
		ComposeService: "api",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create primary app: %v", err)
	}

	if _, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-web", "", model.AppSpec{
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyBuildpacks,
		ComposeService: "web",
	}, model.AppRoute{}); err != nil {
		t.Fatalf("create sibling app: %v", err)
	}

	specCopy := primaryApp.Spec
	sourceCopy := *primaryApp.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           primaryApp.ID,
		DesiredSpec:     &specCopy,
		DesiredSource:   &sourceCopy,
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}

	importer := &recordingImporter{
		githubComposeEnv: map[string]string{
			"KEEP":    "default-value",
			"NEW_KEY": "from-compose",
			"PORT":    "8080",
		},
	}
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
	}

	if err := svc.executeManagedImportOperation(context.Background(), op, primaryApp); err != nil {
		t.Fatalf("execute managed import operation: %v", err)
	}
	if importer.githubComposeEnvReq == nil {
		t.Fatal("expected compose env refresh request")
	}

	wantHosts := map[string]string{
		"api": "demo-api",
		"web": "demo-web",
	}
	if !reflect.DeepEqual(importer.githubComposeEnvReq.AppHosts, wantHosts) {
		t.Fatalf("expected compose app hosts %v, got %v", wantHosts, importer.githubComposeEnvReq.AppHosts)
	}
	if spec, ok := importer.githubComposeEnvReq.ManagedPostgresByOwner["api"]; !ok || spec.ServiceName != "demo-api-postgres" {
		t.Fatalf("expected api postgres spec to be forwarded, got %v", importer.githubComposeEnvReq.ManagedPostgresByOwner)
	}

	ops, err := stateStore.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	var deployOp model.Operation
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeDeploy {
			deployOp = candidate
		}
	}
	if deployOp.ID == "" || deployOp.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", deployOp)
	}
	if got := deployOp.DesiredSpec.Env["KEEP"]; got != "custom-value" {
		t.Fatalf("expected custom KEEP to be preserved, got %q", got)
	}
	if got := deployOp.DesiredSpec.Env["NEW_KEY"]; got != "from-compose" {
		t.Fatalf("expected NEW_KEY to be added from compose, got %q", got)
	}
	if got := deployOp.DesiredSpec.Env["PORT"]; got != "8080" {
		t.Fatalf("expected compose PORT to override build suggestion, got %q", got)
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func clonePostgresSpecMap(values map[string]model.AppPostgresSpec) map[string]model.AppPostgresSpec {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]model.AppPostgresSpec, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
