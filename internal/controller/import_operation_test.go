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
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type recordingImporter struct {
	githubReq *sourceimport.GitHubSourceImportRequest
}

func (r *recordingImporter) ImportPublicGitHubSource(_ context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
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

func TestExecuteManagedImportOperationPassesManagedSharedPlacementToImporter(t *testing.T) {
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

	wantPlacement := map[string]string{
		runtime.SharedPoolLabelKey:          runtime.SharedPoolLabelValue,
		runtime.RegionLabelKey:              "ap-northeast-1",
		runtime.LocationCountryCodeLabelKey: "jp",
	}
	if !reflect.DeepEqual(importer.githubReq.PlacementNodeSelector, wantPlacement) {
		t.Fatalf("expected placement selector %v, got %v", wantPlacement, importer.githubReq.PlacementNodeSelector)
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
