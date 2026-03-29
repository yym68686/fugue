package controller

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type blockingImporter struct {
	started chan struct{}
	release chan struct{}
}

func (i *blockingImporter) ImportPublicGitHubSource(ctx context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	select {
	case <-i.started:
	default:
		close(i.started)
	}

	select {
	case <-ctx.Done():
		return sourceimport.GitHubSourceImportOutput{}, ctx.Err()
	case <-i.release:
	}

	return sourceimport.GitHubSourceImportOutput{
		ImportResult: sourceimport.GitHubImportResult{
			BuildStrategy: model.AppBuildStrategyDockerfile,
			ImageRef:      "registry.push.example/fugue-apps/demo:git-abc123",
			DetectedPort:  8080,
		},
		Source: model.AppSource{
			Type:           model.AppSourceTypeGitHubPublic,
			RepoURL:        req.RepoURL,
			RepoBranch:     stringsOrDefault(req.Branch, "main"),
			BuildStrategy:  model.AppBuildStrategyDockerfile,
			DockerfilePath: "Dockerfile",
		},
	}, nil
}

func (i *blockingImporter) ImportUploadedArchiveSource(context.Context, sourceimport.UploadSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected upload import")
}

func (i *blockingImporter) SuggestPublicGitHubComposeServiceEnv(context.Context, sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

func (i *blockingImporter) SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

type fakeBuilderJobClient struct {
	jobs    []kubeJobInfo
	deleted []string
}

func (f *fakeBuilderJobClient) listJobsBySelector(context.Context, string, string) ([]kubeJobInfo, error) {
	return append([]kubeJobInfo(nil), f.jobs...), nil
}

func (f *fakeBuilderJobClient) deleteJob(_ context.Context, _, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

func TestBackgroundGitHubSyncImportDoesNotBlockForegroundOperations(t *testing.T) {
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

	importApp, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-import", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-import:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/import-app",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		CommitSHA:      "oldcommit",
	}, model.AppRoute{
		Hostname:    "import.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://import.example.com",
		ServicePort: 8080,
	})
	if err != nil {
		t.Fatalf("create import app: %v", err)
	}

	deployApp, err := stateStore.CreateApp(tenant.ID, project.ID, "demo-deploy", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create deploy app: %v", err)
	}

	importSpec := importApp.Spec
	importSource := *importApp.Source
	importOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByGitHubSyncController,
		AppID:           importApp.ID,
		DesiredSpec:     &importSpec,
		DesiredSource:   &importSource,
	})
	if err != nil {
		t.Fatalf("create background import operation: %v", err)
	}

	deploySpec := deployApp.Spec
	deploySpec.Image = "nginx:1.28"
	deployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       deployApp.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	importer := &blockingImporter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	svc := New(stateStore, config.ControllerConfig{
		PollInterval: 10 * time.Millisecond,
		RenderDir:    filepath.Join(t.TempDir(), "render"),
	}, log.New(io.Discard, "", 0))
	svc.importer = importer
	svc.registryPushBase = "registry.push.example"
	svc.registryPullBase = "registry.push.example"

	bgDone := make(chan error, 1)
	go func() {
		bgDone <- svc.drainPendingOperationsInLane(context.Background(), operationLaneGitHubSyncImport)
	}()

	select {
	case <-importer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background import did not start")
	}

	fgDone := make(chan error, 1)
	go func() {
		fgDone <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForeground)
	}()

	select {
	case err := <-fgDone:
		if err != nil {
			t.Fatalf("drain foreground operations: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("foreground deploy was blocked by background import")
	}

	gotDeployOp, err := stateStore.GetOperation(deployOp.ID)
	if err != nil {
		t.Fatalf("get deploy operation: %v", err)
	}
	if gotDeployOp.Status != model.OperationStatusCompleted {
		t.Fatalf("expected deploy operation completed, got %q", gotDeployOp.Status)
	}

	gotImportOp, err := stateStore.GetOperation(importOp.ID)
	if err != nil {
		t.Fatalf("get import operation: %v", err)
	}
	if gotImportOp.Status != model.OperationStatusRunning {
		t.Fatalf("expected import operation running while blocked, got %q", gotImportOp.Status)
	}

	close(importer.release)
	select {
	case err := <-bgDone:
		if err != nil {
			t.Fatalf("drain background imports: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background import did not finish after release")
	}
}

func TestCleanupZombieBuildJobsDeletesOrphanedActiveJobs(t *testing.T) {
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

	runningApp, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-running", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-running:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo-running",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create running app: %v", err)
	}

	runningSpec := runningApp.Spec
	runningSource := *runningApp.Source
	runningOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		AppID:           runningApp.ID,
		DesiredSpec:     &runningSpec,
		DesiredSource:   &runningSource,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
	})
	if err != nil {
		t.Fatalf("create running import operation: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim running import operation: %v", err)
	} else if !found {
		t.Fatal("expected running import operation to be claimed")
	}

	pendingApp, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-pending", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-pending:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo-pending",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create pending app: %v", err)
	}

	pendingSpec := pendingApp.Spec
	pendingSource := *pendingApp.Source
	pendingOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		AppID:           pendingApp.ID,
		DesiredSpec:     &pendingSpec,
		DesiredSource:   &pendingSource,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
	})
	if err != nil {
		t.Fatalf("create pending import operation: %v", err)
	}

	client := &fakeBuilderJobClient{
		jobs: []kubeJobInfo{
			activeBuilderJob("build-running", runningOp.ID),
			activeBuilderJob("build-pending", pendingOp.ID),
			activeBuilderJob("build-orphan", ""),
		},
	}
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}

	if err := svc.cleanupZombieBuildJobsWithClient(context.Background(), client); err != nil {
		t.Fatalf("cleanup zombie build jobs: %v", err)
	}

	sort.Strings(client.deleted)
	wantDeleted := []string{"build-orphan", "build-pending"}
	if fmt.Sprint(client.deleted) != fmt.Sprint(wantDeleted) {
		t.Fatalf("unexpected deleted jobs: got %v want %v", client.deleted, wantDeleted)
	}
}

func activeBuilderJob(name, operationID string) kubeJobInfo {
	var job kubeJobInfo
	job.Metadata.Name = name
	job.Metadata.Labels = map[string]string{}
	if operationID != "" {
		job.Metadata.Labels["fugue.pro/operation-id"] = operationID
	}
	job.Status.Active = 1
	return job
}

func stringsOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
