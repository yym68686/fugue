package controller

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sort"
	"sync"
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

func (i *blockingImporter) ImportDockerImageSource(context.Context, sourceimport.DockerImageSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected docker image import")
}

func (i *blockingImporter) ImportGitHubSource(ctx context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
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

func (i *blockingImporter) SuggestGitHubComposeServiceEnv(context.Context, sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

func (i *blockingImporter) SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

type controlledImporter struct {
	mu       sync.Mutex
	started  chan string
	releases map[string]chan struct{}
}

func newControlledImporter() *controlledImporter {
	return &controlledImporter{
		started:  make(chan string, 16),
		releases: map[string]chan struct{}{},
	}
}

func (i *controlledImporter) ImportDockerImageSource(context.Context, sourceimport.DockerImageSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected docker image import")
}

func (i *controlledImporter) ImportGitHubSource(ctx context.Context, req sourceimport.GitHubSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	opID := req.JobLabels["fugue.pro/operation-id"]
	release := i.releaseChan(opID)

	select {
	case i.started <- opID:
	case <-ctx.Done():
		return sourceimport.GitHubSourceImportOutput{}, ctx.Err()
	}

	select {
	case <-ctx.Done():
		return sourceimport.GitHubSourceImportOutput{}, ctx.Err()
	case <-release:
	}

	return sourceimport.GitHubSourceImportOutput{
		ImportResult: sourceimport.GitHubImportResult{
			BuildStrategy: model.AppBuildStrategyDockerfile,
			ImageRef:      "registry.push.example/fugue-apps/demo:git-abc123",
			DetectedPort:  8080,
		},
		Source: model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       req.RepoURL,
			RepoBranch:    stringsOrDefault(req.Branch, "main"),
			BuildStrategy: model.AppBuildStrategyDockerfile,
		},
	}, nil
}

func (i *controlledImporter) ImportUploadedArchiveSource(context.Context, sourceimport.UploadSourceImportRequest) (sourceimport.GitHubSourceImportOutput, error) {
	return sourceimport.GitHubSourceImportOutput{}, fmt.Errorf("unexpected upload import")
}

func (i *controlledImporter) SuggestGitHubComposeServiceEnv(context.Context, sourceimport.GitHubComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

func (i *controlledImporter) SuggestUploadedComposeServiceEnv(context.Context, sourceimport.UploadComposeServiceEnvRequest) (map[string]string, error) {
	return nil, nil
}

func (i *controlledImporter) release(opID string) {
	release := i.releaseChan(opID)
	select {
	case <-release:
	default:
		close(release)
	}
}

func (i *controlledImporter) releaseChan(opID string) chan struct{} {
	i.mu.Lock()
	defer i.mu.Unlock()
	release := i.releases[opID]
	if release == nil {
		release = make(chan struct{})
		i.releases[opID] = release
	}
	return release
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
		bgDone <- svc.drainPendingOperationsInLane(context.Background(), operationLaneGitHubSync)
	}()

	select {
	case <-importer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background import did not start")
	}

	fgDone := make(chan error, 1)
	go func() {
		fgDone <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForegroundActivate)
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

func TestForegroundImportWorkersProcessDifferentAppsInParallel(t *testing.T) {
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

	appOne, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-one", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-one:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo-one",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		ComposeService: "app",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create first app: %v", err)
	}

	appTwo, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-two", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-two:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo-two",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		ComposeService: "app",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create second app: %v", err)
	}

	specOne := appOne.Spec
	sourceOne := *appOne.Source
	opOne, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           appOne.ID,
		DesiredSpec:     &specOne,
		DesiredSource:   &sourceOne,
	})
	if err != nil {
		t.Fatalf("create first import operation: %v", err)
	}

	specTwo := appTwo.Spec
	sourceTwo := *appTwo.Source
	opTwo, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           appTwo.ID,
		DesiredSpec:     &specTwo,
		DesiredSource:   &sourceTwo,
	})
	if err != nil {
		t.Fatalf("create second import operation: %v", err)
	}

	importer := newControlledImporter()
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.push.example",
	}

	doneOne := make(chan error, 1)
	doneTwo := make(chan error, 1)
	go func() {
		doneOne <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForegroundImport)
	}()
	go func() {
		doneTwo <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForegroundImport)
	}()

	started := map[string]struct{}{
		waitForStartedImportOperation(t, importer.started): {},
		waitForStartedImportOperation(t, importer.started): {},
	}
	if _, ok := started[opOne.ID]; !ok {
		t.Fatalf("expected operation %s to start, got %v", opOne.ID, started)
	}
	if _, ok := started[opTwo.ID]; !ok {
		t.Fatalf("expected operation %s to start, got %v", opTwo.ID, started)
	}

	importer.release(opOne.ID)
	importer.release(opTwo.ID)

	waitForDrain(t, doneOne, "first parallel import drain")
	waitForDrain(t, doneTwo, "second parallel import drain")

	assertOperationStatus(t, stateStore, opOne.ID, model.OperationStatusCompleted)
	assertOperationStatus(t, stateStore, opTwo.ID, model.OperationStatusCompleted)
}

func TestForegroundImportWorkersSerializeOperationsForSameApp(t *testing.T) {
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

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeGitHubPublic,
		RepoURL:        "https://github.com/example/demo",
		RepoBranch:     "main",
		BuildStrategy:  model.AppBuildStrategyDockerfile,
		DockerfilePath: "Dockerfile",
		ComposeService: "app",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	specOne := app.Spec
	sourceOne := *app.Source
	opOne, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specOne,
		DesiredSource:   &sourceOne,
	})
	if err != nil {
		t.Fatalf("create first import operation: %v", err)
	}

	specTwo := app.Spec
	sourceTwo := *app.Source
	opTwo, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specTwo,
		DesiredSource:   &sourceTwo,
	})
	if err != nil {
		t.Fatalf("create second import operation: %v", err)
	}

	importer := newControlledImporter()
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		importer:         importer,
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.push.example",
	}

	doneOne := make(chan error, 1)
	doneTwo := make(chan error, 1)
	go func() {
		doneOne <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForegroundImport)
	}()
	go func() {
		doneTwo <- svc.drainPendingOperationsInLane(context.Background(), operationLaneForegroundImport)
	}()

	if started := waitForStartedImportOperation(t, importer.started); started != opOne.ID {
		t.Fatalf("expected first started operation %s, got %s", opOne.ID, started)
	}
	assertNoStartedImportOperation(t, importer.started, 200*time.Millisecond)

	importer.release(opOne.ID)
	if started := waitForStartedImportOperation(t, importer.started); started != opTwo.ID {
		t.Fatalf("expected second started operation %s after release, got %s", opTwo.ID, started)
	}
	importer.release(opTwo.ID)

	waitForDrain(t, doneOne, "first same-app import drain")
	waitForDrain(t, doneTwo, "second same-app import drain")

	assertOperationStatus(t, stateStore, opOne.ID, model.OperationStatusCompleted)
	assertOperationStatus(t, stateStore, opTwo.ID, model.OperationStatusCompleted)
}

func TestForegroundActivateClaimsDependencyReadyDeployFirst(t *testing.T) {
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

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-app", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-app:git-old",
		Ports:     []int{3000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo-app:old",
		ComposeService:   "app",
		ComposeDependsOn: []string{"redis"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create app service: %v", err)
	}

	redis, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo-redis", "", model.AppSpec{
		Image:     "registry.push.example/fugue-apps/demo-redis:git-old",
		Ports:     []int{6379},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:           model.AppSourceTypeDockerImage,
		ImageRef:       "redis:7-alpine",
		ComposeService: "redis",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create redis service: %v", err)
	}

	appSpec := app.Spec
	appSpec.Image = "registry.push.example/fugue-apps/demo-app:new"
	appSource := *app.Source
	appDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &appSpec,
		DesiredSource: &appSource,
	})
	if err != nil {
		t.Fatalf("create app deploy: %v", err)
	}

	redisSpec := redis.Spec
	redisSpec.Image = "registry.push.example/fugue-apps/demo-redis:new"
	redisSource := *redis.Source
	redisDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         redis.ID,
		DesiredSpec:   &redisSpec,
		DesiredSource: &redisSource,
	})
	if err != nil {
		t.Fatalf("create redis deploy: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}

	claimed, found, err := svc.claimNextPendingOperationInLane(operationLaneForegroundActivate)
	if err != nil {
		t.Fatalf("claim first deploy: %v", err)
	}
	if !found {
		t.Fatal("expected a deploy operation to be claimable")
	}
	if claimed.ID != redisDeploy.ID {
		t.Fatalf("expected dependency deploy %s to be claimed first, got %s", redisDeploy.ID, claimed.ID)
	}

	if _, found, err := svc.claimNextPendingOperationInLane(operationLaneForegroundActivate); err != nil {
		t.Fatalf("claim blocked deploy while dependency running: %v", err)
	} else if found {
		t.Fatal("expected dependent deploy to stay blocked while dependency is running")
	}

	if _, err := stateStore.CompleteManagedOperation(redisDeploy.ID, "", "done"); err != nil {
		t.Fatalf("complete dependency deploy: %v", err)
	}

	claimed, found, err = svc.claimNextPendingOperationInLane(operationLaneForegroundActivate)
	if err != nil {
		t.Fatalf("claim dependent deploy after dependency ready: %v", err)
	}
	if !found {
		t.Fatal("expected dependent deploy to become claimable")
	}
	if claimed.ID != appDeploy.ID {
		t.Fatalf("expected app deploy %s after dependency ready, got %s", appDeploy.ID, claimed.ID)
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

func waitForStartedImportOperation(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case opID := <-started:
		return opID
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for import operation to start")
		return ""
	}
}

func assertNoStartedImportOperation(t *testing.T, started <-chan string, wait time.Duration) {
	t.Helper()

	select {
	case opID := <-started:
		t.Fatalf("expected no import operation to start yet, got %s", opID)
	case <-time.After(wait):
	}
}

func waitForDrain(t *testing.T, done <-chan error, label string) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s timed out", label)
	}
}

func assertOperationStatus(t *testing.T, stateStore *store.Store, operationID string, want string) {
	t.Helper()

	op, err := stateStore.GetOperation(operationID)
	if err != nil {
		t.Fatalf("get operation %s: %v", operationID, err)
	}
	if op.Status != want {
		t.Fatalf("expected operation %s status %q, got %q", operationID, want, op.Status)
	}
}
