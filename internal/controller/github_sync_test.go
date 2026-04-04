package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestSyncGitHubAppsQueuesImportWhenCommitChanges(t *testing.T) {
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
		Image:     "registry.example.com/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyStaticSite,
		CommitSHA:        "oldcommit",
		ImageNameSuffix:  "web",
		ComposeService:   "app",
		ComposeDependsOn: []string{"redis"},
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{GitHubSyncTimeout: time.Second},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
			return "newcommit", "main", nil
		},
	}

	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatalf("sync github apps: %v", err)
	}

	ops, err := stateStore.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 queued operation, got %d", len(ops))
	}
	op := ops[0]
	if op.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", op.Type)
	}
	if op.RequestedByID != model.OperationRequestedByGitHubSyncController {
		t.Fatalf("expected requested by %q, got %q", model.OperationRequestedByGitHubSyncController, op.RequestedByID)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.RepoBranch != "main" {
		t.Fatalf("expected branch main, got %q", op.DesiredSource.RepoBranch)
	}
	if op.DesiredSource.CommitSHA != "newcommit" {
		t.Fatalf("expected queued source commit newcommit, got %q", op.DesiredSource.CommitSHA)
	}
	if op.DesiredSource.ImageNameSuffix != "web" {
		t.Fatalf("expected queued image suffix web, got %q", op.DesiredSource.ImageNameSuffix)
	}
	if op.DesiredSource.ComposeService != "app" {
		t.Fatalf("expected queued compose service app, got %q", op.DesiredSource.ComposeService)
	}
	if len(op.DesiredSource.ComposeDependsOn) != 1 || op.DesiredSource.ComposeDependsOn[0] != "redis" {
		t.Fatalf("expected queued compose dependencies [redis], got %v", op.DesiredSource.ComposeDependsOn)
	}

	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if app.Status.Phase != "importing" {
		t.Fatalf("expected app phase importing, got %q", app.Status.Phase)
	}
}

func TestSyncGitHubAppsSkipsAppsWithInFlightOperations(t *testing.T) {
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
		Image:     "registry.example.com/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "oldcommit",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	spec.Image = "registry.example.com/demo:git-current"
	if _, err := stateStore.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &spec,
	}); err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{GitHubSyncTimeout: time.Second},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
			return "newcommit", "main", nil
		},
	}

	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatalf("sync github apps: %v", err)
	}

	ops, err := stateStore.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected existing operation only, got %d operations", len(ops))
	}
}

func TestSyncGitHubAppsSkipsCommitThatAlreadyFailed(t *testing.T) {
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
		Image:     "registry.example.com/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "oldcommit",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	source := *app.Source
	source.CommitSHA = "newcommit"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByGitHubSyncController,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		t.Fatalf("create failed github sync import: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim failed github sync import: %v", err)
	} else if !found {
		t.Fatal("expected failed github sync import")
	}
	op, err = stateStore.FailOperation(op.ID, "build failed")
	if err != nil {
		t.Fatalf("fail github sync import: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			GitHubSyncTimeout:        time.Second,
			GitHubSyncRetryBaseDelay: 5 * time.Minute,
			GitHubSyncRetryMaxDelay:  time.Hour,
		},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
			return "newcommit", "main", nil
		},
		now: func() time.Time {
			return op.UpdatedAt.Add(4 * time.Minute)
		},
	}

	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatalf("sync github apps: %v", err)
	}

	ops, err := stateStore.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected no extra retry for already-failed commit, got %d operations", len(ops))
	}
}

func TestSyncGitHubAppsRetriesFailedCommitAfterBackoff(t *testing.T) {
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
		Image:     "registry.example.com/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "oldcommit",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	source := *app.Source
	source.CommitSHA = "newcommit"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByGitHubSyncController,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		t.Fatalf("create failed github sync import: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim failed github sync import: %v", err)
	} else if !found {
		t.Fatal("expected failed github sync import")
	}
	op, err = stateStore.FailOperation(op.ID, "build failed")
	if err != nil {
		t.Fatalf("fail github sync import: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			GitHubSyncTimeout:        time.Second,
			GitHubSyncRetryBaseDelay: 5 * time.Minute,
			GitHubSyncRetryMaxDelay:  time.Hour,
		},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
			return "newcommit", "main", nil
		},
		now: func() time.Time {
			return op.UpdatedAt.Add(6 * time.Minute)
		},
	}

	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatalf("sync github apps: %v", err)
	}

	ops, err := stateStore.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected 1 retry operation after backoff, got %d operations", len(ops))
	}
	retry := ops[1]
	if retry.DesiredSource == nil || retry.DesiredSource.CommitSHA != "newcommit" {
		t.Fatalf("expected retry for commit newcommit, got %#v", retry.DesiredSource)
	}
}

func TestSyncGitHubAppsBacksOffRepeatedFailuresForSameCommit(t *testing.T) {
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
		Image:     "registry.example.com/demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
		CommitSHA:     "oldcommit",
	}, model.AppRoute{
		Hostname:    "demo.example.com",
		BaseDomain:  "example.com",
		PublicURL:   "https://demo.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	spec := app.Spec
	source := *app.Source
	source.CommitSHA = "newcommit"
	op1, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByGitHubSyncController,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		t.Fatalf("create first failed github sync import: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim first failed github sync import: %v", err)
	} else if !found {
		t.Fatal("expected first failed github sync import")
	}
	if _, err := stateStore.FailOperation(op1.ID, "builder unavailable"); err != nil {
		t.Fatalf("fail first github sync import: %v", err)
	}

	op2, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByGitHubSyncController,
		AppID:           app.ID,
		DesiredSpec:     &spec,
		DesiredSource:   &source,
	})
	if err != nil {
		t.Fatalf("create second failed github sync import: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim second failed github sync import: %v", err)
	} else if !found {
		t.Fatal("expected second failed github sync import")
	}
	op2, err = stateStore.FailOperation(op2.ID, "builder unavailable")
	if err != nil {
		t.Fatalf("fail second github sync import: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			GitHubSyncTimeout:        time.Second,
			GitHubSyncRetryBaseDelay: 5 * time.Minute,
			GitHubSyncRetryMaxDelay:  time.Hour,
		},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) {
			return "newcommit", "main", nil
		},
		now: func() time.Time {
			return op2.UpdatedAt.Add(9 * time.Minute)
		},
	}

	if err := svc.syncGitHubApps(context.Background()); err != nil {
		t.Fatalf("sync github apps: %v", err)
	}

	ops, err := stateStore.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected retry to remain backed off after two failures, got %d operations", len(ops))
	}
}

func TestDeploymentRolloutReadyRequiresOldReplicasToTerminate(t *testing.T) {
	t.Parallel()

	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 3
	deployment.Status.ObservedGeneration = 3
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	ready, message, err := deploymentRolloutReady(deployment, true, 1, "demo")
	if err != nil {
		t.Fatalf("deployment rollout ready: %v", err)
	}
	if ready {
		t.Fatal("expected rollout to wait while old replicas still exist")
	}
	if message == "" {
		t.Fatal("expected wait message while old replicas terminate")
	}
}

func TestDeploymentRolloutReadyReportsFailureCondition(t *testing.T) {
	t.Parallel()

	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Conditions = []runtime.ManagedAppCondition{
		{
			Type:    "ReplicaFailure",
			Status:  "True",
			Reason:  "FailedCreate",
			Message: "quota exceeded",
		},
	}

	if _, _, err := deploymentRolloutReady(deployment, true, 1, "demo"); err == nil {
		t.Fatal("expected rollout failure condition to surface as error")
	}
}
