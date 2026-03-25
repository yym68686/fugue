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

	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{GitHubSyncTimeout: time.Second},
		Logger: log.New(io.Discard, "", 0),
		latestGitHubCommit: func(context.Context, string, string) (string, string, error) {
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
	if op.RequestedByID != autoGitHubSyncRequestedByID {
		t.Fatalf("expected requested by %q, got %q", autoGitHubSyncRequestedByID, op.RequestedByID)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.RepoBranch != "main" {
		t.Fatalf("expected branch main, got %q", op.DesiredSource.RepoBranch)
	}
	if op.DesiredSource.CommitSHA != "" {
		t.Fatalf("expected queued source commit to be empty, got %q", op.DesiredSource.CommitSHA)
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
		latestGitHubCommit: func(context.Context, string, string) (string, string, error) {
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
