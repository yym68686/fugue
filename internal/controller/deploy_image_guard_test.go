package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestHandleClaimedOperationFailsDeployWhenManagedImageIsMissingFromRegistry(t *testing.T) {
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
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "oldcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-old",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "registry.pull.example/fugue-apps/demo:git-newcommit"
	desiredSource := *app.Source
	desiredSource.CommitSHA = "newcommit"
	desiredSource.ResolvedImageRef = "registry.push.example/fugue-apps/demo:git-newcommit"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected deploy operation to be claimable")
	}

	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != desiredSource.ResolvedImageRef {
				t.Fatalf("unexpected managed image ref %q", imageRef)
			}
			return false, nil, nil
		},
	}

	if err := svc.handleClaimedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("handle claimed operation: %v", err)
	}

	failedOp, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get failed deploy operation: %v", err)
	}
	if failedOp.Status != model.OperationStatusFailed {
		t.Fatalf("expected failed deploy status, got %q", failedOp.Status)
	}
	if !strings.Contains(failedOp.ErrorMessage, "still missing from the registry") {
		t.Fatalf("expected missing registry image error, got %q", failedOp.ErrorMessage)
	}
}
