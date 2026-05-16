package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestAppImageTrackingQueuesAsBackgroundImport(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/web:latest",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	op, err := s.QueueAppImageTrackingImport(app, tracking, model.ActorTypeBootstrap, model.OperationRequestedByImageTracking)
	if err != nil {
		t.Fatalf("queue image tracking import: %v", err)
	}
	if op.RequestedByID != model.OperationRequestedByImageTracking {
		t.Fatalf("expected image tracking requested_by_id, got %q", op.RequestedByID)
	}

	if _, found, err := s.ClaimNextPendingForegroundOperation(); err != nil {
		t.Fatalf("claim foreground operation: %v", err)
	} else if found {
		t.Fatal("image tracking import must not be claimed by foreground workers")
	}

	claimed, found, err := s.ClaimNextPendingGitHubSyncImportOperation()
	if err != nil {
		t.Fatalf("claim background import operation: %v", err)
	}
	if !found {
		t.Fatal("expected background import operation")
	}
	if claimed.ID != op.ID || claimed.TenantID != tenant.ID {
		t.Fatalf("unexpected claimed operation: %+v", claimed)
	}
}

func TestAppImageTrackingRecordsDeployDigest(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/web:latest",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	spec := app.Spec
	spec.Image = "registry.fugue.local/acme/web@sha256:abc123"
	source := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         tracking.ImageRef,
		ResolvedImageRef: "registry.fugue.local/acme/web@sha256:abc123",
	}
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		RequestedByID: model.OperationRequestedByImageTracking,
		AppID:         app.ID,
		DesiredSpec:   &spec,
		DesiredSource: &source,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected deploy operation")
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/web.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	updated, err := s.GetAppImageTracking(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get tracking: %v", err)
	}
	if updated.LastDeployedDigest != "sha256:abc123" {
		t.Fatalf("expected deployed digest to be recorded, got %q", updated.LastDeployedDigest)
	}
	if updated.LastOperationID != deployOp.ID {
		t.Fatalf("expected last operation %s, got %s", deployOp.ID, updated.LastOperationID)
	}
}

func newAppImageTrackingTestStore(t *testing.T) (*Store, model.Tenant, model.Project, model.App) {
	t.Helper()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "web project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "web", "", model.AppSpec{
		Image:     "registry.fugue.local/acme/web@sha256:old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	return s, tenant, project, app
}
