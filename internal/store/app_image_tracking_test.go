package store

import (
	"path/filepath"
	"testing"
	"time"

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

func TestAppImageTrackingImportPreservesComposeMetadataFromUploadSource(t *testing.T) {
	t.Parallel()

	s, _, _, app := newAppImageTrackingTestStore(t)
	upload, err := s.CreateSourceUpload(app.TenantID, "demo.tgz", "application/gzip", []byte("archive"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	uploadSource := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "web",
		ComposeService:   "web",
		ComposeDependsOn: []string{"api"},
		DetectedProvider: "dockerfile",
		DetectedStack:    "nextjs",
	}
	if _, err := s.UpdateAppOriginSource(app.ID, uploadSource); err != nil {
		t.Fatalf("update source: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}

	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/frontend:main",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	op, err := s.QueueAppImageTrackingImport(app, tracking, model.ActorTypeBootstrap, model.OperationRequestedByImageTracking)
	if err != nil {
		t.Fatalf("queue image tracking import: %v", err)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source")
	}
	if op.DesiredSource.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected docker image source, got %q", op.DesiredSource.Type)
	}
	if op.DesiredSource.ImageRef != tracking.ImageRef {
		t.Fatalf("expected tracked image ref %q, got %q", tracking.ImageRef, op.DesiredSource.ImageRef)
	}
	if op.DesiredSource.ImageNameSuffix != "web" {
		t.Fatalf("expected image suffix web, got %q", op.DesiredSource.ImageNameSuffix)
	}
	if op.DesiredSource.ComposeService != "web" {
		t.Fatalf("expected compose service web, got %q", op.DesiredSource.ComposeService)
	}
	if len(op.DesiredSource.ComposeDependsOn) != 1 || op.DesiredSource.ComposeDependsOn[0] != "api" {
		t.Fatalf("expected compose dependency [api], got %v", op.DesiredSource.ComposeDependsOn)
	}
	if op.DesiredSource.DetectedProvider != "dockerfile" {
		t.Fatalf("expected detected provider dockerfile, got %q", op.DesiredSource.DetectedProvider)
	}
	if op.DesiredSource.DetectedStack != "nextjs" {
		t.Fatalf("expected detected stack nextjs, got %q", op.DesiredSource.DetectedStack)
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

func TestAppImageTrackingClearsDeployDigestWhenDifferentSourceDeploys(t *testing.T) {
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

	trackedSpec := app.Spec
	trackedSpec.Image = "registry.fugue.local/acme/web@sha256:abc123"
	trackedSource := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         tracking.ImageRef,
		ResolvedImageRef: "registry.fugue.local/acme/web@sha256:abc123",
	}
	trackedDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		RequestedByID: model.OperationRequestedByImageTracking,
		AppID:         app.ID,
		DesiredSpec:   &trackedSpec,
		DesiredSource: &trackedSource,
	})
	if err != nil {
		t.Fatalf("create tracked deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim tracked deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected tracked deploy operation")
	}
	if _, err := s.CompleteManagedOperation(trackedDeployOp.ID, "/tmp/web.yaml", "deployed"); err != nil {
		t.Fatalf("complete tracked deploy operation: %v", err)
	}

	uploadSpec := trackedSpec
	uploadSpec.Image = "registry.fugue.local/acme/web:upload-abc123"
	uploadSource := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_123",
		UploadFilename:   "web.tgz",
		ArchiveSHA256:    "abc123",
		ArchiveSizeBytes: 128,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "web",
		ComposeService:   "web",
	}
	uploadDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		RequestedByID: "manual-upload",
		AppID:         app.ID,
		DesiredSpec:   &uploadSpec,
		DesiredSource: &uploadSource,
	})
	if err != nil {
		t.Fatalf("create upload deploy operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim upload deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected upload deploy operation")
	}
	if _, err := s.CompleteManagedOperation(uploadDeployOp.ID, "/tmp/web-upload.yaml", "deployed"); err != nil {
		t.Fatalf("complete upload deploy operation: %v", err)
	}

	updated, err := s.GetAppImageTracking(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get tracking: %v", err)
	}
	if updated.LastDeployedDigest != "" {
		t.Fatalf("expected deployed digest to be cleared after upload deploy, got %q", updated.LastDeployedDigest)
	}
	if updated.LastOperationID != uploadDeployOp.ID {
		t.Fatalf("expected last operation %s, got %s", uploadDeployOp.ID, updated.LastOperationID)
	}
}

func TestAppImageTrackingCheckHistoryRecordsAndFilters(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	check, err := s.CreateAppImageTrackingCheck(model.AppImageTrackingCheck{
		TenantID:         tenant.ID,
		AppID:            app.ID,
		TrackingID:       tracking.ID,
		ImageRef:         tracking.ImageRef,
		ObservedDigest:   "sha256:abc123",
		CurrentAppDigest: "sha256:def456",
		Decision:         model.AppImageTrackingDecisionQueued,
		OperationID:      "op_123",
		Event:            "poll",
	})
	if err != nil {
		t.Fatalf("create check: %v", err)
	}
	if check.ID == "" {
		t.Fatal("expected generated check id")
	}
	if check.TenantID != tenant.ID || check.AppID != app.ID || check.TrackingID != tracking.ID {
		t.Fatalf("unexpected check scope: %+v", check)
	}

	checks, err := s.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID: tenant.ID,
		AppID:    app.ID,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("list checks: %v", err)
	}
	if len(checks) != 1 || checks[0].ID != check.ID {
		t.Fatalf("expected one recorded check, got %+v", checks)
	}
	if checks[0].Decision != model.AppImageTrackingDecisionQueued {
		t.Fatalf("expected queued decision, got %q", checks[0].Decision)
	}
}

func TestAppImageTrackingCheckRetentionKeepsBoundedRecentHistory(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newAppImageTrackingTestStore(t)
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	now := time.Now().UTC()
	for i := 0; i < appImageTrackingCheckRetentionLimit+5; i++ {
		if _, err := s.CreateAppImageTrackingCheck(model.AppImageTrackingCheck{
			TenantID:   tenant.ID,
			AppID:      app.ID,
			TrackingID: tracking.ID,
			ImageRef:   tracking.ImageRef,
			Decision:   model.AppImageTrackingDecisionNoChange,
			SkipReason: "test",
			CheckedAt:  now.Add(-time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("create check %d: %v", i, err)
		}
	}
	checks, err := s.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID: tenant.ID,
		AppID:    app.ID,
		Limit:    appImageTrackingCheckRetentionLimit + 10,
	})
	if err != nil {
		t.Fatalf("list checks: %v", err)
	}
	if len(checks) != appImageTrackingCheckRetentionLimit {
		t.Fatalf("expected %d checks after retention, got %d", appImageTrackingCheckRetentionLimit, len(checks))
	}
	if checks[0].CheckedAt.Before(checks[len(checks)-1].CheckedAt) {
		t.Fatalf("expected newest-first order, got first=%s last=%s", checks[0].CheckedAt, checks[len(checks)-1].CheckedAt)
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
