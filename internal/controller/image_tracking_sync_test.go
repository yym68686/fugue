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
	"fugue/internal/store"
)

func TestSyncTrackedAppImagesQueuesWhenHistoryMatchesButCurrentSourceDiffers(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	uploadSource := model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         "upload_123",
		UploadFilename:   "api.tgz",
		ArchiveSHA256:    "abc123",
		ArchiveSizeBytes: 128,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "api",
		ComposeService:   "api",
		DetectedProvider: "dockerfile",
		DetectedStack:    "python",
	}
	app, err := s.UpdateAppOriginSource(app.ID, uploadSource)
	if err != nil {
		t.Fatalf("update app source: %v", err)
	}
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID:           tenant.ID,
		AppID:              app.ID,
		ImageRef:           "ghcr.io/acme/api:main",
		Enabled:            true,
		LastDeployedDigest: "sha256:abc123",
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	svc := &Service{
		Store: s,
		Config: config.ControllerConfig{
			ImageTrackingTimeout: time.Second,
		},
		Logger: log.New(io.Discard, "", 0),
		resolveRemoteImageDigest: func(context.Context, string) (string, error) {
			return "sha256:abc123", nil
		},
		now: func() time.Time {
			return time.Date(2026, time.May, 18, 12, 0, 0, 0, time.UTC)
		},
	}

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}

	queued, found, err := s.ClaimNextPendingGitHubSyncImportOperation()
	if err != nil {
		t.Fatalf("claim background import: %v", err)
	}
	if !found {
		t.Fatal("expected image tracking import to be queued")
	}
	if queued.RequestedByID != model.OperationRequestedByImageTracking {
		t.Fatalf("expected image tracking requested_by_id, got %q", queued.RequestedByID)
	}
	if queued.DesiredSource == nil || queued.DesiredSource.ImageRef != tracking.ImageRef {
		t.Fatalf("expected desired image ref %q, got %+v", tracking.ImageRef, queued.DesiredSource)
	}

	updated, err := s.GetAppImageTracking(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get tracking: %v", err)
	}
	if updated.LastQueuedDigest != "sha256:abc123" {
		t.Fatalf("expected digest to be queued, got %q", updated.LastQueuedDigest)
	}
}

func TestSyncTrackedAppImagesNoopsWhenFugueMirrorTagMatchesDigest(t *testing.T) {
	t.Parallel()

	s, tenant, project, _ := newImageTrackingSyncTestStore(t)
	trackingImageRef := "ghcr.io/acme/api:main"
	fullDigest := "sha256:2fb05ebe4e3768bd79206ee4c3cd768fc4270f0d881f648f75b13f2889cdd1d0"
	trackedSource := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         trackingImageRef,
		ResolvedImageRef: "fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/index-docker-io-acme-api:image-2fb05ebe4e37",
		ImageNameSuffix:  "api",
		ComposeService:   "api",
		DetectedProvider: "docker-image",
	}
	app, err := s.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "tracked-api", "", model.AppSpec{
		Image:     "registry.fugue.internal:5000/fugue-apps/index-docker-io-acme-api:image-2fb05ebe4e37",
		Ports:     []int{8000},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, trackedSource)
	if err != nil {
		t.Fatalf("create tracked app: %v", err)
	}
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID:         tenant.ID,
		AppID:            app.ID,
		ImageRef:         trackingImageRef,
		Enabled:          true,
		LastQueuedDigest: fullDigest,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	svc := &Service{
		Store: s,
		Config: config.ControllerConfig{
			ImageTrackingTimeout: time.Second,
		},
		Logger: log.New(io.Discard, "", 0),
		resolveRemoteImageDigest: func(context.Context, string) (string, error) {
			return fullDigest, nil
		},
		now: func() time.Time {
			return time.Date(2026, time.May, 18, 12, 0, 0, 0, time.UTC)
		},
	}

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}
	if queued, found, err := s.ClaimNextPendingGitHubSyncImportOperation(); err != nil {
		t.Fatalf("claim background import: %v", err)
	} else if found {
		t.Fatalf("expected no image tracking import, got %s", queued.ID)
	}

	updated, err := s.GetAppImageTracking(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("get tracking: %v", err)
	}
	if updated.LastSeenDigest != fullDigest {
		t.Fatalf("expected digest check to be recorded, got %q", updated.LastSeenDigest)
	}
	if updated.LastOperationID != tracking.LastOperationID {
		t.Fatalf("expected no queued operation, got last operation %q", updated.LastOperationID)
	}
}

func newImageTrackingSyncTestStore(t *testing.T) (*store.Store, model.Tenant, model.Project, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "api", "api project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:     "registry.fugue.local/acme/api:upload-abc123",
		Ports:     []int{8000},
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
