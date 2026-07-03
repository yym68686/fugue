package controller

import (
	"context"
	"errors"
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
			return time.Now().UTC()
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
	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionQueued {
		t.Fatalf("expected queued decision, got %+v", check)
	}
	if check.OperationID == "" {
		t.Fatalf("expected queued decision to include operation id, got %+v", check)
	}
	attempts, err := s.ListReleaseAttempts(model.ReleaseAttemptFilter{TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list release attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected one release attempt, got %+v", attempts)
	}
	if attempts[0].TriggerType != model.ReleaseAttemptTriggerImageTrackingAuto || attempts[0].SourceOperationID != queued.ID || attempts[0].TargetDigest != "sha256:abc123" {
		t.Fatalf("unexpected release attempt: %+v", attempts[0])
	}
	steps, err := s.ListReleaseSteps(tenant.ID, false, attempts[0].ID)
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	for _, want := range []string{model.ReleaseStepTypeTriggerReceived, model.ReleaseStepTypeImageTrackingCheck, model.ReleaseStepTypeImageImport} {
		if !releaseStepHasType(steps, want) {
			t.Fatalf("expected release step %s, got %+v", want, steps)
		}
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
			return time.Now().UTC()
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
	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionAlreadyDeployed {
		t.Fatalf("expected already_deployed decision, got %+v", check)
	}
}

func TestSyncTrackedAppImagesRecordsActiveOperationSkip(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	if _, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	}); err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	desiredSpec := app.Spec
	activeOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   "test",
		DesiredSpec:     &desiredSpec,
	})
	if err != nil {
		t.Fatalf("create active op: %v", err)
	}
	svc := newImageTrackingSyncTestService(s, "sha256:new")

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}

	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionActiveOperation {
		t.Fatalf("expected active_operation decision, got %+v", check)
	}
	if check.ActiveOperationID != activeOp.ID {
		t.Fatalf("expected active operation %s, got %+v", activeOp.ID, check)
	}
}

func TestSyncTrackedAppImagesRecordsResolverError(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	if _, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	}); err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	svc := newImageTrackingSyncTestService(s, "")
	svc.resolveRemoteImageDigest = func(context.Context, string) (string, error) {
		return "", errors.New("registry timeout")
	}

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}

	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionResolverError {
		t.Fatalf("expected resolver_error decision, got %+v", check)
	}
	if check.ResolverError != "registry timeout" {
		t.Fatalf("expected resolver error evidence, got %+v", check)
	}
}

func TestSyncTrackedAppImagesRecordsReplicasZeroSkip(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	scaledSpec := app.Spec
	scaledSpec.Replicas = 0
	scaleOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		AppID:       app.ID,
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &scaledSpec,
	})
	if err != nil {
		t.Fatalf("create scale operation: %v", err)
	}
	if _, found, err := s.ClaimNextPendingOperation(); err != nil {
		t.Fatalf("claim scale operation: %v", err)
	} else if !found {
		t.Fatal("expected scale operation")
	}
	if _, err := s.CompleteManagedOperation(scaleOp.ID, "/tmp/api.yaml", "scaled to zero"); err != nil {
		t.Fatalf("complete scale operation: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get scaled app: %v", err)
	}
	if app.Spec.Replicas != 0 {
		t.Fatalf("expected replicas=0 after scale, got %d", app.Spec.Replicas)
	}
	if _, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	}); err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	svc := newImageTrackingSyncTestService(s, "sha256:new")

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}

	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionReplicasZero {
		t.Fatalf("expected replicas_zero decision, got %+v", check)
	}
}

func TestSyncTrackedAppImagesRecordsRetrySuppressed(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	lastTriggered := time.Now().UTC()
	if _, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID:         tenant.ID,
		AppID:            app.ID,
		ImageRef:         "ghcr.io/acme/api:main",
		Enabled:          true,
		LastQueuedDigest: "sha256:abc123",
		LastTriggeredAt:  &lastTriggered,
	}); err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	svc := newImageTrackingSyncTestService(s, "sha256:abc123")
	svc.now = func() time.Time {
		return lastTriggered.Add(time.Minute)
	}

	if err := svc.syncTrackedAppImages(context.Background()); err != nil {
		t.Fatalf("sync tracked images: %v", err)
	}

	check := latestImageTrackingCheckForTest(t, s, tenant.ID, app.ID)
	if check.Decision != model.AppImageTrackingDecisionRetrySuppressed {
		t.Fatalf("expected retry_suppressed decision, got %+v", check)
	}
}

func TestRecordAppImageTrackingDecisionAcceptsKnownDecisionValues(t *testing.T) {
	t.Parallel()

	s, tenant, _, app := newImageTrackingSyncTestStore(t)
	tracking, err := s.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: "ghcr.io/acme/api:main",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}
	svc := newImageTrackingSyncTestService(s, "sha256:new")
	decisions := []string{
		model.AppImageTrackingDecisionQueued,
		model.AppImageTrackingDecisionAlreadyDeployed,
		model.AppImageTrackingDecisionNoChange,
		model.AppImageTrackingDecisionReplicasZero,
		model.AppImageTrackingDecisionActiveOperation,
		model.AppImageTrackingDecisionRetrySuppressed,
		model.AppImageTrackingDecisionResolverError,
		model.AppImageTrackingDecisionQueueConflict,
		model.AppImageTrackingDecisionQueueError,
	}
	for _, decision := range decisions {
		svc.recordAppImageTrackingDecision(appImageTrackingDecisionInput{
			App:       app,
			Tracking:  tracking,
			Decision:  decision,
			StartedAt: time.Now().UTC(),
		})
	}
	checks, err := s.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID: tenant.ID,
		AppID:    app.ID,
		Limit:    len(decisions),
	})
	if err != nil {
		t.Fatalf("list checks: %v", err)
	}
	seen := map[string]bool{}
	for _, check := range checks {
		seen[check.Decision] = true
	}
	for _, decision := range decisions {
		if !seen[decision] {
			t.Fatalf("expected decision %s in %+v", decision, checks)
		}
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

func newImageTrackingSyncTestService(s *store.Store, digest string) *Service {
	return &Service{
		Store: s,
		Config: config.ControllerConfig{
			ImageTrackingTimeout: time.Second,
		},
		Logger: log.New(io.Discard, "", 0),
		resolveRemoteImageDigest: func(context.Context, string) (string, error) {
			return digest, nil
		},
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func latestImageTrackingCheckForTest(t *testing.T, s *store.Store, tenantID, appID string) model.AppImageTrackingCheck {
	t.Helper()
	checks, err := s.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID: tenantID,
		AppID:    appID,
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("list image tracking checks: %v", err)
	}
	if len(checks) == 0 {
		t.Fatal("expected image tracking check")
	}
	return checks[0]
}

func releaseStepHasType(steps []model.ReleaseStep, typ string) bool {
	for _, step := range steps {
		if step.Type == typ {
			return true
		}
	}
	return false
}
