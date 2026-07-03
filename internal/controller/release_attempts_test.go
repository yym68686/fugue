package controller

import (
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestReleaseAttemptTracksImportDeployRolloutLifecycle(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Release Attempt Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "api", "", model.AppSpec{Image: "ghcr.io/example/api:old", Replicas: 1})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	importSpec := app.Spec
	importSource := model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "ghcr.io/example/api:main"}
	importOp, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeImport, AppID: app.ID, DesiredSpec: &importSpec, DesiredSource: &importSource})
	if err != nil {
		t.Fatalf("create import op: %v", err)
	}
	svc := &Service{Store: stateStore, Logger: log.New(io.Discard, "", 0)}
	attempt := svc.createImageTrackingReleaseAttemptBestEffort(app, model.AppImageTracking{ID: "track_1", TenantID: tenant.ID, AppID: app.ID, ImageRef: importSource.ImageRef, LastDeployedDigest: "sha256:old"}, importOp, "sha256:new", model.ReleaseAttemptTriggerImageTrackingAuto, "image tracking queued import")
	if attempt == nil {
		t.Fatal("expected release attempt")
	}
	deploySpec := app.Spec
	deploySpec.Image = "registry.fugue.internal/fugue-apps/api@sha256:new"
	deploySource := importSource
	deploySource.ResolvedImageRef = "registry.push/fugue-apps/api@sha256:new"
	deployOp, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &deploySpec, DesiredSource: &deploySource})
	if err != nil {
		t.Fatalf("create deploy op: %v", err)
	}

	svc.markReleaseAttemptOperationRunning(importOp, app)
	svc.recordImportQueuedDeployReleaseSteps(importOp, app, deployOp)
	svc.markReleaseAttemptOperationRunning(deployOp, app)
	svc.markReleaseAttemptRolloutWaiting(deployOp, app)
	svc.completeReleaseAttemptForOperation(deployOp, app, "deployment ready")

	updated, err := stateStore.GetReleaseAttempt(attempt.ID)
	if err != nil {
		t.Fatalf("get release attempt: %v", err)
	}
	if updated.Status != model.ReleaseAttemptStatusCompleted || updated.Confidence != model.OperationEvidenceConfidenceConfirmed || updated.FinishedAt == nil {
		t.Fatalf("expected completed release attempt, got %+v", updated)
	}
	timeline, err := stateStore.ListReleaseTimeline(tenant.ID, false, attempt.ID)
	if err != nil {
		t.Fatalf("list release timeline: %v", err)
	}
	joined := releaseTimelineTypes(timeline)
	for _, want := range []string{
		model.ReleaseStepTypeTriggerReceived,
		model.ReleaseStepTypeImageTrackingCheck,
		model.ReleaseStepTypeImageImport,
		model.ReleaseStepTypeDeployQueued,
		model.ReleaseStepTypeDeployApply,
		model.ReleaseStepTypeRolloutWait,
		model.ReleaseStepTypeMarkDeployedDigest,
		model.ReleaseStepTypeFinalize,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected timeline to contain %s, got %s", want, joined)
		}
	}
}

func releaseTimelineTypes(timeline []model.ReleaseTimelineEntry) string {
	parts := make([]string, 0, len(timeline))
	for _, entry := range timeline {
		parts = append(parts, entry.Type+":"+entry.Status)
	}
	return strings.Join(parts, ",")
}
