package store

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRepairFailedAppPhaseDoesNotReuseManagedReleaseReadyTimestamp(t *testing.T) {
	t.Parallel()

	readyAt := time.Date(2026, time.April, 7, 1, 38, 22, 0, time.UTC)
	app := model.App{
		Status: model.AppStatus{
			Phase:                 "failed",
			CurrentReleaseReadyAt: &readyAt,
		},
	}

	if !repairFailedAppPhase(&app) {
		t.Fatal("expected failed phase to be repaired")
	}
	if app.Status.Phase != "unknown" {
		t.Fatalf("expected unknown phase without fresh runtime evidence, got %q", app.Status.Phase)
	}
}

func TestRepairFailedAppPhaseDoesNotReuseManagedReleaseStartedTimestamp(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.April, 7, 1, 37, 50, 0, time.UTC)
	app := model.App{
		Status: model.AppStatus{
			Phase:                   "failed",
			CurrentReleaseStartedAt: &startedAt,
		},
	}

	if !repairFailedAppPhase(&app) {
		t.Fatal("expected failed phase to be repaired")
	}
	if app.Status.Phase != "unknown" {
		t.Fatalf("expected unknown phase without fresh runtime evidence, got %q", app.Status.Phase)
	}
}

func TestHistoricalFailedOperationDoesNotSuppressLaterOperation(t *testing.T) {
	t.Parallel()

	status := model.AppStatus{
		Phase:           "deployed",
		CurrentReplicas: 1,
		LastOperationID: "op-success",
		LastFailedOperation: &model.AppOperationFailure{
			ID: "op-old-failure",
		},
	}
	if model.AppHasCurrentFailedOperation(status) {
		t.Fatal("a failure superseded by a later operation must not remain current")
	}
	app := model.App{Status: status}
	if invalidateStoredPhaseAfterFailure(&app) {
		t.Fatalf("historical failure unexpectedly invalidated current phase: %+v", app.Status)
	}
	if app.Status.Phase != "deployed" {
		t.Fatalf("historical failure changed current phase to %q", app.Status.Phase)
	}
}
