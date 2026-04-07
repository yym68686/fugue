package store

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRepairFailedAppPhaseUsesManagedReleaseReadyTimestamp(t *testing.T) {
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
	if app.Status.Phase != "deployed" {
		t.Fatalf("expected deployed phase, got %q", app.Status.Phase)
	}
}

func TestRepairFailedAppPhaseUsesManagedReleaseStartedTimestamp(t *testing.T) {
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
	if app.Status.Phase != "deploying" {
		t.Fatalf("expected deploying phase, got %q", app.Status.Phase)
	}
}
