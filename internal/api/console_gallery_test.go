package api

import (
	"testing"

	"fugue/internal/model"
)

func TestBuildConsoleProjectLifecycleUsesUpdatingForMixedLiveAndPending(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"importing", "building"},
		1,
		2,
		true,
		true,
		true,
	)

	if lifecycle.Label != "Updating" {
		t.Fatalf("expected mixed live/pending lifecycle to be Updating, got %q", lifecycle.Label)
	}
	if !lifecycle.Live {
		t.Fatal("expected updating lifecycle to stay live")
	}
	if lifecycle.SyncMode != "active" {
		t.Fatalf("expected active sync mode, got %q", lifecycle.SyncMode)
	}
	if lifecycle.Tone != "info" {
		t.Fatalf("expected info tone, got %q", lifecycle.Tone)
	}
}

func TestBuildConsoleProjectLifecycleKeepsBuildingForPendingOnly(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"building"},
		1,
		1,
		true,
		false,
		true,
	)

	if lifecycle.Label != "Building" {
		t.Fatalf("expected pending-only lifecycle to stay Building, got %q", lifecycle.Label)
	}
}

func TestReadConsoleActiveReleaseOperationIgnoresPendingDeployForFailedApp(t *testing.T) {
	t.Parallel()

	operation := &model.Operation{
		ID:     "op_demo",
		Type:   model.OperationTypeDeploy,
		Status: model.OperationStatusRunning,
	}
	app := model.App{
		Status: model.AppStatus{
			Phase: "failed",
		},
	}

	if got := readConsoleActiveReleaseOperation(operation, app); got != nil {
		t.Fatalf("expected failed app to ignore active deploy operation, got %+v", got)
	}
}
