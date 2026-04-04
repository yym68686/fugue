package api

import "testing"

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
