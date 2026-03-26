package controller

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestBuildManagedAppStatusKeepsCurrentReleaseDuringRollout(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	currentStartedAt := time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	currentReadyAt := time.Date(2026, time.March, 26, 9, 2, 0, 0, time.UTC).Format(time.RFC3339Nano)
	nextReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			CurrentReleaseKey:       "release_previous",
			CurrentReleaseStartedAt: currentStartedAt,
			CurrentReleaseReadyAt:   currentReadyAt,
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	status := buildManagedAppStatus(managed, app, deployment, true, nil)

	if status.CurrentReleaseKey != "release_previous" {
		t.Fatalf("expected current release key to stay on previous release, got %q", status.CurrentReleaseKey)
	}
	if status.CurrentReleaseStartedAt != currentStartedAt {
		t.Fatalf("expected current release started at to stay %q, got %q", currentStartedAt, status.CurrentReleaseStartedAt)
	}
	if status.CurrentReleaseReadyAt != currentReadyAt {
		t.Fatalf("expected current release ready at to stay %q, got %q", currentReadyAt, status.CurrentReleaseReadyAt)
	}
	if status.PendingReleaseKey != nextReleaseKey {
		t.Fatalf("expected pending release key %q, got %q", nextReleaseKey, status.PendingReleaseKey)
	}
	if status.PendingReleaseStartedAt == "" {
		t.Fatal("expected pending release started at to be set")
	}
}

func TestBuildManagedAppStatusPromotesPendingReleaseWhenReady(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v2",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}

	pendingStartedAt := time.Date(2026, time.March, 26, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	nextReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{
			Generation: 2,
		},
		Spec: runtime.ManagedAppSpec{
			Scheduling: runtime.SchedulingConstraints{},
		},
		Status: runtime.ManagedAppStatus{
			CurrentReleaseKey:       "release_previous",
			CurrentReleaseStartedAt: time.Date(2026, time.March, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			CurrentReleaseReadyAt:   time.Date(2026, time.March, 26, 9, 2, 0, 0, time.UTC).Format(time.RFC3339Nano),
			PendingReleaseKey:       nextReleaseKey,
			PendingReleaseStartedAt: pendingStartedAt,
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	status := buildManagedAppStatus(managed, app, deployment, true, nil)

	if status.CurrentReleaseKey != nextReleaseKey {
		t.Fatalf("expected current release key %q, got %q", nextReleaseKey, status.CurrentReleaseKey)
	}
	if status.CurrentReleaseStartedAt != pendingStartedAt {
		t.Fatalf("expected promoted release started at %q, got %q", pendingStartedAt, status.CurrentReleaseStartedAt)
	}
	if status.CurrentReleaseReadyAt == "" {
		t.Fatal("expected promoted release ready at to be set")
	}
	if status.PendingReleaseKey != "" || status.PendingReleaseStartedAt != "" {
		t.Fatalf("expected pending release to be cleared, got key=%q started_at=%q", status.PendingReleaseKey, status.PendingReleaseStartedAt)
	}
}

func TestBuildManagedBackingServiceStatusTracksCurrentRuntime(t *testing.T) {
	startedAt := time.Date(2026, time.March, 26, 11, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	readyAt := time.Date(2026, time.March, 26, 11, 1, 0, 0, time.UTC).Format(time.RFC3339Nano)
	previous := runtime.ManagedAppStatus{
		BackingServices: []runtime.ManagedBackingServiceStatus{
			{
				ServiceID:               "service_demo",
				RuntimeKey:              "runtime_same",
				CurrentRuntimeStartedAt: startedAt,
				CurrentRuntimeReadyAt:   readyAt,
			},
		},
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	status := buildManagedBackingServiceStatus(previous, runtime.ManagedBackingServiceDeployment{
		ServiceID:    "service_demo",
		ResourceName: "demo-postgres",
		RuntimeKey:   "runtime_same",
	}, deployment, true)

	if status.CurrentRuntimeStartedAt != startedAt {
		t.Fatalf("expected service runtime started at %q, got %q", startedAt, status.CurrentRuntimeStartedAt)
	}
	if status.CurrentRuntimeReadyAt != readyAt {
		t.Fatalf("expected service runtime ready at %q, got %q", readyAt, status.CurrentRuntimeReadyAt)
	}
}
