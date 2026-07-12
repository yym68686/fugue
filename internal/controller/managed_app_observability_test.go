package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppOnlineRolloutSnapshotIgnoresBindingAndBackingServiceObservedDrift(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	oldRuntimeAt := time.Date(2026, time.July, 12, 12, 1, 0, 0, time.UTC)
	newRuntimeAt := time.Date(2026, time.July, 12, 13, 44, 11, 0, time.UTC)
	managedSnapshot := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ResolvedImageRef: "registry.fugue.internal:5000/fugue-apps/demo@sha256:new",
		},
		Spec: model.AppSpec{
			Image:         "registry.fugue.internal:5000/fugue-apps/demo@sha256:new",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineImageUpdate,
		},
		Bindings: []model.ServiceBinding{{
			ID:        "binding_demo",
			TenantID:  "tenant_demo",
			AppID:     "app_demo",
			ServiceID: "service_demo",
			Alias:     "database",
			CreatedAt: createdAt,
			UpdatedAt: oldRuntimeAt,
		}},
		BackingServices: []model.BackingService{{
			ID:          "service_demo",
			TenantID:    "tenant_demo",
			ProjectID:   "project_demo",
			OwnerAppID:  "app_demo",
			Name:        "database",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Status:      model.BackingServiceStatusActive,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
				Database:    "app",
				User:        "app",
				ServiceName: "database",
			}},
			CurrentRuntimeStartedAt: &oldRuntimeAt,
			CurrentRuntimeReadyAt:   &oldRuntimeAt,
			CreatedAt:               createdAt,
			UpdatedAt:               oldRuntimeAt,
		}},
	}
	stored := managedSnapshot
	stored.Spec.RolloutIntent = ""
	stored.Bindings = append([]model.ServiceBinding(nil), managedSnapshot.Bindings...)
	stored.Bindings[0].CreatedAt = newRuntimeAt
	stored.Bindings[0].UpdatedAt = newRuntimeAt
	stored.BackingServices = append([]model.BackingService(nil), managedSnapshot.BackingServices...)
	cpuMilliCores := int64(32)
	stored.BackingServices[0].CurrentResourceUsage = &model.ResourceUsage{CPUMilliCores: &cpuMilliCores}
	stored.BackingServices[0].CurrentRuntimeStartedAt = &newRuntimeAt
	stored.BackingServices[0].CurrentRuntimeReadyAt = &newRuntimeAt
	stored.BackingServices[0].CreatedAt = newRuntimeAt
	stored.BackingServices[0].UpdatedAt = newRuntimeAt

	if got := managedAppOnlineRolloutSnapshotMismatchFields(managedSnapshot, stored); len(got) != 0 {
		t.Fatalf("expected observed timestamps to be ignored, got mismatches %v", got)
	}
	if _, useStored := selectManagedAppDesiredApp(managedSnapshot, stored, false); useStored {
		t.Fatal("expected online rollout snapshot to survive runtime-only service drift")
	}

	changedBinding := stored
	changedBinding.Bindings = append([]model.ServiceBinding(nil), stored.Bindings...)
	changedBinding.Bindings[0].ServiceID = "service_other"
	got := managedAppOnlineRolloutSnapshotMismatchFields(managedSnapshot, changedBinding)
	want := []string{"Identity.Bindings[0].ServiceID"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected binding mismatch fields: got %v want %v", got, want)
	}
	if _, useStored := selectManagedAppDesiredApp(managedSnapshot, changedBinding, false); !useStored {
		t.Fatal("expected changed service binding to reject the online rollout snapshot")
	}

	changedService := stored
	changedService.BackingServices = cloneControllerBackingServices(stored.BackingServices)
	changedService.BackingServices[0].Spec.Postgres.Database = "other"
	if managedAppRolloutSnapshotIdentityEqual(managedSnapshot, changedService) {
		t.Fatal("expected changed backing service spec to reject the online rollout snapshot")
	}
}

func TestManagedAppRolloutDecisionParsesStringMapAnnotations(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Ports:    []int{8080},
			Replicas: 1,
		},
	}
	managed := runtime.ManagedAppObject{}
	managed.Metadata.Name = "app-demo"
	objects := []map[string]any{
		{
			"kind": "Deployment",
			"metadata": map[string]any{
				"name": "app-demo",
				"annotations": map[string]string{
					"fugue.io/downtime-class":         "downtime-required",
					"fugue.io/rollout-mode":           "isolated-singleton",
					"fugue.io/rollout-reason":         "single-writer-storage",
					runtime.FugueAnnotationReleaseKey: "release_123",
				},
			},
			"spec": map[string]any{
				"strategy": map[string]any{
					"type": "Recreate",
				},
			},
		},
	}

	decision := managedAppRolloutDecisionFromObjects(context.Background(), "namespace", managed, app, objects, runtime.ManagedAppReleaseKey(app, managed.Spec.Scheduling))
	if decision.Strategy != "Recreate" {
		t.Fatalf("expected strategy to be parsed, got %q", decision.Strategy)
	}
	if decision.DowntimeClass != "downtime-required" {
		t.Fatalf("expected downtime class to be parsed, got %q", decision.DowntimeClass)
	}
	if decision.RolloutMode != "isolated-singleton" {
		t.Fatalf("expected rollout mode to be parsed, got %q", decision.RolloutMode)
	}
	if decision.Reason != "single-writer-storage" {
		t.Fatalf("expected rollout reason to be parsed, got %q", decision.Reason)
	}
}
