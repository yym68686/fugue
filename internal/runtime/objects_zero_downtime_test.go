package runtime

import (
	"testing"

	"fugue/internal/model"
)

func TestZeroDowntimePolicyNeverRendersRecreateForDurableService(t *testing.T) {
	for _, mode := range []string{model.AppZeroDowntimeModeDrainOnly, model.AppZeroDowntimeModeSafe} {
		t.Run(mode, func(t *testing.T) {
			app := model.App{
				ID:       "app_demo",
				TenantID: "tenant_demo",
				Name:     "demo",
				Spec: model.AppSpec{
					Image:     "ghcr.io/example/demo:latest",
					Ports:     []int{8080},
					Replicas:  1,
					RuntimeID: "runtime_demo",
					Continuity: &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
						Enabled: true,
						Mode:    mode,
					}},
					PersistentStorage: &model.AppPersistentStorageSpec{
						Mode:             model.AppPersistentStorageModeMovableRWO,
						StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
						Mounts: []model.AppPersistentStorageMount{
							{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
						},
					},
				},
			}

			deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
			spec := deployment["spec"].(map[string]any)
			strategy := spec["strategy"].(map[string]any)
			if got := strategy["type"]; got != "RollingUpdate" {
				t.Fatalf("zero-downtime service must never render Recreate, got %#v", got)
			}
			rolling := strategy["rollingUpdate"].(map[string]any)
			if rolling["maxUnavailable"] != 0 || rolling["maxSurge"] != 1 {
				t.Fatalf("unexpected zero-downtime rolling strategy: %#v", rolling)
			}
			annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
			if annotations["fugue.io/downtime-class"] != "online-required" {
				t.Fatalf("expected online-required annotation, got %#v", annotations)
			}
			if annotations["fugue.io/rollout-reason"] != "zero-downtime-policy" {
				t.Fatalf("expected policy rollout reason, got %#v", annotations)
			}
			if annotations[FugueAnnotationZeroDowntimeRequired] != "true" {
				t.Fatalf("expected destructive repair guard annotation, got %#v", annotations)
			}
			if annotations["fugue.io/drain-mode"] == "" {
				t.Fatalf("expected strict drain annotations, got %#v", annotations)
			}
		})
	}
}

func TestEnvironmentRolloutIntentHasPreciseReason(t *testing.T) {
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "ghcr.io/example/demo:latest",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_demo",
			RolloutIntent: model.AppRolloutIntentOnlineEnvironmentUpdate,
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueLocalRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}

	deployment := firstObjectByKind(t, buildAppObjects(app, SchedulingConstraints{}), "Deployment")
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if got := annotations["fugue.io/rollout-reason"]; got != "environment-only" {
		t.Fatalf("expected environment-only rollout reason, got %q", got)
	}
	if annotations[FugueAnnotationZeroDowntimeRequired] != "true" {
		t.Fatalf("expected environment rollout to block destructive repair, got %#v", annotations)
	}
}
