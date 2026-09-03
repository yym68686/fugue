package controller

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestStoredManagedAppDesiredInfersEnvironmentOnlyOnlineRollout(t *testing.T) {
	managedSnapshot := zeroDowntimeReconcilerTestApp()
	managedSnapshot.Spec.Continuity = nil
	storedDesired := managedSnapshot
	storedDesired.Spec.Env = map[string]string{"MODE": "new"}

	got := storedManagedAppDesiredWithRolloutIntent(managedSnapshot, storedDesired)
	if got.Spec.RolloutIntent != model.AppRolloutIntentOnlineEnvironmentUpdate {
		t.Fatalf("expected environment-only rollout intent, got %q", got.Spec.RolloutIntent)
	}
	objects := runtime.BuildManagedAppChildObjects(got, runtime.SchedulingConstraints{}, runtime.ManagedAppOwnerReference(runtime.ManagedAppObject{}))
	deployment := controllerTestFirstObjectByKind(t, objects, "Deployment")
	spec := deployment["spec"].(map[string]any)
	strategy := spec["strategy"].(map[string]any)
	if gotStrategy := strategy["type"]; gotStrategy != "RollingUpdate" {
		t.Fatalf("expected environment-only local RWO update to roll online, got %#v", gotStrategy)
	}
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if gotReason := annotations["fugue.io/rollout-reason"]; gotReason != "environment-only" {
		t.Fatalf("expected environment-only reason, got %q", gotReason)
	}
}

func TestManagedAppSnapshotKeepsCurrentEnvironmentRollout(t *testing.T) {
	stored := zeroDowntimeReconcilerTestApp()
	stored.Spec.Continuity = nil
	stored.Spec.Env = map[string]string{"MODE": "new"}
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineEnvironmentUpdate

	if !managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, stored) {
		t.Fatal("expected matching environment rollout snapshot to remain authoritative")
	}
	newerStored := stored
	newerStored.Spec.Env = map[string]string{"MODE": "newer"}
	if managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, newerStored) {
		t.Fatal("expected a newer stored environment update to supersede the old snapshot")
	}
}

func TestManagedAppSnapshotKeepsCurrentMixedZeroDowntimeRestart(t *testing.T) {
	stored := zeroDowntimeReconcilerTestApp()
	stored.Spec.Image = "ghcr.io/example/demo:v2"
	stored.Spec.TerminationGracePeriodSeconds = 60
	managedSnapshot := stored
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart

	if !managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, stored) {
		t.Fatal("expected matching mixed zero-downtime restart snapshot to remain authoritative")
	}
	newerStored := stored
	newerStored.Spec.Image = "ghcr.io/example/demo:v3"
	if managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, newerStored) {
		t.Fatal("expected a newer mixed restart to supersede the old snapshot")
	}

	disabledStored := stored
	disabledStored.Spec.Continuity = nil
	disabledSnapshot := disabledStored
	disabledSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	if !managedAppSnapshotCarriesCurrentOnlineRollout(disabledSnapshot, disabledStored) {
		t.Fatal("expected completed online policy-disable snapshot to remain authoritative")
	}
}

func TestManagedAppSnapshotTreatsBuildpacksLauncherAsExecutionPlumbing(t *testing.T) {
	t.Parallel()

	stored := zeroDowntimeReconcilerTestApp()
	stored.Spec.PersistentStorage = nil
	stored.Spec.Command = []string{"sh", "-lc", "python -m uvicorn app.main:app"}
	source := &model.AppSource{Type: model.AppSourceTypeUpload, BuildStrategy: model.AppBuildStrategyBuildpacks}
	model.SetAppSourceState(&stored, source, source)
	managedSnapshot := stored
	managedSnapshot.Spec.Command = []string{defaultCNBLauncherPath}
	managedSnapshot.Spec.Args = append([]string(nil), stored.Spec.Command...)
	managedSnapshot.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart

	if !managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, stored) {
		t.Fatal("buildpacks launcher wrapper must not supersede the matching serving snapshot")
	}
}

func zeroDowntimeReconcilerTestApp() model.App {
	return model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Env:       map[string]string{"MODE": "old"},
			Continuity: &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
				Enabled: true,
				Mode:    model.AppZeroDowntimeModeDrainOnly,
			}},
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueLocalRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
}
