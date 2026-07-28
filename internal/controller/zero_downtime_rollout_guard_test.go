package controller

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestZeroDowntimeRolloutGuardAllowsValidatedLocalRWOEnvironmentRestart(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if decision.Refused {
		t.Fatalf("expected validated local RWO rollout to be allowed: %+v", decision)
	}
	if decision.RolloutIntent != model.AppRolloutIntentOnlineEnvironmentUpdate {
		t.Fatalf("expected environment rollout intent, got %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardRefusesWorkspaceRWO(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused {
		t.Fatalf("expected workspace RWO rollout to be refused: %+v", decision)
	}
	if decision.Reason != "storage class fugue-workspace-rwo does not support same-node online dual mount" {
		t.Fatalf("unexpected refusal reason: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardRefusesUnclassifiedRestart(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	desired.Spec.Ports = []int{9090}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused || decision.Reason != "the requested restart has no validated online rollout plan" {
		t.Fatalf("expected unclassified restart to fail closed: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardRefusesMigration(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.RuntimeID = "runtime_other"
	op := model.Operation{Type: model.OperationTypeMigrate, DesiredSpec: &desired.Spec}

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused || decision.Reason != "the requested restart has no validated online rollout plan" {
		t.Fatalf("expected migration without an online handoff plan to fail closed: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardRefusesRestartThatRemovesService(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.NetworkMode = model.AppNetworkModeBackground
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused || decision.Reason != "the requested restart removes the cluster service while zero downtime is enabled" {
		t.Fatalf("expected service removal to fail closed: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardDisablesPolicyThroughOnlineLocalRWORollout(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueLocalRWO)
	desired := current
	desired.Spec.Continuity = nil
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if decision.Refused {
		t.Fatalf("expected local RWO policy disable to preserve zero downtime: %+v", decision)
	}
	if decision.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected policy disable to use online restart, got %+v", decision)
	}
	objects := runtime.BuildManagedAppChildObjects(desired, runtime.SchedulingConstraints{}, nil)
	deployment := controllerTestFirstObjectByKind(t, objects, "Deployment")
	annotations := deployment["metadata"].(map[string]any)["annotations"].(map[string]string)
	if annotations[runtime.FugueAnnotationZeroDowntimeRequired] != "true" {
		t.Fatalf("expected policy-disable transition to retain destructive repair guard, got %#v", annotations)
	}
}

func TestZeroDowntimeRolloutGuardRefusesPolicyDisableThatWouldRecreate(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	desired := current
	desired.Spec.Continuity = nil
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, current, desired)

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused {
		t.Fatalf("expected policy disable that would trigger Recreate to fail closed: %+v", decision)
	}
	if decision.Strategy != "Recreate" || decision.DowntimeClass != "downtime-required" || decision.RolloutMode != "isolated-singleton" {
		t.Fatalf("expected refusal to be based on the final rendered Recreate strategy: %+v", decision)
	}
	if decision.Reason != "the rendered serving workload requires a Recreate rollout while zero downtime is enabled" {
		t.Fatalf("unexpected rendered-strategy refusal reason: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardDoesNotChangePolicyOffBehavior(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if decision.Refused {
		t.Fatalf("policy-off service should retain existing rollout behavior: %+v", decision)
	}
}

func zeroDowntimeGuardTestApp(storageClass string) model.App {
	return model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
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
				StorageClassName: storageClass,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
}
