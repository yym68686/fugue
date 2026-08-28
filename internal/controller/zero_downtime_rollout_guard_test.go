package controller

import (
	"context"
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
		runtime.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: "node-a"}},
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
		runtime.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: "node-a"}},
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

func TestZeroDowntimeRolloutGuardAllowsStatelessManagedRuntimeMigration(t *testing.T) {
	current := zeroDowntimeGuardTestApp("")
	current.Spec.PersistentStorage = nil
	current.Spec.RuntimeID = "runtime_source"
	desired := current
	desired.Spec.RuntimeID = "runtime_target"
	op := model.Operation{Type: model.OperationTypeMigrate, DesiredSpec: &desired.Spec}
	desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecisionWithScheduling(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{NodeSelector: map[string]string{"fugue.io/shared-pool": "internal"}},
		runtime.SchedulingConstraints{NodeSelector: map[string]string{
			"fugue.io/shared-pool":           "internal",
			"fugue.io/location-country-code": "de",
		}},
		"",
	)
	if decision.Refused {
		t.Fatalf("stateless managed runtime migration should have a validated online plan: %+v", decision)
	}
	if decision.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected online restart migration intent, got %+v", decision)
	}
	if !decision.PodTemplateChanged || decision.Strategy != "RollingUpdate" {
		t.Fatalf("expected migration scheduling to produce an online RollingUpdate: %+v", decision)
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
		t.Fatalf("expected policy disable on unsupported storage to fail closed: %+v", decision)
	}
	if decision.Strategy != "RollingUpdate" || decision.DowntimeClass != "online-required" || decision.RolloutMode != "rolling-restart" {
		t.Fatalf("expected the service default to prevent rendering Recreate: %+v", decision)
	}
	if decision.Reason != "storage class fugue-workspace-rwo does not support same-node online dual mount" {
		t.Fatalf("unexpected rendered-strategy refusal reason: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardProtectsRunningServiceWithoutExplicitPolicy(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	current.Status.CurrentReplicas = 1
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused {
		t.Fatalf("running service should fail closed without requiring an explicit policy: %+v", decision)
	}
	if decision.RequirementSource != model.AppZeroDowntimeRequirementSourceServiceDefault {
		t.Fatalf("expected service-default requirement source: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardProtectsPreviouslyServingAppWhileReplicasAreUnobserved(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	current.Status.CurrentReplicas = 0
	current.Status.CurrentRuntimeID = current.Spec.RuntimeID
	desired := current
	desired.Spec.NetworkMode = model.AppNetworkModeBackground
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if !decision.Refused {
		t.Fatalf("previously serving app must remain protected while observed replicas are zero: %+v", decision)
	}
	if decision.Reason != "the requested restart removes the cluster service while zero downtime is enabled" {
		t.Fatalf("unexpected refusal reason: %+v", decision)
	}
}

func TestZeroDowntimeRolloutGuardAllowsInitialDeployWithoutAServiceToProtect(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	current.Status.CurrentReplicas = 0
	current.Status.Phase = "deploying"
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}

	decision := (&Service{Renderer: runtime.Renderer{}}).zeroDowntimeRolloutGuardDecision(
		op,
		current,
		desired,
		runtime.SchedulingConstraints{},
	)
	if decision.Refused || decision.RequirementSource != "" {
		t.Fatalf("initial deploy has no live service to protect: %+v", decision)
	}
}

func TestManagedAppReconcileGuardRefusesStoredDriftBeforeApply(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{Name: "app-demo", Namespace: "fg-tenant-demo"},
		Spec: runtime.ManagedAppSpec{
			AppID:     current.ID,
			TenantID:  current.TenantID,
			ProjectID: current.ProjectID,
			Name:      current.Name,
			AppSpec:   current.Spec,
		},
		Status: runtime.ManagedAppStatus{
			Phase:         runtime.ManagedAppPhaseReady,
			ReadyReplicas: 1,
		},
	}
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	_, err := (&Service{Renderer: runtime.Renderer{}}).prepareManagedAppReconcileRollout(
		context.Background(), managed, desired, runtime.SchedulingConstraints{},
	)
	if err == nil {
		t.Fatal("stored desired drift on unsupported serving storage must be refused before Kubernetes apply")
	}
}

func TestManagedAppReconcileGuardAllowsInitialStoredSnapshot(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{Name: "app-demo", Namespace: "fg-tenant-demo"},
		Spec: runtime.ManagedAppSpec{
			AppID:     current.ID,
			TenantID:  current.TenantID,
			ProjectID: current.ProjectID,
			Name:      current.Name,
			AppSpec:   current.Spec,
		},
		Status: runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhasePending},
	}
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}

	if _, err := (&Service{Renderer: runtime.Renderer{}}).prepareManagedAppReconcileRollout(
		context.Background(), managed, desired, runtime.SchedulingConstraints{},
	); err != nil {
		t.Fatalf("initial stored snapshot must not be blocked: %v", err)
	}
}

func TestManagedAppReconcileGuardAllowsRecoveryFromInitialFailure(t *testing.T) {
	current := zeroDowntimeGuardTestApp(model.AppStorageClassFugueWorkspaceRWO)
	current.Spec.Continuity = nil
	managed := runtime.ManagedAppObject{
		Metadata: runtime.ManagedAppMeta{Name: "app-demo", Namespace: "fg-tenant-demo"},
		Spec: runtime.ManagedAppSpec{
			AppID:     current.ID,
			TenantID:  current.TenantID,
			ProjectID: current.ProjectID,
			Name:      current.Name,
			AppSpec:   current.Spec,
		},
		Status: runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseError},
	}
	if _, err := (&Service{Renderer: runtime.Renderer{}}).prepareManagedAppReconcileRollout(
		context.Background(), managed, current, runtime.SchedulingConstraints{},
	); err != nil {
		t.Fatalf("an initial failed deployment must remain repairable: %v", err)
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
