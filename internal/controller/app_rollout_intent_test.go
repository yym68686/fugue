package controller

import (
	"testing"

	"fugue/internal/model"
)

func TestRolloutIntentForManagedOperationDetectsRestartOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "ghcr.io/example/demo:latest",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml"},
				},
			},
		},
	}
	desired := current
	desired.Spec.RestartToken = "restart_new"
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected online restart rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsNonRestartDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "ghcr.io/example/demo:latest",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
		},
	}
	desired := current
	desired.Spec.Image = "ghcr.io/example/demo:v2"
	desired.Spec.RestartToken = "restart_new"
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for image deploy, got %q", got)
	}
}
