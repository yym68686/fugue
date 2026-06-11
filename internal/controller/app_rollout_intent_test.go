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

func TestRolloutIntentForManagedOperationDetectsResourceOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
			RuntimeID: "runtime_demo",
		},
	}
	desired := current
	desired.Spec.Resources = &model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 1024}
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected online resource update rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationDetectsPostgresResourceOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Postgres: &model.AppPostgresSpec{
				Database:  "demo",
				User:      "demo",
				Password:  "secret",
				Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
			},
		},
	}
	desired := current
	desired.Spec.Postgres = &model.AppPostgresSpec{
		Database:  "demo",
		User:      "demo",
		Password:  "secret",
		Resources: &model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 1024},
	}
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected postgres resource-only rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsResourceAndImageDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			Resources: &model.ResourceSpec{MemoryMebibytes: 512},
			RuntimeID: "runtime_demo",
		},
	}
	desired := current
	desired.Spec.Image = "ghcr.io/example/demo:v2"
	desired.Spec.Resources = &model.ResourceSpec{MemoryMebibytes: 1024}
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for image plus resource deploy, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsNoopDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
	}
	desired := current
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for noop deploy, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationDetectsLifecycleOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:                         "ghcr.io/example/demo:latest",
			Ports:                         []int{8080},
			Replicas:                      1,
			RuntimeID:                     "runtime_demo",
			TerminationGracePeriodSeconds: 30,
		},
	}
	desired := current
	desired.Spec.TerminationGracePeriodSeconds = 2100
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineLifecycleUpdate {
		t.Fatalf("expected online lifecycle update rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsLifecycleAndImageDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:                         "ghcr.io/example/demo:latest",
			Ports:                         []int{8080},
			Replicas:                      1,
			RuntimeID:                     "runtime_demo",
			TerminationGracePeriodSeconds: 30,
		},
	}
	desired := current
	desired.Spec.Image = "ghcr.io/example/demo:v2"
	desired.Spec.TerminationGracePeriodSeconds = 2100
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for image plus lifecycle deploy, got %q", got)
	}
}
