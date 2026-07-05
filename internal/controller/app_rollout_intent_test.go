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

func TestRolloutIntentForManagedOperationDetectsConfigFileOnlyDeploy(t *testing.T) {
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
			Files: []model.AppFile{
				{Path: "/etc/demo/config.yaml", Content: "mode: old\n", Mode: 0o644},
			},
		},
	}
	desired := current
	desired.Spec.Files = append([]model.AppFile(nil), current.Spec.Files...)
	desired.Spec.RestartToken = "restart_new"
	desired.Spec.Files[0].Content = "mode: new\n"
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineConfigUpdate {
		t.Fatalf("expected online config rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsPersistentStorageStructureChange(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml", SeedContent: "providers: []\n"},
				},
			},
		},
	}
	desired := current
	persistent := *current.Spec.PersistentStorage
	persistent.Mounts = append([]model.AppPersistentStorageMount(nil), current.Spec.PersistentStorage.Mounts...)
	desired.Spec.PersistentStorage = &persistent
	desired.Spec.PersistentStorage.Mounts[0].Path = "/home/config/api.yaml"
	desired.Spec.PersistentStorage.Mounts[0].SeedContent = "providers:\n- openai\n"
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for persistent storage structure change, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsImageAndLifecycleDeploy(t *testing.T) {
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
	desired.Spec.TerminationGracePeriodSeconds = 30
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for mixed image and lifecycle deploy, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationDetectsImageOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Source: &model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         "ghcr.io/example/demo:latest",
			ResolvedImageRef: "registry.push.example/demo:image-old",
		},
		Spec: model.AppSpec{
			Image:        "registry.pull.example/fugue-apps/demo@sha256:old",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
		},
	}
	model.NormalizeAppSourceState(&current)
	desired := current
	desired.Spec.Image = "registry.pull.example/fugue-apps/demo@sha256:new"
	desired.Spec.RestartToken = "restart_new"
	nextSource := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo:latest",
		ResolvedImageRef: "registry.push.example/demo:image-new",
	}
	model.SetAppSourceState(&desired, &nextSource, &nextSource)
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != model.AppRolloutIntentOnlineImageUpdate {
		t.Fatalf("expected online image rollout intent, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsSingleWriterImageOnlyDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "registry.pull.example/fugue-apps/demo@sha256:old",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml"},
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/home/data"},
				},
			},
		},
	}
	desired := current
	desired.Spec.Image = "registry.pull.example/fugue-apps/demo@sha256:new"
	desired.Spec.RestartToken = "restart_new"
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no online rollout intent for single-writer storage, got %q", got)
	}
}

func TestRolloutIntentForManagedOperationRejectsImageAndResourceDeploy(t *testing.T) {
	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "registry.pull.example/fugue-apps/demo@sha256:old",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			Resources: &model.ResourceSpec{
				CPUMilliCores: 250,
			},
		},
	}
	desired := current
	desired.Spec.Image = "registry.pull.example/fugue-apps/demo@sha256:new"
	desired.Spec.Resources = &model.ResourceSpec{CPUMilliCores: 500}
	op := model.Operation{
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &desired.Spec,
	}

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no rollout intent for image plus resource deploy, got %q", got)
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

func TestRolloutIntentForManagedOperationRejectsPostgresResourceOnlyDeploy(t *testing.T) {
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

	if got := rolloutIntentForManagedOperation(op, current, desired); got != "" {
		t.Fatalf("expected no online rollout intent for postgres resource-only deploy, got %q", got)
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
