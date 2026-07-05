package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestOnlineDurableRolloutSchedulingSkipsSingleWriterPersistentStorage(t *testing.T) {
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
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	desired := current
	desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate

	svc := &Service{
		newKubeClient: func(namespace string) (*kubeClient, error) {
			t.Fatalf("single-writer storage rollout should not need a current pod node lookup")
			return nil, nil
		},
	}
	base := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_demo",
		},
	}

	got := svc.onlineDurableRolloutScheduling(context.Background(), current, desired, base)
	if got.NodeSelector[runtime.RuntimeIDLabelKey] != "runtime_demo" {
		t.Fatalf("expected runtime node selector to be preserved, got %#v", got.NodeSelector)
	}
	if _, ok := got.NodeSelector[kubeHostnameLabelKey]; ok {
		t.Fatalf("expected single-writer storage rollout not to pin hostname, got %#v", got.NodeSelector)
	}
}

func TestOnlineDurableRolloutSchedulingSkipsSharedRWXPersistentStorage(t *testing.T) {
	current := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeSharedProjectRWX,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	desired := current
	desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate

	svc := &Service{
		newKubeClient: func(namespace string) (*kubeClient, error) {
			t.Fatalf("shared RWX rollout should not need a current pod node lookup")
			return nil, nil
		},
	}
	base := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_demo",
		},
	}

	got := svc.onlineDurableRolloutScheduling(context.Background(), current, desired, base)
	if _, ok := got.NodeSelector[kubeHostnameLabelKey]; ok {
		t.Fatalf("expected shared RWX rollout not to pin hostname, got %#v", got.NodeSelector)
	}
}
