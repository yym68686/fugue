package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestOnlineDurableRolloutSchedulingSkipsWorkspaceRWOPersistentStorage(t *testing.T) {
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
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
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
			t.Fatalf("workspace RWO storage rollout should not need a current pod node lookup")
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
		t.Fatalf("expected workspace RWO rollout not to pin hostname, got %#v", got.NodeSelector)
	}
}

func TestOnlineDurableRolloutSchedulingPinsLocalRWOPersistentStorage(t *testing.T) {
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
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueLocalRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
				},
			},
		},
	}
	desired := current
	desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate
	namespace := runtime.NamespaceForTenant(current.TenantID)
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/"+namespace+"/pods" {
			http.NotFound(w, r)
			return
		}
		pod := kubePod{}
		pod.Metadata.Name = "app-demo-abc"
		pod.Spec.NodeName = "node-a"
		pod.Status.Phase = "Running"
		pod.Status.Conditions = []kubePodCondition{{
			Type:   "Ready",
			Status: "True",
		}}
		_ = json.NewEncoder(w).Encode(kubePodList{Items: []kubePod{pod}})
	}))
	defer kubeServer.Close()

	svc := &Service{
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
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
	if got.NodeSelector[kubeHostnameLabelKey] != "node-a" {
		t.Fatalf("expected local RWO online rollout to pin hostname node-a, got %#v", got.NodeSelector)
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
