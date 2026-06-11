package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestOnlineDurableRolloutSchedulingPinsToCurrentReadyNode(t *testing.T) {
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

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/pods") {
			http.NotFound(w, r)
			return
		}
		if selector := r.URL.Query().Get("labelSelector"); !strings.Contains(selector, current.ID) {
			http.Error(w, "expected pod list selector to include app id", http.StatusBadRequest)
			return
		}
		body := map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{
						"name":              "demo-7d4c4f7c95-abcde",
						"creationTimestamp": "2026-06-11T10:00:00Z",
					},
					"spec": map[string]any{
						"nodeName": "node-a",
					},
					"status": map[string]any{
						"phase": "Running",
						"conditions": []map[string]any{
							{"type": "Ready", "status": "True"},
						},
					},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encode pod list: %v", err)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Logger: log.New(io.Discard, "", 0),
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
		t.Fatalf("expected online rollout to pin new pod to current ready node, got %#v", got.NodeSelector)
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
