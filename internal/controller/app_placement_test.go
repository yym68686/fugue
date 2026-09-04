package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestSelectManagedSharedAppNodePreservesReadyNodeAbovePlacementBudget(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID: "app_demo", Name: "demo",
		Spec: model.AppSpec{
			Replicas: 1,
			Resources: &model.ResourceSpec{
				CPUMilliCores:   100,
				MemoryMebibytes: 128,
			},
		},
	}
	selector := map[string]string{
		runtimepkg.SharedPoolLabelKey:          runtimepkg.SharedPoolLabelValue,
		runtimepkg.LocationCountryCodeLabelKey: "de",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
				{
					"metadata": map[string]any{"name": runtimepkg.RuntimeAppResourceName(app) + "-abc"},
					"spec": map[string]any{
						"nodeName":   "node-a",
						"containers": []map[string]any{{"resources": map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "128Mi"}}}},
					},
					"status": map[string]any{"phase": "Running", "conditions": []map[string]any{{"type": "Ready", "status": "True"}}},
				},
				{
					"metadata": map[string]any{"name": "other-app-abc"},
					"spec": map[string]any{
						"nodeName":   "node-a",
						"containers": []map[string]any{{"resources": map[string]any{"requests": map[string]any{"cpu": "100m", "memory": "900Mi"}}}},
					},
					"status": map[string]any{"phase": "Running"},
				},
			}})
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"metadata": map[string]any{"name": "node-a"}}}})
		case "/api/v1/nodes/node-a":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"name": "node-a", "labels": selector},
				"status": map[string]any{
					"allocatable": map[string]any{"cpu": "1", "memory": "1Gi"},
					"conditions":  []map[string]any{{"type": "Ready", "status": "True"}, {"type": "DiskPressure", "status": "False"}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test"}
	svc := &Service{newKubeClient: func(string) (*kubeClient, error) { return client, nil }}
	nodeName, found, err := svc.selectManagedSharedAppNode(context.Background(), app, selector)
	if err != nil {
		t.Fatalf("select shared app node: %v", err)
	}
	if !found || nodeName != "node-a" {
		t.Fatalf("expected serving node-a to remain pinned, got node=%q found=%t", nodeName, found)
	}
}

func TestManagedSharedAppPlacementUsesCPUOnlyAsRankingWeight(t *testing.T) {
	t.Parallel()

	candidate := managedSharedNodeCandidate{
		allocatableCPUMilli:    1000,
		requestedCPUMilli:      5000,
		allocatableMemoryBytes: 4 * 1024 * 1024 * 1024,
	}
	request := managedSharedNodeRequests{cpuMilli: 500, memoryBytes: 512 * 1024 * 1024}
	if !managedSharedNodeCandidateFitsPolicy(candidate, request, appPlacementPolicy{cpuOvercommitRatio: 1, memoryRequestRatio: 0.9}) {
		t.Fatal("expected CPU request pressure not to eliminate an otherwise safe placement candidate")
	}
}
