package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppRolloutDecisionParsesStringMapAnnotations(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Ports:    []int{8080},
			Replicas: 1,
		},
	}
	managed := runtime.ManagedAppObject{}
	managed.Metadata.Name = "app-demo"
	objects := []map[string]any{
		{
			"kind": "Deployment",
			"metadata": map[string]any{
				"name": "app-demo",
				"annotations": map[string]string{
					"fugue.io/downtime-class":         "downtime-required",
					"fugue.io/rollout-mode":           "isolated-singleton",
					"fugue.io/rollout-reason":         "single-writer-storage",
					runtime.FugueAnnotationReleaseKey: "release_123",
				},
			},
			"spec": map[string]any{
				"strategy": map[string]any{
					"type": "Recreate",
				},
			},
		},
	}

	decision := managedAppRolloutDecisionFromObjects(context.Background(), "namespace", managed, app, objects)
	if decision.Strategy != "Recreate" {
		t.Fatalf("expected strategy to be parsed, got %q", decision.Strategy)
	}
	if decision.DowntimeClass != "downtime-required" {
		t.Fatalf("expected downtime class to be parsed, got %q", decision.DowntimeClass)
	}
	if decision.RolloutMode != "isolated-singleton" {
		t.Fatalf("expected rollout mode to be parsed, got %q", decision.RolloutMode)
	}
	if decision.Reason != "single-writer-storage" {
		t.Fatalf("expected rollout reason to be parsed, got %q", decision.Reason)
	}
}
