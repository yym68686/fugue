package controller

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestDecorateManagedAppObjectsWithFenceEpochUpdatesServiceAndDeploymentSelectors(t *testing.T) {
	t.Parallel()

	app := model.App{
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
	objects := runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil)
	decorateManagedAppObjectsWithFenceEpoch(objects, app, "7")

	deployment := objects[0]
	service := objects[1]
	deploymentSelector := deployment["spec"].(map[string]any)["selector"].(map[string]any)["matchLabels"].(map[string]string)
	if got := deploymentSelector[runtime.FugueLabelFenceEpoch]; got != "7" {
		t.Fatalf("expected deployment fence epoch 7, got %#v", got)
	}
	serviceSelector := service["spec"].(map[string]any)["selector"].(map[string]string)
	if got := serviceSelector[runtime.FugueLabelFenceEpoch]; got != "7" {
		t.Fatalf("expected service fence epoch 7, got %#v", got)
	}
}
