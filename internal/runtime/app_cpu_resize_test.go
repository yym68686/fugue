package runtime

import (
	"fugue/internal/model"
	"reflect"
	"testing"
)

func TestAppCPUResizeLeavesTheDeploymentTemplateUnchanged(t *testing.T) {
	app := model.App{ID: "app_example", TenantID: "tenant_example", Name: "service", Spec: model.AppSpec{
		Image: "registry.example/service:one", Replicas: 1, RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256},
	}}
	before := buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)
	cpu := int64(100)
	app.Spec.CPURequestTemplateMilliCores = &cpu
	app.Spec.Resources.CPUMilliCores = 75
	after := buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("verified resize changed the Deployment/release template")
	}
	app.Spec.CPURequestTemplateMilliCores = nil
	if reflect.DeepEqual(before, buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)) {
		t.Fatal("a new release did not fold the effective target")
	}
}
