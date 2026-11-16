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

func TestAppMemoryResizeLeavesImplicitLimitAndTemplateUnchanged(t *testing.T) {
	for _, limit := range []int64{0, 256} {
		app := model.App{ID: "app_example", TenantID: "tenant_example", Name: "service", Spec: model.AppSpec{Image: "registry.example/service:one", Replicas: 1, RuntimeID: "runtime_managed_shared", Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: limit}}}
		before := buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)
		memory := int64(128)
		app.Spec.MemoryRequestTemplateMebibytes = &memory
		app.Spec.Resources.MemoryMebibytes = 96
		after := buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("memory resize changed release template or implicit limit")
		}
		app.Spec.MemoryRequestTemplateMebibytes = nil
		if reflect.DeepEqual(before, buildAppDeploymentObject("tenant_example", app, nil, SchedulingConstraints{}, nil)) {
			t.Fatal("new release did not fold verified memory")
		}
	}
}
