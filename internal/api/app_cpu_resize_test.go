package api

import (
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
)

func TestAppCPUResizeAutoQueuesWithoutRollingCapacityOrBuildSource(t *testing.T) {
	t.Setenv("FUGUE_APP_CPU_REQUEST_RESIZE_ENABLED", "true")
	t.Setenv("FUGUE_APP_MEMORY_REQUEST_RESIZE_ENABLED", "false")
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := s.CreateTenant("Resize Tenant")
	project, _ := s.CreateProject(tenant.ID, "services", "")
	raiseManagedTestCap(t, s, tenant.ID)
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:one", RuntimeID: "runtime_managed_shared", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, MemoryLimitMebibytes: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	app, _ = s.GetApp(app.ID)
	if err := s.RecordResourceUsageSamples(rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{{cpuMilli: 20, memoryMiB: 64}, {cpuMilli: 25, memoryMiB: 64}, {cpuMilli: 30, memoryMiB: 64}}), time.Time{}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	rec, op, skipped, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil || skipped || op == nil || op.Type != model.OperationTypeAppCPUResize {
		t.Fatal("CPU request depended on rolling capacity/image/source", op, skipped, err)
	}
	if op.DesiredSpec.Resources.MemoryMebibytes != 512 || rec.App.RequestTarget.MemoryMebibytes == 512 {
		t.Fatal("independent memory suggestion was lost or prematurely applied")
	}
	_, duplicate, skipped, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil || !skipped || duplicate != nil {
		t.Fatal("concurrent scan bypassed operation serialization", duplicate, skipped, err)
	}
}
