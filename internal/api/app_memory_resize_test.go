package api

import (
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
)

func TestAppMemoryResizeAutoBypassesFullNodeSurgeWithoutChangingLimits(t *testing.T) {
	for _, scenario := range []string{"full-node", "recent-oom", "failed-target"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("FUGUE_APP_MEMORY_REQUEST_RESIZE_ENABLED", "true")
			t.Setenv("FUGUE_APP_CPU_REQUEST_RESIZE_ENABLED", "false")
			s := newStoreTestDatabase(t, "")
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, _ := s.CreateTenant("Resize Tenant")
			project, _ := s.CreateProject(tenant.ID, "services", "")
			raiseManagedTestCap(t, s, tenant.ID)
			app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:one", Ports: []int{8080}, RuntimeID: "runtime_managed_shared", Replicas: 1,
				Resources: &model.ResourceSpec{CPUMilliCores: 5, MemoryMebibytes: 1024, MemoryLimitMebibytes: 2048}})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SyncManagedAppObservedStatus(app.ID, "ready", 1, "serving", nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			app, _ = s.GetApp(app.ID)
			if err := s.RecordResourceUsageSamples(rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{{cpuMilli: 1, memoryMiB: 64}, {cpuMilli: 1, memoryMiB: 64}, {cpuMilli: 1, memoryMiB: 64}}), time.Time{}); err != nil {
				t.Fatal(err)
			}
			server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
			alloc, used, free := int64(2000), int64(100), int64(0)
			server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{node: model.ClusterNode{Status: "ready", CPU: &model.ClusterNodeCPUStats{AllocatableMilliCores: &alloc, UsedMilliCores: &used}, Memory: &model.ClusterNodeMemoryStats{SchedulableFreeBytes: &free}}}})
			if _, err := server.autoRightSizingAdmission(app, app.Spec, nil); err == nil {
				t.Fatal("fixture must reject an ordinary surge deployment")
			}
			if scenario == "recent-oom" || scenario == "failed-target" {
				desired := app.Spec.DeepCopy()
				desired.Resources.MemoryMebibytes = 768
				var op model.Operation
				var err error
				if scenario == "failed-target" {
					op, err = s.EnqueueAutomaticResourceSizingRequest(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeAppMemoryResize, RequestedByType: model.ActorTypeSystem, RequestedByID: model.OperationRequestedByRightSizingDownscale, DesiredSpec: &desired}, app.Spec)
				} else {
					op, err = s.EnqueueOOMRightSizingDeploy(app, model.OperationRequestedByOOMRightSizing+"/0123456789abcdef01234567", desired, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.FailOperation(op.ID, "preflight blocked"); err != nil {
					t.Fatal(err)
				}
			}
			rec, op, skipped, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "full-node" {
				if !skipped || op != nil {
					t.Fatal("OOM/backoff guard bypassed", op, skipped)
				}
				return
			}
			if skipped || op == nil || op.Type != model.OperationTypeAppMemoryResize {
				t.Fatal("memory downscale required surge capacity", op, skipped)
			}
			if op.DesiredSpec.Resources.MemoryMebibytes != 768 || op.DesiredSpec.Resources.MemoryLimitMebibytes != 2048 || op.DesiredSpec.Resources.CPUMilliCores != 5 || op.DesiredSpec.Image != app.Spec.Image || rec.App.RequestTarget.MemoryMebibytes >= 768 {
				t.Fatal("in-place resize swallowed unrelated dimensions", op.DesiredSpec.Resources)
			}
			_, duplicate, skipped, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
			if err != nil || !skipped || duplicate != nil {
				t.Fatal("parallel scan created a duplicate", duplicate, skipped, err)
			}
		})
	}
}
