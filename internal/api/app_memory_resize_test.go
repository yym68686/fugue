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
				actor := model.OperationRequestedByOOMRightSizing + "/event"
				if scenario == "failed-target" {
					actor = model.OperationRequestedByRightSizingDownscale
				}
				op, err := s.CreateAppRequestResizeOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeAppMemoryResize, RequestedByType: model.ActorTypeSystem, RequestedByID: actor, DesiredSpec: &desired}, app.Spec)
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

func TestAutoAppMemoryRequestChangePreservesOtherDimensionsAndThreshold(t *testing.T) {
	for _, tc := range []struct {
		current, target, want int64
		allowed               bool
	}{{320, 304, 304, true}, {320, 305, 0, false}, {10, 9, 9, true}, {128, 64, 96, true}, {128, 136, 136, true}} {
		current := &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: tc.current, MemoryLimitMebibytes: 2048}
		target := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: tc.target, MemoryLimitMebibytes: 256}
		d := autoAppMemoryRequestChange(current, target)
		if d.allowed != tc.allowed || (tc.allowed && (d.resources.MemoryMebibytes != tc.want || d.resources.MemoryLimitMebibytes != 2048 || d.resources.CPUMilliCores != 100)) {
			t.Fatalf("%d -> %d: %+v", tc.current, tc.target, d)
		}
	}
	if autoAppMemoryRequestChange(&model.ResourceSpec{MemoryMebibytes: 128, MemoryLimitMebibytes: 256}, &model.ResourceSpec{MemoryMebibytes: 512}).allowed {
		t.Fatal("request-only resize raised a limit")
	}
}
