package api

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBuildRightSizingRecommendationUsesUsagePercentilesAndClassPolicy(t *testing.T) {
	t.Parallel()

	samples := rightSizingUsageSamples("tenant_a", model.ClusterNodeWorkloadKindApp, "app_a", []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})

	recommendation := buildRightSizingRecommendation(
		model.ClusterNodeWorkloadKindApp,
		"app_a",
		"demo",
		"",
		model.WorkloadClassService,
		24,
		3,
		&model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 512},
		samples,
	)

	if !recommendation.Ready {
		t.Fatalf("expected recommendation to be ready: %+v", recommendation)
	}
	if recommendation.Recommended == nil {
		t.Fatal("expected recommended resources")
	}
	if got := recommendation.Recommended.CPUMilliCores; got != 75 {
		t.Fatalf("expected p95 CPU recommendation 75m, got %dm", got)
	}
	if got := recommendation.Recommended.CPULimitMilliCores; got != 0 {
		t.Fatalf("expected service CPU limit to remain unset, got %dm", got)
	}
	if got := recommendation.Recommended.MemoryMebibytes; got != 128 {
		t.Fatalf("expected p99 memory recommendation 128Mi, got %dMi", got)
	}
	if got := recommendation.Recommended.MemoryLimitMebibytes; got != 256 {
		t.Fatalf("expected service memory limit 256Mi, got %dMi", got)
	}
	if recommendation.RequestTarget == nil {
		t.Fatal("expected Kubernetes request target")
	}
	if got := recommendation.RequestTarget.CPUMilliCores; got != 30 {
		t.Fatalf("expected p50 CPU request target 30m, got %dm", got)
	}
	if got := recommendation.RequestTarget.MemoryMebibytes; got != 128 {
		t.Fatalf("expected memory request target 128Mi, got %dMi", got)
	}
}

func TestBuildRightSizingRecommendationPreservesUnobservedResourceDimensions(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	firstCPU := int64(20)
	secondCPU := int64(40)
	recommendation := buildRightSizingRecommendation(
		model.ClusterNodeWorkloadKindApp,
		"app_a",
		"demo",
		"",
		model.WorkloadClassService,
		24,
		2,
		&model.ResourceSpec{
			CPUMilliCores:        500,
			CPULimitMilliCores:   500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 768,
		},
		[]model.ResourceUsageSample{
			{ObservedAt: now.Add(-time.Minute), CPUMilliCores: &firstCPU},
			{ObservedAt: now, CPUMilliCores: &secondCPU},
		},
	)

	if !recommendation.Ready || recommendation.Recommended == nil {
		t.Fatalf("expected ready recommendation, got %+v", recommendation)
	}
	if got := recommendation.Recommended.CPUMilliCores; got != 60 {
		t.Fatalf("expected CPU recommendation 60m, got %dm", got)
	}
	if got := recommendation.Recommended.CPULimitMilliCores; got != 0 {
		t.Fatalf("expected service CPU limit to be cleared, got %dm", got)
	}
	if got := recommendation.Recommended.MemoryMebibytes; got != 0 {
		t.Fatalf("expected unobserved memory capacity to remain unknown, got %dMi", got)
	}
	if got := recommendation.Recommended.MemoryLimitMebibytes; got != 0 {
		t.Fatalf("expected unobserved memory limit to remain unknown, got %dMi", got)
	}
	if recommendation.RequestTarget == nil || recommendation.RequestTarget.CPUMilliCores != 25 {
		t.Fatalf("expected p50 CPU request target with 25m floor, got %+v", recommendation.RequestTarget)
	}
	if recommendation.RequestTarget.CPULimitMilliCores != 500 {
		t.Fatalf("expected explicit CPU limit to be preserved, got %+v", recommendation.RequestTarget)
	}
	if recommendation.RequestTarget.MemoryMebibytes != 512 || recommendation.RequestTarget.MemoryLimitMebibytes != 768 {
		t.Fatalf("expected request target to preserve unobserved memory settings, got %+v", recommendation.RequestTarget)
	}
}

func TestAppResourceRecommendationIgnoresLegacyAggregateSamples(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Now().UTC()
	legacyCPU := int64(800)
	perReplicaCPUOne := int64(40)
	perReplicaCPUTwo := int64(50)
	if err := stateStore.RecordResourceUsageSamples([]model.ResourceUsageSample{
		{TenantID: "tenant_a", TargetKind: model.ClusterNodeWorkloadKindApp, TargetID: "app_a", ObservedAt: now.Add(-3 * time.Minute), CPUMilliCores: &legacyCPU},
		{TenantID: "tenant_a", TargetKind: rightSizingSampleTargetKindAppV1, TargetID: "app_a", ObservedAt: now.Add(-2 * time.Minute), CPUMilliCores: &perReplicaCPUOne},
		{TenantID: "tenant_a", TargetKind: rightSizingSampleTargetKindAppV1, TargetID: "app_a", ObservedAt: now.Add(-time.Minute), CPUMilliCores: &perReplicaCPUTwo},
	}, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recommendation, err := server.appResourceRecommendation(model.App{
		ID:       "app_a",
		TenantID: "tenant_a",
		Name:     "demo",
		Spec: model.AppSpec{
			Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128},
		},
	}, 24, 2)
	if err != nil {
		t.Fatalf("build recommendation: %v", err)
	}
	if recommendation.App.SampleCount != 2 || !recommendation.App.Ready || recommendation.App.Recommended == nil {
		t.Fatalf("expected two ready per-replica samples, got %+v", recommendation.App)
	}
	if got := recommendation.App.Recommended.CPUMilliCores; got != 75 {
		t.Fatalf("expected legacy aggregate sample to be ignored and CPU recommendation 75m, got %dm", got)
	}
}

func TestBuildRightSizingRecommendationAddsPostgresMemoryLimitHeadroom(t *testing.T) {
	t.Parallel()

	recommendation := buildRightSizingRecommendation(
		model.ClusterNodeWorkloadKindBackingService,
		"svc_pg",
		"demo-postgres",
		model.BackingServiceTypePostgres,
		model.WorkloadClassCritical,
		24,
		3,
		&model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512},
		rightSizingUsageSamples("tenant_a", model.ClusterNodeWorkloadKindBackingService, "svc_pg", []rightSizingUsageValue{
			{cpuMilli: 50, memoryMiB: 380},
			{cpuMilli: 80, memoryMiB: 420},
			{cpuMilli: 100, memoryMiB: 432},
		}),
	)

	if !recommendation.Ready || recommendation.Recommended == nil {
		t.Fatalf("expected ready postgres recommendation, got %+v", recommendation)
	}
	if got := recommendation.Recommended.MemoryMebibytes; got != 656 {
		t.Fatalf("expected postgres memory request 656Mi, got %dMi", got)
	}
	if got := recommendation.Recommended.MemoryLimitMebibytes; got != 784 {
		t.Fatalf("expected postgres memory limit with headroom 784Mi, got %dMi", got)
	}
	if recommendation.RequestTarget == nil || recommendation.RequestTarget.CPUMilliCores != 100 {
		t.Fatalf("expected postgres p75 CPU request target with 100m floor, got %+v", recommendation.RequestTarget)
	}
}

func TestApplyAppRightSizingRecommendationQueuesDeployForAppAndPostgres(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Right Size Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:   500,
			MemoryMebibytes: 512,
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   250,
				MemoryMebibytes: 512,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one backing service, got %+v", app.BackingServices)
	}
	postgresService := app.BackingServices[0]
	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})
	samples = append(samples, rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindBackingService, postgresService.ID, []rightSizingUsageValue{
		{cpuMilli: 30, memoryMiB: 100},
		{cpuMilli: 40, memoryMiB: 120},
		{cpuMilli: 60, memoryMiB: 150},
	})...)
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recommendation, op, alreadyCurrent, err := server.applyAppRightSizingRecommendation(context.Background(), app, 24, 3, model.ActorTypeAPIKey, "test-key")
	if err != nil {
		t.Fatalf("apply recommendation: %v", err)
	}
	if alreadyCurrent {
		t.Fatal("expected recommendation to queue a deploy")
	}
	if op == nil || op.DesiredSpec == nil {
		t.Fatalf("expected deploy operation with desired spec, got %+v", op)
	}
	if !recommendation.App.Ready || len(recommendation.BackingServices) != 1 || !recommendation.BackingServices[0].Ready {
		t.Fatalf("expected ready app and postgres recommendations, got %+v", recommendation)
	}

	if got := op.DesiredSpec.Resources; got == nil || got.CPUMilliCores != 30 || got.MemoryMebibytes != 128 || got.MemoryLimitMebibytes != 256 {
		t.Fatalf("unexpected app desired resources: %+v", got)
	}
	if op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.Resources == nil {
		t.Fatalf("expected postgres desired resources, got %+v", op.DesiredSpec.Postgres)
	}
	postgresResources := op.DesiredSpec.Postgres.Resources
	if postgresResources.CPUMilliCores != 100 || postgresResources.CPULimitMilliCores != 0 {
		t.Fatalf("unexpected postgres CPU recommendation: %+v", postgresResources)
	}
	if postgresResources.MemoryMebibytes != 256 || postgresResources.MemoryLimitMebibytes != 384 {
		t.Fatalf("unexpected postgres memory recommendation: %+v", postgresResources)
	}
}

func TestAutoRightSizingQueuesSafeDownscaleWithoutPostgres(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Auto Right Size Down Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 1024,
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
			Resources: &model.ResourceSpec{
				CPUMilliCores:        250,
				MemoryMebibytes:      512,
				MemoryLimitMebibytes: 768,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	postgresService := app.BackingServices[0]
	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})
	samples = append(samples, rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindBackingService, postgresService.ID, []rightSizingUsageValue{
		{cpuMilli: 500, memoryMiB: 768},
		{cpuMilli: 600, memoryMiB: 800},
		{cpuMilli: 700, memoryMiB: 832},
	})...)
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recommendation, op, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply auto recommendation: %v", err)
	}
	if alreadyCurrent || op == nil || op.DesiredSpec == nil {
		t.Fatalf("expected auto right-sizing to queue safe downscale, already_current=%v op=%+v", alreadyCurrent, op)
	}
	if got := op.RequestedByID; got != rightSizingAutoDownscaleRequestedByID {
		t.Fatalf("expected downscale requester %q, got %q", rightSizingAutoDownscaleRequestedByID, got)
	}
	resources := op.DesiredSpec.Resources
	if resources == nil {
		t.Fatal("expected desired app resources")
	}
	if resources.CPUMilliCores != 30 || resources.MemoryMebibytes != 512 || resources.MemoryLimitMebibytes != 1024 {
		t.Fatalf("expected direct CPU guarantee downscale with memory floor preserved, got %+v", resources)
	}
	if op.DesiredSpec.Postgres != nil {
		t.Fatalf("auto right-sizing must not mutate postgres resources, got %+v", op.DesiredSpec.Postgres)
	}
	if !recommendation.App.Ready || len(recommendation.BackingServices) != 1 || !recommendation.BackingServices[0].Ready {
		t.Fatalf("expected ready app and postgres recommendations, got %+v", recommendation)
	}
	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 1 {
		t.Fatalf("expected one auto deploy operation, got %+v", operations)
	}
}

func TestAutoRightSizingActiveDeployReturnsBenignSkip(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Auto Right Size Active Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 1024,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	desired := app.Spec
	desired.Resources = &model.ResourceSpec{CPUMilliCores: 375, MemoryMebibytes: 512, MemoryLimitMebibytes: 1024}
	activeOp, outcome, err := stateStore.CreateAutoscalingDeployOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeSystem,
		RequestedByID:   model.OperationRequestedByRightSizingDownscale,
		AppID:           app.ID,
		DesiredSpec:     &desired,
	})
	if err != nil {
		t.Fatalf("create active autoscaling operation: %v", err)
	}
	if outcome.Decision != store.AutoscalingDeployDecisionQueued {
		t.Fatalf("expected active operation queued, outcome=%+v", outcome)
	}
	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	_, op, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply auto recommendation: %v", err)
	}
	if !alreadyCurrent || op != nil {
		t.Fatalf("expected active deploy benign skip, already_current=%v op=%+v", alreadyCurrent, op)
	}
	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 1 || operations[0].ID != activeOp.ID {
		t.Fatalf("expected only active operation %s, got %+v", activeOp.ID, operations)
	}
}

func TestAutoRightSizingAlreadyCurrentReturnsBenignSkipWithoutOperation(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Auto Right Size Noop Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	source := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo:latest",
		ResolvedImageRef: "ghcr.io/example/demo:latest",
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        500,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 1024,
		},
	}, source)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	_, first, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply initial auto recommendation: %v", err)
	}
	if alreadyCurrent || first == nil {
		t.Fatalf("expected first recommendation to queue operation, already_current=%v op=%+v", alreadyCurrent, first)
	}
	if _, err := stateStore.CompleteManagedOperation(first.ID, "/tmp/demo.yaml", "autoscaling applied"); err != nil {
		t.Fatalf("complete first autoscaling operation: %v", err)
	}

	_, duplicate, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply duplicate auto recommendation: %v", err)
	}
	if !alreadyCurrent || duplicate != nil {
		t.Fatalf("expected already-current benign skip, already_current=%v op=%+v", alreadyCurrent, duplicate)
	}
	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 1 {
		t.Fatalf("expected no duplicate operation after already-current skip, got %+v", operations)
	}
}

func TestAutoRightSizingRecentOOMPreservesMemoryButAllowsCPURequestDownscale(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Auto Right Size OOM Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        1000,
			MemoryMebibytes:      2048,
			MemoryLimitMebibytes: 4096,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	oomOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeSystem,
		RequestedByID:   model.OperationRequestedByOOMRightSizing + "/test",
		AppID:           app.ID,
		DesiredSpec:     &app.Spec,
	})
	if err != nil {
		t.Fatalf("create oom operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(oomOp.ID, "", "oom right-sizing complete"); err != nil {
		t.Fatalf("complete oom operation: %v", err)
	}

	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 80, memoryMiB: 256},
		{cpuMilli: 90, memoryMiB: 320},
		{cpuMilli: 100, memoryMiB: 384},
	})
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	_, op, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply auto recommendation: %v", err)
	}
	if alreadyCurrent || op == nil || op.DesiredSpec == nil || op.DesiredSpec.Resources == nil {
		t.Fatalf("expected recent OOM to preserve memory while queuing CPU request downscale, already_current=%v op=%+v", alreadyCurrent, op)
	}
	if got := op.DesiredSpec.Resources; got.CPUMilliCores != 90 || got.MemoryMebibytes != 2048 || got.MemoryLimitMebibytes != 4096 {
		t.Fatalf("expected CPU-only request downscale with OOM memory preserved, got %+v", got)
	}
	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 2 {
		t.Fatalf("expected prior OOM operation and CPU-only downscale, got %+v", operations)
	}
}

func TestAutoRightSizingQueuesMaterialAppIncreaseWithoutPostgres(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Auto Right Size Up Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)

	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Resources: &model.ResourceSpec{
			CPUMilliCores:        100,
			MemoryMebibytes:      128,
			MemoryLimitMebibytes: 256,
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
			Resources: &model.ResourceSpec{
				CPUMilliCores:        250,
				MemoryMebibytes:      512,
				MemoryLimitMebibytes: 768,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	postgresService := app.BackingServices[0]
	samples := rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindApp, app.ID, []rightSizingUsageValue{
		{cpuMilli: 160, memoryMiB: 300},
		{cpuMilli: 180, memoryMiB: 320},
		{cpuMilli: 200, memoryMiB: 400},
	})
	samples = append(samples, rightSizingUsageSamples(tenant.ID, model.ClusterNodeWorkloadKindBackingService, postgresService.ID, []rightSizingUsageValue{
		{cpuMilli: 500, memoryMiB: 768},
		{cpuMilli: 600, memoryMiB: 800},
		{cpuMilli: 700, memoryMiB: 832},
	})...)
	if err := stateStore.RecordResourceUsageSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	_, op, alreadyCurrent, err := server.applyAutoAppRightSizingRecommendation(app, 24, 3)
	if err != nil {
		t.Fatalf("apply auto recommendation: %v", err)
	}
	if alreadyCurrent || op == nil || op.DesiredSpec == nil {
		t.Fatalf("expected material app increase to queue deploy, already_current=%v op=%+v", alreadyCurrent, op)
	}
	if got := op.RequestedByID; got != rightSizingAutoApplyRequestedByID {
		t.Fatalf("expected auto right-sizing requester %q, got %q", rightSizingAutoApplyRequestedByID, got)
	}
	if op.DesiredSpec.Postgres != nil {
		t.Fatalf("auto right-sizing must not mutate postgres resources, got %+v", op.DesiredSpec.Postgres)
	}
	resources := op.DesiredSpec.Resources
	if resources == nil || resources.CPUMilliCores <= 100 || resources.MemoryMebibytes <= 128 {
		t.Fatalf("expected app resources to increase, got %+v", resources)
	}
}

func TestAutoRightSizingMixedDirectionPrioritizesMaterialUpscale(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{
		CPUMilliCores:        1030,
		MemoryMebibytes:      2288,
		MemoryLimitMebibytes: 4576,
	}
	recommended := &model.ResourceSpec{
		CPUMilliCores:        1515,
		MemoryMebibytes:      2256,
		MemoryLimitMebibytes: 4512,
	}

	decision := autoRightSizingAppResourceChange(current, recommended)
	if !decision.allowed {
		t.Fatal("expected a material CPU increase to be allowed even when memory recommendations decrease")
	}
	if decision.downscale {
		t.Fatal("expected mixed-direction recommendation to be treated as an upscale")
	}
	if decision.requestedByID != rightSizingAutoApplyRequestedByID {
		t.Fatalf("expected upscale requester %q, got %q", rightSizingAutoApplyRequestedByID, decision.requestedByID)
	}
	if decision.resources == nil {
		t.Fatal("expected an upscale resource target")
	}
	if got := decision.resources.CPUMilliCores; got != recommended.CPUMilliCores {
		t.Fatalf("expected CPU request to increase to %dm, got %dm", recommended.CPUMilliCores, got)
	}
	if got := decision.resources.MemoryMebibytes; got != current.MemoryMebibytes {
		t.Fatalf("expected memory request to remain at %dMi, got %dMi", current.MemoryMebibytes, got)
	}
	if got := decision.resources.MemoryLimitMebibytes; got != current.MemoryLimitMebibytes {
		t.Fatalf("expected memory limit to remain at %dMi, got %dMi", current.MemoryLimitMebibytes, got)
	}
}

func TestAutoRightSizingAdmissionBlocksUnsupportedRWO(t *testing.T) {
	desired := model.AppSpec{
		Ports:     []int{8080},
		Replicas:  1,
		Workspace: &model.AppWorkspaceSpec{StorageClassName: model.AppStorageClassFugueWorkspaceRWO},
	}
	reason, err := (&Server{}).autoRightSizingAdmission(model.App{}, desired, nil)
	if err != nil {
		t.Fatalf("RWO admission: %v", err)
	}
	if !strings.Contains(reason, "does not support same-node online dual mount") {
		t.Fatalf("expected unsupported RWO reason, got %q", reason)
	}
}

func TestAutoRightSizingAdmissionAllowsSharedRWX(t *testing.T) {
	desired := model.AppSpec{
		Ports:    []int{8080},
		Replicas: 1,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeSharedProjectRWX,
			StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
			Mounts:           []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}},
		},
	}
	reason, err := (&Server{}).autoRightSizingAdmission(model.App{}, desired, nil)
	if err != nil || reason != "" {
		t.Fatalf("expected shared RWX admission to pass, reason=%q err=%v", reason, err)
	}
}

func TestAutoRightSizingAdmissionBlocksInsufficientActualCPUHeadroom(t *testing.T) {
	server := &Server{
		clusterNodeInventoryCache: newExpiringResponseCache[[]clusterNodeSnapshot](time.Minute),
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return nil, errors.New("cached inventory should be used")
		},
	}
	allocatableCPU, usedCPU, freeMemory := int64(1000), int64(800), int64(512*1024*1024)
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{
		node: model.ClusterNode{
			Status: "ready",
			CPU: &model.ClusterNodeCPUStats{
				AllocatableMilliCores: &allocatableCPU,
				UsedMilliCores:        &usedCPU,
			},
			Memory: &model.ClusterNodeMemoryStats{SchedulableFreeBytes: &freeMemory},
		},
	}})
	desired := model.AppSpec{
		Ports: []int{8080}, Replicas: 1,
		Resources: &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 256},
	}
	reason, err := server.autoRightSizingAdmission(model.App{}, desired, &model.ResourceSpec{CPUMilliCores: 200})
	if err == nil || reason != "" || !strings.Contains(err.Error(), "actual CPU headroom") {
		t.Fatalf("expected actual CPU headroom rejection, reason=%q err=%v", reason, err)
	}
}

func TestAutoRightSizingAdmissionIgnoresCPURequestPressure(t *testing.T) {
	server := &Server{
		clusterNodeInventoryCache: newExpiringResponseCache[[]clusterNodeSnapshot](time.Minute),
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return nil, errors.New("cached inventory should be used")
		},
	}
	allocatableCPU, usedCPU, schedulableCPU, freeMemory := int64(1000), int64(100), int64(0), int64(512*1024*1024)
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{
		node: model.ClusterNode{
			Status: "ready",
			CPU: &model.ClusterNodeCPUStats{
				AllocatableMilliCores:     &allocatableCPU,
				UsedMilliCores:            &usedCPU,
				SchedulableFreeMilliCores: &schedulableCPU,
			},
			Memory: &model.ClusterNodeMemoryStats{SchedulableFreeBytes: &freeMemory},
		},
	}})
	desired := model.AppSpec{
		Ports: []int{8080}, Replicas: 1,
		Resources: &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 256},
	}
	reason, err := server.autoRightSizingAdmission(model.App{}, desired, &model.ResourceSpec{CPUMilliCores: 500})
	if err != nil || reason != "" {
		t.Fatalf("expected request pressure to be ignored when actual CPU and memory are safe, reason=%q err=%v", reason, err)
	}
}

func TestAutoRightSizingAdmissionBlocksInsufficientMemory(t *testing.T) {
	server := &Server{
		clusterNodeInventoryCache: newExpiringResponseCache[[]clusterNodeSnapshot](time.Minute),
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return nil, errors.New("cached inventory should be used")
		},
	}
	allocatableCPU, usedCPU, freeMemory := int64(1000), int64(100), int64(128*1024*1024)
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{
		node: model.ClusterNode{
			Status: "ready",
			CPU: &model.ClusterNodeCPUStats{
				AllocatableMilliCores: &allocatableCPU,
				UsedMilliCores:        &usedCPU,
			},
			Memory: &model.ClusterNodeMemoryStats{SchedulableFreeBytes: &freeMemory},
		},
	}})
	desired := model.AppSpec{
		Ports: []int{8080}, Replicas: 1,
		Resources: &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 256},
	}
	reason, err := server.autoRightSizingAdmission(model.App{}, desired, &model.ResourceSpec{CPUMilliCores: 200})
	if err == nil || reason != "" || !strings.Contains(err.Error(), "schedulable memory") {
		t.Fatalf("expected memory rejection, reason=%q err=%v", reason, err)
	}
}

func TestAutoRightSizingFailureBackoffMatchesTargetResources(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, _ := stateStore.CreateTenant("right-sizing backoff")
	project, _ := stateStore.CreateProject(tenant.ID, "apps", "")
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "demo", Replicas: 1, Resources: &model.ResourceSpec{CPUMilliCores: 100}})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	desired := app.Spec
	desired.Resources = &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 256}
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeSystem, RequestedByID: model.OperationRequestedByRightSizing,
		DesiredSpec: &desired,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil || !found {
		t.Fatalf("claim operation: found=%t err=%v", found, err)
	}
	if _, err := stateStore.FailOperation(op.ID, "insufficient capacity"); err != nil {
		t.Fatalf("fail operation: %v", err)
	}
	server := &Server{store: stateStore}
	blocked, err := server.appHasRecentAutoRightSizingFailure(app, desired, time.Now().Add(-rightSizingAutoFailureBackoff))
	if err != nil || !blocked {
		t.Fatalf("expected matching failure backoff, blocked=%t err=%v", blocked, err)
	}
}

func TestAutoRightSizingCPURequestDownscaleConvergesDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     *model.ResourceSpec
		recommended *model.ResourceSpec
		wantCPU     int64
	}{
		{
			name: "below managed default",
			current: &model.ResourceSpec{
				CPUMilliCores:   150,
				MemoryMebibytes: 512,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   25,
				MemoryMebibytes: 512,
			},
			wantCPU: 25,
		},
		{
			name: "low CPU service",
			current: &model.ResourceSpec{
				CPUMilliCores:   50,
				MemoryMebibytes: 512,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   25,
				MemoryMebibytes: 512,
			},
			wantCPU: 25,
		},
		{
			name: "low CPU rounding boundary",
			current: &model.ResourceSpec{
				CPUMilliCores:   15,
				MemoryMebibytes: 128,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   10,
				MemoryMebibytes: 128,
			},
			wantCPU: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decision := autoRightSizingAppResourceChange(test.current, test.recommended)
			if !decision.allowed || !decision.downscale || decision.resources == nil {
				t.Fatalf("expected CPU downscale, got %+v", decision)
			}
			if got := decision.resources.CPUMilliCores; got != test.wantCPU {
				t.Fatalf("expected CPU target %dm, got %dm", test.wantCPU, got)
			}
			current := *test.current
			if decision.resources.CPUMilliCores >= current.CPUMilliCores {
				t.Fatalf("downscale must lower CPU from %dm, got %dm", current.CPUMilliCores, decision.resources.CPUMilliCores)
			}
			if decision.resources.MemoryMebibytes != current.MemoryMebibytes {
				t.Fatalf("CPU-only downscale must preserve memory at %dMi, got %dMi", current.MemoryMebibytes, decision.resources.MemoryMebibytes)
			}
		})
	}
}

func TestAutoRightSizingNilResourcesAreZeroRequestUpscale(t *testing.T) {
	t.Parallel()
	recommended := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 64}
	decision := autoRightSizingAppResourceChange(nil, recommended)
	if !decision.allowed || decision.downscale || decision.resources == nil {
		t.Fatalf("expected nil resources to be an allowed upscale, got %+v", decision)
	}
	if decision.resources.CPUMilliCores != 25 || decision.resources.MemoryMebibytes != 64 {
		t.Fatalf("unexpected upscale target: %+v", decision.resources)
	}
}

func TestAutoRightSizingSmallCPURequestIncreaseUsesGuaranteeHysteresis(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 128}
	recommended := &model.ResourceSpec{CPUMilliCores: 40, MemoryMebibytes: 128}
	decision := autoRightSizingAppResourceChange(current, recommended)
	if !decision.allowed || decision.downscale || decision.resources == nil || decision.resources.CPUMilliCores != 40 {
		t.Fatalf("expected material small CPU guarantee increase to apply, got %+v", decision)
	}
}

func TestAutoRightSizingLowCPUDownscaleKeepsRatioHysteresis(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{CPUMilliCores: 30, MemoryMebibytes: 128}
	recommended := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 128}
	decision := autoRightSizingAppResourceChange(current, recommended)
	if decision.allowed {
		t.Fatalf("expected a CPU change below 20%% to remain blocked, got %+v", decision)
	}
}

func TestAutoRightSizingDownscaleDimensionsAreIndependent(t *testing.T) {
	t.Parallel()

	t.Run("CPU-only downscale preserves memory", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{
			CPUMilliCores:        50,
			MemoryMebibytes:      288,
			MemoryLimitMebibytes: 576,
		}
		recommended := &model.ResourceSpec{
			CPUMilliCores:        25,
			MemoryMebibytes:      256,
			MemoryLimitMebibytes: 512,
		}
		decision := autoRightSizingAppResourceChange(current, recommended)
		if !decision.allowed || decision.resources == nil {
			t.Fatalf("expected CPU downscale, got %+v", decision)
		}
		if got := decision.resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 288 || got.MemoryLimitMebibytes != 576 {
			t.Fatalf("expected only CPU to downscale, got %+v", got)
		}
	})

	t.Run("memory-only downscale preserves CPU", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{
			CPUMilliCores:        30,
			MemoryMebibytes:      1024,
			MemoryLimitMebibytes: 2048,
		}
		recommended := &model.ResourceSpec{
			CPUMilliCores:        25,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 1024,
		}
		decision := autoRightSizingAppResourceChange(current, recommended)
		if !decision.allowed || decision.resources == nil {
			t.Fatalf("expected memory downscale, got %+v", decision)
		}
		if got := decision.resources; got.CPUMilliCores != 30 || got.MemoryMebibytes != 768 || got.MemoryLimitMebibytes != 1536 {
			t.Fatalf("expected only memory to downscale, got %+v", got)
		}
	})

	t.Run("minor memory increase does not block CPU downscale", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{CPUMilliCores: 50, MemoryMebibytes: 256}
		recommended := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 288}
		decision := autoRightSizingAppResourceChange(current, recommended)
		if !decision.allowed || decision.resources == nil {
			t.Fatalf("expected CPU downscale, got %+v", decision)
		}
		if got := decision.resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 256 {
			t.Fatalf("expected CPU downscale with memory preserved, got %+v", got)
		}
	})
}

func TestAutoRightSizingDownscaleNeverRaisesResourcesToDefaults(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{
		CPUMilliCores:        150,
		MemoryMebibytes:      400,
		MemoryLimitMebibytes: 800,
	}
	recommended := &model.ResourceSpec{
		CPUMilliCores:        25,
		MemoryMebibytes:      64,
		MemoryLimitMebibytes: 128,
	}
	decision := autoRightSizingAppResourceChange(current, recommended)
	if !decision.allowed || decision.resources == nil {
		t.Fatalf("expected CPU downscale, got %+v", decision)
	}
	if got := decision.resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 400 || got.MemoryLimitMebibytes != 800 {
		t.Fatalf("downscale must not raise resources to managed defaults, got %+v", got)
	}
}

type rightSizingUsageValue struct {
	cpuMilli  int64
	memoryMiB int64
}

func rightSizingUsageSamples(tenantID, targetKind, targetID string, values []rightSizingUsageValue) []model.ResourceUsageSample {
	now := time.Now().UTC().Add(-time.Duration(len(values)) * time.Minute)
	out := make([]model.ResourceUsageSample, 0, len(values))
	for index, value := range values {
		cpu := value.cpuMilli
		memory := value.memoryMiB * 1024 * 1024
		out = append(out, model.ResourceUsageSample{
			TenantID:      tenantID,
			TargetKind:    rightSizingSampleTargetKind(targetKind),
			TargetID:      targetID,
			ObservedAt:    now.Add(time.Duration(index) * time.Minute),
			CPUMilliCores: &cpu,
			MemoryBytes:   &memory,
		})
	}
	return out
}
