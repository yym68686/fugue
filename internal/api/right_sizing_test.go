package api

import (
	"context"
	"path/filepath"
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
	if got := recommendation.Recommended.MemoryMebibytes; got != 512 {
		t.Fatalf("expected memory request to be preserved, got %dMi", got)
	}
	if got := recommendation.Recommended.MemoryLimitMebibytes; got != 768 {
		t.Fatalf("expected memory limit to be preserved, got %dMi", got)
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

	if got := op.DesiredSpec.Resources; got == nil || got.CPUMilliCores != 75 || got.MemoryMebibytes != 128 || got.MemoryLimitMebibytes != 256 {
		t.Fatalf("unexpected app desired resources: %+v", got)
	}
	if op.DesiredSpec.Postgres == nil || op.DesiredSpec.Postgres.Resources == nil {
		t.Fatalf("expected postgres desired resources, got %+v", op.DesiredSpec.Postgres)
	}
	postgresResources := op.DesiredSpec.Postgres.Resources
	if postgresResources.CPUMilliCores != 100 || postgresResources.CPULimitMilliCores != 100 {
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
	if resources.CPUMilliCores != 375 || resources.MemoryMebibytes != 512 || resources.MemoryLimitMebibytes != 1024 {
		t.Fatalf("expected gradual CPU-only downscale with memory floor preserved, got %+v", resources)
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

func TestAutoRightSizingSkipsDownscaleAfterRecentOOMRightSizing(t *testing.T) {
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
	if !alreadyCurrent || op != nil {
		t.Fatalf("expected recent OOM right-sizing to block auto downscale, already_current=%v op=%+v", alreadyCurrent, op)
	}
	operations, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(operations) != 1 {
		t.Fatalf("expected only the prior OOM operation, got %+v", operations)
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
			TargetKind:    targetKind,
			TargetID:      targetID,
			ObservedAt:    now.Add(time.Duration(index) * time.Minute),
			CPUMilliCores: &cpu,
			MemoryBytes:   &memory,
		})
	}
	return out
}
