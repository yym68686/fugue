package resourcesizing

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRecommendAppReadsOnlyOwnedPostgresAndVersionedReplicaSamples(t *testing.T) {
	since := time.Unix(200, 0)
	postgres := model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{}}
	app := model.App{ID: "app", TenantID: "tenant", BackingServices: []model.BackingService{
		{ID: "foreign", TenantID: "tenant", OwnerAppID: "other-app", Type: model.BackingServiceTypePostgres, Spec: postgres},
		{ID: "nil-postgres", TenantID: "tenant", OwnerAppID: "app", Type: model.BackingServiceTypePostgres},
		{ID: "other-type", TenantID: "tenant", OwnerAppID: "app", Type: "other", Spec: postgres},
		{ID: "database", TenantID: "tenant", OwnerAppID: "app", Type: model.BackingServiceTypePostgres, Spec: postgres},
	}}
	want := map[string]string{"app": "right_sizing_app_per_replica_v1", "database": "right_sizing_backing_service_per_replica_v1"}
	out, err := RecommendApp(app, 2, 1, since, func(tenant, kind, id string, captured time.Time) ([]model.ResourceUsageSample, error) {
		if tenant != app.TenantID || !captured.Equal(since) || want[id] != kind {
			t.Fatalf("unexpected sample scope: %s %s %s %v", tenant, kind, id, captured)
		}
		delete(want, id)
		return nil, nil
	})
	if err != nil || len(want) != 0 || out.App.TargetID != "app" || len(out.BackingServices) != 1 || out.BackingServices[0].TargetID != "database" {
		t.Fatalf("recommendation=%+v missing reads=%v err=%v", out, want, err)
	}
}

func TestBuildRightSizingRecommendationUsesUsagePercentilesAndClassPolicy(t *testing.T) {
	t.Parallel()

	samples := rightSizingUsageSamples("tenant_a", model.ClusterNodeWorkloadKindApp, "app_a", []rightSizingUsageValue{
		{cpuMilli: 20, memoryMiB: 64},
		{cpuMilli: 30, memoryMiB: 80},
		{cpuMilli: 50, memoryMiB: 100},
	})

	recommendation := Recommend(
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
	recommendation := Recommend(
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
	if recommendation.RequestTarget == nil || recommendation.RequestTarget.CPUMilliCores != 20 {
		t.Fatalf("expected p50 CPU request target 20m, got %+v", recommendation.RequestTarget)
	}
	if recommendation.RequestTarget.CPULimitMilliCores != 500 {
		t.Fatalf("expected explicit CPU limit to be preserved, got %+v", recommendation.RequestTarget)
	}
	if recommendation.RequestTarget.MemoryMebibytes != 512 || recommendation.RequestTarget.MemoryLimitMebibytes != 768 {
		t.Fatalf("expected request target to preserve unobserved memory settings, got %+v", recommendation.RequestTarget)
	}
}

func TestBuildRightSizingRecommendationAddsPostgresMemoryLimitHeadroom(t *testing.T) {
	t.Parallel()

	recommendation := Recommend(
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
		t.Fatalf("expected postgres p75 CPU request target 100m, got %+v", recommendation.RequestTarget)
	}
}

func TestBuildRightSizingRecommendationUsesOneMilliCPUFloor(t *testing.T) {
	t.Parallel()

	for _, workloadClass := range []string{"", model.WorkloadClassDemo, model.WorkloadClassService, model.WorkloadClassBatch, model.WorkloadClassCritical} {
		for _, serviceType := range []string{"", model.BackingServiceTypePostgres} {
			t.Run(workloadClass+"/"+serviceType, func(t *testing.T) {
				t.Parallel()

				for _, usage := range []struct {
					cpuMilli int64
					want     int64
				}{
					{cpuMilli: 0, want: 1},
					{cpuMilli: 1, want: 5}, // Preserve the existing 5m rounding step.
					{cpuMilli: 20, want: 20},
				} {
					recommendation := Recommend(
						model.ClusterNodeWorkloadKindApp, "app_a", "demo", serviceType, workloadClass,
						24, 3, &model.ResourceSpec{CPUMilliCores: 100},
						rightSizingUsageSamples("tenant_a", model.ClusterNodeWorkloadKindApp, "app_a", []rightSizingUsageValue{
							{cpuMilli: usage.cpuMilli, memoryMiB: 64},
							{cpuMilli: usage.cpuMilli, memoryMiB: 64},
							{cpuMilli: usage.cpuMilli, memoryMiB: 64},
						}),
					)
					if !recommendation.Ready || recommendation.RequestTarget == nil {
						t.Fatalf("expected ready recommendation, got %+v", recommendation)
					}
					if recommendation.Policy.CPUFloorMilli != 1 || recommendation.RequestTarget.CPUMilliCores != usage.want {
						t.Fatalf("CPU usage %dm: expected floor 1m and request %dm, got policy %+v, target %+v", usage.cpuMilli, usage.want, recommendation.Policy, recommendation.RequestTarget)
					}
				}
			})
		}
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
			TenantID: tenantID, TargetKind: targetKind, TargetID: targetID,
			ObservedAt: now.Add(time.Duration(index) * time.Minute), CPUMilliCores: &cpu, MemoryBytes: &memory,
		})
	}
	return out
}
