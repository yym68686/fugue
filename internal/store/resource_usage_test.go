package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRecordResourceUsageSamplesPrunesAndListsByTarget(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	oldCPU := int64(10)
	recentCPU := int64(40)
	otherCPU := int64(90)
	if err := s.RecordResourceUsageSamples([]model.ResourceUsageSample{
		{
			TenantID:      "tenant_a",
			TargetKind:    model.ClusterNodeWorkloadKindApp,
			TargetID:      "app_a",
			ObservedAt:    now.Add(-3 * time.Hour),
			CPUMilliCores: &oldCPU,
		},
		{
			TenantID:      "tenant_a",
			TargetKind:    model.ClusterNodeWorkloadKindApp,
			TargetID:      "app_a",
			ObservedAt:    now.Add(-1 * time.Hour),
			CPUMilliCores: &recentCPU,
		},
		{
			TenantID:      "tenant_a",
			TargetKind:    model.ClusterNodeWorkloadKindBackingService,
			TargetID:      "app_a",
			ObservedAt:    now.Add(-30 * time.Minute),
			CPUMilliCores: &otherCPU,
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}
	if err := s.RecordResourceUsageSamples(nil, now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("prune samples: %v", err)
	}

	samples, err := s.ListResourceUsageSamples("tenant_a", model.ClusterNodeWorkloadKindApp, "app_a", time.Time{})
	if err != nil {
		t.Fatalf("list samples: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected one current app sample, got %+v", samples)
	}
	if samples[0].CPUMilliCores == nil || *samples[0].CPUMilliCores != recentCPU {
		t.Fatalf("expected recent cpu sample %d, got %+v", recentCPU, samples[0].CPUMilliCores)
	}
}

func TestCreateAppRejectsInvalidResourcePolicyFields(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Resource Policy Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	cases := []struct {
		name string
		spec model.AppSpec
	}{
		{
			name: "invalid workload class",
			spec: baseResourcePolicyTestSpec(func(spec *model.AppSpec) {
				spec.WorkloadClass = "large"
			}),
		},
		{
			name: "invalid right-sizing mode",
			spec: baseResourcePolicyTestSpec(func(spec *model.AppSpec) {
				spec.RightSizing = &model.AppRightSizingSpec{Mode: "aggressive"}
			}),
		},
		{
			name: "cpu limit without cpu request",
			spec: baseResourcePolicyTestSpec(func(spec *model.AppSpec) {
				spec.Resources = &model.ResourceSpec{CPULimitMilliCores: 500, MemoryMebibytes: 128}
			}),
		},
		{
			name: "memory limit without memory request",
			spec: baseResourcePolicyTestSpec(func(spec *model.AppSpec) {
				spec.Resources = &model.ResourceSpec{CPUMilliCores: 100, MemoryLimitMebibytes: 512}
			}),
		},
	}

	for index, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := s.CreateApp(tenant.ID, project.ID, "demo-invalid-"+string(rune('a'+index)), "", tt.spec); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
		})
	}
}

func baseResourcePolicyTestSpec(mutator func(*model.AppSpec)) model.AppSpec {
	spec := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	mutator(&spec)
	return spec
}
