package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestReconcileManagedPostgresRuntimeResourcesIsServerOwned(t *testing.T) {
	t.Parallel()

	current := &model.AppPostgresSpec{
		RuntimeResources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 768},
	}
	omitted := &model.AppPostgresSpec{}
	if err := reconcileManagedPostgresRuntimeResources(omitted, current); err != nil {
		t.Fatalf("preserve omitted runtime target: %v", err)
	}
	if omitted.RuntimeResources == nil || *omitted.RuntimeResources != *current.RuntimeResources {
		t.Fatalf("runtime target was not preserved: %+v", omitted.RuntimeResources)
	}
	if omitted.RuntimeResources == current.RuntimeResources {
		t.Fatal("preserved runtime target aliases persisted state")
	}

	roundTrip := &model.AppPostgresSpec{RuntimeResources: model.CloneResourceSpec(current.RuntimeResources)}
	if err := reconcileManagedPostgresRuntimeResources(roundTrip, current); err != nil {
		t.Fatalf("accept exact read-only round trip: %v", err)
	}

	changed := &model.AppPostgresSpec{RuntimeResources: &model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 768}}
	if err := reconcileManagedPostgresRuntimeResources(changed, current); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected changed runtime target to fail closed, got %v", err)
	}
	created := &model.AppPostgresSpec{RuntimeResources: model.CloneResourceSpec(current.RuntimeResources)}
	if err := reconcileManagedPostgresRuntimeResources(created, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected client-created runtime target to fail closed, got %v", err)
	}
}

func TestNormalizePostgresSpecResourcesKeepsBootstrapAndRuntimeTargetsSeparate(t *testing.T) {
	t.Parallel()

	spec := &model.AppPostgresSpec{
		Resources: &model.ResourceSpec{
			CPUMilliCores:   100,
			MemoryMebibytes: 512,
		},
		RuntimeResources: &model.ResourceSpec{
			CPUMilliCores:        250,
			MemoryMebibytes:      768,
			CPULimitMilliCores:   500,
			MemoryLimitMebibytes: 1024,
		},
	}
	if err := normalizePostgresSpecResources(spec); err != nil {
		t.Fatalf("normalize postgres resources: %v", err)
	}
	if spec.Resources.CPUMilliCores != 100 || spec.Resources.MemoryMebibytes != 512 {
		t.Fatalf("bootstrap resources drifted: %+v", spec.Resources)
	}
	if spec.RuntimeResources.CPUMilliCores != 250 || spec.RuntimeResources.MemoryMebibytes != 768 {
		t.Fatalf("runtime resources drifted: %+v", spec.RuntimeResources)
	}
	if spec.Resources == spec.RuntimeResources {
		t.Fatal("bootstrap and runtime resources must not alias")
	}
}

func TestNormalizePostgresSpecResourcesRejectsInvalidRuntimeTarget(t *testing.T) {
	t.Parallel()

	spec := &model.AppPostgresSpec{
		Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512},
		RuntimeResources: &model.ResourceSpec{
			CPUMilliCores:      500,
			MemoryMebibytes:    512,
			CPULimitMilliCores: 250,
		},
	}
	if err := normalizePostgresSpecResources(spec); err == nil {
		t.Fatal("expected runtime request above limit to fail closed")
	}
}

func TestPostgresEffectiveResourcesUsesRuntimeTarget(t *testing.T) {
	t.Parallel()

	bootstrap := model.ResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 1024}
	runtimeTarget := model.ResourceSpec{CPUMilliCores: 150, MemoryMebibytes: 640}
	got := postgresEffectiveResources(model.AppPostgresSpec{
		Resources:        &bootstrap,
		RuntimeResources: &runtimeTarget,
		StorageSize:      "20Gi",
	})
	if got.CPUMilliCores != 150 || got.MemoryMebibytes != 640 || got.StorageGibibytes != 20 {
		t.Fatalf("expected runtime target to drive effective resources, got %+v", got)
	}
}

func TestCloneBackingServiceDetachesRuntimeResources(t *testing.T) {
	t.Parallel()

	service := model.BackingService{Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Resources:        &model.ResourceSpec{CPUMilliCores: 100},
		RuntimeResources: &model.ResourceSpec{CPUMilliCores: 250},
	}}}
	cloned := cloneBackingService(service)
	cloned.Spec.Postgres.RuntimeResources.CPUMilliCores = 500
	if service.Spec.Postgres.RuntimeResources.CPUMilliCores != 250 {
		t.Fatalf("cloned runtime resources alias original: original=%+v clone=%+v", service, cloned)
	}
}

func TestCreateManagedPostgresRejectsClientRuntimeResources(t *testing.T) {
	t.Parallel()

	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("client-runtime-target")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "client-runtime-target", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	target := &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512}
	if _, err := s.CreateBackingService(tenant.ID, project.ID, "runtime-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{RuntimeResources: target},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected backing-service runtime target to be rejected, got %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "app", "", model.AppSpec{
		Image:    "nginx:1.27",
		Replicas: 1,
		Postgres: &model.AppPostgresSpec{RuntimeResources: target},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected app runtime target to be rejected, got %v", err)
	}
}
