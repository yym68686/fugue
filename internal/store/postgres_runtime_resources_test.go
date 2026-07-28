package store

import (
	"errors"
	"path/filepath"
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

func TestBackingServiceRuntimeResourcesPersistAndOrdinaryUpdateCannotChangeThem(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("runtime-target")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "runtime-target", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "runtime-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}
	runtimeTarget := model.ResourceSpec{
		CPUMilliCores:        150,
		MemoryMebibytes:      640,
		CPULimitMilliCores:   500,
		MemoryLimitMebibytes: 1024,
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, service.ID)
		if index < 0 {
			return ErrNotFound
		}
		state.BackingServices[index].Spec.Postgres.RuntimeResources = model.CloneResourceSpec(&runtimeTarget)
		return nil
	}); err != nil {
		t.Fatalf("seed server-owned runtime target: %v", err)
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	persisted, err := reopened.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted backing service: %v", err)
	}
	if persisted.Spec.Postgres.RuntimeResources == nil || *persisted.Spec.Postgres.RuntimeResources != runtimeTarget {
		t.Fatalf("runtime target did not survive persistence: %+v", persisted.Spec.Postgres.RuntimeResources)
	}

	ordinarySpec := cloneBackingServiceSpec(persisted.Spec)
	ordinarySpec.Postgres.RuntimeResources = nil
	updated, err := reopened.UpdateBackingServiceSpec(service.ID, ordinarySpec)
	if err != nil {
		t.Fatalf("ordinary update with omitted read-only target: %v", err)
	}
	if updated.Spec.Postgres.RuntimeResources == nil || *updated.Spec.Postgres.RuntimeResources != runtimeTarget {
		t.Fatalf("ordinary update dropped runtime target: %+v", updated.Spec.Postgres.RuntimeResources)
	}

	changedSpec := cloneBackingServiceSpec(updated.Spec)
	changedSpec.Postgres.RuntimeResources.CPUMilliCores++
	if _, err := reopened.UpdateBackingServiceSpec(service.ID, changedSpec); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ordinary runtime target change to fail closed, got %v", err)
	}
}

func TestCreateManagedPostgresRejectsClientRuntimeResources(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
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
