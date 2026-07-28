package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestTenantResourceCommitmentIncludesOwnedRuntimeWorkloads(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Owned Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	runtimeObj, _, err := stateStore.CreateRuntime(tenant.ID, "owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "", runtimeObj.ID)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Resources: &model.ResourceSpec{
			CPUMilliCores:   250,
			MemoryMebibytes: 512,
		},
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			User:      "demo",
			Password:  "secret",
			RuntimeID: runtimeObj.ID,
			Resources: &model.ResourceSpec{
				CPUMilliCores:   500,
				MemoryMebibytes: 1024,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		index := findOwnedBackingServiceByAppAndType(state, app.ID, model.BackingServiceTypePostgres)
		if index < 0 {
			return ErrNotFound
		}
		state.BackingServices[index].Spec.Postgres.RuntimeResources = &model.ResourceSpec{
			CPUMilliCores:   300,
			MemoryMebibytes: 768,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed server-owned runtime target: %v", err)
	}

	commitment, err := stateStore.GetTenantResourceCommitment(tenant.ID)
	if err != nil {
		t.Fatalf("get tenant commitment: %v", err)
	}
	if commitment.CPUMilliCores != 550 || commitment.MemoryMebibytes != 1280 {
		t.Fatalf("expected owned runtime resources in tenant commitment, got %+v", commitment)
	}
}
