package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestStorageUnmountOperationCommitsOnlyThePlannedMount(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		s := newStoreTestDatabase(t, "")
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		tenant, err := s.CreateTenant("unmount-test")
		if err != nil {
			t.Fatal(err)
		}
		checkStorageUnmountCommit(t, s, tenant.ID)
	})
	t.Run("postgres", func(t *testing.T) {
		s := billingBatchPGStore(t)
		if err := s.ensureDatabaseReady(); err != nil {
			t.Fatal(err)
		}
		if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), s.databaseURL); err != nil {
			t.Fatal(err)
		}
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		checkStorageUnmountCommit(t, s, billingBatchPGTenant(t, s).ID)
	})
}

func checkStorageUnmountCommit(t *testing.T, s *Store, tenant string) {
	t.Helper()
	project, err := s.CreateProject(tenant, "unmount", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant, project.ID, "catalog", "", model.AppSpec{
		Image: "registry.invalid/catalog:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080},
		Resources: &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 512}, Env: map[string]string{"CATALOG_KEY": "kept"},
		PersistentStorage: &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeMovableRWO,
			StorageClassName: model.AppStorageClassFugueLocalRWO, StorageSize: "1Gi", ClaimName: "catalog-data",
			Mounts: []model.AppPersistentStorageMount{{Kind: "file", Path: "/catalog.conf", Mode: 0o400, SeedContent: "keep-on-volume"},
				{Kind: "directory", Path: "/data", Mode: 0o755}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := model.AppSpecWithoutPersistentMount(app.Spec, "/catalog.conf")
	if err != nil {
		t.Fatal(err)
	}
	req := model.AppStorageUnmountRequest{Path: "/catalog.conf", AuthorizeOverlap: true,
		Probe: model.AppStorageUnmountProbe{Path: "/v1/catalog", BearerEnv: "CATALOG_KEY", ExpectedBodyContains: "catalog_version"}}
	op, err := s.CreateOperationForAppSpec(model.Operation{AppID: app.ID, TenantID: tenant,
		Type: model.OperationTypeAppStorageUnmount, DesiredSpec: &next, StorageUnmount: &req}, model.AppSpecSHA256(app.Spec))
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !ok {
		t.Fatalf("claim: %t %v", ok, err)
	}
	stored, err := s.GetOperation(op.ID)
	if err != nil || stored.StorageUnmount == nil || *stored.StorageUnmount != req || stored.ConfigBaseSpec == nil {
		t.Fatalf("operation lost its immutable request or baseline: %v", err)
	}
	if _, err := cancelOperationForTest(s, op, "test"); !errors.Is(err, ErrConflict) {
		t.Fatalf("running operation cancellation was not fenced: %v", err)
	}
	changed := claimed
	wrong := req
	wrong.Probe.ExpectedBodyContains = "different"
	changed.StorageUnmount = &wrong
	if _, err := s.CompleteManagedOperationGuarded(changed, &app, "", "verified", nil, nil, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("altered request completion was not fenced: %v", err)
	}
	if _, err := s.CompleteManagedOperationGuarded(stored, &app, "", "verified", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(current.Spec, next) {
		t.Fatalf("completion did not preserve the exact remaining spec: %v", err)
	}
}
