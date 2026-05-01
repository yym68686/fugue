package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
)

func TestBuildMovableRWOCopyPlanConvertsDirectSharedProjectMount(t *testing.T) {
	current := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeSharedProjectRWX,
				StorageClassName: "fugue-rwx",
				SharedSubPath:    "sessions/demo",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	desired := current
	desired.Spec.RuntimeID = "runtime_b"
	desired.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: "fugue-local-rwo",
		Mounts: []model.AppPersistentStorageMount{
			{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/workspace",
			},
		},
	}

	svc := &Service{}
	plan, prepared, changed, err := svc.buildMovableRWOCopyPlan(context.Background(), model.Operation{Type: model.OperationTypeDeploy, ID: "op_test"}, current, desired)
	if err != nil {
		t.Fatalf("build copy plan: %v", err)
	}
	if changed {
		t.Fatal("shared-project conversion should not need a generated claim name")
	}
	if plan == nil {
		t.Fatal("expected copy plan")
	}
	if got := plan.sourceMountSubPath; got != "sessions/demo" {
		t.Fatalf("expected source shared subpath, got %q", got)
	}
	if got := plan.targetCopyPath; got == "" || got == "." {
		t.Fatalf("expected direct shared content to copy into target mount subpath, got %q", got)
	}
	if got := prepared.Spec.PersistentStorage.SharedSubPath; got != "" {
		t.Fatalf("expected movable RWO target spec to clear shared subpath, got %q", got)
	}
}

func TestBuildMovableRWOCopyPlanClearsStaleSharedSubPath(t *testing.T) {
	current := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeSharedProjectRWX,
				StorageClassName: "fugue-rwx",
				SharedSubPath:    "sessions/demo",
				Mounts: []model.AppPersistentStorageMount{
					{
						Kind: model.AppPersistentStorageMountKindDirectory,
						Path: "/workspace",
					},
				},
			},
		},
	}
	desired := current
	desired.Spec.PersistentStorage = &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: "fugue-local-rwo",
		SharedSubPath:    "sessions/demo",
		Mounts: []model.AppPersistentStorageMount{
			{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/workspace",
			},
		},
	}

	svc := &Service{}
	_, prepared, changed, err := svc.buildMovableRWOCopyPlan(context.Background(), model.Operation{Type: model.OperationTypeDeploy, ID: "op_test"}, current, desired)
	if err != nil {
		t.Fatalf("build copy plan: %v", err)
	}
	if !changed {
		t.Fatal("expected stale shared subpath cleanup to mark desired spec changed")
	}
	if got := prepared.Spec.PersistentStorage.SharedSubPath; got != "" {
		t.Fatalf("expected stale shared subpath to be cleared, got %q", got)
	}
}

func TestMovableRWONeedsFreshClaimWhenMigratingRuntime(t *testing.T) {
	current := model.App{
		ID: "app_demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
			},
		},
	}
	desired := current
	desired.Spec.RuntimeID = "runtime_b"

	if !movableRWONeedsFreshClaim(model.Operation{
		Type:            model.OperationTypeMigrate,
		SourceRuntimeID: "runtime_a",
		TargetRuntimeID: "runtime_b",
	}, current, desired) {
		t.Fatal("expected runtime migration to allocate a fresh target claim")
	}
}
