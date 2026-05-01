package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
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
	if !plan.sourceSharedProject {
		t.Fatal("expected shared-project source copy plan")
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

func TestDesiredPersistentStorageClaimNameUsesWorkspacePVCWhenClaimNameEmpty(t *testing.T) {
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
	}
	if got, want := desiredPersistentStorageClaimName(app, model.AppPersistentStorageSpec{}), runtimepkg.WorkspacePVCName(app); got != want {
		t.Fatalf("expected empty claim name to use workspace PVC %q, got %q", want, got)
	}
}

func TestBuildMovableRWOCopyPodMountsSharedSourceAndTarget(t *testing.T) {
	pod := buildMovableRWOCopyPod("tenant-a", "copy", map[string]string{"fugue.pro/volume-migration": "demo"}, movableRWOCopyPlan{
		sourceClaimName:     "project-shared",
		sourceMountSubPath:  "sessions/demo",
		sourceCopyPath:      ".",
		sourceSharedProject: true,
		targetClaimName:     "app-workspace",
		targetCopyPath:      "mounts/mount-demo",
	}, runtimepkg.SchedulingConstraints{})

	spec := pod["spec"].(map[string]any)
	containers := spec["containers"].([]map[string]any)
	mounts := containers[0]["volumeMounts"].([]map[string]any)
	if got := mounts[0]["subPath"]; got != "sessions/demo" {
		t.Fatalf("expected source subPath, got %#v", got)
	}
	volumes := spec["volumes"].([]map[string]any)
	sourcePVC := volumes[0]["persistentVolumeClaim"].(map[string]any)
	if got := sourcePVC["claimName"]; got != "project-shared" {
		t.Fatalf("expected shared source claim, got %#v", got)
	}
	targetPVC := volumes[1]["persistentVolumeClaim"].(map[string]any)
	if got := targetPVC["claimName"]; got != "app-workspace" {
		t.Fatalf("expected target claim, got %#v", got)
	}
}

func TestSchedulingForPodNodePinsToNFSNode(t *testing.T) {
	pod := kubePod{}
	pod.Spec.NodeName = "gcp1"
	pod.Spec.Tolerations = []runtimepkg.Toleration{
		{Key: "node-role.kubernetes.io/control-plane", Operator: "Exists", Effect: "NoSchedule"},
	}

	scheduling, ok := schedulingForPodNode(pod)
	if !ok {
		t.Fatal("expected scheduling for pod node")
	}
	if got := scheduling.NodeSelector[kubeHostnameLabelKey]; got != "gcp1" {
		t.Fatalf("expected node selector to pin gcp1, got %q", got)
	}
	if len(scheduling.Tolerations) != 1 || scheduling.Tolerations[0].Key != "node-role.kubernetes.io/control-plane" {
		t.Fatalf("expected NFS pod toleration to be preserved, got %#v", scheduling.Tolerations)
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

func TestMigrateDesiredSpecPreservesManagedPostgresRuntime(t *testing.T) {
	current := model.App{
		ID:   "app_demo",
		Name: "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_a",
			Postgres: &model.AppPostgresSpec{
				Database:  "demo",
				User:      "demo",
				RuntimeID: "runtime_db_source",
			},
		},
	}
	desired := current.Spec
	desired.RuntimeID = "runtime_b"
	desired.Postgres = &model.AppPostgresSpec{
		Database:  "demo",
		User:      "demo",
		RuntimeID: "runtime_b",
	}

	prepared := migrateDesiredSpecForManagedOperation(current, desired)
	if got := prepared.RuntimeID; got != "runtime_b" {
		t.Fatalf("expected app runtime to move to runtime_b, got %q", got)
	}
	if prepared.Postgres == nil {
		t.Fatal("expected managed postgres spec to be preserved")
	}
	if got := prepared.Postgres.RuntimeID; got != "runtime_db_source" {
		t.Fatalf("expected managed postgres runtime to stay on source until database switchover, got %q", got)
	}
}
