package store

import (
	"fugue/internal/model"
	"testing"
)

func TestStoppedDedicatedPVCMigrationRejectsUnsafeStateChanges(t *testing.T) {
	base := model.App{Spec: model.AppSpec{Replicas: 0, PersistentStorage: &model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "source", StorageSize: "10Gi", ClaimName: "data", Mounts: []model.AppPersistentStorageMount{{Path: "/data", Kind: model.AppPersistentStorageMountKindDirectory}}}}}
	for _, name := range []string{"valid", "running", "live-pods", "start-target", "shrink", "other-claim", "missing-mount", "missing-class", "workspace"} {
		t.Run(name, func(t *testing.T) {
			app := base
			src := *base.Spec.PersistentStorage
			app.Spec.PersistentStorage = &src
			desired := app.Spec
			dst := src
			dst.Mode = model.AppPersistentStorageModeMovableRWO
			dst.StorageClassName = "target"
			desired.PersistentStorage = &dst
			switch name {
			case "running":
				app.Spec.Replicas = 1
			case "live-pods":
				app.Status.CurrentReplicas = 1
			case "start-target":
				desired.Replicas = 1
			case "shrink":
				dst.StorageSize = "1Gi"
			case "other-claim":
				dst.ClaimName = "elsewhere"
			case "missing-mount":
				dst.Mounts = nil
			case "missing-class":
				dst.StorageClassName = ""
			case "workspace":
				app.Spec.Workspace = &model.AppWorkspaceSpec{}
			}
			if got := PermitsStoppedDedicatedPVCMigration(app, &desired); got != (name == "valid") {
				t.Fatalf("accepted=%v", got)
			}
		})
	}
}
