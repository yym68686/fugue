package cli

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAppStorageForMutationRejectsLegacyWorkspace(t *testing.T) {
	t.Parallel()

	storage, err := appStorageForMutation(model.AppSpec{
		Workspace: &model.AppWorkspaceSpec{
			MountPath:   "/workspace",
			StorageSize: "10Gi",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "migrate the app to persistent_storage") {
		t.Fatalf("expected explicit legacy workspace rejection, got storage=%+v err=%v", storage, err)
	}
	if storage != nil {
		t.Fatalf("legacy workspace must not be converted by the CLI, got %+v", storage)
	}
}

func TestAppStorageViewUsesOnlyCanonicalPersistentStorage(t *testing.T) {
	t.Parallel()

	legacy := appStorageViewFromSpec(model.App{
		ID: "app_legacy",
		Spec: model.AppSpec{
			Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
		},
	})
	if legacy.Enabled || legacy.StorageMode != "disabled" || legacy.PersistentStorage != nil || len(legacy.Mounts) != 0 {
		t.Fatalf("legacy workspace must not appear as canonical storage, got %+v", legacy)
	}

	canonical := appStorageViewFromSpec(model.App{
		ID: "app_canonical",
		Spec: model.AppSpec{
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data", Mode: 0o755},
				},
			},
		},
	})
	if !canonical.Enabled || canonical.StorageMode != "persistent_storage" || canonical.PersistentStorage == nil || len(canonical.Mounts) != 1 {
		t.Fatalf("canonical persistent storage view is incomplete: %+v", canonical)
	}
}
