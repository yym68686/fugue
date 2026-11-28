package backupusage

import (
	"testing"

	"fugue/internal/model"
)

func TestBackupArtifactObjectKeysForDeletion(t *testing.T) {
	t.Parallel()

	valid := model.BackupArtifact{
		RunID:             "backup_run_test",
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant/project/app/backup_run_test/database.dump",
		ManifestObjectKey: "apps/tenant/project/app/backup_run_test/manifest.json",
		Status:            model.BackupArtifactStatusDeleted,
	}
	keys, err := OwnedObjectKeys(valid)
	if err != nil {
		t.Fatalf("valid artifact rejected: %v", err)
	}
	if len(keys) != 2 || keys[0] != valid.ObjectKey || keys[1] != valid.ManifestObjectKey {
		t.Fatalf("unexpected deletion keys: %#v", keys)
	}

	snapshot := valid
	snapshot.Kind = model.BackupArtifactKindDataSnapshot
	snapshot.ObjectKey = "snapshot-owned-by-workspace"
	keys, err = OwnedObjectKeys(snapshot)
	if err != nil {
		t.Fatalf("data snapshot artifact rejected: %v", err)
	}
	if len(keys) != 1 || keys[0] != snapshot.ManifestObjectKey {
		t.Fatalf("snapshot blob would be deleted: %#v", keys)
	}

	unsafe := valid
	unsafe.ObjectKey = "apps/tenant/project/app/other-run/../backup_run_test/database.dump"
	if _, err := OwnedObjectKeys(unsafe); err == nil {
		t.Fatal("unsafe path was accepted")
	}

	wrongRun := valid
	wrongRun.ObjectKey = "apps/tenant/project/app/another-run/database.dump"
	if _, err := OwnedObjectKeys(wrongRun); err == nil {
		t.Fatal("object outside the artifact run was accepted")
	}

	missingKeys := valid
	missingKeys.ObjectKey = ""
	missingKeys.ManifestObjectKey = ""
	if _, err := OwnedObjectKeys(missingKeys); err == nil {
		t.Fatal("artifact without object keys was treated as successfully deletable")
	}

	databaseManifest := missingKeys
	databaseManifest.Kind = model.BackupArtifactKindLonghornSnapshot
	keys, err = OwnedObjectKeys(databaseManifest)
	if err != nil || len(keys) != 0 {
		t.Fatalf("Longhorn artifact with a database-only manifest rejected: %v %#v", err, keys)
	}
	databaseManifest.ManifestObjectKey = "apps/other-run/manifest.json"
	if _, err := OwnedObjectKeys(databaseManifest); err == nil {
		t.Fatal("Longhorn manifest outside its run was accepted")
	}
}
