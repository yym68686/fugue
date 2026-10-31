package store

import (
	"encoding/json"
	"fugue/internal/model"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLegacyLonghornCleanupReceiptIsReconciled(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreateBackupArtifact(model.BackupArtifact{Kind: model.BackupArtifactKindLonghornSnapshot, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, Status: model.BackupArtifactStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.MarkBackupArtifactDeleted(a.ID, "", true); err != nil {
		t.Fatal(err)
	}
	// Simulate the old collector's false physical deletion receipt.
	s.backupArtifactPhysicalCleanupMu.Lock()
	s.backupArtifactPhysicalCleanup = map[string]time.Time{a.ID: time.Now().UTC()}
	s.backupArtifactPhysicalCleanupMu.Unlock()
	filter := BackupArtifactCleanupFilter{Before: time.Now().Add(time.Hour), Limit: 10}
	items, err := s.ListBackupArtifactCleanupCandidates(filter)
	if err != nil || len(items) != 1 {
		t.Fatalf("legacy receipt skipped: %v %v", items, err)
	}
	if err = s.MarkBackupArtifactPhysicalDeleted(a.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	items, err = s.ListBackupArtifactCleanupCandidates(filter)
	if err != nil || len(items) != 0 {
		t.Fatalf("engine-confirmed receipt retried: %v %v", items, err)
	}
	loaded, err := s.GetBackupArtifact(a.ID, "", true)
	if err != nil || loaded.Manifest.Metadata[model.BackupLonghornCleanupVersionKey] != "1" {
		t.Fatalf("receipt not durable: %+v %v", loaded, err)
	}
}

func TestLonghornCleanupBlocksSharedLiveReferences(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	manifest := model.BackupManifest{Metadata: map[string]string{"longhorn_backup_url": "s3://example@auto/root?backup=one&volume=one"}}
	a, err := s.CreateBackupArtifact(model.BackupArtifact{Kind: model.BackupArtifactKindLonghornSnapshot, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateBackupArtifact(model.BackupArtifact{Kind: model.BackupArtifactKindLonghornSnapshot, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	if referenced, err := s.LonghornBackupHasOtherReferences(a); err != nil || !referenced {
		t.Fatalf("live alias not protected: %v %v", referenced, err)
	}
}

func TestPGLonghornReconciliationProjectionAndReferenceGuard(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	raw, _ := json.Marshal(map[string]string{"longhorn_backup_name": "backup-one", "longhorn_cleanup_version": "1"})
	mock.ExpectQuery(`(?s)SELECT id, run_id, backend_id.*jsonb_build_object.*WHERE kind = 'longhorn-snapshot' ORDER BY created_at DESC, id DESC LIMIT \$1`).WithArgs(501).
		WillReturnRows(sqlmock.NewRows([]string{"id", "run_id", "backend_id", "tenant_id", "status", "protected", "deleted_at", "physical_deleted_at", "physical_delete_attempted_at", "physical_delete_error", "metadata"}).AddRow("artifact-one", "run-one", "backend-one", "", "deleted", false, nil, nil, nil, "", raw))
	items, err := s.ListLonghornReconciliationArtifacts(501)
	if err != nil || len(items) != 1 || items[0].Artifact.Manifest.Metadata[model.BackupLonghornCleanupVersionKey] != "1" {
		t.Fatalf("projection: %+v %v", items, err)
	}
	mock.ExpectQuery(`(?s)SELECT EXISTS.*longhorn_backup_url.*a.id <> \$2.*fugue_backup_restore_plans.*fugue_backup_restore_runs`).WithArgs("s3://example@auto/root?backup=one&volume=one", "artifact-one").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	items[0].Artifact.Manifest.Metadata["longhorn_backup_url"] = "s3://example@auto/root?backup=one&volume=one"
	if referenced, err := s.LonghornBackupHasOtherReferences(items[0].Artifact); err != nil || !referenced {
		t.Fatalf("guard: %v %v", referenced, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
