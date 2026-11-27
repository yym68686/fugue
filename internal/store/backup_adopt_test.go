package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func adoptionTestBackend(t *testing.T, s *Store) model.BackupBackend {
	t.Helper()
	backend, err := s.CreateBackupBackend(model.BackupBackend{Name: model.NewID("test"), Provider: model.DataBackendProviderCloudflareR2, Bucket: "fixture-bucket", Endpoint: "https://example.invalid", Credentials: model.DataBackendCredentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func TestAdoptBackupArtifactPostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable Postgres integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	s := New(address)
	if err := s.Init(); err != nil {
		if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), address); err != nil {
			t.Fatal(err)
		}
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
	}
	defer s.db.Close()
	backend := adoptionTestBackend(t, s)
	defer s.db.Exec(`DELETE FROM fugue_backup_backends WHERE id=$1`, backend.ID)
	run, err := s.CreateBackupRun(model.BackupRun{BackendID: backend.ID, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}, Status: model.BackupRunStatusFailed, ErrorMessage: "artifact persistence failed"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_backup_runs WHERE id=$1`, run.ID)
	artifact := model.BackupArtifact{RunID: run.ID, BackendID: backend.ID, Target: run.Target, Kind: model.BackupArtifactKindLonghornSnapshot, Status: model.BackupArtifactStatusActive, Protected: true, LogicalBytes: 4096, SizeBytes: 1000}
	stale := run
	stale.UpdatedAt = stale.UpdatedAt.Add(-time.Second)
	if _, err := s.AdoptBackupArtifact(artifact, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale run accepted: %v", err)
	}
	created, err := s.AdoptBackupArtifact(artifact, run)
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_backup_artifacts WHERE id=$1`, created.ID)
	if !created.Protected || created.AppID != "" {
		t.Fatalf("invalid platform artifact %+v", created)
	}
	if _, err := s.AdoptBackupArtifact(artifact, run); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate accepted: %v", err)
	}
	updated, err := s.GetBackupRun(run.ID, "", true)
	if err != nil || updated.Status != model.BackupRunStatusFailed || updated.ErrorMessage != run.ErrorMessage || updated.ArtifactCount != 1 || updated.BytesWritten != 1000 || updated.LogicalBytes != 4096 {
		t.Fatalf("run=%+v error=%v", updated, err)
	}
}
