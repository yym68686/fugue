package store

import (
	"fugue/internal/model"
	"os"
	"testing"
	"time"
)

func TestPeriodicRetentionPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable database required")
	}
	s := New(dsn)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	p, err := s.UpsertBackupPolicy(model.BackupPolicy{Name: model.NewID("policy"), Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, RetainCount: 2, Retention: model.BackupRetentionPolicy{RetainDays: 7, ProtectLatest: 1}})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for i := 0; i < 3; i++ {
		a, err := s.CreateBackupArtifact(model.BackupArtifact{PolicyID: p.ID, Target: p.Target, Status: "active", Kind: model.BackupArtifactKindControlPlanePGDump, CreatedAt: time.Now().Add(-time.Duration(i+8) * 24 * time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, a.ID)
	}
	preview, err := s.PreviewBackupRetention(p.ID)
	if err != nil || len(preview) != 2 {
		t.Fatalf("preview %v %v", preview, err)
	}
	if err := s.ReconcileBackupRetention(p.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackupRetention(p.ID); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		a, err := s.GetBackupArtifact(id, "", true)
		if err != nil {
			t.Fatal(err)
		}
		if (a.Status == "active") != (i == 0) {
			t.Errorf("artifact %+v", a)
		}
	}
}
