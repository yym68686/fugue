package store

import (
	"fugue/internal/model"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPeriodicRetentionPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable database required")
	}
	s := New("", dsn)
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

func TestPeriodicRetentionPreservesDisabledProtectedAndRestoring(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err := s.withLockedState(true, func(st *model.State) error {
		st.BackupPolicies = []model.BackupPolicy{
			{ID: "enabled", Enabled: true, Status: "active", RetainCount: 2, Retention: model.BackupRetentionPolicy{RetainDays: 7, ProtectLatest: 1}},
			{ID: "disabled", Enabled: false, Status: "disabled", RetainCount: 1},
		}
		for _, v := range []struct {
			id, policy string
			days       int
			protected  bool
		}{
			{"latest", "enabled", 8, false}, {"old", "enabled", 9, false},
			{"protected", "enabled", 10, true}, {"restoring", "enabled", 11, false},
			{"disabled-new", "disabled", 12, false}, {"disabled-old", "disabled", 13, false},
			{"manual", "", 100, false},
		} {
			st.BackupArtifacts = append(st.BackupArtifacts, model.BackupArtifact{ID: v.id, PolicyID: v.policy, Status: "active", Protected: v.protected, CreatedAt: now.Add(-time.Duration(v.days) * 24 * time.Hour)})
		}
		st.BackupRestorePlans = []model.BackupRestorePlan{{ID: "plan", ArtifactID: "restoring", Status: "planned"}}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := s.PreviewBackupRetention("enabled")
	if err != nil || len(ids) != 1 || ids[0] != "old" {
		t.Fatalf("preview %v %v", ids, err)
	}
	if err := s.ReconcileEnabledBackupRetention(); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(false, func(st *model.State) error {
		for _, a := range st.BackupArtifacts {
			if (a.Status != "active") != (a.ID == "old") {
				t.Errorf("unexpected lifecycle %+v", a)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackupRetention("disabled"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackupRetention("disabled"); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(false, func(st *model.State) error {
		for _, a := range st.BackupArtifacts {
			if a.ID == "disabled-old" && a.DeletedAt == nil {
				t.Error("explicit disabled history reconciliation did not expire old backup")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
