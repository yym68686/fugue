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

func TestRecoveredBackupRetirementFile(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	testRecoveredBackupRetirement(t, s)
}

func TestRecoveredBackupRetirementPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable loopback fugue_test database required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	s := New(dsn)
	if err := s.Init(); err != nil {
		if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), dsn); err != nil {
			t.Fatal(err)
		}
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
	}
	defer s.db.Close()
	testRecoveredBackupRetirement(t, s)
}

func testRecoveredBackupRetirement(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	admin := model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: "test-admin", Scopes: map[string]struct{}{"platform.admin": {}}}
	backend := adoptionTestBackend(t, s)
	target := model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}
	volume := strings.ReplaceAll(model.NewID("fixture"), "_", "-")
	p, err := s.UpsertBackupPolicy(model.BackupPolicy{Name: model.NewID("policy"), Target: target, BackendID: backend.ID, Enabled: true, Schedule: "0 2 * * *", RetainCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	create := func(status, source, suffix string, age time.Duration) model.BackupArtifact {
		t.Helper()
		run, err := s.CreateBackupRun(model.BackupRun{PolicyID: p.ID, Target: target, BackendID: backend.ID, Status: status, CreatedAt: now.Add(-age)})
		if err != nil {
			t.Fatal(err)
		}
		m := model.BackupManifest{RunID: run.ID, Kind: model.BackupArtifactKindLonghornSnapshot, Format: "longhorn-remote-backup", Target: target, Metadata: map[string]string{
			"volume_name": volume, "snapshot_name": "snapshot-" + suffix,
			"longhorn_backup_name": "backup-" + suffix, "backup_target_name": "default",
			"backup_target_url": "s3://fixture-bucket@auto", "longhorn_backup_url": "s3://fixture-bucket@auto?backup=backup-" + suffix + "&volume=" + volume,
			"reconciliation_source": source,
		}}
		a, err := s.CreateBackupArtifact(model.BackupArtifact{RunID: run.ID, PolicyID: p.ID, Target: target, BackendID: backend.ID, Kind: m.Kind, Protected: source != "", Manifest: m, CreatedAt: now.Add(-age)})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	old := create(model.BackupRunStatusFailed, "completed-engine-reconciliation", "old", 30*24*time.Hour)
	replacement := create(model.BackupRunStatusSucceeded, "", "new", 12*time.Hour)
	restoring := create(model.BackupRunStatusCanceled, "completed-longhorn-orphan-manifest", "restore", 30*24*time.Hour)
	if _, err := s.CreateBackupRestorePlan(model.BackupRestorePlan{ArtifactID: restoring.ID, Target: target, Status: model.BackupRestoreStatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	req := model.RecoveredBackupRetirementRequest{ReplacementArtifactID: replacement.ID, Reason: "verified replacement"}
	if blocked, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin); err != nil || len(blocked.Blockers) != 1 || blocked.Confirmation != "" {
		t.Fatalf("fewer than three retained points must block: %+v, %v", blocked, err)
	}
	create(model.BackupRunStatusSucceeded, "", "new", 18*time.Hour)
	second := create(model.BackupRunStatusSucceeded, "", "second", 24*time.Hour)
	if blocked, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin); err != nil || len(blocked.Blockers) != 1 || blocked.Confirmation != "" {
		t.Fatalf("alias did not count as an independent point: %+v, %v", blocked, err)
	}
	third := create(model.BackupRunStatusSucceeded, "", "third", 48*time.Hour)
	if blocked, err := s.RecoveredBackupRetirement(ctx, restoring.ID, req, admin); err != nil || len(blocked.Blockers) == 0 || !strings.Contains(strings.Join(blocked.Blockers, " "), "restore plan or run") {
		t.Fatalf("existing restore reference must block: %+v, %v", blocked, err)
	}
	if _, err := s.MarkBackupArtifactDeleted(old.ID, "", true); !errors.Is(err, ErrConflict) {
		t.Fatalf("ordinary delete bypassed protection: %v", err)
	}
	preview, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin)
	if err != nil || preview.Confirmation == "" || preview.Retired || len(preview.Blockers) != 0 || preview.RestoreEvidenceStatus != "not_provided" ||
		strings.Join(preview.RetainedArtifactIDs, ",") != strings.Join([]string{replacement.ID, second.ID, third.ID}, ",") {
		t.Fatalf("bad preview: %+v, %v", preview, err)
	}
	withEvidence := req
	withEvidence.RestoreEvidenceSHA256 = strings.Repeat("a", 64)
	if got, err := s.RecoveredBackupRetirement(ctx, old.ID, withEvidence, admin); err != nil || got.RestoreEvidenceStatus != "operator_attested" || got.Confirmation == "" || got.Confirmation == preview.Confirmation {
		t.Fatalf("optional receipt must be bound to confirmation: %+v, %v", got, err)
	}
	before, err := s.GetBackupArtifact(old.ID, "", true)
	if err != nil || !before.Protected || before.DeletedAt != nil {
		t.Fatalf("preview mutated target: %+v, %v", before, err)
	}
	stale := req
	stale.Confirmation, stale.Reason = preview.Confirmation, "changed"
	if got, err := s.RecoveredBackupRetirement(ctx, old.ID, stale, admin); err != nil || got.Retired || len(got.Blockers) == 0 {
		t.Fatalf("different intent reused confirmation: %+v, %v", got, err)
	}
	alias := create(model.BackupRunStatusSucceeded, "", "old", 20*time.Hour)
	if got, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin); err != nil || got.Confirmation != "" || len(got.Blockers) != 1 || !strings.Contains(got.Blockers[0], "another artifact") {
		t.Fatalf("duplicate reference must block: %+v, %v", got, err)
	}
	// Any alias (including a logically deleted one) is conservatively excluded.
	if _, err := s.db.Exec("DELETE FROM fugue_backup_artifacts WHERE id=$1", alias.ID); err != nil {
		t.Fatal(err)
	}

	req.Confirmation = preview.Confirmation
	retired, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin)
	if err != nil || !retired.Retired || retired.AuditID == "" {
		t.Fatalf("commit did not retire target: %+v, %v", retired, err)
	}
	if got, err := s.RecoveredBackupRetirement(ctx, old.ID, req, admin); err != nil || got.Retired || len(got.Blockers) == 0 {
		t.Fatalf("duplicate confirmation committed: %+v, %v", got, err)
	}
	after, err := s.GetBackupArtifact(old.ID, "", true)
	if err != nil || after.Protected || after.Status != model.BackupArtifactStatusDeleted || after.DeletedAt == nil {
		t.Fatalf("retired artifact not queued: %+v, %v", after, err)
	}
	run, err := s.GetBackupRun(old.RunID, "", true)
	if err != nil || run.Status != model.BackupRunStatusFailed {
		t.Fatalf("original run changed: %+v, %v", run, err)
	}
	newer, err := s.GetBackupArtifact(replacement.ID, "", true)
	if err != nil || newer.Status != model.BackupArtifactStatusActive {
		t.Fatalf("replacement was not preserved: %+v, %v", newer, err)
	}
	events, err := s.ListAuditEvents("", true, 20)
	if err != nil {
		t.Fatal(err)
	}
	var matching int
	for _, event := range events {
		if event.TargetID == old.ID && event.Action == recoveredBackupRetirementAction {
			matching++
			if event.ID != retired.AuditID || event.Metadata["restore_evidence_sha256"] != req.RestoreEvidenceSHA256 ||
				event.Metadata["evidence_source"] != "not_provided" ||
				event.Metadata["retained_artifact_ids"] != strings.Join(preview.RetainedArtifactIDs, ",") || event.ActorID != admin.ActorID {
				t.Fatalf("bad audit event: %+v", event)
			}
		}
	}
	if matching != 1 {
		t.Fatalf("got %d matching audit events", matching)
	}
}
