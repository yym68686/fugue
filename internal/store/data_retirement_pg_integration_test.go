package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWorkspaceRetirementPolicyEnableRechecksTargetInsideFence(t *testing.T) {
	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("dataset-policy")
	policy.Enabled = false
	policy.Target = model.BackupTarget{Type: model.BackupTargetDataWorkspace, WorkspaceID: "active-dataset"}
	expectGetBackupSchedulePolicy(mock, policy)
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(DataReferenceLock).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	policy.Target.WorkspaceID = "retiring-dataset"
	expectGetBackupSchedulePolicy(mock, policy)
	raw, err := json.Marshal(model.StorageMaintenanceJob{ID: "retirement-task", Kind: "workspace-retire", ScopeID: "retiring-dataset", Execute: true})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT body FROM fugue_storage_maintenance_jobs`).WithArgs("retiring-dataset").
		WillReturnRows(sqlmock.NewRows([]string{"body"}).AddRow(raw))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(DataReferenceLock).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err = s.SetBackupPolicyEnabled(policy.ID, "", true, true, ""); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("changed target escaped the retirement fence", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestWorkspaceRetirementPostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable Postgres integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback fugue_test database required")
	}
	s := New("", address)
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bootstrap := &Store{db: db, databaseURL: address}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bootstrap.applyPostgresSchemaTx(context.Background(), tx); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigratePlatformState, schemamigrate.MigrateImageCacheManifestGraph, schemamigrate.MigrateEdgeInstanceFencing, schemamigrate.MigrateSourceUploadSessions, schemamigrate.MigrateStorageMaintenance} {
		if err = migrate(context.Background(), address); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	tenant, err := s.CreateTenant(model.NewID("retirement-fixture"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, tenant.ID)
	w, err := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset"})
	if err != nil {
		t.Fatal(err)
	}
	m := model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: "abcd", Size: 1, AssetName: "data", RelativePath: "sample"}}}
	if _, err = s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: m}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload", Manifest: m}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.CreateDataGrant(model.DataGrant{WorkspaceID: w.ID, Mode: "read-only"}); err != nil {
		t.Fatal(err)
	}
	tr, err := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "prewarm", Status: "completed", Cache: &model.DataPrewarmCache{State: "ready"}})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := s.UpsertBackupPolicy(model.BackupPolicy{TenantID: tenant.ID, Name: "dataset-policy", Target: model.BackupTarget{Type: model.BackupTargetDataWorkspace, WorkspaceID: w.ID}, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{CancelPlannedTransfers: true, RevokeGrants: true, EvictCaches: true}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(`DELETE FROM fugue_storage_maintenance_jobs WHERE id=$1`, j.ID)
	if _, err = s.SetBackupPolicyEnabled(policy.ID, tenant.ID, false, true, ""); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("Postgres policy enable bypassed the freeze", err)
	}
	if _, err = s.SetBackupPolicyEnabled(policy.ID, tenant.ID, false, false, "operator kill switch"); err != nil {
		t.Fatal("freeze blocked policy disable", err)
	}
	if _, err = s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "two"}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("snapshot freeze missing", err)
	}
	tx, err = s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = lockDataReferencesTx(context.Background(), tx, tenant.ID, &model.AppSpec{Data: &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{WorkspaceID: w.ID}}}})
	tx.Rollback()
	if !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("app freeze missing", err)
	}
	if _, err = s.ReleaseDataWorkspaceRetirement(j); !errors.Is(err, ErrConflict) {
		t.Fatal("unobserved cache cleanup accepted", err)
	}
	tr, err = s.GetDataTransfer(tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	tr.Cache.State = "removed"
	if _, err = s.UpdateDataTransfer(tr); err != nil {
		t.Fatal(err)
	}
	j, err = s.ReleaseDataWorkspaceRetirement(j)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListDataGCReferences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range refs {
		if v.WorkspaceID == w.ID && v.SHA256 == "abcd" {
			found = true
		}
	}
	if !found {
		t.Fatal("retirement references lost")
	}
	objects, err := s.ListDataRetirementObjects(context.Background(), w)
	if err != nil || len(objects) == 0 {
		t.Fatal(objects, err)
	}
	past := time.Now().Add(-time.Hour)
	j.WorkspaceRetirement.ReclaimAfter = &past
	if err = s.FinalizeDataWorkspaceRetirement(j); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetDataWorkspace(w.ID, "", true); !errors.Is(err, ErrNotFound) {
		t.Fatal("metadata not deleted", err)
	}
}
