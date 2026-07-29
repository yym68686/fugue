package store

import (
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGMarkBackupArtifactDeletedFiltersTenantAtomically(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectQuery(`(?s)UPDATE fugue_backup_artifacts SET status = 'deleted', deleted_at = \$2 WHERE id = \$1 AND protected = FALSE AND tenant_id = \$3 RETURNING`).
		WithArgs("artifact_victim", sqlmock.AnyArg(), "tenant_attacker").
		WillReturnError(sql.ErrNoRows)
	if _, err := stateStore.MarkBackupArtifactDeleted("artifact_victim", "tenant_attacker", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tenant-filtered delete miss to return ErrNotFound, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGBackupTenantVisibilityExcludesPlatformRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	mock.ExpectQuery(`(?s)FROM fugue_backup_policies.*AND tenant_id = \$3`).
		WithArgs("platform-policy", "platform-policy", "tenant-user").
		WillReturnError(sql.ErrNoRows)
	if _, err := stateStore.GetBackupPolicy("platform-policy", "tenant-user", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected platform policy to be hidden, got %v", err)
	}

	mock.ExpectQuery(`(?s)FROM fugue_backup_runs.*WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs("platform-run", "tenant-user").
		WillReturnError(sql.ErrNoRows)
	if _, err := stateStore.GetBackupRun("platform-run", "tenant-user", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected platform run to be hidden, got %v", err)
	}

	mock.ExpectQuery(`(?s)FROM fugue_backup_artifacts.*WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs("platform-artifact", "tenant-user").
		WillReturnError(sql.ErrNoRows)
	if _, err := stateStore.GetBackupArtifact("platform-artifact", "tenant-user", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected platform artifact to be hidden, got %v", err)
	}

	mock.ExpectQuery(`(?s)FROM fugue_backup_restore_plans.*WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs("platform-plan", "tenant-user").
		WillReturnError(sql.ErrNoRows)
	if _, err := stateStore.GetBackupRestorePlan("platform-plan", "tenant-user", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected platform restore plan to be hidden, got %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(backupRestorePlanSelectSQL())+` WHERE tenant_id = \$1 ORDER BY created_at DESC LIMIT \$2`).
		WithArgs("tenant-user", defaultBackupRestorePlanListLimit).
		WillReturnRows(sqlmock.NewRows(strings.Split(backupRestorePlanReturningColumns(), ", ")))
	if plans, err := stateStore.ListBackupRestorePlans("tenant-user", false, 0); err != nil || len(plans) != 0 {
		t.Fatalf("list tenant restore plans: plans=%+v err=%v", plans, err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(backupRestoreRunSelectSQL())+` WHERE tenant_id = \$1 ORDER BY created_at DESC LIMIT \$2`).
		WithArgs("tenant-user", defaultBackupRestoreRunListLimit).
		WillReturnRows(sqlmock.NewRows(strings.Split(backupRestoreRunReturningColumns(), ", ")))
	if runs, err := stateStore.ListBackupRestoreRuns("tenant-user", false, 0); err != nil || len(runs) != 0 {
		t.Fatalf("list tenant restore runs: runs=%+v err=%v", runs, err)
	}

	for name, clauses := range map[string][]string{
		"policy":   firstBackupFilterResult(backupPolicyFilterClauses(BackupPolicyFilter{TenantID: "tenant-user"})),
		"run":      firstBackupFilterResult(backupRunFilterClauses(BackupRunFilter{TenantID: "tenant-user"})),
		"artifact": firstBackupFilterResult(backupArtifactFilterClauses(BackupArtifactFilter{TenantID: "tenant-user"})),
	} {
		joined := strings.Join(clauses, " ")
		if !strings.Contains(joined, "tenant_id = $1") || strings.Contains(joined, "tenant_id IS NULL") {
			t.Fatalf("%s filter must exclude platform rows: %s", name, joined)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGBackupBackendMutationsRequireTenantOwnership(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	query := `(?s)FROM fugue_backup_backends.*\(id = \$1 OR name = \$1 OR slug = \$2\) AND tenant_id = \$3`
	for range 3 {
		mock.ExpectQuery(query).
			WithArgs("platform-shared", "platform-shared", "tenant-user").
			WillReturnError(sql.ErrNoRows)
	}
	if _, err := stateStore.RotateBackupBackendCredentials("platform-shared", "tenant-user", false, model.DataBackendCredentials{AccessKeyID: "x", SecretAccessKey: "y"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tenant rotation of platform backend to be hidden, got %v", err)
	}
	if _, err := stateStore.DeleteBackupBackend("platform-shared", "tenant-user", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tenant deletion of platform backend to be hidden, got %v", err)
	}
	if _, err := stateStore.RecordBackupBackendTest("platform-shared", "tenant-user", false, false, "no"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected tenant test mutation of platform backend to be hidden, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func firstBackupFilterResult(clauses []string, _ []any) []string {
	return clauses
}
