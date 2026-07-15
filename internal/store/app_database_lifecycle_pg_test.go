package store

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGCreateAppDatabaseImportJobRejectsUnavailableManagedPostgresUnderAppLock(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name               string
		persistedSuspended bool
		activeSuspend      bool
	}{
		{name: "persisted suspended", persistedSuspended: true},
		{name: "active suspend", activeSuspend: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			now := time.Date(2026, time.July, 15, 8, 9, 10, 0, time.UTC)

			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(
				mock,
				now,
				"tenant_database_import",
				"project_database_import",
				"app_database_import",
				0,
				pgLifecycleBoundServiceRow(
					now,
					"tenant_database_import",
					"project_database_import",
					"app_database_import",
					"service_database_import",
					testCase.persistedSuspended,
				),
			)
			if !testCase.persistedSuspended {
				mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*WHERE app_id = \$1.*type = \$2.*status IN \(\$3, \$4, \$5\)`).
					WithArgs(
						"app_database_import",
						model.OperationTypeDatabaseSuspend,
						model.OperationStatusPending,
						model.OperationStatusRunning,
						model.OperationStatusWaitingAgent,
					).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(testCase.activeSuspend))
			}
			mock.ExpectRollback()

			_, err = stateStore.CreateAppDatabaseImportJob(model.AppDatabaseImportJob{
				AppID:                "app_database_import",
				TenantID:             "tenant_database_import",
				SourceUploadID:       "upload_database_import",
				SourceUploadFilename: "dump.sql",
				SourceUploadSHA256:   "sha256-test",
				Format:               model.AppDatabaseImportFormatSQL,
				Status:               model.OperationStatusPending,
			})
			if !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
				t.Fatalf("CreateAppDatabaseImportJob error = %v, want managed postgres import conflict", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func TestPGClaimAppDatabaseImportJobCommitsTerminalFailureWhenSuspendWins(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Date(2026, time.July, 15, 9, 10, 11, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT app_id
FROM fugue_app_database_import_jobs
WHERE id = $1
`)).
		WithArgs("dbimport_lost_race").
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow("app_database_import"))
	expectPGLifecycleAppForUpdate(
		mock,
		now,
		"tenant_database_import",
		"project_database_import",
		"app_database_import",
		0,
		pgLifecycleBoundServiceRow(
			now,
			"tenant_database_import",
			"project_database_import",
			"app_database_import",
			"service_database_import",
			false,
		),
	)
	mock.ExpectQuery(`(?s)SELECT id, tenant_id, app_id, source_upload_id.*FROM fugue_app_database_import_jobs.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs("dbimport_lost_race").
		WillReturnRows(appDatabaseImportPGTestRows().AddRow(
			"dbimport_lost_race",
			"tenant_database_import",
			"app_database_import",
			"upload_database_import",
			"dump.sql",
			"sha256-test",
			"legacy-vps",
			model.AppDatabaseImportFormatSQL,
			false,
			model.OperationStatusPending,
			"",
			"",
			0,
			"",
			[]byte(`[]`),
			model.ActorTypeAPIKey,
			"user_database_import",
			now,
			now,
			nil,
			nil,
		))
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*WHERE app_id = \$1.*type = \$2.*status IN \(\$3, \$4, \$5\)`).
		WithArgs(
			"app_database_import",
			model.OperationTypeDatabaseSuspend,
			model.OperationStatusPending,
			model.OperationStatusRunning,
			model.OperationStatusWaitingAgent,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(`(?s)UPDATE fugue_app_database_import_jobs.*SET status = \$2.*error_message = \$3.*started_at = NULL.*completed_at = \$4.*WHERE id = \$1 AND status = \$5`).
		WithArgs(
			"dbimport_lost_race",
			model.OperationStatusFailed,
			ManagedPostgresDatabaseImportConflictMessage,
			sqlmock.AnyArg(),
			model.OperationStatusPending,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	_, err = stateStore.ClaimAppDatabaseImportJob("dbimport_lost_race")
	if !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("ClaimAppDatabaseImportJob error = %v, want managed postgres import conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGManagedPostgresSuspendReturnsActionableConflictForActiveDatabaseImport(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID  = "tenant_import_guard"
		projectID = "project_import_guard"
		appID     = "app_import_guard"
		serviceID = "service_import_guard"
	)
	now := time.Date(2026, time.July, 15, 10, 11, 12, 0, time.UTC)
	serviceRow := pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false)

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0, serviceRow)
	expectPGLifecycleAppHydration(mock, appID, serviceRow)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
	expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, pgLifecycleOperationRows())
	expectPGNoActiveAppDatabaseBackup(mock, appID, "demo-postgres", "demo-postgres")
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_app_database_import_jobs.*WHERE app_id = \$1.*status IN \(\$2, \$3\)`).
		WithArgs(appID, model.OperationStatusPending, model.OperationStatusRunning).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	_, err = stateStore.CreateOperation(model.Operation{
		TenantID:  tenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     appID,
		ServiceID: serviceID,
	})
	if !errors.Is(err, ErrManagedPostgresImportInProgressConflict) {
		t.Fatalf("CreateOperation error = %v, want actionable import-in-progress conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGManagedPostgresSuspendReturnsActionableConflictForActiveDatabaseBackup(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID  = "tenant_backup_guard"
		projectID = "project_backup_guard"
		appID     = "app_backup_guard"
		serviceID = "service_backup_guard"
	)
	now := time.Date(2026, time.July, 15, 11, 12, 13, 0, time.UTC)
	serviceRow := pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false)

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0, serviceRow)
	expectPGLifecycleAppHydration(mock, appID, serviceRow)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
	expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, pgLifecycleOperationRows())
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_backup_runs.*status IN \(\$1, \$2\).*target_type = \$3`).
		WithArgs(
			model.BackupRunStatusPending,
			model.BackupRunStatusRunning,
			model.BackupTargetAppDatabase,
			appID,
			"demo-postgres",
			"demo-postgres",
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	_, err = stateStore.CreateOperation(model.Operation{
		TenantID:  tenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     appID,
		ServiceID: serviceID,
	})
	if !errors.Is(err, ErrManagedPostgresBackupInProgressConflict) {
		t.Fatalf("CreateOperation error = %v, want actionable backup-in-progress conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func appDatabaseImportPGTestRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id",
		"tenant_id",
		"app_id",
		"source_upload_id",
		"source_upload_filename",
		"source_upload_sha256",
		"label",
		"format",
		"clean",
		"status",
		"result_message",
		"error_message",
		"retry_count",
		"retry_of_job_id",
		"logs_json",
		"requested_by_type",
		"requested_by_id",
		"created_at",
		"updated_at",
		"started_at",
		"completed_at",
	})
}
