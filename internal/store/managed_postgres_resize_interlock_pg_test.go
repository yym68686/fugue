package store

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGActiveManagedPostgresResizeBlocksLaterAppOperation(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create resize interlock sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	const (
		tenantID  = "tenant_resize_interlock"
		projectID = "project_resize_interlock"
		appID     = "app_resize_interlock"
		serviceID = "service_resize_interlock"
	)
	serviceRow := pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1, serviceRow)
	expectPGLifecycleAppHydration(mock, appID, serviceRow)
	expectPGManagedPostgresExclusiveMutation(mock, appID, "", true)
	mock.ExpectRollback()

	zero := 0
	if _, err := s.CreateOperation(model.Operation{
		TenantID: tenantID, Type: model.OperationTypeScale, AppID: appID, DesiredReplicas: &zero,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("later app operation during resize error = %v, want conflict", err)
	}
	assertResizeInterlockPGExpectations(t, mock)
}

func TestPGActiveManagedPostgresResizeBlocksObservedSpecSynchronization(t *testing.T) {
	t.Parallel()
	const (
		tenantID  = "tenant_resize_sync"
		projectID = "project_resize_sync"
		appID     = "app_resize_sync"
		serviceID = "service_resize_sync"
	)
	now := time.Date(2026, time.July, 28, 12, 5, 0, 0, time.UTC)
	tests := []struct {
		name string
		call func(*Store, model.AppSpec) error
	}{
		{
			name: "managed postgres observed spec",
			call: func(s *Store, spec model.AppSpec) error {
				_, err := s.SyncObservedManagedPostgresSpec(appID, spec)
				return err
			},
		},
		{
			name: "managed app observed baseline",
			call: func(s *Store, spec model.AppSpec) error {
				_, err := s.SyncObservedManagedAppBaseline(appID, spec, nil)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create resize sync sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			serviceRow := pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false)
			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1, serviceRow)
			expectPGManagedPostgresExclusiveMutation(mock, appID, "", true)
			mock.ExpectRollback()

			if err := test.call(s, model.AppSpec{
				Image: "ghcr.io/example/original:1", Replicas: 1, RuntimeID: "runtime_us",
			}); !errors.Is(err, ErrConflict) {
				t.Fatalf("observed spec synchronization during resize error = %v, want conflict", err)
			}
			assertResizeInterlockPGExpectations(t, mock)
		})
	}
}

func TestPGManagedPostgresResizeReportsExactActiveDependency(t *testing.T) {
	t.Parallel()
	const (
		tenantID  = "tenant_resize_dependency"
		projectID = "project_resize_dependency"
		appID     = "app_resize_dependency"
		serviceID = "service_resize_dependency"
	)
	now := time.Date(2026, time.July, 28, 12, 10, 0, 0, time.UTC)
	bootstrap := model.ResourceSpec{
		CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768,
	}
	target := managedPostgresResizeTarget()
	tests := []struct {
		name   string
		want   error
		expect func(sqlmock.Sqlmock)
	}{
		{
			name: "backup",
			want: ErrManagedPostgresBackupInProgressConflict,
			expect: func(mock sqlmock.Sqlmock) {
				expectPGActiveAppDatabaseBackupForResize(mock, appID, true)
			},
		},
		{
			name: "import",
			want: ErrManagedPostgresImportInProgressConflict,
			expect: func(mock sqlmock.Sqlmock) {
				expectPGActiveAppDatabaseBackupForResize(mock, appID, false)
				expectPGActiveAppDatabaseImportForResize(mock, appID, true)
			},
		},
		{
			name: "restore",
			want: ErrManagedPostgresRestoreInProgressConflict,
			expect: func(mock sqlmock.Sqlmock) {
				expectPGActiveAppDatabaseBackupForResize(mock, appID, false)
				expectPGActiveAppDatabaseImportForResize(mock, appID, false)
				mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_backup_restore_runs.*WHERE app_id = \$1.*status IN \(\$2, \$3\)`).
					WithArgs(appID, model.BackupRestoreStatusPlanned, model.BackupRestoreStatusRunning).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create resize dependency sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			serviceRow := pgResizeBoundServiceRow(now, tenantID, projectID, appID, serviceID, bootstrap, nil)
			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0, serviceRow)
			expectPGLifecycleAppHydration(mock, appID, serviceRow)
			expectPGResizeBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, bootstrap, nil)
			expectPGResizeTargetBinding(mock, tenantID, appID, serviceID)
			expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, pgLifecycleOperationRows())
			test.expect(mock)
			mock.ExpectRollback()

			if _, err := s.CreateOperation(model.Operation{
				TenantID: tenantID, Type: model.OperationTypeDatabaseResize, AppID: appID, ServiceID: serviceID,
				DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: model.CloneResourceSpec(&target)}},
			}); !errors.Is(err, test.want) {
				t.Fatalf("resize with active %s error = %v, want %v", test.name, err, test.want)
			}
			assertResizeInterlockPGExpectations(t, mock)
		})
	}
}

func TestPGDatabaseBackupRejectsAndPersistsExactResizeConflict(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		run := model.NormalizeBackupRun(model.BackupRun{
			ID: "backup_resize_conflict", TenantID: "tenant_backup", ProjectID: "project_backup", AppID: "app_backup",
			Target: model.BackupTarget{
				Type: model.BackupTargetAppDatabase, TenantID: "tenant_backup", ProjectID: "project_backup",
				AppID: "app_backup", ServiceName: "postgres-app",
			},
			BackendID: "backup_backend_r2", Trigger: model.BackupRunTriggerManual,
		})
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
			WithArgs(run.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(run.AppID))
		expectPGDatabaseMutationForBackup(mock, run.AppID, run.Target.ServiceName, model.OperationTypeDatabaseResize)
		mock.ExpectRollback()

		if _, err := s.CreateBackupRun(run); !errors.Is(err, ErrManagedPostgresResizeBackupConflict) {
			t.Fatalf("backup create during resize error = %v, want exact resize conflict", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("legacy pending claim", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 28, 12, 20, 0, 0, time.UTC)
		pending := model.NormalizeBackupRun(model.BackupRun{
			ID: "backup_pending_resize", TenantID: "tenant_backup", ProjectID: "project_backup", AppID: "app_backup",
			Target: model.BackupTarget{
				Type: model.BackupTargetAppDatabase, TenantID: "tenant_backup", ProjectID: "project_backup",
				AppID: "app_backup", ServiceName: "postgres-app",
			},
			BackendID: "backup_backend_r2", Trigger: model.BackupRunTriggerScheduled,
			Status: model.BackupRunStatusPending, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
		})
		failed := pending
		failed.Status = model.BackupRunStatusFailed
		failed.ErrorCode = ManagedPostgresResizeBackupConflictCode
		failed.ErrorMessage = ManagedPostgresResizeBackupConflictMessage
		failed.UpdatedAt = now
		failed.FinishedAt = &now
		targetJSON, _ := json.Marshal(pending.Target)

		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT COALESCE\(NULLIF\(app_id, ''\), target_app_id, ''\), target_json.*FROM fugue_backup_runs.*WHERE id = \$1`).
			WithArgs(pending.ID).
			WillReturnRows(sqlmock.NewRows([]string{"app_id", "target_json"}).AddRow(pending.AppID, targetJSON))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
			WithArgs(pending.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(pending.AppID))
		expectPGDatabaseMutationForBackup(mock, pending.AppID, pending.Target.ServiceName, model.OperationTypeDatabaseResize)
		mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*SET status = \$2.*error_code = \$3.*error_message = \$4.*WHERE id = \$1.*AND status = \$6.*RETURNING`).
			WithArgs(
				pending.ID, model.BackupRunStatusFailed, ManagedPostgresResizeBackupConflictCode,
				ManagedPostgresResizeBackupConflictMessage, now, model.BackupRunStatusPending,
			).
			WillReturnRows(backupScheduleRunRows(failed))
		mock.ExpectCommit()

		observed, err := s.ClaimBackupRun(pending.ID, "backup-worker", now, 2*time.Minute)
		if !errors.Is(err, ErrManagedPostgresResizeBackupConflict) ||
			observed.Status != model.BackupRunStatusFailed ||
			observed.ErrorCode != ManagedPostgresResizeBackupConflictCode ||
			observed.ErrorMessage != ManagedPostgresResizeBackupConflictMessage {
			t.Fatalf("legacy claim did not persist exact resize conflict: run=%+v err=%v", observed, err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGCreateBackupRestoreRunUsesAppFenceAndAtomicPlanTransition(t *testing.T) {
	t.Parallel()
	const (
		tenantID  = "tenant_restore_resize"
		projectID = "project_restore_resize"
		appID     = "app_restore_resize"
		planID    = "plan_restore_resize"
	)
	now := time.Date(2026, time.July, 28, 12, 30, 0, 0, time.UTC)
	plan := model.NormalizeBackupRestorePlan(model.BackupRestorePlan{
		ID: planID, TenantID: tenantID, ProjectID: projectID, AppID: appID, ArtifactID: "artifact_restore_resize",
		Target: model.BackupTarget{
			Type: model.BackupTargetAppDatabase, TenantID: tenantID, ProjectID: projectID, AppID: appID, ServiceName: "postgres-app",
		},
		Mode: model.BackupRestoreModeReplace, Status: model.BackupRestoreStatusPlanned,
		Phases:    []model.BackupRestorePhase{{Name: "restore-target", Status: model.BackupRestoreStatusPlanned}},
		CreatedAt: now, UpdatedAt: now,
	})

	t.Run("active resize rejects run under app lock", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("create restore conflict sqlmock db: %v", err)
		}
		defer db.Close()
		s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
		mock.ExpectBegin()
		expectPGResizeRestorePlanForUpdate(mock, plan)
		expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0)
		expectPGManagedPostgresExclusiveMutation(mock, appID, "", true)
		mock.ExpectRollback()

		if _, err := s.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: plan.ID}); !errors.Is(err, ErrManagedPostgresRestoreMutationConflict) {
			t.Fatalf("restore run during resize error = %v, want exact mutation conflict", err)
		}
		assertResizeInterlockPGExpectations(t, mock)
	})

	t.Run("successful run and plan transition commit together", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("create restore success sqlmock db: %v", err)
		}
		defer db.Close()
		s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
		createdAt := now.Add(time.Minute)
		input := model.BackupRestoreRun{
			ID: "restore_run_created", PlanID: plan.ID, RequestedByType: model.ActorTypeAPIKey,
			RequestedByID: "restore-user", CreatedAt: createdAt,
		}
		returned := model.NormalizeBackupRestoreRun(model.BackupRestoreRun{
			ID: input.ID, PlanID: plan.ID, TenantID: tenantID, ProjectID: projectID, AppID: appID,
			ArtifactID: plan.ArtifactID, Mode: plan.Mode, Status: model.BackupRestoreStatusPlanned,
			Phases: plan.Phases, RequestedByType: input.RequestedByType, RequestedByID: input.RequestedByID,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		})
		mock.ExpectBegin()
		expectPGResizeRestorePlanForUpdate(mock, plan)
		expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0)
		expectPGManagedPostgresExclusiveMutation(mock, appID, "", false)
		mock.ExpectQuery(`(?s)INSERT INTO fugue_backup_restore_runs.*RETURNING`).
			WithArgs(
				input.ID, plan.ID, tenantID, projectID, appID, plan.ArtifactID, plan.Mode,
				model.BackupRestoreStatusPlanned, sqlmock.AnyArg(), "", "", input.RequestedByType,
				input.RequestedByID, createdAt, sqlmock.AnyArg(), nil, nil,
			).
			WillReturnRows(pgResizeRestoreRunRows(returned))
		mock.ExpectExec(`(?s)UPDATE fugue_backup_restore_plans.*SET status = \$2, updated_at = \$3.*WHERE id = \$1`).
			WithArgs(plan.ID, model.BackupRestoreStatusRunning, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		created, err := s.CreateBackupRestoreRun(input)
		if err != nil {
			t.Fatalf("create fenced restore run: %v", err)
		}
		if created.ID != input.ID || created.AppID != appID || created.TenantID != tenantID ||
			created.ArtifactID != plan.ArtifactID || created.Mode != plan.Mode ||
			created.Status != model.BackupRestoreStatusPlanned {
			t.Fatalf("restore run did not inherit exact plan identity: %+v", created)
		}
		assertResizeInterlockPGExpectations(t, mock)
	})
}

func expectPGManagedPostgresExclusiveMutation(mock sqlmock.Sqlmock, appID, serviceID string, active bool) {
	pattern := `(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2, \$3\).*status IN \(\$4, \$5, \$6\)`
	args := []driver.Value{
		model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume, model.OperationTypeDatabaseResize,
		model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent,
	}
	if appID != "" {
		pattern += `.*app_id = \$7`
		args = append(args, appID)
	}
	if serviceID != "" {
		pattern += `.*service_id = \$` + strconv.Itoa(len(args)+1)
		args = append(args, serviceID)
	}
	mock.ExpectQuery(pattern).
		WithArgs(args...).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(active))
}

func expectPGActiveAppDatabaseBackupForResize(mock sqlmock.Sqlmock, appID string, active bool) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_backup_runs.*status IN \(\$1, \$2\).*target_type = \$3.*COALESCE\(app_id, ''\) = \$4`).
		WithArgs(
			model.BackupRunStatusPending, model.BackupRunStatusRunning, model.BackupTargetAppDatabase,
			appID, "demo-postgres", "demo-postgres",
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(active))
}

func expectPGActiveAppDatabaseImportForResize(mock sqlmock.Sqlmock, appID string, active bool) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_app_database_import_jobs.*WHERE app_id = \$1.*status IN \(\$2, \$3\)`).
		WithArgs(appID, model.OperationStatusPending, model.OperationStatusRunning).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(active))
}

func expectPGResizeRestorePlanForUpdate(mock sqlmock.Sqlmock, plan model.BackupRestorePlan) {
	targetJSON, _ := json.Marshal(plan.Target)
	warningsJSON, _ := json.Marshal(plan.Warnings)
	phasesJSON, _ := json.Marshal(plan.Phases)
	mock.ExpectQuery(`(?s)FROM fugue_backup_restore_plans.*WHERE id = \$1 FOR UPDATE`).
		WithArgs(plan.ID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "app_id", "artifact_id", "target_json", "mode", "status",
			"warnings_json", "phases_json", "created_by_type", "created_by_id", "created_at", "updated_at",
		}).AddRow(
			plan.ID, plan.TenantID, plan.ProjectID, plan.AppID, plan.ArtifactID, targetJSON, plan.Mode, plan.Status,
			warningsJSON, phasesJSON, plan.CreatedByType, plan.CreatedByID, plan.CreatedAt, plan.UpdatedAt,
		))
}

func pgResizeRestoreRunRows(run model.BackupRestoreRun) *sqlmock.Rows {
	phasesJSON, _ := json.Marshal(run.Phases)
	return sqlmock.NewRows([]string{
		"id", "plan_id", "tenant_id", "project_id", "app_id", "artifact_id", "mode", "status", "phases_json",
		"error_code", "error_message", "requested_by_type", "requested_by_id", "created_at", "updated_at", "started_at", "finished_at",
	}).AddRow(
		run.ID, run.PlanID, run.TenantID, run.ProjectID, run.AppID, run.ArtifactID, run.Mode, run.Status, phasesJSON,
		run.ErrorCode, run.ErrorMessage, run.RequestedByType, run.RequestedByID, run.CreatedAt, run.UpdatedAt, run.StartedAt, run.FinishedAt,
	)
}

func assertResizeInterlockPGExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("resize interlock sqlmock expectations: %v", err)
	}
}
