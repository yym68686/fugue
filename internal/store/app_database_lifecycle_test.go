package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAppDatabaseImportRejectsSuspendedOrSuspendingManagedPostgres(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		prepare func(*testing.T, *Store, model.App, model.BackingService)
	}{
		{
			name: "active suspend operation",
			prepare: func(t *testing.T, stateStore *Store, app model.App, service model.BackingService) {
				t.Helper()
				if _, err := stateStore.CreateOperation(model.Operation{
					TenantID:  app.TenantID,
					Type:      model.OperationTypeDatabaseSuspend,
					AppID:     app.ID,
					ServiceID: service.ID,
				}); err != nil {
					t.Fatalf("create active suspend: %v", err)
				}
			},
		},
		{
			name: "persisted suspended database",
			prepare: func(t *testing.T, stateStore *Store, app model.App, service model.BackingService) {
				t.Helper()
				op, err := stateStore.CreateOperation(model.Operation{
					TenantID:  app.TenantID,
					Type:      model.OperationTypeDatabaseSuspend,
					AppID:     app.ID,
					ServiceID: service.ID,
				})
				if err != nil {
					t.Fatalf("create suspend: %v", err)
				}
				if _, err := stateStore.CompleteManagedOperation(op.ID, "", "database suspended"); err != nil {
					t.Fatalf("complete suspend: %v", err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			stateStore, app, service, upload := appDatabaseImportLifecycleFixture(t)
			testCase.prepare(t, stateStore, app, service)

			if err := stateStore.ValidateAppDatabaseImportRunnable(app.ID); !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
				t.Fatalf("ValidateAppDatabaseImportRunnable error = %v, want managed postgres import conflict", err)
			}
			_, err := stateStore.CreateAppDatabaseImportJob(appDatabaseImportLifecycleJob(app, upload))
			if !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
				t.Fatalf("CreateAppDatabaseImportJob error = %v, want managed postgres import conflict", err)
			}
			jobs, err := stateStore.ListAppDatabaseImportJobs(app.ID)
			if err != nil {
				t.Fatalf("list database import jobs: %v", err)
			}
			if len(jobs) != 0 {
				t.Fatalf("blocked create persisted jobs: %+v", jobs)
			}
		})
	}
}

func TestAppDatabaseImportClaimAtomicallyFailsQueuedJobWhenDatabaseBecomesSuspended(t *testing.T) {
	t.Parallel()

	stateStore, app, service, upload := appDatabaseImportLifecycleFixture(t)
	job, err := stateStore.CreateAppDatabaseImportJob(appDatabaseImportLifecycleJob(app, upload))
	if err != nil {
		t.Fatalf("create database import job: %v", err)
	}
	// Simulate a legacy/race state that predates mutual exclusion. Claim must
	// consume this queued job into a terminal state instead of retrying forever.
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, service.ID)
		if index < 0 || state.BackingServices[index].Spec.Postgres == nil {
			return ErrNotFound
		}
		state.BackingServices[index].Spec.Postgres.Suspended = true
		return nil
	}); err != nil {
		t.Fatalf("prepare suspended race state: %v", err)
	}

	if _, err := stateStore.ClaimAppDatabaseImportJob(job.ID); !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("ClaimAppDatabaseImportJob error = %v, want managed postgres import conflict", err)
	}
	stored, err := stateStore.GetAppDatabaseImportJob(app.ID, job.ID)
	if err != nil {
		t.Fatalf("get terminal database import job: %v", err)
	}
	if stored.Status != model.OperationStatusFailed || stored.CompletedAt == nil || stored.StartedAt != nil {
		t.Fatalf("blocked claim did not become a clean terminal failure: %+v", stored)
	}
	if stored.ErrorMessage != ManagedPostgresDatabaseImportConflictMessage {
		t.Fatalf("blocked claim error_message = %q, want %q", stored.ErrorMessage, ManagedPostgresDatabaseImportConflictMessage)
	}
	pending, err := stateStore.ListPendingAppDatabaseImportJobs(10)
	if err != nil {
		t.Fatalf("list pending database import jobs: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("terminally failed job remained pending: %+v", pending)
	}
}

func TestManagedPostgresSuspendReturnsActionableConflictForActiveDatabaseImport(t *testing.T) {
	t.Parallel()

	for _, claim := range []bool{false, true} {
		name := "pending"
		if claim {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stateStore, app, service, upload := appDatabaseImportLifecycleFixture(t)
			job, err := stateStore.CreateAppDatabaseImportJob(appDatabaseImportLifecycleJob(app, upload))
			if err != nil {
				t.Fatalf("create database import job: %v", err)
			}
			if claim {
				if _, err := stateStore.ClaimAppDatabaseImportJob(job.ID); err != nil {
					t.Fatalf("claim database import job: %v", err)
				}
			}
			_, err = stateStore.CreateOperation(model.Operation{
				TenantID:  app.TenantID,
				Type:      model.OperationTypeDatabaseSuspend,
				AppID:     app.ID,
				ServiceID: service.ID,
			})
			if !errors.Is(err, ErrManagedPostgresImportInProgressConflict) {
				t.Fatalf("suspend error = %v, want actionable import-in-progress conflict", err)
			}
			if !strings.Contains(err.Error(), ManagedPostgresImportInProgressConflictMessage) {
				t.Fatalf("suspend conflict = %q, want stable message %q", err.Error(), ManagedPostgresImportInProgressConflictMessage)
			}
		})
	}
}

func appDatabaseImportLifecycleFixture(t *testing.T) (*Store, model.App, model.BackingService, model.SourceUpload) {
	t.Helper()
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Database Import Lifecycle")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("raise tenant billing cap: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "database-import", "", model.AppSpec{
		Image:     "ghcr.io/example/database-import:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:    "database_import",
			User:        "importer",
			Password:    "test-only-password",
			StorageSize: "1Gi",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one owned managed postgres, got %+v", app.BackingServices)
	}
	zero := 0
	scale, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create app disable: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(scale.ID, "", "disabled for database import lifecycle test"); err != nil {
		t.Fatalf("complete app disable: %v", err)
	}
	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get disabled app: %v", err)
	}
	upload, err := stateStore.CreateSourceUpload(tenant.ID, "dump.sql", "application/sql", []byte("select 1;"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	return stateStore, app, app.BackingServices[0], upload
}

func appDatabaseImportLifecycleJob(app model.App, upload model.SourceUpload) model.AppDatabaseImportJob {
	return model.AppDatabaseImportJob{
		AppID:                app.ID,
		TenantID:             app.TenantID,
		SourceUploadID:       upload.ID,
		SourceUploadFilename: upload.Filename,
		SourceUploadSHA256:   upload.SHA256,
		Format:               model.AppDatabaseImportFormatSQL,
		Status:               model.OperationStatusPending,
	}
}
