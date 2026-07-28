package store

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestManagedPostgresResizeBlocksLaterAppAndServiceMutations(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	consumer, err := f.store.CreateApp(f.tenant.ID, f.project.ID, "resize-consumer", "", model.AppSpec{
		Image:     "ghcr.io/example/consumer:1",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create resize consumer: %v", err)
	}
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
		t.Fatalf("create active resize: %v", err)
	}

	baselineApp, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatalf("get resize app baseline: %v", err)
	}
	baselineService, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get resize service baseline: %v", err)
	}
	zero := 0
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "app scale operation",
			call: func() error {
				_, err := f.store.CreateOperation(model.Operation{
					TenantID: f.tenant.ID, Type: model.OperationTypeScale, AppID: f.app.ID, DesiredReplicas: &zero,
				})
				return err
			},
		},
		{
			name: "observed postgres sync",
			call: func() error {
				_, err := f.store.SyncObservedManagedPostgresSpec(f.app.ID, baselineApp.Spec)
				return err
			},
		},
		{
			name: "observed app baseline sync",
			call: func() error {
				_, err := f.store.SyncObservedManagedAppBaseline(f.app.ID, baselineApp.Spec, model.AppBuildSource(baselineApp))
				return err
			},
		},
		{
			name: "service spec update",
			call: func() error {
				_, err := f.store.UpdateBackingServiceSpec(f.service.ID, baselineService.Spec)
				return err
			},
		},
		{
			name: "service delete",
			call: func() error {
				_, err := f.store.DeleteBackingService(f.service.ID)
				return err
			},
		},
		{
			name: "service bind",
			call: func() error {
				_, err := f.store.BindBackingService(f.tenant.ID, consumer.ID, f.service.ID, "postgres", nil)
				return err
			},
		},
		{
			name: "service unbind",
			call: func() error {
				_, err := f.store.UnbindBackingService(f.app.Bindings[0].ID)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, ErrConflict) {
				t.Fatalf("mutation during resize error = %v, want conflict", err)
			}
		})
	}

	afterApp, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatalf("get app after rejected mutations: %v", err)
	}
	afterService, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get service after rejected mutations: %v", err)
	}
	if !reflect.DeepEqual(afterApp.Spec, baselineApp.Spec) || !reflect.DeepEqual(afterService.Spec, baselineService.Spec) {
		t.Fatalf("rejected mutation changed desired state: app_before=%+v app_after=%+v service_before=%+v service_after=%+v",
			baselineApp.Spec, afterApp.Spec, baselineService.Spec, afterService.Spec)
	}
	operations, err := f.store.ListOperationsByApp(f.tenant.ID, false, f.app.ID)
	if err != nil {
		t.Fatalf("list resize operations: %v", err)
	}
	if len(operations) != 1 || operations[0].Type != model.OperationTypeDatabaseResize ||
		!isActiveOperationStatus(operations[0].Status) {
		t.Fatalf("rejected mutations changed active operation set: %+v", operations)
	}
}

func TestManagedPostgresResizeBlocksDatabaseBackupAndFailsLegacyClaim(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	backend, err := f.store.CreateBackupBackend(model.BackupBackend{
		Name: "resize-r2", Provider: model.DataBackendProviderCloudflareR2, Bucket: "resize", Endpoint: "https://example.invalid",
	})
	if err != nil {
		t.Fatalf("create resize backup backend: %v", err)
	}
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
		t.Fatalf("create active resize: %v", err)
	}

	run := managedPostgresInterlockBackupRun(f.app, f.service, backend, model.BackupRunTriggerManual)
	spoofed := run
	spoofed.AppID = "app-interlock-bypass"
	if _, err := f.store.CreateBackupRun(spoofed); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched backup app identity error = %v, want invalid input", err)
	}
	if _, err := f.store.CreateBackupRun(run); !errors.Is(err, ErrManagedPostgresResizeBackupConflict) {
		t.Fatalf("database backup during resize error = %v, want exact resize conflict", err)
	}
	runs, err := f.store.ListBackupRuns(BackupRunFilter{TenantID: f.tenant.ID})
	if err != nil || len(runs) != 0 {
		t.Fatalf("rejected backup create leaked a run: runs=%+v err=%v", runs, err)
	}

	legacy := managedPostgresInterlockBackupRun(f.app, f.service, backend, model.BackupRunTriggerScheduled)
	legacy.ID = model.NewID("backup_run")
	legacy.CreatedAt = time.Now().UTC().Add(-time.Minute)
	legacy.UpdatedAt = legacy.CreatedAt
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.BackupRuns = append(state.BackupRuns, model.NormalizeBackupRun(legacy))
		return nil
	}); err != nil {
		t.Fatalf("seed legacy pending backup: %v", err)
	}
	failed, err := f.store.ClaimBackupRun(legacy.ID, "backup-worker", time.Now().UTC(), 2*time.Minute)
	if !errors.Is(err, ErrManagedPostgresResizeBackupConflict) {
		t.Fatalf("legacy backup claim during resize error = %v, want exact resize conflict", err)
	}
	if failed.Status != model.BackupRunStatusFailed ||
		failed.ErrorCode != ManagedPostgresResizeBackupConflictCode ||
		failed.ErrorMessage != ManagedPostgresResizeBackupConflictMessage || failed.FinishedAt == nil {
		t.Fatalf("legacy backup was not terminally failed with exact resize evidence: %+v", failed)
	}

	nonDatabase, err := f.store.CreateBackupRun(model.BackupRun{
		TenantID: f.tenant.ID, ProjectID: f.project.ID, AppID: f.app.ID,
		Target: model.BackupTarget{
			Type: model.BackupTargetPersistentStorage, TenantID: f.tenant.ID, ProjectID: f.project.ID,
			AppID: f.app.ID, RuntimeID: f.app.Spec.RuntimeID,
		},
		BackendID: backend.ID, Trigger: model.BackupRunTriggerManual,
	})
	if err != nil || nonDatabase.Status != model.BackupRunStatusPending {
		t.Fatalf("resize incorrectly blocked non-database backup: run=%+v err=%v", nonDatabase, err)
	}
}

func TestManagedPostgresResumeBlocksDatabaseBackupWithExactReason(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	backend, err := f.store.CreateBackupBackend(model.BackupBackend{
		Name: "resume-r2", Provider: model.DataBackendProviderCloudflareR2, Bucket: "resume", Endpoint: "https://example.invalid",
	})
	if err != nil {
		t.Fatalf("create resume backup backend: %v", err)
	}
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.Operations = append(state.Operations, model.Operation{
			ID: model.NewID("operation"), TenantID: f.tenant.ID, AppID: f.app.ID, ServiceID: f.service.ID,
			Type: model.OperationTypeDatabaseResume, Status: model.OperationStatusRunning,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed active resume: %v", err)
	}
	run := managedPostgresInterlockBackupRun(f.app, f.service, backend, model.BackupRunTriggerManual)
	if _, err := f.store.CreateBackupRun(run); !errors.Is(err, ErrManagedPostgresResumeBackupConflict) {
		t.Fatalf("database backup during resume error = %v, want exact resume conflict", err)
	}
}

func TestManagedPostgresResizeBlocksDatabaseImportAndFailsLegacyClaim(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	upload, err := f.store.CreateSourceUpload(f.tenant.ID, "resize.sql", "application/sql", []byte("select 1;"))
	if err != nil {
		t.Fatalf("create database import upload: %v", err)
	}
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
		t.Fatalf("create active resize: %v", err)
	}
	job := model.AppDatabaseImportJob{
		TenantID: f.tenant.ID, AppID: f.app.ID, SourceUploadID: upload.ID,
		SourceUploadFilename: upload.Filename, SourceUploadSHA256: upload.SHA256,
		Format: model.AppDatabaseImportFormatSQL, Status: model.OperationStatusPending,
	}
	if err := f.store.ValidateAppDatabaseImportRunnable(f.app.ID); !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("database import preflight during resize error = %v, want exact import conflict", err)
	}
	if _, err := f.store.CreateAppDatabaseImportJob(job); !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("database import create during resize error = %v, want exact import conflict", err)
	}

	job.ID = model.NewID("dbimport")
	job.CreatedAt = time.Now().UTC().Add(-time.Minute)
	job.UpdatedAt = job.CreatedAt
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.AppDatabaseImportJobs = append(state.AppDatabaseImportJobs, job)
		return nil
	}); err != nil {
		t.Fatalf("seed legacy pending import: %v", err)
	}
	if _, err := f.store.ClaimAppDatabaseImportJob(job.ID); !errors.Is(err, ErrManagedPostgresDatabaseImportConflict) {
		t.Fatalf("legacy import claim during resize error = %v, want exact import conflict", err)
	}
	stored, err := f.store.GetAppDatabaseImportJob(f.app.ID, job.ID)
	if err != nil {
		t.Fatalf("get terminal import: %v", err)
	}
	if stored.Status != model.OperationStatusFailed || stored.CompletedAt == nil ||
		stored.ErrorMessage != ManagedPostgresDatabaseImportConflictMessage {
		t.Fatalf("legacy import was not terminally failed with exact evidence: %+v", stored)
	}
}

func TestManagedPostgresResizeAndDatabaseRestoreAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	t.Run("resize first rejects restore without mutating plan", func(t *testing.T) {
		f := newManagedPostgresResizeFixture(t)
		plan := seedResizeRestorePlan(t, f)
		if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
			t.Fatalf("create active resize: %v", err)
		}
		if _, err := f.store.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: plan.ID}); !errors.Is(err, ErrManagedPostgresRestoreMutationConflict) {
			t.Fatalf("restore during resize error = %v, want exact mutation conflict", err)
		}
		stored, err := f.store.GetBackupRestorePlan(plan.ID, f.tenant.ID, false)
		if err != nil {
			t.Fatalf("get rejected restore plan: %v", err)
		}
		if stored.Status != model.BackupRestoreStatusPlanned {
			t.Fatalf("rejected restore mutated plan: %+v", stored)
		}
		runs, err := f.store.ListBackupRestoreRuns(f.tenant.ID, false, 100)
		if err != nil || len(runs) != 0 {
			t.Fatalf("rejected restore leaked run: runs=%+v err=%v", runs, err)
		}
	})

	t.Run("restore first rejects resize with exact reason", func(t *testing.T) {
		f := newManagedPostgresResizeFixture(t)
		plan := seedResizeRestorePlan(t, f)
		run, err := f.store.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: plan.ID})
		if err != nil || run.Status != model.BackupRestoreStatusPlanned {
			t.Fatalf("create active restore run: run=%+v err=%v", run, err)
		}
		if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); !errors.Is(err, ErrManagedPostgresRestoreInProgressConflict) {
			t.Fatalf("resize during restore error = %v, want exact restore conflict", err)
		}
	})

	t.Run("restore run cannot override plan identity or status", func(t *testing.T) {
		f := newManagedPostgresResizeFixture(t)
		plan := seedResizeRestorePlan(t, f)
		for _, run := range []model.BackupRestoreRun{
			{PlanID: plan.ID, AppID: "app-attacker"},
			{PlanID: plan.ID, TenantID: "tenant-attacker"},
			{PlanID: plan.ID, ArtifactID: "artifact-attacker"},
			{PlanID: plan.ID, Status: model.BackupRestoreStatusSucceeded},
		} {
			if _, err := f.store.CreateBackupRestoreRun(run); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("restore identity/status override was accepted: run=%+v err=%v", run, err)
			}
		}
	})
}

func TestBackupRestorePlanTargetScopeCannotBypassAppFence(t *testing.T) {
	t.Parallel()
	plan := model.BackupRestorePlan{
		TenantID: "tenant-exact", ProjectID: "project-exact", AppID: "app-exact",
		Target: model.BackupTarget{
			Type: model.BackupTargetAppDatabase, TenantID: "tenant-exact", ProjectID: "project-exact", AppID: "app-bypass",
		},
	}
	if err := reconcileBackupRestorePlanTargetScope(&plan); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched restore target app error = %v, want invalid input", err)
	}
}

func TestManagedPostgresResizeDependencyCreationRacesAdmitExactlyOneSide(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		prepare func(*testing.T, managedPostgresResizeFixture) (func() error, func() bool)
	}{
		{
			name: "database backup",
			prepare: func(t *testing.T, f managedPostgresResizeFixture) (func() error, func() bool) {
				backend, err := f.store.CreateBackupBackend(model.BackupBackend{
					Name: "race-r2", Provider: model.DataBackendProviderCloudflareR2, Bucket: "race", Endpoint: "https://example.invalid",
				})
				if err != nil {
					t.Fatalf("create race backup backend: %v", err)
				}
				return func() error {
						_, err := f.store.CreateBackupRun(managedPostgresInterlockBackupRun(f.app, f.service, backend, model.BackupRunTriggerManual))
						return err
					}, func() bool {
						runs, err := f.store.ListBackupRuns(BackupRunFilter{TenantID: f.tenant.ID})
						return err == nil && len(runs) == 1 && (runs[0].Status == model.BackupRunStatusPending || runs[0].Status == model.BackupRunStatusRunning)
					}
			},
		},
		{
			name: "database import",
			prepare: func(t *testing.T, f managedPostgresResizeFixture) (func() error, func() bool) {
				upload, err := f.store.CreateSourceUpload(f.tenant.ID, "race.sql", "application/sql", []byte("select 1;"))
				if err != nil {
					t.Fatalf("create race import upload: %v", err)
				}
				return func() error {
						_, err := f.store.CreateAppDatabaseImportJob(model.AppDatabaseImportJob{
							TenantID: f.tenant.ID, AppID: f.app.ID, SourceUploadID: upload.ID,
							SourceUploadFilename: upload.Filename, SourceUploadSHA256: upload.SHA256,
							Format: model.AppDatabaseImportFormatSQL, Status: model.OperationStatusPending,
						})
						return err
					}, func() bool {
						jobs, err := f.store.ListAppDatabaseImportJobs(f.app.ID)
						return err == nil && len(jobs) == 1 && (jobs[0].Status == model.OperationStatusPending || jobs[0].Status == model.OperationStatusRunning)
					}
			},
		},
		{
			name: "database restore",
			prepare: func(t *testing.T, f managedPostgresResizeFixture) (func() error, func() bool) {
				plan := seedResizeRestorePlan(t, f)
				return func() error {
						_, err := f.store.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: plan.ID})
						return err
					}, func() bool {
						runs, err := f.store.ListBackupRestoreRuns(f.tenant.ID, false, 100)
						return err == nil && len(runs) == 1 &&
							(runs[0].Status == model.BackupRestoreStatusPlanned || runs[0].Status == model.BackupRestoreStatusRunning)
					}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newManagedPostgresResizeFixture(t)
			createDependency, dependencyActive := test.prepare(t, f)
			start := make(chan struct{})
			results := make(chan error, 2)
			go func() {
				<-start
				_, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget()))
				results <- err
			}()
			go func() {
				<-start
				results <- createDependency()
			}()
			close(start)
			first, second := <-results, <-results
			succeeded := 0
			for _, err := range []error{first, second} {
				if err == nil {
					succeeded++
					continue
				}
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("unexpected race error: %v", err)
				}
			}
			if succeeded != 1 {
				t.Fatalf("race admitted %d successful sides, want exactly one: first=%v second=%v", succeeded, first, second)
			}
			operations, err := f.store.ListOperationsByApp(f.tenant.ID, false, f.app.ID)
			if err != nil {
				t.Fatalf("list race operations: %v", err)
			}
			resizeActive := false
			for _, operation := range operations {
				if operation.Type == model.OperationTypeDatabaseResize && isActiveOperationStatus(operation.Status) {
					resizeActive = true
				}
			}
			if resizeActive == dependencyActive() {
				t.Fatalf("exclusive race invariant violated: resize_active=%t dependency_active=%t", resizeActive, dependencyActive())
			}
		})
	}
}

func seedResizeRestorePlan(t *testing.T, f managedPostgresResizeFixture) model.BackupRestorePlan {
	t.Helper()
	now := time.Now().UTC()
	plan := model.NormalizeBackupRestorePlan(model.BackupRestorePlan{
		ID: model.NewID("backup_restore_plan"), TenantID: f.tenant.ID, ProjectID: f.project.ID, AppID: f.app.ID,
		ArtifactID: "artifact-resize", Target: model.BackupTarget{
			Type: model.BackupTargetAppDatabase, TenantID: f.tenant.ID, ProjectID: f.project.ID,
			AppID: f.app.ID, ServiceName: f.service.Name,
		},
		Mode: model.BackupRestoreModeReplace, Status: model.BackupRestoreStatusPlanned,
		CreatedAt: now, UpdatedAt: now,
	})
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.BackupRestorePlans = append(state.BackupRestorePlans, plan)
		return nil
	}); err != nil {
		t.Fatalf("seed resize restore plan: %v", err)
	}
	return plan
}
