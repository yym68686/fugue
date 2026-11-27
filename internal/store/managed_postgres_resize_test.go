package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
)

type managedPostgresResizeFixture struct {
	store     *Store
	path      string
	tenant    model.Tenant
	project   model.Project
	app       model.App
	service   model.BackingService
	bootstrap model.ResourceSpec
}

func newManagedPostgresResizeFixture(t *testing.T) managedPostgresResizeFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgres-fixture")
	s := newStoreTestDatabase(t, path)
	if err := s.Init(); err != nil {
		t.Fatalf("init resize store: %v", err)
	}
	tenant, err := s.CreateTenant("Postgres Resize")
	if err != nil {
		t.Fatalf("create resize tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create resize project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("raise resize billing cap: %v", err)
	}
	bootstrap := model.ResourceSpec{
		CPUMilliCores:        100,
		MemoryMebibytes:      512,
		CPULimitMilliCores:   200,
		MemoryLimitMebibytes: 768,
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "resize-demo", "", model.AppSpec{
		Image:     "ghcr.io/example/original:1",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:    "resize_demo",
			User:        "resize_demo",
			Password:    "secret",
			StorageSize: "1Gi",
			Resources:   model.CloneResourceSpec(&bootstrap),
		},
	})
	if err != nil {
		t.Fatalf("create resize app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one owned postgres service, got %+v", app.BackingServices)
	}
	return managedPostgresResizeFixture{
		store:     s,
		path:      path,
		tenant:    tenant,
		project:   project,
		app:       app,
		service:   app.BackingServices[0],
		bootstrap: bootstrap,
	}
}

func managedPostgresResizeTarget() model.ResourceSpec {
	return model.ResourceSpec{
		CPUMilliCores:        250,
		MemoryMebibytes:      640,
		CPULimitMilliCores:   500,
		MemoryLimitMebibytes: 1024,
	}
}

func TestManagedPostgresResizeExclusivelyBoundStandaloneService(t *testing.T) {
	f := newManagedPostgresResizeFixture(t)
	mutatePostgresResizeService(t, f, func(s *model.BackingService) { s.OwnerAppID = "" })
	target := managedPostgresResizeTarget()
	request := managedPostgresResizeOperation(f, target)
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("public resize contract allowed standalone database: %v", err)
	}
	request.RequestedByType = model.ActorTypeSystem
	request.RequestedByID = ManagedPostgresAutoCPURequestActorPrefix + "/cluster-uid/system-id"
	op, _, err := f.store.CreateOperationWithResult(request)
	if err != nil {
		t.Fatalf("queue standalone resize: %v", err)
	}
	if _, err := f.store.CompleteManagedOperation(op.ID, "", "verified in-place resize"); err != nil {
		t.Fatal(err)
	}
	service, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatal(err)
	}
	if service.OwnerAppID != "" || service.Spec.Postgres.RuntimeResources == nil || *service.Spec.Postgres.RuntimeResources != target || *service.Spec.Postgres.Resources != f.bootstrap {
		t.Fatalf("standalone resize changed ownership/bootstrap or lost runtime target: %+v", service.Spec.Postgres)
	}
	// An ownerless service bound to multiple applications must stay blocked.
	addPostgresResizeBinding(t, f)
	target.CPUMilliCores = 300
	request.DesiredSpec.Postgres.RuntimeResources = &target
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("shared standalone target accepted: %v", err)
	}
}

func TestManagedPostgresCPULimitAutoResizeRequiresSystemActorAndOwner(t *testing.T) {
	f := newManagedPostgresResizeFixture(t)
	target := f.bootstrap
	target.CPULimitMilliCores = 400
	request := managedPostgresResizeOperation(f, target)
	request.RequestedByID = ManagedPostgresAutoCPULimitActorPrefix + "/identity-hash/200"
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("non-system actor resized the CPU limit: %v", err)
	}
	request.RequestedByType = model.ActorTypeSystem
	op, result, err := f.store.CreateOperationWithResult(request)
	if err != nil || !result.Created {
		t.Fatalf("authorized enrolled system operation was rejected: result=%+v err=%v", result, err)
	}
	if _, err := f.store.CompleteManagedOperation(op.ID, "", "verified in-place CPU limit resize"); err != nil {
		t.Fatal(err)
	}
	service, err := f.store.GetBackingService(f.service.ID)
	if err != nil || service.OwnerAppID != f.app.ID || *service.Spec.Postgres.RuntimeResources != target || *service.Spec.Postgres.Resources != f.bootstrap {
		t.Fatalf("limit resize changed ownership/bootstrap or lost runtime target: service=%+v err=%v", service, err)
	}
	mutatePostgresResizeService(t, f, func(s *model.BackingService) { s.OwnerAppID = "" })
	request.DesiredSpec.Postgres.RuntimeResources = &target
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("CPU limit automation accepted a non-owner database: %v", err)
	}
}

func managedPostgresResizeOperation(f managedPostgresResizeFixture, target model.ResourceSpec) model.Operation {
	replicas := 99
	return model.Operation{
		TenantID:        f.tenant.ID,
		Type:            model.OperationTypeDatabaseResize,
		AppID:           f.app.ID,
		ServiceID:       f.service.ID,
		TargetRuntimeID: "runtime-attacker",
		DesiredReplicas: &replicas,
		DesiredSource:   &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/image"},
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "requester",
		DesiredSpec: &model.AppSpec{
			Image:     "ghcr.io/attacker/replaced:latest",
			Replicas:  99,
			RuntimeID: "runtime-attacker",
			Env:       map[string]string{"MUTATED": "true"},
			Postgres: &model.AppPostgresSpec{
				Database:         "attacker",
				RuntimeID:        "runtime-attacker",
				Resources:        &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999},
				RuntimeResources: model.CloneResourceSpec(&target),
			},
		},
	}
}

func TestManagedPostgresMemoryResizeStandaloneRequiresSystemActorAndExclusiveBinding(t *testing.T) {
	f := newManagedPostgresResizeFixture(t)
	mutatePostgresResizeService(t, f, func(s *model.BackingService) { s.OwnerAppID = "" })
	target := f.bootstrap
	target.MemoryMebibytes = 384
	request := managedPostgresResizeOperation(f, target)
	request.RequestedByID = ManagedPostgresAutoMemoryRequestActorPrefix + "/identity-hash/512"
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("non-system actor resized standalone database: %v", err)
	}
	request.RequestedByType = model.ActorTypeSystem
	op, _, err := f.store.CreateOperationWithResult(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CompleteManagedOperation(op.ID, "", "verified request-only resize"); err != nil {
		t.Fatal(err)
	}
	service, err := f.store.GetBackingService(f.service.ID)
	if err != nil || service.OwnerAppID != "" || *service.Spec.Postgres.Resources != f.bootstrap || *service.Spec.Postgres.RuntimeResources != target {
		t.Fatalf("resize changed ownership/bootstrap or lost runtime target: service=%+v err=%v", service, err)
	}
	addPostgresResizeBinding(t, f)
	target.MemoryMebibytes = 320
	request.DesiredSpec.Postgres.RuntimeResources = &target
	if _, _, err := f.store.CreateOperationWithResult(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("shared database admitted: %v", err)
	}
}

func TestManagedPostgresResizePersistsOnlyExactRuntimeTarget(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	baselineApp, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatalf("get baseline resize app: %v", err)
	}
	baselineService, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get baseline resize service: %v", err)
	}
	target := managedPostgresResizeTarget()

	created, result, err := f.store.CreateOperationWithResult(managedPostgresResizeOperation(f, target))
	if err != nil {
		t.Fatalf("create database resize operation: %v", err)
	}
	if !result.Created || created.Status != model.OperationStatusPending || created.ExecutionMode != model.ExecutionModeManaged {
		t.Fatalf("resize operation was not queued for the managed controller: op=%+v result=%+v", created, result)
	}
	if created.ServiceID != f.service.ID || created.SourceRuntimeID != model.DefaultManagedRuntimeID ||
		created.TargetRuntimeID != model.DefaultManagedRuntimeID || created.DesiredReplicas != nil ||
		created.DesiredSource != nil || created.DesiredOriginSource != nil {
		t.Fatalf("resize operation retained caller-controlled routing/source state: %+v", created)
	}
	if created.DesiredSpec == nil || created.DesiredSpec.Postgres == nil ||
		created.DesiredSpec.Postgres.RuntimeResources == nil ||
		*created.DesiredSpec.Postgres.RuntimeResources != target {
		t.Fatalf("resize operation lost the exact runtime target: %+v", created.DesiredSpec)
	}
	if created.DesiredSpec.Image != baselineApp.Spec.Image || created.DesiredSpec.Replicas != baselineApp.Spec.Replicas ||
		created.DesiredSpec.RuntimeID != baselineApp.Spec.RuntimeID || created.DesiredSpec.Env["MUTATED"] != "" {
		t.Fatalf("resize operation accepted caller-controlled app changes: %+v", created.DesiredSpec)
	}
	if created.DesiredSpec.Postgres.Resources == nil || *created.DesiredSpec.Postgres.Resources != f.bootstrap {
		t.Fatalf("resize operation changed the CNPG bootstrap template: %+v", created.DesiredSpec.Postgres.Resources)
	}
	queuedService, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get queued resize service: %v", err)
	}
	if !reflect.DeepEqual(queuedService.Spec, baselineService.Spec) {
		t.Fatalf("queued resize changed persisted service before completion: before=%+v after=%+v", baselineService.Spec, queuedService.Spec)
	}

	malicious := baselineApp.Spec
	malicious.Image = "ghcr.io/attacker/completion:latest"
	malicious.Replicas = 77
	malicious.RuntimeID = "runtime-attacker"
	malicious.Env = map[string]string{"MUTATED": "true"}
	malicious.Postgres = &model.AppPostgresSpec{
		Resources:        &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999},
		RuntimeResources: &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999, CPULimitMilliCores: 9999, MemoryLimitMebibytes: 9999},
	}
	completed, err := f.store.CompleteManagedOperationWithResult(
		created.ID,
		"",
		"database runtime resources persisted",
		&malicious,
		&model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/completion"},
	)
	if err != nil {
		t.Fatalf("complete database resize operation: %v", err)
	}
	if completed.Status != model.OperationStatusCompleted || completed.DesiredSpec == nil ||
		completed.DesiredSpec.Postgres == nil || completed.DesiredSpec.Postgres.RuntimeResources == nil ||
		*completed.DesiredSpec.Postgres.RuntimeResources != target {
		t.Fatalf("completion replaced the server-owned resize target: %+v", completed)
	}

	afterService, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get resized service: %v", err)
	}
	if afterService.Spec.Postgres == nil || afterService.Spec.Postgres.RuntimeResources == nil ||
		*afterService.Spec.Postgres.RuntimeResources != target {
		t.Fatalf("runtime target was not persisted on the exact service: %+v", afterService.Spec.Postgres)
	}
	beforePostgres := model.CloneAppPostgresSpec(baselineService.Spec.Postgres)
	afterPostgres := model.CloneAppPostgresSpec(afterService.Spec.Postgres)
	beforePostgres.RuntimeResources = nil
	afterPostgres.RuntimeResources = nil
	if !reflect.DeepEqual(beforePostgres, afterPostgres) {
		t.Fatalf("resize completion changed bootstrap or database identity: before=%+v after=%+v", beforePostgres, afterPostgres)
	}
	afterApp, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatalf("get app after resize completion: %v", err)
	}
	if !reflect.DeepEqual(afterApp.Spec, baselineApp.Spec) || !reflect.DeepEqual(afterApp.Status, baselineApp.Status) ||
		!afterApp.UpdatedAt.Equal(baselineApp.UpdatedAt) || !reflect.DeepEqual(afterApp.Source, baselineApp.Source) ||
		!reflect.DeepEqual(afterApp.BuildSource, baselineApp.BuildSource) || !reflect.DeepEqual(afterApp.OriginSource, baselineApp.OriginSource) {
		t.Fatalf("database resize changed app state: before=%+v after=%+v", baselineApp, afterApp)
	}

	reopened := newStoreTestDatabase(t, f.path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen resize store: %v", err)
	}
	persisted, err := reopened.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get reopened resize service: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.RuntimeResources == nil ||
		*persisted.Spec.Postgres.RuntimeResources != target {
		t.Fatalf("runtime target did not survive JSON persistence: %+v", persisted.Spec.Postgres)
	}
	persistedOperation, err := reopened.GetOperation(completed.ID)
	if err != nil {
		t.Fatalf("get reopened resize operation: %v", err)
	}
	if persistedOperation.DesiredSpec == nil || persistedOperation.DesiredSpec.Postgres == nil ||
		persistedOperation.DesiredSpec.Postgres.RuntimeResources == nil ||
		*persistedOperation.DesiredSpec.Postgres.RuntimeResources != target {
		t.Fatalf("resize operation target did not survive JSON persistence: %+v", persistedOperation)
	}
}

func TestManagedPostgresResizeRetryIsExactAndManaged(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	target := managedPostgresResizeTarget()
	request := managedPostgresResizeOperation(f, target)
	created, result, err := f.store.CreateOperationWithResult(request)
	if err != nil || !result.Created {
		t.Fatalf("create first resize operation: op=%+v result=%+v err=%v", created, result, err)
	}
	reused, result, err := f.store.CreateOperationWithResult(request)
	if err != nil || result.Created || reused.ID != created.ID {
		t.Fatalf("exact retry did not reuse active resize: first=%+v retry=%+v result=%+v err=%v", created, reused, result, err)
	}
	changed := target
	changed.CPUMilliCores++
	if returned, result, err := f.store.CreateOperationWithResult(managedPostgresResizeOperation(f, changed)); !errors.Is(err, ErrConflict) || returned.ID != "" || returned.DesiredSpec != nil || result.Created {
		t.Fatalf("changed active resize did not fail closed: op=%+v result=%+v err=%v", returned, result, err)
	}
	claimed, found, err := f.store.TryClaimPendingOperation(created.ID)
	if err != nil || !found || claimed.Status != model.OperationStatusRunning ||
		claimed.ExecutionMode != model.ExecutionModeManaged || claimed.AssignedRuntimeID != "" {
		t.Fatalf("resize escaped the managed controller: op=%+v found=%t err=%v", claimed, found, err)
	}
}

func TestManagedPostgresResizeRejectsIncompleteOrUnsafeIntent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, *managedPostgresResizeFixture, *model.Operation)
		want   error
	}{
		{
			name: "missing exact service",
			mutate: func(_ *testing.T, _ *managedPostgresResizeFixture, op *model.Operation) {
				op.ServiceID = ""
			},
			want: ErrInvalidInput,
		},
		{
			name: "removing configured cpu limit",
			mutate: func(_ *testing.T, _ *managedPostgresResizeFixture, op *model.Operation) {
				op.DesiredSpec.Postgres.RuntimeResources.CPULimitMilliCores = 0
			},
			want: ErrInvalidInput,
		},
		{
			name: "request exceeds limit",
			mutate: func(_ *testing.T, _ *managedPostgresResizeFixture, op *model.Operation) {
				op.DesiredSpec.Postgres.RuntimeResources.CPUMilliCores = 600
			},
			want: ErrInvalidInput,
		},
		{
			name: "suspended database",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				mutatePostgresResizeService(t, *f, func(s *model.BackingService) { s.Spec.Postgres.Suspended = true })
			},
			want: ErrConflict,
		},
		{
			name: "database owned by another app",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				other := createPostgresResizePeer(t, *f)
				mutatePostgresResizeService(t, *f, func(s *model.BackingService) { s.OwnerAppID = other.ID })
			},
			want: ErrConflict,
		},
		{
			name: "multiple bindings",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				addPostgresResizeBinding(t, *f)
			},
			want: ErrConflict,
		},
		{
			name: "active backup",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				backend, err := f.store.CreateBackupBackend(model.BackupBackend{
					TenantID: f.tenant.ID, Name: "resize-backup",
					Provider: model.DataBackendProviderS3, Bucket: "backups",
				})
				if err != nil {
					t.Fatalf("create backup backend: %v", err)
				}
				run, err := f.store.CreateBackupRun(managedPostgresInterlockBackupRun(f.app, f.service, backend, model.BackupRunTriggerManual))
				if err != nil || run.Status != model.BackupRunStatusPending {
					t.Fatalf("create active backup: status=%s err=%v", run.Status, err)
				}
			},
			want: ErrConflict,
		},
		{
			name: "active import",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				upload, err := f.store.CreateSourceUpload(f.tenant.ID, "data.sql", "application/sql", []byte("SELECT 1;"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.CreateAppDatabaseImportJob(model.AppDatabaseImportJob{TenantID: f.tenant.ID, AppID: f.app.ID, SourceUploadID: upload.ID}); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrConflict,
		},
		{
			name: "active restore",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				p := seedResizeRestorePlan(t, *f)
				if _, err := f.store.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: p.ID}); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrConflict,
		},
		{
			name: "active app operation",
			mutate: func(t *testing.T, f *managedPostgresResizeFixture, _ *model.Operation) {
				op, err := f.store.CreateOperation(model.Operation{TenantID: f.tenant.ID, AppID: f.app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &f.app.Spec})
				if err != nil {
					t.Fatal(err)
				}
				if _, ok, err := f.store.TryClaimPendingOperation(op.ID); err != nil || !ok {
					t.Fatal(ok, err)
				}
			},
			want: ErrConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newManagedPostgresResizeFixture(t)
			op := managedPostgresResizeOperation(f, managedPostgresResizeTarget())
			test.mutate(t, &f, &op)
			before, err := f.store.ListOperationsByApp(f.tenant.ID, false, f.app.ID)
			if err != nil {
				t.Fatalf("list operations before rejected resize: %v", err)
			}
			created, result, err := f.store.CreateOperationWithResult(op)
			if !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
			if created.ID != "" || created.DesiredSpec != nil || result.Created {
				t.Fatalf("rejected resize leaked an operation: op=%+v result=%+v", created, result)
			}
			after, listErr := f.store.ListOperationsByApp(f.tenant.ID, false, f.app.ID)
			if listErr != nil || len(after) != len(before) {
				t.Fatalf("rejected resize changed operation count: before=%d after=%d err=%v", len(before), len(after), listErr)
			}
		})
	}
}

func TestManagedPostgresResizeRestoreConflictRecognizesRunningOnly(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	p := seedResizeRestorePlan(t, f)
	run, err := f.store.CreateBackupRestoreRun(model.BackupRestoreRun{PlanID: p.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.ExecContext(t.Context(), `UPDATE fugue_backup_restore_runs SET status=$2,finished_at=now() WHERE id=$1`, run.ID, model.BackupRestoreStatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
		t.Fatalf("finished restore incorrectly blocked resize: %v", err)
	}
}

func TestManagedPostgresResizePreservesUnlimitedCPU(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	mutatePostgresResizeService(t, f, func(s *model.BackingService) { s.Spec.Postgres.Resources.CPULimitMilliCores = 0 })
	target := managedPostgresResizeTarget()
	target.CPULimitMilliCores = 0
	op, result, err := f.store.CreateOperationWithResult(managedPostgresResizeOperation(f, target))
	if err != nil || !result.Created || op.DesiredSpec.Postgres.RuntimeResources.CPULimitMilliCores != 0 {
		t.Fatalf("unlimited CPU preservation rejected: op=%+v result=%+v err=%v", op, result, err)
	}
}
