package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

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
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
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

	reopened := New(f.path)
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
		mutate func(*managedPostgresResizeFixture, *model.Operation)
		want   error
	}{
		{
			name: "missing exact service",
			mutate: func(_ *managedPostgresResizeFixture, op *model.Operation) {
				op.ServiceID = ""
			},
			want: ErrInvalidInput,
		},
		{
			name: "missing cpu limit",
			mutate: func(_ *managedPostgresResizeFixture, op *model.Operation) {
				op.DesiredSpec.Postgres.RuntimeResources.CPULimitMilliCores = 0
			},
			want: ErrInvalidInput,
		},
		{
			name: "request exceeds limit",
			mutate: func(_ *managedPostgresResizeFixture, op *model.Operation) {
				op.DesiredSpec.Postgres.RuntimeResources.CPUMilliCores = 600
			},
			want: ErrInvalidInput,
		},
		{
			name: "suspended database",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.BackingServices[findBackingService(state, f.service.ID)].Spec.Postgres.Suspended = true
				})
			},
			want: ErrConflict,
		},
		{
			name: "shared database",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.BackingServices[findBackingService(state, f.service.ID)].OwnerAppID = ""
				})
			},
			want: ErrConflict,
		},
		{
			name: "multiple bindings",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.ServiceBindings = append(state.ServiceBindings, model.ServiceBinding{
						ID: "binding-conflict", TenantID: f.tenant.ID, AppID: "app-other", ServiceID: f.service.ID,
					})
				})
			},
			want: ErrConflict,
		},
		{
			name: "active backup",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.BackupRuns = append(state.BackupRuns, model.BackupRun{
						ID: "backup-active", AppID: f.app.ID, Status: model.BackupRunStatusRunning,
						Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: f.app.ID, ServiceName: f.service.Name},
					})
				})
			},
			want: ErrConflict,
		},
		{
			name: "active import",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.AppDatabaseImportJobs = append(state.AppDatabaseImportJobs, model.AppDatabaseImportJob{
						ID: "import-active", AppID: f.app.ID, Status: model.OperationStatusPending,
					})
				})
			},
			want: ErrConflict,
		},
		{
			name: "active restore",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.BackupRestoreRuns = append(state.BackupRestoreRuns, model.BackupRestoreRun{
						ID: "restore-active", AppID: f.app.ID, Status: model.BackupRestoreStatusPlanned,
					})
				})
			},
			want: ErrConflict,
		},
		{
			name: "active app operation",
			mutate: func(f *managedPostgresResizeFixture, _ *model.Operation) {
				mutateResizeFixtureState(t, f.store, func(state *model.State) {
					state.Operations = append(state.Operations, model.Operation{
						ID: "op-active", TenantID: f.tenant.ID, AppID: f.app.ID,
						Type: model.OperationTypeDeploy, Status: model.OperationStatusRunning,
					})
				})
			},
			want: ErrConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newManagedPostgresResizeFixture(t)
			op := managedPostgresResizeOperation(f, managedPostgresResizeTarget())
			test.mutate(&f, &op)
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

func TestManagedPostgresResizeUpscaleHonorsBillingCap(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	if _, err := f.store.UpdateTenantBilling(f.tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    f.bootstrap.CPUMilliCores,
		MemoryMebibytes:  f.bootstrap.MemoryMebibytes,
		StorageGibibytes: 1,
	}); err != nil {
		t.Fatalf("lower resize billing cap to current commitment: %v", err)
	}
	if err := f.store.withLockedState(false, func(state *model.State) error {
		app := state.Apps[findApp(state, f.app.ID)]
		hydrateAppBackingServices(state, &app)
		op := managedPostgresResizeOperation(f, managedPostgresResizeTarget())
		if err := prepareManagedPostgresResizeOperation(app, &op); err != nil {
			return err
		}
		billing := state.TenantBilling[findTenantBillingRecord(state, f.tenant.ID)]
		current, next, err := projectedTenantManagedTotalsWithBilling(state, app, op, billing)
		if current.CPUMilliCores != f.bootstrap.CPUMilliCores || current.MemoryMebibytes != f.bootstrap.MemoryMebibytes ||
			next.CPUMilliCores != managedPostgresResizeTarget().CPUMilliCores || next.MemoryMebibytes != managedPostgresResizeTarget().MemoryMebibytes {
			t.Fatalf("resize billing projection did not use runtime target: current=%+v next=%+v", current, next)
		}
		return err
	}); err != nil {
		t.Fatalf("inspect resize billing projection: %v", err)
	}
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected resize upscale to honor billing cap, got %v", err)
	}
}

func mutateResizeFixtureState(t *testing.T, s *Store, mutate func(*model.State)) {
	t.Helper()
	if err := s.withLockedState(true, func(state *model.State) error {
		mutate(state)
		return nil
	}); err != nil {
		t.Fatalf("mutate resize fixture: %v", err)
	}
}

func TestManagedPostgresResizeRestoreConflictRecognizesRunningOnly(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresResizeFixture(t)
	mutateResizeFixtureState(t, f.store, func(state *model.State) {
		state.BackupRestoreRuns = []model.BackupRestoreRun{{
			ID: "restore-finished", AppID: f.app.ID, Status: model.BackupRestoreStatusSucceeded,
			FinishedAt: func() *time.Time { value := time.Now().UTC(); return &value }(),
		}}
	})
	if _, err := f.store.CreateOperation(managedPostgresResizeOperation(f, managedPostgresResizeTarget())); err != nil {
		t.Fatalf("finished restore incorrectly blocked resize: %v", err)
	}
}
