package store

import (
	"errors"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

type managedPostgresPlacementFixture struct {
	store         *Store
	tenant        model.Tenant
	project       model.Project
	app           model.App
	service       model.BackingService
	sourceRuntime model.Runtime
	targetRuntime model.Runtime
}

func newManagedPostgresPlacementFixture(t *testing.T, bound bool) managedPostgresPlacementFixture {
	t.Helper()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init placement store: %v", err)
	}
	tenant, err := s.CreateTenant("Postgres Placement")
	if err != nil {
		t.Fatalf("create placement tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "placement", "")
	if err != nil {
		t.Fatalf("create placement project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("raise placement billing cap: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "placement-source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create placement source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "placement-target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create placement target runtime: %v", err)
	}

	appSpec := model.AppSpec{
		Image: "ghcr.io/example/placement:1", Replicas: 1, RuntimeID: sourceRuntime.ID,
	}
	postgres := model.AppPostgresSpec{
		Database: "placement", User: "placement", Password: "secret",
		ServiceName: "placement-postgres", RuntimeID: sourceRuntime.ID,
		PrimaryNodeName: "node-source", Instances: 2, SynchronousReplicas: 1,
		StorageSize: "1Gi",
	}
	if !bound {
		appSpec.Postgres = model.CloneAppPostgresSpec(&postgres)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "placement-app", "", appSpec)
	if err != nil {
		t.Fatalf("create placement app: %v", err)
	}

	var service model.BackingService
	if bound {
		service, err = s.CreateBackingService(tenant.ID, project.ID, "placement-postgres", "", model.BackingServiceSpec{
			Postgres: model.CloneAppPostgresSpec(&postgres),
		})
		if err != nil {
			t.Fatalf("create bound placement service: %v", err)
		}
		if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "postgres", nil); err != nil {
			t.Fatalf("bind placement service: %v", err)
		}
		app, err = s.GetApp(app.ID)
		if err != nil {
			t.Fatalf("reload bound placement app: %v", err)
		}
	} else {
		if len(app.BackingServices) != 1 {
			t.Fatalf("expected one owned placement service, got %+v", app.BackingServices)
		}
		service = app.BackingServices[0]
	}

	return managedPostgresPlacementFixture{
		store: s, tenant: tenant, project: project, app: app, service: service,
		sourceRuntime: sourceRuntime, targetRuntime: targetRuntime,
	}
}

func managedPostgresPlacementMutationFor(
	f managedPostgresPlacementFixture,
	expected, desired model.AppPostgresSpec,
	runtimeID, nodeName string,
) ManagedPostgresPlacementMutation {
	return ManagedPostgresPlacementMutation{
		Witness: ManagedPostgresPlacementWitness{
			AppID: f.app.ID, TenantID: f.tenant.ID, ProjectID: f.project.ID,
			ServiceID: f.service.ID, ServiceName: expected.ServiceName,
			RuntimeID: runtimeID, NodeName: nodeName,
			PrimaryPod: expected.ServiceName + "-2", PodIP: "10.0.0.22",
		},
		Expected: ManagedPostgresPlacementStateFromSpec(expected),
		Desired:  ManagedPostgresPlacementStateFromSpec(desired),
	}
}

func TestSyncObservedManagedPostgresPlacementCorrectsBoundSameRuntimeNode(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, true)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node-source-current"
	mutation := managedPostgresPlacementMutationFor(f, current, desired, current.RuntimeID, desired.PrimaryNodeName)

	updated, err := f.store.SyncObservedManagedPostgresPlacement(mutation)
	if err != nil {
		t.Fatalf("sync bound placement: %v", err)
	}
	if updated.Spec.Postgres != nil {
		t.Fatalf("bound placement leaked postgres into app spec: %+v", updated.Spec.Postgres)
	}
	stored, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatalf("get corrected bound service: %v", err)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != desired.PrimaryNodeName {
		t.Fatalf("bound primary node = %q, want %q", got, desired.PrimaryNodeName)
	}
	if got := stored.Spec.Postgres.RuntimeID; got != current.RuntimeID {
		t.Fatalf("same-runtime correction changed runtime to %q", got)
	}
}

func TestSyncObservedManagedPostgresPlacementRejectsEveryActiveAppOperation(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, false)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node-source-current"
	one := 1
	if _, err := f.store.CreateOperation(model.Operation{
		TenantID: f.tenant.ID, Type: model.OperationTypeScale, AppID: f.app.ID, DesiredReplicas: &one,
	}); err != nil {
		t.Fatalf("create active app operation: %v", err)
	}

	_, err := f.store.SyncObservedManagedPostgresPlacement(
		managedPostgresPlacementMutationFor(f, current, desired, current.RuntimeID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("placement sync during active operation error = %v, want conflict", err)
	}
	stored, getErr := f.store.GetBackingService(f.service.ID)
	if getErr != nil {
		t.Fatalf("get service after rejected placement sync: %v", getErr)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != current.PrimaryNodeName {
		t.Fatalf("rejected placement sync changed node to %q", got)
	}
}

func TestSyncObservedManagedPostgresPlacementRejectsCrossRuntimeNodeSnapshot(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, false)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node-target"

	_, err := f.store.SyncObservedManagedPostgresPlacement(
		managedPostgresPlacementMutationFor(f, current, desired, f.targetRuntime.ID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-runtime node snapshot error = %v, want invalid input", err)
	}
	stored, getErr := f.store.GetBackingService(f.service.ID)
	if getErr != nil {
		t.Fatalf("get service after rejected cross-runtime placement: %v", getErr)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != current.PrimaryNodeName {
		t.Fatalf("cross-runtime rejection changed node to %q", got)
	}
}

func TestSyncObservedManagedPostgresPlacementRejectsFailoverConsumptionWhileSourceActive(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, false)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	current.FailoverTargetRuntimeID = f.targetRuntime.ID
	current.Instances = 2
	current.SynchronousReplicas = 1
	updatedService, err := f.store.UpdateBackingServiceSpec(f.service.ID, model.BackingServiceSpec{Postgres: &current})
	if err != nil {
		t.Fatalf("configure placement failover target: %v", err)
	}
	f.service = updatedService
	f.app, err = f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatalf("reload placement failover app: %v", err)
	}
	desired := *model.CloneAppPostgresSpec(&current)
	desired.RuntimeID = f.targetRuntime.ID
	desired.FailoverTargetRuntimeID = ""
	desired.PrimaryNodeName = "node-target"
	desired.Instances = 1
	desired.SynchronousReplicas = 0
	desired.PrimaryPlacementPendingRebalance = false

	_, err = f.store.SyncObservedManagedPostgresPlacement(
		managedPostgresPlacementMutationFor(f, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("failover consumption with active source error = %v, want conflict", err)
	}
	stored, getErr := f.store.GetBackingService(f.service.ID)
	if getErr != nil {
		t.Fatalf("get service after rejected failover consumption: %v", getErr)
	}
	if got := stored.Spec.Postgres.RuntimeID; got != current.RuntimeID {
		t.Fatalf("rejected failover consumption changed runtime to %q", got)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != current.PrimaryNodeName {
		t.Fatalf("rejected failover consumption changed node to %q", got)
	}
}

func TestSyncObservedManagedPostgresPlacementRequiresExactStandaloneBinding(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, true)
	consumer, err := f.store.CreateApp(f.tenant.ID, f.project.ID, "placement-consumer", "", model.AppSpec{
		Image: "ghcr.io/example/consumer:1", Replicas: 1, RuntimeID: f.sourceRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create placement consumer: %v", err)
	}
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.ServiceBindings = append(state.ServiceBindings, model.ServiceBinding{
			ID: model.NewID("binding"), TenantID: f.tenant.ID, AppID: consumer.ID, ServiceID: f.service.ID,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed non-standalone binding: %v", err)
	}
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node-current"

	_, err = f.store.SyncObservedManagedPostgresPlacement(
		managedPostgresPlacementMutationFor(f, current, desired, current.RuntimeID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("non-standalone placement error = %v, want conflict", err)
	}
}

func TestSyncObservedManagedPostgresPlacementRejectsMismatchedIdentity(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, false)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node-current"
	mutation := managedPostgresPlacementMutationFor(f, current, desired, current.RuntimeID, desired.PrimaryNodeName)
	mutation.Witness.ProjectID = "project_other"

	_, err := f.store.SyncObservedManagedPostgresPlacement(mutation)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("mismatched placement identity error = %v, want not found", err)
	}
	stored, getErr := f.store.GetBackingService(f.service.ID)
	if getErr != nil {
		t.Fatalf("get service after rejected identity: %v", getErr)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != current.PrimaryNodeName {
		t.Fatalf("mismatched identity changed node to %q", got)
	}
}

func TestCompleteManagedPostgresSwitchoverWithPlacementAtomicallyPersistsTarget(t *testing.T) {
	t.Parallel()
	for _, bound := range []bool{false, true} {
		bound := bound
		name := "owned"
		if bound {
			name = "bound"
		}
		t.Run(name, func(t *testing.T) {
			f := newManagedPostgresPlacementFixture(t, bound)
			operationServiceID := f.service.ID
			if !bound {
				// The app-scoped API does not send a backing-service ID; the store
				// must still resolve and lock the exact app-owned service.
				operationServiceID = ""
			}
			created, err := f.store.CreateOperation(model.Operation{
				TenantID: f.tenant.ID, Type: model.OperationTypeDatabaseSwitchover,
				AppID: f.app.ID, ServiceID: operationServiceID, TargetRuntimeID: f.targetRuntime.ID,
			})
			if err != nil {
				t.Fatalf("create switchover operation: %v", err)
			}
			current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
			desired := *model.CloneAppPostgresSpec(&current)
			desired.RuntimeID = f.targetRuntime.ID
			desired.FailoverTargetRuntimeID = f.sourceRuntime.ID
			desired.PrimaryNodeName = "node-target"
			desired.Instances = 2
			desired.SynchronousReplicas = 1
			mutation := managedPostgresPlacementMutationFor(f, current, desired, desired.RuntimeID, desired.PrimaryNodeName)

			completed, err := f.store.CompleteManagedPostgresSwitchoverWithPlacement(
				created.ID, "/tmp/placement.yaml", "switchover complete", mutation,
			)
			if err != nil {
				t.Fatalf("complete switchover with placement: %v", err)
			}
			if completed.Status != model.OperationStatusCompleted || completed.CompletedAt == nil {
				t.Fatalf("operation not completed atomically: %+v", completed)
			}
			stored, err := f.store.GetBackingService(f.service.ID)
			if err != nil {
				t.Fatalf("get switched service: %v", err)
			}
			if got := stored.Spec.Postgres.RuntimeID; got != desired.RuntimeID {
				t.Fatalf("stored runtime = %q, want %q", got, desired.RuntimeID)
			}
			if got := stored.Spec.Postgres.FailoverTargetRuntimeID; got != desired.FailoverTargetRuntimeID {
				t.Fatalf("stored failover runtime = %q, want %q", got, desired.FailoverTargetRuntimeID)
			}
			if got := stored.Spec.Postgres.PrimaryNodeName; got != desired.PrimaryNodeName {
				t.Fatalf("stored primary node = %q, want %q", got, desired.PrimaryNodeName)
			}
		})
	}
}

func TestCompleteManagedPostgresSwitchoverWithPlacementRejectsCompetingOperationWithoutPartialWrite(t *testing.T) {
	t.Parallel()
	f := newManagedPostgresPlacementFixture(t, true)
	created, err := f.store.CreateOperation(model.Operation{
		TenantID: f.tenant.ID, Type: model.OperationTypeDatabaseSwitchover,
		AppID: f.app.ID, ServiceID: f.service.ID, TargetRuntimeID: f.targetRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create switchover operation: %v", err)
	}
	if err := f.store.withLockedState(true, func(state *model.State) error {
		state.Operations = append(state.Operations, model.Operation{
			ID: model.NewID("operation"), TenantID: f.tenant.ID, AppID: f.app.ID,
			Type: model.OperationTypeScale, Status: model.OperationStatusRunning,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed competing operation: %v", err)
	}
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	desired := *model.CloneAppPostgresSpec(&current)
	desired.RuntimeID = f.targetRuntime.ID
	desired.FailoverTargetRuntimeID = f.sourceRuntime.ID
	desired.PrimaryNodeName = "node-target"

	_, err = f.store.CompleteManagedPostgresSwitchoverWithPlacement(
		created.ID, "/tmp/placement.yaml", "switchover complete",
		managedPostgresPlacementMutationFor(f, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("completion with competing operation error = %v, want conflict", err)
	}
	active, getErr := f.store.GetOperation(created.ID)
	if getErr != nil {
		t.Fatalf("get rejected switchover operation: %v", getErr)
	}
	if active.Status == model.OperationStatusCompleted {
		t.Fatalf("rejected switchover was completed: %+v", active)
	}
	stored, getErr := f.store.GetBackingService(f.service.ID)
	if getErr != nil {
		t.Fatalf("get service after rejected switchover: %v", getErr)
	}
	if got := stored.Spec.Postgres.RuntimeID; got != current.RuntimeID {
		t.Fatalf("rejected switchover partially changed runtime to %q", got)
	}
	if got := stored.Spec.Postgres.PrimaryNodeName; got != current.PrimaryNodeName {
		t.Fatalf("rejected switchover partially changed node to %q", got)
	}
}
