package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestManagedPostgresSuspendResumePersistsOnlyAfterCompletion(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Postgres Lifecycle")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:    "demo",
			User:        "demo",
			Password:    "secret",
			StorageSize: "1Gi",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   750,
				MemoryMebibytes: 1024,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	serviceID := app.BackingServices[0].ID

	zero := 0
	scale, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create disable operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scale.ID, "", "disabled"); err != nil {
		t.Fatalf("complete disable operation: %v", err)
	}
	baseline, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get disabled app: %v", err)
	}

	suspend, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: serviceID,
	})
	if err != nil {
		t.Fatalf("create suspend operation: %v", err)
	}
	if suspend.DesiredSpec == nil || suspend.DesiredSpec.Postgres == nil || !suspend.DesiredSpec.Postgres.Suspended {
		t.Fatalf("expected suspended desired postgres spec, got %+v", suspend.DesiredSpec)
	}
	if suspend.ServiceID != serviceID || suspend.TargetRuntimeID != model.DefaultManagedRuntimeID {
		t.Fatalf("unexpected lifecycle target: %+v", suspend)
	}
	assertPostgresSuspended(t, s, serviceID, false)
	queuedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app with queued suspend: %v", err)
	}
	if queuedApp.Status.Phase != baseline.Status.Phase || queuedApp.Status.LastOperationID != baseline.Status.LastOperationID {
		t.Fatalf("database lifecycle must preserve app status while pending: before=%+v after=%+v", baseline.Status, queuedApp.Status)
	}
	if _, err := s.FailOperation(suspend.ID, "runtime did not confirm hibernation"); err != nil {
		t.Fatalf("fail suspend operation: %v", err)
	}
	assertPostgresSuspended(t, s, serviceID, false)

	suspend, err = s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: serviceID,
	})
	if err != nil {
		t.Fatalf("retry suspend operation: %v", err)
	}
	maliciousCompletion := baseline.Spec
	maliciousCompletion.Image = "ghcr.io/attacker/replaced:latest"
	maliciousCompletion.Replicas = 9
	maliciousCompletion.RuntimeID = "runtime_attacker"
	maliciousCompletion.Env = map[string]string{"MUTATED": "true"}
	if _, err := s.CompleteManagedOperationWithResult(suspend.ID, "", "database suspended", &maliciousCompletion, &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/image"}); err != nil {
		t.Fatalf("complete suspend operation: %v", err)
	}
	assertPostgresSuspended(t, s, serviceID, true)
	afterSuspend, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after suspend: %v", err)
	}
	if !reflect.DeepEqual(afterSuspend.Spec, baseline.Spec) {
		t.Fatalf("lifecycle completion changed app spec: before=%+v after=%+v", baseline.Spec, afterSuspend.Spec)
	}
	if afterSuspend.Source != nil || afterSuspend.BuildSource != nil || afterSuspend.OriginSource != nil {
		t.Fatalf("lifecycle completion changed app source state: %+v", afterSuspend.Source)
	}

	commitment, err := s.GetTenantResourceCommitment(tenant.ID)
	if err != nil {
		t.Fatalf("get tenant commitment: %v", err)
	}
	if commitment.CPUMilliCores != 0 || commitment.MemoryMebibytes != 0 {
		t.Fatalf("suspended database must not reserve compute, got %+v", commitment)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{StorageGibibytes: 1}); err != nil {
		t.Fatalf("lower cap to retained storage: %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseResume,
		AppID:     app.ID,
		ServiceID: serviceID,
	}); !errors.Is(err, ErrBillingCapExceeded) {
		t.Fatalf("expected resume projection to enforce compute cap, got %v", err)
	}
	assertPostgresSuspended(t, s, serviceID, true)

	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    1000,
		MemoryMebibytes:  2048,
		StorageGibibytes: 1,
	}); err != nil {
		t.Fatalf("raise cap for resume: %v", err)
	}
	resume, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseResume,
		AppID:     app.ID,
		ServiceID: serviceID,
	})
	if err != nil {
		t.Fatalf("create resume operation: %v", err)
	}
	assertPostgresSuspended(t, s, serviceID, true)
	if _, err := s.CompleteManagedOperation(resume.ID, "", "database resumed"); err != nil {
		t.Fatalf("complete resume operation: %v", err)
	}
	assertPostgresSuspended(t, s, serviceID, false)
}

func TestManagedPostgresLifecycleCreateReusesOnlyExactActiveOperation(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Lifecycle Retry")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 50,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "retry-demo", "", model.AppSpec{
		Image:     "ghcr.io/example/retry:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database: "retry_demo",
			User:     "retry_demo",
			Password: "retained-password-must-not-leak-on-conflict",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   500,
				MemoryMebibytes: 512,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	serviceID := app.BackingServices[0].ID
	zero := 0
	scaleDown, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create scale down: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scaleDown.ID, "", "disabled"); err != nil {
		t.Fatalf("complete scale down: %v", err)
	}
	candidate := model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: serviceID,
	}

	created, result, err := s.CreateOperationWithResult(candidate)
	if err != nil {
		t.Fatalf("create lifecycle operation: %v", err)
	}
	if !result.Created || created.ID == "" {
		t.Fatalf("first request must create an operation: op=%+v result=%+v", created, result)
	}
	lookedUp, found, err := s.GetActiveManagedPostgresLifecycleOperation(
		tenant.ID, app.ID, serviceID, model.OperationTypeDatabaseSuspend,
	)
	if err != nil || !found || lookedUp.ID != created.ID {
		t.Fatalf("lookup exact active lifecycle operation: op=%+v found=%t err=%v", lookedUp, found, err)
	}
	if leaked, found, err := s.GetActiveManagedPostgresLifecycleOperation(
		tenant.ID, app.ID, serviceID, model.OperationTypeDatabaseResume,
	); !errors.Is(err, ErrConflict) || found || leaked.ID != "" || leaked.DesiredSpec != nil {
		t.Fatalf("opposite lookup must conflict without leaking operation: op=%+v found=%t err=%v", leaked, found, err)
	}

	reused, result, err := s.CreateOperationWithResult(candidate)
	if err != nil {
		t.Fatalf("retry exact lifecycle request: %v", err)
	}
	if result.Created || reused.ID != created.ID {
		t.Fatalf("exact retry must reuse the original operation: first=%s retry=%s result=%+v", created.ID, reused.ID, result)
	}
	legacy, err := s.CreateOperation(candidate)
	if err != nil || legacy.ID != created.ID {
		t.Fatalf("legacy create wrapper must share lifecycle idempotency: op=%+v err=%v", legacy, err)
	}
	operations, err := s.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list lifecycle operations: %v", err)
	}
	lifecycleCount := 0
	for _, operation := range operations {
		if operation.Type == model.OperationTypeDatabaseSuspend && operation.ServiceID == serviceID {
			lifecycleCount++
		}
	}
	if lifecycleCount != 1 {
		t.Fatalf("retries must not append lifecycle operations, got %d lifecycle operations in %+v", lifecycleCount, operations)
	}

	assertRejectedWithoutOperationLeak := func(name string, request model.Operation, want error) {
		t.Helper()
		returned, result, err := s.CreateOperationWithResult(request)
		if !errors.Is(err, want) {
			t.Fatalf("%s: expected %v, got %v", name, want, err)
		}
		if returned.ID != "" || returned.DesiredSpec != nil || result.Created {
			t.Fatalf("%s: conflicting request leaked existing operation: op=%+v result=%+v", name, returned, result)
		}
	}

	opposite := candidate
	opposite.Type = model.OperationTypeDatabaseResume
	assertRejectedWithoutOperationLeak("opposite direction", opposite, ErrConflict)

	wrongTenant := candidate
	wrongTenant.TenantID = "tenant_other"
	assertRejectedWithoutOperationLeak("wrong tenant", wrongTenant, ErrNotFound)

	mutateOperation := func(mutate func(*model.Operation)) {
		t.Helper()
		if err := s.withLockedState(true, func(state *model.State) error {
			index := findOperation(state, created.ID)
			if index < 0 {
				return ErrNotFound
			}
			mutate(&state.Operations[index])
			return nil
		}); err != nil {
			t.Fatalf("mutate lifecycle fixture: %v", err)
		}
	}

	mutateOperation(func(op *model.Operation) { op.TenantID = "tenant_corrupt" })
	assertRejectedWithoutOperationLeak("existing tenant mismatch", candidate, ErrConflict)
	mutateOperation(func(op *model.Operation) { op.TenantID = tenant.ID })

	mutateOperation(func(op *model.Operation) { op.ServiceID = "service_other" })
	assertRejectedWithoutOperationLeak("existing service mismatch", candidate, ErrConflict)
	mutateOperation(func(op *model.Operation) { op.ServiceID = serviceID })

	mutateOperation(func(op *model.Operation) { op.AppID = "app_other" })
	assertRejectedWithoutOperationLeak("existing app mismatch", candidate, ErrConflict)
	mutateOperation(func(op *model.Operation) { op.AppID = app.ID })

	mutateOperation(func(op *model.Operation) { op.DesiredSpec.Postgres.Suspended = false })
	assertRejectedWithoutOperationLeak("corrupt desired direction", candidate, ErrConflict)
	mutateOperation(func(op *model.Operation) { op.DesiredSpec.Postgres.Suspended = true })

	mutateOperation(func(op *model.Operation) {
		op.DesiredSpec.Postgres.RuntimeID = "runtime_corrupt"
		op.SourceRuntimeID = "runtime_corrupt"
		op.TargetRuntimeID = "runtime_corrupt"
	})
	assertRejectedWithoutOperationLeak("corrupt lifecycle runtime", candidate, ErrConflict)
	mutateOperation(func(op *model.Operation) {
		op.DesiredSpec.Postgres.RuntimeID = model.DefaultManagedRuntimeID
		op.SourceRuntimeID = model.DefaultManagedRuntimeID
		op.TargetRuntimeID = model.DefaultManagedRuntimeID
	})

	if err := s.withLockedState(true, func(state *model.State) error {
		serviceIndex := findBackingService(state, serviceID)
		if serviceIndex < 0 {
			return ErrNotFound
		}
		state.BackingServices[serviceIndex].ProjectID = "project_other"
		return nil
	}); err != nil {
		t.Fatalf("mutate service project: %v", err)
	}
	assertRejectedWithoutOperationLeak("target project mismatch", candidate, ErrInvalidInput)
	if err := s.withLockedState(true, func(state *model.State) error {
		serviceIndex := findBackingService(state, serviceID)
		state.BackingServices[serviceIndex].ProjectID = project.ID
		return nil
	}); err != nil {
		t.Fatalf("restore service project: %v", err)
	}

	if err := s.withLockedState(true, func(state *model.State) error {
		index := findOperation(state, created.ID)
		duplicate := cloneOperation(state.Operations[index])
		duplicate.ID = "op_duplicate_active"
		state.Operations = append(state.Operations, duplicate)
		return nil
	}); err != nil {
		t.Fatalf("seed duplicate active operation: %v", err)
	}
	assertRejectedWithoutOperationLeak("multiple active operations", candidate, ErrConflict)
}

func TestManagedPostgresLifecycleBillingProjectionCountsBackingServiceOnce(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Lifecycle Billing Projection")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    2000,
		MemoryMebibytes:  4096,
		StorageGibibytes: 10,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "billing-demo", "", model.AppSpec{
		Image:     "ghcr.io/example/billing:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:    "billing_demo",
			User:        "billing_demo",
			Password:    "retained-secret",
			StorageSize: "1Gi",
			Resources: &model.ResourceSpec{
				CPUMilliCores:   500,
				MemoryMebibytes: 512,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	serviceID := app.BackingServices[0].ID
	zero := 0
	scaleDown, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create scale down: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scaleDown.ID, "", "disabled"); err != nil {
		t.Fatalf("complete scale down: %v", err)
	}

	assertProjection := func(operationType string, wantCurrent, wantNext model.BillingResourceSpec) {
		t.Helper()
		if err := s.withLockedState(false, func(state *model.State) error {
			appIndex := findApp(state, app.ID)
			if appIndex < 0 {
				return ErrNotFound
			}
			projectedApp := state.Apps[appIndex]
			hydrateAppBackingServices(state, &projectedApp)
			op := model.Operation{
				TenantID:  tenant.ID,
				Type:      operationType,
				AppID:     app.ID,
				ServiceID: serviceID,
			}
			if err := prepareManagedPostgresLifecycleOperation(
				projectedApp, &op, operationType == model.OperationTypeDatabaseSuspend,
			); err != nil {
				return err
			}
			billingIndex := findTenantBillingRecord(state, tenant.ID)
			if billingIndex < 0 {
				return ErrNotFound
			}
			current, next, err := projectedTenantManagedTotalsWithBilling(state, projectedApp, op, state.TenantBilling[billingIndex])
			if err != nil {
				return err
			}
			if current != wantCurrent || next != wantNext {
				t.Fatalf("%s projection counted lifecycle resources incorrectly: current=%+v next=%+v", operationType, current, next)
			}
			currentPublic, nextPublic, err := projectedTenantPublicRuntimeHourlyRates(state, projectedApp, op)
			if err != nil {
				return err
			}
			if currentPublic != nextPublic {
				t.Fatalf("%s unexpectedly changed public runtime rate: current=%d next=%d", operationType, currentPublic, nextPublic)
			}
			return nil
		}); err != nil {
			t.Fatalf("project %s billing: %v", operationType, err)
		}
	}

	activeResources := model.BillingResourceSpec{CPUMilliCores: 500, MemoryMebibytes: 512, StorageGibibytes: 1}
	suspendedResources := model.BillingResourceSpec{StorageGibibytes: 1}
	assertProjection(model.OperationTypeDatabaseSuspend, activeResources, suspendedResources)

	if err := s.withLockedState(true, func(state *model.State) error {
		billingIndex := findTenantBillingRecord(state, tenant.ID)
		state.TenantBilling[billingIndex].ManagedCap = suspendedResources
		state.TenantBilling[billingIndex].BalanceMicroCents = 0
		return nil
	}); err != nil {
		t.Fatalf("lower cap for capacity-reducing suspend: %v", err)
	}
	suspend, result, err := s.CreateOperationWithResult(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: serviceID,
	})
	if err != nil || !result.Created {
		t.Fatalf("capacity-reducing suspend must not be blocked by low cap/balance: op=%+v result=%+v err=%v", suspend, result, err)
	}
	if _, err := s.CompleteManagedOperation(suspend.ID, "", "database suspended"); err != nil {
		t.Fatalf("complete suspend: %v", err)
	}
	assertProjection(model.OperationTypeDatabaseResume, suspendedResources, activeResources)

	if err := s.withLockedState(true, func(state *model.State) error {
		billingIndex := findTenantBillingRecord(state, tenant.ID)
		state.TenantBilling[billingIndex].ManagedCap = activeResources
		state.TenantBilling[billingIndex].BalanceMicroCents = 1_000_000_000
		return nil
	}); err != nil {
		t.Fatalf("set exact resume cap: %v", err)
	}
	resume, result, err := s.CreateOperationWithResult(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseResume,
		AppID:     app.ID,
		ServiceID: serviceID,
	})
	if err != nil || !result.Created {
		t.Fatalf("exact single-database cap must allow resume: op=%+v result=%+v err=%v", resume, result, err)
	}
}

func TestManagedPostgresLifecycleClaimAlwaysUsesManagedController(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operationType string
		claim         func(*Store, string) (model.Operation, bool, error)
	}{
		{
			name:          "try claim suspend",
			operationType: model.OperationTypeDatabaseSuspend,
			claim: func(s *Store, operationID string) (model.Operation, bool, error) {
				return s.TryClaimPendingOperation(operationID)
			},
		},
		{
			name:          "claim next resume",
			operationType: model.OperationTypeDatabaseResume,
			claim: func(s *Store, _ string) (model.Operation, bool, error) {
				return s.ClaimNextPendingOperation()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "store.json"))
			if err := s.Init(); err != nil {
				t.Fatalf("init store: %v", err)
			}
			if err := s.withLockedState(true, func(state *model.State) error {
				state.Apps = append(state.Apps, model.App{
					ID:        "app_demo",
					TenantID:  "tenant_demo",
					ProjectID: "project_demo",
					Spec: model.AppSpec{
						Image:     "ghcr.io/example/demo:latest",
						Replicas:  0,
						RuntimeID: "runtime_external",
					},
					Status: model.AppStatus{Phase: "disabled"},
				})
				state.Runtimes = append(state.Runtimes, model.Runtime{
					ID:   "runtime_external",
					Type: model.RuntimeTypeExternalOwned,
				})
				state.Operations = append(state.Operations, model.Operation{
					ID:              "op_lifecycle",
					TenantID:        "tenant_demo",
					AppID:           "app_demo",
					Type:            test.operationType,
					Status:          model.OperationStatusPending,
					TargetRuntimeID: "runtime_external",
				})
				return nil
			}); err != nil {
				t.Fatalf("seed lifecycle operation: %v", err)
			}
			claimed, found, err := test.claim(s, "op_lifecycle")
			if err != nil {
				t.Fatalf("claim lifecycle operation: %v", err)
			}
			if !found {
				t.Fatal("expected lifecycle operation to be claimed")
			}
			if claimed.ExecutionMode != model.ExecutionModeManaged || claimed.Status != model.OperationStatusRunning || claimed.AssignedRuntimeID != "" {
				t.Fatalf("lifecycle operation escaped managed controller: %+v", claimed)
			}
		})
	}
}

func TestManagedPostgresLifecyclePersistsStandaloneBoundServiceByID(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Bound Postgres Lifecycle")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 50,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "shared-postgres", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database:    "shared",
			User:        "shared",
			Password:    "preserved-secret",
			RuntimeID:   model.DefaultManagedRuntimeID,
			StorageSize: "1Gi",
		},
	})
	if err != nil {
		t.Fatalf("create standalone postgres: %v", err)
	}
	if service.OwnerAppID != "" {
		t.Fatalf("expected standalone service without owner, got %q", service.OwnerAppID)
	}
	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "postgres", nil); err != nil {
		t.Fatalf("bind standalone postgres: %v", err)
	}
	zero := 0
	scaleDown, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create standalone app scale down: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scaleDown.ID, "", "disabled"); err != nil {
		t.Fatalf("complete standalone app scale down: %v", err)
	}

	suspend, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	})
	if err != nil {
		t.Fatalf("create bound-service suspend: %v", err)
	}
	if _, err := s.CompleteManagedOperation(suspend.ID, "", "database suspended"); err != nil {
		t.Fatalf("complete bound-service suspend: %v", err)
	}
	assertPostgresSuspended(t, s, service.ID, true)

	resume, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseResume,
		AppID:     app.ID,
		ServiceID: service.ID,
	})
	if err != nil {
		t.Fatalf("create bound-service resume: %v", err)
	}
	if _, err := s.CompleteManagedOperation(resume.ID, "", "database resumed"); err != nil {
		t.Fatalf("complete bound-service resume: %v", err)
	}
	assertPostgresSuspended(t, s, service.ID, false)

	if err := s.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, service.ID)
		if index < 0 {
			return ErrNotFound
		}
		state.BackingServices[index].OwnerAppID = "app_other"
		return nil
	}); err != nil {
		t.Fatalf("prepare mismatched owner: %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected mismatched owner to be rejected, got %v", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, service.ID)
		if index < 0 {
			return ErrNotFound
		}
		state.BackingServices[index].OwnerAppID = ""
		state.ServiceBindings = append(state.ServiceBindings, model.ServiceBinding{
			ID:        "binding_conflict",
			TenantID:  tenant.ID,
			AppID:     "app_other",
			ServiceID: service.ID,
		})
		return nil
	}); err != nil {
		t.Fatalf("prepare legacy multi-binding conflict: %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected multi-binding lifecycle target to fail closed, got %v", err)
	}
}

func TestManagedPostgresSuspensionCannotBypassLifecycle(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Postgres Suspension Guards")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    5000,
		MemoryMebibytes:  10240,
		StorageGibibytes: 100,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	if _, err := s.CreateApp(tenant.ID, project.ID, "invalid-app", "", model.AppSpec{
		Image:     "ghcr.io/example/invalid:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:  "invalid",
			User:      "invalid",
			Password:  "secret",
			Suspended: true,
		},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected suspended app creation to be rejected, got %v", err)
	}
	if _, err := s.CreateBackingService(tenant.ID, project.ID, "invalid-service", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database:  "invalid",
			User:      "invalid",
			Password:  "secret",
			RuntimeID: model.DefaultManagedRuntimeID,
			Suspended: true,
		},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected suspended service creation to be rejected, got %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database:    "demo",
			User:        "demo",
			Password:    "preserved-secret",
			StorageSize: "1Gi",
		},
	})
	if err != nil {
		t.Fatalf("create guarded app: %v", err)
	}
	service := app.BackingServices[0]
	directSpec := cloneBackingServiceSpec(service.Spec)
	directSpec.Postgres.Suspended = true
	if _, err := s.UpdateBackingServiceSpec(service.ID, directSpec); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected direct service suspension to be rejected, got %v", err)
	}
	desired := app.Spec
	desired.Postgres = directSpec.Postgres
	if _, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &desired,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected deploy suspension transition to be rejected, got %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected direct suspend of live app to be rejected, got %v", err)
	}

	zero := 0
	scaleDown, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create scale down: %v", err)
	}
	if _, err := s.CompleteManagedOperation(scaleDown.ID, "", "disabled"); err != nil {
		t.Fatalf("complete scale down: %v", err)
	}
	suspend, err := s.CreateOperation(model.Operation{
		TenantID:  tenant.ID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     app.ID,
		ServiceID: service.ID,
	})
	if err != nil {
		t.Fatalf("create lifecycle suspend: %v", err)
	}
	if _, err := s.GetBackingService(service.ID); err != nil {
		t.Fatalf("service reads must remain available during lifecycle: %v", err)
	}
	runningDuringLifecycle, err := s.CreateApp(tenant.ID, project.ID, "running-during-lifecycle", "", model.AppSpec{
		Image:     "ghcr.io/example/running-during-lifecycle:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app for lifecycle bind guard: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, runningDuringLifecycle.ID, service.ID, "postgres", nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected bind during active lifecycle to be rejected, got %v", err)
	}
	if _, err := s.UpdateBackingServiceSpec(service.ID, service.Spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected service update during lifecycle to be rejected, got %v", err)
	}
	if _, err := s.UnbindBackingService(app.Bindings[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected unbind during lifecycle to be rejected, got %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected queued scale during lifecycle to be rejected, got %v", err)
	}
	if _, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected delete during lifecycle to be rejected, got %v", err)
	}
	if _, err := s.CompleteManagedOperation(suspend.ID, "", "suspended"); err != nil {
		t.Fatalf("complete lifecycle suspend: %v", err)
	}
	suspended, err := s.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get suspended service: %v", err)
	}
	directResume := cloneBackingServiceSpec(suspended.Spec)
	directResume.Postgres.Suspended = false
	if _, err := s.UpdateBackingServiceSpec(service.ID, directResume); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected direct service resume to be rejected, got %v", err)
	}
	one := 1
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &one,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected scale-up with suspended postgres to be rejected, got %v", err)
	}
	current, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get guarded app: %v", err)
	}
	deploy := current.Spec
	deploy.Replicas = 1
	if _, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploy,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected deploy with suspended postgres to be rejected, got %v", err)
	}
	importSpec := current.Spec
	importSpec.Replicas = 1
	if _, err := s.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeImport,
		AppID:         app.ID,
		DesiredSpec:   &importSpec,
		DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: importSpec.Image},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected import with suspended postgres to be rejected, got %v", err)
	}
	for _, operationType := range []string{
		model.OperationTypeMigrate,
		model.OperationTypeFailover,
		model.OperationTypeDatabaseSwitchover,
		model.OperationTypeDatabaseLocalize,
	} {
		if _, err := s.CreateOperation(model.Operation{
			TenantID:        tenant.ID,
			Type:            operationType,
			AppID:           app.ID,
			ServiceID:       service.ID,
			TargetRuntimeID: "runtime_other",
		}); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected %s with suspended postgres to be rejected, got %v", operationType, err)
		}
	}
	if _, err := s.UnbindBackingService(app.Bindings[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected suspended service recovery binding to be preserved, got %v", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		bindingIndex := findServiceBinding(state, app.Bindings[0].ID)
		if bindingIndex < 0 {
			return ErrNotFound
		}
		state.ServiceBindings = append(state.ServiceBindings[:bindingIndex], state.ServiceBindings[bindingIndex+1:]...)
		return nil
	}); err != nil {
		t.Fatalf("seed legacy unbound suspended service: %v", err)
	}
	runningApp, err := s.CreateApp(tenant.ID, project.ID, "running", "", model.AppSpec{
		Image:     "ghcr.io/example/running:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create running app for bind guard: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, runningApp.ID, service.ID, "postgres", nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected suspended service binding to running app to be rejected, got %v", err)
	}
	stoppedApp, err := s.CreateApp(tenant.ID, project.ID, "stopped", "", model.AppSpec{
		Image:     "ghcr.io/example/stopped:latest",
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create stopped app for bind guard: %v", err)
	}
	zero = 0
	stopOperation, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           stoppedApp.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create stopped app scale operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(stopOperation.ID, "", "disabled"); err != nil {
		t.Fatalf("complete stopped app scale operation: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, stoppedApp.ID, service.ID, "postgres", nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected suspended service binding to stopped app to be rejected, got %v", err)
	}
}

func TestAdoptOrphanManagedAppRestoresExactResourceIDsDisabledAndIdempotent(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Orphan Adoption")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    2000,
		MemoryMebibytes:  4096,
		StorageGibibytes: 50,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	original, err := s.CreateAppWithRoute(tenant.ID, project.ID, "orphan-demo", "retained data", model.AppSpec{
		Image:     "ghcr.io/example/orphan:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		Postgres: &model.AppPostgresSpec{
			Database: "orphan_demo",
			User:     "orphan_demo",
			Password: "preserved-secret",
		},
	}, model.AppRoute{Hostname: "orphan.example.com", PathPrefix: "/"})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	snapshot := cloneOrphanAdoptedApp(original)
	observed := time.Now().UTC()
	snapshot.BackingServices[0].RuntimeStatus = &model.BackingServiceRuntimeStatus{Phase: "active", ReadyInstances: 1, DesiredInstances: 1}
	snapshot.BackingServices[0].CurrentRuntimeStartedAt = &observed
	snapshot.BackingServices[0].CurrentRuntimeReadyAt = &observed
	if _, _, err := s.AdoptOrphanManagedApp(snapshot); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected active app takeover to be rejected, got %v", err)
	}

	deleteOp, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    original.ID,
	})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deleteOp.ID, "", "deleted"); err != nil {
		t.Fatalf("complete delete operation: %v", err)
	}
	if _, err := s.GetApp(original.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted app to be absent, got %v", err)
	}

	invisible := cloneOrphanAdoptedApp(snapshot)
	invisible.Spec.RuntimeID = "runtime_missing"
	invisible.BackingServices[0].Spec.Postgres.RuntimeID = "runtime_missing"
	if _, _, err := s.AdoptOrphanManagedApp(invisible); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected invisible runtime takeover to be rejected, got %v", err)
	}
	if _, err := s.GetBackingService(original.BackingServices[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed takeover must not partially restore service, got %v", err)
	}

	nameConflict, err := s.CreateBackingService(tenant.ID, project.ID, original.BackingServices[0].Name, "name conflict", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			Database:  "conflict",
			User:      "conflict",
			Password:  "conflict-secret",
			RuntimeID: model.DefaultManagedRuntimeID,
		},
	})
	if err != nil {
		t.Fatalf("create conflicting backing service: %v", err)
	}
	if _, _, err := s.AdoptOrphanManagedApp(snapshot); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected backing service conflict to reject takeover, got %v", err)
	}
	if _, err := s.GetApp(original.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting takeover must roll back tombstone restoration, got %v", err)
	}
	if _, err := s.GetBackingService(original.BackingServices[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting takeover must not partially restore service, got %v", err)
	}
	if _, err := s.DeleteBackingService(nameConflict.ID); err != nil {
		t.Fatalf("remove test name conflict: %v", err)
	}

	adopted, already, err := s.AdoptOrphanManagedApp(snapshot)
	if err != nil {
		t.Fatalf("adopt orphan: %v", err)
	}
	if already {
		t.Fatal("first adoption must not report already adopted")
	}
	if adopted.ID != original.ID || adopted.Name != original.Name || adopted.Spec.Replicas != 0 {
		t.Fatalf("unexpected adopted app identity/state: %+v", adopted)
	}
	if adopted.Route != nil || adopted.Status.Phase != "disabled" || adopted.Status.CurrentReplicas != 0 {
		t.Fatalf("adopted app must remain non-serving: route=%+v status=%+v", adopted.Route, adopted.Status)
	}
	if len(adopted.BackingServices) != 1 || adopted.BackingServices[0].ID != original.BackingServices[0].ID {
		t.Fatalf("backing service ID was not restored: %+v", adopted.BackingServices)
	}
	service := adopted.BackingServices[0]
	if service.OwnerAppID != original.ID || service.RuntimeStatus != nil || service.CurrentRuntimeStartedAt != nil || service.CurrentRuntimeReadyAt != nil {
		t.Fatalf("expected ownership restored and observed fields cleared: %+v", service)
	}
	if service.Spec.Postgres == nil || service.Spec.Postgres.Password != "preserved-secret" {
		t.Fatal("managed postgres credential was not preserved")
	}
	if len(adopted.Bindings) != 1 || adopted.Bindings[0].ID != original.Bindings[0].ID {
		t.Fatalf("binding ID was not restored: %+v", adopted.Bindings)
	}
	if adopted.Bindings[0].Env["DB_PASSWORD"] != "preserved-secret" {
		t.Fatal("binding was not rebuilt from the retained postgres credential")
	}

	repeated, already, err := s.AdoptOrphanManagedApp(snapshot)
	if err != nil {
		t.Fatalf("repeat orphan adoption: %v", err)
	}
	if !already || repeated.ID != adopted.ID {
		t.Fatalf("expected exact repeat to be idempotent, already=%v app=%+v", already, repeated)
	}
	verified, err := s.VerifyAdoptedOrphanManagedApp(snapshot)
	if err != nil {
		t.Fatalf("verify adopted orphan: %v", err)
	}
	if verified.ID != adopted.ID || len(verified.BackingServices) != 1 || len(verified.Bindings) != 1 {
		t.Fatalf("unexpected verified orphan state: %+v", verified)
	}

	conflicting := cloneOrphanAdoptedApp(snapshot)
	conflicting.BackingServices[0].Spec.Postgres.Password = "different-secret"
	if _, err := s.VerifyAdoptedOrphanManagedApp(conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflicting verification to fail without writes, got %v", err)
	}
	if _, _, err := s.AdoptOrphanManagedApp(conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflicting takeover to be rejected, got %v", err)
	}
}

func TestPrepareOrphanManagedAppAdoptionRejectsUnsafeSnapshots(t *testing.T) {
	t.Parallel()

	base := model.App{
		ID:        "app_orphan",
		TenantID:  "tenant_orphan",
		ProjectID: "project_orphan",
		Name:      "orphan",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/orphan:latest",
			Replicas:  0,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		BackingServices: []model.BackingService{{
			ID:          "service_orphan",
			TenantID:    "tenant_orphan",
			ProjectID:   "project_orphan",
			OwnerAppID:  "app_orphan",
			Name:        "orphan-postgres",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Status:      model.BackingServiceStatusActive,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
				Database:  "orphan",
				User:      "orphan",
				Password:  "preserved-secret",
				RuntimeID: model.DefaultManagedRuntimeID,
			}},
		}},
		Bindings: []model.ServiceBinding{{
			ID:        "binding_orphan",
			TenantID:  "tenant_orphan",
			AppID:     "app_orphan",
			ServiceID: "service_orphan",
			Alias:     "postgres",
		}},
	}

	tests := []struct {
		name   string
		mutate func(*model.App)
	}{
		{name: "service tenant mismatch", mutate: func(app *model.App) { app.BackingServices[0].TenantID = "tenant_other" }},
		{name: "service project mismatch", mutate: func(app *model.App) { app.BackingServices[0].ProjectID = "project_other" }},
		{name: "owner mismatch", mutate: func(app *model.App) { app.BackingServices[0].OwnerAppID = "app_other" }},
		{name: "unmanaged service", mutate: func(app *model.App) { app.BackingServices[0].Provisioner = "external" }},
		{name: "missing database password", mutate: func(app *model.App) { app.BackingServices[0].Spec.Postgres.Password = "" }},
		{name: "binding id missing", mutate: func(app *model.App) { app.Bindings[0].ID = "" }},
		{name: "binding app mismatch", mutate: func(app *model.App) { app.Bindings[0].AppID = "app_other" }},
		{name: "binding service mismatch", mutate: func(app *model.App) { app.Bindings[0].ServiceID = "service_other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cloneOrphanAdoptedApp(base)
			test.mutate(&snapshot)
			if _, err := prepareOrphanManagedAppAdoption(snapshot, time.Now().UTC()); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected unsafe snapshot to be rejected, got %v", err)
			}
		})
	}
	standalone := cloneOrphanAdoptedApp(base)
	standalone.BackingServices[0].OwnerAppID = ""
	prepared, err := prepareOrphanManagedAppAdoption(standalone, time.Now().UTC())
	if err != nil {
		t.Fatalf("prepare exact-bound standalone orphan: %v", err)
	}
	if got := prepared.Services[0].OwnerAppID; got != base.ID {
		t.Fatalf("expected explicit adoption to take service ownership as %q, got %q", base.ID, got)
	}
}

func assertPostgresSuspended(t *testing.T, s *Store, serviceID string, want bool) {
	t.Helper()
	service, err := s.GetBackingService(serviceID)
	if err != nil {
		t.Fatalf("get backing service: %v", err)
	}
	if service.Spec.Postgres == nil || service.Spec.Postgres.Suspended != want {
		t.Fatalf("expected postgres suspended=%v, got %+v", want, service.Spec.Postgres)
	}
}
