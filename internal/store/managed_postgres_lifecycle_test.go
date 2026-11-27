package store

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestManagedPostgresSuspendResumePersistsOnlyAfterCompletion(t *testing.T) {
	t.Parallel()

	s := newStoreTestDatabase(t, "")
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

func TestAdoptOrphanManagedAppRestoresExactResourceIDsDisabledAndIdempotent(t *testing.T) {
	t.Parallel()

	s := newStoreTestDatabase(t, "")
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
