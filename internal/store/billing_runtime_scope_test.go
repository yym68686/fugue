package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func deployBillingTestApp(t *testing.T, s *Store, tenantID, projectID, name string, spec model.AppSpec) model.App {
	t.Helper()

	app, err := s.CreateApp(tenantID, projectID, name, "", spec)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	deploySpec := app.Spec
	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenantID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/"+name+".yaml", "done"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	deployed, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get deployed app: %v", err)
	}

	return deployed
}

func TestManagedOwnedAppRuntimeDoesNotConsumeBillingOrBlockScale(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("BYO Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "tenant-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create managed-owned runtime: %v", err)
	}

	appResources := model.DefaultManagedAppResources()
	app := deployBillingTestApp(t, s, tenant.ID, project.ID, "demo", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: runtimeObj.ID,
		Workspace: &model.AppWorkspaceSpec{},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
	})

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}
	if summary.ManagedCommitted != (model.BillingResourceSpec{}) {
		t.Fatalf("expected managed-owned deployment to stay outside billing, got %+v", summary.ManagedCommitted)
	}

	if _, err := s.SetTenantBillingBalance(tenant.ID, 0, nil); err != nil {
		t.Fatalf("deplete billing balance: %v", err)
	}

	replicas := 2
	if _, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	}); err != nil {
		t.Fatalf("expected managed-owned scale-up to bypass billing restriction, got %v", err)
	}
}

func TestManagedOwnedBackingServiceDoesNotConsumeManagedBilling(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Mixed Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "tenant-db-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create managed-owned runtime: %v", err)
	}

	appResources := model.DefaultManagedAppResources()
	deployBillingTestApp(t, s, tenant.ID, project.ID, "internal-app", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		Resources: &appResources,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			RuntimeID: runtimeObj.ID,
		},
	})

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	want := model.BillingResourceSpecFromResourceSpec(appResources)
	if summary.ManagedCommitted != want {
		t.Fatalf("expected managed-owned postgres to stay free, want %+v got %+v", want, summary.ManagedCommitted)
	}
}

func TestManagedSharedBackingServiceStillCountsWhenAppRunsOnManagedOwned(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Mixed Service Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "tenant-app-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create managed-owned runtime: %v", err)
	}

	deployBillingTestApp(t, s, tenant.ID, project.ID, "byo-app", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			RuntimeID: "runtime_managed_shared",
		},
	})

	summary, err := s.GetTenantBillingSummary(tenant.ID)
	if err != nil {
		t.Fatalf("get billing summary: %v", err)
	}

	want := model.DefaultManagedPostgresBillingResources()
	if summary.ManagedCommitted != want {
		t.Fatalf("expected managed-shared postgres to remain billable, want %+v got %+v", want, summary.ManagedCommitted)
	}
}
