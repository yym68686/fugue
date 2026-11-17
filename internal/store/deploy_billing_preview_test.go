package store

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestDeployBillingPreviewPostgresReadOnly(t *testing.T) {
	s := billingBatchPGStore(t)
	// This fixture runs after the same explicit migrations as import persistence.
	prepareImportBuildPostgres(t, s)
	app, op := importBuildFixture(t, s)
	if _, err := s.FailManagedOperationGuarded(op, "fixture complete"); err != nil {
		t.Fatal(err)
	}
	resources := model.DefaultManagedAppResources()
	spec := app.Spec
	spec.Resources = &resources
	if _, err := s.db.Exec(`UPDATE fugue_tenant_billing SET balance_microcents=0 WHERE tenant_id=$1`, app.TenantID); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		var result string
		if err := s.db.QueryRow(`SELECT row_to_json(b)::text || ':' || (SELECT count(*)::text FROM fugue_billing_events WHERE tenant_id=$1) || ':' || (SELECT count(*)::text FROM fugue_operations WHERE app_id=$2) FROM fugue_tenant_billing b WHERE tenant_id=$1`, app.TenantID, app.ID).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	before := snapshot()
	if err := s.CheckDeployBilling(context.Background(), app.ID, spec); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatal("read-only preview admitted unpaid resources", err)
	}
	if snapshot() != before {
		t.Fatal("preview changed billing or operations")
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &spec}); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatal("preview differs from admission", err)
	}
	if _, err := s.TopUpTenantBilling(app.TenantID, 1000, "fund release"); err != nil {
		t.Fatal(err)
	}
	before = snapshot()
	if err := s.CheckDeployBilling(context.Background(), app.ID, spec); err != nil {
		t.Fatal(err)
	}
	if snapshot() != before {
		t.Fatal("funded preview wrote billing")
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &spec}); err != nil {
		t.Fatal(err)
	}
}
