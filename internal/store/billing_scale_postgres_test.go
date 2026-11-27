package store

import (
	"encoding/json"
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestPostgresScaleBillingEnforcesLimitsWithoutBlockingRecovery(t *testing.T) {
	for _, tt := range []struct {
		name         string
		start, scale int
		recovery     int
		deplete      bool
		want         error
	}{
		{name: "at cap", start: 1, scale: 2, recovery: 0, want: ErrBillingCapExceeded},
		{name: "already above cap", start: 2, scale: 3, recovery: 1, want: ErrBillingCapExceeded},
		{name: "depleted balance", start: 1, scale: 2, recovery: 0, deplete: true, want: ErrBillingBalanceDepleted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newStoreTestDatabase(t, "")
			tenant, err := s.CreateTenant("Billing fixture")
			if err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(tenant.ID, "apps", "")
			if err != nil {
				t.Fatal(err)
			}
			resources := model.DefaultManagedAppResources()
			app, err := s.CreateApp(tenant.ID, project.ID, "workload", "", model.AppSpec{
				Image: "registry.example/workload:stable", Ports: []int{8080},
				Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Resources: &resources,
			})
			if err != nil {
				t.Fatal(err)
			}
			capacity := model.BillingResourceSpecFromResourceSpec(resources)
			if tt.start > 1 || tt.deplete {
				capacity.CPUMilliCores *= 2
				capacity.MemoryMebibytes *= 2
				capacity.StorageGibibytes *= 2
			}
			if _, err = s.UpdateTenantBilling(tenant.ID, capacity); err != nil {
				t.Fatal(err)
			}
			if _, err = s.TopUpTenantBilling(tenant.ID, 500, "fixture"); err != nil {
				t.Fatal(err)
			}
			spec := app.Spec
			spec.Replicas = tt.start
			deploy, err := s.CreateOperation(model.Operation{Type: model.OperationTypeDeploy, TenantID: tenant.ID, AppID: app.ID, DesiredSpec: &spec})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.CompleteManagedOperation(deploy.ID, "", "fixture deployed"); err != nil {
				t.Fatal(err)
			}
			if tt.start > 1 {
				// Represent a historical allocation already above the current cap.
				capacity = model.BillingResourceSpecFromResourceSpec(resources)
				raw, err := json.Marshal(capacity)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.db.ExecContext(t.Context(), `UPDATE fugue_tenant_billing SET managed_cap_json=$2 WHERE tenant_id=$1`, tenant.ID, raw); err != nil {
					t.Fatal(err)
				}
			}
			if tt.deplete {
				if _, err = s.SetTenantBillingBalance(tenant.ID, 0, nil); err != nil {
					t.Fatal(err)
				}
			}
			scale := func(replicas int) error {
				_, err := s.CreateOperation(model.Operation{Type: model.OperationTypeScale, TenantID: tenant.ID, AppID: app.ID, DesiredReplicas: &replicas})
				return err
			}
			if err := scale(tt.scale); !errors.Is(err, tt.want) {
				t.Fatalf("scale up: got %v, want %v", err, tt.want)
			}
			if err := scale(tt.recovery); err != nil {
				t.Fatalf("billing must allow scale down: %v", err)
			}
			if summary, err := s.GetTenantBillingSummary(tenant.ID); err != nil || summary.ManagedCap != capacity {
				t.Fatalf("scale changed the configured cap: got %v, want %v: %v", summary.ManagedCap, capacity, err)
			}
		})
	}
}
