package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestPaymentReferenceCreditsExactlyOnce(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("settlement fixture")
	if err != nil {
		t.Fatal(err)
	}
	another, err := s.CreateTenant("other settlement fixture")
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		summary, err := s.SettleTenantBilling(tenant.ID, 200, "fixture", "provider:order-1")
		if err != nil || summary.BalanceMicroCents != before.BalanceMicroCents+200*microCentsPerCent {
			t.Fatalf("delivery %d: balance %d, error %v", i, summary.BalanceMicroCents, err)
		}
	}
	for _, tc := range []struct {
		id    string
		cents int64
	}{{tenant.ID, 300}, {another.ID, 200}} {
		if _, err := s.SettleTenantBilling(tc.id, tc.cents, "fixture", "provider:order-1"); !errors.Is(err, ErrConflict) {
			t.Fatalf("conflicting settlement accepted: %v", err)
		}
	}
}
