package store

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestTenantBillingSummaryUsesIndexedRelationships(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	consumerTenantID := "tenant_consumer"
	ownerTenantID := "tenant_owner"

	offer, err := normalizeRuntimePublicOffer(model.RuntimePublicOffer{
		ReferenceBundle: model.BillingResourceSpec{
			CPUMilliCores:    2000,
			MemoryMebibytes:  4096,
			StorageGibibytes: 20,
		},
		ReferenceMonthlyPriceMicroCents: 400 * microCentsPerCent,
	})
	if err != nil {
		t.Fatalf("normalize public offer: %v", err)
	}

	publicRuntime := model.Runtime{
		ID:          "runtime_public",
		TenantID:    ownerTenantID,
		Name:        "public-runtime",
		Type:        model.RuntimeTypeManagedOwned,
		AccessMode:  model.RuntimeAccessModePublic,
		PublicOffer: &offer,
	}
	managedRuntime := model.Runtime{
		ID:   model.DefaultManagedRuntimeID,
		Name: "managed-shared",
		Type: model.RuntimeTypeManagedShared,
	}

	publicResources := model.ResourceSpec{
		CPUMilliCores:   250,
		MemoryMebibytes: 512,
	}
	publicApp := model.App{
		ID:       "app_public",
		TenantID: consumerTenantID,
		Spec: model.AppSpec{
			Resources: &publicResources,
			RuntimeID: publicRuntime.ID,
		},
		Status: model.AppStatus{
			CurrentRuntimeID: publicRuntime.ID,
			CurrentReplicas:  2,
		},
		CreatedAt: now,
	}

	managedResources := model.ResourceSpec{
		CPUMilliCores:   500,
		MemoryMebibytes: 1024,
	}
	managedApp := model.App{
		ID:       "app_managed",
		TenantID: consumerTenantID,
		Spec: model.AppSpec{
			Resources: &managedResources,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{
			CurrentRuntimeID: model.DefaultManagedRuntimeID,
			CurrentReplicas:  1,
		},
		CreatedAt: now.Add(time.Second),
	}

	serviceResources := model.ResourceSpec{
		CPUMilliCores:   100,
		MemoryMebibytes: 256,
	}
	service := model.BackingService{
		ID:          "svc_postgres",
		TenantID:    consumerTenantID,
		OwnerAppID:  managedApp.ID,
		Type:        model.BackingServiceTypePostgres,
		Provisioner: model.BackingServiceProvisionerManaged,
		Status:      model.BackingServiceStatusActive,
		Spec: model.BackingServiceSpec{
			Postgres: &model.AppPostgresSpec{
				Resources:   &serviceResources,
				StorageSize: "2Gi",
			},
		},
		CreatedAt: now,
	}

	state := &model.State{
		Runtimes:        []model.Runtime{managedRuntime, publicRuntime},
		Apps:            []model.App{publicApp, managedApp},
		BackingServices: []model.BackingService{service},
		ServiceBindings: []model.ServiceBinding{
			{ID: "binding_one", TenantID: consumerTenantID, AppID: managedApp.ID, ServiceID: service.ID, CreatedAt: now},
			{ID: "binding_duplicate", TenantID: consumerTenantID, AppID: managedApp.ID, ServiceID: service.ID, CreatedAt: now.Add(time.Second)},
		},
	}
	record := model.TenantBilling{
		TenantID: consumerTenantID,
		ManagedCap: model.BillingResourceSpec{
			CPUMilliCores:    2000,
			MemoryMebibytes:  4096,
			StorageGibibytes: 20,
		},
		PriceBook:         model.DefaultBillingPriceBook(),
		BalanceMicroCents: 1000 * microCentsPerCent,
		LastAccruedAt:     now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	index := newBillingStateIndex(state)
	summary := buildTenantBillingSummaryWithIndex(state, index, record)

	wantCommitted := addResourceSpec(appEffectiveResources(managedApp.Spec), backingServiceResources(service))
	if summary.ManagedCommitted != wantCommitted {
		t.Fatalf("expected duplicate bindings to count service once, want %+v got %+v", wantCommitted, summary.ManagedCommitted)
	}

	publicUsage := multiplyResourceSpec(appEffectiveResources(publicApp.Spec), int64(publicApp.Status.CurrentReplicas))
	wantPublicHourly := publicRuntimeOfferHourlyRateMicroCents(offer, publicUsage)
	wantHourly := billingHourlyRateMicroCents(record) + wantPublicHourly
	if summary.HourlyRateMicroCents != wantHourly {
		t.Fatalf("expected hourly rate %d, got %d", wantHourly, summary.HourlyRateMicroCents)
	}
}
