package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestImageStorageBillingCommitIsTenantBound(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	owner, err := s.CreateTenant("image storage owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateTenant("other image storage tenant")
	if err != nil {
		t.Fatal(err)
	}
	var ownerAppID string
	for _, tenant := range []model.Tenant{owner, other} {
		project, err := s.CreateProject(tenant.ID, "storage measurement", "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := s.CreateApp(tenant.ID, project.ID, "fixture", "", model.AppSpec{
			Image: "registry.example/fixture:stable", Replicas: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if tenant.ID == owner.ID {
			ownerAppID = app.ID
		}
	}
	// Reads and writes must agree on the tenant's canonical ID; an empty
	// inventory here would overwrite a real tenant's measured storage.
	repo := s.ImageStorageBilling(" " + owner.ID + "\t")
	if _, ok := any(repo).(*Store); ok {
		t.Fatal("image billing port exposes the unrestricted Store")
	}
	apps, err := repo.Apps()
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 1 || apps[0].ID != ownerAppID {
		t.Fatalf("expected only the billed tenant's app, got %#v", apps)
	}
	if err := repo.CommitStorage(3); err != nil {
		t.Fatal(err)
	}
	for tenant, want := range map[string]int64{owner.ID: 3, other.ID: 0} {
		summary, err := s.GetTenantBillingSummary(tenant)
		if err != nil {
			t.Fatal(err)
		}
		if got := summary.ManagedCommitted.StorageGibibytes; got != want {
			t.Fatalf("tenant storage = %d GiB, want %d GiB", got, want)
		}
	}
}

func TestImageStorageBillingRefusesBlankTenantWithoutDatabase(t *testing.T) {
	repo := new(Store).ImageStorageBilling(" \t\n")
	_, appsErr := repo.Apps()
	_, operationsErr := repo.RetentionOperations()
	commitErr := repo.CommitStorage(0)
	for action, err := range map[string]error{"apps": appsErr, "operations": operationsErr, "commit": commitErr} {
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s with a blank tenant: got %v, want ErrInvalidInput", action, err)
		}
	}
}
