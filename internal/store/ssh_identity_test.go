package store

import (
	"bytes"
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func setupSSHIdentityStore(test *testing.T) (*Store, model.App, sshidentity.Keyring) {
	test.Helper()
	stateStore := newStoreTestDatabase(test, "")
	if err := stateStore.Init(); err != nil {
		test.Fatal(err)
	}
	tenant, err := stateStore.CreateTenant("identity-test")
	if err != nil {
		test.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "workloads", "")
	if err != nil {
		test.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "workspace", "", model.AppSpec{Image: "example/workspace:v1", Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	keyring, err := sshidentity.NewKeyring("storage-v1", map[string][]byte{"storage-v1": bytes.Repeat([]byte{21}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	return stateStore, app, keyring
}

func TestAppSSHIdentityTenantIsolationAndIndependentKeys(test *testing.T) {
	stateStore, app, _ := setupSSHIdentityStore(test)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.GetAppSSHIdentity("tenant_other", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatalf("identity visible to another tenant: %v", err)
	}
	if _, err := stateStore.PrepareAppSSHIdentity("tenant_other", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatalf("another tenant can prepare identity: %v", err)
	}
	if _, err := stateStore.AppSSHIdentityPrivateKey("tenant_other", app.ID, identity.ID); !errors.Is(err, ErrNotFound) {
		test.Fatalf("private identity visible to another tenant: %v", err)
	}
	if _, err := stateStore.AppSSHIdentityPrivateKey(app.TenantID, app.ID, "sshidentity_wrong"); !errors.Is(err, ErrConflict) {
		test.Fatalf("private identity read ignored identity precondition: %v", err)
	}
	other, err := stateStore.CreateApp(app.TenantID, app.ProjectID, "second-workspace", "", model.AppSpec{Image: app.Spec.Image, Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	otherIdentity, err := stateStore.PrepareAppSSHIdentity(other.TenantID, other.ID)
	if err != nil || otherIdentity.Fingerprint == identity.Fingerprint {
		test.Fatal("applications sharing an image also share SSH identity", err)
	}
}
