package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sshidentity"

	"golang.org/x/crypto/ssh"
)

func setupSSHIdentityStore(test *testing.T) (*Store, model.App, sshidentity.Keyring) {
	test.Helper()
	stateStore := New(filepath.Join(test.TempDir(), "store.json"))
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

func TestAppSSHIdentityConcurrentPreparationPreservesAppAndIdentity(test *testing.T) {
	stateStore, app, keyring := setupSSHIdentityStore(test)
	before, err := os.ReadFile(stateStore.path)
	if err != nil {
		test.Fatal(err)
	}
	const workers = 12
	identities := make(chan model.AppSSHIdentity, workers)
	failures := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			independentStore := New(stateStore.path)
			independentStore.ConfigureSSHIdentityKeyring(keyring)
			identity, err := independentStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
			identities <- identity
			failures <- err
		}()
	}
	group.Wait()
	close(identities)
	close(failures)
	for err := range failures {
		if err != nil {
			test.Fatal(err)
		}
	}
	var expected model.AppSSHIdentity
	for identity := range identities {
		if expected.ID == "" {
			expected = identity
		}
		if !reflect.DeepEqual(identity, expected) {
			test.Fatal("concurrent requests produced different identities")
		}
	}
	after, err := os.ReadFile(stateStore.path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("preparing identity modified application state", err)
	}
	if _, err := stateStore.GetAppSSHEndpoint(app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatalf("preparing identity allocated or changed a route: %v", err)
	}
	reopened := New(stateStore.path)
	reopened.ConfigureSSHIdentityKeyring(keyring)
	privateKey, err := reopened.AppSSHIdentityPrivateKey(app.TenantID, app.ID, expected.ID)
	if err != nil {
		test.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil || ssh.FingerprintSHA256(signer.PublicKey()) != expected.Fingerprint {
		test.Fatal("reopened identity does not match its durable private key", err)
	}
	if err := reopened.withLockedState(true, func(state *model.State) error {
		state.Apps[findApp(state, app.ID)].Spec.Image = "example/workspace:v2"
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	reconciled, err := reopened.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil || !reflect.DeepEqual(reconciled, expected) {
		test.Fatal("application release regenerated SSH identity", err)
	}
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

func TestAppSSHIdentityCorruptionAndKeyLossNeverRegenerate(test *testing.T) {
	stateStore, app, keyring := setupSSHIdentityStore(test)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	path := stateStore.path + ".ssh-identities.json"
	original, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(sshidentity.Keyring{})
	if _, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID); !errors.Is(err, sshidentity.ErrKeyUnavailable) {
		test.Fatalf("key loss failed open: %v", err)
	}
	public, err := stateStore.GetAppSSHIdentity(app.TenantID, app.ID)
	if err != nil || !reflect.DeepEqual(public, identity) {
		test.Fatal("public identity unavailable during encryption key recovery", err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, unchanged) {
		test.Fatal("missing encryption key rewrote identity", err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	if err := os.Remove(path); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID); !errors.Is(err, sshidentity.ErrIntegrity) {
		test.Fatal("missing identity file generated a replacement", err)
	}
	for _, corrupted := range [][]byte{[]byte("{"), bytes.Replace(original, []byte(`"ciphertext":"`), []byte(`"ciphertext":"broken`), 1)} {
		if err := os.WriteFile(path, corrupted, 0o600); err != nil {
			test.Fatal(err)
		}
		if _, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID); !errors.Is(err, sshidentity.ErrIntegrity) {
			test.Fatalf("corrupt material was not rejected: %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, corrupted) {
			test.Fatal("corrupt identity was silently replaced", err)
		}
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		test.Fatal(err)
	}
	recovered, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil || !reflect.DeepEqual(recovered, identity) {
		test.Fatal("restoring encrypted record changed identity", err)
	}
}
