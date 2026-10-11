package api

import (
	"bytes"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func TestSSHIdentityPreparationIsPublicOnlyAndDoesNotDeploy(test *testing.T) {
	stateStore, server, apiKey, app := setupAppSSHTestServer(test)
	keyring, err := sshidentity.NewKeyring("identity-storage", map[string][]byte{"identity-storage": bytes.Repeat([]byte{37}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	path := "/v1/apps/" + app.ID + "/ssh/identity"
	missing := performJSONRequest(test, server, http.MethodGet, path, apiKey, nil)
	if missing.Code != http.StatusNotFound {
		test.Fatalf("read generated an identity: %d", missing.Code)
	}
	var identity model.AppSSHIdentity
	for attempt := 0; attempt < 2; attempt++ {
		response := performJSONRequest(test, server, http.MethodPost, path, apiKey, nil)
		if response.Code != http.StatusOK {
			test.Fatalf("prepare: %d %s", response.Code, response.Body.String())
		}
		for _, forbidden := range []string{"ciphertext", "encryption_key", "PRIVATE KEY", "private_key"} {
			if strings.Contains(response.Body.String(), forbidden) {
				test.Fatal("private storage data in identity response")
			}
		}
		var result struct {
			Identity model.AppSSHIdentity `json:"identity"`
		}
		mustDecodeJSON(test, response, &result)
		if attempt == 0 {
			identity = result.Identity
		} else if !reflect.DeepEqual(identity, result.Identity) {
			test.Fatal("retry regenerated identity")
		}
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(app.Spec, after.Spec) {
		test.Fatal("identity preparation changed workload", err)
	}
	operations, err := stateStore.ListActiveOperations()
	if err != nil || len(operations) != 0 {
		test.Fatal("identity preparation scheduled a deployment", err)
	}
	stateStore.ConfigureSSHIdentityKeyring(sshidentity.Keyring{})
	response := performJSONRequest(test, server, http.MethodPost, path, apiKey, nil)
	if response.Code != http.StatusServiceUnavailable {
		test.Fatalf("missing storage key was not reported: %d", response.Code)
	}
	response = performJSONRequest(test, server, http.MethodGet, path, apiKey, nil)
	if response.Code != http.StatusOK {
		test.Fatalf("public recovery metadata unavailable: %d", response.Code)
	}
}

func TestSSHIdentityPermissionsAndTenantIsolation(test *testing.T) {
	stateStore, server, _, app := setupAppSSHTestServer(test)
	path := "/v1/apps/" + app.ID + "/ssh/identity"
	_, appReadKey, err := stateStore.CreateAPIKey(app.TenantID, "app-reader", []string{"app.read"})
	if err != nil {
		test.Fatal(err)
	}
	_, sshReadKey, err := stateStore.CreateAPIKey(app.TenantID, "ssh-reader", []string{"app.ssh.read"})
	if err != nil {
		test.Fatal(err)
	}
	otherTenant, err := stateStore.CreateTenant("other-identity-owner")
	if err != nil {
		test.Fatal(err)
	}
	_, otherKey, err := stateStore.CreateAPIKey(otherTenant.ID, "ssh-owner", []string{"app.ssh.read", "app.ssh.write"})
	if err != nil {
		test.Fatal(err)
	}
	for _, check := range []struct{ method, key string }{
		{http.MethodGet, appReadKey}, {http.MethodPost, appReadKey},
		{http.MethodPost, sshReadKey}, {http.MethodGet, otherKey}, {http.MethodPost, otherKey},
	} {
		response := performJSONRequest(test, server, check.method, path, check.key, nil)
		if response.Code != http.StatusForbidden {
			test.Fatalf("SSH identity authorization did not reject request: %d %s", response.Code, response.Body.String())
		}
	}
}
