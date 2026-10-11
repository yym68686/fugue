package api

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func TestManagedSSHIntentAPIContractAndIsolation(test *testing.T) {
	stateStore, server, apiKey, app := setupAppSSHTestServer(test)
	keyring, err := sshidentity.NewKeyring("storage", map[string][]byte{"storage": bytes.Repeat([]byte{63}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	path := "/v1/apps/" + app.ID + "/ssh/managed"
	spec := model.ManagedSSHIntentSpec{Enabled: true, IdentityID: identity.ID, Container: "workspace", User: "app", WorkingDirectory: "/", AuthorizedKeyIDs: []string{}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}
	payload := map[string]any{"expected_generation": 0, "spec": spec}
	for _, invalid := range []map[string]any{
		{"spec": spec}, {"expected_generation": 0}, {"expected_generation": nil, "spec": spec},
		{"expected_generation": 0, "spec": map[string]any{"identity_id": identity.ID}},
		{"expected_generation": 0, "spec": spec, "activate": true},
	} {
		response := performJSONRequest(test, server, http.MethodPut, path, apiKey, invalid)
		if response.Code != http.StatusBadRequest {
			test.Fatalf("invalid intent accepted: %d %s", response.Code, response.Body.String())
		}
	}
	_, reader, err := stateStore.CreateAPIKey(app.TenantID, "ssh-reader", []string{"app.ssh.read"})
	if err != nil {
		test.Fatal(err)
	}
	response := performJSONRequest(test, server, http.MethodPut, path, reader, payload)
	if response.Code != http.StatusForbidden {
		test.Fatal("read-only key updated intent", response.Code)
	}
	otherTenant, err := stateStore.CreateTenant("other-managed-owner")
	if err != nil {
		test.Fatal(err)
	}
	_, otherKey, err := stateStore.CreateAPIKey(otherTenant.ID, "ssh-owner", []string{"app.ssh.read", "app.ssh.write"})
	if err != nil {
		test.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		response := performJSONRequest(test, server, method, path, otherKey, payload)
		if response.Code != http.StatusForbidden {
			test.Fatal("managed intent crossed tenant boundary", response.Code)
		}
	}
	response = performJSONRequest(test, server, http.MethodPut, path, apiKey, payload)
	if response.Code != http.StatusOK {
		test.Fatalf("put intent: %d %s", response.Code, response.Body.String())
	}
	var result struct {
		Intent model.ManagedSSHIntent `json:"intent"`
	}
	mustDecodeJSON(test, response, &result)
	if result.Intent.Generation != 1 || !reflect.DeepEqual(result.Intent.Spec, spec) {
		test.Fatal("intent response differs from contract")
	}
	response = performJSONRequest(test, server, http.MethodPut, path, apiKey, payload)
	if response.Code != http.StatusConflict {
		test.Fatal("stale writer overwrote intent", response.Code)
	}
	response = performJSONRequest(test, server, http.MethodGet, path, reader, nil)
	if response.Code != http.StatusOK {
		test.Fatal("reader cannot inspect intent", response.Code)
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(app.Spec, after.Spec) {
		test.Fatal("managed intent changed application spec", err)
	}
	operations, err := stateStore.ListActiveOperations()
	if err != nil || len(operations) != 0 {
		test.Fatal("managed intent scheduled an application deployment", err)
	}
}
