package api

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func TestManagedSSHStatusBindingAndActivationPreconditions(test *testing.T) {
	stateStore, server, apiKey, app := setupAppSSHTestServer(test)
	keyring, err := sshidentity.NewKeyring("storage", map[string][]byte{"storage": bytes.Repeat([]byte{76}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	spec := model.ManagedSSHIntentSpec{Enabled: true, IdentityID: identity.ID, Container: "workspace", User: "app", WorkingDirectory: "/workspace", AuthorizedKeyIDs: []string{}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}
	if _, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 0, spec); err != nil {
		test.Fatal(err)
	}
	path := "/v1/apps/" + app.ID + "/ssh/managed"
	response := performJSONRequest(test, server, http.MethodGet, path+"/status", apiKey, nil)
	if response.Code != http.StatusOK {
		test.Fatal("status failed", response.Body.String())
	}
	var status struct {
		LegacyRouteDigest string                         `json:"legacy_route_digest"`
		Binding           *model.ManagedSSHPublicBinding `json:"binding"`
	}
	mustDecodeJSON(test, response, &status)
	input := map[string]any{"identity_id": identity.ID, "legacy_route_digest": status.LegacyRouteDigest}
	_, reader, err := stateStore.CreateAPIKey(app.TenantID, "reader", []string{"app.ssh.read"})
	if err != nil {
		test.Fatal(err)
	}
	response = performJSONRequest(test, server, http.MethodPost, path+"/binding", reader, input)
	if response.Code != http.StatusForbidden {
		test.Fatal("read-only caller reserved a port")
	}
	response = performJSONRequest(test, server, http.MethodPost, path+"/binding", apiKey, map[string]any{"identity_id": identity.ID})
	if response.Code != http.StatusBadRequest {
		test.Fatal("missing route review accepted")
	}
	response = performJSONRequest(test, server, http.MethodPost, path+"/binding", apiKey, input)
	if response.Code != http.StatusOK {
		test.Fatal("binding preparation failed", response.Body.String())
	}
	response = performJSONRequest(test, server, http.MethodGet, path+"/status", reader, nil)
	if response.Code != http.StatusOK {
		test.Fatal("reader cannot see status", response.Body.String())
	}
	mustDecodeJSON(test, response, &status)
	if status.Binding == nil || status.Binding.IdentityID != identity.ID || status.Binding.LegacyRouteDigest != status.LegacyRouteDigest {
		test.Fatal("binding review metadata is inconsistent")
	}
	response = performJSONRequest(test, server, http.MethodPatch, "/v1/apps/"+app.ID+"/ssh", apiKey, map[string]any{"enabled": false})
	if response.Code != http.StatusConflict {
		test.Fatal("legacy mutation changed managed endpoint", response.Code)
	}
	response = performJSONRequest(test, server, http.MethodPost, path+"/activate", apiKey, map[string]any{"fingerprint": identity.Fingerprint})
	if response.Code != http.StatusBadRequest {
		test.Fatal("incomplete activation accepted")
	}
	response = performJSONRequest(test, server, http.MethodPost, path+"/activate", reader, map[string]any{})
	if response.Code != http.StatusForbidden {
		test.Fatal("read-only activation admitted")
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(app.Spec, after.Spec) {
		test.Fatal("binding/status changed workload", err)
	}
}
