package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestAppSSHKeyAndEndpointLifecycle(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppSSHTestServer(t)

	createKey := performJSONRequest(t, server, http.MethodPost, "/v1/ssh/keys", apiKey, map[string]any{
		"label":      "laptop",
		"public_key": testAPISSHPublicKey,
	})
	if createKey.Code != http.StatusCreated {
		t.Fatalf("expected create key status %d, got %d body=%s", http.StatusCreated, createKey.Code, createKey.Body.String())
	}
	var createKeyResp struct {
		SSHKey model.SSHKey `json:"ssh_key"`
	}
	mustDecodeJSON(t, createKey, &createKeyResp)
	if createKeyResp.SSHKey.Fingerprint == "" {
		t.Fatalf("expected key fingerprint, got %+v", createKeyResp.SSHKey)
	}

	listKeys := performJSONRequest(t, server, http.MethodGet, "/v1/ssh/keys", apiKey, nil)
	if listKeys.Code != http.StatusOK {
		t.Fatalf("expected list keys status %d, got %d body=%s", http.StatusOK, listKeys.Code, listKeys.Body.String())
	}
	var listKeysResp struct {
		SSHKeys []model.SSHKey `json:"ssh_keys"`
	}
	mustDecodeJSON(t, listKeys, &listKeysResp)
	if len(listKeysResp.SSHKeys) != 1 || listKeysResp.SSHKeys[0].ID != createKeyResp.SSHKey.ID {
		t.Fatalf("expected created key in list, got %+v", listKeysResp.SSHKeys)
	}

	enable := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/ssh", apiKey, map[string]any{
		"enabled":            true,
		"target_port":        2222,
		"authorized_key_ids": []string{createKeyResp.SSHKey.ID},
	})
	if enable.Code != http.StatusAccepted {
		t.Fatalf("expected enable status %d, got %d body=%s", http.StatusAccepted, enable.Code, enable.Body.String())
	}
	var enableResp struct {
		SSH       model.AppSSHStatus   `json:"ssh"`
		Endpoint  model.AppSSHEndpoint `json:"endpoint"`
		Operation model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, enable, &enableResp)
	if enableResp.SSH.Hostname != "ssh.fugue.pro" || enableResp.SSH.PublicPort != model.DefaultAppSSHPublicPortStart || enableResp.SSH.TargetPort != 2222 {
		t.Fatalf("unexpected SSH status after enable: %+v", enableResp.SSH)
	}
	if enableResp.SSH.User != model.DefaultAppSSHUser || enableResp.Endpoint.User != model.DefaultAppSSHUser {
		t.Fatalf("expected default SSH user %q, got status=%q endpoint=%q", model.DefaultAppSSHUser, enableResp.SSH.User, enableResp.Endpoint.User)
	}
	if enableResp.Operation.ID == "" {
		t.Fatalf("expected deploy operation after enabling SSH, got %+v", enableResp.Operation)
	}

	storedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if storedApp.Spec.SSH == nil || len(storedApp.Spec.SSH.AuthorizedKeys) != 1 {
		t.Fatalf("expected app spec to include resolved SSH keys, got %+v", storedApp.Spec.SSH)
	}

	diagnose := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/ssh/diagnose", apiKey, nil)
	if diagnose.Code != http.StatusOK {
		t.Fatalf("expected diagnose status %d, got %d body=%s", http.StatusOK, diagnose.Code, diagnose.Body.String())
	}
	var diagnoseResp struct {
		Checks []appSSHDiagnosisCheck `json:"checks"`
	}
	mustDecodeJSON(t, diagnose, &diagnoseResp)
	if !appSSHTestCheckPassed(diagnoseResp.Checks, "ssh_enabled") || !appSSHTestCheckPassed(diagnoseResp.Checks, "edge_route") {
		t.Fatalf("expected ssh_enabled and edge_route checks to pass, got %+v", diagnoseResp.Checks)
	}

	rotate := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/ssh/rotate-port", apiKey, nil)
	if rotate.Code != http.StatusOK {
		t.Fatalf("expected rotate status %d, got %d body=%s", http.StatusOK, rotate.Code, rotate.Body.String())
	}
	var rotateResp struct {
		SSH model.AppSSHStatus `json:"ssh"`
	}
	mustDecodeJSON(t, rotate, &rotateResp)
	if rotateResp.SSH.PublicPort == enableResp.SSH.PublicPort {
		t.Fatalf("expected rotated port to change from %d, got %+v", enableResp.SSH.PublicPort, rotateResp.SSH)
	}

	disable := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/ssh", apiKey, map[string]any{
		"enabled": false,
	})
	if disable.Code != http.StatusAccepted {
		t.Fatalf("expected disable status %d, got %d body=%s", http.StatusAccepted, disable.Code, disable.Body.String())
	}
	var disableResp struct {
		SSH model.AppSSHStatus `json:"ssh"`
	}
	mustDecodeJSON(t, disable, &disableResp)
	if disableResp.SSH.Ready {
		t.Fatalf("expected disabled SSH to be not ready, got %+v", disableResp.SSH)
	}

	deleteKey := performJSONRequest(t, server, http.MethodDelete, "/v1/ssh/keys/"+createKeyResp.SSHKey.ID, apiKey, nil)
	if deleteKey.Code != http.StatusOK {
		t.Fatalf("expected delete key status %d, got %d body=%s", http.StatusOK, deleteKey.Code, deleteKey.Body.String())
	}
}

func TestEdgeSSHRoutesPublishesFilteredBundle(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppSSHTestServer(t)
	enable := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/ssh", apiKey, map[string]any{
		"enabled":         true,
		"authorized_keys": []string{testAPISSHPublicKey},
	})
	if enable.Code != http.StatusAccepted {
		t.Fatalf("expected enable status %d, got %d body=%s", http.StatusAccepted, enable.Code, enable.Body.String())
	}

	routes := performJSONRequest(t, server, http.MethodGet, "/v1/edge/ssh/routes?token=edge-secret&edge_group_id=edge-group-country-us", "", nil)
	if routes.Code != http.StatusOK {
		t.Fatalf("expected routes status %d, got %d body=%s", http.StatusOK, routes.Code, routes.Body.String())
	}
	var bundle model.EdgeSSHRouteBundle
	mustDecodeJSON(t, routes, &bundle)
	if bundle.Version == "" || bundle.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("unexpected bundle metadata: %+v", bundle)
	}
	if len(bundle.Routes) != 1 || bundle.Routes[0].AppID != app.ID || bundle.Routes[0].PublicPort != model.DefaultAppSSHPublicPortStart {
		t.Fatalf("unexpected SSH routes: %+v", bundle.Routes)
	}

	empty := performJSONRequest(t, server, http.MethodGet, "/v1/edge/ssh/routes?token=edge-secret&edge_group_id=edge-group-country-de", "", nil)
	if empty.Code != http.StatusOK {
		t.Fatalf("expected filtered routes status %d, got %d body=%s", http.StatusOK, empty.Code, empty.Body.String())
	}
	var emptyBundle model.EdgeSSHRouteBundle
	mustDecodeJSON(t, empty, &emptyBundle)
	if len(emptyBundle.Routes) != 0 {
		t.Fatalf("expected filtered bundle to be empty, got %+v", emptyBundle.Routes)
	}
}

const testAPISSHPublicKey = "ssh-ed25519 AQIDBAUGBwg= laptop"

func setupAppSSHTestServer(t *testing.T) (*store.Store, *Server, string, model.App) {
	t.Helper()
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "Apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "us-runtime", model.RuntimeTypeManagedOwned, "", map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "agent", "", model.AppSpec{
		Image:     "ghcr.io/example/agent:ssh",
		RuntimeID: runtimeObj.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "ssh-admin", []string{"ssh.key.read", "ssh.key.write", "app.ssh.read", "app.ssh.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:        "fugue.pro",
		SSHPublicPortStart:   model.DefaultAppSSHPublicPortStart,
		SSHPublicPortEnd:     model.DefaultAppSSHPublicPortStart + 10,
		EdgeTLSAskToken:      "edge-secret",
		AllowLegacyEdgeToken: true,
	})
	return s, server, apiKey, app
}

func appSSHTestCheckPassed(checks []appSSHDiagnosisCheck, name string) bool {
	for _, check := range checks {
		if check.Name == name {
			return check.Pass
		}
	}
	return false
}
