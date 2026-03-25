package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPatchAPIKeyUpdatesLabelAndScopes(t *testing.T) {
	t.Parallel()

	s, server, adminSecret, targetSecret, _, app := setupAPIKeyTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", targetSecret, map[string]any{})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d before scope update, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	targetKey := firstNonAdminAPIKey(t, s)
	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/api-keys/"+targetKey.ID, adminSecret, map[string]any{
		"label":  "preview-ops",
		"scopes": []string{"app.write", "app.deploy"},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		APIKey model.APIKey `json:"api_key"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.APIKey.Label != "preview-ops" {
		t.Fatalf("expected updated label, got %q", response.APIKey.Label)
	}
	if len(response.APIKey.Scopes) != 2 || response.APIKey.Scopes[0] != "app.write" || response.APIKey.Scopes[1] != "app.deploy" {
		t.Fatalf("expected updated scopes, got %+v", response.APIKey.Scopes)
	}
	if response.APIKey.Hash != "" {
		t.Fatalf("expected redacted hash, got %q", response.APIKey.Hash)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", targetSecret, map[string]any{})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d after scope update, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
}

func TestRotateAPIKeyInvalidatesOldSecretAndReturnsNewSecret(t *testing.T) {
	t.Parallel()

	s, server, adminSecret, targetSecret, targetKey, app := setupAPIKeyTestServer(t)
	_ = s

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/api-keys/"+targetKey.ID+"/rotate", adminSecret, map[string]any{
		"label":  "preview-ops",
		"scopes": []string{"app.write", "app.deploy"},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		APIKey model.APIKey `json:"api_key"`
		Secret string       `json:"secret"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.APIKey.Label != "preview-ops" {
		t.Fatalf("expected rotated key label preview-ops, got %q", response.APIKey.Label)
	}
	if response.Secret == "" {
		t.Fatal("expected rotated secret")
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d for old secret, got %d body=%s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", response.Secret, map[string]any{})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d for rotated secret, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
}

func TestRotateAPIKeyAllowsEmptyBodyAndKeepsCurrentSettings(t *testing.T) {
	t.Parallel()

	_, server, adminSecret, targetSecret, targetKey, _ := setupAPIKeyTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/api-keys/"+targetKey.ID+"/rotate", adminSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		APIKey model.APIKey `json:"api_key"`
		Secret string       `json:"secret"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.APIKey.Label != targetKey.Label {
		t.Fatalf("expected label %q to be preserved, got %q", targetKey.Label, response.APIKey.Label)
	}
	if len(response.APIKey.Scopes) != len(targetKey.Scopes) || response.APIKey.Scopes[0] != targetKey.Scopes[0] {
		t.Fatalf("expected scopes %+v to be preserved, got %+v", targetKey.Scopes, response.APIKey.Scopes)
	}
	if response.Secret == "" {
		t.Fatal("expected rotated secret")
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d for old secret, got %d body=%s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", response.Secret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for new secret, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}

func TestDisableAndEnableAPIKeyTogglesAuthentication(t *testing.T) {
	t.Parallel()

	s, server, adminSecret, targetSecret, _, _ := setupAPIKeyTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d before disable, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	targetKey := firstNonAdminAPIKey(t, s)
	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/api-keys/"+targetKey.ID+"/disable", adminSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var disableResponse struct {
		APIKey model.APIKey `json:"api_key"`
	}
	mustDecodeJSON(t, recorder, &disableResponse)
	if disableResponse.APIKey.Status != model.APIKeyStatusDisabled {
		t.Fatalf("expected disabled status, got %q", disableResponse.APIKey.Status)
	}
	if disableResponse.APIKey.DisabledAt == nil {
		t.Fatal("expected disabled_at to be populated")
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d for disabled key, got %d body=%s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/api-keys/"+targetKey.ID+"/enable", adminSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var enableResponse struct {
		APIKey model.APIKey `json:"api_key"`
	}
	mustDecodeJSON(t, recorder, &enableResponse)
	if enableResponse.APIKey.Status != model.APIKeyStatusActive {
		t.Fatalf("expected active status after enable, got %q", enableResponse.APIKey.Status)
	}
	if enableResponse.APIKey.DisabledAt != nil {
		t.Fatalf("expected disabled_at to be cleared, got %v", enableResponse.APIKey.DisabledAt)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d after enable, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}

func TestDeleteAPIKeyRevokesSecretAndRemovesItFromList(t *testing.T) {
	t.Parallel()

	s, server, adminSecret, targetSecret, _, _ := setupAPIKeyTestServer(t)

	targetKey := firstNonAdminAPIKey(t, s)
	recorder := performJSONRequest(t, server, http.MethodDelete, "/v1/api-keys/"+targetKey.ID, adminSecret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var deleteResponse struct {
		Deleted bool         `json:"deleted"`
		APIKey  model.APIKey `json:"api_key"`
	}
	mustDecodeJSON(t, recorder, &deleteResponse)
	if !deleteResponse.Deleted {
		t.Fatal("expected deleted=true in response")
	}
	if deleteResponse.APIKey.ID != targetKey.ID {
		t.Fatalf("expected deleted key id %q, got %q", targetKey.ID, deleteResponse.APIKey.ID)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", targetSecret, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d for deleted key, got %d body=%s", http.StatusUnauthorized, recorder.Code, recorder.Body.String())
	}

	keys, err := s.ListAPIKeys("", true)
	if err != nil {
		t.Fatalf("list api keys after delete: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 api key after delete, got %d", len(keys))
	}
	if keys[0].ID == targetKey.ID {
		t.Fatalf("expected deleted key %q to be absent", targetKey.ID)
	}
}

func setupAPIKeyTestServer(t *testing.T) (*store.Store, *Server, string, string, model.APIKey, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("API Key Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, adminSecret, err := s.CreateAPIKey(tenant.ID, "key-admin", []string{"apikey.write", "app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create admin api key: %v", err)
	}
	targetKey, targetSecret, err := s.CreateAPIKey(tenant.ID, "preview", []string{"app.write"})
	if err != nil {
		t.Fatalf("create target api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, adminSecret, targetSecret, targetKey, app
}

func firstNonAdminAPIKey(t *testing.T, s *store.Store) model.APIKey {
	t.Helper()

	keys, err := s.ListAPIKeys("", true)
	if err != nil {
		t.Fatalf("list api keys: %v", err)
	}
	for _, key := range keys {
		if key.Label != "key-admin" {
			return key
		}
	}
	t.Fatal("expected target api key")
	return model.APIKey{}
}
