package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppReadRedactsSecretFilesButDedicatedConfigEndpointsReturnValues(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"OPENAI_API_KEY": "sk-demo",
			"LOG_LEVEL":      "debug",
		},
		Files: []model.AppFile{
			{
				Path:    "/home/api.yaml",
				Content: "providers: []",
				Secret:  true,
				Mode:    0o600,
			},
			{
				Path:    "/srv/banner.txt",
				Content: "hello",
				Mode:    0o644,
			},
		},
	})
	_ = s

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var appResponse struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &appResponse)
	if got := appResponse.App.Spec.Files[0].Content; got != "" {
		t.Fatalf("expected secret file to be redacted, got %q", got)
	}
	if got := appResponse.App.Spec.Files[1].Content; got != "hello" {
		t.Fatalf("expected non-secret file content to remain visible, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listResponse struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &listResponse)
	if len(listResponse.Apps) != 1 {
		t.Fatalf("expected 1 app, got %d", len(listResponse.Apps))
	}
	if got := listResponse.Apps[0].Spec.Files[0].Content; got != "" {
		t.Fatalf("expected secret file to be redacted in list response, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["OPENAI_API_KEY"]; got != "sk-demo" {
		t.Fatalf("expected env value to be visible, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var filesResponse struct {
		Files []model.AppFile `json:"files"`
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(filesResponse.Files))
	}
	if got := filesResponse.Files[0].Content; got != "providers: []" {
		t.Fatalf("expected dedicated files endpoint to return file content, got %q", got)
	}
}

func TestPatchAppEnvAndRestartCreateDeployOperations(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"OLD": "1",
		},
	})

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/env", apiKey, map[string]any{
		"set": map[string]string{
			"OLD": "3",
			"NEW": "2",
		},
		"delete": []string{"MISSING"},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var patchResponse struct {
		Env       map[string]string `json:"env"`
		Operation model.Operation   `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &patchResponse)
	if patchResponse.Operation.DesiredSpec == nil {
		t.Fatal("expected desired spec in deploy operation")
	}
	if got := patchResponse.Operation.DesiredSpec.Env["NEW"]; got != "2" {
		t.Fatalf("expected NEW env in desired spec, got %q", got)
	}
	if got := patchResponse.Env["OLD"]; got != "3" {
		t.Fatalf("expected OLD env to be updated, got %q", got)
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["OLD"]; got != "3" {
		t.Fatalf("expected persisted OLD env=3, got %q", got)
	}
	if got := envResponse.Env["NEW"]; got != "2" {
		t.Fatalf("expected persisted NEW env=2, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/restart", apiKey, map[string]any{})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var restartResponse struct {
		RestartToken string          `json:"restart_token"`
		Operation    model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &restartResponse)
	if restartResponse.RestartToken == "" {
		t.Fatal("expected restart token")
	}
	if restartResponse.Operation.DesiredSpec == nil || restartResponse.Operation.DesiredSpec.RestartToken == "" {
		t.Fatal("expected restart token in desired spec")
	}

	completeNextManagedOperation(t, s)

	updatedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after restart: %v", err)
	}
	if updatedApp.Spec.RestartToken == "" {
		t.Fatal("expected restart token to persist on app spec")
	}
}

func TestGetAppEnvMergesBindingEnvAndAppEnvOverrides(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"DB_HOST":   "override-db.internal",
			"LOG_LEVEL": "debug",
		},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "root",
			Password: "secret",
		},
	})

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &response)
	if got := response.Env["DB_TYPE"]; got != "postgres" {
		t.Fatalf("expected DB_TYPE=postgres, got %q", got)
	}
	if got := response.Env["DB_HOST"]; got != "override-db.internal" {
		t.Fatalf("expected app env to override DB_HOST, got %q", got)
	}
	if got := response.Env["DB_USER"]; got != "root" {
		t.Fatalf("expected DB_USER=root from binding env, got %q", got)
	}
	if got := response.Env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected DB_NAME=demo from binding env, got %q", got)
	}
	if got := response.Env["LOG_LEVEL"]; got != "debug" {
		t.Fatalf("expected LOG_LEVEL=debug, got %q", got)
	}
}

func TestUpsertAndDeleteAppFilesCreateDeployOperations(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Files: []model.AppFile{
			{
				Path:    "/home/api.yaml",
				Content: "providers: []",
				Secret:  true,
				Mode:    0o600,
			},
		},
	})

	recorder := performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/files", apiKey, map[string]any{
		"files": []map[string]any{
			{
				"path":    "/home/api.yaml",
				"content": "providers:\n  - gemini",
				"secret":  true,
				"mode":    0o600,
			},
			{
				"path":    "/srv/banner.txt",
				"content": "hello",
				"mode":    0o644,
			},
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var upsertResponse struct {
		Files     []model.AppFile `json:"files"`
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &upsertResponse)
	if len(upsertResponse.Files) != 2 {
		t.Fatalf("expected 2 files after upsert, got %d", len(upsertResponse.Files))
	}
	if upsertResponse.Operation.DesiredSpec == nil || len(upsertResponse.Operation.DesiredSpec.Files) != 2 {
		t.Fatal("expected desired spec with 2 files")
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var filesResponse struct {
		Files []model.AppFile `json:"files"`
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 2 {
		t.Fatalf("expected 2 files after deploy, got %d", len(filesResponse.Files))
	}
	if got := filesResponse.Files[0].Content; got != "providers:\n  - gemini" {
		t.Fatalf("expected updated file content, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/files?path=/srv/banner.txt", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/files", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	mustDecodeJSON(t, recorder, &filesResponse)
	if len(filesResponse.Files) != 1 {
		t.Fatalf("expected 1 file after delete, got %d", len(filesResponse.Files))
	}
	if filesResponse.Files[0].Path != "/home/api.yaml" {
		t.Fatalf("unexpected remaining file path %q", filesResponse.Files[0].Path)
	}
}

func setupAppConfigTestServer(t *testing.T, spec model.AppSpec) (*store.Store, *Server, string, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Config Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", spec)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, apiKey, app
}

func performJSONRequest(t *testing.T, server *Server, method, target, apiKey string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
	}

	req := httptest.NewRequest(method, target, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func mustDecodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
}

func completeNextManagedOperation(t *testing.T, s *store.Store) {
	t.Helper()
	op, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim next operation: %v", err)
	}
	if !found {
		t.Fatal("expected pending operation")
	}
	if _, err := s.CompleteManagedOperation(op.ID, "/tmp/app.yaml", "done"); err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}
}
