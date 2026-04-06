package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportGitHubStoresRequestedEnvOnCreatedApp(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Import Env Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "importer", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:    "apps.example.com",
		RegistryPushBase: "registry.internal.example",
	})
	startupCommand := "npm run start"
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-github", apiKey, map[string]any{
		"tenant_id":       tenant.ID,
		"repo_url":        "https://github.com/example/demo",
		"name":            "demo",
		"build_strategy":  model.AppBuildStrategyDockerfile,
		"dockerfile_path": "Dockerfile",
		"startup_command": startupCommand,
		"env": map[string]string{
			"OPENAI_API_KEY": "sk-demo",
			"APP_ENV":        "production",
		},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)

	app, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if got := app.Spec.Env["OPENAI_API_KEY"]; got != "sk-demo" {
		t.Fatalf("expected app env OPENAI_API_KEY=sk-demo, got %q", got)
	}
	if got := app.Spec.Env["APP_ENV"]; got != "production" {
		t.Fatalf("expected app env APP_ENV=production, got %q", got)
	}
	if len(app.Spec.Command) != 3 || app.Spec.Command[0] != "sh" || app.Spec.Command[1] != "-lc" || app.Spec.Command[2] != startupCommand {
		t.Fatalf("expected app command to wrap startup command, got %#v", app.Spec.Command)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSpec == nil {
		t.Fatal("expected desired spec on queued operation")
	}
	if got := op.DesiredSpec.Env["OPENAI_API_KEY"]; got != "sk-demo" {
		t.Fatalf("expected desired spec env OPENAI_API_KEY=sk-demo, got %q", got)
	}
	if got := op.DesiredSpec.Env["APP_ENV"]; got != "production" {
		t.Fatalf("expected desired spec env APP_ENV=production, got %q", got)
	}
	if len(op.DesiredSpec.Command) != 3 || op.DesiredSpec.Command[0] != "sh" || op.DesiredSpec.Command[1] != "-lc" || op.DesiredSpec.Command[2] != startupCommand {
		t.Fatalf("expected desired spec command to wrap startup command, got %#v", op.DesiredSpec.Command)
	}
}
