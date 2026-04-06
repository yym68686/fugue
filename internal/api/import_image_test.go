package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportImageStoresRequestedEnvOnCreatedApp(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Import Image Tenant")
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
	startupCommand := "npm run serve"
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-image", apiKey, map[string]any{
		"tenant_id":       tenant.ID,
		"image_ref":       "ghcr.io/example/demo:1.2.3",
		"service_port":    9090,
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

	if response.App.Name != "demo" {
		t.Fatalf("expected generated app name demo, got %q", response.App.Name)
	}
	if response.App.Source == nil {
		t.Fatal("expected app source in response")
	}
	if response.App.Source.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected source type %q, got %q", model.AppSourceTypeDockerImage, response.App.Source.Type)
	}
	if response.App.Source.ImageRef != "ghcr.io/example/demo:1.2.3" {
		t.Fatalf("expected image_ref to be preserved, got %q", response.App.Source.ImageRef)
	}

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
	if len(app.Spec.Ports) != 1 || app.Spec.Ports[0] != 9090 {
		t.Fatalf("expected requested service port 9090, got %v", app.Spec.Ports)
	}
	if len(app.Spec.Command) != 3 || app.Spec.Command[0] != "sh" || app.Spec.Command[1] != "-lc" || app.Spec.Command[2] != startupCommand {
		t.Fatalf("expected app command to wrap startup command, got %#v", app.Spec.Command)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected desired source type %q, got %q", model.AppSourceTypeDockerImage, op.DesiredSource.Type)
	}
	if op.DesiredSource.ImageRef != "ghcr.io/example/demo:1.2.3" {
		t.Fatalf("expected queued image_ref to match request, got %q", op.DesiredSource.ImageRef)
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
