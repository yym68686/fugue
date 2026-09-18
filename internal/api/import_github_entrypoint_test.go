package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImportGitHubV2GeneratedEntrypointPersistsRoutes(t *testing.T) {
	fixture := newGitHubTemplateFixture(t, map[string]string{
		"fugue.yaml": `version: 2
primary_service: browser
entrypoints:
  - name: public
    routes:
      - path: /
        service: browser
services:
  browser:
    public: true
    port: 3000
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
  api:
    port: 8000
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
    env:
      APP_PUBLIC_URL: '${FUGUE_ENTRYPOINT_ORIGIN:public}'
      PASSKEY_RP_ID: '${FUGUE_ENTRYPOINT_HOST:public}'
`, "Dockerfile": "FROM scratch\nEXPOSE 3000\n",
	})
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("Example Tenant")
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := s.CreateAPIKey(tenant.ID, "deployer", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{ImportWorkDir: t.TempDir(), AppBaseDomain: "apps.example.com", RegistryPushBase: "registry.example.com"})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-github", key, map[string]any{"repo_url": fixture.repoURL, "project": map[string]string{"name": "example-project"}})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected import accepted, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Apps       []model.App       `json:"apps"`
		Operations []model.Operation `json:"operations"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Apps) != 2 || len(response.Operations) != 2 {
		t.Fatalf("expected full topology, apps=%d ops=%d", len(response.Apps), len(response.Operations))
	}
	table, err := s.GetProjectRouteTable(response.Apps[0].ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := store.CompileProjectRouteTableBindings(table, response.Apps)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Hostname == "" {
		t.Fatalf("missing generated entrypoint: %+v", bindings)
	}
	for _, queued := range response.Operations {
		op, err := s.GetOperation(queued.ID)
		if err != nil {
			t.Fatal(err)
		}
		if op.DesiredSource.ComposeService == "api" && (op.DesiredSpec.Env["PASSKEY_RP_ID"] != bindings[0].Hostname || op.DesiredSpec.Env["APP_PUBLIC_URL"] != "https://"+bindings[0].Hostname) {
			t.Fatalf("route table and application origin diverge")
		}
	}
}
