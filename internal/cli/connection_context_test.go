package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextPrecedenceAndAPIIsolation(t *testing.T) {
	t.Setenv("FUGUE_CONTEXT_FILE", filepath.Join(t.TempDir(), "contexts.json"))
	t.Setenv("FUGUE_CONTEXT", "")
	t.Setenv("FUGUE_BASE_URL", "")
	t.Setenv("FUGUE_API_URL", "")
	t.Setenv("FUGUE_PROJECT", "")
	t.Setenv("FUGUE_PROJECT_NAME", "")
	cfg := connectionContextFile{SchemaVersion: 1, Active: "production", Contexts: []connectionContext{{Name: "production", BaseURL: "https://api.example.com", Tenant: "tenant-a", Project: "project-a"}}}
	if err := saveConnectionContexts(cfg); err != nil {
		t.Fatal(err)
	}
	c := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	if c.effectiveBaseURL() != "https://api.example.com" || c.effectiveProjectName() != "project-a" {
		t.Fatal("context defaults missing")
	}
	t.Setenv("FUGUE_PROJECT", "from-env")
	if c.effectiveProjectName() != "from-env" {
		t.Fatal("env must override context")
	}
	c.root.ProjectName = "from-flag"
	if c.effectiveProjectName() != "from-flag" {
		t.Fatal("flag must override environment")
	}
	c.root.BaseURL = "https://other.example.com"
	if c.contextValue("project") != "" || c.contextValue("tenant") != "" {
		t.Fatal("cross-API scope leaked")
	}
	raw, err := os.ReadFile(connectionContextPath())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "token") || strings.Contains(string(raw), "spec") {
		t.Fatal("context must not persist credentials or serving state")
	}
}
func TestContextRejectsSecretURLsAndUnknownFields(t *testing.T) {
	for _, base := range []string{"https://user:secret@example.com", "https://api.example.com?token=secret", "https://api.example.com#secret", "file:///tmp/test"} {
		if validateConnectionContext(connectionContext{Name: "test", BaseURL: base}) == nil {
			t.Fatal(base)
		}
	}
	t.Setenv("FUGUE_CONTEXT_FILE", filepath.Join(t.TempDir(), "contexts.json"))
	if err := os.WriteFile(connectionContextPath(), []byte(`{"schema_version":1,"contexts":[],"token":"forbidden"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readConnectionContexts(); err == nil {
		t.Fatal("unknown fields accepted")
	}
	t.Setenv("FUGUE_CONTEXT", "none")
	if value, err := loadActiveConnectionContext(); err != nil || value != nil {
		t.Fatal("explicit recovery bypass failed")
	}
}
func TestCapabilitiesSeparatesProtocolAndAuthorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("unexpected mutation %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/auth/context":
			fmt.Fprint(w, `{"principal":{"scopes":["app.read"],"platform_admin":false}}`)
		case "/openapi.json":
			fmt.Fprint(w, `{"info":{"version":"1"},"paths":{"/v1/apps":{"get":{"operationId":"listApps"},"post":{"operationId":"createApp"}}}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test", "--json", "capabilities"}, &out, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["authorization"] != "enforced_by_server" || result["feature_enablement"] != "unknown" {
		t.Fatal(result)
	}
	if len(result["server"].(map[string]any)["advertised_operations"].([]any)) != 2 {
		t.Fatal(result)
	}
}
