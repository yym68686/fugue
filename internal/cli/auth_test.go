package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthCredentialStoreUsesFileFallbackAndPrecedence(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	t.Setenv("FUGUE_CONFIG_FILE", configPath)
	t.Setenv("FUGUE_AUTH_STORAGE", "file")
	t.Setenv("FUGUE_TOKEN", "")
	t.Setenv("FUGUE_API_KEY", "")
	t.Setenv("FUGUE_BOOTSTRAP_KEY", "")

	cred, err := saveAuthCredential("HTTPS://API.EXAMPLE.COM/", "saved-token")
	if err != nil {
		t.Fatalf("save credential: %v", err)
	}
	if cred.Source != string(authTokenSourceSavedFile) {
		t.Fatalf("expected file credential source, got %q", cred.Source)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if info.Mode().Perm() != authConfigFileMode {
		t.Fatalf("expected config mode %o, got %o", authConfigFileMode, info.Mode().Perm())
	}
	loaded, ok, err := loadSavedAuthCredential("https://api.example.com")
	if err != nil || !ok {
		t.Fatalf("load saved credential ok=%t err=%v", ok, err)
	}
	if loaded.Token != "saved-token" {
		t.Fatalf("unexpected saved token %q", loaded.Token)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cli := newCLI(&stdout, &stderr)
	cli.root.BaseURL = "https://api.example.com"
	if token, source := cli.effectiveTokenWithSource(); token != "saved-token" || source != authTokenSourceSavedFile {
		t.Fatalf("expected saved token, got token=%q source=%q", token, source)
	}
	t.Setenv("FUGUE_API_KEY", "env-token")
	if token, source := cli.effectiveTokenWithSource(); token != "env-token" || source != authTokenSourceEnvAPIKey {
		t.Fatalf("expected env token, got token=%q source=%q", token, source)
	}
	cli.root.Token = "flag-token"
	if token, source := cli.effectiveTokenWithSource(); token != "flag-token" || source != authTokenSourceFlag {
		t.Fatalf("expected flag token, got token=%q source=%q", token, source)
	}
}

func TestAuthLoginStatusLogoutAndSavedTokenUse(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	t.Setenv("FUGUE_CONFIG_FILE", configPath)
	t.Setenv("FUGUE_AUTH_STORAGE", "file")
	t.Setenv("FUGUE_SKIP_UPDATE_CHECK", "1")
	t.Setenv("FUGUE_TOKEN", "")
	t.Setenv("FUGUE_API_KEY", "")
	t.Setenv("FUGUE_BOOTSTRAP_KEY", "")

	const secret = "test-secret"
	server := newAuthCommandTestServer(t, secret)
	defer server.Close()

	stdout, stderr, err := runAuthCLI("--base-url", server.URL, "auth", "login", "--token", secret)
	if err != nil {
		t.Fatalf("auth login: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Saved Fugue API key") || strings.Contains(stdout, secret) {
		t.Fatalf("unexpected login output %q", stdout)
	}

	stdout, stderr, err = runAuthCLI("--base-url", server.URL, "auth", "status")
	if err != nil {
		t.Fatalf("auth status: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	for _, want := range []string{"Status: API key configured", "Active source:", "Token: sha256:"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected status output to contain %q, got %q", want, stdout)
		}
	}
	if strings.Contains(stdout, secret) {
		t.Fatalf("status leaked token: %q", stdout)
	}

	stdout, stderr, err = runAuthCLI("--base-url", server.URL, "tenant", "ls")
	if err != nil {
		t.Fatalf("tenant ls with saved token: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Acme") {
		t.Fatalf("expected tenant output, got %q", stdout)
	}

	stdout, stderr, err = runAuthCLI("--base-url", server.URL, "auth", "logout")
	if err != nil {
		t.Fatalf("auth logout: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Removed saved Fugue API key") {
		t.Fatalf("unexpected logout output %q", stdout)
	}
	stdout, stderr, err = runAuthCLI("--base-url", server.URL, "auth", "status")
	if err != nil {
		t.Fatalf("auth status after logout: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Status: no API key configured") {
		t.Fatalf("expected no auth status, got %q", stdout)
	}
}

func TestSaveTokenFlagPersistsVerifiedRootToken(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	t.Setenv("FUGUE_CONFIG_FILE", configPath)
	t.Setenv("FUGUE_AUTH_STORAGE", "file")
	t.Setenv("FUGUE_SKIP_UPDATE_CHECK", "1")
	t.Setenv("FUGUE_TOKEN", "")
	t.Setenv("FUGUE_API_KEY", "")
	t.Setenv("FUGUE_BOOTSTRAP_KEY", "")

	const secret = "save-token-secret"
	server := newAuthCommandTestServer(t, secret)
	defer server.Close()

	stdout, stderr, err := runAuthCLI("--base-url", server.URL, "--token", secret, "--save-token", "tenant", "ls")
	if err != nil {
		t.Fatalf("tenant ls with save-token: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stderr, "Saved Fugue API key") {
		t.Fatalf("expected save-token progress on stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "Acme") {
		t.Fatalf("expected tenant output, got %q", stdout)
	}

	stdout, stderr, err = runAuthCLI("--base-url", server.URL, "tenant", "ls")
	if err != nil {
		t.Fatalf("tenant ls with persisted token: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Acme") {
		t.Fatalf("expected tenant output, got %q", stdout)
	}
}

func newAuthCommandTestServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			http.Error(w, fmt.Sprintf("bad auth %q", got), http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/auth/context":
			_, _ = w.Write([]byte(`{"principal":{"actor_type":"api-key","actor_id":"key_test","tenant_id":"tenant_123","scopes":["data.read"],"platform_admin":false}}`))
		case "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
}

func runAuthCLI(args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}
