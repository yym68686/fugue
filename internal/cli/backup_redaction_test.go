package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAppBackupStatusJSONRedactsAppSecretsFromOlderServers(t *testing.T) {
	t.Parallel()

	const (
		envSecret      = "must-not-leak-env-secret"
		postgresSecret = "must-not-leak-postgres-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"replicas":1},"status":{"phase":"ready"},"created_at":"2026-07-29T00:00:00Z","updated_at":"2026-07-29T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/backups/status":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"replicas":1,"env":{"API_KEY":"` + envSecret + `"},"postgres":{"password":"` + postgresSecret + `"}},"status":{"phase":"ready"},"created_at":"2026-07-29T00:00:00Z","updated_at":"2026-07-29T00:00:00Z"},"policies":[],"artifacts":[],"posture":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"app", "backup", "status", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app backup status: %v stderr=%s", err, stderr.String())
	}
	output := stdout.String()
	for _, leaked := range []string{envSecret, postgresSecret} {
		if strings.Contains(output, leaked) {
			t.Fatalf("default JSON output leaked %q: %s", leaked, output)
		}
	}
	if strings.Count(output, redactedSecretValue) < 2 {
		t.Fatalf("expected env and postgres secrets to be redacted, got %s", output)
	}
}
