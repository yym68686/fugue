package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAppSSHConfigPrintsOpenSSHBlock(t *testing.T) {
	t.Parallel()

	server := newCLISSHTestServer(t, model.AppSSHStatus{
		Supported:  true,
		Ready:      true,
		Hostname:   "ssh.fugue.pro",
		PublicPort: 23417,
		TargetPort: 22,
		User:       "fugue",
	})
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "test-token",
		"app", "ssh", "config", "agent", "--identity", "~/.ssh/id_ed25519",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app ssh config: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"Host fugue-agent",
		"HostName ssh.fugue.pro",
		"Port 23417",
		"User fugue",
		"IdentityFile ~/.ssh/id_ed25519",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected config output to contain %q, got %q", want, out)
		}
	}
}

func TestAppSSHConnectReportsMissingLocalSSHBinary(t *testing.T) {
	server := newCLISSHTestServer(t, model.AppSSHStatus{
		Supported:  true,
		Ready:      true,
		Hostname:   "ssh.fugue.pro",
		PublicPort: 23417,
		TargetPort: 22,
		User:       "fugue",
	})
	defer server.Close()
	t.Setenv("PATH", t.TempDir())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "test-token",
		"app", "ssh", "agent",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected missing ssh binary error")
	}
	if !strings.Contains(err.Error(), "local ssh binary not found") {
		t.Fatalf("expected missing ssh binary error, got %v", err)
	}
}

func TestAppSSHConfigRejectsUnsupportedStatus(t *testing.T) {
	t.Parallel()

	server := newCLISSHTestServer(t, model.AppSSHStatus{
		Supported: false,
		Ready:     false,
		Message:   "external-owned runtimes do not support native ssh routes yet",
	})
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "test-token",
		"app", "ssh", "config", "agent",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected unsupported app SSH error")
	}
	if !strings.Contains(err.Error(), "app SSH is unsupported") {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func TestSSHKeyListRendersTable(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/ssh/keys" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ssh_keys": []model.SSHKey{
				{
					ID:          "sshkey_123",
					Label:       "laptop",
					Fingerprint: "SHA256:test",
					Status:      model.SSHKeyStatusActive,
					CreatedAt:   createdAt,
				},
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "test-token",
		"ssh-key", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run ssh-key ls: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "KEY") || !strings.Contains(out, "laptop") || !strings.Contains(out, "SHA256:test") {
		t.Fatalf("unexpected ssh-key table output: %q", out)
	}
}

func newCLISSHTestServer(t *testing.T, status model.AppSSHStatus) *httptest.Server {
	t.Helper()
	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Name:      "agent",
		Spec: model.AppSpec{
			Image:    "ghcr.io/example/agent:ssh",
			Replicas: 1,
		},
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/ssh":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"app": app,
				"ssh": status,
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}
