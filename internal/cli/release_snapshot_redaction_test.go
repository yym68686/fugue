package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseSnapshotsRedactedInCLIOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_example","name":"demo","tenant_id":"tenant_example","spec":{"replicas":1}}]}`))
		case "/v1/apps/app_example/releases", "/v1/apps/app_example/traffic":
			_, _ = w.Write([]byte(`{"app_id":"app_example","releases":[{"id":"release_example","role":"stable","status":"serving","resolved_image_ref":"registry.example/image:stable","spec_snapshot":{"env":{"API_KEY":"synthetic-env-secret"},"postgres":{"password":"synthetic-db-secret"}}}]}`))
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"versions", []string{"app", "release", "versions", "demo"}},
		{"version", []string{"app", "release", "version", "demo", "release_example"}},
		{"versions-v1", []string{"app", "release", "versions", "demo", "--output-version", "v1"}},
		{"traffic", []string{"app", "traffic", "show", "demo"}},
	} {
		for _, raw := range []bool{false, true} {
			name := tc.name
			if raw {
				name += "-explicit-raw"
			}
			t.Run(name, func(t *testing.T) {
				outputFile := filepath.Join(t.TempDir(), "output.json")
				args := []string{"--base-url", server.URL, "--token", "test-token", "--json", "--output-file", outputFile}
				if raw {
					args = append(args, "--redact=false", "--confirm-raw-output")
				}
				args = append(args, tc.args...)
				var stdout, stderr bytes.Buffer
				if err := runWithStreams(args, &stdout, &stderr); err != nil {
					t.Fatalf("run: %v stderr=%s", err, &stderr)
				}
				saved, err := os.ReadFile(outputFile)
				if err != nil {
					t.Fatal(err)
				}
				for _, output := range []string{stdout.String(), string(saved)} {
					for _, secret := range []string{"synthetic-env-secret", "synthetic-db-secret"} {
						if strings.Contains(output, secret) != raw {
							t.Fatalf("secret visibility does not match explicit raw opt-in: raw=%t", raw)
						}
					}
					if !strings.Contains(output, "registry.example/image:stable") || (!raw && !strings.Contains(output, redactedSecretValue)) {
						t.Fatal("release metadata or redaction marker missing")
					}
				}
			})
		}
	}
}
