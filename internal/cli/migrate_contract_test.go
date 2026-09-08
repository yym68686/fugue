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

func TestMigrationScannerNeverReportsArgumentsOrCredentialFiles(t *testing.T) {
	dir := t.TempDir()
	script := "fugue --token 'synthetic-secret-arg' env ls private-app\nfugue app env ls current-app\n\"$FUGUE_BIN\" app release ls private-app\n"
	if err := os.WriteFile(filepath.Join(dir, "deploy.sh"), []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, ".env"), []byte("fugue env ls private-env-value"), 0600)
	result, err := scanCommandMigrations([]string{dir})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	if len(result.Findings) != 2 || !result.Findings[1].ReviewRequired {
		t.Fatal(string(raw))
	}
	for _, secret := range []string{"synthetic-secret-arg", "private-app", "current-app", "private-env-value"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("scanner disclosed %s", secret)
		}
	}
}
func TestRemovedCommandsAndFlagsDoNotMakeHTTPRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("removed command made %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()
	for _, path := range []string{"env ls demo", "apps routes set demo", "project rename project old", "runtime access grant node tenant", "app release ls demo", "admin artifact release artifact --force-publish", "app failover policy set demo --zero-downtime safe"} {
		var out, stderr bytes.Buffer
		args := append([]string{"--base-url", server.URL, "--token", "synthetic-secret", "--json"}, strings.Fields(path)...)
		err := runWithStreams(args, &out, &stderr)
		if ExitCodeForError(err) != 2 {
			t.Errorf("%s: %v", path, err)
		}
		var object map[string]any
		if json.Unmarshal(out.Bytes(), &object) != nil {
			t.Error(out.String())
		}
		if strings.Contains(out.String(), "synthetic-secret") {
			t.Fatal("error disclosed credential")
		}
	}
}
func TestExplicitResourceOutputVersionPreservesLegacyDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/apps" {
			fmt.Fprint(w, `{"apps":[{"id":"app_test","name":"demo"}]}`)
		} else {
			fmt.Fprint(w, `{"releases":[],"traffic":{"stable_weight":100}}`)
		}
	}))
	defer server.Close()
	for _, version := range []string{"legacy", "v1"} {
		var out, stderr bytes.Buffer
		err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "--json", "app", "release", "versions", "demo", "--output-version", version}, &out, &stderr)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		json.Unmarshal(out.Bytes(), &payload)
		if version == "legacy" {
			if payload["releases"] == nil || payload["schema_version"] != nil {
				t.Fatal(payload)
			}
		} else if payload["schema_version"] != "fugue.app-release.v1" || payload["data"] == nil {
			t.Fatal(payload)
		}
	}
}
