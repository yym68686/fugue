package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDeploymentEnvWithManifestDefaultUsesTopLevelEnvFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "fugue.yaml"), "version: 1\nenv_file: .env.fugue\nservices: {}\n")
	writeTestFile(t, filepath.Join(dir, ".env"), "APP_ENV=wrong\n")
	writeTestFile(t, filepath.Join(dir, ".env.fugue"), "APP_ENV=fugue\nFUGUE_ONLY=true\n")

	env, path, err := loadDeploymentEnvWithManifestDefault(dir, "", false)
	if err != nil {
		t.Fatalf("load env: %v", err)
	}
	if path != filepath.Join(dir, ".env.fugue") {
		t.Fatalf("expected .env.fugue path, got %q", path)
	}
	if env["APP_ENV"] != "fugue" || env["FUGUE_ONLY"] != "true" {
		t.Fatalf("expected manifest env file values, got %#v", env)
	}
}

func TestLoadDeploymentEnvWithManifestDefaultPrefersExplicitEnvFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "fugue.yaml"), "version: 1\nenv_file: .env.fugue\nservices: {}\n")
	writeTestFile(t, filepath.Join(dir, ".env.fugue"), "APP_ENV=fugue\n")
	writeTestFile(t, filepath.Join(dir, "custom.env"), "APP_ENV=custom\n")

	env, path, err := loadDeploymentEnvWithManifestDefault(dir, "custom.env", true)
	if err != nil {
		t.Fatalf("load env: %v", err)
	}
	if path != filepath.Join(dir, "custom.env") {
		t.Fatalf("expected explicit env path, got %q", path)
	}
	if env["APP_ENV"] != "custom" {
		t.Fatalf("expected explicit env file to win, got %#v", env)
	}
}

func TestLoadDeploymentEnvWithManifestDefaultRejectsNonStringEnvFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "fugue.yaml"), "version: 1\nenv_file:\n  - .env.fugue\nservices: {}\n")

	_, _, err := loadDeploymentEnvWithManifestDefault(dir, "", false)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "top-level env_file") {
		t.Fatalf("expected top-level env_file error, got %v", err)
	}
}

func writeTestFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
