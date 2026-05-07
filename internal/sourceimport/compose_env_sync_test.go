package sourceimport

import (
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestSuggestComposeServiceEnvRewritesCurrentTopologyHosts(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "apps", "api"), 0o755); err != nil {
		t.Fatalf("mkdir api dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write root Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "apps", "api", "Dockerfile"), []byte("FROM scratch\nEXPOSE 8000\n"), 0o644); err != nil {
		t.Fatalf("write api Dockerfile: %v", err)
	}
	compose := `services:
  worker:
    build: .
    environment:
      API_BASE_URL: http://api:8000/v1
      DATABASE_URL: postgresql://demo:secret@db:5432/demo
      ARGUS_FUGUE_RUNTIME_COMPOSE_SERVICE: api
    depends_on:
      - api
      - db
  api:
    build: ./apps/api
    environment:
      DATABASE_URL: postgresql://demo:placeholder@db:5432/demo
      DB_HOST: db
      DATABASE_HOST: db
      DATABASE_PORT: 15432
      DATABASE_USER: placeholder
      DATABASE_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}
      DATABASE_DBNAME: placeholder
    depends_on:
      - db
  db:
    image: postgres:17.6-alpine
`
	if err := os.WriteFile(filepath.Join(repoDir, "docker-compose.yml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	services, err := inspectImportableServicesFromRepo(clonedGitHubRepo{RepoDir: repoDir, DefaultAppName: "demo"})
	if err != nil {
		t.Fatalf("inspect importable services: %v", err)
	}

	appHosts := map[string]string{
		"api":    "demo-api",
		"worker": "demo-worker",
	}
	managedPostgresByOwner := map[string]model.AppPostgresSpec{
		"api": {
			ServiceName: "demo-api-postgres",
			Database:    "demo",
			User:        "demo",
			Password:    "secret-pass",
		},
	}

	workerEnv, err := suggestComposeServiceEnv(services, "worker", appHosts, managedPostgresByOwner)
	if err != nil {
		t.Fatalf("suggest worker env: %v", err)
	}
	if got := workerEnv["API_BASE_URL"]; got != "http://demo-api:8000/v1" {
		t.Fatalf("expected worker API_BASE_URL rewrite, got %q", got)
	}
	if got := workerEnv["DATABASE_URL"]; got != "postgresql://demo:secret@demo-api-postgres-rw:5432/demo" {
		t.Fatalf("expected worker DATABASE_URL host rewrite, got %q", got)
	}
	if got := workerEnv["ARGUS_FUGUE_RUNTIME_COMPOSE_SERVICE"]; got != "api" {
		t.Fatalf("expected logical compose service selector to be preserved, got %q", got)
	}

	apiEnv, err := suggestComposeServiceEnv(services, "api", appHosts, managedPostgresByOwner)
	if err != nil {
		t.Fatalf("suggest api env: %v", err)
	}
	if got := apiEnv["DB_HOST"]; got != "demo-api-postgres-rw" {
		t.Fatalf("expected api DB_HOST rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_HOST"]; got != "demo-api-postgres-rw" {
		t.Fatalf("expected api DATABASE_HOST rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_PORT"]; got != "5432" {
		t.Fatalf("expected api DATABASE_PORT rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_USER"]; got != "demo" {
		t.Fatalf("expected api DATABASE_USER rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_PASSWORD"]; got != "secret-pass" {
		t.Fatalf("expected api DATABASE_PASSWORD rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_DBNAME"]; got != "demo" {
		t.Fatalf("expected api DATABASE_DBNAME rewrite, got %q", got)
	}
	if got := apiEnv["DATABASE_URL"]; got != "postgresql://demo:secret-pass@demo-api-postgres-rw:5432/demo" {
		t.Fatalf("expected api DATABASE_URL managed postgres rewrite, got %q", got)
	}
	if err := ValidateNoMissingRequiredComposeEnv("api", apiEnv); err != nil {
		t.Fatalf("expected managed postgres rewrite to satisfy required DATABASE_PASSWORD: %v", err)
	}
}

func TestValidateNoMissingRequiredComposeEnvRejectsUnresolvedRequiredValue(t *testing.T) {
	env := parseComposeEnvironment(map[string]any{
		"API_KEY": "${API_KEY:?API_KEY is required}",
	}, nil)

	err := ValidateNoMissingRequiredComposeEnv("api", env)
	if err == nil {
		t.Fatal("expected missing required env error")
	}
	if got := err.Error(); got != `compose service "api" env "API_KEY" requires API_KEY, but it was not provided` {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestValidateNoMissingRequiredComposeEnvRejectsEmbeddedRequiredValue(t *testing.T) {
	env := parseComposeEnvironment(map[string]any{
		"API_URL": "https://${API_HOST:?API_HOST is required}/v1",
	}, nil)

	err := ValidateNoMissingRequiredComposeEnv("api", env)
	if err == nil {
		t.Fatal("expected missing required env error")
	}
	if got := err.Error(); got != `compose service "api" env "API_URL" requires API_HOST, but it was not provided` {
		t.Fatalf("unexpected error: %q", got)
	}
}
