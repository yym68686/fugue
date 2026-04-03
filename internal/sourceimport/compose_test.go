package sourceimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectComposeStackFromRepoParsesBuildAndPostgresServices(t *testing.T) {
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
  web:
    build: .
    environment:
      API_BASE_URL: http://api:8000/v1
    ports:
      - "3000:3000"
  db:
    image: postgres:17.6-alpine
    environment:
      POSTGRES_DB: demo
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: secret
  api:
    build: ./apps/api
    environment:
      DATABASE_URL: postgresql://demo:secret@db:5432/demo
    depends_on:
      - db
`
	if err := os.WriteFile(filepath.Join(repoDir, "docker-compose.yml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	stack, err := inspectComposeStackFromRepo(clonedGitHubRepo{
		RepoOwner:      "example",
		RepoName:       "demo",
		RepoDir:        repoDir,
		Branch:         "main",
		CommitSHA:      "abcdef123456",
		DefaultAppName: "demo",
	})
	if err != nil {
		t.Fatalf("inspect compose stack: %v", err)
	}
	if stack.ComposePath != "docker-compose.yml" {
		t.Fatalf("unexpected compose path: %q", stack.ComposePath)
	}
	if len(stack.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", stack.Warnings)
	}
	if len(stack.Services) != 3 {
		t.Fatalf("expected 3 services, got %d", len(stack.Services))
	}

	var apiService, dbService, webService ComposeService
	for _, service := range stack.Services {
		switch service.Name {
		case "api":
			apiService = service
		case "db":
			dbService = service
		case "web":
			webService = service
		}
	}

	if webService.Kind != ComposeServiceKindApp {
		t.Fatalf("expected web to be an app, got %q", webService.Kind)
	}
	if webService.BuildStrategy != "dockerfile" || webService.DockerfilePath != "Dockerfile" || webService.BuildContextDir != "." {
		t.Fatalf("unexpected web build inputs: %+v", webService)
	}
	if !webService.Published || webService.InternalPort != 3000 {
		t.Fatalf("unexpected web exposure: %+v", webService)
	}

	if apiService.Kind != ComposeServiceKindApp {
		t.Fatalf("expected api to be an app, got %q", apiService.Kind)
	}
	if apiService.DockerfilePath != "apps/api/Dockerfile" || apiService.BuildContextDir != "apps/api" {
		t.Fatalf("unexpected api build inputs: %+v", apiService)
	}
	if apiService.InternalPort != 8000 {
		t.Fatalf("expected api port 8000, got %d", apiService.InternalPort)
	}
	if len(apiService.DependsOn) != 1 || apiService.DependsOn[0] != "db" {
		t.Fatalf("unexpected api depends_on: %v", apiService.DependsOn)
	}

	if dbService.Kind != ComposeServiceKindPostgres {
		t.Fatalf("expected db to be postgres, got %q", dbService.Kind)
	}
	if dbService.Image != "postgres:17.6-alpine" {
		t.Fatalf("unexpected db image: %q", dbService.Image)
	}
	if dbService.Environment["POSTGRES_DB"] != "demo" {
		t.Fatalf("unexpected db env: %v", dbService.Environment)
	}
}

func TestInspectComposeStackFromRepoParsesImageBackedServices(t *testing.T) {
	repoDir := t.TempDir()
	compose := `services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_DB: claude_code_hub
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: secret
  redis:
    image: redis:7-alpine
  app:
    image: ghcr.io/ding113/claude-code-hub:latest
    environment:
      DSN: postgresql://demo:secret@postgres:5432/claude_code_hub
      REDIS_URL: redis://redis:6379
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "23000:3000"
`
	if err := os.WriteFile(filepath.Join(repoDir, "docker-compose.yaml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	stack, err := inspectComposeStackFromRepo(clonedGitHubRepo{
		RepoOwner:      "ding113",
		RepoName:       "claude-code-hub",
		RepoDir:        repoDir,
		Branch:         "main",
		CommitSHA:      "abcdef123456",
		DefaultAppName: "claude-code-hub",
	})
	if err != nil {
		t.Fatalf("inspect compose stack: %v", err)
	}
	if stack.ComposePath != "docker-compose.yaml" {
		t.Fatalf("unexpected compose path: %q", stack.ComposePath)
	}
	if len(stack.Warnings) != 2 {
		t.Fatalf("expected warnings for app and redis image services, got %v", stack.Warnings)
	}
	if len(stack.Services) != 3 {
		t.Fatalf("expected 3 services, got %d", len(stack.Services))
	}

	var appService, redisService, postgresService ComposeService
	for _, service := range stack.Services {
		switch service.Name {
		case "app":
			appService = service
		case "redis":
			redisService = service
		case "postgres":
			postgresService = service
		}
	}

	if appService.Kind != ComposeServiceKindApp {
		t.Fatalf("expected app to be importable, got %q", appService.Kind)
	}
	if appService.Image != "ghcr.io/ding113/claude-code-hub:latest" {
		t.Fatalf("unexpected app image: %q", appService.Image)
	}
	if !appService.Published || appService.InternalPort != 3000 {
		t.Fatalf("unexpected app exposure: %+v", appService)
	}
	if len(appService.DependsOn) != 2 || appService.DependsOn[0] != "postgres" || appService.DependsOn[1] != "redis" {
		t.Fatalf("unexpected app depends_on: %v", appService.DependsOn)
	}

	if redisService.Kind != ComposeServiceKindApp {
		t.Fatalf("expected redis to be importable as app, got %q", redisService.Kind)
	}
	if redisService.Image != "redis:7-alpine" {
		t.Fatalf("unexpected redis image: %q", redisService.Image)
	}
	if redisService.Published || redisService.InternalPort != 0 {
		t.Fatalf("unexpected redis exposure: %+v", redisService)
	}

	if postgresService.Kind != ComposeServiceKindPostgres {
		t.Fatalf("expected postgres to stay managed, got %q", postgresService.Kind)
	}
}
