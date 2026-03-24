package sourceimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectFugueManifestFromRepoParsesExplicitTopology(t *testing.T) {
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
	manifest := `version: 1
primary_service: web
services:
  web:
    public: true
    port: 3000
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
    env:
      API_BASE_URL: http://api:8000/v1
    depends_on:
      - api
  api:
    port: 8000
    build:
      strategy: dockerfile
      context: apps/api
      dockerfile: Dockerfile
    environment:
      DATABASE_URL: postgresql://demo:secret@db:5432/demo
    depends_on:
      - db
  db:
    type: postgres
    image: postgres:17.6-alpine
    database: demo
    user: demo
    password: secret
    service_name: explicit-db
`
	if err := os.WriteFile(filepath.Join(repoDir, "fugue.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write fugue manifest: %v", err)
	}

	parsed, err := inspectFugueManifestFromRepo(clonedGitHubRepo{
		RepoOwner:      "example",
		RepoName:       "demo",
		RepoDir:        repoDir,
		Branch:         "main",
		CommitSHA:      "abcdef123456",
		DefaultAppName: "demo",
	})
	if err != nil {
		t.Fatalf("inspect fugue manifest: %v", err)
	}
	if parsed.ManifestPath != "fugue.yaml" {
		t.Fatalf("unexpected manifest path: %q", parsed.ManifestPath)
	}
	if parsed.PrimaryService != "web" {
		t.Fatalf("unexpected primary service: %q", parsed.PrimaryService)
	}
	if len(parsed.Services) != 3 {
		t.Fatalf("expected 3 services, got %d", len(parsed.Services))
	}

	var apiService, dbService, webService ComposeService
	for _, service := range parsed.Services {
		switch service.Name {
		case "api":
			apiService = service
		case "db":
			dbService = service
		case "web":
			webService = service
		}
	}

	if webService.Kind != ComposeServiceKindApp || !webService.Published {
		t.Fatalf("unexpected web service: %+v", webService)
	}
	if webService.DockerfilePath != "Dockerfile" || webService.BuildContextDir != "." || webService.InternalPort != 3000 {
		t.Fatalf("unexpected web build inputs: %+v", webService)
	}
	if apiService.Kind != ComposeServiceKindApp || apiService.DockerfilePath != "apps/api/Dockerfile" || apiService.BuildContextDir != "apps/api" || apiService.InternalPort != 8000 {
		t.Fatalf("unexpected api service: %+v", apiService)
	}
	if dbService.Kind != ComposeServiceKindPostgres || dbService.Postgres == nil {
		t.Fatalf("unexpected db service: %+v", dbService)
	}
	if dbService.Postgres.ServiceName != "explicit-db" {
		t.Fatalf("unexpected explicit postgres service name: %q", dbService.Postgres.ServiceName)
	}
	if dbService.Postgres.Database != "demo" || dbService.Postgres.User != "demo" || dbService.Postgres.Password != "secret" {
		t.Fatalf("unexpected postgres spec: %+v", *dbService.Postgres)
	}
}

func TestInspectFugueManifestRejectsMultiplePublicServicesWithoutPrimary(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
services:
  web:
    public: true
    build:
      context: .
      dockerfile: Dockerfile
  admin:
    public: true
    build:
      context: .
      dockerfile: Dockerfile
`
	if err := os.WriteFile(filepath.Join(repoDir, "fugue.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write fugue manifest: %v", err)
	}

	_, err := inspectFugueManifestFromRepo(clonedGitHubRepo{
		RepoOwner:      "example",
		RepoName:       "demo",
		RepoDir:        repoDir,
		Branch:         "main",
		CommitSHA:      "abcdef123456",
		DefaultAppName: "demo",
	})
	if err == nil {
		t.Fatal("expected multiple public services to be rejected")
	}
}
