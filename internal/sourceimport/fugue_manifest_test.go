package sourceimport

import (
	"os"
	"path/filepath"
	"strings"
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

func TestInspectFugueManifestAllowsSinglePublicServiceWithoutPrimary(t *testing.T) {
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
  worker:
    build:
      context: .
      dockerfile: Dockerfile
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
	if parsed.PrimaryService != "web" {
		t.Fatalf("expected inferred primary service web, got %q", parsed.PrimaryService)
	}
}

func TestInspectFugueManifestSupportsBackgroundWorkers(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
primary_service: web
services:
  web:
    public: true
    build:
      context: .
      dockerfile: Dockerfile
  worker:
    network_mode: background
    build:
      context: .
      dockerfile: Dockerfile
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

	var workerService ComposeService
	for _, service := range parsed.Services {
		if service.Name == "worker" {
			workerService = service
			break
		}
	}
	if workerService.Name == "" {
		t.Fatal("expected worker service to be present")
	}
	if workerService.NetworkMode != "background" {
		t.Fatalf("expected worker network mode background, got %q", workerService.NetworkMode)
	}
	if workerService.InternalPort != 0 {
		t.Fatalf("expected worker internal port to be cleared for background mode, got %d", workerService.InternalPort)
	}
}

func TestInspectFugueManifestRejectsPublicBackgroundService(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
services:
  web:
    public: true
    network_mode: background
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
	if err == nil || !strings.Contains(err.Error(), "public=true") {
		t.Fatalf("expected public/background conflict error, got %v", err)
	}
}

func TestInspectFugueManifestParsesTemplateMetadata(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "apps", "web"), 0o755); err != nil {
		t.Fatalf("mkdir web dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "apps", "web", "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
primary_service: web
template:
  name: Starter storefront
  slug: starter-storefront
  description: Deploy a ready-made storefront.
  docs_url: https://docs.example.com/storefront
  demo_url: https://demo.example.com/storefront
  default_runtime: runtime_edge_hk
  source_mode: github
  variables:
    - key: NEXT_PUBLIC_API_BASE_URL
      label: API base URL
      description: Public API endpoint.
      default: https://api.example.com
      required: true
    - key: SESSION_SECRET
      label: Session secret
      description: Used to sign cookies.
      generate: password
      secret: true
services:
  web:
    public: true
    build:
      strategy: dockerfile
      context: apps/web
      dockerfile: Dockerfile
`
	if err := os.WriteFile(filepath.Join(repoDir, "fugue.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write fugue manifest: %v", err)
	}

	parsed, err := inspectFugueManifestFromRepo(clonedGitHubRepo{
		RepoOwner:      "example",
		RepoName:       "storefront",
		RepoDir:        repoDir,
		Branch:         "main",
		CommitSHA:      "abcdef123456",
		DefaultAppName: "storefront",
	})
	if err != nil {
		t.Fatalf("inspect fugue manifest: %v", err)
	}
	if parsed.Template == nil {
		t.Fatal("expected template metadata to be parsed")
	}
	if parsed.Template.Slug != "starter-storefront" {
		t.Fatalf("unexpected template slug: %q", parsed.Template.Slug)
	}
	if parsed.Template.Name != "Starter storefront" {
		t.Fatalf("unexpected template name: %q", parsed.Template.Name)
	}
	if parsed.Template.DefaultRuntime != "runtime_edge_hk" {
		t.Fatalf("unexpected default runtime: %q", parsed.Template.DefaultRuntime)
	}
	if parsed.Template.SourceMode != "github" {
		t.Fatalf("unexpected source mode: %q", parsed.Template.SourceMode)
	}
	if len(parsed.Template.Variables) != 2 {
		t.Fatalf("expected 2 template variables, got %d", len(parsed.Template.Variables))
	}
	if parsed.Template.Variables[0].Key != "NEXT_PUBLIC_API_BASE_URL" || parsed.Template.Variables[0].DefaultValue != "https://api.example.com" || !parsed.Template.Variables[0].Required {
		t.Fatalf("unexpected first template variable: %+v", parsed.Template.Variables[0])
	}
	if parsed.Template.Variables[1].Key != "SESSION_SECRET" || parsed.Template.Variables[1].Generate != "password" || !parsed.Template.Variables[1].Secret {
		t.Fatalf("unexpected second template variable: %+v", parsed.Template.Variables[1])
	}
}

func TestInspectFugueManifestParsesBackingServicesAndBindings(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
primary_service: api
services:
  api:
    public: true
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
    bindings:
      - db
      - cache
backing_services:
  db:
    type: postgres
    image: postgres:17.6-alpine
    owner_service: api
  cache:
    service_type: redis
    image: redis:7-alpine
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
	if len(parsed.Services) != 3 {
		t.Fatalf("expected 3 services, got %d", len(parsed.Services))
	}

	var apiService, dbService, cacheService ComposeService
	for _, service := range parsed.Services {
		switch service.Name {
		case "api":
			apiService = service
		case "db":
			dbService = service
		case "cache":
			cacheService = service
		}
	}

	if len(apiService.Bindings) != 2 {
		t.Fatalf("expected explicit api bindings, got %+v", apiService.Bindings)
	}
	if dbService.Kind != ComposeServiceKindPostgres || !dbService.BackingService || dbService.OwnerService != "api" {
		t.Fatalf("unexpected db service: %+v", dbService)
	}
	if cacheService.Kind != ComposeServiceKindApp || cacheService.ServiceType != ServiceTypeRedis || !cacheService.BackingService {
		t.Fatalf("unexpected cache service: %+v", cacheService)
	}
	if len(parsed.InferenceReport) == 0 {
		t.Fatalf("expected inference report, got %+v", parsed)
	}
}

func TestInspectFugueManifestMergesPersistentStorageSettings(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "state"), 0o755); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 8080\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	manifest := `version: 1
services:
  gateway:
    public: true
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./state:/data/argus
    persistent_storage:
      storage_size: 512Mi
      storage_class_name: fast-rwo
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
	if len(parsed.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(parsed.Services))
	}
	service := parsed.Services[0]
	if service.PersistentStorage == nil {
		t.Fatal("expected persistent storage to be present")
	}
	if got := service.PersistentStorage.StorageSize; got != "512Mi" {
		t.Fatalf("expected storage size 512Mi, got %q", got)
	}
	if got := service.PersistentStorage.StorageClassName; got != "fast-rwo" {
		t.Fatalf("expected storage class fast-rwo, got %q", got)
	}
	if len(service.PersistentStorage.Mounts) != 1 || service.PersistentStorage.Mounts[0].Path != "/data/argus" {
		t.Fatalf("expected imported volume mount to remain attached, got %+v", service.PersistentStorage.Mounts)
	}
}
