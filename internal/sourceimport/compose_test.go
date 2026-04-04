package sourceimport

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestInspectComposeStackFromRepoParsesBuildAndPostgresServices(t *testing.T) {
	repoDir := t.TempDir()
	password := composeFixturePassword()
	if err := os.MkdirAll(filepath.Join(repoDir, "apps", "api"), 0o755); err != nil {
		t.Fatalf("mkdir api dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 3000\n"), 0o644); err != nil {
		t.Fatalf("write root Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "apps", "api", "Dockerfile"), []byte("FROM scratch\nEXPOSE 8000\n"), 0o644); err != nil {
		t.Fatalf("write api Dockerfile: %v", err)
	}
	compose := fmt.Sprintf(`services:
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
      POSTGRES_PASSWORD: %s
  api:
    build: ./apps/api
    environment:
      DATABASE_URL: %s
    depends_on:
      - db
`, password, composeFixturePostgresURL("demo", password, "db", "demo"))
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
	password := composeFixturePassword()
	database := composeFixtureDatabaseName()
	compose := fmt.Sprintf(`services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_DB: %s
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: %s
  redis:
    image: redis:7-alpine
  app:
    image: ghcr.io/ding113/claude-code-hub:latest
    environment:
      DSN: %s
      REDIS_URL: redis://redis:6379
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "23000:3000"
`, database, password, composeFixturePostgresURL("demo", password, "postgres", database))
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
	if redisService.ServiceType != ServiceTypeRedis || !redisService.BackingService {
		t.Fatalf("expected redis to be classified as mirrored redis backing service, got %+v", redisService)
	}
	if redisService.Image != "redis:7-alpine" {
		t.Fatalf("unexpected redis image: %q", redisService.Image)
	}
	if redisService.Published || redisService.InternalPort != 6379 {
		t.Fatalf("unexpected redis exposure: %+v", redisService)
	}

	if postgresService.Kind != ComposeServiceKindPostgres {
		t.Fatalf("expected postgres to stay managed, got %q", postgresService.Kind)
	}
}

func TestInspectComposeStackFromRepoParsesEnvFilesAndIgnoredFields(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 8080\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "api.yaml"), []byte("providers: []\n"), 0o644); err != nil {
		t.Fatalf("write api yaml: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(repoDir, "data"), 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(repoDir, "env"), 0o755); err != nil {
		t.Fatalf("mkdir env dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "env", "app.env"), []byte("FROM_FILE=true\nSHARED=env-file\n"), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	compose := `services:
  app:
    build: .
    env_file:
      - env/app.env
    environment:
      SHARED: inline
    command:
      - ./server
    labels:
      demo.owner: fugue
    deploy:
      replicas: 2
    volumes:
      - ./api.yaml:/home/api.yaml
      - ./data:/home/data
`
	if err := os.WriteFile(filepath.Join(repoDir, "compose.yaml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	stack, err := inspectComposeStackFromRepo(clonedGitHubRepo{RepoDir: repoDir, DefaultAppName: "demo"})
	if err != nil {
		t.Fatalf("inspect compose stack: %v", err)
	}
	if len(stack.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(stack.Services))
	}
	service := stack.Services[0]
	if len(service.EnvFiles) != 1 || service.EnvFiles[0] != "env/app.env" {
		t.Fatalf("unexpected env files: %#v", service.EnvFiles)
	}
	if service.Environment["FROM_FILE"] != "true" || service.Environment["SHARED"] != "inline" {
		t.Fatalf("expected env_file values to merge with inline env, got %#v", service.Environment)
	}
	if service.PersistentStorage == nil || len(service.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected persistent storage mounts, got %+v", service.PersistentStorage)
	}
	fileMounts := 0
	dirMounts := 0
	for _, mount := range service.PersistentStorage.Mounts {
		switch mount.Kind {
		case model.AppPersistentStorageMountKindFile:
			fileMounts++
		case model.AppPersistentStorageMountKindDirectory:
			dirMounts++
		}
	}
	if fileMounts != 1 || dirMounts != 1 {
		t.Fatalf("expected one file and one directory mount, got %+v", service.PersistentStorage.Mounts)
	}
	for _, field := range service.IgnoredFields {
		if field == "volumes" {
			t.Fatalf("expected supported bind mounts not to remain in ignored fields, got %#v", service.IgnoredFields)
		}
	}
	if len(service.IgnoredFields) == 0 {
		t.Fatalf("expected ignored field report, got %+v", service)
	}
}

func TestInspectComposeStackFromRepoIgnoresMissingEnvFilesDuringImport(t *testing.T) {
	repoDir := t.TempDir()
	user := composeFixtureUser()
	password := composeFixturePassword()
	database := composeFixtureDatabaseName()
	compose := fmt.Sprintf(`services:
  postgres:
    image: postgres:18
    env_file:
      - ./.env
    environment:
      POSTGRES_USER: ${DB_USER:-%s}
      POSTGRES_PASSWORD: ${DB_PASSWORD:-%s}
      POSTGRES_DB: ${DB_NAME:-%s}
  app:
    image: ghcr.io/ding113/claude-code-hub:latest
    env_file:
      - ./.env
    environment:
      DSN: postgresql://${DB_USER:-%s}:${DB_PASSWORD:-%s}@postgres:5432/${DB_NAME:-%s}
      REDIS_URL: redis://redis:6379
    depends_on:
      - postgres
    ports:
      - "23000:3000"
`, user, password, database, user, password, database)
	if err := os.WriteFile(filepath.Join(repoDir, "docker-compose.yaml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	stack, err := inspectComposeStackFromRepo(clonedGitHubRepo{RepoDir: repoDir, DefaultAppName: "demo"})
	if err != nil {
		t.Fatalf("inspect compose stack: %v", err)
	}
	if len(stack.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(stack.Services))
	}

	var appService, postgresService ComposeService
	for _, service := range stack.Services {
		switch service.Name {
		case "app":
			appService = service
		case "postgres":
			postgresService = service
		}
	}

	if len(appService.EnvFiles) != 1 || appService.EnvFiles[0] != ".env" {
		t.Fatalf("unexpected app env files: %#v", appService.EnvFiles)
	}
	if appService.Environment["DSN"] != composeFixturePostgresURL(user, password, "postgres", database) {
		t.Fatalf("unexpected app DSN: %#v", appService.Environment)
	}
	if !hasComposeInference(appService.InferenceReport, InferenceLevelWarning, "env_file", ".env") {
		t.Fatalf("expected app env_file warning, got %#v", appService.InferenceReport)
	}

	if len(postgresService.EnvFiles) != 1 || postgresService.EnvFiles[0] != ".env" {
		t.Fatalf("unexpected postgres env files: %#v", postgresService.EnvFiles)
	}
	if postgresService.Environment["POSTGRES_PASSWORD"] != password {
		t.Fatalf("unexpected postgres env: %#v", postgresService.Environment)
	}
	if !hasComposeInference(postgresService.InferenceReport, InferenceLevelWarning, "env_file", ".env") {
		t.Fatalf("expected postgres env_file warning, got %#v", postgresService.InferenceReport)
	}
}

func TestInspectComposeStackFromRepoSupportsOptionalEnvFileEntries(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "env"), 0o755); err != nil {
		t.Fatalf("mkdir env dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "env", "app.env"), []byte("FROM_FILE=true\n"), 0o644); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 8080\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	compose := `services:
  app:
    build: .
    env_file:
      - path: ./.env
        required: false
      - path: ./env/app.env
    environment:
      SHARED: inline
`
	if err := os.WriteFile(filepath.Join(repoDir, "compose.yaml"), []byte(compose), 0o644); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	stack, err := inspectComposeStackFromRepo(clonedGitHubRepo{RepoDir: repoDir, DefaultAppName: "demo"})
	if err != nil {
		t.Fatalf("inspect compose stack: %v", err)
	}
	if len(stack.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(stack.Services))
	}

	service := stack.Services[0]
	if len(service.EnvFiles) != 2 || service.EnvFiles[0] != ".env" || service.EnvFiles[1] != "env/app.env" {
		t.Fatalf("unexpected env files: %#v", service.EnvFiles)
	}
	if service.Environment["FROM_FILE"] != "true" || service.Environment["SHARED"] != "inline" {
		t.Fatalf("unexpected environment: %#v", service.Environment)
	}
	if hasComposeInference(service.InferenceReport, InferenceLevelWarning, "env_file", ".env") {
		t.Fatalf("expected optional missing env_file to stay quiet, got %#v", service.InferenceReport)
	}
}

func TestInspectComposeStackFromRepoInfersMissingBindMountsAsEmptyPersistentStorage(t *testing.T) {
	repoDir := t.TempDir()
	compose := `services:
  app:
    image: ghcr.io/example/app:latest
    volumes:
      - ./api.yaml:/home/api.yaml
      - ./data:/home/data
      - ./Dockerfile:/workspace/Dockerfile
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
	if len(stack.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(stack.Services))
	}

	service := stack.Services[0]
	if service.PersistentStorage == nil || len(service.PersistentStorage.Mounts) != 3 {
		t.Fatalf("expected inferred persistent storage mounts, got %+v", service.PersistentStorage)
	}

	fileMount := service.PersistentStorage.Mounts[0]
	if fileMount.Kind != model.AppPersistentStorageMountKindFile || fileMount.Path != "/home/api.yaml" {
		t.Fatalf("unexpected inferred file mount: %+v", fileMount)
	}
	if fileMount.SeedContent != "" {
		t.Fatalf("expected missing file mount to be empty, got %q", fileMount.SeedContent)
	}
	if fileMount.Mode != defaultComposePersistentFileMode {
		t.Fatalf("expected missing file mount mode %o, got %o", defaultComposePersistentFileMode, fileMount.Mode)
	}

	dirMount := service.PersistentStorage.Mounts[1]
	if dirMount.Kind != model.AppPersistentStorageMountKindDirectory || dirMount.Path != "/home/data" {
		t.Fatalf("unexpected inferred directory mount: %+v", dirMount)
	}
	if dirMount.Mode != defaultComposePersistentDirectoryMode {
		t.Fatalf("expected missing directory mount mode %o, got %o", defaultComposePersistentDirectoryMode, dirMount.Mode)
	}

	extensionlessFileMount := service.PersistentStorage.Mounts[2]
	if extensionlessFileMount.Kind != model.AppPersistentStorageMountKindFile || extensionlessFileMount.Path != "/workspace/Dockerfile" {
		t.Fatalf("unexpected inferred extensionless file mount: %+v", extensionlessFileMount)
	}
	if len(service.PersistentStorageSeedFiles) != 2 {
		t.Fatalf("expected editable seed files for the missing file mounts, got %+v", service.PersistentStorageSeedFiles)
	}
	if service.PersistentStorageSeedFiles[0].Path != "/home/api.yaml" || service.PersistentStorageSeedFiles[0].Mode != defaultComposePersistentFileMode || service.PersistentStorageSeedFiles[0].SeedContent != "" {
		t.Fatalf("unexpected first editable seed file: %+v", service.PersistentStorageSeedFiles[0])
	}
	if service.PersistentStorageSeedFiles[1].Path != "/workspace/Dockerfile" || service.PersistentStorageSeedFiles[1].Mode != defaultComposePersistentFileMode || service.PersistentStorageSeedFiles[1].SeedContent != "" {
		t.Fatalf("unexpected second editable seed file: %+v", service.PersistentStorageSeedFiles[1])
	}

	for _, field := range service.IgnoredFields {
		if field == "volumes" {
			t.Fatalf("expected inferred bind mounts not to remain in ignored fields, got %#v", service.IgnoredFields)
		}
	}

	reportMessages := make([]string, 0, len(service.InferenceReport))
	for _, inference := range service.InferenceReport {
		reportMessages = append(reportMessages, inference.Message)
	}
	reportText := strings.Join(reportMessages, "\n")
	for _, expected := range []string{
		`inferred missing repository bind mount "./api.yaml" -> "/home/api.yaml" as empty persistent file storage because the source path does not exist in the repository`,
		`inferred missing repository bind mount "./data" -> "/home/data" as empty persistent directory storage because the source path does not exist in the repository`,
		`inferred missing repository bind mount "./Dockerfile" -> "/workspace/Dockerfile" as empty persistent file storage because the source path does not exist in the repository`,
	} {
		if !strings.Contains(reportText, expected) {
			t.Fatalf("expected inference report to contain %q, got %s", expected, reportText)
		}
	}
}

func hasComposeInference(report []TopologyInference, level, category, contains string) bool {
	for _, inference := range report {
		if inference.Level != level || inference.Category != category {
			continue
		}
		if contains == "" || strings.Contains(inference.Message, contains) {
			return true
		}
	}
	return false
}

func composeFixtureUser() string {
	return strings.Join([]string{"fixture", "user"}, "_")
}

func composeFixturePassword() string {
	return strings.Join([]string{"fixture", "pass"}, "_")
}

func composeFixtureDatabaseName() string {
	return strings.Join([]string{"fixture", "db"}, "_")
}

func composeFixturePostgresURL(user, password, host, database string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:5432/%s", user, password, host, database)
}
