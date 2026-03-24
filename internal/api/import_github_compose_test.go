package api

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestRewriteComposeEnvironmentRewritesInternalServiceHosts(t *testing.T) {
	env := map[string]string{
		"API_BASE_URL": "http://api:8000/v1",
		"DATABASE_URL": "postgresql://demo:secret@db:5432/demo",
		"DB_HOST":      "db",
	}

	got := rewriteComposeEnvironment(env, map[string]string{
		"api": "uni-api-web-api",
		"db":  "uni-api-web-api-db-postgres",
	})

	if got["API_BASE_URL"] != "http://uni-api-web-api:8000/v1" {
		t.Fatalf("unexpected API_BASE_URL rewrite: %q", got["API_BASE_URL"])
	}
	if got["DATABASE_URL"] != "postgresql://demo:secret@uni-api-web-api-db-postgres:5432/demo" {
		t.Fatalf("unexpected DATABASE_URL rewrite: %q", got["DATABASE_URL"])
	}
	if got["DB_HOST"] != "uni-api-web-api-db-postgres" {
		t.Fatalf("unexpected DB_HOST rewrite: %q", got["DB_HOST"])
	}
}

func TestBuildQueuedGitHubSourcePreservesComposeMetadata(t *testing.T) {
	source, err := buildQueuedGitHubSource(
		"https://github.com/example/demo",
		"main",
		"",
		"apps/api/Dockerfile",
		"apps/api",
		model.AppBuildStrategyDockerfile,
		"",
		"api",
		"api",
	)
	if err != nil {
		t.Fatalf("build queued source: %v", err)
	}
	if source.ImageNameSuffix != "api" {
		t.Fatalf("expected image suffix api, got %q", source.ImageNameSuffix)
	}
	if source.ComposeService != "api" {
		t.Fatalf("expected compose service api, got %q", source.ComposeService)
	}
}

func TestBuildImportedAppSpecAllowsGenericPostgres(t *testing.T) {
	server := &Server{}
	spec, err := server.buildImportedAppSpec(
		"",
		model.AppBuildStrategyDockerfile,
		"demo-api",
		"",
		"runtime_managed_shared",
		1,
		8000,
		"",
		nil,
		&model.AppPostgresSpec{
			Image:    "postgres:17.6-alpine",
			Database: "demo",
			User:     "demo",
		},
		map[string]string{"DATABASE_URL": "postgresql://demo:secret@demo-api-db-postgres:5432/demo"},
	)
	if err != nil {
		t.Fatalf("build imported app spec: %v", err)
	}
	if spec.Postgres == nil {
		t.Fatal("expected postgres spec to be preserved")
	}
	if spec.Postgres.ServiceName != "demo-api-postgres" {
		t.Fatalf("unexpected postgres service name: %q", spec.Postgres.ServiceName)
	}
	if spec.Postgres.Password == "" {
		t.Fatal("expected postgres password to be generated")
	}
	if spec.Env["DATABASE_URL"] == "" {
		t.Fatalf("expected env to be preserved, got %v", spec.Env)
	}
}

func TestPickPrimaryComposeServicePrefersPublishedWeb(t *testing.T) {
	primary := pickPrimaryComposeService([]sourceimport.ComposeService{
		{Name: "api", Kind: sourceimport.ComposeServiceKindApp, Published: true, InternalPort: 8000},
		{Name: "web", Kind: sourceimport.ComposeServiceKindApp, Published: true, InternalPort: 3000},
	})
	if primary.Name != "web" {
		t.Fatalf("expected web to be selected as primary, got %q", primary.Name)
	}
}
