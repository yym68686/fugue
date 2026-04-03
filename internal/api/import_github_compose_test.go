package api

import (
	"strings"
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
		"db":  "uni-api-web-api-db-postgres-rw",
	})

	if got["API_BASE_URL"] != "http://uni-api-web-api:8000/v1" {
		t.Fatalf("unexpected API_BASE_URL rewrite: %q", got["API_BASE_URL"])
	}
	if got["DATABASE_URL"] != "postgresql://demo:secret@uni-api-web-api-db-postgres-rw:5432/demo" {
		t.Fatalf("unexpected DATABASE_URL rewrite: %q", got["DATABASE_URL"])
	}
	if got["DB_HOST"] != "uni-api-web-api-db-postgres-rw" {
		t.Fatalf("unexpected DB_HOST rewrite: %q", got["DB_HOST"])
	}
}

func TestApplyManagedPostgresEnvironmentRewritesGeneratedDatabaseURL(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL": "postgresql+asyncpg://uniapi:@uni-api-web-api-db-postgres:5432/uniapi",
	}

	got := applyManagedPostgresEnvironment(env, model.AppPostgresSpec{
		ServiceName: "uni-api-web-api-db-postgres",
		Database:    "uniapi",
		User:        "uniapi",
		Password:    "secret-pass",
	})

	if got["DATABASE_URL"] != "postgresql+asyncpg://uniapi:secret-pass@uni-api-web-api-db-postgres-rw:5432/uniapi" {
		t.Fatalf("unexpected DATABASE_URL rewrite: %q", got["DATABASE_URL"])
	}
}

func TestApplyManagedPostgresEnvironmentKeepsExternalDatabaseURL(t *testing.T) {
	env := map[string]string{
		"DATABASE_URL": "postgresql+asyncpg://uniapi:secret@db.example.com:5432/uniapi",
	}

	got := applyManagedPostgresEnvironment(env, model.AppPostgresSpec{
		ServiceName: "uni-api-web-api-db-postgres",
		Database:    "uniapi",
		User:        "uniapi",
		Password:    "new-secret",
	})

	if got["DATABASE_URL"] != env["DATABASE_URL"] {
		t.Fatalf("expected external DATABASE_URL to be preserved, got %q", got["DATABASE_URL"])
	}
}

func TestBuildQueuedGitHubSourcePreservesComposeMetadata(t *testing.T) {
	source, err := buildQueuedGitHubSource(
		"https://github.com/example/demo",
		model.AppSourceTypeGitHubPublic,
		"",
		"main",
		"",
		"apps/api/Dockerfile",
		"apps/api",
		model.AppBuildStrategyDockerfile,
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

func TestBuildImportedAppSpecAllowsGenericStatefulInputs(t *testing.T) {
	server := &Server{}
	spec, err := server.buildImportedAppSpec(
		model.AppBuildStrategyDockerfile,
		"demo-api",
		"",
		"runtime_managed_shared",
		1,
		8000,
		"providers: []\n",
		[]model.AppFile{{
			Path:    "/etc/demo.env",
			Content: "DEMO=true\n",
			Secret:  true,
		}},
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
	if spec.Postgres.Image != "" {
		t.Fatalf("expected official postgres image to be stripped, got %q", spec.Postgres.Image)
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
	if len(spec.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(spec.Files))
	}
	if spec.Files[0].Path != defaultImportedConfigPath {
		t.Fatalf("unexpected config file path: %q", spec.Files[0].Path)
	}
	if spec.Files[1].Path != "/etc/demo.env" {
		t.Fatalf("unexpected extra file path: %q", spec.Files[1].Path)
	}
	if len(spec.Ports) != 1 || spec.Ports[0] != 8000 {
		t.Fatalf("unexpected ports: %#v", spec.Ports)
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

func TestResolveTopologyPrimaryServiceUsesPreferredService(t *testing.T) {
	primary, err := resolveTopologyPrimaryService([]sourceimport.ComposeService{
		{Name: "api", Kind: sourceimport.ComposeServiceKindApp, InternalPort: 8000},
		{Name: "web", Kind: sourceimport.ComposeServiceKindApp, InternalPort: 3000},
	}, "api")
	if err != nil {
		t.Fatalf("resolve topology primary service: %v", err)
	}
	if primary.Name != "api" {
		t.Fatalf("expected api to be selected, got %q", primary.Name)
	}
}

func TestResolveTopologyPrimaryServiceFallsBackWhenPreferredIsEmpty(t *testing.T) {
	primary, err := resolveTopologyPrimaryService([]sourceimport.ComposeService{
		{Name: "api", Kind: sourceimport.ComposeServiceKindApp, Published: true, InternalPort: 8000},
		{Name: "web", Kind: sourceimport.ComposeServiceKindApp, Published: true, InternalPort: 3000},
	}, "")
	if err != nil {
		t.Fatalf("resolve topology primary service: %v", err)
	}
	if primary.Name != "web" {
		t.Fatalf("expected auto-selected web, got %q", primary.Name)
	}
}

func TestComposePostgresSpecKeepsExplicitServiceName(t *testing.T) {
	spec, err := composePostgresSpec(sourceimport.ComposeService{
		Name:  "db",
		Kind:  sourceimport.ComposeServiceKindPostgres,
		Image: "postgres:17.6-alpine",
		Postgres: &model.AppPostgresSpec{
			ServiceName: "custom-db",
			Database:    "demo",
			User:        "demo",
			Password:    "secret",
		},
	}, "demo-api")
	if err != nil {
		t.Fatalf("compose postgres spec: %v", err)
	}
	if spec.ServiceName != "custom-db" {
		t.Fatalf("expected explicit service name to be preserved, got %q", spec.ServiceName)
	}
}

func TestComposePostgresSpecDefaultsToAppScopedUser(t *testing.T) {
	spec, err := composePostgresSpec(sourceimport.ComposeService{
		Name:  "db",
		Kind:  sourceimport.ComposeServiceKindPostgres,
		Image: "postgres:17.6-alpine",
	}, "fugue-web")
	if err != nil {
		t.Fatalf("compose postgres spec: %v", err)
	}
	if spec.Image != "" {
		t.Fatalf("expected official postgres image to be stripped, got %q", spec.Image)
	}
	if spec.User != "fugue_web" {
		t.Fatalf("expected app-scoped user fugue_web, got %q", spec.User)
	}
}

func TestComposePostgresSpecKeepsExplicitCNPGImage(t *testing.T) {
	spec, err := composePostgresSpec(sourceimport.ComposeService{
		Name:  "db",
		Kind:  sourceimport.ComposeServiceKindPostgres,
		Image: "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie",
	}, "fugue-web")
	if err != nil {
		t.Fatalf("compose postgres spec: %v", err)
	}
	if spec.Image != "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie" {
		t.Fatalf("expected CNPG image to be preserved, got %q", spec.Image)
	}
}

func TestComposePostgresSpecRejectsReservedCNPGUser(t *testing.T) {
	_, err := composePostgresSpec(sourceimport.ComposeService{
		Name:  "db",
		Kind:  sourceimport.ComposeServiceKindPostgres,
		Image: "postgres:17.6-alpine",
		Postgres: &model.AppPostgresSpec{
			User: "postgres",
		},
	}, "fugue-web")
	if err == nil {
		t.Fatal("expected reserved user error")
	}
	if !strings.Contains(err.Error(), `managed CNPG postgres user "postgres" is reserved`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
