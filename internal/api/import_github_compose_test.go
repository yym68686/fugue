package api

import (
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
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

func TestBuildQueuedImageSourcePreservesComposeMetadata(t *testing.T) {
	source, err := buildQueuedImageSource("redis:7-alpine", "redis", "redis")
	if err != nil {
		t.Fatalf("build queued image source: %v", err)
	}
	if source.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected image source type %q, got %q", model.AppSourceTypeDockerImage, source.Type)
	}
	if source.ImageRef != "redis:7-alpine" {
		t.Fatalf("expected image ref redis:7-alpine, got %q", source.ImageRef)
	}
	if source.ImageNameSuffix != "redis" {
		t.Fatalf("expected image suffix redis, got %q", source.ImageNameSuffix)
	}
	if source.ComposeService != "redis" {
		t.Fatalf("expected compose service redis, got %q", source.ComposeService)
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

func TestImportResolvedGitHubTopologySupportsImageBackedComposeServices(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Compose Import Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:    "apps.example.com",
		RegistryPushBase: "registry.internal.example",
	})

	result, err := server.importResolvedGitHubTopology(
		model.Principal{ActorType: model.ActorTypeAPIKey, ActorID: "key"},
		tenant.ID,
		importGitHubRequest{
			ProjectID:      project.ID,
			RepoURL:        "https://github.com/ding113/claude-code-hub",
			RepoVisibility: "public",
		},
		"runtime_managed_shared",
		1,
		"Imported from GitHub",
		"claude-code-hub",
		[]sourceimport.ComposeService{
			{
				Name:         "app",
				Kind:         sourceimport.ComposeServiceKindApp,
				Image:        "ghcr.io/ding113/claude-code-hub:latest",
				InternalPort: 3000,
				Published:    true,
				Environment: map[string]string{
					"DSN":       "postgresql://postgres:postgres@postgres:5432/claude_code_hub",
					"REDIS_URL": "redis://redis:6379",
				},
				DependsOn: []string{"postgres", "redis"},
			},
			{
				Name:  "postgres",
				Kind:  sourceimport.ComposeServiceKindPostgres,
				Image: "postgres:18",
				Environment: map[string]string{
					"POSTGRES_DB":       "claude_code_hub",
					"POSTGRES_USER":     "postgres",
					"POSTGRES_PASSWORD": "postgres",
				},
			},
			{
				Name:  "redis",
				Kind:  sourceimport.ComposeServiceKindApp,
				Image: "redis:7-alpine",
			},
		},
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("import resolved topology: %v", err)
	}
	if result.PrimaryService != "app" {
		t.Fatalf("expected primary service app, got %q", result.PrimaryService)
	}
	if len(result.Apps) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(result.Apps))
	}
	if len(result.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(result.Operations))
	}

	appsByService := map[string]model.App{}
	for _, app := range result.Apps {
		if app.Source == nil {
			t.Fatalf("expected app %s to keep source metadata", app.Name)
		}
		appsByService[app.Source.ComposeService] = app
	}

	primaryApp, ok := appsByService["app"]
	if !ok {
		t.Fatal("expected primary app compose service metadata")
	}
	if primaryApp.Source.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected primary source type %q, got %q", model.AppSourceTypeDockerImage, primaryApp.Source.Type)
	}
	if primaryApp.Source.ImageRef != "ghcr.io/ding113/claude-code-hub:latest" {
		t.Fatalf("unexpected primary image ref: %q", primaryApp.Source.ImageRef)
	}
	if got := primaryApp.Spec.Env["REDIS_URL"]; got != "redis://claude-code-hub-redis:6379" {
		t.Fatalf("expected REDIS_URL to target mirrored redis app, got %q", got)
	}
	if got := primaryApp.Spec.Env["DSN"]; got != "postgresql://claude_code_hub:postgres@claude-code-hub-postgres-postgres-rw:5432/claude_code_hub" {
		t.Fatalf("expected DSN rewrite to managed postgres host, got %q", got)
	}
	if primaryApp.Route == nil || primaryApp.Route.ServicePort != 3000 {
		t.Fatalf("expected primary route to keep service port 3000, got %+v", primaryApp.Route)
	}

	redisApp, ok := appsByService["redis"]
	if !ok {
		t.Fatal("expected redis app compose service metadata")
	}
	if redisApp.Source.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected redis source type %q, got %q", model.AppSourceTypeDockerImage, redisApp.Source.Type)
	}
	if redisApp.Route != nil && redisApp.Route.ServicePort != 0 {
		t.Fatalf("expected redis to remain internal-only before image port detection, got %+v", redisApp.Route)
	}
	if redisApp.Spec.Ports != nil {
		t.Fatalf("expected redis spec ports to be detected at import time, got %#v", redisApp.Spec.Ports)
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

func TestComposePostgresSpecNormalizesReservedComposeEnvUser(t *testing.T) {
	spec, err := composePostgresSpec(sourceimport.ComposeService{
		Name:  "db",
		Kind:  sourceimport.ComposeServiceKindPostgres,
		Image: "postgres:17.6-alpine",
		Environment: map[string]string{
			"POSTGRES_DB":       "claude_code_hub",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
		},
	}, "claude-code-hub")
	if err != nil {
		t.Fatalf("compose postgres spec: %v", err)
	}
	if spec.User != "claude_code_hub" {
		t.Fatalf("expected reserved compose user to normalize to claude_code_hub, got %q", spec.User)
	}
	if spec.Password != "postgres" {
		t.Fatalf("expected compose password to be preserved, got %q", spec.Password)
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
