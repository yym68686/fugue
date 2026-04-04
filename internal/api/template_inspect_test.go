package api

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/store"
)

func TestInspectGitHubTemplateReturnsTemplateMetadata(t *testing.T) {
	fixture := newGitHubTemplateFixture(t, map[string]string{
		"fugue.yaml": `version: 1
primary_service: web
template:
  name: Next starter
  slug: next-starter
  description: Ship a ready-made Next.js app.
  source_mode: github
  default_runtime: runtime_edge_hk
  variables:
    - key: NEXT_PUBLIC_SITE_URL
      label: Site URL
      description: Public app URL.
      default: https://example.com
      required: true
services:
  web:
    public: true
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./api.yaml:/workspace/api.yaml
`,
		"Dockerfile": "FROM scratch\nEXPOSE 3000\n",
	})

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Template Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "template-reader", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ImportWorkDir: t.TempDir(),
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/templates/inspect-github", apiKey, map[string]any{
		"repo_url": fixture.repoURL,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Repository struct {
			RepoOwner         string `json:"repo_owner"`
			RepoName          string `json:"repo_name"`
			Branch            string `json:"branch"`
			DefaultAppName    string `json:"default_app_name"`
			CommitCommittedAt string `json:"commit_committed_at"`
		} `json:"repository"`
		FugueManifest *struct {
			ManifestPath   string `json:"manifest_path"`
			PrimaryService string `json:"primary_service"`
			Services       []struct {
				Service                    string `json:"service"`
				InternalPort               int    `json:"internal_port"`
				PersistentStorageSeedFiles []struct {
					Path        string `json:"path"`
					Mode        int32  `json:"mode"`
					SeedContent string `json:"seed_content"`
				} `json:"persistent_storage_seed_files"`
				Published bool `json:"published"`
			} `json:"services"`
		} `json:"fugue_manifest"`
		Template *struct {
			Slug           string `json:"slug"`
			Name           string `json:"name"`
			DefaultRuntime string `json:"default_runtime"`
			SourceMode     string `json:"source_mode"`
			Variables      []struct {
				Key          string `json:"key"`
				DefaultValue string `json:"default_value"`
				Required     bool   `json:"required"`
			} `json:"variables"`
		} `json:"template"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Repository.RepoOwner != "example" || response.Repository.RepoName != "next-template" {
		t.Fatalf("unexpected repository metadata: %+v", response.Repository)
	}
	if response.Repository.Branch != "main" || response.Repository.DefaultAppName != "next-template" {
		t.Fatalf("unexpected repository defaults: %+v", response.Repository)
	}
	if response.Repository.CommitCommittedAt == "" {
		t.Fatalf("expected commit timestamp, got %+v", response.Repository)
	}
	if response.FugueManifest == nil {
		t.Fatal("expected fugue manifest response")
	}
	if response.FugueManifest.ManifestPath != "fugue.yaml" || response.FugueManifest.PrimaryService != "web" {
		t.Fatalf("unexpected fugue manifest: %+v", response.FugueManifest)
	}
	if len(response.FugueManifest.Services) != 1 || response.FugueManifest.Services[0].InternalPort != 3000 || !response.FugueManifest.Services[0].Published {
		t.Fatalf("unexpected manifest services: %+v", response.FugueManifest.Services)
	}
	if len(response.FugueManifest.Services[0].PersistentStorageSeedFiles) != 1 {
		t.Fatalf("expected one editable persistent storage file, got %+v", response.FugueManifest.Services[0].PersistentStorageSeedFiles)
	}
	seedFile := response.FugueManifest.Services[0].PersistentStorageSeedFiles[0]
	if seedFile.Path != "/workspace/api.yaml" || seedFile.Mode != 0o644 || seedFile.SeedContent != "" {
		t.Fatalf("unexpected persistent storage seed file: %+v", seedFile)
	}
	if response.Template == nil {
		t.Fatal("expected template metadata")
	}
	if response.Template.Slug != "next-starter" || response.Template.Name != "Next starter" {
		t.Fatalf("unexpected template metadata: %+v", response.Template)
	}
	if response.Template.DefaultRuntime != "runtime_edge_hk" || response.Template.SourceMode != "github" {
		t.Fatalf("unexpected template runtime or source mode: %+v", response.Template)
	}
	if len(response.Template.Variables) != 1 || response.Template.Variables[0].Key != "NEXT_PUBLIC_SITE_URL" || response.Template.Variables[0].DefaultValue != "https://example.com" || !response.Template.Variables[0].Required {
		t.Fatalf("unexpected template variables: %+v", response.Template.Variables)
	}
}

func TestInspectGitHubTemplateReturnsComposeInspection(t *testing.T) {
	fixture := newGitHubTemplateFixture(t, map[string]string{
		"docker-compose.yml": `services:
  api:
    build:
      context: .
    ports:
      - "3000:3000"
    volumes:
      - ./api.yaml:/home/api.yaml
`,
		"Dockerfile": "FROM scratch\nEXPOSE 3000\n",
	})

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Template Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "template-reader", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ImportWorkDir: t.TempDir(),
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/templates/inspect-github", apiKey, map[string]any{
		"repo_url": fixture.repoURL,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Repository struct {
			Branch            string `json:"branch"`
			CommitCommittedAt string `json:"commit_committed_at"`
			DefaultAppName    string `json:"default_app_name"`
			RepoName          string `json:"repo_name"`
			RepoOwner         string `json:"repo_owner"`
		} `json:"repository"`
		FugueManifest *struct {
			ManifestPath string `json:"manifest_path"`
		} `json:"fugue_manifest"`
		ComposeStack *struct {
			ComposePath    string `json:"compose_path"`
			PrimaryService string `json:"primary_service"`
			Services       []struct {
				Service                    string `json:"service"`
				InternalPort               int    `json:"internal_port"`
				PersistentStorageSeedFiles []struct {
					Path        string `json:"path"`
					Mode        int32  `json:"mode"`
					SeedContent string `json:"seed_content"`
				} `json:"persistent_storage_seed_files"`
				Published bool `json:"published"`
			} `json:"services"`
		} `json:"compose_stack"`
		Template *struct {
			Slug string `json:"slug"`
		} `json:"template"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Repository.RepoOwner != "example" || response.Repository.RepoName != "next-template" {
		t.Fatalf("unexpected repository metadata: %+v", response.Repository)
	}
	if response.Repository.Branch != "main" || response.Repository.DefaultAppName != "next-template" {
		t.Fatalf("unexpected repository defaults: %+v", response.Repository)
	}
	if response.Repository.CommitCommittedAt == "" {
		t.Fatalf("expected commit timestamp, got %+v", response.Repository)
	}
	if response.FugueManifest != nil {
		t.Fatalf("expected fugue manifest to be absent, got %+v", response.FugueManifest)
	}
	if response.ComposeStack == nil {
		t.Fatal("expected compose stack inspection")
	}
	if response.ComposeStack.ComposePath != "docker-compose.yml" || response.ComposeStack.PrimaryService != "api" {
		t.Fatalf("unexpected compose stack metadata: %+v", response.ComposeStack)
	}
	if len(response.ComposeStack.Services) != 1 || response.ComposeStack.Services[0].Service != "api" || response.ComposeStack.Services[0].InternalPort != 3000 || !response.ComposeStack.Services[0].Published {
		t.Fatalf("unexpected compose stack services: %+v", response.ComposeStack.Services)
	}
	if len(response.ComposeStack.Services[0].PersistentStorageSeedFiles) != 1 {
		t.Fatalf("expected one editable persistent storage file, got %+v", response.ComposeStack.Services[0].PersistentStorageSeedFiles)
	}
	seedFile := response.ComposeStack.Services[0].PersistentStorageSeedFiles[0]
	if seedFile.Path != "/home/api.yaml" || seedFile.Mode != 0o644 || seedFile.SeedContent != "" {
		t.Fatalf("unexpected compose persistent storage seed file: %+v", seedFile)
	}
	if response.Template != nil {
		t.Fatalf("expected template metadata to be absent, got %+v", response.Template)
	}
}

type gitHubTemplateFixture struct {
	repoURL string
}

func newGitHubTemplateFixture(t *testing.T, files map[string]string) gitHubTemplateFixture {
	t.Helper()

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(homeDir, ".config"))

	workDir := t.TempDir()
	remoteDir := filepath.Join(t.TempDir(), "next-template.git")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	for path, content := range files {
		fullPath := filepath.Join(workDir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir parent for %s: %v", path, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	runGit(t, "", "init", "--bare", remoteDir)
	runGit(t, workDir, "init", "-b", "main")
	runGit(t, workDir, "config", "user.name", "Fixture User")
	runGit(t, workDir, "config", "user.email", "fixture@example.com")
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", "Initial commit")
	runGit(t, workDir, "remote", "add", "origin", remoteDir)
	runGit(t, workDir, "push", "-u", "origin", "main")
	runGit(t, remoteDir, "symbolic-ref", "HEAD", "refs/heads/main")

	repoURL := "https://github.com/example/next-template"
	runGit(t, "", "config", "--global", "url."+fmt.Sprintf("file://%s", remoteDir)+".insteadOf", repoURL)

	return gitHubTemplateFixture{
		repoURL: repoURL,
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(output))
	}
}
