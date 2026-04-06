package api

import (
	"net/http/httptest"
	"testing"
)

func TestResolveIdempotencyKeyPrefersHeader(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/apps/import-github", nil)
	req.Header.Set("Idempotency-Key", "header-key")

	key, err := resolveIdempotencyKey(req, "")
	if err != nil {
		t.Fatalf("resolve idempotency key: %v", err)
	}
	if key != "header-key" {
		t.Fatalf("expected header-key, got %s", key)
	}
}

func TestResolveIdempotencyKeyRejectsMismatch(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/apps/import-github", nil)
	req.Header.Set("Idempotency-Key", "header-key")

	if _, err := resolveIdempotencyKey(req, "body-key"); err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestHashImportGitHubRequestChangesWhenRequestChanges(t *testing.T) {
	req := importGitHubRequest{
		ProjectID: "project_1",
		RepoURL:   "https://github.com/example/demo",
		Branch:    "main",
		Name:      "demo",
	}
	hashA, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request a: %v", err)
	}

	req.Name = "demo-2"
	hashB, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request b: %v", err)
	}

	if hashA == hashB {
		t.Fatal("expected different hashes for different requests")
	}
}

func TestHashImportGitHubRequestChangesWhenEnvChanges(t *testing.T) {
	req := importGitHubRequest{
		ProjectID: "project_1",
		RepoURL:   "https://github.com/example/demo",
		Branch:    "main",
		Name:      "demo",
		Env: map[string]string{
			"OPENAI_API_KEY": "sk-demo",
		},
	}
	hashA, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request a: %v", err)
	}

	req.Env["OPENAI_API_KEY"] = "sk-updated"
	hashB, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request b: %v", err)
	}

	if hashA == hashB {
		t.Fatal("expected different hashes when env changes")
	}
}

func TestHashImportGitHubRequestChangesWhenPersistentStorageSeedFilesChange(t *testing.T) {
	req := importGitHubRequest{
		ProjectID: "project_1",
		RepoURL:   "https://github.com/example/demo",
		Branch:    "main",
		Name:      "demo",
		PersistentStorageSeedFiles: []importGitHubPersistentStorageSeedFile{
			{
				Service:     "app",
				Path:        "/home/api.yaml",
				SeedContent: "providers: []\n",
			},
		},
	}
	hashA, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request a: %v", err)
	}

	req.PersistentStorageSeedFiles[0].SeedContent = "providers:\n  - openai\n"
	hashB, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request b: %v", err)
	}

	if hashA == hashB {
		t.Fatal("expected different hashes when persistent storage seed files change")
	}
}

func TestHashImportGitHubRequestChangesWhenStartupCommandChanges(t *testing.T) {
	command := "npm run start"
	req := importGitHubRequest{
		ProjectID:      "project_1",
		RepoURL:        "https://github.com/example/demo",
		Branch:         "main",
		Name:           "demo",
		StartupCommand: &command,
	}
	hashA, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request a: %v", err)
	}

	command = "npm run worker"
	hashB, err := hashImportGitHubRequest("tenant_1", req, "runtime_managed_shared", 1)
	if err != nil {
		t.Fatalf("hash request b: %v", err)
	}

	if hashA == hashB {
		t.Fatal("expected different hashes when startup command changes")
	}
}
