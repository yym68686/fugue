package datacontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/cli"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataWorkspacePushPullAndConflictPreflight(t *testing.T) {
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.delete", "data.grant", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sourceDir := filepath.Join(t.TempDir(), "my-training-project")
	if err := os.MkdirAll(filepath.Join(sourceDir, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "data", "sample.txt"), []byte("training-data"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "track", "./data")
	pushOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push", "--version", "before-provider-move")
	if !strings.Contains(pushOut, "before-provider-move") {
		t.Fatalf("expected push output to mention version, got %s", pushOut)
	}
	req, err := http.NewRequest(http.MethodPut, httpServer.URL+"/v1/data/blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("build direct blob request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("direct blob request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected direct blob write without transfer_id to be forbidden, got %d", resp.StatusCode)
	}

	targetDir := filepath.Join(t.TempDir(), "target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("create target dir: %v", err)
	}
	runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "use", "my-training-project")
	pullOut := runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull")
	if !strings.Contains(pullOut, "Restored version") || !strings.Contains(pullOut, "before-provider-move") {
		t.Fatalf("expected pull output to mention restored version, got %s", pullOut)
	}
	restored, err := os.ReadFile(filepath.Join(targetDir, "data", "sample.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != "training-data" {
		t.Fatalf("unexpected restored content %q", string(restored))
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "data", "sample.txt"), []byte("training-data-v2"), 0o644); err != nil {
		t.Fatalf("write source file v2: %v", err)
	}
	firstAuto := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push")
	if !strings.Contains(firstAuto, "Created version") || strings.Contains(firstAuto, "version: latest") {
		t.Fatalf("expected default push to create a concrete version, got %s", firstAuto)
	}
	secondAuto := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push")
	if !strings.Contains(secondAuto, "Created version") || strings.Contains(secondAuto, "version: latest") {
		t.Fatalf("expected repeated default push to create a concrete version, got %s", secondAuto)
	}

	conflictDir := filepath.Join(t.TempDir(), "conflict")
	if err := os.MkdirAll(conflictDir, 0o755); err != nil {
		t.Fatalf("create conflict dir: %v", err)
	}
	runDataCLIInDir(t, conflictDir, "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "use", "my-training-project")
	if err := os.WriteFile(filepath.Join(conflictDir, "data"), []byte("local-file"), 0o644); err != nil {
		t.Fatalf("write conflicting data path: %v", err)
	}
	stdout, stderr, err := runDataCLIInDirErr(t, conflictDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull")
	if err == nil {
		t.Fatalf("expected conflict error, got stdout=%s stderr=%s", stdout, stderr)
	}
	if !strings.Contains(stdout, "Pull preflight found conflicts") || !strings.Contains(stdout, "remote asset is a directory") {
		t.Fatalf("expected preflight conflict output, got stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
	content, err := os.ReadFile(filepath.Join(conflictDir, "data"))
	if err != nil {
		t.Fatalf("read local conflict file: %v", err)
	}
	if string(content) != "local-file" {
		t.Fatalf("pull changed local conflict file: %q", string(content))
	}
}

func TestDataWorkspaceShowRendersLatestAssetSizes(t *testing.T) {
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID: tenant.ID,
		Name:     "training-project",
		Assets: []model.DataAsset{
			{Name: "checkpoints", Path: "./checkpoints"},
			{Name: "data", Path: "./data"},
		},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err = stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{
			{AssetName: "data", RelativePath: ".", Kind: model.DataManifestEntryKindDir},
			{AssetName: "data", RelativePath: "a.txt", Kind: model.DataManifestEntryKindFile, Size: 3, SHA256: strings.Repeat("a", 64)},
			{AssetName: "data", RelativePath: "b.txt", Kind: model.DataManifestEntryKindFile, Size: 4, SHA256: strings.Repeat("b", 64)},
			{AssetName: "checkpoints", RelativePath: "model.pt", Kind: model.DataManifestEntryKindFile, Size: 10, SHA256: strings.Repeat("c", 64)},
		}}),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	out := runDataCLIInDir(t, t.TempDir(), "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "show", "training-project")
	for _, want := range []string{"Latest files", "Latest size", "17 B", "Files", "checkpoints", "10 B", "data", "7 B"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected workspace show output to contain %q, got %s", want, out)
		}
	}
}

func TestDataEvictFreesAndRestoresTrackedAssets(t *testing.T) {
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.delete", "data.grant", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sourceDir := filepath.Join(t.TempDir(), "evict-project")
	if err := os.MkdirAll(filepath.Join(sourceDir, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	sourceFile := filepath.Join(sourceDir, "data", "sample.txt")
	if err := os.WriteFile(sourceFile, []byte("training-data"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "track", "./data")
	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push", "--version", "safe-to-evict", "--no-progress")

	planOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "evict", "--no-progress")
	if !strings.Contains(planOut, "dry-run") || !strings.Contains(planOut, "fugue data evict --confirm") {
		t.Fatalf("expected dry-run evict plan, got %s", planOut)
	}
	if _, err := os.Stat(sourceFile); err != nil {
		t.Fatalf("dry-run evict removed source file: %v", err)
	}

	evictOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "evict", "--confirm", "--no-progress")
	if !strings.Contains(evictOut, "Evicted assets") || !strings.Contains(evictOut, "safe-to-evict") {
		t.Fatalf("expected evict output, got %s", evictOut)
	}
	if _, err := os.Stat(filepath.Join(sourceDir, "data")); !os.IsNotExist(err) {
		t.Fatalf("expected data directory to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(sourceDir, ".fugue/data.yaml")); err != nil {
		t.Fatalf("evict should preserve local data config: %v", err)
	}

	statusOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "status", "--no-progress")
	if !strings.Contains(statusOut, "evicted") {
		t.Fatalf("expected status to show evicted asset, got %s", statusOut)
	}

	pullOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "--verify", "--no-progress")
	if !strings.Contains(pullOut, "Restored version") {
		t.Fatalf("expected pull restore output, got %s", pullOut)
	}
	restored, err := os.ReadFile(sourceFile)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != "training-data" {
		t.Fatalf("unexpected restored content %q", string(restored))
	}

	if err := os.WriteFile(sourceFile, []byte("local-change"), 0o644); err != nil {
		t.Fatalf("write local change: %v", err)
	}
	stdout, stderr, err := runDataCLIInDirErr(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "evict", "--confirm", "--no-progress")
	if err == nil {
		t.Fatalf("expected evict to block changed local data, stdout=%s stderr=%s", stdout, stderr)
	}
	if !strings.Contains(stdout, "blocked") || !strings.Contains(stdout, "local content differs") {
		t.Fatalf("expected changed-data block, stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
	if got, err := os.ReadFile(sourceFile); err != nil || string(got) != "local-change" {
		t.Fatalf("blocked evict changed local file, got=%q err=%v", string(got), err)
	}
}

func TestDataPullAcceptsPositionalAssetSelectors(t *testing.T) {
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.delete", "data.grant", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sourceDir := filepath.Join(t.TempDir(), "selector-project")
	sourceFile := filepath.Join(sourceDir, "kernel.pt")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("create source dir: %v", err)
	}
	if err := os.WriteFile(sourceFile, []byte("weights"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "track", "kernel.pt")
	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push", "--version", "weights-v1", "--no-progress")

	if err := os.Remove(sourceFile); err != nil {
		t.Fatalf("remove source file: %v", err)
	}
	pullOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "kernel.pt", "--verify", "--no-progress")
	if !strings.Contains(pullOut, "Restored version") {
		t.Fatalf("expected positional pull output, got %s", pullOut)
	}
	if restored, err := os.ReadFile(sourceFile); err != nil || string(restored) != "weights" {
		t.Fatalf("unexpected restored content %q err=%v", string(restored), err)
	}

	if err := os.Remove(sourceFile); err != nil {
		t.Fatalf("remove restored file: %v", err)
	}
	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "kernel-pt", "--verify", "--no-progress")
	if restored, err := os.ReadFile(sourceFile); err != nil || string(restored) != "weights" {
		t.Fatalf("unexpected restored content by asset name %q err=%v", string(restored), err)
	}

	stdout, stderr, err := runDataCLIInDirErr(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "kernel.pt", "--asset", "kernel-pt", "--no-progress")
	if err == nil {
		t.Fatalf("expected --asset plus positional selector to fail, stdout=%s stderr=%s", stdout, stderr)
	}
	if !strings.Contains(err.Error(), "either --asset or positional") {
		t.Fatalf("expected selector conflict error, stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
}

func TestDataStatusWithoutLocalBindingShowsAccountOverview(t *testing.T) {
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "prod-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "lanyun-tmp",
		StorageBackendID: backend.ID,
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	backendName := backend.Name
	workspace, err = stateStore.UpdateDataWorkspace(workspace.ID, tenant.ID, false, store.DataWorkspaceUpdate{StorageBackendID: &backendName})
	if err != nil {
		t.Fatalf("update workspace backend: %v", err)
	}
	if _, err := stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "before-provider-move",
		Manifest: model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "sample.bin",
			Kind:         model.DataManifestEntryKindFile,
			Size:         42,
			SHA256:       strings.Repeat("a", 64),
		}}},
	}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	unboundDir := t.TempDir()
	out := runDataCLIInDir(t, unboundDir, "--base-url", httpServer.URL, "--token", secret, "data", "status")
	for _, want := range []string{
		"No local data workspace is bound in this directory.",
		"Your data workspaces:",
		"lanyun-tmp",
		"prod-r2",
		"42 B",
		"fugue data workspace use <workspace>",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected status output to contain %q, got %s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(unboundDir, ".fugue/data.yaml")); !os.IsNotExist(err) {
		t.Fatalf("expected status not to create %s, stat err=%v", ".fugue/data.yaml", err)
	}

	jsonOut := runDataCLIInDir(t, unboundDir, "--base-url", httpServer.URL, "--token", secret, "data", "status", "--json")
	if !strings.Contains(jsonOut, `"local_bound": false`) || !strings.Contains(jsonOut, `"lanyun-tmp"`) {
		t.Fatalf("expected json account status, got %s", jsonOut)
	}

	invalidDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(invalidDir, ".fugue"), 0o755); err != nil {
		t.Fatalf("create .fugue: %v", err)
	}
	if err := os.WriteFile(filepath.Join(invalidDir, ".fugue/data.yaml"), []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	stdout, stderr, err := runDataCLIInDirErr(t, invalidDir, "--base-url", httpServer.URL, "--token", secret, "data", "status")
	if err == nil {
		t.Fatalf("expected invalid local config to fail, stdout=%s stderr=%s", stdout, stderr)
	}
	if !strings.Contains(err.Error(), "workspace is required") {
		t.Fatalf("expected invalid config error, stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
}

func TestS3CompatibleDataWorkspacePushPullIntegration(t *testing.T) {
	for _, provider := range []string{model.DataBackendProviderS3, model.DataBackendProviderMinIO} {
		provider := provider
		t.Run(provider, func(t *testing.T) {
			var mu sync.Mutex
			objects := map[string][]byte{}
			objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := r.URL.Path
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					if body, ok := objects[key]; ok {
						w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
						w.WriteHeader(http.StatusOK)
						return
					}
					w.WriteHeader(http.StatusNotFound)
				case http.MethodPut:
					body, _ := io.ReadAll(r.Body)
					objects[key] = body
					w.WriteHeader(http.StatusOK)
				case http.MethodGet:
					body, ok := objects[key]
					if !ok {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
					_, _ = w.Write(body)
				default:
					t.Fatalf("unexpected object request %s %s", r.Method, r.URL.String())
				}
			}))
			defer objectServer.Close()

			stateStore := newStoreTestDatabase(t, "")
			if err := stateStore.Init(); err != nil {
				t.Fatalf("init store: %v", err)
			}
			tenant, err := stateStore.CreateTenant("Data Tenant")
			if err != nil {
				t.Fatalf("create tenant: %v", err)
			}
			_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.delete", "data.grant", "data.admin"})
			if err != nil {
				t.Fatalf("create api key: %v", err)
			}
			backend, err := stateStore.CreateDataBackend(model.DataBackend{
				TenantID: tenant.ID,
				Name:     provider + "-backend",
				Provider: provider,
				Bucket:   "bucket",
				Endpoint: objectServer.URL,
				Region:   "us-east-1",
				Credentials: model.DataBackendCredentials{
					AccessKeyID:     "access",
					SecretAccessKey: "secret",
				},
				Capabilities: model.DataBackendCapabilitiesForProvider(provider),
			})
			if err != nil {
				t.Fatalf("create backend: %v", err)
			}
			server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
			httpServer := httptest.NewServer(server.Handler())
			defer httpServer.Close()
			sourceDir := filepath.Join(t.TempDir(), "training-project")
			if err := os.MkdirAll(filepath.Join(sourceDir, "data"), 0o755); err != nil {
				t.Fatalf("create source dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(sourceDir, "data", "sample.txt"), []byte("training-data-"+provider), 0o644); err != nil {
				t.Fatalf("write source file: %v", err)
			}
			runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "track", "./data")
			_, err = stateStore.CreateDataWorkspace(model.DataWorkspace{
				TenantID:         tenant.ID,
				Name:             "training-project",
				StorageBackendID: backend.ID,
				Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
			})
			if err != nil {
				t.Fatalf("create workspace: %v", err)
			}
			runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push", "--version", provider+"-v1")
			targetDir := filepath.Join(t.TempDir(), "target")
			if err := os.MkdirAll(targetDir, 0o755); err != nil {
				t.Fatalf("create target dir: %v", err)
			}
			runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "use", "training-project")
			runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "--version", provider+"-v1", "--verify")
			restored, err := os.ReadFile(filepath.Join(targetDir, "data", "sample.txt"))
			if err != nil {
				t.Fatalf("read restored file: %v", err)
			}
			if string(restored) != "training-data-"+provider {
				t.Fatalf("unexpected restored content %q", string(restored))
			}
		})
	}
}

func TestDataPullRefreshesExpiredPresignedURL(t *testing.T) {
	content := []byte("refresh-after-expiry")
	digest := testCLIDigest(string(content))
	var getCalls int
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls++
			if getCalls == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			_, _ = w.Write(content)
		default:
			t.Fatalf("unexpected object request %s %s", r.Method, r.URL.String())
		}
	}))
	defer objectServer.Close()
	stateStore := newStoreTestDatabase(t, "")
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: objectServer.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderS3),
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "refresh-project",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err = stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "sample.txt",
			Kind:         model.DataManifestEntryKindFile,
			Size:         int64(len(content)),
			SHA256:       digest,
		}}}),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	targetDir := filepath.Join(t.TempDir(), "target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("create target dir: %v", err)
	}
	runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "use", "refresh-project")
	runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull", "--version", "v1", "--verify")
	if getCalls != 2 {
		t.Fatalf("expected download retry after refresh, got %d GETs", getCalls)
	}
}

func testCLIDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func runDataCLIInDir(t *testing.T, dir string, args ...string) string {
	t.Helper()
	stdout, stderr, err := runDataCLIInDirErr(t, dir, args...)
	if err != nil {
		t.Fatalf("run fugue data command: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	return stdout
}

func runDataCLIInDirErr(t *testing.T, dir string, args ...string) (string, string, error) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(previous); err != nil {
			t.Error(err)
		}
	}()
	var stdout, stderr bytes.Buffer
	err = cli.RunWithContext(t.Context(), args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}
