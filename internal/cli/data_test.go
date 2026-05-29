package cli

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
	"time"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDataWorkspacePushPullAndConflictPreflight(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
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
	stdout, stderr, err := runDataCLIInDirErr(conflictDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull")
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

			stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
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
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
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

func TestDownloadDataBlobMultipartResume(t *testing.T) {
	content := []byte("abcdefghijklmnopqrstuvwxyz")
	sum := sha256.Sum256(content)
	expectedSHA := hex.EncodeToString(sum[:])
	var mu sync.Mutex
	requestedRanges := map[string]int{}
	blobServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int
		rangeHeader := r.Header.Get("Range")
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			t.Fatalf("expected range request, got %q", rangeHeader)
		}
		if start < 0 || end >= len(content) || start > end {
			t.Fatalf("invalid range request %q", rangeHeader)
		}
		mu.Lock()
		requestedRanges[rangeHeader]++
		mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
	}))
	defer blobServer.Close()

	root := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("get wd: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(previous)

	partDir := filepath.Join(root, ".fugue", "tmp", expectedSHA+".download-parts")
	if err := os.MkdirAll(partDir, 0o700); err != nil {
		t.Fatalf("create part dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(partDir, "000001.part"), content[:5], 0o600); err != nil {
		t.Fatalf("seed completed part: %v", err)
	}
	client := &Client{httpClient: http.DefaultClient}
	targetPath := filepath.Join(root, "data", "sample.bin")
	if err := client.downloadDataBlobMultipart(blobServer.URL, targetPath, expectedSHA, int64(len(content)), true, true, 3, 5, nil); err != nil {
		t.Fatalf("download multipart: %v", err)
	}
	restored, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(content) {
		t.Fatalf("unexpected restored content %q", string(restored))
	}
	if requestedRanges["bytes=0-4"] != 0 {
		t.Fatalf("expected completed first part to be skipped, got ranges %+v", requestedRanges)
	}
	if len(requestedRanges) != 5 {
		t.Fatalf("expected five missing ranges, got %+v", requestedRanges)
	}
}

func TestDataConfigParserPathIgnoreScannerAndLargeDirectoryHint(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".fugue"), 0o755); err != nil {
		t.Fatalf("create .fugue: %v", err)
	}
	rawConfig := []byte(`
version: 1
workspace: parser-test
assets:
  - name: dataset
    path: ./data
ignore:
  - "*.tmp"
  - cache
`)
	if err := os.WriteFile(filepath.Join(root, dataConfigPath), rawConfig, 0o644); err != nil {
		t.Fatalf("write data config: %v", err)
	}
	cfg, err := readDataConfig(root)
	if err != nil {
		t.Fatalf("read data config: %v", err)
	}
	if cfg.Workspace != "parser-test" || len(cfg.Assets) != 1 || cfg.Assets[0].Name != "dataset" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cleaned, err := cleanConfigPath("data/../data"); err != nil || cleaned != "./data" {
		t.Fatalf("unexpected clean path %q err=%v", cleaned, err)
	}
	if _, err := cleanConfigPath("../outside"); err == nil {
		t.Fatal("expected escaping path to fail")
	}
	if !shouldIgnoreDataPath("nested/file.tmp", "file.tmp", cfg.Ignore) || !shouldIgnoreDataPath("cache", "cache", cfg.Ignore) {
		t.Fatal("expected ignore rules to match")
	}
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", "sample.txt"), []byte("sample"), 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", "skip.tmp"), []byte("skip"), 0o644); err != nil {
		t.Fatalf("write ignored: %v", err)
	}
	estimate, err := estimateDataManifestScan(root, cfg, "")
	if err != nil {
		t.Fatalf("estimate manifest scan: %v", err)
	}
	if estimate.Files != 1 || estimate.Bytes != int64(len("sample")) {
		t.Fatalf("unexpected scan estimate: %+v", estimate)
	}
	manifest, paths, err := scanDataManifest(root, cfg, "")
	if err != nil {
		t.Fatalf("scan manifest: %v", err)
	}
	if manifest.FileCount != 1 || manifest.TotalBytes != int64(len("sample")) || len(paths) != 1 {
		t.Fatalf("unexpected manifest: %+v paths=%+v", manifest, paths)
	}
	if err := os.MkdirAll(filepath.Join(root, "large-untracked"), 0o755); err != nil {
		t.Fatalf("create large dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "large-untracked", "blob.bin"), []byte("0123456789"), 0o644); err != nil {
		t.Fatalf("write large dir file: %v", err)
	}
	dirs, err := findUntrackedLargeDataDirectories(root, cfg, 5)
	if err != nil {
		t.Fatalf("find large dirs: %v", err)
	}
	if len(dirs) != 1 || dirs[0].Path != "./large-untracked" {
		t.Fatalf("unexpected large dir hints: %+v", dirs)
	}
}

func TestDataManifestHashCacheReusesUnchangedFilesAndInvalidatesChangedFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".fugue"), 0o755); err != nil {
		t.Fatalf("create .fugue: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	cfg := dataConfig{Assets: []model.DataAsset{{Name: "dataset", Path: "./data"}}}
	source := filepath.Join(root, "data", "sample.txt")
	if err := os.WriteFile(source, []byte("sample"), 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}
	fileTime := time.Unix(100, 0)
	if err := os.Chtimes(source, fileTime, fileTime); err != nil {
		t.Fatalf("chtimes sample: %v", err)
	}

	originalNow := dataHashCacheNow
	defer func() { dataHashCacheNow = originalNow }()
	firstComputedAt := time.Unix(1000, 0).UTC().Format(time.RFC3339Nano)
	secondComputedAt := time.Unix(2000, 0).UTC().Format(time.RFC3339Nano)
	dataHashCacheNow = func() time.Time { return time.Unix(1000, 0) }

	firstManifest, _, err := scanDataManifest(root, cfg, "")
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	firstDigest := manifestDigestForTest(t, firstManifest, "sample.txt")
	firstEntry := dataHashCacheEntryForTest(t, root, "./data/sample.txt")
	if firstEntry.SHA256 != firstDigest || firstEntry.ComputedAt != firstComputedAt {
		t.Fatalf("unexpected first cache entry %+v digest=%s", firstEntry, firstDigest)
	}

	dataHashCacheNow = func() time.Time { return time.Unix(2000, 0) }
	secondManifest, _, err := scanDataManifest(root, cfg, "")
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	secondDigest := manifestDigestForTest(t, secondManifest, "sample.txt")
	secondEntry := dataHashCacheEntryForTest(t, root, "./data/sample.txt")
	if secondDigest != firstDigest {
		t.Fatalf("expected cached digest %s, got %s", firstDigest, secondDigest)
	}
	if secondEntry.ComputedAt != firstComputedAt {
		t.Fatalf("expected unchanged cache computed_at %q, got %+v", firstComputedAt, secondEntry)
	}

	if err := os.WriteFile(source, []byte("changed"), 0o644); err != nil {
		t.Fatalf("modify sample: %v", err)
	}
	changedTime := time.Unix(200, 0)
	if err := os.Chtimes(source, changedTime, changedTime); err != nil {
		t.Fatalf("chtimes changed sample: %v", err)
	}
	changedManifest, _, err := scanDataManifest(root, cfg, "")
	if err != nil {
		t.Fatalf("changed scan: %v", err)
	}
	changedDigest := manifestDigestForTest(t, changedManifest, "sample.txt")
	changedEntry := dataHashCacheEntryForTest(t, root, "./data/sample.txt")
	if changedDigest == firstDigest {
		t.Fatalf("expected changed digest, got %s", changedDigest)
	}
	if changedEntry.ComputedAt != secondComputedAt {
		t.Fatalf("expected changed cache computed_at %q, got %+v", secondComputedAt, changedEntry)
	}

	if key := dataHashCacheIdentityKey(changedEntry.Device, changedEntry.Inode, changedEntry.Size, changedEntry.MTimeUnixNano); key != "" {
		renamed := filepath.Join(root, "data", "renamed.txt")
		if err := os.Rename(source, renamed); err != nil {
			t.Fatalf("rename sample: %v", err)
		}
		dataHashCacheNow = func() time.Time { return time.Unix(3000, 0) }
		renamedManifest, _, err := scanDataManifest(root, cfg, "")
		if err != nil {
			t.Fatalf("renamed scan: %v", err)
		}
		renamedDigest := manifestDigestForTest(t, renamedManifest, "renamed.txt")
		renamedEntry := dataHashCacheEntryForTest(t, root, "./data/renamed.txt")
		if renamedDigest != changedDigest {
			t.Fatalf("expected renamed digest %s, got %s", changedDigest, renamedDigest)
		}
		if renamedEntry.ComputedAt != secondComputedAt {
			t.Fatalf("expected rename to reuse cache computed_at %q, got %+v", secondComputedAt, renamedEntry)
		}
		cache, err := loadDataHashCache(root)
		if err != nil {
			t.Fatalf("load cache after rename: %v", err)
		}
		for _, entry := range cache.Entries {
			if entry.Path == "./data/sample.txt" {
				t.Fatalf("expected deleted path to be removed from cache, got %+v", cache.Entries)
			}
		}
	}
}

func TestBuildPullPlanConflictsAndPolicies(t *testing.T) {
	root := t.TempDir()
	cfg := dataConfig{Assets: []model.DataAsset{{Name: "data", Path: "./data"}}}
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{
		{AssetName: "data", RelativePath: ".", Kind: model.DataManifestEntryKindDir},
		{AssetName: "data", RelativePath: "sample.txt", Kind: model.DataManifestEntryKindFile, Size: 6, SHA256: testCLIDigest("remote")},
	}})
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("local-file"), 0o644); err != nil {
		t.Fatalf("write type conflict: %v", err)
	}
	plan, err := buildPullPlan(root, cfg, manifest, false, false, false)
	if err != nil {
		t.Fatalf("build conflict plan: %v", err)
	}
	if len(plan.Conflicts) == 0 || !strings.Contains(plan.Conflicts[0].Reason, "directory") {
		t.Fatalf("expected file-vs-dir conflict, got %+v", plan)
	}

	root = t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", "sample.txt"), []byte("local"), 0o644); err != nil {
		t.Fatalf("write changed file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", "extra.txt"), []byte("extra"), 0o644); err != nil {
		t.Fatalf("write extra file: %v", err)
	}
	plan, err = buildPullPlan(root, cfg, manifest, false, false, false)
	if err != nil {
		t.Fatalf("build changed plan: %v", err)
	}
	if len(plan.Conflicts) != 1 || len(plan.Warnings) != 1 {
		t.Fatalf("expected checksum conflict and extra warning, got %+v", plan)
	}
	keepPlan, err := buildPullPlan(root, cfg, manifest, false, true, false)
	if err != nil {
		t.Fatalf("build keep-local plan: %v", err)
	}
	if len(keepPlan.Conflicts) != 0 || len(keepPlan.Warnings) < 2 {
		t.Fatalf("expected keep-local warnings without conflicts, got %+v", keepPlan)
	}
	overwritePlan, err := buildPullPlan(root, cfg, manifest, true, false, false)
	if err != nil {
		t.Fatalf("build overwrite plan: %v", err)
	}
	if len(overwritePlan.Conflicts) != 0 || len(overwritePlan.Download) == 0 {
		t.Fatalf("expected overwrite download without conflicts, got %+v", overwritePlan)
	}
	prunePlan, err := buildPullPlan(root, cfg, manifest, true, false, true)
	if err != nil {
		t.Fatalf("build prune plan: %v", err)
	}
	if len(prunePlan.Prune) != 1 || !strings.HasSuffix(prunePlan.Prune[0], "extra.txt") {
		t.Fatalf("expected extra file prune, got %+v", prunePlan)
	}
}

func TestManifestDiffTransferStateAndProgressRenderer(t *testing.T) {
	from := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{AssetName: "data", RelativePath: "a.txt", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: testCLIDigest("a")}}})
	to := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{
		{AssetName: "data", RelativePath: "a.txt", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: testCLIDigest("b")},
		{AssetName: "data", RelativePath: "b.txt", Kind: model.DataManifestEntryKindFile, Size: 1, SHA256: testCLIDigest("c")},
	}})
	diff := diffDataManifests(from, to)
	if len(diff["changed"]) != 1 || len(diff["added"]) != 1 {
		t.Fatalf("unexpected manifest diff: %+v", diff)
	}
	root := t.TempDir()
	if err := saveDataTransferState(root, dataTransferState{TransferID: "transfer_1", Direction: model.DataTransferDirectionUpload}); err != nil {
		t.Fatalf("save transfer state: %v", err)
	}
	if _, ok, err := loadDataTransferState(root, "transfer_1"); err != nil || !ok {
		t.Fatalf("load transfer state ok=%t err=%v", ok, err)
	}
	if err := os.WriteFile(dataTransferStatePath(root, "broken"), []byte("{"), 0o600); err != nil {
		t.Fatalf("write broken state: %v", err)
	}
	if _, _, err := loadDataTransferState(root, "broken"); err == nil {
		t.Fatal("expected broken transfer state to fail")
	}
	var out bytes.Buffer
	progress := newDataProgressRenderer(&out, true, "Upload", 10)
	progress.advance(5)
	progress.advance(5)
	if !strings.Contains(out.String(), "Upload") {
		t.Fatalf("expected progress output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "ETA") || !strings.Contains(out.String(), "█") {
		t.Fatalf("expected progress bar with ETA, got %q", out.String())
	}
	payload := []byte("progress-hash")
	source := filepath.Join(root, "source.bin")
	if err := os.WriteFile(source, payload, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	var progressed int64
	gotDigest, err := sha256LocalFileWithProgress(source, func(delta int64) {
		progressed += delta
	})
	if err != nil {
		t.Fatalf("hash with progress: %v", err)
	}
	if progressed != int64(len(payload)) {
		t.Fatalf("expected hash progress %d, got %d", len(payload), progressed)
	}
	if gotDigest != testCLIDigest(string(payload)) {
		t.Fatalf("unexpected digest %s", gotDigest)
	}
}

func TestUploadDataPlanBlobsUsesConcurrencyForSingleBlobUploads(t *testing.T) {
	root := t.TempDir()
	var mu sync.Mutex
	var active, maxActive, puts, checkpoints int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/upload/"):
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			active--
			puts++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/data/transfers/transfer-1/checkpoint":
			mu.Lock()
			checkpoints++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	blobs := make([]dataBlobPlan, 0, 4)
	pathsByDigest := map[string]string{}
	for idx := 0; idx < 4; idx++ {
		digest := fmt.Sprintf("%064d", idx+1)
		filePath := filepath.Join(root, fmt.Sprintf("file-%d.bin", idx))
		if err := os.WriteFile(filePath, []byte(strings.Repeat("x", 1024)), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		pathsByDigest[digest] = filePath
		blobs = append(blobs, dataBlobPlan{
			SHA256:     digest,
			Size:       1024,
			UploadMode: model.DataBlobUploadModeSingle,
			UploadURL:  server.URL + "/upload/" + digest,
		})
	}
	cli := &CLI{stdout: io.Discard, stderr: io.Discard}
	if err := cli.uploadDataPlanBlobs(client, "workspace-1", "transfer-1", "manifest-1", blobs, pathsByDigest, false, true, 4, 4096, 0, 4, nil); err != nil {
		t.Fatalf("upload blobs: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if puts != 4 {
		t.Fatalf("expected 4 uploads, got %d", puts)
	}
	if maxActive <= 1 {
		t.Fatalf("expected concurrent uploads, max active=%d", maxActive)
	}
	if checkpoints != 1 {
		t.Fatalf("expected final checkpoint, got %d", checkpoints)
	}
}

func TestUploadDataPlanBlobsContinuesAfterTransientCheckpointFailure(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	oldSleep := dataControlPlaneRetrySleep
	dataControlPlaneRetrySleep = func(time.Duration) {}
	t.Cleanup(func() { dataControlPlaneRetrySleep = oldSleep })

	var mu sync.Mutex
	var puts, checkpoints int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/upload/blob":
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			puts++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/data/transfers/transfer-1/checkpoint":
			mu.Lock()
			checkpoints++
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("write failed: write tcp 10.0.0.1:1234->10.0.0.2:5432: write: connection reset by peer"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	digest := strings.Repeat("a", 64)
	source := filepath.Join(root, "blob.bin")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	var stderr bytes.Buffer
	cli := &CLI{stdout: io.Discard, stderr: &stderr}
	err = cli.uploadDataPlanBlobs(client, "workspace-1", "transfer-1", "manifest-1", []dataBlobPlan{{
		SHA256:     digest,
		Size:       7,
		UploadMode: model.DataBlobUploadModeSingle,
		UploadURL:  server.URL + "/upload/blob",
	}}, map[string]string{digest: source}, true, true, 1, 7, 0, 1, nil)
	if err != nil {
		t.Fatalf("upload blobs: %v", err)
	}
	mu.Lock()
	gotPuts := puts
	gotCheckpoints := checkpoints
	mu.Unlock()
	if gotPuts != 1 {
		t.Fatalf("expected upload to complete, got puts=%d", gotPuts)
	}
	if gotCheckpoints != dataControlPlaneTransferMaxAttempts {
		t.Fatalf("expected checkpoint retries, got %d", gotCheckpoints)
	}
	if !strings.Contains(stderr.String(), "checkpoint sync was delayed") {
		t.Fatalf("expected checkpoint warning, got %q", stderr.String())
	}
}

func TestPutDataBlobRetriesTransientObjectFailure(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "blob.bin")
	payload := []byte(strings.Repeat("x", 4096))
	if err := os.WriteFile(source, payload, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	var mu sync.Mutex
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/upload/blob" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()
		if current == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("try again"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	var progressed int64
	if err := client.PutDataBlobWithProgress(server.URL+"/upload/blob", source, func(delta int64) {
		progressed += delta
	}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("expected retry, got %d attempts", attempts)
	}
	if progressed != int64(len(payload)) {
		t.Fatalf("expected progress %d, got %d", len(payload), progressed)
	}
}

func TestCheckpointDataTransferRetriesTransientReadFailure(t *testing.T) {
	var mu sync.Mutex
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/data/transfers/transfer-1/checkpoint" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()
		if current == 1 {
			w.Header().Set("Content-Length", "64")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"checkpointed":true}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := client.CheckpointDataTransfer("transfer-1", -1, -1, []dataBlobPlan{{SHA256: strings.Repeat("a", 64), Size: 1}}); err != nil {
		t.Fatalf("checkpoint transfer: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("expected retry, got %d attempts", attempts)
	}
}

func TestRefreshDataTransferAuthorizationRetriesTransientStatus(t *testing.T) {
	var mu sync.Mutex
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/data/transfers/transfer-1/refresh" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()
		if current == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"gateway reset"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"transfer":{"id":"transfer-1"},"blobs":[]}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, err := client.RefreshDataTransferAuthorizationPage("transfer-1", 0, 10); err != nil {
		t.Fatalf("refresh transfer authorization: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("expected retry, got %d attempts", attempts)
	}
}

func TestUploadResumeStatePreservesCompletedBlobs(t *testing.T) {
	completedDigest := strings.Repeat("a", 64)
	otherDigest := strings.Repeat("b", 64)
	newDigest := strings.Repeat("c", 64)
	loaded := dataTransferState{
		TransferID:     "transfer-1",
		Direction:      model.DataTransferDirectionUpload,
		WorkspaceID:    "workspace-1",
		Version:        "old-version",
		ManifestDigest: "old-manifest",
		Blobs: []dataBlobPlan{
			{SHA256: completedDigest, Exists: true, UploadURL: "https://old.example/upload"},
			{SHA256: otherDigest, Exists: true},
		},
	}
	refresh := dataTransferAuthorizationResponse{
		Transfer: model.DataTransfer{ID: "transfer-1", WorkspaceID: "workspace-1", Version: "new-version"},
		Blobs: []dataBlobPlan{
			{SHA256: completedDigest, UploadURL: "https://new.example/upload"},
			{SHA256: newDigest, UploadURL: "https://new.example/new"},
		},
	}

	state := uploadResumeStateFromRefresh(refresh, "new-manifest", loaded, true)

	if state.Version != "new-version" || state.ManifestDigest != "new-manifest" {
		t.Fatalf("resume metadata was not refreshed: %+v", state)
	}
	completed, ok := dataPlanBlobByDigest(state.Blobs, completedDigest)
	if !ok || !completed.Exists {
		t.Fatalf("expected completed blob to remain completed, got %+v ok=%t", completed, ok)
	}
	if completed.UploadURL != "" {
		t.Fatalf("expected stored completed blob to be sanitized, got upload url %q", completed.UploadURL)
	}
	if _, ok := dataPlanBlobByDigest(state.Blobs, otherDigest); !ok {
		t.Fatalf("expected completed blob from another page to be preserved")
	}
	if fresh, ok := dataPlanBlobByDigest(state.Blobs, newDigest); !ok || fresh.Exists {
		t.Fatalf("expected new refreshed blob to be tracked as pending, got %+v ok=%t", fresh, ok)
	}
}

func TestDataControlPlanePlainStatusErrorsAreTransient(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("request failed: status=401 body=unauthorized"),
		fmt.Errorf("request failed: status=500 body=app lookup failed"),
	} {
		if !isTransientDataControlPlaneError(err) {
			t.Fatalf("expected %q to be transient", err.Error())
		}
	}
}

func testCLIDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func manifestDigestForTest(t *testing.T, manifest model.DataManifest, relativePath string) string {
	t.Helper()
	for _, entry := range manifest.Entries {
		if entry.Kind == model.DataManifestEntryKindFile && entry.RelativePath == relativePath {
			return entry.SHA256
		}
	}
	t.Fatalf("manifest entry %s not found in %+v", relativePath, manifest.Entries)
	return ""
}

func dataHashCacheEntryForTest(t *testing.T, root, cachePath string) dataHashCacheEntry {
	t.Helper()
	cache, err := loadDataHashCache(root)
	if err != nil {
		t.Fatalf("load cache: %v", err)
	}
	for _, entry := range cache.Entries {
		if entry.Path == cachePath {
			return entry
		}
	}
	t.Fatalf("cache entry %s not found in %+v", cachePath, cache.Entries)
	return dataHashCacheEntry{}
}

func runDataCLIInDir(t *testing.T, dir string, args ...string) string {
	t.Helper()
	stdout, stderr, err := runDataCLIInDirErr(dir, args...)
	if err != nil {
		t.Fatalf("run fugue %s: %v\nstdout=%s\nstderr=%s", strings.Join(args, " "), err, stdout, stderr)
	}
	return stdout
}

func runDataCLIInDirErr(dir string, args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	previous, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	if err := os.Chdir(dir); err != nil {
		return "", "", err
	}
	defer os.Chdir(previous)
	err = runWithStreams(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}
