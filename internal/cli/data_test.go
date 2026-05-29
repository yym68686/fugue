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
	if !strings.Contains(pullOut, "Restored version before-provider-move") {
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
	if !strings.Contains(firstAuto, "Created version:") || strings.Contains(firstAuto, "version: latest") {
		t.Fatalf("expected default push to create a concrete version, got %s", firstAuto)
	}
	secondAuto := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push")
	if !strings.Contains(secondAuto, "Created version:") || strings.Contains(secondAuto, "version: latest") {
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
	if !strings.Contains(out.String(), "Upload progress") {
		t.Fatalf("expected progress output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "ETA") || !strings.Contains(out.String(), "[") {
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

func testCLIDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
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
