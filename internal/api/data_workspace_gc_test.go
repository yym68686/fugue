package api

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestSweepDataWorkspaceGCPreservesLiveBlobsAndDeletesOrphans(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "workspace"})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	liveDigest := testDataDigest("live")
	orphanDigest := testDataDigest("orphan")
	if _, err := stateStore.WriteDataBlob(liveDigest, strings.NewReader("live")); err != nil {
		t.Fatalf("write live blob: %v", err)
	}
	if _, err := stateStore.WriteDataBlob(orphanDigest, strings.NewReader("orphan")); err != nil {
		t.Fatalf("write orphan blob: %v", err)
	}
	old := time.Now().Add(-10 * 24 * time.Hour)
	if err := touchDataBlob(storePath, liveDigest, old); err != nil {
		t.Fatalf("touch live blob: %v", err)
	}
	if err := touchDataBlob(storePath, orphanDigest, old); err != nil {
		t.Fatalf("touch orphan blob: %v", err)
	}
	_, err = stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "live.txt",
			Kind:         model.DataManifestEntryKindFile,
			Size:         4,
			SHA256:       liveDigest,
		}}}),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	server := NewServer(stateStore, nil, nil, ServerConfig{})
	dryRun, err := server.sweepDataWorkspaceGC(nil, workspace, 7, true)
	if err != nil {
		t.Fatalf("dry-run gc: %v", err)
	}
	if dryRun.Deleted != 0 || len(dryRun.Candidates) != 1 || dryRun.Candidates[0].Key != orphanDigest {
		t.Fatalf("unexpected dry-run result: %+v", dryRun)
	}
	result, err := server.sweepDataWorkspaceGC(nil, workspace, 7, false)
	if err != nil {
		t.Fatalf("gc sweep: %v", err)
	}
	if result.Deleted != 1 {
		t.Fatalf("expected one deleted blob, got %+v", result)
	}
	if !stateStore.DataBlobExists(liveDigest) {
		t.Fatal("live blob was deleted")
	}
	if stateStore.DataBlobExists(orphanDigest) {
		t.Fatal("orphan blob was not deleted")
	}
}

func TestSweepDataWorkspaceGCCleansOldMigrationBackendObjects(t *testing.T) {
	old := time.Now().UTC().Add(-10 * 24 * time.Hour).Format(time.RFC3339)
	liveDigest := testDataDigest("live")
	orphanDigest := testDataDigest("orphan")
	liveKey := model.DataObjectKey(liveDigest)
	orphanKey := model.DataObjectKey(orphanDigest)
	var deletedOldBackend bool
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated>` +
				`<Contents><Key>` + liveKey + `</Key><LastModified>` + old + `</LastModified><Size>4</Size></Contents>` +
				`<Contents><Key>` + orphanKey + `</Key><LastModified>` + old + `</LastModified><Size>6</Size></Contents>` +
				`</ListBucketResult>`))
		case r.Method == http.MethodPost && hasQueryKey(r, "delete"):
			deletedOldBackend = true
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<DeleteResult></DeleteResult>`))
		default:
			t.Fatalf("unexpected source backend request %s %s", r.Method, r.URL.String())
		}
	}))
	defer sourceServer.Close()
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
			return
		}
		t.Fatalf("unexpected target backend request %s %s", r.Method, r.URL.String())
	}))
	defer targetServer.Close()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	sourceBackend, err := stateStore.CreateDataBackend(model.DataBackend{TenantID: tenant.ID, Name: "old-r2", Provider: model.DataBackendProviderCloudflareR2, Bucket: "bucket", Endpoint: sourceServer.URL, Region: "auto", Credentials: model.DataBackendCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}})
	if err != nil {
		t.Fatalf("create source backend: %v", err)
	}
	targetBackend, err := stateStore.CreateDataBackend(model.DataBackend{TenantID: tenant.ID, Name: "new-s3", Provider: model.DataBackendProviderS3, Bucket: "bucket", Endpoint: targetServer.URL, Region: "us-east-1", Credentials: model.DataBackendCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}})
	if err != nil {
		t.Fatalf("create target backend: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "workspace", StorageBackendID: targetBackend.ID})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	_, err = stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "v1",
		Manifest: model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "live.txt",
			Kind:         model.DataManifestEntryKindFile,
			Size:         4,
			SHA256:       liveDigest,
		}}}),
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	_, err = stateStore.CreateDataTransfer(model.DataTransfer{
		WorkspaceID: workspace.ID,
		Direction:   model.DataTransferDirectionMigrate,
		Status:      model.DataTransferStatusCompleted,
		Source:      sourceBackend.ID,
		Target:      targetBackend.ID,
	})
	if err != nil {
		t.Fatalf("create migration transfer: %v", err)
	}
	server := NewServer(stateStore, nil, nil, ServerConfig{})
	result, err := server.sweepDataWorkspaceGC(nil, workspace, 7, false)
	if err != nil {
		t.Fatalf("gc sweep: %v", err)
	}
	if !deletedOldBackend || result.Deleted != 1 || len(result.Candidates) != 1 || result.Candidates[0].Reason != "old-backend-unreferenced" {
		t.Fatalf("expected old backend orphan deletion, deleted=%t result=%+v", deletedOldBackend, result)
	}
	if result.Candidates[0].Key != orphanKey {
		t.Fatalf("expected orphan key candidate, got %+v", result.Candidates)
	}
}

func testDataDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func clearDefaultDataBackendEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"FUGUE_DATA_BACKEND_PROVIDER",
		"FUGUE_DATA_BACKEND_BUCKET",
		"FUGUE_DATA_BACKEND_ACCESS_KEY_ID",
		"FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY",
		"FUGUE_DATA_BACKEND_SESSION_TOKEN",
		"FUGUE_DATA_BACKEND_ENDPOINT",
		"FUGUE_DATA_R2_ACCOUNT_ID",
		"FUGUE_DATA_BACKEND_REGION",
		"FUGUE_DATA_BACKEND_PREFIX",
		"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY",
	} {
		t.Setenv(key, "")
	}
}

func touchDataBlob(storePath, digest string, when time.Time) error {
	blobPath := filepath.Join(filepath.Dir(storePath), "data-blobs", "sha256", digest[:2], digest[2:4], digest)
	return os.Chtimes(blobPath, when, when)
}
