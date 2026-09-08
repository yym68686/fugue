package dataprewarm

import (
	"context"
	"crypto/sha256"
	"fmt"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeCacheRequiresActualContentDigestAndCleansFailure(t *testing.T) {
	data := []byte("immutable fixture")
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	corrupt := false
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if corrupt {
			fmt.Fprint(w, "corrupt")
		} else {
			w.Write(data)
		}
	}))
	defer server.Close()
	manifest := model.NormalizeDataManifest(model.DataManifest{Entries: []model.DataManifestEntry{{AssetName: "dataset", Kind: "file", RelativePath: "../untrusted-path", Size: int64(len(data)), SHA256: digest}, {AssetName: "dataset", Kind: "file", RelativePath: "copy", Size: int64(len(data)), SHA256: digest}}})
	manifest.Digest = ManifestDigest(manifest)
	plan := Plan{TransferID: "transfer_test", Manifest: manifest, ExpiresAt: time.Now().Add(time.Hour), Blobs: []Blob{{SHA256: digest, Size: int64(len(data)), URL: server.URL}}}
	cache := t.TempDir()
	var progress Progress
	if err := Run(context.Background(), plan, cache, func(p Progress) { progress = p }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || progress.State != "ready" || progress.BytesDone != manifest.TotalBytes || progress.FilesDone != 2 {
		t.Fatal(calls, progress)
	}
	if _, err := os.Stat(filepath.Join(cache, "ready.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(cache), "untrusted-path")); !os.IsNotExist(err) {
		t.Fatal("manifest path was interpreted")
	}
	corrupt = true
	if err := Run(context.Background(), plan, cache, nil); err == nil {
		t.Fatal("corrupt cache marked ready")
	}
	if _, err := os.Stat(filepath.Join(cache, "ready.json")); !os.IsNotExist(err) {
		t.Fatal("failed attempt retained ready marker")
	}
	if _, err := os.Stat(filepath.Join(cache, "blobs")); !os.IsNotExist(err) {
		t.Fatal("failed attempt retained blobs")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, plan, t.TempDir(), nil); err == nil {
		t.Fatal("canceled transfer completed")
	}
}
