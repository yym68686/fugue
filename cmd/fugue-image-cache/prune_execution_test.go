package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
)

func newPruneExecutionFixture(t *testing.T) (*imageCache, string, []string) {
	t.Helper()
	root := t.TempDir()
	c := &imageCache{
		storeDir: root, manifestDir: filepath.Join(root, "_manifests"), pinStorePath: filepath.Join(root, "_pins.json"),
		registry:  registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(root))),
		diskLimit: imageCacheDiskLimit{Enabled: true, HighWatermarkPercent: 100, LowWatermarkPercent: 99, MaxDeleteBytesPerRun: 1 << 30},
	}
	blobs := []string{writeTestImageCacheBlob(t, root, []byte(`{"config":"prune"}`)), writeTestImageCacheBlob(t, root, []byte("prune-layer"))}
	body := testImageCacheManifest(blobs[0], blobs[1])
	request := httptest.NewRequest(http.MethodPut, "/v2/fugue-apps/synthetic/manifests/build", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	response := httptest.NewRecorder()
	c.serveRegistryWrite(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create image: %d %s", response.Code, response.Body.String())
	}
	return c, manifestBodyDigest([]byte(body)), blobs
}

func fixturePrunePlan(t *testing.T, c *imageCache, digest string) imageCachePrunePlan {
	t.Helper()
	records, err := c.managementManifestRecords()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := c.planImageCachePrune(imageCachePruneRequest{
		targets:     []imageCachePruneTarget{{Repo: "fugue-apps/synthetic", Target: "build", Digest: digest}, {Repo: "fugue-apps/synthetic", Target: digest, Digest: digest}},
		allowDelete: true, includeUnreferencedBlobs: true, requestMaxBytes: 1 << 30,
	}, records, nil)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func requirePruneBlobs(t *testing.T, c *imageCache, blobs []string, exist bool) {
	t.Helper()
	for _, digest := range blobs {
		_, err := os.Stat(imageCacheBlobPath(c.storeDir, digest))
		if exist && err != nil {
			t.Fatalf("required blob %s disappeared: %v", digest, err)
		}
		if !exist && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unreferenced blob %s remains: %v", digest, err)
		}
	}
}

func TestPruneExecutionBothAliasOrdersAndRestart(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "tag_first", true: "digest_first"}[reverse], func(t *testing.T) {
			t.Parallel()
			c, digest, blobs := newPruneExecutionFixture(t)
			plan := fixturePrunePlan(t, c, digest)
			if len(plan.deleteManifests) != 2 {
				t.Fatalf("expected both aliases: %+v", plan)
			}
			if reverse {
				plan.deleteManifests[0], plan.deleteManifests[1] = plan.deleteManifests[1], plan.deleteManifests[0]
			}
			if err := c.executeImageCachePrunePlan(&plan); err != nil {
				t.Fatal(err)
			}
			requirePruneBlobs(t, c, blobs, false)
			if plan.DeletedBytes != plan.PlannedDeleteBytes || plan.DeletedBlobBytes == 0 || !plan.RemainingSnapshotValid || plan.RemainingUnreferencedBytes != 0 {
				t.Fatalf("incomplete reclamation: %+v", plan)
			}
			c.registry = registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(c.storeDir)))
			if err := c.loadPersistedManifests(); err != nil {
				t.Fatal(err)
			}
			if c.localManifestAvailable("fugue-apps/synthetic", digest) {
				t.Fatal("deleted image returned after restart")
			}
		})
	}
}

func TestPruneExecutionRetainsSharedLayersAcrossRestart(t *testing.T) {
	t.Parallel()
	c, digest, blobs := newPruneExecutionFixture(t)
	liveConfig := writeTestImageCacheBlob(t, c.storeDir, []byte(`{"config":"live"}`))
	body := testImageCacheManifest(liveConfig, blobs[1])
	req := httptest.NewRequest(http.MethodPut, "/v2/fugue-apps/retained/manifests/current", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	rec := httptest.NewRecorder()
	c.serveRegistryWrite(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("retained put: %d", rec.Code)
	}
	plan := fixturePrunePlan(t, c, digest)
	if err := c.executeImageCachePrunePlan(&plan); err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, []string{blobs[0]}, false)
	requirePruneBlobs(t, c, []string{liveConfig, blobs[1]}, true)
	c.registry = registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(c.storeDir)))
	if err := c.loadPersistedManifests(); err != nil {
		t.Fatal(err)
	}
	if !c.localManifestAvailable("fugue-apps/retained", "current") {
		t.Fatal("retained image unavailable after restart")
	}
}

func TestPruneExecutionPinProtectsEveryAlias(t *testing.T) {
	t.Parallel()
	c, digest, blobs := newPruneExecutionFixture(t)
	if err := c.writePins(cachePinsFile{Pins: []cachePin{{Repo: "fugue-apps/synthetic", Target: "build", Digest: digest}}}); err != nil {
		t.Fatal(err)
	}
	plan := fixturePrunePlan(t, c, digest)
	if err := c.executeImageCachePrunePlan(&plan); err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, blobs, true)
	records, err := c.managementManifestRecords()
	if err != nil || len(records) != 2 || plan.Deleted {
		t.Fatalf("pin failed to protect aliases: %v %+v", err, plan)
	}
}

func TestPruneExecutionReferenceFailureRetainsBlobs(t *testing.T) {
	t.Parallel()
	c, digest, blobs := newPruneExecutionFixture(t)
	plan := fixturePrunePlan(t, c, digest)
	underlying := c.registry
	c.registry = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Error(w, "injected unavailable", http.StatusServiceUnavailable)
			return
		}
		underlying.ServeHTTP(w, r)
	})
	if err := c.executeImageCachePrunePlan(&plan); err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, blobs, true)
	if plan.ExecutionStatus != "partial" || plan.SkippedReason != "reference_check_failed" || plan.SkippedBlobBytes == 0 || plan.RemainingSnapshotValid {
		t.Fatalf("uncertainty not reported: %+v", plan)
	}
}

func TestPruneExecutionNewWriterDoesNotWaitForReferenceScan(t *testing.T) {
	t.Parallel()
	c, digest, blobs := newPruneExecutionFixture(t)
	plan := fixturePrunePlan(t, c, digest)
	underlying := c.registry
	scanning, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.registry = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			once.Do(func() { close(scanning); <-resume })
		}
		underlying.ServeHTTP(w, r)
	})
	done := make(chan error, 1)
	go func() { done <- c.executeImageCachePrunePlan(&plan) }()
	select {
	case <-scanning:
	case <-time.After(5 * time.Second):
		t.Fatal("reference scan not reached")
	}
	writerDone := make(chan struct{})
	go func() {
		body := testImageCacheManifest(blobs[0], blobs[1])
		req := httptest.NewRequest(http.MethodPut, "/v2/fugue-apps/new-reference/manifests/current", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		c.serveRegistryWrite(httptest.NewRecorder(), req)
		close(writerDone)
	}()
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("writer blocked on GC scan")
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, blobs, true)
	if plan.ExecutionStatus != "partial" || plan.SkippedReason != "content_changed" || plan.RemainingSnapshotValid {
		t.Fatalf("changed observation not deferred: %+v", plan)
	}
}

func TestPruneExecutionPartialManifestFailureCanRetry(t *testing.T) {
	t.Parallel()
	c, digest, blobs := newPruneExecutionFixture(t)
	plan := fixturePrunePlan(t, c, digest)
	underlying := c.registry
	deletes := 0
	c.registry = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes++
			if deletes == 2 {
				http.Error(w, "injected failure", http.StatusInternalServerError)
				return
			}
		}
		underlying.ServeHTTP(w, r)
	})
	if err := c.executeImageCachePrunePlan(&plan); err == nil {
		t.Fatal("expected manifest delete failure")
	}
	requirePruneBlobs(t, c, blobs, true)
	if plan.ExecutionStatus != "failed" || plan.DeletedManifestBytes == 0 || plan.DeletedBlobBytes != 0 {
		t.Fatalf("lost partial progress: %+v", plan)
	}
	c.registry = underlying
	retry := fixturePrunePlan(t, c, digest)
	if err := c.executeImageCachePrunePlan(&retry); err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, blobs, false)
}

func TestPruneExecutionReceiptMatchesActualFiles(t *testing.T) {
	t.Parallel()
	c, digest, _ := newPruneExecutionFixture(t)
	body, _ := json.Marshal(map[string]any{"dry_run": false, "allow_delete": true, "targets": []imageCachePruneTarget{{Repo: "fugue-apps/synthetic", Target: "build", Digest: digest}, {Repo: "fugue-apps/synthetic", Target: digest, Digest: digest}}, "max_delete_bytes": "1Gi"})
	rec := httptest.NewRecorder()
	c.handleManagementPrune(rec, httptest.NewRequest(http.MethodPost, "/fugue/cache/v1/prune", strings.NewReader(string(body))))
	var receipt imageCachePrunePlan
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &receipt) != nil {
		t.Fatalf("invalid receipt: %d %s", rec.Code, rec.Body.String())
	}
	if receipt.ExecutionStatus != "completed" || receipt.DeletedBytes != receipt.DeletedBlobBytes+receipt.DeletedManifestBytes || receipt.DeletedBlobBytes == 0 || !receipt.RemainingSnapshotValid {
		t.Fatalf("misleading receipt: %+v", receipt)
	}
}

func TestPruneExecutionDoesNotSweepUnadmittedUploadBlobs(t *testing.T) {
	t.Parallel()
	c, digest, _ := newPruneExecutionFixture(t)
	admitted := writeTestImageCacheBlob(t, c.storeDir, []byte("old-orphan"))
	fresh := writeTestImageCacheBlob(t, c.storeDir, []byte("new-upload-awaiting-manifest"))
	records, err := c.managementManifestRecords()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := c.planImageCachePrune(imageCachePruneRequest{
		targets:     []imageCachePruneTarget{{Repo: "fugue-apps/synthetic", Target: "build", Digest: digest}, {Repo: "fugue-apps/synthetic", Target: digest, Digest: digest}},
		allowDelete: true, includeUnreferencedBlobs: true, unreferencedBlobDigests: []string{admitted}, requestMaxBytes: 1 << 30,
	}, records, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.executeImageCachePrunePlan(&plan); err != nil {
		t.Fatal(err)
	}
	requirePruneBlobs(t, c, []string{admitted}, false)
	requirePruneBlobs(t, c, []string{fresh}, true)
}
