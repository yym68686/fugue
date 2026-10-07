package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUploadGCSessionActivityProtectsBothFilesAndBudget(t *testing.T) {
	c := &imageCache{storeDir: t.TempDir(), uploadTTL: time.Hour, uploadGCMaxBytes: 1, uploadOpenFiles: func() (map[string]bool, error) { return map[string]bool{}, nil }}
	state, err := c.createBlobUpload("demo", strings.NewReader("partial"))
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(c.blobUploadStatePath(state.UUID), old, old); err != nil {
		t.Fatal(err)
	}
	// Old JSON plus a recently written data file is one active resumable session.
	result, err := c.cleanupStaleBlobUploads(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 0 || len(result.Deleted) != 0 {
		t.Fatalf("fresh session split: %+v", result)
	}
	state.UpdatedAt = old
	state.CreatedAt = old
	if err := c.writeBlobUploadState(state); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{c.blobUploadStatePath(state.UUID), c.blobUploadDataPath(state.UUID)} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	result, err = c.cleanupStaleBlobUploads(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != 0 || len(result.Skipped) != 2 || result.Skipped[0].Reason != "upload_budget_exhausted" {
		t.Fatalf("budget split session: %+v", result)
	}
	c.uploadGCMaxBytes = 1 << 20
	result, err = c.cleanupStaleBlobUploads(time.Now())
	if err != nil || len(result.Deleted) != 2 {
		t.Fatalf("expired session not reclaimed: %+v %v", result, err)
	}
}

type partialUploadError struct{ read bool }

func (r *partialUploadError) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.ErrUnexpectedEOF
	}
	r.read = true
	return copy(p, []byte("partial")), io.ErrUnexpectedEOF
}
func TestUploadInterruptedAppendPersistsActualLength(t *testing.T) {
	c := &imageCache{storeDir: t.TempDir()}
	state, err := c.createBlobUpload("demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.appendBlobUpload("demo", state.UUID, &partialUploadError{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("lost transport error: %v", err)
	}
	state, err = c.readBlobUploadState(state.UUID)
	if err != nil || state.SizeBytes != 7 {
		t.Fatalf("partial length lost: %+v %v", state, err)
	}
	state, err = c.appendBlobUpload("demo", state.UUID, strings.NewReader("more"))
	if err != nil || state.SizeBytes != 11 {
		t.Fatalf("resume length corrupt: %+v %v", state, err)
	}
}
func TestUploadGCDoesNotRaceHTTPRequestOrTraverseSymlink(t *testing.T) {
	c := &imageCache{storeDir: t.TempDir(), uploadTTL: time.Hour, uploadOpenFiles: func() (map[string]bool, error) { return map[string]bool{}, nil }}
	state, err := c.createBlobUpload("demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		defer close(done)
		r := httptest.NewRequest(http.MethodPatch, "/v2/demo/blobs/uploads/"+state.UUID, reader)
		close(started)
		c.ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-started
	// Writing a byte guarantees the handler entered io.Copy under its read lock.
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	result, err := c.cleanupStaleBlobUploads(time.Now().Add(48 * time.Hour))
	if err != nil || len(result.Deleted) != 0 {
		t.Fatalf("removed in-flight upload: %+v %v", result, err)
	}
	writer.Close()
	<-done
	outside := filepath.Join(t.TempDir(), "outside")
	os.WriteFile(outside, []byte("keep"), 0600)
	os.Symlink(outside, filepath.Join(c.storeDir, "upload-symlink"))
	result, err = c.cleanupStaleBlobUploads(time.Now().Add(48 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(outside)
	if err != nil || !bytes.Equal(got, []byte("keep")) {
		t.Fatal("followed symlink")
	}
	for _, e := range result.Deleted {
		if e.Path == "upload-symlink" {
			t.Fatal("treated symlink as temporary regular file")
		}
	}
}
func TestImageCachePruneBindsMutableTagToExpectedDigest(t *testing.T) {
	if manifestMatchesPruneTarget(imageCacheManifestRecord{Repo: "demo", Target: "old", Digest: "sha256:" + strings.Repeat("b", 64)}, imageCachePruneRequest{repo: "demo", target: "old", digest: "sha256:" + strings.Repeat("a", 64)}) {
		t.Fatal("retargeted tag accepted stale delete authority")
	}
}
