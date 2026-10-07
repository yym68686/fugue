package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// These tests own files only in this process. Observe real descriptors without
// requiring permission to inspect unrelated processes on a shared CI host.
func testUploadOpenFiles() (map[string]bool, error) {
	dir := "/proc/self/fd"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := map[string]bool{}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(dir, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result[target] = true
	}
	return result, nil
}

func TestImageCacheUploadGCRemovesExpiredLegacyAndResumableFiles(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("open-file protection is exercised on the Linux deployment host")
	}

	storeDir := t.TempDir()
	cache := &imageCache{storeDir: storeDir, uploadTTL: time.Hour, uploadOpenFiles: testUploadOpenFiles}
	if err := os.MkdirAll(cache.blobUploadDir(), 0o755); err != nil {
		t.Fatalf("mkdir upload dir: %v", err)
	}
	legacy := filepath.Join(storeDir, "upload-legacy")
	if err := os.WriteFile(legacy, []byte("legacy"), 0o600); err != nil {
		t.Fatalf("write legacy upload: %v", err)
	}
	resumable := "upload-resumable"
	if err := os.WriteFile(cache.blobUploadDataPath(resumable), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write resumable data: %v", err)
	}
	if err := cache.writeBlobUploadState(imageCacheBlobUploadState{
		Repo: "fugue-apps/demo", UUID: resumable,
		SizeBytes: 7, CreatedAt: time.Now().Add(-2 * time.Hour), UpdatedAt: time.Now().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("write resumable state: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, path := range []string{legacy, cache.blobUploadDataPath(resumable), cache.blobUploadStatePath(resumable)} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("age upload %s: %v", path, err)
		}
	}

	cleanup, err := cache.cleanupStaleBlobUploads(time.Now())
	if err != nil {
		t.Fatalf("cleanup uploads: %v", err)
	}
	if len(cleanup.Deleted) != 3 || cleanup.DeletedBytes <= 0 {
		t.Fatalf("deleted uploads = %+v", cleanup)
	}
	for _, path := range []string{legacy, cache.blobUploadDataPath(resumable), cache.blobUploadStatePath(resumable)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expired upload %s still exists, err=%v", path, err)
		}
	}
}

func TestImageCacheUploadGCDoesNotRemoveOpenUpload(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("open-file protection is exercised on the Linux deployment host")
	}

	storeDir := t.TempDir()
	cache := &imageCache{storeDir: storeDir, uploadTTL: time.Hour, uploadOpenFiles: testUploadOpenFiles}
	path := filepath.Join(storeDir, "upload-active")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open active upload: %v", err)
	}
	defer file.Close()
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("age active upload: %v", err)
	}
	cleanup, err := cache.cleanupStaleBlobUploads(time.Now())
	if err != nil {
		t.Fatalf("cleanup active upload: %v", err)
	}
	if len(cleanup.Deleted) != 0 || len(cleanup.Skipped) != 1 || cleanup.Skipped[0].Reason != "active_upload" {
		t.Fatalf("active upload cleanup = %+v", cleanup)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active upload was removed: %v", err)
	}
}

func TestImageCacheUploadGCRetainsFilesWhenObservationDenied(t *testing.T) {
	storeDir := t.TempDir()
	cache := &imageCache{storeDir: storeDir, uploadTTL: time.Hour, uploadOpenFiles: func() (map[string]bool, error) { return nil, os.ErrPermission }}
	name := filepath.Join(storeDir, "upload-retained")
	if err := os.WriteFile(name, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(name, old, old); err != nil {
		t.Fatal(err)
	}
	result, err := cache.cleanupStaleBlobUploads(time.Now())
	if !errors.Is(err, os.ErrPermission) || len(result.Deleted) != 0 {
		t.Fatalf("uncertain observation allowed deletion: %+v %v", result, err)
	}
	if _, err := os.Stat(name); err != nil {
		t.Fatal("protected upload missing", err)
	}
}
