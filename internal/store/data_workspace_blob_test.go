package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDataBlobStoreRejectsNonCanonicalDigestsBeforeFilesystemAccess(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	stateStore := New(filepath.Join(root, "store.json"))
	validDigest := dataBlobTestDigest("valid")
	invalidDigests := []string{
		strings.ToUpper(validDigest),
		strings.Repeat("g", 64),
		"../../../../sensitive-file",
		strings.Repeat("a", 63),
	}

	for _, digest := range invalidDigests {
		digest := digest
		t.Run(digest, func(t *testing.T) {
			if _, err := stateStore.WriteDataBlob(digest, strings.NewReader("payload")); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("WriteDataBlob(%q) error = %v, want ErrInvalidInput", digest, err)
			}
			if _, _, err := stateStore.OpenDataBlob(digest); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("OpenDataBlob(%q) error = %v, want ErrInvalidInput", digest, err)
			}
			if err := stateStore.DeleteDataBlobDigest(digest); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("DeleteDataBlobDigest(%q) error = %v, want ErrInvalidInput", digest, err)
			}
			if stateStore.DataBlobExists(digest) {
				t.Fatalf("DataBlobExists(%q) = true, want false", digest)
			}
		})
	}

	if _, err := os.Stat(filepath.Join(root, "data-blobs")); !os.IsNotExist(err) {
		t.Fatalf("invalid digests touched the blob filesystem, stat error = %v", err)
	}
}

func TestWriteDataBlobExactEnforcesPlannedSizeBeforeCommit(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))

	tooLargeDigest := dataBlobTestDigest("abcdef")
	if _, err := stateStore.WriteDataBlobExact(tooLargeDigest, strings.NewReader("abcdef"), 5); !errors.Is(err, ErrDataBlobTooLarge) {
		t.Fatalf("oversized write error = %v, want ErrDataBlobTooLarge", err)
	}
	if stateStore.DataBlobExists(tooLargeDigest) {
		t.Fatal("oversized blob was committed")
	}

	tooSmallDigest := dataBlobTestDigest("abcd")
	if _, err := stateStore.WriteDataBlobExact(tooSmallDigest, strings.NewReader("abcd"), 5); !errors.Is(err, ErrDataBlobSizeMismatch) {
		t.Fatalf("short write error = %v, want ErrDataBlobSizeMismatch", err)
	}
	if stateStore.DataBlobExists(tooSmallDigest) {
		t.Fatal("short blob was committed")
	}

	content := "hello"
	digest := dataBlobTestDigest(content)
	written, err := stateStore.WriteDataBlobExact(digest, strings.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("exact write failed: %v", err)
	}
	if written != int64(len(content)) {
		t.Fatalf("written = %d, want %d", written, len(content))
	}
	file, info, err := stateStore.OpenDataBlob(digest)
	if err != nil {
		t.Fatalf("open exact blob: %v", err)
	}
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read exact blob: %v", err)
	}
	if string(got) != content || info.Size() != int64(len(content)) {
		t.Fatalf("stored blob = %q size=%d, want %q size=%d", got, info.Size(), content, len(content))
	}
}

func dataBlobTestDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
