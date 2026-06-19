package registrymaintenance

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanCountsManifestReachableAndUnreferencedBlobs(t *testing.T) {
	root := t.TempDir()
	configDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	layerDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	manifestDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	orphanDigest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	writeBlob(t, root, configDigest, []byte(`{"architecture":"amd64"}`))
	writeBlob(t, root, layerDigest, []byte("layer"))
	writeBlob(t, root, manifestDigest, []byte(`{"config":{"digest":"`+configDigest+`"},"layers":[{"digest":"`+layerDigest+`"}]}`))
	writeBlob(t, root, orphanDigest, []byte("orphan"))

	revisionPath := filepath.Join(root, "repositories", "fugue-apps", "demo", "_manifests", "revisions", "sha256", manifestDigest[7:], "link")
	if err := os.MkdirAll(filepath.Dir(revisionPath), 0o755); err != nil {
		t.Fatalf("mkdir revision: %v", err)
	}
	if err := os.WriteFile(revisionPath, []byte(manifestDigest), 0o600); err != nil {
		t.Fatalf("write revision: %v", err)
	}

	result, err := Scan(root, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.BlobCount != 4 || result.ReferencedBlobCount != 3 || result.UnreferencedBlobCount != 1 {
		t.Fatalf("unexpected scan result: %+v", result)
	}
	if result.UnreferencedBlobBytes != int64(len("orphan")) {
		t.Fatalf("expected orphan bytes %d, got %+v", len("orphan"), result)
	}
}

func TestScanAddsWorkloadDigestToKeepSet(t *testing.T) {
	root := t.TempDir()
	manifestDigest := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	layerDigest := "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	writeBlob(t, root, layerDigest, []byte("kept-layer"))
	writeBlob(t, root, manifestDigest, []byte(`{"layers":[{"digest":"`+layerDigest+`"}]}`))

	result, err := Scan(root, []string{manifestDigest})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.KeepDigestCount != 1 || result.MissingKeepDigestCount != 0 || result.UnreferencedBlobCount != 0 {
		t.Fatalf("unexpected keep-set result: %+v", result)
	}
}

func TestScanDoesNotTraverseLayerBlobsAsManifests(t *testing.T) {
	root := t.TempDir()
	configDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	layerDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	manifestDigest := "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	orphanDigest := "sha256:4444444444444444444444444444444444444444444444444444444444444444"

	writeBlob(t, root, configDigest, []byte(`{"architecture":"amd64"}`))
	writeBlob(t, root, layerDigest, []byte(`{"layers":[{"digest":"`+orphanDigest+`"}]}`))
	writeBlob(t, root, orphanDigest, []byte("orphan"))
	writeBlob(t, root, manifestDigest, []byte(`{"config":{"digest":"`+configDigest+`"},"layers":[{"digest":"`+layerDigest+`"}]}`))

	revisionPath := filepath.Join(root, "repositories", "fugue-apps", "demo", "_manifests", "revisions", "sha256", manifestDigest[7:], "link")
	if err := os.MkdirAll(filepath.Dir(revisionPath), 0o755); err != nil {
		t.Fatalf("mkdir revision: %v", err)
	}
	if err := os.WriteFile(revisionPath, []byte(manifestDigest), 0o600); err != nil {
		t.Fatalf("write revision: %v", err)
	}

	result, err := Scan(root, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if result.ReferencedBlobCount != 3 || result.UnreferencedBlobCount != 1 {
		t.Fatalf("scan should not traverse layer JSON as a manifest: %+v", result)
	}
	if result.UnreferencedBlobBytes != int64(len("orphan")) {
		t.Fatalf("expected orphan bytes %d, got %+v", len("orphan"), result)
	}
}

func writeBlob(t *testing.T, root, digest string, data []byte) {
	t.Helper()
	hexDigest := digest[7:]
	path := filepath.Join(root, "blobs", "sha256", hexDigest[:2], hexDigest, "data")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir blob: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
}
