package sourceimport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"testing"
)

func TestExtractUploadedArchiveDerivesDefaultAppNameFromArchiveFilenameWhenAppNameBlank(t *testing.T) {
	t.Parallel()

	importer := Importer{
		WorkDir: t.TempDir(),
	}
	archiveBytes := mustUploadTestTarGz(t, map[string]string{
		"index.html": "<h1>demo</h1>\n",
	})

	src, err := importer.extractUploadedArchive(UploadSourceImportRequest{
		UploadID:         "upload_test",
		ArchiveFilename:  "demo-main.tgz",
		ArchiveSHA256:    "sha256-demo",
		ArchiveSizeBytes: int64(len(archiveBytes)),
		ArchiveData:      archiveBytes,
	})
	if err != nil {
		t.Fatalf("extract uploaded archive: %v", err)
	}
	t.Cleanup(func() {
		releaseExtractedUploadSource(src)
	})

	if src.DefaultAppName != "demo-main" {
		t.Fatalf("expected derived default app name demo-main, got %q", src.DefaultAppName)
	}
}

func TestDefaultUploadedImageRefAvoidsDuplicateServiceSuffix(t *testing.T) {
	t.Parallel()

	got := defaultUploadedImageRef(
		"registry.push.example",
		"fugue-apps",
		"argus-runtime",
		"abcdef1234567890",
		"runtime",
	)
	want := "registry.push.example/fugue-apps/argus-runtime:upload-abcdef123456"
	if got != want {
		t.Fatalf("expected upload image ref %q, got %q", want, got)
	}
}

func mustUploadTestTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	for name, content := range files {
		header := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if _, err := tarWriter.Write([]byte(content)); err != nil {
			t.Fatalf("write tar content: %v", err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buffer.Bytes()
}
