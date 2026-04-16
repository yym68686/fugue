package sourceimport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"fugue/internal/model"
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

func TestValidateUploadedImportOutputRequiresImageRef(t *testing.T) {
	t.Parallel()

	err := validateUploadedImportOutput(model.AppBuildStrategyDockerfile, UploadSourceImportRequest{
		UploadID:        "upload_demo",
		ComposeService:  "gateway",
		DockerfilePath:  "Dockerfile",
		BuildContextDir: ".",
	}, GitHubSourceImportOutput{})
	if err == nil {
		t.Fatal("expected empty image ref to be rejected")
	}
	if !strings.Contains(err.Error(), "empty image ref") {
		t.Fatalf("expected empty image ref error, got %v", err)
	}
}

func TestValidateUploadedImportOutputRequiresBuilderEvidenceForBuilderStrategies(t *testing.T) {
	t.Parallel()

	err := validateUploadedImportOutput(model.AppBuildStrategyBuildpacks, UploadSourceImportRequest{
		UploadID:       "upload_demo",
		ComposeService: "runtime",
	}, GitHubSourceImportOutput{
		ImportResult: GitHubImportResult{
			ImageRef: "registry.push.example/fugue-apps/demo:upload-abc123",
		},
	})
	if err == nil {
		t.Fatal("expected missing builder job name to be rejected")
	}
	if !strings.Contains(err.Error(), "empty builder job name") {
		t.Fatalf("expected empty builder job name error, got %v", err)
	}
}

func TestValidateUploadedImportOutputAllowsStaticSiteWithoutBuilderEvidence(t *testing.T) {
	t.Parallel()

	err := validateUploadedImportOutput(model.AppBuildStrategyStaticSite, UploadSourceImportRequest{
		UploadID: "upload_demo",
	}, GitHubSourceImportOutput{
		ImportResult: GitHubImportResult{
			ImageRef: "registry.push.example/fugue-apps/demo:upload-abc123",
		},
	})
	if err != nil {
		t.Fatalf("expected static-site import output to be valid, got %v", err)
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
