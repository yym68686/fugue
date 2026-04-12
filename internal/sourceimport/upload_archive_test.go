package sourceimport

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractUploadArchiveSupportsZipAndNormalizesSingleRoot(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	extractedRoot, err := extractUploadArchive(rootDir, "demo-main.zip", mustUploadZipArchive(t, map[string]string{
		"demo-main/package.json":       "{\"name\":\"demo\"}\n",
		"demo-main/src/index.js":       "console.log('demo')\n",
		"demo-main/.DS_Store":          "ignored\n",
		"__MACOSX/._src/index.js":      "ignored\n",
		"demo-main/.well-known/ok.txt": "ok\n",
	}))
	if err != nil {
		t.Fatalf("extract upload archive: %v", err)
	}

	expectedRoot := filepath.Join(rootDir, "demo-main")
	if extractedRoot != expectedRoot {
		t.Fatalf("expected extracted root %q, got %q", expectedRoot, extractedRoot)
	}

	if _, err := os.Stat(filepath.Join(extractedRoot, "package.json")); err != nil {
		t.Fatalf("expected package.json in extracted root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(extractedRoot, "src", "index.js")); err != nil {
		t.Fatalf("expected src/index.js in extracted root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootDir, "__MACOSX")); !os.IsNotExist(err) {
		t.Fatalf("expected __MACOSX to be skipped, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(extractedRoot, ".DS_Store")); !os.IsNotExist(err) {
		t.Fatalf("expected .DS_Store to be skipped, got err=%v", err)
	}
}

func TestDetectUploadArchiveFormatRejectsMismatchedExtension(t *testing.T) {
	t.Parallel()

	if _, err := DetectUploadArchiveFormat("demo.zip", []byte{0x1f, 0x8b, 0x08, 0x00}); err == nil {
		t.Fatal("expected mismatched archive extension to fail")
	}
}

func mustUploadZipArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, content := range files {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatalf("create zip entry: %v", err)
		}
		if _, err := entry.Write([]byte(content)); err != nil {
			t.Fatalf("write zip entry: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buffer.Bytes()
}
