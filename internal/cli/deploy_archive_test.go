package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateSourceArchiveHonorsDockerignoreAndDefaultSkips(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mustWriteArchiveTestFile(t, root, ".dockerignore", strings.TrimSpace(`
# local-only files
tests
*.pyc
!important.pyc
`)+"\n")
	mustWriteArchiveTestFile(t, root, "Dockerfile", "FROM scratch\n")
	mustWriteArchiveTestFile(t, root, "app.py", "print('ok')\n")
	mustWriteArchiveTestFile(t, root, "ignored.pyc", "bytecode")
	mustWriteArchiveTestFile(t, root, "important.pyc", "keep")
	mustWriteArchiveTestFile(t, root, "tests/test_app.py", "def test_app(): pass\n")
	mustWriteArchiveTestFile(t, root, ".venv/pyvenv.cfg", "home = /tmp/python\n")
	mustWriteArchiveTestFile(t, root, ".venv-local/pyvenv.cfg", "home = /tmp/python\n")
	mustWriteArchiveTestFile(t, root, ".codex-test-venv/pyvenv.cfg", "home = /tmp/python\n")
	mustWriteArchiveTestFile(t, root, ".pytest_cache/CACHEDIR.TAG", "Signature: 8a477f597d28d172789f06886806bc55\n")
	mustWriteArchiveTestFile(t, root, "demo.egg-info/PKG-INFO", "Metadata-Version: 2.4\n")

	archiveBytes, archiveName, err := createSourceArchive(root, "demo")
	if err != nil {
		t.Fatalf("create source archive: %v", err)
	}
	if archiveName != "demo.tgz" {
		t.Fatalf("expected archive name demo.tgz, got %q", archiveName)
	}
	entries := listArchiveTestEntries(t, archiveBytes)

	for _, want := range []string{"Dockerfile", ".dockerignore", "app.py", "important.pyc"} {
		if !entries[want] {
			t.Fatalf("expected archive to contain %q, got entries %#v", want, entries)
		}
	}
	for _, unwanted := range []string{
		"ignored.pyc",
		"tests/",
		"tests/test_app.py",
		".venv/",
		".venv/pyvenv.cfg",
		".venv-local/",
		".venv-local/pyvenv.cfg",
		".codex-test-venv/",
		".codex-test-venv/pyvenv.cfg",
		".pytest_cache/",
		".pytest_cache/CACHEDIR.TAG",
		"demo.egg-info/",
		"demo.egg-info/PKG-INFO",
	} {
		if entries[unwanted] {
			t.Fatalf("expected archive to exclude %q, got entries %#v", unwanted, entries)
		}
	}
}

func TestValidateSourceArchiveSizeExplainsUploadLimit(t *testing.T) {
	t.Parallel()

	err := validateSourceArchiveSize("demo.tgz", 129, 128)
	if err == nil {
		t.Fatal("expected archive size error")
	}
	message := err.Error()
	for _, want := range []string{"demo.tgz", "129 bytes", "128 bytes", ".dockerignore"} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected error %q to contain %q", message, want)
		}
	}
	if err := validateSourceArchiveSize("demo.tgz", 128, 128); err != nil {
		t.Fatalf("expected archive at limit to pass, got %v", err)
	}
}

func mustWriteArchiveTestFile(t *testing.T, root, relPath, content string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir parent for %s: %v", relPath, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", relPath, err)
	}
}

func listArchiveTestEntries(t *testing.T, archiveBytes []byte) map[string]bool {
	t.Helper()
	gzipReader, err := gzip.NewReader(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatalf("open gzip: %v", err)
	}
	defer func() {
		_ = gzipReader.Close()
	}()

	entries := map[string]bool{}
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar entry: %v", err)
		}
		entries[header.Name] = true
	}
	return entries
}
