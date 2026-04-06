package sourceimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectPrimaryTechStackPrefersNextjsOverReact(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "package.json"), []byte(`{
  "dependencies": {
    "next": "15.0.0",
    "react": "19.0.0",
    "react-dom": "19.0.0"
  }
}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	if got := detectPrimaryTechStack(repoDir, "."); got != "nextjs" {
		t.Fatalf("expected nextjs stack, got %q", got)
	}
}

func TestDetectPrimaryTechStackWalksUpFromStaticOutputDir(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "dist"), 0o755); err != nil {
		t.Fatalf("mkdir dist: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "dist", "index.html"), []byte("<html></html>"), 0o644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "package.json"), []byte(`{
  "dependencies": {
    "next": "15.0.0",
    "react": "19.0.0"
  }
}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	if got := detectPrimaryTechStack(repoDir, "dist"); got != "nextjs" {
		t.Fatalf("expected nextjs stack from dist source dir, got %q", got)
	}
}

func TestDetectPrimaryTechStackRecognizesPythonSourceWithoutManifest(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte("print('hello')\n"), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}

	if got := detectPrimaryTechStack(repoDir, "."); got != "python" {
		t.Fatalf("expected python stack, got %q", got)
	}
}
