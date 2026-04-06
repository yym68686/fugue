package sourceimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectDockerfilePortSignalMarksExposeAsPublic(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nEXPOSE 8080\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	port, exposesPublicService, err := detectDockerfilePortSignal(repoDir, "Dockerfile")
	if err != nil {
		t.Fatalf("detect dockerfile port signal: %v", err)
	}
	if port != 8080 || !exposesPublicService {
		t.Fatalf("unexpected dockerfile port signal: got %d/%t want 8080/true", port, exposesPublicService)
	}
}

func TestDetectDockerfilePortSignalFallsBackWithoutExpose(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\nCMD [\"/app\"]\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	port, exposesPublicService, err := detectDockerfilePortSignal(repoDir, "Dockerfile")
	if err != nil {
		t.Fatalf("detect dockerfile port signal: %v", err)
	}
	if port != 80 || exposesPublicService {
		t.Fatalf("unexpected dockerfile port signal: got %d/%t want 80/false", port, exposesPublicService)
	}
}

func TestShouldSuppressDetectedPublicServiceForDualModePythonProject(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM python:3.11-slim\nEXPOSE 8000\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "pyproject.toml"), []byte("[project]\nname='demo-bot'\n"), 0o644); err != nil {
		t.Fatalf("write pyproject.toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "bot.py"), []byte(`from telegram.ext import ApplicationBuilder
app = ApplicationBuilder().token("demo").build()

if WEB_HOOK:
    app.run_webhook("0.0.0.0", 8000, webhook_url=WEB_HOOK)
else:
    app.run_polling()
`), 0o644); err != nil {
		t.Fatalf("write bot.py: %v", err)
	}

	if !shouldSuppressDetectedPublicServiceForProject(repoDir, ".", "python") {
		t.Fatal("expected dual-mode python project to suppress auto public-service detection")
	}
}
