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
