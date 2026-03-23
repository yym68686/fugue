package sourceimport

import (
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestDetectAutoImportInputsPrefersDockerfile(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, "", "", "")
	if err != nil {
		t.Fatalf("detect auto inputs: %v", err)
	}
	if buildStrategy != model.AppBuildStrategyDockerfile {
		t.Fatalf("expected dockerfile strategy, got %q", buildStrategy)
	}
	if sourceDir != "" {
		t.Fatalf("expected empty source dir for dockerfile, got %q", sourceDir)
	}
	if dockerfilePath != "Dockerfile" {
		t.Fatalf("expected Dockerfile path, got %q", dockerfilePath)
	}
	if buildContextDir != "." {
		t.Fatalf("expected build context ., got %q", buildContextDir)
	}
}

func TestDetectAutoImportInputsUsesReadyStaticSite(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "dist"), 0o755); err != nil {
		t.Fatalf("mkdir dist: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "dist", "index.html"), []byte("<html></html>"), 0o644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, "", "", "")
	if err != nil {
		t.Fatalf("detect auto inputs: %v", err)
	}
	if buildStrategy != model.AppBuildStrategyStaticSite {
		t.Fatalf("expected static-site strategy, got %q", buildStrategy)
	}
	if sourceDir != "dist" {
		t.Fatalf("expected source dir dist, got %q", sourceDir)
	}
	if dockerfilePath != "" || buildContextDir != "" {
		t.Fatalf("expected no dockerfile inputs, got dockerfile=%q context=%q", dockerfilePath, buildContextDir)
	}
}

func TestDetectAutoImportInputsPrefersBuildpacksForSupportedApps(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "package.json"), []byte(`{"name":"demo"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, "", "", "")
	if err != nil {
		t.Fatalf("detect auto inputs: %v", err)
	}
	if buildStrategy != model.AppBuildStrategyBuildpacks {
		t.Fatalf("expected buildpacks strategy, got %q", buildStrategy)
	}
	if sourceDir != "." {
		t.Fatalf("expected source dir ., got %q", sourceDir)
	}
	if dockerfilePath != "" || buildContextDir != "" {
		t.Fatalf("expected no dockerfile inputs, got dockerfile=%q context=%q", dockerfilePath, buildContextDir)
	}
}

func TestDetectAutoImportInputsFallsBackToNixpacksForUnsupportedBuildpacksProvider(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Cargo.toml"), []byte(`[package]
name = "demo"
version = "0.1.0"
`), 0o644); err != nil {
		t.Fatalf("write Cargo.toml: %v", err)
	}

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, "", "", "")
	if err != nil {
		t.Fatalf("detect auto inputs: %v", err)
	}
	if buildStrategy != model.AppBuildStrategyNixpacks {
		t.Fatalf("expected nixpacks strategy, got %q", buildStrategy)
	}
	if sourceDir != "." {
		t.Fatalf("expected source dir ., got %q", sourceDir)
	}
	if dockerfilePath != "" || buildContextDir != "" {
		t.Fatalf("expected no dockerfile inputs, got dockerfile=%q context=%q", dockerfilePath, buildContextDir)
	}
}

func TestDetectNixpacksProviderAndPort(t *testing.T) {
	tests := []struct {
		name     string
		files    []string
		wantProv string
		wantPort int
	}{
		{name: "node", files: []string{"package.json"}, wantProv: "nodejs", wantPort: 3000},
		{name: "python", files: []string{"pyproject.toml"}, wantProv: "python", wantPort: 8000},
		{name: "go", files: []string{"go.mod"}, wantProv: "go", wantPort: 8080},
		{name: "generic", files: []string{"README.md"}, wantProv: "generic", wantPort: 3000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repoDir := t.TempDir()
			for _, file := range tt.files {
				if err := os.WriteFile(filepath.Join(repoDir, file), []byte("x"), 0o644); err != nil {
					t.Fatalf("write %s: %v", file, err)
				}
			}
			gotProv, gotPort := detectNixpacksProviderAndPort(repoDir, ".")
			if gotProv != tt.wantProv || gotPort != tt.wantPort {
				t.Fatalf("unexpected provider/port: got %s/%d want %s/%d", gotProv, gotPort, tt.wantProv, tt.wantPort)
			}
		})
	}
}
