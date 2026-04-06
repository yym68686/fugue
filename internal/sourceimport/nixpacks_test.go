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

func TestDetectAutoImportInputsPrefersBuildpacksForPythonSourceWithoutManifest(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte(`import fastapi
import uvicorn

uvicorn.run(app, port=4684)
`), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
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
		name       string
		files      map[string]string
		wantProv   string
		wantPort   int
		wantPublic bool
	}{
		{
			name: "node-web",
			files: map[string]string{
				"package.json": `{"name":"demo","dependencies":{"next":"15.0.0"}}`,
			},
			wantProv:   "nodejs",
			wantPort:   3000,
			wantPublic: true,
		},
		{
			name: "node-worker",
			files: map[string]string{
				"package.json": `{"name":"demo","dependencies":{"telegraf":"4.0.0"}}`,
			},
			wantProv:   "nodejs",
			wantPort:   3000,
			wantPublic: false,
		},
		{
			name: "python-web",
			files: map[string]string{
				"main.py": `from fastapi import FastAPI
app = FastAPI()
`,
			},
			wantProv:   "python",
			wantPort:   8000,
			wantPublic: true,
		},
		{
			name: "python-worker",
			files: map[string]string{
				"main.py": `from telegram.ext import ApplicationBuilder
app = ApplicationBuilder().token("demo").build()
`,
			},
			wantProv:   "python",
			wantPort:   8000,
			wantPublic: false,
		},
		{
			name: "python-dual-mode-bot",
			files: map[string]string{
				"bot.py": `from telegram.ext import ApplicationBuilder
app = ApplicationBuilder().token("demo").build()

if WEB_HOOK:
    app.run_webhook("0.0.0.0", 8000, webhook_url=WEB_HOOK)
else:
    app.run_polling()
`,
				"pyproject.toml": "[project]\nname='demo-bot'\n",
			},
			wantProv:   "python",
			wantPort:   8000,
			wantPublic: false,
		},
		{
			name: "go-web",
			files: map[string]string{
				"go.mod":  "module example.com/demo\n",
				"main.go": "package main\nimport \"net/http\"\nfunc main(){ _ = http.ListenAndServe(\":8080\", nil) }\n",
			},
			wantProv:   "go",
			wantPort:   8080,
			wantPublic: true,
		},
		{
			name: "generic",
			files: map[string]string{
				"README.md": "demo",
			},
			wantProv:   "generic",
			wantPort:   3000,
			wantPublic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repoDir := t.TempDir()
			for file, content := range tt.files {
				fullPath := filepath.Join(repoDir, file)
				if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", filepath.Dir(fullPath), err)
				}
				if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
					t.Fatalf("write %s: %v", file, err)
				}
			}
			gotProv, gotPort, gotPublic := detectZeroConfigProviderAndPortSignal(repoDir, ".")
			if gotProv != tt.wantProv || gotPort != tt.wantPort || gotPublic != tt.wantPublic {
				t.Fatalf("unexpected provider/port/public: got %s/%d/%t want %s/%d/%t", gotProv, gotPort, gotPublic, tt.wantProv, tt.wantPort, tt.wantPublic)
			}
		})
	}
}
