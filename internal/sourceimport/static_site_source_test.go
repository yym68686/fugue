package sourceimport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetectAutoImportInputsPrefersReadyBuildOutputOverRootFrontendSource(t *testing.T) {
	repoDir := t.TempDir()
	writeTestFile(t, filepath.Join(repoDir, "index.html"), `<!doctype html><script type="module" src="/index.tsx"></script>`)
	writeTestFile(t, filepath.Join(repoDir, "index.tsx"), `console.log("source")`)
	writeTestFile(t, filepath.Join(repoDir, "package.json"), `{
  "name":"demo",
  "scripts":{"build":"vite build"},
  "dependencies":{"react":"19.0.0","react-dom":"19.0.0"},
  "devDependencies":{"vite":"6.0.0"}
}`)
	writeTestFile(t, filepath.Join(repoDir, "vite.config.ts"), `export default {}`)
	writeTestFile(t, filepath.Join(repoDir, "dist", "index.html"), `<!doctype html><script type="module" src="/assets/app.js"></script>`)

	buildStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(repoDir, "", "", "")
	if err != nil {
		t.Fatalf("detect auto inputs: %v", err)
	}
	if buildStrategy != "static-site" {
		t.Fatalf("expected static-site strategy, got %q", buildStrategy)
	}
	if sourceDir != "dist" {
		t.Fatalf("expected source dir dist, got %q", sourceDir)
	}
	if dockerfilePath != "" || buildContextDir != "" {
		t.Fatalf("expected no dockerfile inputs, got dockerfile=%q context=%q", dockerfilePath, buildContextDir)
	}
}

func TestPlanStaticSiteImportBuildsFrontendSource(t *testing.T) {
	repoDir := t.TempDir()
	writeTestFile(t, filepath.Join(repoDir, "index.html"), `<!doctype html><script type="module" src="/index.tsx"></script>`)
	writeTestFile(t, filepath.Join(repoDir, "index.tsx"), `console.log("source")`)
	writeTestFile(t, filepath.Join(repoDir, "package.json"), `{
  "name":"demo",
  "scripts":{"build":"vite build"},
  "dependencies":{"react":"19.0.0","react-dom":"19.0.0"},
  "devDependencies":{"vite":"6.0.0"}
}`)
	writeTestFile(t, filepath.Join(repoDir, "vite.config.ts"), `export default {}`)

	plan, err := planStaticSiteImport(repoDir, ".")
	if err != nil {
		t.Fatalf("plan static-site import: %v", err)
	}
	if plan.SourceDir != "" {
		t.Fatalf("expected root source dir to normalize to empty string, got %q", plan.SourceDir)
	}
	if plan.DetectedProvider != "nodejs" {
		t.Fatalf("expected nodejs provider, got %q", plan.DetectedProvider)
	}
	if plan.DetectedStack != "react" {
		t.Fatalf("expected react stack, got %q", plan.DetectedStack)
	}
	if plan.DockerfilePath != generatedStaticSiteDockerfilePath {
		t.Fatalf("expected generated dockerfile path %q, got %q", generatedStaticSiteDockerfilePath, plan.DockerfilePath)
	}
	if plan.BuildContextDir != "." {
		t.Fatalf("expected root build context, got %q", plan.BuildContextDir)
	}
	if len(plan.SourceOverlay) != 2 {
		t.Fatalf("expected two overlay files, got %d", len(plan.SourceOverlay))
	}
	content := plan.SourceOverlay[1].Content
	if !strings.Contains(content, "npm run build") {
		t.Fatalf("expected generated dockerfile to run build, got %q", content)
	}
	if !strings.Contains(content, "COPY --from=build /tmp/fugue-static-site/ /usr/share/caddy/") {
		t.Fatalf("expected generated dockerfile to copy built assets into caddy")
	}
}

func TestPlanStaticSiteImportLeavesReadyOutputUnbuilt(t *testing.T) {
	repoDir := t.TempDir()
	writeTestFile(t, filepath.Join(repoDir, "dist", "index.html"), `<!doctype html>`)

	plan, err := planStaticSiteImport(repoDir, "dist")
	if err != nil {
		t.Fatalf("plan static-site import: %v", err)
	}
	if plan.SourceDir != "dist" {
		t.Fatalf("expected dist source dir, got %q", plan.SourceDir)
	}
	if plan.DetectedProvider != "" {
		t.Fatalf("expected no detected provider for ready static output, got %q", plan.DetectedProvider)
	}
	if plan.DockerfilePath != "" || plan.BuildContextDir != "" || len(plan.SourceOverlay) != 0 {
		t.Fatalf("expected direct static packaging plan, got dockerfile=%q context=%q overlays=%d", plan.DockerfilePath, plan.BuildContextDir, len(plan.SourceOverlay))
	}
}

func TestShouldBuildStaticSiteSourceSkipsPlainRootStaticSite(t *testing.T) {
	repoDir := t.TempDir()
	writeTestFile(t, filepath.Join(repoDir, "index.html"), `<!doctype html><script src="/app.js"></script>`)
	writeTestFile(t, filepath.Join(repoDir, "app.js"), `console.log("plain")`)
	writeTestFile(t, filepath.Join(repoDir, "package.json"), `{"name":"demo","scripts":{"lint":"echo ok"}}`)

	if shouldBuildStaticSiteSource(repoDir, ".") {
		t.Fatal("expected plain root static site to skip source build")
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
