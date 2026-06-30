package sourceimport

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAnalyzePythonProjectInfersRequirementsAndPortFromSource(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "proxy_server.py"), []byte(`import asyncio
import json
from fastapi import FastAPI
from pydantic import BaseModel
import aiohttp
import uvicorn

from token_manager import TokenManager
from immersive_glm_manager import ImmersiveTranslateGLMManager

uvicorn.run(app, host="0.0.0.0", port=4684)
`), 0o644); err != nil {
		t.Fatalf("write proxy_server.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "token_manager.py"), []byte("class TokenManager: pass\n"), 0o644); err != nil {
		t.Fatalf("write token_manager.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "immersive_glm_manager.py"), []byte("import cloudscraper\n"), 0o644); err != nil {
		t.Fatalf("write immersive_glm_manager.py: %v", err)
	}

	analysis, err := analyzePythonProject(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze python project: %v", err)
	}
	if !analysis.IsPythonProject {
		t.Fatal("expected python project to be detected")
	}
	if analysis.HasDependencyManifest {
		t.Fatal("expected project without dependency manifest")
	}
	if analysis.DetectedPort != 4684 {
		t.Fatalf("expected detected port 4684, got %d", analysis.DetectedPort)
	}

	wantRequirements := []string{"aiohttp", "cloudscraper", "fastapi", "pydantic", "uvicorn"}
	if !slices.Equal(analysis.InferredRequirements, wantRequirements) {
		t.Fatalf("unexpected inferred requirements: got %v want %v", analysis.InferredRequirements, wantRequirements)
	}
}

func TestBuildPythonOverlayFilesGeneratesRequirementsManifest(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte("import requests\n"), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}

	files, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !analysis.IsPythonProject {
		t.Fatal("expected python project analysis")
	}
	if len(files) != 1 {
		t.Fatalf("expected one overlay file, got %d", len(files))
	}
	if files[0].RelativePath != "requirements.txt" {
		t.Fatalf("expected requirements.txt overlay, got %q", files[0].RelativePath)
	}
	if !files[0].OnlyIfMissing {
		t.Fatal("expected generated requirements.txt to only write when missing")
	}
	if !strings.Contains(files[0].Content, "requests") {
		t.Fatalf("expected generated requirements to contain requests, got %q", files[0].Content)
	}
}

func TestInferPythonRequirementsSkipsAdditionalStdlibModules(t *testing.T) {
	t.Parallel()

	imports := map[string]struct{}{
		"atexit":     {},
		"fastapi":    {},
		"github":     {},
		"locale":     {},
		"py_compile": {},
		"tomllib":    {},
	}

	got := inferPythonRequirements(imports, nil)
	want := []string{"PyGithub", "fastapi"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected inferred requirements: got %v want %v", got, want)
	}
}

func TestBuildPythonOverlayFilesSkipsTestOnlyImports(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte("from fastapi import FastAPI\n"), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}
	testsDir := filepath.Join(repoDir, "tests")
	if err := os.MkdirAll(testsDir, 0o755); err != nil {
		t.Fatalf("create tests dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(testsDir, "test_compliance_checks.py"), []byte("from check_constraints import check_constraints\n"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	files, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !slices.Equal(analysis.InferredRequirements, []string{"fastapi"}) {
		t.Fatalf("unexpected inferred requirements: got %v want %v", analysis.InferredRequirements, []string{"fastapi"})
	}
	if len(files) == 0 || files[0].RelativePath != "requirements.txt" {
		t.Fatalf("expected requirements.txt overlay, got %v", files)
	}
	if strings.Contains(files[0].Content, "check_constraints") {
		t.Fatalf("expected test-only import to be excluded from requirements, got %q", files[0].Content)
	}
}

func TestBuildPythonOverlayFilesSkipsArchivedRuntimeImports(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte("from fastapi import FastAPI\n"), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}
	oldDir := filepath.Join(repoDir, "old")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatalf("create old dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "legacy_service.py"), []byte("import missing_legacy_dependency\n"), 0o644); err != nil {
		t.Fatalf("write archived service: %v", err)
	}

	_, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !slices.Equal(analysis.InferredRequirements, []string{"fastapi"}) {
		t.Fatalf("unexpected inferred requirements: got %v want %v", analysis.InferredRequirements, []string{"fastapi"})
	}
}

func TestBuildPythonOverlayFilesTreatsNestedSrcPackagesAsLocal(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	packageDir := filepath.Join(repoDir, "services", "api", "src", "local_service")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("create package dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "__init__.py"), []byte(""), 0o644); err != nil {
		t.Fatalf("write package init: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "main.py"), []byte("from local_service.api import router\nfrom fastapi import FastAPI\n"), 0o644); err != nil {
		t.Fatalf("write package main: %v", err)
	}

	_, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !slices.Equal(analysis.InferredRequirements, []string{"fastapi"}) {
		t.Fatalf("unexpected inferred requirements: got %v want %v", analysis.InferredRequirements, []string{"fastapi"})
	}
}

func TestBuildPythonOverlayFilesSkipsProjectsWithExplicitManifest(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("fastapi\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte("import fastapi\n"), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}

	files, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !analysis.HasDependencyManifest {
		t.Fatal("expected explicit manifest to be detected")
	}
	if len(files) != 0 {
		t.Fatalf("expected no generated overlay files, got %d", len(files))
	}
}

func TestBuildPythonOverlayFilesGeneratesProcfileForPollingEntrypointWithManifest(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("aiogram==3.25.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte(`import asyncio

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
`), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}

	files, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if got := analysis.SuggestedStartCommand; got != "python main.py" {
		t.Fatalf("expected suggested startup command %q, got %q", "python main.py", got)
	}
	if len(files) != 1 {
		t.Fatalf("expected one overlay file, got %d", len(files))
	}
	if files[0].RelativePath != "Procfile" {
		t.Fatalf("expected Procfile overlay, got %q", files[0].RelativePath)
	}
	if files[0].Content != "web: python main.py\n" {
		t.Fatalf("unexpected Procfile content: %q", files[0].Content)
	}
	if !files[0].OnlyIfMissing {
		t.Fatal("expected generated Procfile to only write when missing")
	}
}

func TestBuildPythonOverlayFilesKeepsExplicitProcfile(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("aiogram==3.25.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "Procfile"), []byte("web: custom\n"), 0o644); err != nil {
		t.Fatalf("write Procfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte(`if __name__ == "__main__":
    app.run_polling()
`), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}

	files, analysis, err := buildPythonOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build python overlay files: %v", err)
	}
	if !analysis.HasProcfile {
		t.Fatal("expected explicit Procfile to be detected")
	}
	if len(files) != 0 {
		t.Fatalf("expected no overlay files when Procfile already exists, got %d", len(files))
	}
}

func TestAnalyzePythonProjectSuggestsExecutableWebScriptStartupCommand(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "app.py"), []byte(`from flask import Flask

app = Flask(__name__)

@app.route("/")
def index():
    return "ok"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
`), 0o644); err != nil {
		t.Fatalf("write app.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("flask\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}

	analysis, err := analyzePythonProject(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze python project: %v", err)
	}
	if got := analysis.SuggestedStartCommand; got != "python app.py" {
		t.Fatalf("expected suggested startup command %q, got %q", "python app.py", got)
	}
	if analysis.DetectedPort != 5000 {
		t.Fatalf("expected detected port 5000, got %d", analysis.DetectedPort)
	}
}

func TestAnalyzePythonProjectSuggestsPollingScriptStartupCommand(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "main.py"), []byte(`import asyncio

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
`), 0o644); err != nil {
		t.Fatalf("write main.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("aiogram\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}

	analysis, err := analyzePythonProject(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze python project: %v", err)
	}
	if got := analysis.SuggestedStartCommand; got != "python main.py" {
		t.Fatalf("expected suggested startup command %q, got %q", "python main.py", got)
	}
	if !analysis.HasPollingEntrypoint {
		t.Fatal("expected polling entrypoint to be detected")
	}
}

func TestAnalyzePythonProjectSuggestsFlaskModuleStartupCommandWithoutMainGuard(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "service.py"), []byte(`from flask import Flask

application = Flask(__name__)

@application.route("/")
def index():
    return "ok"
`), 0o644); err != nil {
		t.Fatalf("write service.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "requirements.txt"), []byte("flask\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}

	analysis, err := analyzePythonProject(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze python project: %v", err)
	}
	want := "python -m flask --app service:application run --host 0.0.0.0 --port ${PORT:-8000}"
	if got := analysis.SuggestedStartCommand; got != want {
		t.Fatalf("expected suggested startup command %q, got %q", want, got)
	}
}

func TestAnalyzePythonProjectDetectsDualWebhookAndPollingModes(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "bot.py"), []byte(`from telegram.ext import ApplicationBuilder

application = ApplicationBuilder().token("demo").build()

if WEB_HOOK:
    application.run_webhook("0.0.0.0", 8000, webhook_url=WEB_HOOK)
else:
    application.run_polling()
`), 0o644); err != nil {
		t.Fatalf("write bot.py: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "pyproject.toml"), []byte("[project]\nname='demo-bot'\n"), 0o644); err != nil {
		t.Fatalf("write pyproject.toml: %v", err)
	}

	analysis, err := analyzePythonProject(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze python project: %v", err)
	}
	if !analysis.HasWebhookEntrypoint {
		t.Fatal("expected webhook entrypoint to be detected")
	}
	if !analysis.HasPollingEntrypoint {
		t.Fatal("expected polling entrypoint to be detected")
	}
	if !pythonProjectPrefersBackgroundNetwork(analysis) {
		t.Fatal("expected dual-mode bot to prefer background network")
	}
}
