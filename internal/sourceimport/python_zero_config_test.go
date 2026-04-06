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
