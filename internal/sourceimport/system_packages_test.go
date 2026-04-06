package sourceimport

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAnalyzeSystemPackagesInfersGitFromPythonImports(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "app.py"), []byte(`from git import Repo

def clone(url, dst):
    return Repo.clone_from(url, dst)
`), 0o644); err != nil {
		t.Fatalf("write app.py: %v", err)
	}

	analysis, err := analyzeSystemPackages(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze system packages: %v", err)
	}
	if got, want := analysis.Packages, []string{"git"}; !slices.Equal(got, want) {
		t.Fatalf("unexpected inferred packages: got %v want %v", got, want)
	}
}

func TestAnalyzeSystemPackagesInfersKnownCommandsAcrossLanguages(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "worker.ts"), []byte(`import { spawn } from "node:child_process";

export function probe(input: string) {
  return spawn("ffprobe", ["-v", "error", input]);
}
`), 0o644); err != nil {
		t.Fatalf("write worker.ts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "sync.go"), []byte(`package main

import "os/exec"

func main() {
  exec.Command("git", "status")
}
`), 0o644); err != nil {
		t.Fatalf("write sync.go: %v", err)
	}

	analysis, err := analyzeSystemPackages(repoDir, ".")
	if err != nil {
		t.Fatalf("analyze system packages: %v", err)
	}
	if got, want := analysis.Packages, []string{"ffmpeg", "git"}; !slices.Equal(got, want) {
		t.Fatalf("unexpected inferred packages: got %v want %v", got, want)
	}
}

func TestBuildBuildpacksSystemPackageOverlayFilesGeneratesAptfileWhenMissing(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "app.py"), []byte("from git import Repo\n"), 0o644); err != nil {
		t.Fatalf("write app.py: %v", err)
	}

	files, analysis, err := buildBuildpacksSystemPackageOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build buildpacks system package overlay files: %v", err)
	}
	if analysis.HasExplicitBuildpackApt {
		t.Fatal("expected generated overlay path, not explicit Aptfile")
	}
	if len(files) != 1 {
		t.Fatalf("expected one overlay file, got %d", len(files))
	}
	if files[0].RelativePath != "Aptfile" {
		t.Fatalf("expected Aptfile overlay, got %q", files[0].RelativePath)
	}
	if !files[0].OnlyIfMissing {
		t.Fatal("expected generated Aptfile overlay to only write when missing")
	}
	if strings.TrimSpace(files[0].Content) != "git" {
		t.Fatalf("expected generated Aptfile to contain git, got %q", files[0].Content)
	}
}

func TestBuildBuildpacksSystemPackageOverlayFilesSkipsExplicitAptfile(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Aptfile"), []byte("git\n"), 0o644); err != nil {
		t.Fatalf("write Aptfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "app.py"), []byte("from git import Repo\n"), 0o644); err != nil {
		t.Fatalf("write app.py: %v", err)
	}

	files, analysis, err := buildBuildpacksSystemPackageOverlayFiles(repoDir, ".")
	if err != nil {
		t.Fatalf("build buildpacks system package overlay files: %v", err)
	}
	if !analysis.HasExplicitBuildpackApt {
		t.Fatal("expected explicit Aptfile to be detected")
	}
	if len(files) != 0 {
		t.Fatalf("expected no generated overlay files when Aptfile exists, got %d", len(files))
	}
}
