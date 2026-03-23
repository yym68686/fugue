package sourceimport

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGitCloneArgsIncludesRecursiveSubmodules(t *testing.T) {
	got := gitCloneArgs("https://github.com/yym68686/Cerebr", "/tmp/repo", "")
	want := []string{
		"clone",
		"--depth", "1",
		"--recurse-submodules",
		"--shallow-submodules",
		"https://github.com/yym68686/Cerebr",
		"/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected clone args:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestGitCloneArgsIncludesBranch(t *testing.T) {
	got := gitCloneArgs("https://github.com/yym68686/Cerebr", "/tmp/repo", "main")
	want := []string{
		"clone",
		"--depth", "1",
		"--recurse-submodules",
		"--shallow-submodules",
		"--branch", "main",
		"https://github.com/yym68686/Cerebr",
		"/tmp/repo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected clone args:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestDetectDockerBuildInputs(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(repoDir, "deploy"), 0o755); err != nil {
		t.Fatalf("mkdir deploy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "deploy", "Dockerfile.api"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}

	dockerfilePath, contextDir, err := detectDockerBuildInputs(repoDir, "deploy/Dockerfile.api", ".")
	if err != nil {
		t.Fatalf("detect docker build inputs: %v", err)
	}
	if dockerfilePath != "deploy/Dockerfile.api" {
		t.Fatalf("unexpected dockerfile path: %s", dockerfilePath)
	}
	if contextDir != "." {
		t.Fatalf("unexpected context dir: %s", contextDir)
	}
}

func TestBuildGitContextURL(t *testing.T) {
	got, err := buildGitContextURL("https://github.com/yym68686/uni-api", "main", "abcdef1234567890")
	if err != nil {
		t.Fatalf("build git context url: %v", err)
	}
	want := "git://github.com/yym68686/uni-api.git#refs/heads/main#abcdef1234567890"
	if got != want {
		t.Fatalf("unexpected git context url:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestIsInsecureRegistryHostTreatsClusterServiceAsInsecure(t *testing.T) {
	t.Parallel()

	if !isInsecureRegistryHost("fugue-fugue-registry.fugue-system.svc.cluster.local") {
		t.Fatalf("expected cluster-local registry host to be treated as insecure")
	}
}

func TestKanikoDestinationArgsIncludeInsecureFlagsForClusterService(t *testing.T) {
	t.Parallel()

	args := kanikoDestinationArgs(
		"fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abc123",
		"--context=dir:///workspace/generated",
		"--dockerfile=/workspace/generated/Dockerfile",
	)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--insecure") {
		t.Fatalf("expected --insecure in args: %v", args)
	}
	if !strings.Contains(joined, "--insecure-registry=fugue-fugue-registry.fugue-system.svc.cluster.local") {
		t.Fatalf("expected --insecure-registry in args: %v", args)
	}
}
