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

func TestKanikoDockerfilePathRelativeToBuildContext(t *testing.T) {
	t.Parallel()

	got, err := kanikoDockerfilePath("apps/api/Dockerfile", "apps/api")
	if err != nil {
		t.Fatalf("kaniko dockerfile path: %v", err)
	}
	if got != "Dockerfile" {
		t.Fatalf("unexpected dockerfile path: got %q want %q", got, "Dockerfile")
	}
}

func TestKanikoDockerfilePathRejectsDockerfileOutsideBuildContext(t *testing.T) {
	t.Parallel()

	if _, err := kanikoDockerfilePath("Dockerfile", "apps/api"); err == nil {
		t.Fatal("expected dockerfile outside build context to be rejected")
	}
}

func TestBuildKanikoJobObjectUsesDockerfileRelativeToContextSubPath(t *testing.T) {
	t.Parallel()

	jobObject, err := buildKanikoJobObject("fugue-system", "build-demo", dockerfileBuildRequest{
		RepoURL:         "https://github.com/yym68686/uni-api-web",
		Branch:          "main",
		CommitSHA:       "2251810595260b7ffbdbb1e45bbc875cb13ff631",
		DockerfilePath:  "apps/api/Dockerfile",
		BuildContextDir: "apps/api",
		ImageRef:        "10.128.0.2:30500/fugue-apps/yym68686-uni-api-web-api:git-225181059526",
	})
	if err != nil {
		t.Fatalf("build kaniko job object: %v", err)
	}

	spec := jobObject["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]map[string]any)
	args := containers[0]["args"].([]string)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--dockerfile=/workspace/repo/apps/api/Dockerfile") {
		t.Fatalf("expected dockerfile path inside cloned workspace, got args: %v", args)
	}
	if !strings.Contains(joined, "--context-sub-path=apps/api") {
		t.Fatalf("expected context-sub-path to be preserved, got args: %v", args)
	}
}
