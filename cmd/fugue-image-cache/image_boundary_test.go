package main

import (
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	imagePlaneWorkflowPath = "../../.github/workflows/build-image-plane-image.yml"
	imagePlaneDockerfile   = "../../Dockerfile.image-cache"
	imagePlaneProbeScript  = "../../scripts/test_image_plane_image.sh"
)

func TestImagePlaneWorkflowIsPathScopedBuildOnly(t *testing.T) {
	data, err := os.ReadFile(imagePlaneWorkflowPath)
	if err != nil {
		t.Fatalf("read image-plane workflow: %v", err)
	}
	var workflow struct {
		Name        string            `yaml:"name"`
		Permissions map[string]string `yaml:"permissions"`
		On          struct {
			Push struct {
				Branches []string `yaml:"branches"`
				Paths    []string `yaml:"paths"`
			} `yaml:"push"`
			PullRequest struct {
				Paths []string `yaml:"paths"`
			} `yaml:"pull_request"`
		} `yaml:"on"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			RunsOn      string                   `yaml:"runs-on"`
			Permissions map[string]string        `yaml:"permissions"`
			Steps       []imagePlaneWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse image-plane workflow: %v", err)
	}
	if workflow.Name != "build image-plane image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || len(workflow.On.PullRequest.Paths) == 0 ||
		!reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "image-plane-image-${{ github.ref }}" ||
		!workflow.Concurrency.CancelInProgress || len(workflow.Jobs) != 1 {
		t.Fatalf("image-plane workflow safety boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["build-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("image-plane build-only job boundary drifted: %+v", workflow.Jobs)
	}

	raw := string(data)
	for _, required := range []string{
		"Dockerfile.image-cache",
		"cmd/fugue-image-cache/**",
		"internal/imagecacheusage/**",
		"scripts/test_image_plane_image.sh",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"docker buildx build",
		"--load",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("image-plane workflow is missing required boundary %q", required)
		}
	}
	for _, forbidden := range []string{
		"packages: write",
		"docker/login-action@",
		"--push",
		"push: true",
		"ghcr.io/",
		"environment: production",
		"workflow_dispatch",
		"kubectl ",
		"helm ",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("image-plane build-only workflow contains forbidden mutation %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("workflow action %q is not pinned to a full commit SHA", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("workflow step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}
}

func TestImagePlaneDockerfileCopiesOnlyTheLocalDependencyClosure(t *testing.T) {
	data, err := os.ReadFile(imagePlaneDockerfile)
	if err != nil {
		t.Fatalf("read image-plane Dockerfile: %v", err)
	}
	raw := string(data)
	for _, required := range []string{
		"golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587",
		"alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d",
		"COPY cmd/fugue-image-cache ./cmd/fugue-image-cache",
		"COPY internal/imagecacheusage ./internal/imagecacheusage",
		"-trimpath -buildvcs=false",
		`ENTRYPOINT ["/usr/local/bin/fugue-image-cache"]`,
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("image-plane Dockerfile is missing %q", required)
		}
	}
	for _, forbidden := range []string{"COPY cmd ./cmd", "COPY internal ./internal"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("image-plane Dockerfile widened its local source boundary with %q", forbidden)
		}
	}
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "FROM ") &&
			!regexp.MustCompile(`@sha256:[0-9a-f]{64}(?:\s|$)`).MatchString(line) {
			t.Fatalf("image-plane Dockerfile base is not digest-pinned: %q", line)
		}
	}

	want := []string{"fugue/cmd/fugue-image-cache", "fugue/internal/imagecacheusage"}
	for _, architecture := range []string{"amd64", "arm64"} {
		command := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./cmd/fugue-image-cache")
		command.Dir = "../.."
		command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+architecture)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("list linux/%s image-plane dependencies: %v\n%s", architecture, err, output)
		}
		var local []string
		for _, dependency := range strings.Fields(string(output)) {
			if strings.HasPrefix(dependency, "fugue/") {
				local = append(local, dependency)
			}
		}
		sort.Strings(local)
		if !reflect.DeepEqual(local, want) {
			t.Fatalf("linux/%s local dependency closure = %v, want %v; update Dockerfile and workflow atomically", architecture, local, want)
		}
	}
}

func TestImagePlaneProbeScriptIsExecutableAndValidBash(t *testing.T) {
	info, err := os.Stat(imagePlaneProbeScript)
	if err != nil {
		t.Fatalf("stat image-plane probe: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("image-plane probe mode %o is not executable", info.Mode().Perm())
	}
	output, err := exec.Command("bash", "-n", imagePlaneProbeScript).CombinedOutput()
	if err != nil {
		t.Fatalf("image-plane probe has invalid bash: %v\n%s", err, output)
	}
}

type imagePlaneWorkflowStep struct {
	Name string `yaml:"name"`
	Run  string `yaml:"run"`
	Uses string `yaml:"uses"`
}
