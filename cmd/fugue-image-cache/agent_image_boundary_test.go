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
	imagePlaneAgentWorkflowPath = "../../.github/workflows/build-image-plane-agent-image.yml"
	imagePlaneAgentDockerfile   = "../../Dockerfile.image-plane-agent"
	imagePlaneAgentProbeScript  = "../../scripts/test_image_plane_agent_image.sh"
)

func TestImagePlaneAgentWorkflowIsIndependentBuildOnly(t *testing.T) {
	data, err := os.ReadFile(imagePlaneAgentWorkflowPath)
	if err != nil {
		t.Fatalf("read image-plane agent workflow: %v", err)
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
		t.Fatalf("parse image-plane agent workflow: %v", err)
	}
	if workflow.Name != "build image-plane agent image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "image-plane-agent-${{ github.ref }}" || !workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("image-plane agent workflow safety boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["build-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("image-plane agent build-only job boundary drifted: %+v", workflow.Jobs)
	}

	raw := string(data)
	for _, required := range []string{
		"Dockerfile.image-plane-agent",
		"cmd/fugue-image-cache/platform_plan_shadow.go",
		"cmd/fugue-image-cache/shadow_agent.go",
		"cmd/fugue-image-cache/shadow_agent_main.go",
		"cmd/fugue-image-cache/agent_image_boundary_test.go",
		"cmd/fugue-image-plane-release-plan/**",
		"internal/imageplanerelease/**",
		"deploy/helm/fugue-image-plane/**",
		"docs/architecture/component-ownership-v1.yaml",
		"scripts/test_image_plane_agent_image.sh",
		"go build -tags=imageplaneagent",
		"go test ./cmd/fugue-image-plane-release-plan ./internal/imageplanerelease",
		"go list -tags=imageplaneagent -deps",
		"azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"helm lint deploy/helm/fugue-image-plane",
		"helm template image-plane-shadow deploy/helm/fugue-image-plane",
		"docker buildx build",
		"--load",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("image-plane agent workflow is missing required boundary %q", required)
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
		"helm install",
		"helm upgrade",
		"helm uninstall",
		"helm push",
		"helm package",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("image-plane agent build-only workflow contains forbidden mutation %q", forbidden)
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

func TestImagePlaneAgentDockerfileHasExactSourceAndRuntimeBoundary(t *testing.T) {
	data, err := os.ReadFile(imagePlaneAgentDockerfile)
	if err != nil {
		t.Fatalf("read image-plane agent Dockerfile: %v", err)
	}
	raw := string(data)
	for _, required := range []string{
		"golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587",
		"alpine:3.21@sha256:48b0309ca019d89d40f670aa1bc06e426dc0931948452e8491e3d65087abc07d",
		"COPY cmd/fugue-image-cache/env.go ./cmd/fugue-image-cache/env.go",
		"COPY cmd/fugue-image-cache/http_lifecycle.go ./cmd/fugue-image-cache/http_lifecycle.go",
		"COPY cmd/fugue-image-cache/platform_plan_shadow.go ./cmd/fugue-image-cache/platform_plan_shadow.go",
		"COPY cmd/fugue-image-cache/shadow_agent.go ./cmd/fugue-image-cache/shadow_agent.go",
		"COPY cmd/fugue-image-cache/shadow_agent_main.go ./cmd/fugue-image-cache/shadow_agent_main.go",
		"go build -tags=imageplaneagent -trimpath -buildvcs=false",
		"USER 65532:65532",
		`ENTRYPOINT ["/usr/local/bin/fugue-image-plane-agent"]`,
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("image-plane agent Dockerfile is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"COPY cmd/fugue-image-cache ./cmd/fugue-image-cache",
		"COPY cmd ./cmd",
		"COPY internal ./internal",
		"COPY cmd/fugue-image-cache/main.go",
		"internal/imagecacheusage",
		"EXPOSE ",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("image-plane agent Dockerfile widened its source or runtime boundary with %q", forbidden)
		}
	}
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "FROM ") &&
			!regexp.MustCompile(`@sha256:[0-9a-f]{64}(?:\s|$)`).MatchString(line) {
			t.Fatalf("image-plane agent base is not digest-pinned: %q", line)
		}
	}
}

func TestImagePlaneAgentBuildTagExcludesLegacyDependencies(t *testing.T) {
	for _, architecture := range []string{"amd64", "arm64"} {
		command := exec.Command("go", "list", "-tags=imageplaneagent", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./cmd/fugue-image-cache")
		command.Dir = "../.."
		command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+architecture)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("list linux/%s image-plane agent dependencies: %v\n%s", architecture, err, output)
		}
		dependencies := strings.Fields(string(output))
		var local []string
		for _, dependency := range dependencies {
			if strings.HasPrefix(dependency, "fugue/") {
				local = append(local, dependency)
			}
			for _, forbidden := range []string{"github.com/google/go-containerregistry", "k8s.io/", "github.com/jackc/pgx"} {
				if strings.HasPrefix(dependency, forbidden) {
					t.Fatalf("linux/%s agent dependency closure contains %q", architecture, dependency)
				}
			}
		}
		sort.Strings(local)
		if want := []string{"fugue/cmd/fugue-image-cache"}; !reflect.DeepEqual(local, want) {
			t.Fatalf("linux/%s agent local dependency closure=%v, want %v", architecture, local, want)
		}
	}
}

func TestImagePlaneAgentProbeIsExecutableAndValidBash(t *testing.T) {
	info, err := os.Stat(imagePlaneAgentProbeScript)
	if err != nil {
		t.Fatalf("stat image-plane agent probe: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("image-plane agent probe mode %o is not executable", info.Mode().Perm())
	}
	data, err := os.ReadFile(imagePlaneAgentProbeScript)
	if err != nil {
		t.Fatalf("read image-plane agent probe: %v", err)
	}
	for _, required := range []string{
		`["/usr/local/bin/fugue-image-plane-agent"]`,
		"65532:65532",
		"artifact declares network ports",
		"exposed forbidden legacy path",
		"initialized legacy registry state",
	} {
		if !strings.Contains(string(data), required) {
			t.Fatalf("image-plane agent probe is missing assertion %q", required)
		}
	}
	if output, err := exec.Command("bash", "-n", imagePlaneAgentProbeScript).CombinedOutput(); err != nil {
		t.Fatalf("image-plane agent probe has invalid bash: %v\n%s", err, output)
	}
}
