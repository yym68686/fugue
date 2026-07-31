package componentmanifest

import (
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const releaseControlImageWorkflowPath = "../../.github/workflows/build-release-control-image.yml"

func TestReleaseControlImageWorkflowIsPathScopedBuildOnly(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(releaseControlImageWorkflowPath)
	if err != nil {
		t.Fatalf("read release-control image workflow: %v", err)
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
			RunsOn      string            `yaml:"runs-on"`
			Permissions map[string]string `yaml:"permissions"`
			Steps       []workflowStep    `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse release-control image workflow: %v", err)
	}
	if workflow.Name != "build release-control image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || len(workflow.On.PullRequest.Paths) == 0 ||
		!reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "release-control-image-${{ github.ref }}" || !workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("workflow safety boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["build-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("build-only job boundary drifted: %+v", workflow.Jobs)
	}

	raw := string(data)
	for _, required := range []string{
		"Dockerfile.release-control",
		"cmd/fugue-release-control/**",
		"deploy/helm/fugue-release-control/**",
		"internal/releasecontrol/**",
		"scripts/test_release_control_image.sh",
		"azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"helm lint deploy/helm/fugue-release-control",
		"helm template release-control deploy/helm/fugue-release-control",
		"docker buildx build",
		"--load",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("workflow is missing required boundary %q", required)
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
			t.Fatalf("build-only workflow contains forbidden mutation %q", forbidden)
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

type workflowStep struct {
	Name string `yaml:"name"`
	Run  string `yaml:"run"`
	Uses string `yaml:"uses"`
}
