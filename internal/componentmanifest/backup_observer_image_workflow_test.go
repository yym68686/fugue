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

const backupObserverImageWorkflowPath = "../../.github/workflows/build-backup-observer-image.yml"

func TestBackupObserverImageWorkflowIsPathScopedBuildOnly(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupObserverImageWorkflowPath)
	if err != nil {
		t.Fatalf("read backup observer workflow: %v", err)
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
		t.Fatalf("parse backup observer workflow: %v", err)
	}
	if workflow.Name != "build backup observer image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-observer-image-${{ github.ref }}" || !workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("backup observer workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["build-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("backup observer build-only job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"Dockerfile.backup-observer",
		"cmd/fugue-backup-observer/**",
		"internal/backupcontrol/**",
		"internal/backupobserver/**",
		"scripts/test_backup_observer_image.sh",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"docker buildx build",
		"--load",
		"GOARCH=\"${arch}\"",
		"build and probe observer (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("backup observer workflow is missing %q", required)
		}
	}
	for _, unrelated := range []string{
		"docs/architecture",
		"internal/componentmanifest/**",
		"deploy/helm/fugue-image-plane",
		"cmd/fugue-release-control",
	} {
		if strings.Contains(raw, unrelated) {
			t.Fatalf("backup observer workflow widened into unrelated path %q", unrelated)
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
		"workflow_run",
		"actions/upload-artifact",
		"kubectl ",
		"helm ",
		"gh workflow run",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("backup observer build-only workflow contains forbidden mutation %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("backup observer workflow action %q is not pinned", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("backup observer workflow step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}

	script, err := os.ReadFile("../../scripts/test_backup_observer_image.sh")
	if err != nil {
		t.Fatalf("read backup observer image probe: %v", err)
	}
	command := exec.Command("bash", "-n")
	command.Stdin = strings.NewReader(string(script))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("backup observer image probe has invalid bash: %v output=%q", err, output)
	}
	for _, required := range []string{"--read-only", "probe health", "probe ready", "65532:65532", ".Config.ExposedPorts", "docker stop --time 5"} {
		if !strings.Contains(string(script), required) {
			t.Fatalf("backup observer image probe is missing %q", required)
		}
	}
}
