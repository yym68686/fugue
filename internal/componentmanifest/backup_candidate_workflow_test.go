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

const backupCandidateWorkflowPath = "../../.github/workflows/validate-backup-release-candidate.yml"

func TestBackupCandidateWorkflowIsPathScopedValidationOnly(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupCandidateWorkflowPath)
	if err != nil {
		t.Fatalf("read backup candidate workflow: %v", err)
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
		t.Fatalf("parse backup candidate workflow: %v", err)
	}
	if workflow.Name != "validate backup release candidate" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-candidate-${{ github.ref }}" || !workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("backup candidate workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("backup candidate validate-only job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"cmd/fugue-backup-release-plan/**",
		"internal/backuprelease/**",
		"internal/componentmanifest/**",
		"deploy/helm/fugue-backup-observer/**",
		"docs/architecture/component-ownership-v1.yaml",
		"azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
		"go test ./cmd/fugue-backup-release-plan ./internal/backuprelease ./internal/componentmanifest ./deploy/helm/fugue-backup-observer",
		"go build -trimpath -buildvcs=false -o /tmp/fugue-backup-release-plan",
		"helm lint deploy/helm/fugue-backup-observer",
		"--digest-chart",
		"validate backup candidate (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("backup candidate workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"packages: write",
		"docker/login-action@",
		"docker build",
		"--push",
		"push: true",
		"ghcr.io/",
		"environment: production",
		"workflow_dispatch",
		"workflow_run",
		"actions/upload-artifact",
		"kubectl ",
		"helm install",
		"helm upgrade",
		"helm uninstall",
		"helm push",
		"helm package",
		"gh workflow run",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("backup candidate workflow contains forbidden mutation %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("workflow action %q is not pinned", step.Uses)
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
