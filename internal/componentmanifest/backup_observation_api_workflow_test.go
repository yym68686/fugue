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

const backupObservationAPIWorkflowPath = "../../.github/workflows/validate-backup-observation-api.yml"

func TestBackupObservationAPIWorkflowIsReadOnly(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupObservationAPIWorkflowPath)
	if err != nil {
		t.Fatalf("read backup observation API workflow: %v", err)
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
			RunsOn         string            `yaml:"runs-on"`
			TimeoutMinutes int               `yaml:"timeout-minutes"`
			Permissions    map[string]string `yaml:"permissions"`
			Steps          []workflowStep    `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse backup observation API workflow: %v", err)
	}
	if workflow.Name != "validate backup observation API" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-observation-api-${{ github.ref }}" ||
		!workflow.Concurrency.CancelInProgress || len(workflow.Jobs) != 1 {
		t.Fatalf("backup observation API workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 15 || len(job.Permissions) != 0 {
		t.Fatalf("backup observation API job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"internal/api/backup_observation.go",
		"internal/api/routes_gen.go",
		"internal/apispec/backup_observation_test.go",
		"internal/backupidentity/**",
		"internal/store/backup_observation.go",
		"openapi/openapi.yaml",
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
		"go test -race ./internal/api",
		"go run ./cmd/fugue-openapi-gen",
		"validate backup observation API (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("backup observation API workflow is missing %q", required)
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
		"git push",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("backup observation API workflow contains forbidden mutation %q", forbidden)
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
