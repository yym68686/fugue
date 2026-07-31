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

const backupMaterializerDryRunGuardWorkflowPath = "../../.github/workflows/validate-backup-materializer-dry-run-guard.yml"

func TestBackupMaterializerDryRunGuardWorkflowIsReadOnlyAndPathScoped(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupMaterializerDryRunGuardWorkflowPath)
	if err != nil {
		t.Fatalf("read dry-run guard workflow: %v", err)
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
		t.Fatalf("parse dry-run guard workflow: %v", err)
	}
	if workflow.Name != "validate backup materializer dry-run guard" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-materializer-dry-run-guard-${{ github.ref }}" ||
		!workflow.Concurrency.CancelInProgress || len(workflow.Jobs) != 1 {
		t.Fatalf("dry-run guard workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 10 || len(job.Permissions) != 0 {
		t.Fatalf("dry-run guard job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"internal/backupcontrol/**",
		"internal/backupmaterializer/contract/**",
		"internal/backupmaterializer/materialization/**",
		"internal/backupmaterializer/dryrunguard/**",
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
		"go test ./internal/backupmaterializer/dryrunguard -count=10",
		"go test -race ./internal/backupmaterializer/dryrunguard -count=3",
		"go vet ./internal/backupmaterializer/dryrunguard",
		"go list -deps ./internal/backupmaterializer/dryrunguard",
		"validate backup materializer dry-run guard (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("dry-run guard workflow is missing %q", required)
		}
	}
	for _, unrelated := range []string{
		"cmd/",
		"deploy/helm/",
		"Dockerfile",
		"internal/api/",
		"internal/store/",
		"internal/backupmaterializer/validationcomposition/",
	} {
		if strings.Contains(raw, unrelated) {
			t.Fatalf("dry-run guard workflow widened into unrelated path %q", unrelated)
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
		"helm ",
		"gh workflow run",
		"git push",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("dry-run guard workflow contains forbidden capability %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("dry-run guard action %q is not pinned", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("dry-run guard step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}
}
