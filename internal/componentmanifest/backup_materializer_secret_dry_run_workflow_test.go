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

const backupMaterializerSecretDryRunWorkflowPath = "../../.github/workflows/validate-backup-materializer-secret-dry-run.yml"

func TestBackupMaterializerSecretDryRunWorkflowIsReadOnlyAndPathScoped(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupMaterializerSecretDryRunWorkflowPath)
	if err != nil {
		t.Fatalf("read Secret dry-run workflow: %v", err)
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
		t.Fatalf("parse Secret dry-run workflow: %v", err)
	}
	if workflow.Name != "validate backup materializer Secret dry-run" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-materializer-secret-dry-run-${{ github.ref }}" ||
		!workflow.Concurrency.CancelInProgress || len(workflow.Jobs) != 1 {
		t.Fatalf("Secret dry-run workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("Secret dry-run job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"internal/backupcontrol/**",
		"internal/backupmaterializer/contract/**",
		"internal/backupmaterializer/dryrunreconciler/**",
		"internal/backupmaterializer/materialization/**",
		"internal/backupmaterializer/reconcile/**",
		"internal/backupmaterializer/reconciler/**",
		"internal/backupmaterializer/secretwriter/**",
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
		"go test ./internal/backupmaterializer/secretwriter/... -count=10",
		"go test -race ./internal/backupmaterializer/secretwriter/... -count=3",
		"go list -deps ./internal/backupmaterializer/secretwriter",
		"go list -deps ./internal/backupmaterializer/secretwriter/projected",
		"go test ./internal/backupmaterializer/dryrunreconciler -count=10",
		"go test -race ./internal/backupmaterializer/dryrunreconciler -count=3",
		"go list -deps ./internal/backupmaterializer/dryrunreconciler",
		"validate backup materializer Secret dry-run (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("Secret dry-run workflow is missing %q", required)
		}
	}
	for _, unrelated := range []string{
		"docs/architecture",
		"internal/componentmanifest/**",
		"internal/backupmaterializer/agent/**",
		"internal/backupmaterializer/client/**",
		"internal/backupmaterializer/secretreader/**",
		"deploy/helm/",
		"Dockerfile",
	} {
		if strings.Contains(raw, unrelated) {
			t.Fatalf("Secret dry-run workflow widened into unrelated path %q", unrelated)
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
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("Secret dry-run workflow contains forbidden capability %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("Secret dry-run workflow action %q is not pinned", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("Secret dry-run step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}
}
