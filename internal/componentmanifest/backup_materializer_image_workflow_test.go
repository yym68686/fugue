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

const backupMaterializerImageWorkflowPath = "../../.github/workflows/build-backup-materializer-image.yml"

func TestBackupMaterializerImageWorkflowIsPathScopedBuildOnly(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(backupMaterializerImageWorkflowPath)
	if err != nil {
		t.Fatalf("read backup materializer workflow: %v", err)
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
		t.Fatalf("parse backup materializer workflow: %v", err)
	}
	if workflow.Name != "build backup materializer image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "backup-materializer-image-${{ github.ref }}" ||
		!workflow.Concurrency.CancelInProgress || len(workflow.Jobs) != 1 {
		t.Fatalf("backup materializer workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["build-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Permissions) != 0 {
		t.Fatalf("backup materializer build-only job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"Dockerfile.backup-materializer",
		"cmd/fugue-backup-materializer/**",
		"internal/backupcontrol/**",
		"internal/backupmaterializer/agent/**",
		"internal/backupmaterializer/client/**",
		"internal/backupmaterializer/contract/**",
		"internal/backupmaterializer/materialization/**",
		"internal/backupmaterializer/reconcile/**",
		"internal/backupmaterializer/reconciler/**",
		"internal/backupmaterializer/secretreader/**",
		"deploy/helm/fugue-backup-materializer/**",
		"scripts/test_backup_materializer_image.sh",
		"azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"go test -race",
		"go list -deps ./cmd/fugue-backup-materializer",
		"GOARCH=\"${arch}\"",
		"helm lint deploy/helm/fugue-backup-materializer",
		"go test ./deploy/helm/fugue-backup-materializer",
		"helm template backup-materializer deploy/helm/fugue-backup-materializer",
		"docker buildx build",
		"--load",
		"build and probe materializer (no publish)",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("backup materializer workflow is missing %q", required)
		}
	}
	for _, unrelated := range []string{
		"docs/architecture",
		"internal/componentmanifest/**",
		"internal/backupmaterializer/composition/**",
		"internal/backupmaterializer/httpapi/**",
		"internal/backupmaterializer/localissuer/**",
		"internal/backupmaterializer/storesource/**",
		"internal/backupmaterializeridentity/**",
		"internal/backupmaterializerreview/**",
		"deploy/helm/fugue-backup-observer",
		"deploy/helm/fugue-image-plane",
		"deploy/helm/fugue-release-control",
		"cmd/fugue-backup-observer",
	} {
		if strings.Contains(raw, unrelated) {
			t.Fatalf("backup materializer workflow widened into unrelated path %q", unrelated)
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
		"helm install",
		"helm upgrade",
		"helm uninstall",
		"helm push",
		"helm package",
		"gh workflow run",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("backup materializer build-only workflow contains forbidden mutation %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("backup materializer workflow action %q is not pinned", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("backup materializer workflow step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}

	script, err := os.ReadFile("../../scripts/test_backup_materializer_image.sh")
	if err != nil {
		t.Fatalf("read backup materializer image probe: %v", err)
	}
	info, err := os.Stat("../../scripts/test_backup_materializer_image.sh")
	if err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("backup materializer image probe is not executable: info=%v err=%v", info, err)
	}
	command := exec.Command("bash", "-n")
	command.Stdin = strings.NewReader(string(script))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("backup materializer image probe has invalid bash: %v output=%q", err, output)
	}
	for _, required := range []string{
		"--network none", "--read-only", "--cap-drop ALL", "--security-opt no-new-privileges",
		"probe health", "probe ready", "65532:65532", ".Config.ExposedPorts", "/bin/sh",
		"docker stop --time 5",
	} {
		if !strings.Contains(string(script), required) {
			t.Fatalf("backup materializer image probe is missing %q", required)
		}
	}
}
