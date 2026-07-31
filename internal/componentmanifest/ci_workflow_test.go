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

func TestCIWorkflowAvoidsDuplicateFeatureBranchRuns(t *testing.T) {
	t.Parallel()

	const workflowPath = "../../.github/workflows/ci.yml"
	data, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	var workflow struct {
		Name        string            `yaml:"name"`
		Permissions map[string]string `yaml:"permissions"`
		On          struct {
			Push struct {
				Branches []string `yaml:"branches"`
			} `yaml:"push"`
			PullRequest map[string]any `yaml:"pull_request"`
		} `yaml:"on"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			RunsOn string         `yaml:"runs-on"`
			Steps  []workflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse CI workflow: %v", err)
	}
	if workflow.Name != "ci" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		workflow.On.PullRequest == nil ||
		workflow.Concurrency.Group != "ci-${{ github.event.pull_request.number || github.sha }}" ||
		!workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("CI trigger/concurrency boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["test"]
	if !ok || job.RunsOn != "ubuntu-latest" || len(job.Steps) != 4 {
		t.Fatalf("CI test job boundary drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, forbidden := range []string{
		"packages: write",
		"docker/login-action@",
		"--push",
		"environment: production",
		"workflow_dispatch",
		"kubectl ",
		"helm install",
		"helm upgrade",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("CI workflow contains forbidden mutation %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("CI workflow action %q is not pinned to a full commit SHA", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("CI workflow step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}
	if !strings.Contains(raw, "go test ./...") {
		t.Fatal("CI workflow no longer runs the complete Go test suite")
	}
}
