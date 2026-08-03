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

func TestEdgeControlImageWorkflowPublishesOnlyAnImmutableImage(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("../../.github/workflows/publish-edge-control-image.yml")
	if err != nil {
		t.Fatal(err)
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
			Environment string            `yaml:"environment"`
			Permissions map[string]string `yaml:"permissions"`
			Steps       []workflowStep    `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "publish edge-control image" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) ||
		len(workflow.On.Push.Paths) == 0 || !reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "edge-control-image-${{ github.ref }}" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("workflow boundary drifted: %+v", workflow)
	}
	verify, verifyOK := workflow.Jobs["verify"]
	publish, publishOK := workflow.Jobs["publish"]
	if !verifyOK || !publishOK || len(workflow.Jobs) != 2 || verify.Environment != "" || len(verify.Permissions) != 0 ||
		publish.Environment != "production" || !reflect.DeepEqual(publish.Permissions, map[string]string{"contents": "read", "packages": "write"}) {
		t.Fatalf("job capability boundary drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"Dockerfile.edge-control", "cmd/fugue-edge-control/**", "internal/edgecontrol/**",
		"deploy/helm/fugue-edge-control/**", "scripts/test_edge_control_image.sh",
		"--platform linux/amd64,linux/arm64", "--push", "edge-control-image-receipt/v1",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("workflow missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"kubectl ", "k3s ", "helm install", "helm upgrade", "helm uninstall", "workflow_dispatch",
		"deploy-control-plane", "release-public-data-plane", "FUGUE_API_KEY", "FUGUE_DATABASE_URL", "FUGUE_BUNDLE_SIGNING_KEY",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("image workflow gained forbidden capability %q", forbidden)
		}
	}
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
				t.Fatalf("action is not pinned: %q", step.Uses)
			}
			if step.Run != "" {
				command := exec.Command("bash", "-n")
				command.Stdin = strings.NewReader(step.Run)
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("invalid bash in step %q: %v output=%s", step.Name, err, output)
				}
			}
		}
	}
}

type workflowStep struct {
	Name string `yaml:"name"`
	Run  string `yaml:"run"`
	Uses string `yaml:"uses"`
}
