package platformsafety

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflowNeeds []string

func (n *workflowNeeds) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		*n = workflowNeeds{node.Value}
		return nil
	case yaml.SequenceNode:
		values := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode {
				return fmt.Errorf("workflow need must be a scalar")
			}
			values = append(values, item.Value)
		}
		*n = values
		return nil
	default:
		return fmt.Errorf("workflow needs must be a scalar or sequence")
	}
}

type releaseWorkflow struct {
	Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	If              string                `yaml:"if"`
	ContinueOnError bool                  `yaml:"continue-on-error"`
	Steps           []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	Run string `yaml:"run"`
}

func TestControlPlaneDeployRequiresInternalReleaseGate(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}

	gate, ok := workflow.Jobs["release-gate"]
	if !ok {
		t.Fatal("control-plane workflow must define release-gate")
	}
	if gate.ContinueOnError {
		t.Fatal("release-gate must fail closed")
	}
	commands := make([]string, 0, len(gate.Steps))
	for _, step := range gate.Steps {
		commands = append(commands, step.Run)
	}
	joinedCommands := strings.Join(commands, "\n")
	for _, required := range []string{
		"make generate-openapi-check",
		"bash scripts/test_release_domain_safety.sh",
		"go test ./...",
	} {
		if !strings.Contains(joinedCommands, required) {
			t.Fatalf("release-gate must run %q", required)
		}
	}

	build, ok := workflow.Jobs["build"]
	if !ok || !containsWorkflowNeed(build.Needs, "release-gate") {
		t.Fatal("image build must wait for release-gate")
	}
	deploy, ok := workflow.Jobs["deploy"]
	if !ok || !containsWorkflowNeed(deploy.Needs, "release-gate") {
		t.Fatal("control-plane deploy must wait for release-gate")
	}
	for _, required := range []string{
		"needs.release-baseline.result == 'success'",
		"needs.release-gate.result == 'success'",
	} {
		if !strings.Contains(deploy.If, required) {
			t.Fatalf("deploy condition must require %q", required)
		}
	}
}

func containsWorkflowNeed(needs workflowNeeds, expected string) bool {
	for _, need := range needs {
		if need == expected {
			return true
		}
	}
	return false
}
