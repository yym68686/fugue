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

func TestEdgeControlShadowWorkflowIsExactReceiptDrivenAndCredentialFree(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("../../.github/workflows/deploy-edge-control-shadow.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Name        string            `yaml:"name"`
		Permissions map[string]string `yaml:"permissions"`
		On          struct {
			WorkflowRun struct {
				Workflows []string `yaml:"workflows"`
				Types     []string `yaml:"types"`
			} `yaml:"workflow_run"`
		} `yaml:"on"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			If          string            `yaml:"if"`
			RunsOn      []string          `yaml:"runs-on"`
			Environment string            `yaml:"environment"`
			Permissions map[string]string `yaml:"permissions"`
			Steps       []workflowStep    `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "deploy edge-control shadow" || len(workflow.Permissions) != 0 ||
		!reflect.DeepEqual(workflow.On.WorkflowRun.Workflows, []string{"publish edge-control image"}) ||
		!reflect.DeepEqual(workflow.On.WorkflowRun.Types, []string{"completed"}) ||
		workflow.Concurrency.Group != "fugue-edge-control-shadow-production-v1" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("shadow workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["deploy-shadow"]
	if !ok || len(workflow.Jobs) != 1 || job.Environment != "production" ||
		!strings.Contains(job.If, "workflow_run.event == 'push'") ||
		!strings.Contains(job.If, "workflow_run.head_branch == 'main'") ||
		!strings.Contains(job.If, "workflow_run.conclusion == 'success'") ||
		!strings.Contains(job.If, "workflow_run.run_attempt == 1") ||
		!reflect.DeepEqual(job.RunsOn, []string{"self-hosted", "linux", "x64", "fugue", "control-plane"}) ||
		!reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "read"}) {
		t.Fatalf("shadow job capability drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"SOURCE_WORKFLOW_PATH", "SOURCE_RUN_ATTEMPT", "remote_main", "wait_for_gate ci.yml",
		"source_artifact_digest", "deploy_edge_control_shadow.sh",
		"fugue-edge-control-shadow-production-v1", "edge-control-shadow-result/receipt.json",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("shadow workflow missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"secrets.", "FUGUE_API_KEY", "FUGUE_BOOTSTRAP_KEY", "FUGUE_DATABASE_URL", "FUGUE_BUNDLE_SIGNING_KEY",
		"helm uninstall", "kubectl delete", "kubectl patch",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("shadow workflow gained forbidden capability %q", forbidden)
		}
	}
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

func TestEdgeControlShadowDeployScriptIsSingleWriteAndFailClosed(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../scripts/deploy_edge_control_shadow.sh")
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, required := range []string{
		"helm upgrade --install", "--rollback-on-failure", "--atomic", "--reset-values", "kube apply --dry-run=server",
		"snapshot_legacy", "verify_edge_control", "authority\":\"none", "boundary-only",
		"legacy_spec_digest_before", "soak_samples", "pod_restart_count",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("shadow deploy script missing %q", required)
		}
	}
	if strings.Count(raw, "helm upgrade --install") != 1 {
		t.Fatalf("shadow deploy script has more than one Helm write path")
	}
	for _, forbidden := range []string{
		"helm uninstall", "helm rollback", "kubectl delete", "kubectl patch", "kube delete", "kube patch",
		"FUGUE_API_KEY", "FUGUE_BOOTSTRAP_KEY", "FUGUE_DATABASE_URL", "FUGUE_BUNDLE_SIGNING_KEY",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("shadow deploy script gained forbidden capability %q", forbidden)
		}
	}
	command := exec.Command("bash", "-n", "../../scripts/deploy_edge_control_shadow.sh")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("shadow deploy script is invalid: %v output=%s", err, output)
	}
}
