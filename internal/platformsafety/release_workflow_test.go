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
	On   releaseWorkflowTriggers       `yaml:"on"`
	Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowTriggers struct {
	Push             releaseWorkflowPushTrigger      `yaml:"push"`
	WorkflowDispatch *releaseWorkflowDispatchTrigger `yaml:"workflow_dispatch"`
}

type releaseWorkflowPushTrigger struct {
	Paths []string `yaml:"paths"`
}

type releaseWorkflowDispatchTrigger struct {
	Inputs map[string]yaml.Node `yaml:"inputs"`
}

type releaseWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	If              string                `yaml:"if"`
	Outputs         map[string]string     `yaml:"outputs"`
	ContinueOnError bool                  `yaml:"continue-on-error"`
	Steps           []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	ID   string            `yaml:"id"`
	Name string            `yaml:"name"`
	If   string            `yaml:"if"`
	Uses string            `yaml:"uses"`
	Env  map[string]string `yaml:"env"`
	With map[string]string `yaml:"with"`
	Run  string            `yaml:"run"`
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
	if workflow.On.WorkflowDispatch == nil {
		t.Fatal("control-plane workflow must support workflow_dispatch")
	}
	if len(workflow.On.WorkflowDispatch.Inputs) != 0 {
		t.Fatalf("workflow_dispatch must not expose release bypass inputs: %+v", workflow.On.WorkflowDispatch.Inputs)
	}
	workflowSource := string(data)
	if strings.Contains(workflowSource, "existing_image_tag") || strings.Contains(workflowSource, "inputs.") {
		t.Fatal("control-plane workflow must not retain a dispatch input or an equivalent inputs-context bypass")
	}
	if count := strings.Count(workflowSource, "workflow_dispatch"); count != 1 {
		t.Fatalf("workflow_dispatch must appear only as an unconditional trigger, found %d references", count)
	}
	for _, required := range []string{
		"Makefile",
		"scripts/test_release_domain_safety.sh",
	} {
		if !containsString(workflow.On.Push.Paths, required) {
			t.Fatalf("control-plane workflow must run when release-gate dependency %q changes", required)
		}
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

	baseline, ok := workflow.Jobs["release-baseline"]
	if !ok {
		t.Fatal("control-plane workflow must define release-baseline")
	}
	for key, want := range map[string]string{
		"changed_files": "${{ steps.release_changes.outputs.changed_files }}",
		"baseline_refs": "${{ steps.release_changes.outputs.baseline_refs }}",
		"target_ref":    "${{ steps.release_changes.outputs.target_ref }}",
	} {
		if got := baseline.Outputs[key]; got != want {
			t.Fatalf("release baseline output %s drifted: got %q want %q", key, got, want)
		}
	}
	const checkoutAction = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
	for _, jobName := range []string{"release-baseline", "release-gate", "build", "deploy"} {
		job, exists := workflow.Jobs[jobName]
		if !exists {
			t.Fatalf("control-plane workflow must define %s", jobName)
		}
		checkout := workflowStepByName(t, job, "Checkout")
		if checkout.Uses != checkoutAction {
			t.Fatalf("%s checkout must use the pinned action: got %q want %q", jobName, checkout.Uses, checkoutAction)
		}
		if got, want := checkout.With["ref"], "${{ github.sha }}"; got != want {
			t.Fatalf("%s checkout must bind the exact event commit: got %q want %q", jobName, got, want)
		}
	}
	checkoutCount := 0
	for jobName, job := range workflow.Jobs {
		if strings.Contains(job.If, "workflow_dispatch") {
			t.Fatalf("job %s must not condition behavior on workflow_dispatch: %q", jobName, job.If)
		}
		for _, step := range job.Steps {
			if strings.Contains(step.If, "workflow_dispatch") {
				t.Fatalf("step %s/%s must not condition behavior on workflow_dispatch: %q", jobName, step.Name, step.If)
			}
			if strings.HasPrefix(step.Uses, "actions/checkout@") {
				checkoutCount++
				if step.Uses != checkoutAction {
					t.Fatalf("step %s/%s uses an unapproved checkout action: %q", jobName, step.Name, step.Uses)
				}
				if got, want := step.With["ref"], "${{ github.sha }}"; got != want {
					t.Fatalf("step %s/%s checkout ref drifted: got %q want %q", jobName, step.Name, got, want)
				}
			}
		}
	}
	if checkoutCount != 4 {
		t.Fatalf("control-plane workflow must bind exactly four checkout steps, found %d", checkoutCount)
	}

	baselineLiveImages := workflowStepByName(t, baseline, "Resolve live image metadata")
	if baselineLiveImages.ID != "live_images" {
		t.Fatalf("release baseline live image step id drifted: %q", baselineLiveImages.ID)
	}
	if got, want := baselineLiveImages.Env["FUGUE_IMAGE_TAG"], "${{ github.sha }}"; got != want {
		t.Fatalf("release baseline image target must be the dispatched commit: got %q want %q", got, want)
	}
	baselineChanges := workflowStepByName(t, baseline, "Compute live-to-target release changed files")
	if baselineChanges.ID != "release_changes" {
		t.Fatalf("release baseline changed-files step id drifted: %q", baselineChanges.ID)
	}
	if got, want := baselineChanges.Env["FUGUE_RELEASE_TARGET_REF"], "${{ github.sha }}"; got != want {
		t.Fatalf("release baseline diff target must be the dispatched commit: got %q want %q", got, want)
	}

	build, ok := workflow.Jobs["build"]
	if !ok || !containsWorkflowNeed(build.Needs, "release-baseline") || !containsWorkflowNeed(build.Needs, "release-gate") {
		t.Fatal("image build must wait for release-baseline and release-gate")
	}
	if strings.TrimSpace(build.If) != "" {
		t.Fatalf("image build must run for push and workflow_dispatch without a bypass condition: %q", build.If)
	}
	for key, want := range map[string]string{
		"image_tag":                        "${{ steps.meta.outputs.image_tag }}",
		"api_image_repository":             "${{ steps.meta.outputs.api_image_repository }}",
		"controller_image_repository":      "${{ steps.meta.outputs.controller_image_repository }}",
		"drain_agent_image_repository":     "${{ steps.meta.outputs.drain_agent_image_repository }}",
		"telemetry_agent_image_repository": "${{ steps.meta.outputs.telemetry_agent_image_repository }}",
		"image_cache_image_repository":     "${{ steps.meta.outputs.image_cache_image_repository }}",
		"edge_image_repository":            "${{ steps.meta.outputs.edge_image_repository }}",
		"build_api":                        "${{ steps.plan.outputs.build_api }}",
		"build_controller":                 "${{ steps.plan.outputs.build_controller }}",
		"build_drain_agent":                "${{ steps.plan.outputs.build_drain_agent }}",
		"build_telemetry_agent":            "${{ steps.plan.outputs.build_telemetry_agent }}",
		"build_image_cache":                "${{ steps.plan.outputs.build_image_cache }}",
		"build_edge":                       "${{ steps.plan.outputs.build_edge }}",
	} {
		if got := build.Outputs[key]; got != want {
			t.Fatalf("image build output %s drifted: got %q want %q", key, got, want)
		}
	}
	buildMeta := workflowStepByName(t, build, "Compute image metadata")
	if buildMeta.ID != "meta" {
		t.Fatalf("image metadata step id drifted: %q", buildMeta.ID)
	}
	const imageTagOutput = `echo "image_tag=${GITHUB_SHA}" >> "${GITHUB_OUTPUT}"`
	if strings.Count(buildMeta.Run, "image_tag=") != 1 || !strings.Contains(buildMeta.Run, imageTagOutput) {
		t.Fatalf("image metadata must publish only GITHUB_SHA as image_tag: %q", buildMeta.Run)
	}
	buildPlan := workflowStepByName(t, build, "Compute image build plan")
	if buildPlan.ID != "plan" {
		t.Fatalf("image build-plan step id drifted: %q", buildPlan.ID)
	}
	if got, want := buildPlan.Env["FUGUE_RELEASE_TARGET_REF"], "${{ needs.release-baseline.outputs.target_ref }}"; got != want {
		t.Fatalf("image build plan must use the baseline target ref: got %q want %q", got, want)
	}
	buildPush := workflowStepByName(t, build, "Build and push selected control-plane images")
	if buildPush.ID != "build_images" {
		t.Fatalf("image build-push step id drifted: %q", buildPush.ID)
	}
	if got, want := buildPush.Env["FUGUE_IMAGE_TAG"], "${{ steps.meta.outputs.image_tag }}"; got != want {
		t.Fatalf("image build-push tag source drifted: got %q want %q", got, want)
	}
	if got, want := buildPush.Env["FUGUE_CONTROL_PLANE_IMAGE_TARGETS"], "${{ steps.plan.outputs.targets }}"; got != want {
		t.Fatalf("image build-push target source drifted: got %q want %q", got, want)
	}

	deploy, ok := workflow.Jobs["deploy"]
	if !ok || !containsWorkflowNeed(deploy.Needs, "release-baseline") || !containsWorkflowNeed(deploy.Needs, "release-gate") || !containsWorkflowNeed(deploy.Needs, "build") {
		t.Fatal("control-plane deploy must wait for release-baseline, release-gate, and build")
	}
	const deployCondition = "${{ always() && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' }}"
	if strings.TrimSpace(deploy.If) != deployCondition {
		t.Fatalf("deploy condition must require every prerequisite success without bypass: got %q want %q", deploy.If, deployCondition)
	}

	statefulGuard := workflowStepByName(t, deploy, "Guard stateful component files")
	if strings.TrimSpace(statefulGuard.If) != "" {
		t.Fatalf("stateful component guard must run for every event: %q", statefulGuard.If)
	}
	if got, want := statefulGuard.Env["FUGUE_RELEASE_CHANGED_FILES"], "${{ needs.release-baseline.outputs.changed_files }}"; got != want {
		t.Fatalf("stateful component guard must consume the trusted baseline changed files: got %q want %q", got, want)
	}
	if !strings.Contains(statefulGuard.Run, "independent controlled release window") || strings.Contains(statefulGuard.Run, "manual release") {
		t.Fatal("stateful component guard must direct operators to an independent controlled release window")
	}
	const deployImageTag = "${{ needs.build.outputs.image_tag || github.sha }}"
	explain := workflowStepByName(t, deploy, "Explain runner and fail closed target")
	if got := explain.Env["FUGUE_IMAGE_TAG"]; got != deployImageTag {
		t.Fatalf("deploy attribution must use the built image tag chain: got %q want %q", got, deployImageTag)
	}
	deployLiveImages := workflowStepByName(t, deploy, "Resolve live image metadata")
	if deployLiveImages.ID != "live_images" {
		t.Fatalf("deploy live image step id drifted: %q", deployLiveImages.ID)
	}
	if got := deployLiveImages.Env["FUGUE_IMAGE_TAG"]; got != deployImageTag {
		t.Fatalf("deploy live image resolution must use the built image tag chain: got %q want %q", got, deployImageTag)
	}

	upgrade := workflowStepByName(t, deploy, "Upgrade Fugue control plane")
	if strings.TrimSpace(upgrade.If) != "" {
		t.Fatalf("control-plane upgrade must not have an event-specific bypass condition: %q", upgrade.If)
	}
	for key, want := range map[string]string{
		"FUGUE_API_IMAGE_REPOSITORY":             "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.api_image_repository || steps.live_images.outputs.api_image_repository }}",
		"FUGUE_API_IMAGE_TAG":                    "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.api_image_tag }}",
		"FUGUE_CONTROLLER_IMAGE_REPOSITORY":      "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.controller_image_repository || steps.live_images.outputs.controller_image_repository }}",
		"FUGUE_CONTROLLER_IMAGE_TAG":             "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.controller_image_tag }}",
		"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY":     "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.drain_agent_image_repository || steps.live_images.outputs.drain_agent_image_repository }}",
		"FUGUE_DRAIN_AGENT_IMAGE_TAG":            "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.drain_agent_image_tag }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY": "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.telemetry_agent_image_repository || steps.live_images.outputs.telemetry_agent_image_repository }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_TAG":        "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.telemetry_agent_image_tag }}",
		"FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY":     "${{ needs.build.outputs.build_image_cache == 'true' && needs.build.outputs.image_cache_image_repository || steps.live_images.outputs.image_cache_image_repository }}",
		"FUGUE_IMAGE_CACHE_IMAGE_TAG":            "${{ needs.build.outputs.build_image_cache == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.image_cache_image_tag }}",
		"FUGUE_EDGE_IMAGE_REPOSITORY":            "${{ needs.build.outputs.build_edge == 'true' && needs.build.outputs.edge_image_repository || steps.live_images.outputs.edge_image_repository }}",
		"FUGUE_EDGE_IMAGE_TAG":                   "${{ needs.build.outputs.build_edge == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.edge_image_tag }}",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("upgrade image selection %s drifted: got %q want %q", key, got, want)
		}
	}
	if got, want := upgrade.Env["FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE"], "${{ vars.FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE || needs.build.outputs.build_edge == 'true' }}"; got != want {
		t.Fatalf("public data-plane auto release must depend only on explicit policy or an edge build: got %q want %q", got, want)
	}
}

func workflowStepByName(t *testing.T, job releaseWorkflowJob, name string) releaseWorkflowStep {
	t.Helper()
	var match releaseWorkflowStep
	found := false
	for _, step := range job.Steps {
		if step.Name == name {
			if found {
				t.Fatalf("workflow job defines duplicate step %q", name)
			}
			match = step
			found = true
		}
	}
	if !found {
		t.Fatalf("workflow job does not define step %q", name)
	}
	return match
}

func containsWorkflowNeed(needs workflowNeeds, expected string) bool {
	return containsString([]string(needs), expected)
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
