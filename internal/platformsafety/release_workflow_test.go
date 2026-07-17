package platformsafety

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	On          releaseWorkflowTriggers       `yaml:"on"`
	Permissions map[string]string             `yaml:"permissions"`
	Jobs        map[string]releaseWorkflowJob `yaml:"jobs"`
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

type releaseWorkflowDispatchInput struct {
	Required bool       `yaml:"required"`
	Type     string     `yaml:"type"`
	Default  *yaml.Node `yaml:"default"`
}

type releaseWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	If              string                `yaml:"if"`
	Outputs         map[string]string     `yaml:"outputs"`
	Permissions     map[string]string     `yaml:"permissions"`
	ContinueOnError bool                  `yaml:"continue-on-error"`
	Steps           []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	ID              string            `yaml:"id"`
	Name            string            `yaml:"name"`
	If              string            `yaml:"if"`
	Uses            string            `yaml:"uses"`
	Env             map[string]string `yaml:"env"`
	With            map[string]string `yaml:"with"`
	Run             string            `yaml:"run"`
	ContinueOnError bool              `yaml:"continue-on-error"`
}

func TestRP0MigrationLaneRegistrationIsHostedAndZeroWrite(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "migrate-control-plane-release-baseline-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 migration lane registration workflow: %v", err)
	}
	const expectedSource = `name: prepare-control-plane-release-baseline-rp0

on:
  workflow_dispatch:
    inputs:
      expected_sha:
        description: Exact lowercase main SHA registering the dormant RP0 migration lane
        required: true
        type: string

permissions: {}

concurrency:
  group: fugue-release-policy-rp0-migration-lane-registration-v1
  cancel-in-progress: false

jobs:
  prove-hosted-zero-write-registration:
    runs-on: ubuntu-latest
    timeout-minutes: 3
    environment: production
    permissions: {}
    steps:
      - name: Verify exact SHA and observe unchanged production health
        env:
          EXPECTED_SHA: ${{ inputs.expected_sha }}
        run: |
          set -euo pipefail
          readonly health_url='https://api.fugue.pro/healthz'
          [[ "${GITHUB_EVENT_NAME}" == 'workflow_dispatch' ]]
          [[ "${GITHUB_REF}" == 'refs/heads/main' ]]
          [[ "${GITHUB_RUN_ATTEMPT}" == '1' ]]
          [[ "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]]
          [[ "${EXPECTED_SHA}" == "${GITHUB_SHA}" ]]
          for sample in 1 2 3 4 5; do
            response="$(curl --fail --silent --show-error \
              --connect-timeout 5 --max-time 10 "${health_url}")"
            python3 - "${response}" <<'PY'
          import json, sys
          if json.loads(sys.argv[1]) != {"status": "ok"}:
              raise SystemExit("production health payload drifted")
          PY
            [[ "${sample}" == '5' ]] || sleep 15
          done
          printf '%s\n' 'hosted RP0 migration lane registration is exact-SHA and zero-write'
`
	if got := string(data); got != expectedSource {
		t.Fatalf("RP0 registration workflow must match the exact reviewed zero-write source\ngot:\n%s", got)
	}
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			RunsOn         string                `yaml:"runs-on"`
			TimeoutMinutes int                   `yaml:"timeout-minutes"`
			Environment    string                `yaml:"environment"`
			Permissions    map[string]string     `yaml:"permissions"`
			Steps          []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 migration lane registration workflow: %v", err)
	}
	workflowDispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("RP0 registration must be dispatch-only with one input: %+v", workflow.On)
	}
	var workflowDispatch releaseWorkflowDispatchTrigger
	if err := workflowDispatchNode.Decode(&workflowDispatch); err != nil {
		t.Fatalf("decode RP0 workflow_dispatch trigger: %v", err)
	}
	if len(workflowDispatch.Inputs) != 1 {
		t.Fatalf("RP0 registration must expose only expected_sha: %+v", workflowDispatch.Inputs)
	}
	expectedSHAInput, ok := workflowDispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("RP0 registration must require expected_sha")
	}
	var expectedSHA releaseWorkflowDispatchInput
	if err := expectedSHAInput.Decode(&expectedSHA); err != nil {
		t.Fatalf("decode RP0 expected_sha input: %v", err)
	}
	if !expectedSHA.Required || expectedSHA.Type != "string" || expectedSHA.Default != nil {
		t.Fatalf("RP0 expected_sha must be a required string without a default: %+v", expectedSHA)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("RP0 registration must have empty top-level permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["prove-hosted-zero-write-registration"]
	if !ok {
		t.Fatal("RP0 registration job is absent")
	}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 3 || job.Environment != "production" || len(job.Permissions) != 0 {
		t.Fatalf("RP0 registration job must be hosted, bounded, production-scoped, and permissions-empty: %+v", job)
	}
	if len(job.Steps) != 1 || job.Steps[0].Uses != "" || job.Steps[0].Name != "Verify exact SHA and observe unchanged production health" {
		t.Fatalf("RP0 registration must contain one shell-only verification step: %+v", job.Steps)
	}
	step := job.Steps[0]
	if len(step.Env) != 1 {
		t.Fatalf("RP0 registration step must expose only EXPECTED_SHA: %+v", step.Env)
	}
	if got, want := step.Env["EXPECTED_SHA"], "${{ inputs.expected_sha }}"; got != want {
		t.Fatalf("RP0 registration expected SHA binding drifted: got %q want %q", got, want)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`"${GITHUB_RUN_ATTEMPT}" == '1'`,
		`"${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$`,
		`"${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		"for sample in 1 2 3 4 5",
		"sleep 15",
		"https://api.fugue.pro/healthz",
		`{"status": "ok"}`,
	} {
		if !strings.Contains(step.Run, required) {
			t.Fatalf("RP0 registration verification must contain %q", required)
		}
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "actions/checkout", "contents: write", "actions: write",
		"kubectl ", "helm ", "ssh ", "docker ", "gh api", "git push",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("RP0 registration contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestDisabledWorkflowRerunProbeIsHostedPermissionsEmptyAndZeroWrite(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "probe-disabled-workflow-rerun.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read disabled-workflow rerun probe: %v", err)
	}
	const expectedSource = `name: probe-disabled-workflow-rerun

on:
  workflow_dispatch:
    inputs:
      expected_sha:
        description: Exact lowercase main SHA for the harmless disabled-workflow rerun probe
        required: true
        type: string

permissions: {}

concurrency:
  group: fugue-release-policy-disabled-workflow-rerun-probe-v1
  cancel-in-progress: false

jobs:
  prove-hosted-zero-write-probe:
    runs-on: ubuntu-latest
    timeout-minutes: 3
    permissions: {}
    steps:
      - name: Verify exact SHA and observe unchanged production health
        env:
          EXPECTED_SHA: ${{ inputs.expected_sha }}
        run: |
          set -euo pipefail
          readonly health_url='https://api.fugue.pro/healthz'
          [[ "${GITHUB_EVENT_NAME}" == 'workflow_dispatch' ]]
          [[ "${GITHUB_REF}" == 'refs/heads/main' ]]
          [[ "${GITHUB_RUN_ATTEMPT}" == '1' ]]
          [[ "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]]
          [[ "${EXPECTED_SHA}" == "${GITHUB_SHA}" ]]
          for sample in 1 2 3 4 5; do
            response="$(curl --fail --silent --show-error \
              --connect-timeout 5 --max-time 10 "${health_url}")"
            python3 - "${response}" <<'PY'
          import json, sys
          if json.loads(sys.argv[1]) != {"status": "ok"}:
              raise SystemExit("production health payload drifted")
          PY
            [[ "${sample}" == '5' ]] || sleep 15
          done
          printf '%s\n' 'disabled-workflow rerun probe is exact-SHA, hosted, permissions-empty, and zero-write'
`
	if got := string(data); got != expectedSource {
		t.Fatalf("disabled-workflow rerun probe must match the exact reviewed zero-write source\ngot:\n%s", got)
	}
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			RunsOn          string                `yaml:"runs-on"`
			TimeoutMinutes  int                   `yaml:"timeout-minutes"`
			Environment     string                `yaml:"environment"`
			Needs           workflowNeeds         `yaml:"needs"`
			If              string                `yaml:"if"`
			Outputs         map[string]string     `yaml:"outputs"`
			Permissions     map[string]string     `yaml:"permissions"`
			ContinueOnError bool                  `yaml:"continue-on-error"`
			Steps           []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse disabled-workflow rerun probe: %v", err)
	}
	workflowDispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("disabled-workflow rerun probe must be dispatch-only: %+v", workflow.On)
	}
	var workflowDispatch releaseWorkflowDispatchTrigger
	if err := workflowDispatchNode.Decode(&workflowDispatch); err != nil {
		t.Fatalf("decode disabled-workflow rerun probe trigger: %v", err)
	}
	if len(workflowDispatch.Inputs) != 1 {
		t.Fatalf("disabled-workflow rerun probe must expose only expected_sha: %+v", workflowDispatch.Inputs)
	}
	expectedSHAInput, ok := workflowDispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("disabled-workflow rerun probe must require expected_sha")
	}
	var expectedSHA releaseWorkflowDispatchInput
	if err := expectedSHAInput.Decode(&expectedSHA); err != nil {
		t.Fatalf("decode disabled-workflow rerun probe expected_sha: %v", err)
	}
	if !expectedSHA.Required || expectedSHA.Type != "string" || expectedSHA.Default != nil {
		t.Fatalf("disabled-workflow rerun probe expected_sha must be required without a default: %+v", expectedSHA)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("disabled-workflow rerun probe must have empty top-level permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["prove-hosted-zero-write-probe"]
	if !ok {
		t.Fatal("disabled-workflow rerun probe job is absent")
	}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 3 || job.Environment != "" || len(job.Permissions) != 0 {
		t.Fatalf("disabled-workflow rerun probe must be hosted, bounded, environment-free, and permissions-empty: %+v", job)
	}
	if len(job.Needs) != 0 || job.If != "" || len(job.Outputs) != 0 || job.ContinueOnError {
		t.Fatalf("disabled-workflow rerun probe must not depend on, gate, export, or soften another job: %+v", job)
	}
	if len(job.Steps) != 1 {
		t.Fatalf("disabled-workflow rerun probe must contain exactly one step: %+v", job.Steps)
	}
	step := job.Steps[0]
	if step.Name != "Verify exact SHA and observe unchanged production health" || step.Uses != "" || step.If != "" || len(step.With) != 0 || step.ContinueOnError {
		t.Fatalf("disabled-workflow rerun probe must contain one strict shell-only step: %+v", step)
	}
	if len(step.Env) != 1 || step.Env["EXPECTED_SHA"] != "${{ inputs.expected_sha }}" {
		t.Fatalf("disabled-workflow rerun probe expected SHA binding drifted: %+v", step.Env)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`"${GITHUB_RUN_ATTEMPT}" == '1'`,
		`"${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$`,
		`"${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		"for sample in 1 2 3 4 5",
		"sleep 15",
		"https://api.fugue.pro/healthz",
		`{"status": "ok"}`,
	} {
		if !strings.Contains(step.Run, required) {
			t.Fatalf("disabled-workflow rerun probe must contain %q", required)
		}
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "actions/checkout", "uses:", "environment:",
		"contents:", "actions:", "id-token:", "GITHUB_TOKEN", "github.token", "secrets.",
		"kubectl ", "helm ", "ssh ", "scp ", "rsync ", "docker ", "gh ",
		"git push", "git tag", "git update-ref", "curl -X", "curl --request",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("disabled-workflow rerun probe contains out-of-scope capability %q", forbidden)
		}
	}
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
	if len(workflow.On.WorkflowDispatch.Inputs) != 1 {
		t.Fatalf("workflow_dispatch must expose only expected_sha: %+v", workflow.On.WorkflowDispatch.Inputs)
	}
	expectedSHAInput, ok := workflow.On.WorkflowDispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("workflow_dispatch must require expected_sha")
	}
	var expectedSHA releaseWorkflowDispatchInput
	if err := expectedSHAInput.Decode(&expectedSHA); err != nil {
		t.Fatalf("decode expected_sha input: %v", err)
	}
	if !expectedSHA.Required || expectedSHA.Type != "string" || expectedSHA.Default != nil {
		t.Fatalf("expected_sha must be a required string without a default: %+v", expectedSHA)
	}
	workflowSource := string(data)
	if strings.Contains(workflowSource, "existing_image_tag") || len(workflow.On.Push.Paths) != 0 {
		t.Fatal("control-plane release must be dispatch-only without an image bypass")
	}

	inputGuard, ok := workflow.Jobs["release-input-guard"]
	if !ok {
		t.Fatal("control-plane workflow must define release-input-guard")
	}
	guard := workflowStepByName(t, inputGuard, "Guard exact main commit authorization")
	for key, want := range map[string]string{
		"EXPECTED_SHA":   "${{ inputs.expected_sha }}",
		"ACTUAL_SHA":     "${{ github.sha }}",
		"EVENT_NAME":     "${{ github.event_name }}",
		"EVENT_REF":      "${{ github.ref }}",
		"EVENT_REF_NAME": "${{ github.ref_name }}",
		"EVENT_REF_TYPE": "${{ github.ref_type }}",
	} {
		if got := guard.Env[key]; got != want {
			t.Fatalf("release input guard env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{"refs/heads/main", "^[0-9a-f]{40}$", `"${EXPECTED_SHA}" == "${ACTUAL_SHA}"`} {
		if !strings.Contains(guard.Run, required) {
			t.Fatalf("release input guard must contain %q", required)
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
		"bash scripts/test_release_domain_workflow.sh",
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
		"domain_base_sha":         "${{ steps.domain_baseline.outputs.domain_base_sha }}",
		"baseline_ref_object_sha": "${{ steps.domain_baseline.outputs.baseline_ref_object_sha }}",
		"changed_files":           "${{ steps.release_changes.outputs.changed_files }}",
		"baseline_refs":           "${{ steps.release_changes.outputs.baseline_refs }}",
		"target_ref":              "${{ steps.release_changes.outputs.target_ref }}",
	} {
		if got := baseline.Outputs[key]; got != want {
			t.Fatalf("release baseline output %s drifted: got %q want %q", key, got, want)
		}
	}
	const checkoutAction = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
	for _, jobName := range []string{"release-baseline", "release-gate", "build", "deploy", "record-release-baseline"} {
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
	if checkoutCount != 5 {
		t.Fatalf("control-plane workflow must bind exactly five checkout steps, found %d", checkoutCount)
	}

	if !containsWorkflowNeed(baseline.Needs, "release-input-guard") {
		t.Fatal("release-baseline must wait for the exact input guard")
	}
	domainBaseline := workflowStepByName(t, baseline, "Resolve release-domain baseline")
	for _, required := range []string{
		"fugue-control-plane-release-baseline",
		"723116882214ae9efeaee0877bb378d0db2dcea7",
		"8b4bdc2a2b443be6d1244f9b4739cd0be1313d71",
		"4d74c6f963258f9f5c3925613891db9163327330",
		`"${fetched_ref_object_sha}" == "${remote_object}"`,
		`"${remote_object}" == "${domain_base_sha}"`,
		`"${actual_parent}" == "${genesis_parent_sha}"`,
		`"${parent_b3}" == "${genesis_b3_sha}"`,
		`"${b3_base}" == "${genesis_base_sha}"`,
		`domain_base_sha="${genesis_base_sha}"`,
		"git merge-base --is-ancestor",
	} {
		if !strings.Contains(domainBaseline.Run, required) {
			t.Fatalf("release-domain baseline resolver must contain %q", required)
		}
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
	if got, want := baselineChanges.Env["FUGUE_RELEASE_BASE_REFS"], "${{ steps.live_images.outputs.release_baseline_tags }}"; got != want {
		t.Fatalf("release image diff must retain the live deployed image baselines: got %q want %q", got, want)
	}

	build, ok := workflow.Jobs["build"]
	if !ok || !containsWorkflowNeed(build.Needs, "release-baseline") || !containsWorkflowNeed(build.Needs, "release-gate") {
		t.Fatal("image build must wait for release-baseline and release-gate")
	}
	if strings.TrimSpace(build.If) != "" {
		t.Fatalf("image build must run after the guarded dispatch without a bypass condition: %q", build.If)
	}
	if got, want := build.Permissions, map[string]string{"contents": "read", "packages": "write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("image build permissions drifted: got %v want %v", got, want)
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
	buildProvenance := workflowStepByName(t, build, "Publish verified control-plane image provenance")
	if buildProvenance.ID != "build_images" {
		t.Fatalf("image provenance step id drifted: %q", buildProvenance.ID)
	}
	if strings.TrimSpace(buildProvenance.If) != "" {
		t.Fatalf("image provenance must be published for empty and non-empty build plans: %q", buildProvenance.If)
	}
	if got, want := buildProvenance.Env["FUGUE_IMAGE_TAG"], "${{ steps.meta.outputs.image_tag }}"; got != want {
		t.Fatalf("image provenance tag source drifted: got %q want %q", got, want)
	}
	if got, want := buildProvenance.Env["FUGUE_CONTROL_PLANE_IMAGE_TARGETS"], "${{ steps.plan.outputs.targets }}"; got != want {
		t.Fatalf("image provenance target source drifted: got %q want %q", got, want)
	}

	deploy, ok := workflow.Jobs["deploy"]
	if !ok || !containsWorkflowNeed(deploy.Needs, "release-baseline") || !containsWorkflowNeed(deploy.Needs, "release-gate") || !containsWorkflowNeed(deploy.Needs, "build") {
		t.Fatal("control-plane deploy must wait for release-baseline, release-gate, and build")
	}
	const deployCondition = "${{ always() && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' }}"
	if strings.TrimSpace(deploy.If) != deployCondition {
		t.Fatalf("deploy condition must require every prerequisite success without bypass: got %q want %q", deploy.If, deployCondition)
	}
	if got, want := deploy.Permissions, map[string]string{"actions": "read", "contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deploy permissions drifted: got %v want %v", got, want)
	}
	if deploy.ContinueOnError {
		t.Fatal("deploy job must fail closed")
	}
	buildTools := workflowStepByName(t, deploy, "Build private release-domain tools")
	for _, required := range []string{
		"${RUNNER_TEMP}/fugue-release-tools",
		"for goarch in amd64 arm64; do",
		"CGO_ENABLED=0",
		`GOARCH="${goarch}"`,
		"GOOS=linux",
		"GOFLAGS=-mod=readonly",
		"go list -mod=readonly -buildvcs=false -deps ./cmd/...",
		"go mod verify",
		"GOPROXY=https://proxy.golang.org",
		"'GOVCS=*:off'",
		"git diff --exit-code -- go.mod go.sum",
		"./cmd/fugue-release-domain-evidence",
		"./cmd/fugue-release-domain-dispatch",
		"chmod 0700",
	} {
		if !strings.Contains(buildTools.Run, required) {
			t.Fatalf("deploy release tool build must contain %q", required)
		}
	}
	if strings.Contains(buildTools.Run, "go mod download all") {
		t.Fatal("deploy release tool build must not preload unrelated module versions")
	}
	if strings.Contains(buildTools.Run, "GOPROXY=off") {
		t.Fatal("deploy release tool cache validation must not disable the module proxy")
	}
	preloadIndex := strings.Index(buildTools.Run, "go list -mod=readonly -buildvcs=false -deps ./cmd/...")
	verifyIndex := strings.Index(buildTools.Run, "go mod verify")
	evidenceBuildIndex := strings.Index(buildTools.Run, `go build -trimpath -o "${tools_dir}/fugue-release-domain-evidence"`)
	if preloadIndex < 0 || verifyIndex < 0 || evidenceBuildIndex < 0 || preloadIndex >= verifyIndex || verifyIndex >= evidenceBuildIndex {
		t.Fatal("deploy must preload and verify both command dependency graphs before building evidence")
	}
	genesisEvidence := workflowStepByName(t, deploy, "Write genesis public release evidence")
	if got, want := genesisEvidence.If, "${{ needs.release-baseline.outputs.is_genesis == 'true' }}"; got != want {
		t.Fatalf("genesis evidence condition drifted: got %q want %q", got, want)
	}
	for _, required := range []string{
		"write-genesis-public-evidence",
		`--ownership "${GITHUB_WORKSPACE}/deploy/release-domains/ownership-v1.yaml"`,
		`--expected-head-sha "${GENESIS_SHA}"`,
		`--evidence-base-sha "${DOMAIN_BASE_SHA}"`,
		`--actual-parent-sha "${GENESIS_PARENT_SHA}"`,
	} {
		if !strings.Contains(genesisEvidence.Run, required) {
			t.Fatalf("genesis evidence command must contain %q", required)
		}
	}
	expectedGenesisChanges := []string{
		".github/workflows/deploy-control-plane.yml",
		"cmd/fugue-release-domain-dispatch/classify_files.go",
		"cmd/fugue-release-domain-dispatch/main.go",
		"cmd/fugue-release-domain-dispatch/main_test.go",
		"cmd/fugue-release-domain-dispatch/public_evidence.go",
		"cmd/fugue-release-domain-dispatch/public_evidence_test.go",
		"cmd/fugue-release-domain-dispatch/secure_files.go",
		"cmd/fugue-release-domain-dispatch/stat_times_darwin.go",
		"cmd/fugue-release-domain-dispatch/stat_times_linux.go",
		"cmd/fugue-release-domain-dispatch/stat_times_other.go",
		"cmd/fugue-release-domain-dispatch/strict_json.go",
		"cmd/fugue-release-domain-evidence/evidence.go",
		"cmd/fugue-release-domain-evidence/evidence_test.go",
		"cmd/fugue-release-domain-evidence/main.go",
		"cmd/fugue-release-domain-evidence/manifest.go",
		"cmd/fugue-release-domain-evidence/manifest_test.go",
		"cmd/fugue-release-domain-plan/main.go",
		"cmd/fugue-release-domain-plan/main_test.go",
		"cmd/fugue-release-domain-plan/output.go",
		"cmd/fugue-release-domain-plan/output_test.go",
		"deploy/release-domains/ownership-v1.yaml",
		"docs/runbooks/release-domain-planner.md",
		"internal/api/topology_labeler_test.go",
		"internal/platformsafety/release_workflow_test.go",
		"internal/releaseadapter/adapter.go",
		"internal/releaseadapter/dispatcher.go",
		"internal/releaseadapter/dispatcher_test.go",
		"internal/releaseadapter/trace.go",
		"internal/releaseadapter/transaction.go",
		"internal/releaseadapter/transaction_test.go",
		"internal/releasedomain/changed_file_evidence.go",
		"internal/releasedomain/changed_file_evidence_test.go",
		"internal/releasedomain/file_classifier_test.go",
		"internal/releasedomain/ownership_test.go",
		"internal/releasedomain/plan_artifacts.go",
		"internal/releasedomain/plan_artifacts_test.go",
		"internal/releasedomain/rendered_classifier_test.go",
		"internal/releasedomain/rollback_ownership.go",
		"internal/releasedomain/rollback_ownership_test.go",
		"internal/releasedomain/transaction_envelope.go",
		"internal/releaseevidence/public.go",
		"internal/releaseevidence/public_test.go",
		"scripts/lib/control_plane_release_domain_production.sh",
		"scripts/lib/control_plane_release_domains.sh",
		"scripts/lib/control_plane_release_render.sh",
		"scripts/test_control_plane_release_domain_production.sh",
		"scripts/test_control_plane_release_main_wiring.sh",
		"scripts/test_control_plane_release_render.sh",
		"scripts/test_release_domain_safety.sh",
		"scripts/test_release_domain_workflow.sh",
		"scripts/test_single_domain_release.sh",
		"scripts/upgrade_fugue_control_plane.sh",
	}
	if len(expectedGenesisChanges) != 52 {
		t.Fatalf("genesis expected-change allowlist must contain exactly 52 paths, found %d", len(expectedGenesisChanges))
	}
	seenGenesisChanges := make(map[string]struct{}, len(expectedGenesisChanges))
	for _, path := range expectedGenesisChanges {
		if path == "" {
			t.Fatal("genesis expected-change allowlist contains an empty path")
		}
		if _, duplicate := seenGenesisChanges[path]; duplicate {
			t.Fatalf("genesis expected-change allowlist repeats %q", path)
		}
		seenGenesisChanges[path] = struct{}{}
	}
	const expectedChangePrefix = `--expected-change "`
	const expectedChangeSuffix = "\" \\"
	actualGenesisChanges := make([]string, 0, len(expectedGenesisChanges))
	for _, line := range strings.Split(genesisEvidence.Run, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, "--expected-change") {
			continue
		}
		if !strings.HasPrefix(trimmed, expectedChangePrefix) || !strings.HasSuffix(trimmed, expectedChangeSuffix) {
			t.Fatalf("genesis expected-change must be one literal quoted path per flag: %q", trimmed)
		}
		path := strings.TrimSuffix(strings.TrimPrefix(trimmed, expectedChangePrefix), expectedChangeSuffix)
		actualGenesisChanges = append(actualGenesisChanges, path)
	}
	if !reflect.DeepEqual(actualGenesisChanges, expectedGenesisChanges) {
		t.Fatalf("genesis expected-change allowlist drifted:\n got: %q\nwant: %q", actualGenesisChanges, expectedGenesisChanges)
	}
	for _, line := range strings.Split(genesisEvidence.Run, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "upgrade_fugue_control_plane.sh") && !strings.HasPrefix(trimmed, expectedChangePrefix) {
			t.Fatal("genesis evidence path must never invoke the upgrade script")
		}
	}

	statefulGuard := workflowStepByName(t, deploy, "Guard stateful component files")
	const nonGenesisCondition = "${{ needs.release-baseline.outputs.is_genesis != 'true' }}"
	genesisReachable := map[string]string{
		"Checkout":                              "",
		"Setup Go":                              "",
		"Build private release-domain tools":    "",
		"Write genesis public release evidence": "${{ needs.release-baseline.outputs.is_genesis == 'true' }}",
		"Upload release-domain public evidence": "always()",
	}
	for _, candidate := range deploy.Steps {
		if want, allowed := genesisReachable[candidate.Name]; allowed {
			if candidate.If != want {
				t.Fatalf("genesis-reachable step %s condition drifted: got %q want %q", candidate.Name, candidate.If, want)
			}
			continue
		}
		if !strings.Contains(candidate.If, "needs.release-baseline.outputs.is_genesis != 'true'") {
			t.Fatalf("unreviewed deploy step %s is reachable from genesis: %q", candidate.Name, candidate.If)
		}
	}
	if strings.TrimSpace(statefulGuard.If) != nonGenesisCondition {
		t.Fatalf("stateful component guard must run only for ordinary releases: %q", statefulGuard.If)
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
	if strings.TrimSpace(upgrade.If) != nonGenesisCondition {
		t.Fatalf("control-plane upgrade must be unreachable from the genesis evidence path: %q", upgrade.If)
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
	for key, want := range map[string]string{
		"FUGUE_RELEASE_DOMAIN_BASE_SHA":             "${{ needs.release-baseline.outputs.domain_base_sha }}",
		"FUGUE_RELEASE_DOMAIN_TARGET_SHA":           "${{ github.sha }}",
		"FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL":        "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
		"FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL":        "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
		"FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE": "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("upgrade release-domain input %s drifted: got %q want %q", key, got, want)
		}
	}

	publicUpload := workflowStepByName(t, deploy, "Upload release-domain public evidence")
	if got, want := publicUpload.If, "always()"; got != want {
		t.Fatalf("public evidence must always be uploaded: got %q want %q", got, want)
	}
	if publicUpload.ContinueOnError {
		t.Fatal("public evidence upload must fail closed")
	}
	for key, want := range map[string]string{
		"path":                 "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
		"if-no-files-found":    "error",
		"retention-days":       "90",
		"include-hidden-files": "false",
		"overwrite":            "false",
	} {
		if got := publicUpload.With[key]; got != want {
			t.Fatalf("public evidence upload %s drifted: got %q want %q", key, got, want)
		}
	}

	recordBaseline, ok := workflow.Jobs["record-release-baseline"]
	if !ok {
		t.Fatal("control-plane workflow must define record-release-baseline")
	}
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy"} {
		if !containsWorkflowNeed(recordBaseline.Needs, required) {
			t.Fatalf("record-release-baseline must wait for %s", required)
		}
	}
	if got, want := recordBaseline.Permissions, map[string]string{"contents": "write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("record-release-baseline permissions drifted: got %v want %v", got, want)
	}
	const recordBaselineCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' }}"
	if recordBaseline.If != recordBaselineCondition {
		t.Fatalf("record-release-baseline success condition drifted: got %q want %q", recordBaseline.If, recordBaselineCondition)
	}
	advanceBaseline := workflowStepByName(t, recordBaseline, "Advance dedicated release baseline tag")
	if got, want := advanceBaseline.Env["EXPECTED_BASE_REF_OBJECT"], "${{ needs.release-baseline.outputs.baseline_ref_object_sha }}"; got != want {
		t.Fatalf("record-release-baseline ref-object binding drifted: got %q want %q", got, want)
	}
	for _, required := range []string{
		"refs/tags/fugue-control-plane-release-baseline",
		"723116882214ae9efeaee0877bb378d0db2dcea7",
		"8b4bdc2a2b443be6d1244f9b4739cd0be1313d71",
		"4d74c6f963258f9f5c3925613891db9163327330",
		`"${EXPECTED_BASE_SHA}" == "${genesis_base_sha}"`,
		`"${target_parent}" == "${genesis_parent_sha}"`,
		`"${parent_b3}" == "${genesis_b3_sha}"`,
		`"${b3_base}" == "${genesis_base_sha}"`,
		`"${remote_object}" == "${EXPECTED_BASE_REF_OBJECT}"`,
		`"${fetched_ref_object_sha}" == "${EXPECTED_BASE_REF_OBJECT}"`,
		`"${current_base_sha}" == "${EXPECTED_BASE_SHA}"`,
		`"${EXPECTED_BASE_REF_OBJECT}" == "${EXPECTED_BASE_SHA}"`,
		`--force-with-lease="${lease}"`,
		`"${TARGET_SHA}:${baseline_ref}"`,
	} {
		if !strings.Contains(advanceBaseline.Run, required) {
			t.Fatalf("release baseline advancement must contain %q", required)
		}
	}

	freeze, ok := workflow.Jobs["freeze-release-lane-on-failure"]
	if !ok {
		t.Fatal("control-plane workflow must define the automatic release-lane freeze finalizer")
	}
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline"} {
		if !containsWorkflowNeed(freeze.Needs, required) {
			t.Fatalf("release-lane freeze finalizer must wait for %s", required)
		}
	}
	if len(freeze.Needs) != 6 {
		t.Fatalf("release-lane freeze finalizer has unexpected dependencies: %v", freeze.Needs)
	}
	const freezeCondition = "${{ always() && (needs.release-input-guard.result != 'success' || needs.release-baseline.result != 'success' || needs.release-gate.result != 'success' || needs.build.result != 'success' || needs.deploy.result != 'success' || needs.record-release-baseline.result != 'success') }}"
	if freeze.If != freezeCondition {
		t.Fatalf("release-lane freeze condition drifted: got %q want %q", freeze.If, freezeCondition)
	}
	if got, want := freeze.Permissions["actions"], "write"; got != want {
		t.Fatalf("release-lane freeze finalizer needs actions:write: got %q want %q", got, want)
	}
	if got, want := freeze.Permissions["contents"], "read"; got != want {
		t.Fatalf("release-lane freeze finalizer needs contents:read: got %q want %q", got, want)
	}
	if len(freeze.Permissions) != 2 {
		t.Fatalf("release-lane freeze finalizer has unexpected permissions: %v", freeze.Permissions)
	}
	if got, want := workflow.Permissions, map[string]string{"contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("workflow default permissions must be contents:read only: got %v want %v", got, want)
	}
	for jobName, job := range workflow.Jobs {
		if jobName != "freeze-release-lane-on-failure" && job.Permissions["actions"] == "write" {
			t.Fatalf("job %s must not receive actions:write", jobName)
		}
	}

	freezeRecord := workflowStepByName(t, freeze, "Record release lane freeze evidence")
	for key, want := range map[string]string{
		"RELEASE_INPUT_GUARD_RESULT":     "${{ needs.release-input-guard.result }}",
		"RELEASE_BASELINE_RESULT":        "${{ needs.release-baseline.result }}",
		"RELEASE_GATE_RESULT":            "${{ needs.release-gate.result }}",
		"BUILD_RESULT":                   "${{ needs.build.result }}",
		"DEPLOY_RESULT":                  "${{ needs.deploy.result }}",
		"RECORD_RELEASE_BASELINE_RESULT": "${{ needs.record-release-baseline.result }}",
	} {
		if got := freezeRecord.Env[key]; got != want {
			t.Fatalf("release-lane freeze evidence env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{"lane-freeze.json", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "job_results", "os.replace"} {
		if !strings.Contains(freezeRecord.Run, required) {
			t.Fatalf("release-lane freeze evidence must contain %q", required)
		}
	}

	freezeUpload := workflowStepByName(t, freeze, "Upload release lane freeze evidence")
	const uploadArtifactAction = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
	if freezeUpload.ID != "freeze_evidence_upload" || strings.TrimSpace(freezeUpload.If) != "always()" || freezeUpload.Uses != uploadArtifactAction || !freezeUpload.ContinueOnError {
		t.Fatalf("release-lane freeze evidence upload must be pinned and non-blocking: %#v", freezeUpload)
	}
	if got, want := freezeUpload.With["if-no-files-found"], "error"; got != want {
		t.Fatalf("release-lane freeze evidence upload must reject an absent file: got %q want %q", got, want)
	}

	freezeLane := workflowStepByName(t, freeze, "Disable release lane and cancel queued runs")
	if freezeLane.ID != "freeze_lane" || strings.TrimSpace(freezeLane.If) != "always()" {
		t.Fatalf("release-lane disable step must always run after evidence generation: %#v", freezeLane)
	}
	for _, required := range []string{
		"actions/workflows/${workflow_id}/disable",
		"disabled_manually",
		"for status in queued in_progress requested waiting pending",
		"status=${status}",
		"actions/runs/${run_id}/cancel",
		"CURRENT_RUN_ID",
		"pending_other_runs",
	} {
		if !strings.Contains(freezeLane.Run, required) {
			t.Fatalf("release-lane disable step must contain %q", required)
		}
	}

	requireFreezeEvidence := workflowStepByName(t, freeze, "Require release lane freeze evidence")
	if got, want := requireFreezeEvidence.If, "${{ always() && steps.freeze_evidence_upload.outcome != 'success' }}"; got != want {
		t.Fatalf("release-lane evidence failure condition drifted: got %q want %q", got, want)
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
