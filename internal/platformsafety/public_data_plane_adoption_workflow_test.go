package platformsafety

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const publicDataPlaneAdoptionWorkflow = "../../.github/workflows/adopt-public-data-plane-helm-baseline.yml"

const pinnedPublicDataPlaneSetupGo = "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16"

func assertPublicDataPlaneSetupGoBeforeBuild(t *testing.T, data []byte, jobName, buildStepName string) {
	t.Helper()
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	job, ok := workflow.Jobs[jobName]
	if !ok {
		t.Fatalf("workflow is missing %q job", jobName)
	}
	setupIndex, buildIndex := -1, -1
	for index, step := range job.Steps {
		switch step.Name {
		case "Setup Go":
			if setupIndex >= 0 {
				t.Fatal("workflow contains more than one Setup Go step")
			}
			setupIndex = index
			if step.Uses != pinnedPublicDataPlaneSetupGo ||
				!reflect.DeepEqual(step.With, map[string]string{"go-version-file": "go.mod", "cache": "false"}) ||
				step.If != "" || step.Run != "" || len(step.Env) != 0 {
				t.Fatalf("Setup Go contract drifted: %+v", step)
			}
		case buildStepName:
			if buildIndex >= 0 {
				t.Fatalf("workflow contains more than one %q step", buildStepName)
			}
			buildIndex = index
		}
	}
	if setupIndex < 0 || buildIndex < 0 || setupIndex >= buildIndex {
		t.Fatalf("Setup Go must precede %q: setup=%d build=%d", buildStepName, setupIndex, buildIndex)
	}
}

func TestPublicDataPlaneAdoptionWorkflowIsDedicatedAndSerialized(t *testing.T) {
	data, err := os.ReadFile(publicDataPlaneAdoptionWorkflow)
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			RunsOn         []string              `yaml:"runs-on"`
			Environment    string                `yaml:"environment"`
			TimeoutMinutes int                   `yaml:"timeout-minutes"`
			Steps          []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if len(workflow.On) != 1 || workflow.On["workflow_dispatch"].Kind == 0 {
		t.Fatalf("adoption trigger must be workflow_dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	dispatchNode := workflow.On["workflow_dispatch"]
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatal(err)
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, []string{"dry_run", "expected_sha"}) {
		t.Fatalf("adoption input inventory drifted: %v", got)
	}
	var expected releaseWorkflowDispatchInput
	expectedNode := dispatch.Inputs["expected_sha"]
	if err := expectedNode.Decode(&expected); err != nil || !expected.Required || expected.Type != "string" {
		t.Fatalf("expected_sha contract drifted: %+v err=%v", expected, err)
	}
	var dryRun releaseWorkflowDispatchInput
	dryRunNode := dispatch.Inputs["dry_run"]
	if err := dryRunNode.Decode(&dryRun); err != nil || dryRun.Required || dryRun.Type != "boolean" || dryRun.Default != true {
		t.Fatalf("dry_run contract drifted: %+v err=%v", dryRun, err)
	}
	if !reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		workflow.Concurrency.Group != "fugue-production-cluster-mutation-v1" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("adoption permissions/concurrency drifted: %+v %+v", workflow.Permissions, workflow.Concurrency)
	}
	if len(workflow.Jobs) != 1 {
		t.Fatalf("adoption workflow must have one typed transaction job: %+v", workflow.Jobs)
	}
	job := workflow.Jobs["adopt"]
	wantRunner := []string{"self-hosted", "linux", "x64", "fugue", "control-plane"}
	if !reflect.DeepEqual(job.RunsOn, wantRunner) || job.Environment != "production" || job.TimeoutMinutes != 45 {
		t.Fatalf("adoption runner boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact candidate",
		"Verify immutable workflow identity",
		"Setup Go",
		"Build typed adoption tools",
		"Execute dedicated authoritative DNS Stage1",
		"Publish immutable Stage1 handoff",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("adoption step count drifted: %+v", job.Steps)
	}
	for index, step := range job.Steps {
		if step.Name != wantSteps[index] {
			t.Fatalf("adoption step %d = %q, want %q", index, step.Name, wantSteps[index])
		}
	}
	assertPublicDataPlaneSetupGoBeforeBuild(t, data, "adopt", "Build typed adoption tools")
	text := string(data)
	for _, required := range []string{
		"fetch-depth: 0", "FUGUE_EXPECTED_SHA", "FUGUE_PUBLIC_DATA_PLANE_ADOPTION_DRY_RUN",
		"./scripts/adopt_public_data_plane_helm_baseline.sh", "stage1-baseline.json",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("adoption workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{"pull-requests: write", "packages: write", "cancel-in-progress: true"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("adoption workflow contains forbidden capability %q", forbidden)
		}
	}
}

func TestPublicDataPlaneAdoptionScriptHasOneHelmBoundaryAndNoWholeReleaseReversal(t *testing.T) {
	data, err := os.ReadFile("../../scripts/adopt_public_data_plane_helm_baseline.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Count(text, `"${HELM}" upgrade`) != 1 {
		t.Fatalf("Stage1 must have one Helm apply boundary")
	}
	for _, forbidden := range []string{"helm rollback", `"${HELM}" rollback`, "--atomic"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Stage1 contains forbidden whole-release behavior %q", forbidden)
		}
	}
	for _, required := range []string{
		"acquire_control_plane_backup_coordination_lease",
		"arm_control_plane_release_recovery_fence",
		"verify-prewrite", "transaction-post-render", "restore-patches", "verify-restore", "finalize",
		"public_data_plane_adoption_persist_recovery_wal",
		"public_data_plane_adoption_advance_recovery_wal",
		"prewrite-base.yaml", "prewrite-values.yaml", "prewrite-target.yaml",
		"prewrite-repeated-target.yaml", "prewrite-observed.yaml", "prewrite-snapshot.json",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Stage1 script is missing %q", required)
		}
	}
}

func TestPublicDataPlaneAdoptionRecoveryWorkflowIsDefaultOffAndOriginBound(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/recover-public-data-plane-helm-adoption.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	assertPublicDataPlaneSetupGoBeforeBuild(t, data, "recover", "Build typed recovery tools")
	for _, required := range []string{
		"confirm_recovery:", "default: false", "if: ${{ inputs.confirm_recovery }}",
		"expected_sha:", "expected_wal_digest:", "origin_run_id:",
		"fugue-production-cluster-mutation-v1", "cancel-in-progress: false",
		"actions: read", "contents: read", "run_attempt",
		"./scripts/recover_public_data_plane_helm_adoption.sh",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("recovery workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{"cancel-in-progress: true", "actions: write", "contents: write"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("recovery workflow contains forbidden capability %q", forbidden)
		}
	}
	recovery, err := os.ReadFile("../../scripts/recover_public_data_plane_helm_adoption.sh")
	if err != nil {
		t.Fatal(err)
	}
	recoveryText := string(recovery)
	if strings.Contains(recoveryText, `"${HELM}" upgrade`) || strings.Contains(recoveryText, "helm rollback") {
		t.Fatal("cross-process recovery can execute a second Helm apply or whole-release reversal")
	}
	for _, required := range []string{
		"restore-succeeded-awaiting-helm-compensation", "verify-recovery-base",
		"FUGUE_EXPECTED_WAL_DIGEST", "FUGUE_EXPECTED_ORIGIN_RUN_ID",
		"originRunId",
		"control_plane_stale_release_old_process_absent",
	} {
		if !strings.Contains(recoveryText, required) {
			t.Fatalf("recovery script is missing %q", required)
		}
	}
}

func TestDeployWorkflowConsumesStage1HandoffAtPlannerAndPrewrite(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/deploy-control-plane.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, required := range []string{
		"public_data_plane_adoption_run_id:",
		"public_data_plane_adoption_baseline_digest:",
		"Download exact public data-plane Stage1 handoff",
		"Download Stage1 handoff for planner base",
		"Verify Stage1 handoff before release planning",
		"Download Stage1 handoff for deploy prewrite",
		"Reverify Stage1 handoff at deploy prewrite",
		"public-data-plane-adoption-${{ inputs.public_data_plane_adoption_run_id }}-1",
		".github/workflows/adopt-public-data-plane-helm-baseline.yml",
		"stage1-baseline.json", "execution-trace.json", "lease-released",
		"FUGUE_PUBLIC_DATA_PLANE_STAGE1_BASELINE_FILE",
		"FUGUE_PUBLIC_DATA_PLANE_STAGE1_TRACE_FILE",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("deploy Stage2 consumer is missing %q", required)
		}
	}
	production, err := os.ReadFile("../../scripts/lib/control_plane_release_domain_production.sh")
	if err != nil {
		t.Fatal(err)
	}
	productionText := string(production)
	acquire := strings.Index(productionText, "acquire_control_plane_backup_coordination_lease || return")
	prewrite := strings.Index(productionText, "control_plane_release_domain_verify_public_data_plane_stage1_handoff_prewrite || return")
	fence := strings.Index(productionText, "arm_control_plane_release_recovery_fence \"single-domain-${CONTROL_PLANE_RELEASE_SELECTED_DOMAIN}\" || return")
	if acquire < 0 || prewrite <= acquire || fence <= prewrite {
		t.Fatal("Stage2 handoff is not reverified under the owned Lease before the release fence/write boundary")
	}
	if strings.Count(text, `"${tool}" verify-stage2`) != 2 {
		t.Fatalf("Stage2 handoff must be verified before planning and again at deploy prewrite")
	}
	if !strings.Contains(text, `[[ -z "${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}" && -z "${PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST}" ]]`) ||
		!strings.Contains(text, `[[ "${stage1_status}" == completed && "${stage1_conclusion}" == success ]]`) {
		t.Fatal("Stage2 input pairing or completed Stage1 run guard is missing")
	}
	for _, forbidden := range []string{
		"rm -f ${RUNNER_TEMP}/public-data-plane-stage1",
		"delete-artifact",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Stage2 rollback would destroy one-shot handoff state via %q", forbidden)
		}
	}
}
