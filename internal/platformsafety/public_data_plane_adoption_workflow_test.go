package platformsafety

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const publicDataPlaneAdoptionWorkflow = "../../.github/workflows/adopt-public-data-plane-helm-baseline.yml"

const pinnedPublicDataPlaneSetupGo = "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16"

var publicDataPlaneRecoveryDeltaPaths = []string{
	".github/workflows/recover-public-data-plane-helm-adoption.yml",
	"internal/platformsafety/public_data_plane_adoption_workflow_test.go",
	"internal/releasedomain/public_data_plane_adoption.go",
	"internal/releasedomain/public_data_plane_adoption_test.go",
	"scripts/adopt_public_data_plane_helm_baseline.sh",
	"scripts/lib/public_data_plane_adoption_recovery.sh",
	"scripts/recover_public_data_plane_helm_adoption.sh",
	"scripts/test_public_data_plane_helm_adoption.sh",
	"scripts/test_public_data_plane_helm_adoption_recovery.sh",
	"scripts/test_release_domain_workflow.sh",
}

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
		"--dry-run=server", "secret-lookup-witness", "canonicalize-secret-free",
		"--secret-hmac-key-file", "secret-lookup-witness.json", "prewrite-secret-lookup-witness.json",
		"trap cleanup EXIT", "trap 'exit 143' TERM", `rm -f -- "${SECRET_HMAC_KEY_FILE}"`,
		"public_data_plane_adoption_persist_recovery_wal",
		"public_data_plane_adoption_advance_recovery_wal",
		"prewrite-base.yaml", "prewrite-values.yaml", "prewrite-target.yaml",
		"prewrite-repeated-target.yaml", "prewrite-observed.yaml", "prewrite-snapshot.json",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Stage1 script is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"controlPlanePostgres.existingSecretName=", "controlPlanePostgres.password=", "--set-string controlPlanePostgres",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Stage1 script injects a Secret value/ownership override %q", forbidden)
		}
	}
}

func TestPublicDataPlaneAdoptionRecoveryWorkflowIsDefaultOffAndOriginBound(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/recover-public-data-plane-helm-adoption.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	var workflow struct {
		On map[string]yaml.Node `yaml:"on"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	var dispatch releaseWorkflowDispatchTrigger
	dispatchNode := workflow.On["workflow_dispatch"]
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatal(err)
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, []string{
		"confirm_recovery", "expected_source_sha", "expected_wal_digest", "origin_run_id",
	}) {
		t.Fatalf("recovery input inventory drifted: %v", got)
	}
	for _, name := range []string{"expected_source_sha", "expected_wal_digest", "origin_run_id"} {
		var input releaseWorkflowDispatchInput
		inputNode := dispatch.Inputs[name]
		if err := inputNode.Decode(&input); err != nil || !input.Required || input.Type != "string" {
			t.Fatalf("recovery input %s drifted: %+v err=%v", name, input, err)
		}
	}
	var confirm releaseWorkflowDispatchInput
	confirmNode := dispatch.Inputs["confirm_recovery"]
	if err := confirmNode.Decode(&confirm); err != nil || confirm.Required || confirm.Type != "boolean" || confirm.Default != false {
		t.Fatalf("confirm_recovery contract drifted: %+v err=%v", confirm, err)
	}
	assertPublicDataPlaneSetupGoBeforeBuild(t, data, "recover", "Build typed recovery tools")
	for _, required := range []string{
		"confirm_recovery:", "default: false", "if: ${{ inputs.confirm_recovery }}",
		"expected_source_sha:", "expected_wal_digest:", "origin_run_id:",
		"FUGUE_RECOVERY_SHA", "FUGUE_EXPECTED_SOURCE_SHA", "git diff --name-status --no-renames",
		"ref: ${{ github.sha }}", "ACTUAL_REF: ${{ github.ref }}", "repos/${REPOSITORY}/git/ref/heads/main",
		"git merge-base --is-ancestor", "git rev-list --count", "git rev-list --min-parents=2",
		`"${commit_count}" -le 3`,
		"curl", "--config \"${curl_config}\"", "curl.config", "path.chmod(0o600)",
		"object_pairs_hook=unique_object", "duplicate JSON key",
		"--connect-timeout 5", "--max-time 15", "API_URL: ${{ github.api_url }}",
		"fugue-production-cluster-mutation-v1", "cancel-in-progress: false",
		"actions: read", "contents: read", "run_attempt", "terminal-wal.json",
		"./scripts/recover_public_data_plane_helm_adoption.sh",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("recovery workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"cancel-in-progress: true", "actions: write", "contents: write",
		"ref: ${{ inputs.", "FUGUE_EXPECTED_SHA:", "inputs.expected_sha", "gh api", "command -v gh",
		`--header "Authorization: Bearer ${GITHUB_TOKEN}"`,
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("recovery workflow contains forbidden capability %q", forbidden)
		}
	}
	identity := publicDataPlaneRecoveryWorkflowStep(t, "Verify exact recovery identity")
	recover := publicDataPlaneRecoveryWorkflowStep(t, "Recover or finalize the durable Stage1 transaction")
	upload := publicDataPlaneRecoveryWorkflowStep(t, "Publish recovered Stage1 handoff")
	if identity.ID != "identity" || recover.ID != "recover" {
		t.Fatalf("recovery step outcome IDs drifted: identity=%q recover=%q", identity.ID, recover.ID)
	}
	if upload.If != "${{ always() && steps.identity.outcome == 'success' && steps.recover.outcome != 'skipped' }}" {
		t.Fatalf("recovery artifact preflight/run condition drifted: %q", upload.If)
	}
	if upload.With["if-no-files-found"] != "error" || !strings.Contains(upload.With["path"], "terminal-wal.json") {
		t.Fatalf("recovery terminal artifact fail-closed contract drifted: %+v", upload.With)
	}
	recovery, err := os.ReadFile("../../scripts/recover_public_data_plane_helm_adoption.sh")
	if err != nil {
		t.Fatal(err)
	}
	recoveryText := string(recovery)
	if strings.Contains(recoveryText, `"${HELM}" upgrade`) || strings.Contains(recoveryText, "helm rollback") {
		t.Fatal("cross-process recovery can execute a second Helm apply or whole-release reversal")
	}
	if strings.Contains(recoveryText, "FUGUE_EXPECTED_SHA") {
		t.Fatal("recovery script can conflate the Stage1 source with the recovery implementation SHA")
	}
	for _, required := range []string{
		"restore-succeeded-awaiting-helm-compensation", "verify-recovery-base",
		"aborted-before-apply", "verify_aborted_before_apply_state",
		"canonicalize-secret-free",
		"FUGUE_RECOVERY_SHA", "FUGUE_EXPECTED_SOURCE_SHA",
		"FUGUE_EXPECTED_WAL_DIGEST", "FUGUE_EXPECTED_ORIGIN_RUN_ID",
		"originRunId",
		"control_plane_stale_release_old_process_absent",
	} {
		if !strings.Contains(recoveryText, required) {
			t.Fatalf("recovery script is missing %q", required)
		}
	}
}

func publicDataPlaneRecoveryWorkflowStep(t *testing.T, name string) releaseWorkflowStep {
	t.Helper()
	data, err := os.ReadFile("../../.github/workflows/recover-public-data-plane-helm-adoption.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	job, ok := workflow.Jobs["recover"]
	if !ok {
		t.Fatal("recovery workflow job is missing")
	}
	var matches []releaseWorkflowStep
	for _, step := range job.Steps {
		if step.Name == name {
			matches = append(matches, step)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("recovery workflow step %q count=%d", name, len(matches))
	}
	return matches[0]
}

func runPublicDataPlaneRecoveryGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v output=%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func publicDataPlaneRecoveryIdentityRepository(t *testing.T, mode string) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	runPublicDataPlaneRecoveryGit(t, dir, "init", "-q")
	runPublicDataPlaneRecoveryGit(t, dir, "config", "user.name", "Recovery Test")
	runPublicDataPlaneRecoveryGit(t, dir, "config", "user.email", "recovery@example.invalid")
	for _, path := range publicDataPlaneRecoveryDeltaPaths {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("source\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runPublicDataPlaneRecoveryGit(t, dir, "add", "--", ".")
	runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "-m", "source")
	source := runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
	sourceTree := runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD^{tree}")
	for _, path := range publicDataPlaneRecoveryDeltaPaths {
		if err := os.WriteFile(filepath.Join(dir, path), []byte("recovery-one\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runPublicDataPlaneRecoveryGit(t, dir, "add", "--", ".")
	runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "-m", "recovery one")
	first := runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
	recovery := first
	switch mode {
	case "valid", "branch", "source-equals-recovery":
		if err := os.WriteFile(filepath.Join(dir, publicDataPlaneRecoveryDeltaPaths[0]), []byte("recovery-two\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		runPublicDataPlaneRecoveryGit(t, dir, "add", "--", publicDataPlaneRecoveryDeltaPaths[0])
		runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "-m", "recovery two")
		recovery = runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
		if mode == "valid" {
			runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "--allow-empty", "-m", "recovery three")
			recovery = runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
		} else if mode == "branch" {
			source = runPublicDataPlaneRecoveryGit(t, dir, "commit-tree", sourceTree, "-m", "unrelated source")
		} else if mode == "source-equals-recovery" {
			source = recovery
		}
	case "merge":
		firstTree := runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", first+"^{tree}")
		recovery = runPublicDataPlaneRecoveryGit(t, dir, "commit-tree", firstTree, "-p", first, "-p", source, "-m", "forbidden merge")
	case "over-commit":
		runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "--allow-empty", "-m", "recovery two")
		runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "--allow-empty", "-m", "recovery three")
		runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "--allow-empty", "-m", "recovery four")
		recovery = runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
	case "extra-file":
		if err := os.WriteFile(filepath.Join(dir, "unexpected.txt"), []byte("forbidden\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		runPublicDataPlaneRecoveryGit(t, dir, "add", "--", "unexpected.txt")
		runPublicDataPlaneRecoveryGit(t, dir, "commit", "-q", "-m", "forbidden extra file")
		recovery = runPublicDataPlaneRecoveryGit(t, dir, "rev-parse", "HEAD")
	default:
		t.Fatalf("unknown recovery identity fixture mode %q", mode)
	}
	switch mode {
	case "valid":
		if count := runPublicDataPlaneRecoveryGit(t, dir, "rev-list", "--count", source+".."+recovery); count != "3" {
			t.Fatalf("valid recovery fixture count=%s", count)
		}
	case "branch":
		probe := exec.Command("git", "merge-base", "--is-ancestor", source, recovery)
		probe.Dir = dir
		if err := probe.Run(); err == nil {
			t.Fatal("branch fixture source is unexpectedly an ancestor")
		}
	case "merge":
		if merges := runPublicDataPlaneRecoveryGit(t, dir, "rev-list", "--min-parents=2", source+".."+recovery); merges == "" {
			t.Fatal("merge fixture has no merge commit")
		}
	case "over-commit":
		if count := runPublicDataPlaneRecoveryGit(t, dir, "rev-list", "--count", source+".."+recovery); count != "4" {
			t.Fatalf("over-commit fixture count=%s", count)
		}
	case "extra-file":
		if delta := runPublicDataPlaneRecoveryGit(t, dir, "diff", "--name-status", "--no-renames", source, recovery); !strings.Contains(delta, "A\tunexpected.txt") {
			t.Fatalf("extra-file fixture delta=%s", delta)
		}
	case "source-equals-recovery":
		if source != recovery {
			t.Fatal("source-equals-recovery fixture drifted")
		}
	}
	runPublicDataPlaneRecoveryGit(t, dir, "checkout", "-q", "--detach", recovery)
	if status := runPublicDataPlaneRecoveryGit(t, dir, "status", "--porcelain"); status != "" {
		t.Fatalf("recovery identity fixture is dirty: %s", status)
	}
	return dir, source, recovery
}

func runPublicDataPlaneRecoveryIdentity(t *testing.T, mode, apiMode string) ([]byte, error, string) {
	t.Helper()
	step := publicDataPlaneRecoveryWorkflowStep(t, "Verify exact recovery identity")
	dir, source, recovery := publicDataPlaneRecoveryIdentityRepository(t, mode)
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(mockBin, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bash", "git", "python3", "rm", "chmod", "mktemp"} {
		target, err := exec.LookPath(name)
		if err != nil {
			t.Fatalf("locate recovery identity harness command %s: %v", name, err)
		}
		if err := os.Symlink(target, filepath.Join(mockBin, name)); err != nil {
			t.Fatalf("link recovery identity harness command %s: %v", name, err)
		}
	}
	curlArguments := filepath.Join(tempDir, "curl-arguments")
	curl := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' '---' "$@" >>"${CURL_ARGUMENTS}"
config=''; output=''; url=''
while (( $# )); do
  case "$1" in
    --config) config="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
[[ -n "${config}" && -f "${config}" && ! -L "${config}" && -n "${output}" && -n "${url}" ]]
CONFIG="${config}" GITHUB_TOKEN="${GITHUB_TOKEN}" python3 - <<'PY'
import os, pathlib, stat
path=pathlib.Path(os.environ["CONFIG"])
assert stat.S_IMODE(path.stat().st_mode) == 0o600
assert path.read_text() == 'header = "Authorization: Bearer '+os.environ["GITHUB_TOKEN"]+'"\n'
PY
printf '%s\n' 'curl_config_private=true' >>"${CURL_ARGUMENTS}"
case "${url}" in
  */git/ref/heads/main)
    sha="${RECOVERY_SHA}"
    [[ "${MOCK_API_MODE}" != main-mismatch ]] || sha=0000000000000000000000000000000000000000
    if [[ "${MOCK_API_MODE}" == main-duplicate ]]; then
      printf '{"ref":"refs/heads/main","ref":"refs/heads/main","object":{"type":"commit","sha":"%s","sha":"%s"}}\n' "${sha}" "${sha}" >"${output}"
    else
      printf '{"ref":"refs/heads/main","object":{"type":"commit","sha":"%s"}}\n' "${sha}" >"${output}"
    fi
    ;;
  */actions/runs/*)
    [[ "${MOCK_API_MODE}" != origin-timeout ]] || exit 28
    [[ "${MOCK_API_MODE}" != origin-http-failure ]] || exit 22
    sha="${EXPECTED_SOURCE_SHA}"
    [[ "${MOCK_API_MODE}" != origin-mismatch ]] || sha=0000000000000000000000000000000000000000
    document="$(printf '{"id":%s,"run_attempt":1,"event":"workflow_dispatch","head_branch":"main","head_sha":"%s","status":"completed","conclusion":"failure","path":".github/workflows/adopt-public-data-plane-helm-baseline.yml","repository":{"full_name":"example/fugue"}}' "${ORIGIN_RUN_ID}" "${sha}")"
    if [[ "${MOCK_API_MODE}" == origin-duplicate ]]; then
      printf '{"id":%s,"id":%s,"run_attempt":1,"event":"workflow_dispatch","head_branch":"main","head_sha":"%s","status":"completed","conclusion":"failure","path":".github/workflows/adopt-public-data-plane-helm-baseline.yml","repository":{"full_name":"example/fugue","full_name":"example/fugue"}}\n' "${ORIGIN_RUN_ID}" "${ORIGIN_RUN_ID}" "${sha}" >"${output}"
    elif [[ "${MOCK_API_MODE}" == origin-multiple ]]; then
      printf '%s\n{}\n' "${document}" >"${output}"
    else
      printf '%s\n' "${document}" >"${output}"
    fi
    ;;
  *) exit 23 ;;
esac
`
	curlPath := filepath.Join(mockBin, "curl")
	if err := os.WriteFile(curlPath, []byte(curl), 0o700); err != nil {
		t.Fatal(err)
	}
	path := mockBin
	noGH := exec.Command("bash", "-c", "command -v gh")
	noGH.Env = []string{"PATH=" + path}
	if err := noGH.Run(); err == nil {
		t.Fatal("recovery identity harness PATH unexpectedly contains gh")
	}
	command := exec.Command("bash", "-c", step.Run)
	command.Dir = dir
	command.Env = []string{
		"PATH=" + path,
		"HOME=" + tempDir,
		"RUNNER_TEMP=" + tempDir,
		"RECOVERY_SHA=" + recovery,
		"EXPECTED_SOURCE_SHA=" + source,
		"EXPECTED_REF=refs/heads/main",
		"ACTUAL_REF=refs/heads/main",
		"EXPECTED_WAL_DIGEST=sha256:" + strings.Repeat("1", 64),
		"ORIGIN_RUN_ID=123",
		"GITHUB_TOKEN=test-token",
		"API_URL=https://api.github.com",
		"REPOSITORY=example/fugue",
		"MOCK_API_MODE=" + apiMode,
		"CURL_ARGUMENTS=" + curlArguments,
		"FUGUE_PUBLIC_DATA_PLANE_ADOPTION_COORDINATION_LIBRARY=",
		"FUGUE_PUBLIC_DATA_PLANE_ADOPTION_RECOVERY_LIBRARY=",
	}
	output, err := command.CombinedOutput()
	arguments, readErr := os.ReadFile(curlArguments)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	return output, err, string(arguments)
}

func TestPublicDataPlaneAdoptionRecoveryIdentityHarness(t *testing.T) {
	t.Run("self-hosted PATH without gh and exact API evidence", func(t *testing.T) {
		output, err, arguments := runPublicDataPlaneRecoveryIdentity(t, "valid", "success")
		if err != nil {
			t.Fatalf("exact recovery identity failed without gh: %v output=%s", err, output)
		}
		for _, required := range []string{
			"--config", "curl_config_private=true", "--fail", "--silent", "--show-error", "--proto", "=https", "--tlsv1.2",
			"--connect-timeout", "5", "--max-time", "15", "--retry", "0",
			"Accept: application/vnd.github+json",
			"X-GitHub-Api-Version: 2022-11-28", "https://api.github.com/repos/example/fugue/git/ref/heads/main",
			"https://api.github.com/repos/example/fugue/actions/runs/123",
		} {
			if !strings.Contains(arguments, required) {
				t.Fatalf("strict curl arguments are missing %q: %s", required, arguments)
			}
		}
		if strings.Contains(arguments, "--location") {
			t.Fatal("recovery identity API client permits redirects")
		}
		for _, leaked := range []string{"test-token", "Authorization: Bearer"} {
			if strings.Contains(arguments, leaked) {
				t.Fatalf("recovery identity curl argv leaked %q: %s", leaked, arguments)
			}
		}
	})
	for _, test := range []struct {
		name    string
		apiMode string
	}{
		{name: "origin API identity mismatch", apiMode: "origin-mismatch"},
		{name: "main API identity mismatch", apiMode: "main-mismatch"},
		{name: "origin API HTTP failure", apiMode: "origin-http-failure"},
		{name: "origin API timeout", apiMode: "origin-timeout"},
		{name: "duplicate main ref or object keys", apiMode: "main-duplicate"},
		{name: "duplicate origin run or repository keys", apiMode: "origin-duplicate"},
		{name: "multiple origin JSON documents", apiMode: "origin-multiple"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if output, err, _ := runPublicDataPlaneRecoveryIdentity(t, "valid", test.apiMode); err == nil {
				t.Fatalf("recovery identity accepted %s: %s", test.apiMode, output)
			}
		})
	}
	for _, mode := range []string{"branch", "merge", "over-commit", "extra-file", "source-equals-recovery"} {
		t.Run("reject lineage "+mode, func(t *testing.T) {
			if output, err, _ := runPublicDataPlaneRecoveryIdentity(t, mode, "success"); err == nil {
				t.Fatalf("recovery identity accepted %s lineage: %s", mode, output)
			}
		})
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
