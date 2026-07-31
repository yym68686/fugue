package platformsafety

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const rp5ReleaseLanePromotionWorkflow = "../../.github/workflows/promote-control-plane-release-lane-rp5.yml"

func TestRP5ReleaseLanePromotionIsOneShotReadOnlyQualificationAndEnable(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp5ReleaseLanePromotionWorkflow)
	if err != nil {
		t.Fatalf("read RP5 lane promotion workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "9bb7165be4e3dc8cd5353cea22c4a2a8354003445b9a38a2fcd3c2ed2507f899")
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress string `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			Needs           workflowNeeds         `yaml:"needs"`
			RunsOn          yaml.Node             `yaml:"runs-on"`
			TimeoutMinutes  int                   `yaml:"timeout-minutes"`
			Environment     string                `yaml:"environment"`
			Permissions     map[string]string     `yaml:"permissions"`
			Outputs         map[string]string     `yaml:"outputs"`
			ContinueOnError bool                  `yaml:"continue-on-error"`
			Steps           []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP5 lane promotion workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "qualify-control-plane-lane", "promote-deploy-lane")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, jobsNode, "qualify-control-plane-lane"),
		"runs-on", "timeout-minutes", "environment", "permissions", "outputs", "steps")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, jobsNode, "promote-deploy-lane"),
		"needs", "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	if len(workflow.On) != 1 {
		t.Fatalf("lane promotion trigger inventory drifted: %+v", workflow.On)
	}
	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok {
		t.Fatal("lane promotion must be workflow_dispatch-only")
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode lane promotion dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_api_health_url",
		"expected_baseline_oid",
		"expected_coredns_status",
		"expected_runner_name",
		"expected_runtime_sha",
		"expected_sha",
		"expected_terminal_oid",
		"supersede_stuck_run_id",
		"supersede_stuck_run_sha",
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("lane promotion inputs = %v, want %v", got, wantInputs)
	}
	for _, name := range wantInputs[:len(wantInputs)-2] {
		var input releaseWorkflowDispatchInput
		node := dispatch.Inputs[name]
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode %s input: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("%s must be a required string without default: %+v", name, input)
		}
	}
	for _, name := range []string{"supersede_stuck_run_id", "supersede_stuck_run_sha"} {
		var supersedeInput releaseWorkflowDispatchInput
		supersedeNode := dispatch.Inputs[name]
		if err := supersedeNode.Decode(&supersedeInput); err != nil {
			t.Fatalf("decode %s input: %v", name, err)
		}
		if supersedeInput.Required || supersedeInput.Type != "string" || supersedeInput.Default != "" {
			t.Fatalf("%s must be an optional empty-default string: %+v", name, supersedeInput)
		}
	}
	if workflow.Concurrency.Group != "fugue-control-plane-release-lane-promotion-rp5" ||
		workflow.Concurrency.CancelInProgress != "false" {
		t.Fatalf("lane promotion concurrency recovery boundary drifted: %+v", workflow.Concurrency)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 2 {
		t.Fatalf("lane promotion top-level boundary drifted: %+v", workflow)
	}

	qualify := workflow.Jobs["qualify-control-plane-lane"]
	var qualifyLabels []string
	if err := qualify.RunsOn.Decode(&qualifyLabels); err != nil {
		t.Fatalf("decode qualification runner labels: %v", err)
	}
	wantLabels := []string{"self-hosted", "linux", "x64", "fugue", "control-plane"}
	if !reflect.DeepEqual(qualifyLabels, wantLabels) ||
		qualify.TimeoutMinutes != 15 || qualify.Environment != "production" ||
		qualify.ContinueOnError ||
		!reflect.DeepEqual(qualify.Permissions, map[string]string{"actions": "read", "contents": "read"}) {
		t.Fatalf("lane qualification job boundary drifted: labels=%v job=%+v", qualifyLabels, qualify)
	}
	if !reflect.DeepEqual(qualify.Outputs, map[string]string{
		"artifact_name":   "${{ steps.evidence.outputs.artifact_name }}",
		"artifact_digest": "${{ steps.normalize.outputs.artifact_digest }}",
	}) {
		t.Fatalf("lane qualification outputs drifted: %+v", qualify.Outputs)
	}
	wantQualifySteps := []string{
		"Checkout exact lane promotion policy SHA",
		"Verify exact read-only lane qualification authorization",
		"Qualify runner and control plane without mutation",
		"Upload read-only lane qualification evidence",
		"Normalize qualification artifact digest",
	}
	assertRP5PromotionStepInventory(t, qualify.Steps, wantQualifySteps)

	promote := workflow.Jobs["promote-deploy-lane"]
	var promoteRunner string
	if err := promote.RunsOn.Decode(&promoteRunner); err != nil {
		t.Fatalf("decode promotion runner: %v", err)
	}
	if promoteRunner != "ubuntu-latest" || promote.TimeoutMinutes != 10 ||
		promote.Environment != "production" || promote.ContinueOnError ||
		!reflect.DeepEqual([]string(promote.Needs), []string{"qualify-control-plane-lane"}) ||
		!reflect.DeepEqual(promote.Permissions, map[string]string{"actions": "write", "contents": "read"}) {
		t.Fatalf("lane promotion job boundary drifted: runner=%q job=%+v", promoteRunner, promote)
	}
	wantPromoteSteps := []string{
		"Checkout exact lane promotion policy SHA",
		"Verify one-shot promotion policy identity",
		"Download exact read-only qualification evidence",
		"Consume exact read-only qualification evidence",
		"Enable deploy workflow with exact readback",
	}
	assertRP5PromotionStepInventory(t, promote.Steps, wantPromoteSteps)

	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if step.Run == "" {
				continue
			}
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("lane promotion step %q is invalid bash: %v output=%q", step.Name, err, output)
			}
		}
	}

	authorize := qualify.Steps[1]
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`"${policy_commit}" =~ ^[0-9a-f]{40}$`,
		`git merge-base --is-ancestor "${policy_commit}" "${GITHUB_SHA}"`,
		`M\t.github/workflows/promote-control-plane-release-lane-rp5.yml`,
		`M\tinternal/platformsafety/release_lane_promotion_workflow_test.go`,
		`github_api_get()`,
		`curl \`,
		`--header "Authorization: Bearer ${GITHUB_TOKEN}"`,
		`--header 'X-GitHub-Api-Version: 2022-11-28'`,
		`validate_supersede_source_policy()`,
		`"${SUPERSEDE_STUCK_RUN_SHA}" != "${GITHUB_SHA}"`,
		`git merge-base --is-ancestor "${SUPERSEDE_STUCK_RUN_SHA}" "${GITHUB_SHA}"`,
		`readonly trusted_supersede_policy_anchor_sha='3fa198cbf9733df54d14de65e44668e3f5f51679'`,
		`readonly trusted_supersede_policy_baseline_oid='321428db29d486539e6a4ee26e4405c1d209076d'`,
		`git merge-base --is-ancestor "${trusted_supersede_policy_anchor_sha}" "${GITHUB_SHA}"`,
		`supersede_delta="$(git diff --no-renames --name-status`,
		`"${SUPERSEDE_STUCK_RUN_SHA}" "${trusted_supersede_policy_anchor_sha}")`,
		`"${supersede_delta}" == $'M\t.github/workflows/promote-control-plane-release-lane-rp5.yml\nM\tinternal/platformsafety/release_lane_promotion_workflow_test.go'`,
		`"${SUPERSEDE_STUCK_RUN_SHA}:${workflow_path}"`,
		`"${SUPERSEDE_STUCK_RUN_SHA}:${test_path}"`,
		`git cat-file blob "${historical_workflow_blob}" | sha256sum`,
		`2a59067621f8d933b7c5a12638acb4f87103af556d3ee73561244dfb9abad7ea`,
		`4f8afcfce987f53224d0ac9ff08a54d5b2c3248457cd39922f56d082c1ca2dc0`,
		`validate_trusted_supersede_baseline()`,
		`"${trusted_supersede_policy_baseline_oid}" "${EXPECTED_BASELINE_OID}"`,
		`"${trusted_supersede_policy_anchor_sha}" "${EXPECTED_RUNTIME_SHA}"`,
		`trusted supersede baseline identity drifted`,
		`validate_trusted_supersede_baseline || exit 1`,
		`validate_superseded_run()`,
		`"${GITHUB_ACTOR}" == "${GITHUB_REPOSITORY_OWNER}"`,
		`"${SUPERSEDE_STUCK_RUN_ID}" != "${GITHUB_RUN_ID}"`,
		`/jobs?filter=all&per_page=100`,
		`superseded run has historical jobs`,
		`("queued", None)`,
		`"${RUNNER_NAME}" == "${EXPECTED_RUNNER_NAME}"`,
		`"${RUNNER_OS}" == 'Linux'`,
		`"${RUNNER_ARCH}" == 'X64'`,
		`actions/workflows/deploy-control-plane.yml`,
		`"${deploy_state}" == 'disabled_manually'`,
		`refs/heads/fugue-control-plane-release-baseline`,
		`git/ref/heads/fugue-control-plane-release-terminal-state`,
		`refs/heads/fugue-control-plane-release-terminal-state`,
		`.fugue-release-terminal-state.json`,
		`value["terminal_mode"] != "frozen"`,
		`"${terminal_previous}" == "${terminal_parent}"`,
		`fugue-runtime-baseline.json`,
		`"${represented_previous}" == "${baseline_parent}"`,
		`git merge-base --is-ancestor "${EXPECTED_RUNTIME_SHA}" "${GITHUB_SHA}"`,
		`for run_status in queued in_progress waiting pending requested`,
		`if identifier != current and str(identifier) != superseded:`,
	} {
		if !strings.Contains(authorize.Run, required) {
			t.Fatalf("lane qualification authorization must contain %q", required)
		}
	}
	if strings.Contains(authorize.Run, "gh api") {
		t.Fatal("self-hosted lane qualification must not depend on the absent gh CLI")
	}
	promoteIdentity := promote.Steps[1]
	for _, required := range []string{
		`"${policy_commit}" =~ ^[0-9a-f]{40}$`,
		`git merge-base --is-ancestor "${policy_commit}" "${GITHUB_SHA}"`,
		`"$(git log --format='%H' -n 1 -- "${policy_path}")" == "${policy_commit}"`,
		`verify_superseded_run_is_quarantined()`,
		`superseded run left zero-job quarantine`,
		`git cat-file blob "${historical_workflow_blob}" | sha256sum`,
		`readonly trusted_supersede_policy_anchor_sha='3fa198cbf9733df54d14de65e44668e3f5f51679'`,
		`git merge-base --is-ancestor "${trusted_supersede_policy_anchor_sha}" "${GITHUB_SHA}"`,
		`supersede_delta="$(git diff --no-renames --name-status`,
		`"${SUPERSEDE_STUCK_RUN_SHA}" "${trusted_supersede_policy_anchor_sha}")`,
		`"${supersede_delta}" == $'M\t.github/workflows/promote-control-plane-release-lane-rp5.yml\nM\tinternal/platformsafety/release_lane_promotion_workflow_test.go'`,
	} {
		if !strings.Contains(promoteIdentity.Run, required) {
			t.Fatalf("hosted lane promotion policy identity must contain %q", required)
		}
	}

	qualifyRuntime := qualify.Steps[2]
	for _, required := range []string{
		`"${EXPECTED_API_HEALTH_URL}" == 'https://api.fugue.pro/healthz'`,
		`KUBECTL=(kubectl)`,
		`KUBECTL=(k3s kubectl)`,
		`get --raw='/readyz?verbose'`,
		`-n kube-system get deployment coredns`,
		`curl --fail --silent --show-error --max-time 10`,
		`helm status "${FUGUE_RELEASE_NAME}"`,
		`"api_health_url": os.environ["EXPECTED_API_HEALTH_URL"]`,
		`"terminal_mode": "frozen"`,
		`"supersede_stuck_run_id": supersede_id`,
		`"supersede_stuck_run_sha": supersede_sha`,
		`"trusted_supersede_policy_anchor_sha": "3fa198cbf9733df54d14de65e44668e3f5f51679" if recovery else ""`,
		`"trusted_supersede_policy_baseline_oid": "321428db29d486539e6a4ee26e4405c1d209076d" if recovery else ""`,
		`"workflow_mutation_attempted": False`,
		`"deploy_dispatch_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"runtime_mutation_attempted": False`,
		`"production_write": False`,
	} {
		if !strings.Contains(qualifyRuntime.Run, required) {
			t.Fatalf("lane read-only qualification must contain %q", required)
		}
	}
	upload := qualify.Steps[3]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("lane qualification upload drifted: %+v", upload)
	}
	download := promote.Steps[2]
	if download.Uses != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" {
		t.Fatalf("lane qualification download drifted: %+v", download)
	}
	consume := promote.Steps[3]
	for _, required := range []string{
		`"api_health_url": os.environ["EXPECTED_API_HEALTH_URL"]`,
		`"terminal_mode": "frozen"`,
		`"repository": os.environ["GITHUB_REPOSITORY"]`,
		`"run_id": os.environ["GITHUB_RUN_ID"]`,
		`"run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"])`,
		`"supersede_stuck_run_id": supersede_id`,
		`"supersede_stuck_run_sha": supersede_sha`,
		`"trusted_supersede_policy_anchor_sha": "3fa198cbf9733df54d14de65e44668e3f5f51679" if recovery else ""`,
		`"trusted_supersede_policy_baseline_oid": "321428db29d486539e6a4ee26e4405c1d209076d" if recovery else ""`,
	} {
		if !strings.Contains(consume.Run, required) {
			t.Fatalf("lane qualification consumer must contain %q", required)
		}
	}

	enable := promote.Steps[4]
	for _, required := range []string{
		`readonly workflow_id='deploy-control-plane.yml'`,
		`git/ref/heads/main`,
		`git/ref/heads/fugue-control-plane-release-baseline`,
		`git/ref/heads/fugue-control-plane-release-terminal-state`,
		`"${main_head}" == "${EXPECTED_SHA}"`,
		`"${baseline_oid}" == "${EXPECTED_BASELINE_OID}"`,
		`"${terminal_oid}" == "${EXPECTED_TERMINAL_OID}"`,
		`superseded run left zero-job quarantine before enable`,
		`readonly trusted_supersede_policy_anchor_sha='3fa198cbf9733df54d14de65e44668e3f5f51679'`,
		`git merge-base --is-ancestor "${trusted_supersede_policy_anchor_sha}" "${GITHUB_SHA}"`,
		`supersede_delta="$(git diff --no-renames --name-status`,
		`"${SUPERSEDE_STUCK_RUN_SHA}" "${trusted_supersede_policy_anchor_sha}")`,
		`"${supersede_delta}" == $'M\t.github/workflows/promote-control-plane-release-lane-rp5.yml\nM\tinternal/platformsafety/release_lane_promotion_workflow_test.go'`,
		`for run_status in queued in_progress waiting pending requested`,
		`str(identifier) not in {current, superseded}`,
		`"${state_before}" == 'disabled_manually'`,
		`--method PUT`,
		`actions/workflows/${workflow_id}/enable`,
		`mutation_status=$?`,
		`for attempt in 1 2 3 4 5`,
		`"${state_after}" == 'active'`,
		`"${settled}" == 'true'`,
	} {
		if !strings.Contains(enable.Run, required) {
			t.Fatalf("lane promotion enable step must contain %q", required)
		}
	}

	source := string(data)
	if strings.Count(source, "--method PUT") != 1 ||
		strings.Count(source, "actions/workflows/${workflow_id}/enable") != 1 ||
		strings.Count(source, "actions/upload-artifact@") != 1 ||
		strings.Count(source, `supersede_delta="$(git diff --no-renames --name-status`) != 3 ||
		strings.Count(source, `"${SUPERSEDE_STUCK_RUN_SHA}" "${trusted_supersede_policy_anchor_sha}")`) != 3 ||
		strings.Count(source, "3fa198cbf9733df54d14de65e44668e3f5f51679") != 5 ||
		strings.Count(source, "321428db29d486539e6a4ee26e4405c1d209076d") != 3 {
		t.Fatal("lane promotion capability inventory drifted")
	}
	for _, forbidden := range []string{
		"${{ secrets.", "actions/workflows/deploy-control-plane.yml/dispatches",
		"/dispatches", "/disable", "/cancel", "git push", "git update-ref", "git commit-tree",
		"updateRefs", "createRef", "deleteRef", "--method POST", "--method PATCH", "--method DELETE",
		"helm upgrade", "helm rollback", "kubectl apply", "kubectl patch", "kubectl delete",
		"k3s kubectl apply", "k3s kubectl patch", "k3s kubectl delete",
		"./scripts/upgrade_fugue_control_plane.sh", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("lane promotion contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP5ReleaseLanePromotionPolicyIdentityUsesRealForwardMainHistory(t *testing.T) {
	t.Parallel()

	steps := []struct {
		name string
		step releaseWorkflowStep
	}{
		{name: "self-hosted qualification", step: rp5PromotionWorkflowStep(t, "qualify-control-plane-lane", "Verify exact read-only lane qualification authorization")},
		{name: "hosted promotion", step: rp5PromotionWorkflowStep(t, "promote-deploy-lane", "Verify one-shot promotion policy identity")},
	}
	for _, step := range steps {
		step := step
		t.Run(step.name, func(t *testing.T) {
			t.Parallel()
			identity := rp5PromotionPolicyIdentitySnippet(t, step.step.Run)
			for _, test := range []struct {
				name     string
				scenario string
				wantPass bool
			}{
				{name: "unchanged policy on forward main", scenario: "forward", wantPass: true},
				{name: "one policy file changed later", scenario: "drift", wantPass: false},
				{name: "policy files split across commits", scenario: "split", wantPass: false},
				{name: "policy commit is not ancestor of execution SHA", scenario: "non-ancestor", wantPass: false},
			} {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()
					repository, executionSHA := prepareRP5PolicyIdentityRepository(t, test.scenario)
					command := exec.Command("bash", "-c", "set -euo pipefail\n"+
						"readonly workflow_path='.github/workflows/promote-control-plane-release-lane-rp5.yml'\n"+
						"readonly test_path='internal/platformsafety/release_lane_promotion_workflow_test.go'\n"+identity)
					command.Dir = repository
					command.Env = append(os.Environ(), "GITHUB_SHA="+executionSHA)
					output, err := command.CombinedOutput()
					if test.wantPass && err != nil {
						t.Fatalf("forward-main identity rejected valid history: %v output=%s", err, output)
					}
					if !test.wantPass && err == nil {
						t.Fatalf("forward-main identity accepted %s: output=%s", test.scenario, output)
					}
				})
			}
		})
	}
}

func TestRP5ReleaseLanePromotionFrozenTerminalWorkflowEnum(t *testing.T) {
	t.Parallel()

	authorize := rp5PromotionWorkflowStep(t, "qualify-control-plane-lane", "Verify exact read-only lane qualification authorization")
	const startMarker = `terminal_fields="$(git cat-file blob "${terminal_blob}" | python3 -c '` + "\n"
	const endMarker = "\n')\" || exit 1"
	start := strings.Index(authorize.Run, startMarker)
	if start < 0 {
		t.Fatal("frozen terminal parser start marker is absent")
	}
	start += len(startMarker)
	endOffset := strings.Index(authorize.Run[start:], endMarker)
	if endOffset < 0 {
		t.Fatal("frozen terminal parser end marker is absent")
	}
	parser := authorize.Run[start : start+endOffset]

	const document = `{"schema_version":1,"certificate_kind":"fugue-control-plane-release-policy-terminal-finalization","terminal_mode":"frozen","source_run_id":"101","source_run_attempt":1,"source_head_sha":"1111111111111111111111111111111111111111","source_workflow":"SOURCE_WORKFLOW","source_conclusion":"success","previous_terminal_state_oid":"3333333333333333333333333333333333333333","reservation_oid":"3333333333333333333333333333333333333333","freeze_reason":"reservation_stale"}` + "\n"
	for _, test := range []struct {
		name     string
		workflow string
		wantPass bool
	}{
		{name: "allowed terminal writer", workflow: ".github/workflows/write-control-plane-release-terminal-rp1.yml", wantPass: true},
		{name: "reject attacker controlled workflow", workflow: ".github/workflows/attacker-controlled.yml", wantPass: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command("python3", "-c", parser)
			command.Stdin = strings.NewReader(strings.Replace(document, "SOURCE_WORKFLOW", test.workflow, 1))
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("frozen terminal parser rejected allowed workflow: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("frozen terminal parser accepted unsupported workflow: output=%s", output)
			}
		})
	}
}

func TestRP5ReleaseLanePromotionSelfHostedGitHubClientUsesCurl(t *testing.T) {
	t.Parallel()

	authorize := rp5PromotionWorkflowStep(t, "qualify-control-plane-lane", "Verify exact read-only lane qualification authorization")
	const startMarker = "github_api_get() {\n"
	const endMarker = "\n}\nvalidate_supersede_source_policy()"
	start := strings.Index(authorize.Run, startMarker)
	if start < 0 {
		t.Fatal("self-hosted GitHub API client start marker is absent")
	}
	endOffset := strings.Index(authorize.Run[start:], endMarker)
	if endOffset < 0 {
		t.Fatal("self-hosted GitHub API client end marker is absent")
	}
	client := authorize.Run[start : start+endOffset+2]

	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(mockBin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	argumentsPath := filepath.Join(tempDir, "curl-arguments")
	writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
	writeRP5PromotionExecutable(t, filepath.Join(mockBin, "curl"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >"${CURL_ARGUMENTS}"
printf '%s\n' '{"object":{"sha":"1111111111111111111111111111111111111111"}}'
`)
	command := exec.Command("bash", "-c", client+"\ngithub_api_get repos/example/ref")
	command.Env = append(os.Environ(),
		"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CURL_ARGUMENTS="+argumentsPath,
		"GITHUB_TOKEN=test-token",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("self-hosted curl GitHub client failed without gh: %v output=%s", err, output)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatalf("read curl arguments: %v", err)
	}
	for _, required := range []string{
		"--fail", "--silent", "--show-error", "--location",
		"Accept: application/vnd.github+json",
		"Authorization: Bearer test-token",
		"X-GitHub-Api-Version: 2022-11-28",
		"https://api.github.com/repos/example/ref",
	} {
		if !strings.Contains(string(arguments), required) {
			t.Fatalf("self-hosted curl arguments must contain %q: %s", required, arguments)
		}
	}
}

func TestRP5ReleaseLanePromotionSupersedeValidationHarness(t *testing.T) {
	t.Parallel()

	authorize := rp5PromotionWorkflowStep(t, "qualify-control-plane-lane", "Verify exact read-only lane qualification authorization")
	const startMarker = "validate_supersede_source_policy() {\n"
	const endMarker = "\nvalidate_supersede_source_policy || exit 1\nvalidate_superseded_run || exit 1"
	start := strings.Index(authorize.Run, startMarker)
	if start < 0 {
		t.Fatal("superseded source validator start marker is absent")
	}
	endOffset := strings.Index(authorize.Run[start:], endMarker)
	if endOffset < 0 {
		t.Fatal("superseded run validator end marker is absent")
	}
	validator := authorize.Run[start : start+endOffset]

	const (
		expectedSHA   = "1111111111111111111111111111111111111111"
		historicalSHA = "2222222222222222222222222222222222222222"
		currentRun    = "444"
		targetRun     = "333"
	)
	validCurrentRun := `{"id":444,"workflow_id":77,"head_sha":"` + expectedSHA + `","path":".github/workflows/promote-control-plane-release-lane-rp5.yml","created_at":"2026-07-31T02:00:00Z"}`
	validRun := `{"id":333,"workflow_id":77,"run_attempt":1,"event":"workflow_dispatch","head_branch":"main","head_sha":"` + historicalSHA + `","actor":{"login":"owner"},"path":".github/workflows/promote-control-plane-release-lane-rp5.yml","status":"queued","conclusion":null,"created_at":"2026-07-31T01:00:00Z"}`
	validJobs := `{"total_count":0,"jobs":[]}`
	tests := []struct {
		name            string
		target          string
		targetSHA       string
		runJSON         string
		jobsJSON        string
		actor           string
		owner           string
		ancestorExit    string
		anchorExit      string
		untrustedPolicy bool
		extraDelta      bool
		wantPass        bool
	}{
		{name: "normal dispatch preserves empty recovery path", actor: "owner", owner: "owner", wantPass: true},
		{name: "valid historical zero-job ghost", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner", wantPass: true},
		{name: "run id without source sha", target: targetRun, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "source sha without run id", targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "forged nonnumeric run id", target: "not-a-run", targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "forged source sha", target: targetRun, targetSHA: "not-a-sha", runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "non-owner recovery", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "collaborator", owner: "owner"},
		{name: "current run cannot supersede itself", target: currentRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "current source cannot be quarantined", target: targetRun, targetSHA: expectedSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "non-ancestor source rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner", ancestorExit: "1"},
		{name: "trusted anchor must be current ancestor", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner", anchorExit: "1"},
		{name: "business file in source delta rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner", extraDelta: true},
		{name: "untrusted historical policy rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: validJobs, actor: "owner", owner: "owner", untrustedPolicy: true},
		{name: "historical job rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: validRun, jobsJSON: `{"total_count":1,"jobs":[{"id":9}]}`, actor: "owner", owner: "owner"},
		{name: "wrong source sha rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, historicalSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "completed run is not an active ghost", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, `"status":"queued","conclusion":null`, `"status":"completed","conclusion":"cancelled"`, 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "wrong workflow path rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, "promote-control-plane-release-lane-rp5.yml", "deploy-control-plane.yml", 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "wrong workflow identity rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, `"workflow_id":77`, `"workflow_id":88`, 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "wrong actor rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, `"login":"owner"`, `"login":"collaborator"`, 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
		{name: "non-historical creation time rejects recovery", target: targetRun, targetSHA: historicalSHA, runJSON: strings.Replace(validRun, "2026-07-31T01:00:00Z", "2026-07-31T03:00:00Z", 1), jobsJSON: validJobs, actor: "owner", owner: "owner"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()
			mockBin := filepath.Join(tempDir, "bin")
			if err := os.Mkdir(mockBin, 0o700); err != nil {
				t.Fatalf("create mock bin: %v", err)
			}
			command := exec.Command("bash", "-c", `set -euo pipefail
readonly workflow_path='.github/workflows/promote-control-plane-release-lane-rp5.yml'
readonly test_path='internal/platformsafety/release_lane_promotion_workflow_test.go'
readonly trusted_supersede_policy_anchor_sha='3fa198cbf9733df54d14de65e44668e3f5f51679'
readonly trusted_supersede_policy_baseline_oid='321428db29d486539e6a4ee26e4405c1d209076d'
git() {
  if [[ "$1" == "cat-file" && "$2" == "-e" ]]; then
    return 0
  fi
	  if [[ "$1" == "merge-base" ]]; then
	    if [[ "$3" == "${trusted_supersede_policy_anchor_sha}" ]]; then
	      return "${MOCK_ANCHOR_EXIT:-0}"
	    fi
	    return "${MOCK_ANCESTOR_EXIT:-0}"
	  fi
	  if [[ "$1" == "diff" ]]; then
	    [[ "$4" == "${SUPERSEDE_STUCK_RUN_SHA}" && "$5" == "${trusted_supersede_policy_anchor_sha}" ]] || return 92
    printf 'M\t.github/workflows/promote-control-plane-release-lane-rp5.yml\n'
    printf 'M\tinternal/platformsafety/release_lane_promotion_workflow_test.go\n'
    if [[ "${MOCK_EXTRA_DELTA}" == "true" ]]; then
      printf 'M\tinternal/api/server.go\n'
    fi
    return 0
  fi
  if [[ "$1" == "rev-parse" && "$*" == *"promote-control-plane-release-lane-rp5.yml"* ]]; then
    printf '%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    return 0
  fi
  if [[ "$1" == "rev-parse" && "$*" == *"release_lane_promotion_workflow_test.go"* ]]; then
    printf '%s\n' 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
    return 0
  fi
  if [[ "$1" == "cat-file" && "$2" == "blob" && "$3" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]]; then
    printf '%s\n' 'historical-workflow'
    return 0
  fi
  if [[ "$1" == "cat-file" && "$2" == "blob" && "$3" == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ]]; then
    printf '%s\n' 'historical-test'
    return 0
  fi
  return 91
}
sha256sum() {
  local value
  value="$(cat)"
  if [[ "${MOCK_UNTRUSTED_POLICY}" == "true" ]]; then
    printf '%064d  -\n' 0
    return 0
  fi
  if [[ "${value}" == "historical-workflow" ]]; then
    printf '%s  -\n' '2a59067621f8d933b7c5a12638acb4f87103af556d3ee73561244dfb9abad7ea'
    return 0
  fi
  if [[ "${value}" == "historical-test" ]]; then
    printf '%s  -\n' '4f8afcfce987f53224d0ac9ff08a54d5b2c3248457cd39922f56d082c1ca2dc0'
    return 0
  fi
  return 91
}
github_api_get() {
  if [[ "$1" == *"/jobs?"* ]]; then
    printf '%s\n' "${MOCK_JOBS_JSON}"
  elif [[ "$1" == *"/actions/runs/${GITHUB_RUN_ID}" ]]; then
    printf '%s\n' "${MOCK_CURRENT_RUN_JSON}"
  else
    printf '%s\n' "${MOCK_RUN_JSON}"
  fi
}
`+validator+"\nvalidate_supersede_source_policy && validate_superseded_run")
			ancestorExit := test.ancestorExit
			if ancestorExit == "" {
				ancestorExit = "0"
			}
			anchorExit := test.anchorExit
			if anchorExit == "" {
				anchorExit = "0"
			}
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"SUPERSEDE_STUCK_RUN_ID="+test.target,
				"SUPERSEDE_STUCK_RUN_SHA="+test.targetSHA,
				"MOCK_RUN_JSON="+test.runJSON,
				"MOCK_CURRENT_RUN_JSON="+validCurrentRun,
				"MOCK_JOBS_JSON="+test.jobsJSON,
				"MOCK_ANCESTOR_EXIT="+ancestorExit,
				"MOCK_ANCHOR_EXIT="+anchorExit,
				"MOCK_UNTRUSTED_POLICY="+strconv.FormatBool(test.untrustedPolicy),
				"MOCK_EXTRA_DELTA="+strconv.FormatBool(test.extraDelta),
				"GITHUB_ACTOR="+test.actor,
				"GITHUB_REPOSITORY_OWNER="+test.owner,
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_RUN_ID="+currentRun,
				"GITHUB_SHA="+expectedSHA,
				"EXPECTED_SHA="+expectedSHA,
			)
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("superseded run validator rejected valid case: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("superseded run validator accepted invalid case: output=%s", output)
			}
		})
	}
}

func TestRP5ReleaseLanePromotionEnableSettlementHarness(t *testing.T) {
	t.Parallel()

	enable := rp5PromotionWorkflowStep(t, "promote-deploy-lane", "Enable deploy workflow with exact readback")
	const (
		expectedSHA      = "1111111111111111111111111111111111111111"
		historicalSHA    = "4444444444444444444444444444444444444444"
		expectedBaseline = "2222222222222222222222222222222222222222"
		expectedTerminal = "3333333333333333333333333333333333333333"
		driftedOID       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	tests := []struct {
		name          string
		initialState  string
		mutate        string
		putExit       string
		mainDrift     bool
		baselineDrift bool
		terminalDrift bool
		otherRuns     string
		supersede     string
		supersedeSHA  string
		ghostState    string
		ghostJobs     bool
		extraDelta    bool
		wantPass      bool
		wantState     string
		wantWrites    string
	}{
		{name: "successful response settles", mutate: "true", putExit: "0", wantPass: true, wantState: "active", wantWrites: "PUT\n"},
		{name: "lost response settles by readback", mutate: "true", putExit: "23", wantPass: true, wantState: "active", wantWrites: "PUT\n"},
		{name: "unsettled enable fails closed", mutate: "false", putExit: "23", wantPass: false, wantState: "disabled_manually", wantWrites: "PUT\n"},
		{name: "already active cannot replay", initialState: "active", mutate: "false", putExit: "0", wantPass: false, wantState: "active"},
		{name: "main drift blocks before enable", mutate: "false", putExit: "0", mainDrift: true, wantPass: false, wantState: "disabled_manually"},
		{name: "baseline drift blocks before enable", mutate: "false", putExit: "0", baselineDrift: true, wantPass: false, wantState: "disabled_manually"},
		{name: "terminal drift blocks before enable", mutate: "false", putExit: "0", terminalDrift: true, wantPass: false, wantState: "disabled_manually"},
		{name: "active run blocks before enable", mutate: "false", putExit: "0", otherRuns: "999", wantPass: false, wantState: "disabled_manually"},
		{name: "recovery ignores only exact quarantined ghost", mutate: "true", putExit: "0", otherRuns: "999", supersede: "999", supersedeSHA: historicalSHA, wantPass: true, wantState: "active", wantWrites: "PUT\n"},
		{name: "recovery rejects a different active run", mutate: "false", putExit: "0", otherRuns: "998", supersede: "999", supersedeSHA: historicalSHA, wantPass: false, wantState: "disabled_manually"},
		{name: "recovery rejects one-sided identity", mutate: "false", putExit: "0", otherRuns: "999", supersede: "999", wantPass: false, wantState: "disabled_manually"},
		{name: "recovery rejects ghost leaving queue", mutate: "false", putExit: "0", otherRuns: "999", supersede: "999", supersedeSHA: historicalSHA, ghostState: "completed", wantPass: false, wantState: "disabled_manually"},
		{name: "recovery rejects ghost with historical jobs", mutate: "false", putExit: "0", otherRuns: "999", supersede: "999", supersedeSHA: historicalSHA, ghostJobs: true, wantPass: false, wantState: "disabled_manually"},
		{name: "recovery rejects extra source delta before enable", mutate: "false", putExit: "0", otherRuns: "999", supersede: "999", supersedeSHA: historicalSHA, extraDelta: true, wantPass: false, wantState: "disabled_manually"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()
			mockBin := filepath.Join(tempDir, "bin")
			if err := os.Mkdir(mockBin, 0o700); err != nil {
				t.Fatalf("create mock bin: %v", err)
			}
			stateFile := filepath.Join(tempDir, "state")
			mutationLog := filepath.Join(tempDir, "mutations")
			initialState := test.initialState
			if initialState == "" {
				initialState = "disabled_manually"
			}
			if err := os.WriteFile(stateFile, []byte(initialState+"\n"), 0o600); err != nil {
				t.Fatalf("write initial workflow state: %v", err)
			}
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sleep"), "#!/usr/bin/env bash\nexit 0\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "git"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "cat-file" && "$2" == "-e" ]]; then
  exit 0
fi
if [[ "$1" == "merge-base" ]]; then
  exit 0
fi
if [[ "$1" == "diff" ]]; then
  printf 'M\t.github/workflows/promote-control-plane-release-lane-rp5.yml\n'
  printf 'M\tinternal/platformsafety/release_lane_promotion_workflow_test.go\n'
  if [[ "${MOCK_EXTRA_DELTA}" == "true" ]]; then
    printf 'M\tinternal/api/server.go\n'
  fi
  exit 0
fi
if [[ "$1" == "rev-parse" && "$*" == *"promote-control-plane-release-lane-rp5.yml"* ]]; then
  printf '%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  exit 0
fi
if [[ "$1" == "rev-parse" && "$*" == *"release_lane_promotion_workflow_test.go"* ]]; then
  printf '%s\n' 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
  exit 0
fi
if [[ "$1" == "cat-file" && "$2" == "blob" && "$3" == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]]; then
  printf '%s\n' 'historical-workflow'
  exit 0
fi
if [[ "$1" == "cat-file" && "$2" == "blob" && "$3" == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ]]; then
  printf '%s\n' 'historical-test'
  exit 0
fi
exit 91
`)
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sha256sum"), `#!/usr/bin/env bash
set -euo pipefail
value="$(cat)"
if [[ "${value}" == "historical-workflow" ]]; then
  printf '%s  -\n' '2a59067621f8d933b7c5a12638acb4f87103af556d3ee73561244dfb9abad7ea'
  exit 0
fi
if [[ "${value}" == "historical-test" ]]; then
  printf '%s  -\n' '4f8afcfce987f53224d0ac9ff08a54d5b2c3248457cd39922f56d082c1ca2dc0'
  exit 0
fi
exit 91
`)
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"actions/workflows/deploy-control-plane.yml/enable"* ]]; then
  printf 'PUT\n' >>"${MUTATION_LOG}"
  if [[ "${MUTATE}" == 'true' ]]; then
    printf 'active\n' >"${STATE_FILE}"
  fi
  exit "${PUT_EXIT}"
fi
if [[ "$*" == *"git/ref/heads/main"* ]]; then
  printf '%s\n' "${OBSERVED_MAIN_SHA}"
  exit 0
fi
if [[ "$*" == *"git/ref/heads/fugue-control-plane-release-baseline"* ]]; then
  printf '%s\n' "${OBSERVED_BASELINE_OID}"
  exit 0
fi
if [[ "$*" == *"git/ref/heads/fugue-control-plane-release-terminal-state"* ]]; then
  printf '%s\n' "${OBSERVED_TERMINAL_OID}"
  exit 0
fi
if [[ -n "${SUPERSEDE_STUCK_RUN_ID}" && "$*" == *"actions/runs/${SUPERSEDE_STUCK_RUN_ID}/jobs?"* ]]; then
  printf '%s\n' "${MOCK_SUPERSEDED_JOBS_JSON}"
  exit 0
fi
if [[ -n "${SUPERSEDE_STUCK_RUN_ID}" && "$*" == *"actions/runs/${SUPERSEDE_STUCK_RUN_ID}"* ]]; then
  printf '%s\n' "${MOCK_SUPERSEDED_RUN_JSON}"
  exit 0
fi
if [[ "$*" == *"actions/runs/${GITHUB_RUN_ID}"* ]]; then
  printf '%s\n' "${MOCK_CURRENT_RUN_JSON}"
  exit 0
fi
if [[ "$*" == *"actions/runs?status="* ]]; then
  if [[ -n "${OTHER_RUNS}" ]]; then
    printf '{"workflow_runs":[{"id":%s}]}' "${OTHER_RUNS}"
  else
    printf '%s' '{"workflow_runs":[]}'
  fi
  exit 0
fi
if [[ "$*" == *"actions/workflows/deploy-control-plane.yml"* ]]; then
  cat "${STATE_FILE}"
  exit 0
fi
exit 91
`)
			observedMain := expectedSHA
			if test.mainDrift {
				observedMain = driftedOID
			}
			observedBaseline := expectedBaseline
			if test.baselineDrift {
				observedBaseline = driftedOID
			}
			observedTerminal := expectedTerminal
			if test.terminalDrift {
				observedTerminal = driftedOID
			}
			ghostState := test.ghostState
			ghostConclusion := "null"
			if ghostState == "" {
				ghostState = "queued"
			}
			if ghostState == "completed" {
				ghostConclusion = `"cancelled"`
			}
			ghostJobs := `{"total_count":0,"jobs":[]}`
			if test.ghostJobs {
				ghostJobs = `{"total_count":1,"jobs":[{"id":9}]}`
			}
			currentRunJSON := `{"id":444,"workflow_id":77,"head_sha":"` + expectedSHA + `","path":".github/workflows/promote-control-plane-release-lane-rp5.yml","created_at":"2026-07-31T02:00:00Z"}`
			supersededRunJSON := `{"id":999,"workflow_id":77,"run_attempt":1,"event":"workflow_dispatch","head_branch":"main","head_sha":"` + historicalSHA + `","actor":{"login":"owner"},"path":".github/workflows/promote-control-plane-release-lane-rp5.yml","status":"` + ghostState + `","conclusion":` + ghostConclusion + `,"created_at":"2026-07-31T01:00:00Z"}`
			command := exec.Command("bash", "-c", enable.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STATE_FILE="+stateFile,
				"MUTATION_LOG="+mutationLog,
				"MUTATE="+test.mutate,
				"PUT_EXIT="+test.putExit,
				"EXPECTED_SHA="+expectedSHA,
				"EXPECTED_BASELINE_OID="+expectedBaseline,
				"EXPECTED_TERMINAL_OID="+expectedTerminal,
				"OBSERVED_MAIN_SHA="+observedMain,
				"OBSERVED_BASELINE_OID="+observedBaseline,
				"OBSERVED_TERMINAL_OID="+observedTerminal,
				"OTHER_RUNS="+test.otherRuns,
				"MOCK_CURRENT_RUN_JSON="+currentRunJSON,
				"MOCK_SUPERSEDED_RUN_JSON="+supersededRunJSON,
				"MOCK_SUPERSEDED_JOBS_JSON="+ghostJobs,
				"MOCK_EXTRA_DELTA="+strconv.FormatBool(test.extraDelta),
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_ACTOR=owner",
				"GITHUB_REPOSITORY_OWNER=owner",
				"GITHUB_RUN_ID=444",
				"GITHUB_SHA="+expectedSHA,
				"SUPERSEDE_STUCK_RUN_ID="+test.supersede,
				"SUPERSEDE_STUCK_RUN_SHA="+test.supersedeSHA,
				"GITHUB_STEP_SUMMARY="+filepath.Join(tempDir, "summary"),
				"GH_TOKEN=test",
			)
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("enable settlement failed: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("enable settlement unexpectedly passed: output=%s", output)
			}
			finalState, readErr := os.ReadFile(stateFile)
			if readErr != nil {
				t.Fatalf("read final state: %v", readErr)
			}
			if strings.TrimSpace(string(finalState)) != test.wantState {
				t.Fatalf("final state = %q, want %q", finalState, test.wantState)
			}
			writes, readErr := os.ReadFile(mutationLog)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatalf("read mutation log: %v", readErr)
			}
			if string(writes) != test.wantWrites {
				t.Fatalf("mutation calls = %q, want %q", writes, test.wantWrites)
			}
		})
	}
}

func assertRP5PromotionStepInventory(t *testing.T, steps []releaseWorkflowStep, expected []string) {
	t.Helper()
	if len(steps) != len(expected) {
		t.Fatalf("lane promotion step count = %d, want %d: %+v", len(steps), len(expected), steps)
	}
	for index, name := range expected {
		if steps[index].Name != name || steps[index].If != "" || steps[index].ContinueOnError {
			t.Fatalf("lane promotion step %d drifted: %+v", index, steps[index])
		}
	}
}

func rp5PromotionWorkflowStep(t *testing.T, jobName, stepName string) releaseWorkflowStep {
	t.Helper()
	data, err := os.ReadFile(rp5ReleaseLanePromotionWorkflow)
	if err != nil {
		t.Fatalf("read RP5 lane promotion workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP5 lane promotion workflow: %v", err)
	}
	var match releaseWorkflowStep
	found := false
	for _, step := range workflow.Jobs[jobName].Steps {
		if step.Name == stepName {
			if found {
				t.Fatalf("duplicate step %q", stepName)
			}
			match = step
			found = true
		}
	}
	if !found {
		t.Fatalf("step %q is absent from job %q", stepName, jobName)
	}
	return match
}

func rp5PromotionPolicyIdentitySnippet(t *testing.T, script string) string {
	t.Helper()
	const startMarker = `policy_commit="$(git log --format='%H' -n 1 -- "${workflow_path}" "${test_path}")" || exit 1`
	start := strings.Index(script, startMarker)
	if start < 0 {
		t.Fatal("RP5 policy identity start marker is absent")
	}
	const endMarker = "\ndone"
	endOffset := strings.Index(script[start:], endMarker)
	if endOffset < 0 {
		t.Fatal("RP5 policy identity end marker is absent")
	}
	end := start + endOffset + len(endMarker)
	return script[start:end] + "\n"
}

func prepareRP5PolicyIdentityRepository(t *testing.T, scenario string) (string, string) {
	t.Helper()
	repository := filepath.Join(t.TempDir(), "repository")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatalf("create synthetic repository: %v", err)
	}
	runGit := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", repository}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%s", args, err, output)
		}
		return string(output)
	}
	write := func(path, value string) {
		t.Helper()
		fullPath := filepath.Join(repository, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
			t.Fatalf("create parent for %s: %v", path, err)
		}
		if err := os.WriteFile(fullPath, []byte(value), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	commit := func(message string) {
		t.Helper()
		runGit("add", ".")
		runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", message)
	}

	const workflowPath = ".github/workflows/promote-control-plane-release-lane-rp5.yml"
	const testPath = "internal/platformsafety/release_lane_promotion_workflow_test.go"
	runGit("init", "--quiet", "--initial-branch=main")
	write(workflowPath, "policy v1\n")
	write(testPath, "test v1\n")
	write("README.md", "base\n")
	commit("base")
	baseSHA := strings.TrimSpace(runGit("rev-parse", "HEAD"))

	switch scenario {
	case "forward", "drift", "non-ancestor":
		write(workflowPath, "policy v2\n")
		write(testPath, "test v2\n")
		commit("policy")
	case "split":
		write(workflowPath, "policy v2\n")
		commit("workflow policy")
		write(testPath, "test v2\n")
		commit("test policy")
	default:
		t.Fatalf("unknown RP5 policy identity scenario %q", scenario)
	}

	switch scenario {
	case "forward":
		write("README.md", "forward main\n")
		commit("forward main")
	case "drift":
		write(workflowPath, "policy v3\n")
		commit("policy drift")
	}
	executionSHA := strings.TrimSpace(runGit("rev-parse", "HEAD"))
	if scenario == "non-ancestor" {
		executionSHA = baseSHA
	}
	return repository, executionSHA
}

func writeRP5PromotionExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}
