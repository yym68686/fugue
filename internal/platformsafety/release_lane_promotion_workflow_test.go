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

const rp5ReleaseLanePromotionWorkflow = "../../.github/workflows/promote-control-plane-release-lane-rp5.yml"

func TestRP5ReleaseLanePromotionIsOneShotReadOnlyQualificationAndEnable(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp5ReleaseLanePromotionWorkflow)
	if err != nil {
		t.Fatalf("read RP5 lane promotion workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "63ad76296a83211127ad01d64ce39ba708af36c402d0e811fe815e378f2be1ef")
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
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
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("lane promotion inputs = %v, want %v", got, wantInputs)
	}
	for _, name := range wantInputs {
		var input releaseWorkflowDispatchInput
		node := dispatch.Inputs[name]
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode %s input: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("%s must be a required string without default: %+v", name, input)
		}
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
		`"${policy_commit}" == "${GITHUB_SHA}"`,
		`M\t.github/workflows/promote-control-plane-release-lane-rp5.yml`,
		`M\tinternal/platformsafety/release_lane_promotion_workflow_test.go`,
		`github_api_get()`,
		`curl \`,
		`--header "Authorization: Bearer ${GITHUB_TOKEN}"`,
		`--header 'X-GitHub-Api-Version: 2022-11-28'`,
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
		`if identifier != current:`,
	} {
		if !strings.Contains(authorize.Run, required) {
			t.Fatalf("lane qualification authorization must contain %q", required)
		}
	}
	if strings.Contains(authorize.Run, "gh api") {
		t.Fatal("self-hosted lane qualification must not depend on the absent gh CLI")
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
		`for run_status in queued in_progress waiting pending requested`,
		`select(.id != ${GITHUB_RUN_ID})`,
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
		strings.Count(source, "actions/upload-artifact@") != 1 {
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
	const endMarker = "\n}\npolicy_commit="
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

func TestRP5ReleaseLanePromotionEnableSettlementHarness(t *testing.T) {
	t.Parallel()

	enable := rp5PromotionWorkflowStep(t, "promote-deploy-lane", "Enable deploy workflow with exact readback")
	const (
		expectedSHA      = "1111111111111111111111111111111111111111"
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
if [[ "$*" == *"actions/runs?status="* ]]; then
  printf '%s' "${OTHER_RUNS}"
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
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_RUN_ID=444",
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

func writeRP5PromotionExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}
