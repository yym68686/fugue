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

const rp5ReleaseLaneRearmWorkflow = "../../.github/workflows/rearm-control-plane-release-lane-rp5.yml"

func TestRP5ReleaseLaneRearmWorkflowContract(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp5ReleaseLaneRearmWorkflow)
	if err != nil {
		t.Fatalf("read RP5 lane rearm workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "17978be9ee5d586614b8d518234e8fc8f907066c2e9d01c859a973e57bf0ac1a")
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			RunsOn          yaml.Node             `yaml:"runs-on"`
			TimeoutMinutes  int                   `yaml:"timeout-minutes"`
			Environment     string                `yaml:"environment"`
			Permissions     map[string]string     `yaml:"permissions"`
			ContinueOnError bool                  `yaml:"continue-on-error"`
			Steps           []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP5 lane rearm workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "rearm-deploy-lane")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, jobsNode, "rearm-deploy-lane"),
		"runs-on", "timeout-minutes", "environment", "permissions", "steps")

	if len(workflow.On) != 1 {
		t.Fatalf("lane rearm trigger inventory drifted: %+v", workflow.On)
	}
	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok {
		t.Fatal("lane rearm must be workflow_dispatch-only")
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode lane rearm dispatch: %v", err)
	}
	wantInputs := []string{"expected_baseline_oid", "expected_runtime_sha", "expected_sha", "expected_terminal_oid"}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("lane rearm inputs = %v, want %v", got, wantInputs)
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
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("lane rearm top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["rearm-deploy-lane"]
	var runner string
	if err := job.RunsOn.Decode(&runner); err != nil {
		t.Fatalf("decode lane rearm runner: %v", err)
	}
	if runner != "ubuntu-latest" || job.TimeoutMinutes != 10 || job.Environment != "production" ||
		job.ContinueOnError ||
		!reflect.DeepEqual(job.Permissions, map[string]string{"actions": "write", "contents": "read"}) {
		t.Fatalf("lane rearm job boundary drifted: runner=%q job=%+v", runner, job)
	}
	wantSteps := []string{
		"Checkout exact lane rearm policy SHA",
		"Verify exact rearm authorization",
		"Disable formal deploy lane with exact readback",
		"Upload exact lane rearm evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("lane rearm step count = %d, want %d", len(job.Steps), len(wantSteps))
	}
	for index, name := range wantSteps {
		if job.Steps[index].Name != name {
			t.Fatalf("lane rearm step %d = %q, want %q", index, job.Steps[index].Name, name)
		}
		if job.Steps[index].Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(job.Steps[index].Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("lane rearm step %q is invalid bash: %v output=%q", name, err, output)
			}
		}
	}

	authorize := job.Steps[1].Run
	for _, required := range []string{
		"\"${GITHUB_EVENT_NAME}\" == 'workflow_dispatch'",
		"\"${GITHUB_REF}\" == 'refs/heads/main'",
		"git merge-base --is-ancestor \"${policy_commit}\" \"${GITHUB_SHA}\"",
		"A\\t.github/workflows/rearm-control-plane-release-lane-rp5.yml",
		"A\\tinternal/platformsafety/release_lane_rearm_workflow_test.go",
		"git/ref/heads/fugue-control-plane-release-baseline",
		"git/ref/heads/fugue-control-plane-release-terminal-state",
		"fugue-runtime-baseline.json",
		"\"${represented_runtime}\" == \"${EXPECTED_RUNTIME_SHA}\"",
		"for run_status in queued in_progress waiting pending requested",
		"select(.id != ${GITHUB_RUN_ID})",
		"\"${state_before}\" == 'active'",
	} {
		if !strings.Contains(authorize, required) {
			t.Fatalf("lane rearm authorization must contain %q", required)
		}
	}
	disable := job.Steps[2].Run
	for _, required := range []string{
		"readonly workflow_id='deploy-control-plane.yml'",
		"git/ref/heads/main",
		"git/ref/heads/fugue-control-plane-release-baseline",
		"git/ref/heads/fugue-control-plane-release-terminal-state",
		"for run_status in queued in_progress waiting pending requested",
		"\"${state_before}\" == 'active'",
		"--method PUT",
		"actions/workflows/${workflow_id}/disable",
		"mutation_status=$?",
		"for attempt in 1 2 3 4 5",
		"\"${state_after}\" == 'disabled_manually'",
		"\"${settled}\" == 'true'",
		"\"workflow_mutation_attempted\": True",
		"\"ref_mutation_attempted\": False",
		"\"runtime_mutation_attempted\": False",
		"\"cluster_mutation_attempted\": False",
		"\"production_write\": False",
	} {
		if !strings.Contains(disable, required) {
			t.Fatalf("lane rearm mutation must contain %q", required)
		}
	}
	upload := job.Steps[3]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("lane rearm artifact upload drifted: %+v", upload)
	}
	source := string(data)
	if strings.Count(source, "--method PUT") != 1 ||
		strings.Count(source, "actions/workflows/${workflow_id}/disable") != 1 ||
		strings.Count(source, "actions/upload-artifact@") != 1 {
		t.Fatal("lane rearm capability inventory drifted")
	}
	for _, forbidden := range []string{
		"${{ secrets.", "/enable", "/dispatches", "/cancel", "git push", "git update-ref",
		"git commit-tree", "updateRefs", "createRef", "deleteRef", "--method POST",
		"--method PATCH", "--method DELETE", "helm ", "kubectl ", "k3s kubectl",
		"./scripts/upgrade_fugue_control_plane.sh", "fugue app ", "gh api +", "git fetch --no-tags origin +",
		"--method PUT +",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("lane rearm contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP5ReleaseLaneRearmSettlementHarness(t *testing.T) {
	t.Parallel()

	disable := rp5ReleaseLaneRearmStep(t, "Disable formal deploy lane with exact readback")
	const (
		expectedSHA      = "1111111111111111111111111111111111111111"
		expectedBaseline = "2222222222222222222222222222222222222222"
		expectedRuntime  = "3333333333333333333333333333333333333333"
		expectedTerminal = "4444444444444444444444444444444444444444"
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
		{name: "successful response settles", initialState: "active", mutate: "true", putExit: "0", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n"},
		{name: "lost response settles by readback", initialState: "active", mutate: "true", putExit: "23", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n"},
		{name: "unsettled disable fails closed", initialState: "active", mutate: "false", putExit: "23", wantPass: false, wantState: "active", wantWrites: "PUT\n"},
		{name: "already disabled cannot replay", initialState: "disabled_manually", mutate: "false", putExit: "0", wantPass: false, wantState: "disabled_manually"},
		{name: "main drift blocks before disable", initialState: "active", mutate: "false", putExit: "0", mainDrift: true, wantPass: false, wantState: "active"},
		{name: "baseline drift blocks before disable", initialState: "active", mutate: "false", putExit: "0", baselineDrift: true, wantPass: false, wantState: "active"},
		{name: "terminal drift blocks before disable", initialState: "active", mutate: "false", putExit: "0", terminalDrift: true, wantPass: false, wantState: "active"},
		{name: "active run blocks before disable", initialState: "active", mutate: "false", putExit: "0", otherRuns: "999", wantPass: false, wantState: "active"},
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
			if err := os.WriteFile(stateFile, []byte(test.initialState+"\n"), 0o600); err != nil {
				t.Fatalf("write initial state: %v", err)
			}
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sleep"), "#!/usr/bin/env bash\nexit 0\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\n"+
				"set -euo pipefail\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/disable\"* ]]; then\n"+
				"  printf 'PUT\\n' >>\"${MUTATION_LOG}\"\n"+
				"  if [[ \"${MUTATE}\" == 'true' ]]; then printf 'disabled_manually\\n' >\"${STATE_FILE}\"; fi\n"+
				"  exit \"${PUT_EXIT}\"\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/main\"* ]]; then printf '%s\\n' \"${OBSERVED_MAIN_SHA}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/fugue-control-plane-release-baseline\"* ]]; then printf '%s\\n' \"${OBSERVED_BASELINE_OID}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/fugue-control-plane-release-terminal-state\"* ]]; then printf '%s\\n' \"${OBSERVED_TERMINAL_OID}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/runs?status=\"* ]]; then printf '%s' \"${OTHER_RUNS}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml\"* ]]; then cat \"${STATE_FILE}\"; exit 0; fi\n"+
				"exit 91\n")
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
			command := exec.Command("bash", "-c", disable.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STATE_FILE="+stateFile,
				"MUTATION_LOG="+mutationLog,
				"MUTATE="+test.mutate,
				"PUT_EXIT="+test.putExit,
				"EXPECTED_SHA="+expectedSHA,
				"EXPECTED_BASELINE_OID="+expectedBaseline,
				"EXPECTED_RUNTIME_SHA="+expectedRuntime,
				"EXPECTED_TERMINAL_OID="+expectedTerminal,
				"OBSERVED_MAIN_SHA="+observedMain,
				"OBSERVED_BASELINE_OID="+observedBaseline,
				"OBSERVED_TERMINAL_OID="+observedTerminal,
				"OTHER_RUNS="+test.otherRuns,
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_RUN_ID=555",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA="+expectedSHA,
				"GITHUB_OUTPUT="+filepath.Join(tempDir, "outputs"),
				"RUNNER_TEMP="+tempDir,
				"GH_TOKEN=test",
			)
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("rearm settlement failed: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("rearm settlement unexpectedly passed: output=%s", output)
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

func rp5ReleaseLaneRearmStep(t *testing.T, name string) releaseWorkflowStep {
	t.Helper()
	data, err := os.ReadFile(rp5ReleaseLaneRearmWorkflow)
	if err != nil {
		t.Fatalf("read RP5 lane rearm workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP5 lane rearm workflow: %v", err)
	}
	for _, step := range workflow.Jobs["rearm-deploy-lane"].Steps {
		if step.Name == name {
			return step
		}
	}
	t.Fatalf("lane rearm step %q is absent", name)
	return releaseWorkflowStep{}
}
