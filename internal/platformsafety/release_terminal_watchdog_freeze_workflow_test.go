package platformsafety

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const rp2TerminalWatchdogFreezeWorkflow = "../../.github/workflows/enforce-control-plane-release-watchdog-rp2.yml"

func TestRP2TerminalWatchdogFreezeIsBoundedWorkflowRunEnforcer(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp2TerminalWatchdogFreezeWorkflow)
	if err != nil {
		t.Fatalf("read RP2 terminal watchdog freeze workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "f266d467cfe7b8149668197b62f6a2c07dc3ac44fe8aca4bd09a61592d21070d")
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			RunsOn          string                `yaml:"runs-on"`
			TimeoutMinutes  int                   `yaml:"timeout-minutes"`
			Environment     string                `yaml:"environment"`
			Permissions     map[string]string     `yaml:"permissions"`
			ContinueOnError bool                  `yaml:"continue-on-error"`
			Steps           []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP2 terminal watchdog freeze workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "enforce-release-lane-freeze")
	jobNode := workflowMappingValue(t, jobsNode, "enforce-release-lane-freeze")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	if len(workflow.On) != 1 {
		t.Fatalf("freeze enforcer trigger inventory drifted: %+v", workflow.On)
	}
	var trigger struct {
		Workflows []string `yaml:"workflows"`
		Types     []string `yaml:"types"`
	}
	workflowRunNode, ok := workflow.On["workflow_run"]
	if !ok {
		t.Fatal("freeze enforcer must be triggered only by workflow_run")
	}
	if err := workflowRunNode.Decode(&trigger); err != nil {
		t.Fatalf("decode freeze enforcer workflow_run trigger: %v", err)
	}
	if !reflect.DeepEqual(trigger.Workflows, []string{"observe-control-plane-release-terminal-watchdog-rp2"}) ||
		!reflect.DeepEqual(trigger.Types, []string{"completed"}) {
		t.Fatalf("freeze enforcer workflow_run trigger drifted: %+v", trigger)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("freeze enforcer top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["enforce-release-lane-freeze"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 10 || job.Environment != "production" ||
		job.ContinueOnError ||
		!reflect.DeepEqual(job.Permissions, map[string]string{"actions": "write", "contents": "read"}) {
		t.Fatalf("freeze enforcer job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact freeze policy SHA",
		"Verify exact freeze enforcer authorization",
		"Verify observer evidence and choose freeze decision",
		"Enforce freeze decision with exact readback",
		"Upload freeze enforcement evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("freeze enforcer step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("freeze enforcer step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("freeze enforcer step %q is invalid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("freeze enforcer checkout drifted: %+v", checkout)
	}

	authorize := job.Steps[1]
	wantAuthorizeEnv := map[string]string{
		"SOURCE_RUN_ID":        "${{ github.event.workflow_run.id }}",
		"SOURCE_RUN_ATTEMPT":   "${{ github.event.workflow_run.run_attempt }}",
		"SOURCE_HEAD_SHA":      "${{ github.event.workflow_run.head_sha }}",
		"SOURCE_HEAD_BRANCH":   "${{ github.event.workflow_run.head_branch }}",
		"SOURCE_EVENT":         "${{ github.event.workflow_run.event }}",
		"SOURCE_CONCLUSION":    "${{ github.event.workflow_run.conclusion }}",
		"SOURCE_WORKFLOW_PATH": "${{ github.event.workflow_run.path }}",
		"GH_TOKEN":             "${{ github.token }}",
	}
	if !reflect.DeepEqual(authorize.Env, wantAuthorizeEnv) {
		t.Fatalf("freeze enforcer authorization environment drifted: %+v", authorize.Env)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_run'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`"${SOURCE_HEAD_SHA}" == "${GITHUB_SHA}"`,
		`"${SOURCE_HEAD_BRANCH}" == 'main'`,
		`schedule|workflow_dispatch`,
		`observe-control-plane-release-terminal-watchdog-rp2.yml`,
		`policy_commit="$(git log --format='%H' -n 1 -- "${workflow_path}" "${test_path}")"`,
		`git merge-base --is-ancestor "${policy_commit}" "${GITHUB_SHA}"`,
		`A\t.github/workflows/enforce-control-plane-release-watchdog-rp2.yml`,
		`A\tinternal/platformsafety/release_terminal_watchdog_freeze_workflow_test.go`,
		`"${main_head}" == "${GITHUB_SHA}"`,
	} {
		if !strings.Contains(authorize.Run, required) {
			t.Fatalf("freeze enforcer authorization must contain %q", required)
		}
	}

	decide := job.Steps[2]
	for _, required := range []string{
		`fugue-rp2-terminal-watchdog-report-{run_id}-{run_attempt}`,
		`re.fullmatch(r"sha256:[0-9a-f]{64}", digest)`,
		`names != ["observation.json"]`,
		`set(value) != expected_keys`,
		`value.get("policy_sha") != head_sha`,
		`"settled_frozen": "none"`,
		`"settled_success": "none"`,
		`"reservation_active": "none"`,
		`"freeze_candidate_missing": "freeze_required"`,
		`"reservation_stale_success": "freeze_required"`,
		`"reservation_terminal_failure": "freeze_required"`,
		`classification='indeterminate_source_evidence'`,
		`decision='freeze'`,
		`decision='no_op'`,
	} {
		if !strings.Contains(decide.Run, required) {
			t.Fatalf("freeze enforcer decision step must contain %q", required)
		}
	}
	enforce := job.Steps[3]
	for _, required := range []string{
		`readonly workflow_id='deploy-control-plane.yml'`,
		`if [[ "${DECISION}" == 'freeze' && "${state_before}" != 'disabled_manually' ]]`,
		`mutation_status='not_attempted'`,
		`mutation_status=$?`,
		`actions/workflows/${workflow_id}/disable`,
		`"${state_after}" == 'disabled_manually'`,
		`"${mutation_attempted}" == 'false' && "${state_after}" == "${state_before}"`,
		`"workflow_mutation_attempted": mutation_attempted == "true"`,
		`"workflow_mutation_response_status": (`,
		`"ref_mutation_attempted": False`,
		`"object_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"runtime_mutation_attempted": False`,
		`"production_write": False`,
	} {
		if !strings.Contains(enforce.Run, required) {
			t.Fatalf("freeze enforcer mutation step must contain %q", required)
		}
	}
	upload := job.Steps[4]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("freeze enforcer evidence upload drifted: %+v", upload)
	}
	source := string(data)
	if strings.Count(source, "--method PUT") != 1 ||
		strings.Count(source, "actions/workflows/${workflow_id}/disable") != 1 ||
		strings.Count(source, "actions/upload-artifact@") != 1 {
		t.Fatalf("freeze enforcer capability inventory drifted")
	}
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "updateRefs", "createRef", "deleteRef",
		"--method POST", "--method PATCH", "--method DELETE", "/enable", "/cancel", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("freeze enforcer contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP2TerminalWatchdogFreezeAuthorizationUsesRealAdditiveGitHistory(t *testing.T) {
	t.Parallel()

	authorize := rp2FreezeWorkflowStep(t, "Verify exact freeze enforcer authorization")
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
	runGit("init", "--quiet", "--initial-branch=main")
	write("README.md", "base\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "base")
	write(".github/workflows/enforce-control-plane-release-watchdog-rp2.yml", "policy\n")
	write("internal/platformsafety/release_terminal_watchdog_freeze_workflow_test.go", "test\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "policy")
	write("README.md", "unrelated descendant\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "descendant")
	executionSHA := strings.TrimSpace(runGit("rev-parse", "HEAD"))

	mockBin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(mockBin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	writeRP2FreezeExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
	writeRP2FreezeExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"${GITHUB_SHA}\"\n")
	command := exec.Command("bash", "-c", authorize.Run)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GITHUB_EVENT_NAME=workflow_run",
		"GITHUB_REF=refs/heads/main",
		"GITHUB_RUN_ATTEMPT=1",
		"GITHUB_REPOSITORY=example/fugue",
		"GITHUB_SHA="+executionSHA,
		"SOURCE_RUN_ID=12345",
		"SOURCE_RUN_ATTEMPT=1",
		"SOURCE_HEAD_SHA="+executionSHA,
		"SOURCE_HEAD_BRANCH=main",
		"SOURCE_EVENT=workflow_dispatch",
		"SOURCE_CONCLUSION=success",
		"SOURCE_WORKFLOW_PATH=.github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml",
		"GH_TOKEN=test",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run freeze enforcer authorization fixture: %v output=%s", err, output)
	}
}

func TestRP2TerminalWatchdogFreezeEnforcementHarness(t *testing.T) {
	t.Parallel()

	enforce := rp2FreezeWorkflowStep(t, "Enforce freeze decision with exact readback")
	tests := []struct {
		name             string
		decision         string
		action           string
		wantAfter        string
		wantMutation     bool
		wantMutationCall string
		putExit          string
		wantResponse     any
	}{
		{name: "settled report is a no-op", decision: "no_op", action: "none", wantAfter: "active"},
		{name: "freeze decision disables exactly once", decision: "freeze", action: "freeze_required", wantAfter: "disabled_manually", wantMutation: true, wantMutationCall: "PUT\n", putExit: "0", wantResponse: float64(0)},
		{name: "lost disable response settles by readback", decision: "freeze", action: "freeze_required", wantAfter: "disabled_manually", wantMutation: true, wantMutationCall: "PUT\n", putExit: "23", wantResponse: float64(23)},
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
			if err := os.WriteFile(stateFile, []byte("active\n"), 0o600); err != nil {
				t.Fatalf("write initial workflow state: %v", err)
			}
			writeRP2FreezeExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP2FreezeExecutable(t, filepath.Join(mockBin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"actions/workflows/deploy-control-plane.yml/disable"* ]]; then
  printf 'PUT\n' >>"${MUTATION_LOG}"
  printf 'disabled_manually\n' >"${STATE_FILE}"
  exit "${PUT_EXIT}"
fi
if [[ "$*" == *"actions/workflows/deploy-control-plane.yml"* ]]; then
  cat "${STATE_FILE}"
  exit 0
fi
exit 91
`)
			summaryPath := filepath.Join(tempDir, "summary")
			command := exec.Command("bash", "-c", enforce.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STATE_FILE="+stateFile,
				"MUTATION_LOG="+mutationLog,
				"PUT_EXIT="+test.putExit,
				"RUNNER_TEMP="+tempDir,
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_RUN_ID=98765",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA=0123456789abcdef0123456789abcdef01234567",
				"GITHUB_STEP_SUMMARY="+summaryPath,
				"SOURCE_RUN_ID=12345",
				"SOURCE_RUN_ATTEMPT=1",
				"SOURCE_HEAD_SHA=0123456789abcdef0123456789abcdef01234567",
				"SOURCE_CONCLUSION=success",
				"CLASSIFICATION=settled_frozen",
				"ACTION="+test.action,
				"DECISION="+test.decision,
				"ARTIFACT_ID=777",
				"ARTIFACT_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"GH_TOKEN=test",
			)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("run freeze enforcement: %v output=%s", err, output)
			}
			mutationCalls, err := os.ReadFile(mutationLog)
			if err != nil && !os.IsNotExist(err) {
				t.Fatalf("read mutation log: %v", err)
			}
			if string(mutationCalls) != test.wantMutationCall {
				t.Fatalf("mutation calls = %q, want %q", mutationCalls, test.wantMutationCall)
			}
			raw, err := os.ReadFile(filepath.Join(tempDir, "fugue-rp2-terminal-watchdog-freeze", "freeze-result.json"))
			if err != nil {
				t.Fatalf("read freeze result: %v", err)
			}
			var result map[string]any
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("decode freeze result: %v", err)
			}
			if result["decision"] != test.decision ||
				result["deploy_workflow_state_before"] != "active" ||
				result["deploy_workflow_state_after"] != test.wantAfter ||
				result["workflow_mutation_attempted"] != test.wantMutation ||
				result["workflow_mutation_response_status"] != test.wantResponse ||
				result["production_write"] != false ||
				result["ref_mutation_attempted"] != false ||
				result["object_mutation_attempted"] != false ||
				result["cluster_mutation_attempted"] != false ||
				result["runtime_mutation_attempted"] != false {
				t.Fatalf("freeze result drifted: %+v", result)
			}
		})
	}
}

func rp2FreezeWorkflowStep(t *testing.T, name string) releaseWorkflowStep {
	t.Helper()
	data, err := os.ReadFile(rp2TerminalWatchdogFreezeWorkflow)
	if err != nil {
		t.Fatalf("read RP2 terminal watchdog freeze workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP2 terminal watchdog freeze workflow: %v", err)
	}
	var match releaseWorkflowStep
	found := false
	for _, step := range workflow.Jobs["enforce-release-lane-freeze"].Steps {
		if step.Name == name {
			if found {
				t.Fatalf("duplicate RP2 freeze workflow step %q", name)
			}
			match = step
			found = true
		}
	}
	if !found {
		t.Fatalf("RP2 freeze workflow step %q is absent", name)
	}
	return match
}

func writeRP2FreezeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}
