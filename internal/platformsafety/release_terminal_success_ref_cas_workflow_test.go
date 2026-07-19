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

const rp1TerminalSuccessRefCASWorkflow = "../../.github/workflows/advance-control-plane-release-terminal-success-rp1.yml"

func TestRP1TerminalSuccessRefCASIsHostedExactForwardOnly(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalSuccessRefCASWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal success ref CAS workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "50ae07008b25fe7b752cd2ffb675b7b055176a753a67461f52cc6b4f6437fd8d")
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
		t.Fatalf("parse RP1 terminal success ref CAS workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "advance-terminal-success")
	jobNode := workflowMappingValue(t, jobsNode, "advance-terminal-success")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("terminal success ref CAS must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode terminal success ref CAS dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_previous_terminal_oid",
		"expected_sha",
		"materializer_intent_artifact_digest",
		"materializer_intent_artifact_id",
		"materializer_run_id",
		"success_terminal_oid",
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("terminal success ref CAS inputs = %v, want %v", got, wantInputs)
	}
	for _, name := range wantInputs {
		var input releaseWorkflowDispatchInput
		inputNode := dispatch.Inputs[name]
		if err := inputNode.Decode(&input); err != nil {
			t.Fatalf("decode %s input: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("%s must be a required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("terminal success ref CAS top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["advance-terminal-success"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "write"}) {
		t.Fatalf("terminal success ref CAS job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact terminal success CAS policy SHA",
		"Verify exact terminal success CAS authorization",
		"Write terminal success ref CAS intent evidence",
		"Upload terminal success ref CAS intent evidence",
		"Observe unchanged health before terminal success ref CAS",
		"Advance terminal ref by one exact success CAS",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("terminal success ref CAS step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("terminal success ref CAS step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("terminal success ref CAS step %q is not valid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("terminal success ref CAS checkout drifted: %+v", checkout)
	}
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                        "${{ inputs.expected_sha }}",
		"EXPECTED_PREVIOUS_TERMINAL_OID":      "${{ inputs.expected_previous_terminal_oid }}",
		"SUCCESS_TERMINAL_OID":                "${{ inputs.success_terminal_oid }}",
		"MATERIALIZER_RUN_ID":                 "${{ inputs.materializer_run_id }}",
		"MATERIALIZER_INTENT_ARTIFACT_ID":     "${{ inputs.materializer_intent_artifact_id }}",
		"MATERIALIZER_INTENT_ARTIFACT_DIGEST": "${{ inputs.materializer_intent_artifact_digest }}",
		"HEALTH_URL":                          "${{ vars.FUGUE_CONTROL_PLANE_RP1_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                            "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("terminal success ref CAS verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/advance-control-plane-release-terminal-success-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_success_ref_cas_workflow_test.go`,
		`"${terminal_oid}" == "${EXPECTED_PREVIOUS_TERMINAL_OID}"`,
		`"${run_head}" == "${policy_parent}"`,
		`.github/workflows/materialize-control-plane-release-terminal-success-rp1.yml`,
		`"${artifact_digest}" == "${MATERIALIZER_INTENT_ARTIFACT_DIGEST}"`,
		`"reservation-to-success-object-pending-ref-unchanged"`,
		`"${success_blob}" == '6f02bd3684b1cbee348db49a140938b9cfcc7998'`,
		`"${success_tree}" == '321f2bf24ff5a33681baa30600531313af5e3616'`,
		`"success commit ancestry drifted"`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("terminal success ref CAS verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("terminal success ref CAS verifier must not hide source command status through process substitution")
	}
	upload := job.Steps[3]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" {
		t.Fatalf("terminal success ref CAS upload action must use the full reviewed SHA: %+v", upload)
	}
	intent := job.Steps[2]
	for _, required := range []string{
		`"transition": "reservation-to-success-ref-cas-pending"`,
		`"ref_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"git_history_rewritten": False`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("terminal success ref CAS intent must contain %q", required)
		}
	}

	cas := job.Steps[5]
	for _, required := range []string{
		`readonly terminal_ref='refs/heads/fugue-control-plane-release-terminal-state'`,
		`"${before_oid}" == "${PREVIOUS_TERMINAL_OID}"`,
		`"${success_parent}" == "${PREVIOUS_TERMINAL_OID}"`,
		`beforeOid:$beforeOid,afterOid:$afterOid,force:$force`,
		`-f "beforeOid=${PREVIOUS_TERMINAL_OID}" -f "afterOid=${SUCCESS_TERMINAL_OID}" -F 'force=false'`,
		`"${observed}" == "${SUCCESS_TERMINAL_OID}"`,
		`terminal success ref settled at exact commit`,
	} {
		if !strings.Contains(cas.Run, required) {
			t.Fatalf("terminal success ref CAS mutation must contain %q", required)
		}
	}
	if strings.Count(cas.Run, "updateRefs(input:") != 1 ||
		strings.Count(cas.Run, `-f "query=${mutation}"`) != 1 ||
		strings.Count(cas.Run, "-F 'force=false'") != 1 {
		t.Fatalf("terminal success ref CAS exact mutation inventory drifted:\n%s", cas.Run)
	}
	if !strings.Contains(cas.Run, `printf 'terminal success ref settled at exact commit %s\n' "${SUCCESS_TERMINAL_OID}" || true`+"\n    exit 0") {
		t.Fatal("terminal success ref CAS must have no fallible operation after exact target settlement")
	}

	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH",
		"--method PUT", "--method DELETE", " -X ", "createRef", "deleteRef", "force=true",
		`"repos/${GITHUB_REPOSITORY}/git/refs`, "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("terminal success ref CAS contains out-of-scope capability %q", forbidden)
		}
	}
	if strings.Count(source, "actions/upload-artifact@") != 1 || strings.Count(source, "updateRefs(input:") != 1 {
		t.Fatal("terminal success ref CAS must have exactly one intent upload and one ref mutation")
	}
}

func TestRP1TerminalSuccessRefCASSettlementHarness(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalSuccessRefCASWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal success ref CAS workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal success ref CAS workflow: %v", err)
	}
	var cas releaseWorkflowStep
	for _, step := range workflow.Jobs["advance-terminal-success"].Steps {
		if step.Name == "Advance terminal ref by one exact success CAS" {
			cas = step
		}
	}
	if cas.Run == "" {
		t.Fatal("terminal success ref CAS executable step is absent")
	}

	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
if [[ "${arguments}" == *'/git/ref/heads/main'* ]]; then
  [[ "${MODE}" != 'main_drift' ]] && printf '%s\n' "${EXPECTED_SHA}" || printf '%040d\n' 8
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-terminal-state'* ]]; then
  if [[ "${MODE}" == 'ref_drift' ]]; then
    printf '%040d\n' 9
  else
    cat "${STATE_FILE}"
  fi
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  parent="${PREVIOUS_TERMINAL_OID}"
  [[ "${MODE}" != 'parent_drift' ]] || parent="$(printf '%040d' 7)"
  printf '%s\t%s\n' "${SUCCESS_TERMINAL_OID}" "${parent}"
  exit 0
fi
if [[ "${arguments}" == *'repository(owner:'* ]]; then
  printf 'R_mock_repository\n'
  exit 0
fi
if [[ "${arguments}" == *'updateRefs(input:'* ]]; then
  case "${MODE}" in
    mutation_rejected)
      exit 7
      ;;
    settlement_missing)
      printf '%s\n' "${MUTATION_ID}"
      exit 0
      ;;
    lost_response)
      printf '%s\n' "${SUCCESS_TERMINAL_OID}" >"${STATE_FILE}"
      exit 7
      ;;
    *)
      printf '%s\n' "${SUCCESS_TERMINAL_OID}" >"${STATE_FILE}"
      printf '%s\n' "${MUTATION_ID}"
      exit 0
      ;;
  esac
fi
exit 97
`
	timeoutMock := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 125
shift
exec "$@"
`
	for name, source := range map[string]string{"gh": ghMock, "timeout": timeoutMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write %s mock: %v", name, err)
		}
	}

	const previousOID = "18f6abf0ffe4dcfadcddabe368e94cbb2310ae9a"
	const successOID = "8686c3e867dbdbd98bfc77afb374d88007b045d7"
	policySHA := strings.Repeat("a", 40)
	mutationID := "fugue-rp1-terminal-success-" + previousOID[:12] + "-" + successOID[:12]
	type result struct {
		err       error
		log       string
		state     string
		mutations int
	}
	run := func(t *testing.T, mode string) result {
		t.Helper()
		statePath := filepath.Join(root, "state-"+mode)
		logPath := filepath.Join(root, "log-"+mode)
		if err := os.WriteFile(statePath, []byte(previousOID+"\n"), 0o600); err != nil {
			t.Fatalf("write initial state: %v", err)
		}
		command := exec.Command("bash")
		command.Stdin = strings.NewReader(cas.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"LOG_FILE="+logPath,
			"STATE_FILE="+statePath,
			"MUTATION_ID="+mutationID,
			"EXPECTED_SHA="+policySHA,
			"PREVIOUS_TERMINAL_OID="+previousOID,
			"SUCCESS_TERMINAL_OID="+successOID,
			"GITHUB_EVENT_NAME=workflow_dispatch",
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"GITHUB_REPOSITORY=test/repository",
			"GITHUB_REPOSITORY_OWNER=test",
		)
		_, runErr := command.CombinedOutput()
		logBytes, _ := os.ReadFile(logPath)
		stateBytes, _ := os.ReadFile(statePath)
		log := string(logBytes)
		return result{
			err:       runErr,
			log:       log,
			state:     strings.TrimSpace(string(stateBytes)),
			mutations: strings.Count(log, "updateRefs(input:"),
		}
	}

	for _, mode := range []string{"success", "lost_response"} {
		result := run(t, mode)
		if result.err != nil || result.state != successOID || result.mutations != 1 {
			t.Fatalf("%s must settle exact success once: %+v", mode, result)
		}
		if strings.Contains(result.log, "--method PATCH") || strings.Contains(result.log, "force=true") {
			t.Fatalf("%s used a forbidden mutation path: %s", mode, result.log)
		}
	}
	for _, mode := range []string{"mutation_rejected", "settlement_missing"} {
		result := run(t, mode)
		if result.err == nil || result.state != previousOID || result.mutations != 1 {
			t.Fatalf("%s must fail closed after one unresolved mutation: %+v", mode, result)
		}
	}
	for _, mode := range []string{"main_drift", "ref_drift", "parent_drift"} {
		result := run(t, mode)
		if result.err == nil || result.state != previousOID || result.mutations != 0 {
			t.Fatalf("%s must fail before mutation: %+v", mode, result)
		}
	}
}
