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

const rp1TerminalFrozenRefCASWorkflow = "../../.github/workflows/advance-control-plane-release-terminal-frozen-rp1.yml"

func TestRP1TerminalFrozenRefCASIsHostedExactForwardOnly(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalFrozenRefCASWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal frozen ref CAS workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "81e5f145bf791eca9f1625915fb4125fe72c6ed17908133cec7ccc2973955254")
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
		t.Fatalf("parse RP1 terminal frozen ref CAS workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "advance-terminal-frozen")
	jobNode := workflowMappingValue(t, jobsNode, "advance-terminal-frozen")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("terminal frozen ref CAS must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode terminal frozen ref CAS dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_previous_reservation_oid",
		"expected_sha",
		"frozen_terminal_oid",
		"materializer_result_artifact_digest",
		"materializer_result_artifact_id",
		"materializer_run_id",
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("terminal frozen ref CAS inputs = %v, want %v", got, wantInputs)
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
		t.Fatalf("terminal frozen ref CAS top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["advance-terminal-frozen"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "write"}) {
		t.Fatalf("terminal frozen ref CAS job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact terminal frozen CAS policy SHA",
		"Verify exact terminal frozen CAS authorization",
		"Write terminal frozen ref CAS intent evidence",
		"Upload terminal frozen ref CAS intent evidence",
		"Observe unchanged health before terminal frozen ref CAS",
		"Advance terminal ref by one exact frozen CAS",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("terminal frozen ref CAS step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("terminal frozen ref CAS step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("terminal frozen ref CAS step %q is not valid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("terminal frozen ref CAS checkout drifted: %+v", checkout)
	}
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                        "${{ inputs.expected_sha }}",
		"EXPECTED_PREVIOUS_RESERVATION_OID":   "${{ inputs.expected_previous_reservation_oid }}",
		"FROZEN_TERMINAL_OID":                 "${{ inputs.frozen_terminal_oid }}",
		"MATERIALIZER_RUN_ID":                 "${{ inputs.materializer_run_id }}",
		"MATERIALIZER_RESULT_ARTIFACT_ID":     "${{ inputs.materializer_result_artifact_id }}",
		"MATERIALIZER_RESULT_ARTIFACT_DIGEST": "${{ inputs.materializer_result_artifact_digest }}",
		"HEALTH_URL":                          "${{ vars.FUGUE_CONTROL_PLANE_RP1_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                            "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("terminal frozen ref CAS verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/advance-control-plane-release-terminal-frozen-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_frozen_ref_cas_workflow_test.go`,
		`"${terminal_oid}" == "${EXPECTED_PREVIOUS_RESERVATION_OID}"`,
		`"${run_head}" == "${policy_parent}"`,
		`.github/workflows/materialize-control-plane-release-terminal-frozen-rp1.yml`,
		`"${artifact_digest}" == "${MATERIALIZER_RESULT_ARTIFACT_DIGEST}"`,
		`"fugue-rp1-terminal-frozen-result-${MATERIALIZER_RUN_ID}-1"`,
		`"reservation-to-frozen-object-materialized-ref-reservation"`,
		`"source_conclusion"] != "success"`,
		`"freeze_reason"] != "reservation_stale"`,
		`readonly expected_previous='5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820'`,
		`readonly expected_frozen='c5355438136ac167cf921928ceb86306a52b42e3'`,
		`readonly expected_materializer_run='29683128715'`,
		`readonly expected_result_artifact='8441215076'`,
		`readonly expected_result_digest='sha256:d729da79b98d00cdcd7d05d51a0edaf0d889125dc119b32b6af0d9d4e8467649'`,
		`value["source_run_id"] != "29677982238"`,
		`value["source_head_sha"] != "5446508ecce13c60f1f6b9c5459ba089b9ea26c9"`,
		`value["source_result_artifact_id"] != 8439593464`,
		`value["source_result_artifact_digest"] != "sha256:0761facb8ebddd0d970d28d850ae697dd0abb2c842a26af43af625bf1d5572ca"`,
		`"${frozen_blob}" == '6975df8c047e98e17592b97fe9703788c9e140c8'`,
		`"${frozen_tree}" == '6e204fbcffe097c5a874453a06c9e91c2f3198e1'`,
		`"source_workflow": ".github/workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yml"`,
		`"transport_status"`,
		`"payload_blob_sha"`,
		`"payload_tree_sha"`,
		`"frozen commit ancestry drifted"`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("terminal frozen ref CAS verifier must contain %q", required)
		}
	}
	if strings.Count(verify.Run, `fugue release terminal frozen {source_run_id}`) != 2 {
		t.Fatal("terminal frozen verifier must compare and reconstruct the exact materializer commit message")
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("terminal frozen ref CAS verifier must not hide source command status through process substitution")
	}
	upload := job.Steps[3]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" {
		t.Fatalf("terminal frozen ref CAS upload action must use the full reviewed SHA: %+v", upload)
	}
	intent := job.Steps[2]
	for _, required := range []string{
		`"transition": "reservation-to-frozen-ref-cas-pending"`,
		`"ref_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"git_history_rewritten": False`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("terminal frozen ref CAS intent must contain %q", required)
		}
	}

	cas := job.Steps[5]
	for _, required := range []string{
		`readonly terminal_ref='refs/heads/fugue-control-plane-release-terminal-state'`,
		`"${before_oid}" == "${PREVIOUS_TERMINAL_OID}"`,
		`"${frozen_parent}" == "${PREVIOUS_TERMINAL_OID}"`,
		`beforeOid:$beforeOid,afterOid:$afterOid,force:$force`,
		`-f "beforeOid=${PREVIOUS_TERMINAL_OID}" -f "afterOid=${FROZEN_TERMINAL_OID}" -F 'force=false'`,
		`"${observed}" == "${FROZEN_TERMINAL_OID}"`,
		`terminal frozen ref settled at exact commit`,
	} {
		if !strings.Contains(cas.Run, required) {
			t.Fatalf("terminal frozen ref CAS mutation must contain %q", required)
		}
	}
	if strings.Count(cas.Run, "updateRefs(input:") != 1 ||
		strings.Count(cas.Run, `-f "query=${mutation}"`) != 1 ||
		strings.Count(cas.Run, "-F 'force=false'") != 1 {
		t.Fatalf("terminal frozen ref CAS exact mutation inventory drifted:\n%s", cas.Run)
	}
	if !strings.Contains(cas.Run, `printf 'terminal frozen ref settled at exact commit %s\n' "${FROZEN_TERMINAL_OID}" || true`+"\n    exit 0") {
		t.Fatal("terminal frozen ref CAS must have no fallible operation after exact target settlement")
	}

	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH",
		"--method PUT", "--method DELETE", " -X ", "createRef", "deleteRef", "force=true",
		`"repos/${GITHUB_REPOSITORY}/git/refs`, "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("terminal frozen ref CAS contains out-of-scope capability %q", forbidden)
		}
	}
	if strings.Count(source, "actions/upload-artifact@") != 1 || strings.Count(source, "updateRefs(input:") != 1 {
		t.Fatal("terminal frozen ref CAS must have exactly one intent upload and one ref mutation")
	}
}

func TestRP1TerminalFrozenRefCASSettlementHarness(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalFrozenRefCASWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal frozen ref CAS workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal frozen ref CAS workflow: %v", err)
	}
	var cas releaseWorkflowStep
	for _, step := range workflow.Jobs["advance-terminal-frozen"].Steps {
		if step.Name == "Advance terminal ref by one exact frozen CAS" {
			cas = step
		}
	}
	if cas.Run == "" {
		t.Fatal("terminal frozen ref CAS executable step is absent")
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
  printf '%s\t%s\n' "${FROZEN_TERMINAL_OID}" "${parent}"
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
      printf '%s\n' "${FROZEN_TERMINAL_OID}" >"${STATE_FILE}"
      exit 7
      ;;
    *)
      printf '%s\n' "${FROZEN_TERMINAL_OID}" >"${STATE_FILE}"
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

	const previousOID = "5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820"
	const frozenOID = "c5355438136ac167cf921928ceb86306a52b42e3"
	policySHA := strings.Repeat("a", 40)
	mutationID := "fugue-rp1-terminal-frozen-" + previousOID[:12] + "-" + frozenOID[:12]
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
			"FROZEN_TERMINAL_OID="+frozenOID,
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
		if result.err != nil || result.state != frozenOID || result.mutations != 1 {
			t.Fatalf("%s must settle exact frozen once: %+v", mode, result)
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
