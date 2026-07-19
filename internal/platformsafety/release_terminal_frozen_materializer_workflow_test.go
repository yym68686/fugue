package platformsafety

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/releaseterminal"

	"gopkg.in/yaml.v3"
)

const rp1TerminalFrozenMaterializerWorkflow = "../../.github/workflows/materialize-control-plane-release-terminal-frozen-rp1.yml"

func TestReleaseTerminalFrozenMaterializerIsHostedObjectOnlyAndSourceBound(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalFrozenMaterializerWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal frozen materializer: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "5c03483cbd3e4c7981b81e7f42490078926f0044a4a56bef21c4337a0844bba1")
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
		t.Fatalf("parse RP1 terminal frozen materializer: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "materialize-frozen-finalization")
	jobNode := workflowMappingValue(t, jobsNode, "materialize-frozen-finalization")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("frozen materializer must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode frozen workflow_dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_sha",
		"materializer_artifact_digest",
		"materializer_artifact_id",
		"reservation_oid",
		"source_run_id",
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("frozen materializer inputs = %v, want %v", got, wantInputs)
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
		t.Fatalf("frozen materializer top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["materialize-frozen-finalization"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "write"}) {
		t.Fatalf("frozen materializer job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact frozen materializer policy SHA",
		"Verify exact frozen materialization authorization",
		"Write frozen materialization intent evidence",
		"Upload frozen materialization intent evidence",
		"Observe unchanged health before frozen object write",
		"Materialize canonical frozen objects without moving a ref",
		"Write frozen materialization result evidence",
		"Upload frozen materialization result evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("frozen materializer step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("frozen materializer step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("frozen step %q is not valid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("frozen materializer checkout drifted: %+v", checkout)
	}
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                 "${{ inputs.expected_sha }}",
		"RESERVATION_OID":              "${{ inputs.reservation_oid }}",
		"SOURCE_RUN_ID":                "${{ inputs.source_run_id }}",
		"MATERIALIZER_ARTIFACT_ID":     "${{ inputs.materializer_artifact_id }}",
		"MATERIALIZER_ARTIFACT_DIGEST": "${{ inputs.materializer_artifact_digest }}",
		"HEALTH_URL":                   "${{ vars.FUGUE_CONTROL_PLANE_RP1_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                     "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("frozen verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`readonly expected_reservation='5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820'`,
		`readonly expected_source_run='29677982238'`,
		`A\t.github/workflows/materialize-control-plane-release-terminal-frozen-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_frozen_materializer_workflow_test.go`,
		`"${run_status}" == 'completed' && "${run_conclusion}" == 'success'`,
		`"${terminal_oid}" == "${RESERVATION_OID}"`,
		`"source materializer result drifted"`,
		`"current terminal reservation commit OID drifted"`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("frozen verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("frozen verifier must not hide source command status through process substitution")
	}
	for _, index := range []int{3, 7} {
		if job.Steps[index].Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" {
			t.Fatalf("frozen upload %d must use the full reviewed SHA: %+v", index, job.Steps[index])
		}
	}
	intent := job.Steps[2]
	for _, required := range []string{
		`"source_conclusion": "success"`,
		`"freeze_reason": "reservation_stale"`,
		`"transition": "reservation-to-frozen-object-pending-ref-reservation"`,
		`"ref_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"git_history_rewritten": False`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("frozen intent must contain %q", required)
		}
	}
	materialize := job.Steps[5]
	for _, required := range []string{
		`readonly expected_blob='6975df8c047e98e17592b97fe9703788c9e140c8'`,
		`readonly expected_tree='6e204fbcffe097c5a874453a06c9e91c2f3198e1'`,
		`readonly expected_commit='c5355438136ac167cf921928ceb86306a52b42e3'`,
		`"terminal_mode": "frozen"`,
		`"source_conclusion": "success"`,
		`"freeze_reason": "reservation_stale"`,
		`f"tree {tree_oid}\nparent {reservation}\nauthor`,
		`"parents": [reservation]`,
		`"${terminal_before}" == "${RESERVATION_OID}"`,
		`"${terminal_after}" == "${RESERVATION_OID}"`,
		`"frozen commit ancestry drifted"`,
	} {
		if !strings.Contains(materialize.Run, required) {
			t.Fatalf("frozen materializer must contain %q", required)
		}
	}
	if strings.Count(materialize.Run, "gh api --method POST") != 3 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/blobs"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/trees"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/commits"`) != 1 {
		t.Fatalf("frozen object API inventory drifted:\n%s", materialize.Run)
	}
	result := job.Steps[6]
	if !strings.Contains(result.Run, `"reservation-to-frozen-object-materialized-ref-reservation"`) ||
		!strings.Contains(result.Run, `"transport_status"`) {
		t.Fatal("frozen result evidence is not settlement-bound")
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH",
		"--method PUT", "--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		"force=", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("frozen materializer contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestReleaseTerminalFrozenMaterializerDeterministicSettlement(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalFrozenMaterializerWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal frozen materializer: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal frozen materializer: %v", err)
	}
	var materialize releaseWorkflowStep
	for _, step := range workflow.Jobs["materialize-frozen-finalization"].Steps {
		if step.Name == "Materialize canonical frozen objects without moving a ref" {
			materialize = step
		}
	}
	if materialize.Run == "" {
		t.Fatal("frozen materializer executable step is absent")
	}

	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create frozen mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
input=''
previous=''
for argument in "$@"; do
  if [[ "${previous}" == '--input' ]]; then input="${argument}"; fi
  previous="${argument}"
done
if [[ "${arguments}" == *'--method POST'*'/git/blobs'* ]]; then
  cp "${input}" "${REMOTE_DIR}/blob-request.json"
  [[ "${MODE}" != 'blob_lost' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/trees'* ]]; then
  cp "${input}" "${REMOTE_DIR}/tree-request.json"
  [[ "${MODE}" != 'tree_lost' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/commits'* ]]; then
  cp "${input}" "${REMOTE_DIR}/commit-request.json"
  [[ "${MODE}" != 'commit_lost' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'/git/blobs/'* ]]; then
  [[ "${MODE}" != 'blob_absent' ]] || exit 7
  sha="${arguments#*/git/blobs/}"
  python3 - "${REMOTE_DIR}/blob-request.json" "${sha}" <<'PY'
import base64, json, sys
request = json.load(open(sys.argv[1], encoding="utf-8"))
content = request["content"].encode("utf-8")
print(json.dumps({"sha": sys.argv[2], "encoding": "base64", "content": base64.b64encode(content).decode("ascii")}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/trees/'* ]]; then
  sha="${arguments#*/git/trees/}"
  python3 - "${REMOTE_DIR}/tree-request.json" "${sha}" <<'PY'
import json, os, sys
request = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({"sha": sys.argv[2], "truncated": os.environ["MODE"] == "tree_drift", "tree": request["tree"]}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  sha="${arguments#*/git/commits/}"
  python3 - "${REMOTE_DIR}/commit-request.json" "${sha}" <<'PY'
import json, os, sys
request = json.load(open(sys.argv[1], encoding="utf-8"))
parent = request["parents"][0] if os.environ["MODE"] != "commit_parent" else "7" * 40
print(json.dumps({"sha": sys.argv[2], "message": request["message"], "tree": {"sha": request["tree"]}, "parents": [{"sha": parent}], "author": request["author"], "committer": request["committer"]}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/fugue-control-plane-release-terminal-state'* ]]; then
  [[ "${MODE}" != 'ref_drift' ]] && printf '%s\n' "${RESERVATION_OID}" || printf '%040d\n' 9
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/main'* ]]; then
  [[ "${MODE}" != 'main_drift' ]] && printf '%s\n' "${EXPECTED_SHA}" || printf '%040d\n' 8
  exit 0
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
			t.Fatalf("write frozen %s mock: %v", name, err)
		}
	}

	const reservationOID = "5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820"
	const sourceRunID = "29677982238"
	const sourceHead = "5446508ecce13c60f1f6b9c5459ba089b9ea26c9"
	const terminalDate = "2026-07-19T07:13:13Z"
	policySHA := strings.Repeat("a", 40)
	type result struct {
		err     error
		log     string
		outputs map[string]string
		remote  string
	}
	run := func(t *testing.T, mode string) result {
		t.Helper()
		caseDir := t.TempDir()
		remoteDir := filepath.Join(caseDir, "remote")
		if err := os.Mkdir(remoteDir, 0o700); err != nil {
			t.Fatalf("create frozen remote fixture: %v", err)
		}
		logPath := filepath.Join(caseDir, "gh.log")
		outputPath := filepath.Join(caseDir, "github-output")
		command := exec.Command("bash")
		command.Stdin = strings.NewReader(materialize.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"LOG_FILE="+logPath,
			"REMOTE_DIR="+remoteDir,
			"EXPECTED_SHA="+policySHA,
			"RESERVATION_OID="+reservationOID,
			"SOURCE_RUN_ID="+sourceRunID,
			"SOURCE_HEAD="+sourceHead,
			"TERMINAL_DATE="+terminalDate,
			"GITHUB_EVENT_NAME=workflow_dispatch",
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"GITHUB_REPOSITORY=test/repository",
			"GITHUB_OUTPUT="+outputPath,
		)
		_, runErr := command.CombinedOutput()
		logBytes, _ := os.ReadFile(logPath)
		outputs := map[string]string{}
		if runErr == nil {
			outputs = readWorkflowOutputFile(t, outputPath)
		}
		return result{err: runErr, log: string(logBytes), outputs: outputs, remote: remoteDir}
	}

	for _, mode := range []string{"success", "blob_lost", "tree_lost", "commit_lost"} {
		result := run(t, mode)
		if result.err != nil || strings.Count(result.log, "--method POST") != 3 {
			t.Fatalf("%s must settle all three immutable frozen objects: err=%v log=%s", mode, result.err, result.log)
		}
		payloadRequest, err := os.ReadFile(filepath.Join(result.remote, "blob-request.json"))
		if err != nil {
			t.Fatalf("%s read frozen blob request: %v", mode, err)
		}
		var request struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(payloadRequest, &request); err != nil {
			t.Fatalf("%s decode frozen blob request: %v", mode, err)
		}
		document, err := releaseterminal.Decode([]byte(request.Content))
		if err != nil {
			t.Fatalf("%s decode frozen document: %v", mode, err)
		}
		if document.TerminalMode != releaseterminal.ModeFrozen ||
			document.SourceWorkflow != releaseterminal.WorkflowTerminalNextReservationMaterializer ||
			document.SourceRunID != sourceRunID || document.SourceHeadSHA != sourceHead ||
			document.SourceConclusion != "success" || document.ReservationOID != reservationOID ||
			document.PreviousTerminalStateOID != reservationOID ||
			document.FreezeReason != releaseterminal.FreezeReasonReservationStale {
			t.Fatalf("%s frozen document drifted: %+v", mode, document)
		}
		if result.outputs["payload_blob_sha"] != "6975df8c047e98e17592b97fe9703788c9e140c8" ||
			result.outputs["payload_tree_sha"] != "6e204fbcffe097c5a874453a06c9e91c2f3198e1" ||
			result.outputs["frozen_oid"] != "c5355438136ac167cf921928ceb86306a52b42e3" {
			t.Fatalf("%s frozen object outputs drifted: %+v", mode, result.outputs)
		}
	}
	for _, mode := range []string{"ref_drift", "main_drift"} {
		result := run(t, mode)
		if result.err == nil || strings.Count(result.log, "--method POST") != 0 {
			t.Fatalf("%s must fail before frozen object mutation: err=%v log=%s", mode, result.err, result.log)
		}
	}
	for _, mode := range []string{"blob_absent", "tree_drift", "commit_parent"} {
		result := run(t, mode)
		if result.err == nil {
			t.Fatalf("%s must fail closed on frozen readback drift", mode)
		}
	}
}
