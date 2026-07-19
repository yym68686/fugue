package platformsafety

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/releaseterminal"

	"gopkg.in/yaml.v3"
)

const rp1TerminalSuccessMaterializerWorkflow = "../../.github/workflows/materialize-control-plane-release-terminal-success-rp1.yml"

func TestRP1TerminalSuccessMaterializerIsHostedObjectOnlyAndSourceBound(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalSuccessMaterializerWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal success materializer: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "0f2f0ed9d90b8abe1a8423491c13bb8d6aa160c9d2f066b7a449f3c85c50e2b7")
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
		t.Fatalf("parse RP1 terminal success materializer: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "materialize-success-finalization")
	jobNode := workflowMappingValue(t, jobsNode, "materialize-success-finalization")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("success materializer must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode success workflow_dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_sha",
		"materializer_artifact_digest",
		"materializer_artifact_id",
		"reservation_oid",
		"source_run_id",
	}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("success materializer inputs = %v, want %v", got, wantInputs)
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
		t.Fatalf("success materializer top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["materialize-success-finalization"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "write"}) {
		t.Fatalf("success materializer job boundary drifted: %+v", job)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"materialize-success-finalization": {Steps: job.Steps},
	}, map[string]string{
		"materialize-success-finalization/Verify exact success materialization authorization":                      "dc5457fb554ca9ad3c5407a706800e0a6a3357d985f285792697251dae0e5bda",
		"materialize-success-finalization/Write success finalization intent evidence":                              "7bd3e21a0efc0d2080b58c88ceb9783c1a7cd62e275defc0062c1fc1ff1e1b53",
		"materialize-success-finalization/Observe unchanged production health before success object write":         "cebde1718b247d6d5ca0bad326c5b44aa1695d28905a303aab6f42af26c0cfc9",
		"materialize-success-finalization/Materialize canonical success finalization objects without moving a ref": "f39b03869293af3d91c304dff04e8619b630bde5e8dc51997a6d07ed77bf44fa",
	})

	wantSteps := []string{
		"Checkout exact success materializer policy SHA",
		"Verify exact success materialization authorization",
		"Write success finalization intent evidence",
		"Upload success finalization intent evidence",
		"Observe unchanged production health before success object write",
		"Materialize canonical success finalization objects without moving a ref",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("success materializer step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("success materializer step %d drifted: %+v", index, step)
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("success materializer checkout drifted: %+v", checkout)
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
		t.Fatalf("success materializer verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/materialize-control-plane-release-terminal-success-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_success_materializer_workflow_test.go`,
		`"${terminal_oid}" == "${RESERVATION_OID}"`,
		`"${run_status}" == 'completed' && "${run_conclusion}" == 'success'`,
		`.github/workflows/write-control-plane-release-terminal-rp1.yml`,
		`"${artifact_digest}" == "${MATERIALIZER_ARTIFACT_DIGEST}"`,
		`"${reservation_blob}" == '0b5b82ddd3184402d723df60beb53129b6ed6a35'`,
		`"${reservation_tree}" == '1b439fb6e1ab969a6cfe77cad4d76f4272b64d3a'`,
		`"${terminal_date}" == '2026-07-19T00:52:33Z'`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("success materializer verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("success materializer verifier must not hide source command status through process substitution")
	}

	intent := job.Steps[2]
	materialize := job.Steps[5]
	for _, required := range []string{
		`"terminal_mode": "success"`,
		`"source_conclusion": "success"`,
		`"previous_terminal_state_oid": reservation`,
		`"reservation_oid": reservation`,
		`readonly expected_blob='6f02bd3684b1cbee348db49a140938b9cfcc7998'`,
		`readonly expected_tree='321f2bf24ff5a33681baa30600531313af5e3616'`,
		`readonly expected_commit='8686c3e867dbdbd98bfc77afb374d88007b045d7'`,
		`"transition": "reservation-to-success-object-pending-ref-unchanged"`,
		`"ref_mutation_attempted": False`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("success intent must contain %q", required)
		}
	}
	for _, required := range []string{
		`"${terminal_oid}" == "${RESERVATION_OID}"`,
		`"${main_head}" == "${EXPECTED_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${EXPECTED_BLOB}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${EXPECTED_TREE}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${EXPECTED_COMMIT}"`,
		`len(parents) != 1`,
		`parents[0].get("sha") != reservation`,
		`success finalization objects settled at exact commit`,
	} {
		if !strings.Contains(materialize.Run, required) {
			t.Fatalf("success materializer must contain %q", required)
		}
	}
	if strings.Count(materialize.Run, "gh api --method POST") != 3 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/blobs"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/trees"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/commits"`) != 1 ||
		strings.Count(materialize.Run, `git/matching-refs/heads/fugue-control-plane-release-terminal-state`) != 1 {
		t.Fatalf("success materializer object/ref API inventory drifted:\n%s", materialize.Run)
	}
	lastHeredocEnd := strings.LastIndex(materialize.Run, "\nPY\n")
	if lastHeredocEnd < 0 || materialize.Run[lastHeredocEnd:] != "\nPY\nprintf 'success finalization objects settled at exact commit %s\\n' \"${EXPECTED_COMMIT}\" || true\n" {
		t.Fatalf("success materializer has a fallible tail after exact commit settlement:\n%s", materialize.Run[lastHeredocEnd+1:])
	}

	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		`"repos/${GITHUB_REPOSITORY}/git/refs`, "force=", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("success materializer contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP1TerminalSuccessMaterializerDeterministicObjectsAndSettlement(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalSuccessMaterializerWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal success materializer: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal success materializer: %v", err)
	}
	var intent, materialize releaseWorkflowStep
	for _, step := range workflow.Jobs["materialize-success-finalization"].Steps {
		switch step.Name {
		case "Write success finalization intent evidence":
			intent = step
		case "Materialize canonical success finalization objects without moving a ref":
			materialize = step
		}
	}
	if intent.Run == "" || materialize.Run == "" {
		t.Fatal("success materializer executable steps are absent")
	}

	const sourceRunID = "29667919251"
	const terminalDate = "2026-07-19T00:52:33Z"
	const reservationOID = "18f6abf0ffe4dcfadcddabe368e94cbb2310ae9a"
	const materializerHead = "ea671271f974e6b9f17a79a55413cb363bc91f79"
	const expectedBlob = "6f02bd3684b1cbee348db49a140938b9cfcc7998"
	const expectedTree = "321f2bf24ff5a33681baa30600531313af5e3616"
	const expectedCommit = "8686c3e867dbdbd98bfc77afb374d88007b045d7"
	policySHA := strings.Repeat("a", 40)

	root := t.TempDir()
	runnerTemp := filepath.Join(root, "runner")
	if err := os.Mkdir(runnerTemp, 0o700); err != nil {
		t.Fatalf("create runner temp: %v", err)
	}
	outputPath := filepath.Join(root, "github-output")
	command := exec.Command("bash")
	command.Stdin = strings.NewReader(intent.Run)
	command.Env = append(os.Environ(),
		"RUNNER_TEMP="+runnerTemp,
		"EXPECTED_SHA="+policySHA,
		"RESERVATION_OID="+reservationOID,
		"SOURCE_RUN_ID="+sourceRunID,
		"MATERIALIZER_HEAD="+materializerHead,
		"TERMINAL_DATE="+terminalDate,
		"GITHUB_REPOSITORY=fugue-test/repository",
		"GITHUB_RUN_ID=29680000001",
		"GITHUB_RUN_ATTEMPT=1",
		"GITHUB_OUTPUT="+outputPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run success intent: %v output=%q", err, output)
	}
	outputs := readWorkflowOutputFile(t, outputPath)
	if outputs["expected_blob"] != expectedBlob || outputs["expected_tree"] != expectedTree ||
		outputs["expected_commit"] != expectedCommit {
		t.Fatalf("success object OIDs drifted: %+v", outputs)
	}
	objectDir := outputs["object_dir"]
	payload, err := os.ReadFile(filepath.Join(objectDir, "terminal-state.json"))
	if err != nil {
		t.Fatalf("read success payload: %v", err)
	}
	resolved, err := releaseterminal.Decode(payload)
	if err != nil {
		t.Fatalf("decode success payload: %v", err)
	}
	wantDocument := releaseterminal.Document{
		SchemaVersion:            releaseterminal.SchemaVersion,
		CertificateKind:          releaseterminal.CertificateKindFinalization,
		TerminalMode:             releaseterminal.ModeSuccess,
		SourceRunID:              sourceRunID,
		SourceRunAttempt:         1,
		SourceHeadSHA:            materializerHead,
		SourceWorkflow:           releaseterminal.WorkflowTerminalWriter,
		SourceConclusion:         "success",
		PreviousTerminalStateOID: reservationOID,
		ReservationOID:           reservationOID,
	}
	if resolved != wantDocument {
		t.Fatalf("success payload = %#v, want %#v", resolved, wantDocument)
	}

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create success mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-terminal-state'* ]]; then
  [[ "${MODE}" != 'ref_drift' ]] && printf '%s\n' "${RESERVATION_OID}" || printf '%040d\n' 9
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/main'* ]]; then
  [[ "${MODE}" != 'main_drift' ]] && printf '%s\n' "${EXPECTED_SHA}" || printf '%040d\n' 8
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/blobs'* ]]; then
  [[ "${MODE}" != 'blob_lost' && "${MODE}" != 'blob_absent' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/trees'* ]]; then
  [[ "${MODE}" != 'tree_lost' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/commits'* ]]; then
  [[ "${MODE}" != 'commit_lost' ]] || exit 7
  printf '{}\n'
  exit 0
fi
if [[ "${arguments}" == *'/git/blobs/'* ]]; then
  [[ "${MODE}" != 'blob_absent' ]] || exit 7
  python3 - <<'PY'
import base64, json, os, pathlib
payload = pathlib.Path(os.environ["OBJECT_DIR"], "terminal-state.json").read_bytes()
if os.environ["MODE"] == "blob_drift":
    payload += b"drift"
print(json.dumps({"sha": os.environ["EXPECTED_BLOB"], "encoding": "base64", "content": base64.b64encode(payload).decode("ascii")}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/trees/'* ]]; then
  python3 - <<'PY'
import json, os
print(json.dumps({"sha": os.environ["EXPECTED_TREE"], "truncated": os.environ["MODE"] == "tree_drift", "tree": [{"path": ".fugue-release-terminal-state.json", "mode": "100644", "type": "blob", "sha": os.environ["EXPECTED_BLOB"]}]}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  python3 - <<'PY'
import json, os
identity = {"name": "Fugue Release Terminal", "email": "release-terminal@fugue.invalid", "date": os.environ["TERMINAL_DATE"]}
parent = os.environ["RESERVATION_OID"] if os.environ["MODE"] != "commit_parent" else "7" * 40
print(json.dumps({"sha": os.environ["EXPECTED_COMMIT"], "message": "fugue release terminal success " + os.environ["SOURCE_RUN_ID"], "tree": {"sha": os.environ["EXPECTED_TREE"]}, "parents": [{"sha": parent, "url": "https://api.github.test/parent"}], "author": identity, "committer": identity}))
PY
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
			t.Fatalf("write success %s mock: %v", name, err)
		}
	}

	type result struct {
		posts int
		log   string
		err   error
	}
	runMaterializer := func(t *testing.T, mode string) result {
		t.Helper()
		caseDir := t.TempDir()
		caseObjectDir := filepath.Join(caseDir, "objects")
		if err := os.Mkdir(caseObjectDir, 0o700); err != nil {
			t.Fatalf("create case object dir: %v", err)
		}
		entries, err := os.ReadDir(objectDir)
		if err != nil {
			t.Fatalf("read source object dir: %v", err)
		}
		for _, entry := range entries {
			data, err := os.ReadFile(filepath.Join(objectDir, entry.Name()))
			if err != nil {
				t.Fatalf("read %s: %v", entry.Name(), err)
			}
			if err := os.WriteFile(filepath.Join(caseObjectDir, entry.Name()), data, 0o600); err != nil {
				t.Fatalf("write case %s: %v", entry.Name(), err)
			}
		}
		logPath := filepath.Join(caseDir, "gh.log")
		command := exec.Command("bash")
		command.Stdin = strings.NewReader(materialize.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"LOG_FILE="+logPath,
			"GITHUB_EVENT_NAME=workflow_dispatch",
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"GITHUB_REPOSITORY=fugue-test/repository",
			"EXPECTED_SHA="+policySHA,
			"RESERVATION_OID="+reservationOID,
			"SOURCE_RUN_ID="+sourceRunID,
			"MATERIALIZER_HEAD="+materializerHead,
			"TERMINAL_DATE="+terminalDate,
			"OBJECT_DIR="+caseObjectDir,
			"EXPECTED_BLOB="+expectedBlob,
			"EXPECTED_TREE="+expectedTree,
			"EXPECTED_COMMIT="+expectedCommit,
			"GH_TOKEN=test-token",
		)
		combined, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read success gh log: %v", err)
		}
		return result{posts: strings.Count(string(log), "--method POST"), log: string(combined), err: runErr}
	}

	for _, mode := range []string{"success", "blob_lost", "tree_lost", "commit_lost"} {
		t.Run(mode, func(t *testing.T) {
			got := runMaterializer(t, mode)
			if got.err != nil || got.posts != 3 || !strings.Contains(got.log, "success finalization objects settled at exact commit "+expectedCommit) {
				t.Fatalf("success object settlement failed: mode=%s err=%v posts=%d log=%q", mode, got.err, got.posts, got.log)
			}
		})
	}
	for _, test := range []struct {
		mode      string
		wantPosts int
	}{
		{mode: "ref_drift", wantPosts: 0},
		{mode: "main_drift", wantPosts: 0},
		{mode: "blob_absent", wantPosts: 1},
		{mode: "blob_drift", wantPosts: 1},
		{mode: "tree_drift", wantPosts: 2},
		{mode: "commit_parent", wantPosts: 3},
	} {
		t.Run(test.mode, func(t *testing.T) {
			got := runMaterializer(t, test.mode)
			if got.err == nil || got.posts != test.wantPosts || strings.Contains(got.log, "success finalization objects settled") {
				t.Fatalf("success materializer accepted drift: mode=%s err=%v posts=%d log=%q", test.mode, got.err, got.posts, got.log)
			}
		})
	}
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slicesSortStrings(keys)
	return keys
}

func slicesSortStrings(values []string) {
	for index := 1; index < len(values); index++ {
		for current := index; current > 0 && values[current] < values[current-1]; current-- {
			values[current], values[current-1] = values[current-1], values[current]
		}
	}
}

func readWorkflowOutputFile(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read workflow output: %v", err)
	}
	outputs := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || outputs[key] != "" {
			t.Fatalf("workflow output is malformed: %q", data)
		}
		outputs[key] = value
	}
	return outputs
}
