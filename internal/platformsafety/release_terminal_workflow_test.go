package platformsafety

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/releaseterminal"

	"gopkg.in/yaml.v3"
)

const rp1TerminalGenesisWorkflow = "../../.github/workflows/write-control-plane-release-terminal-rp1.yml"

func TestRP1TerminalGenesisMaterializerIsHostedRefFreeAndSourceBound(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalGenesisWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal genesis workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "6f6ef07f229fd3bdec0436038b052d3f6bf07822d0f6ac0269db319b80fe323a")
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
		t.Fatalf("parse RP1 terminal genesis workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "materialize-genesis-reservation")
	jobNode := workflowMappingValue(t, jobsNode, "materialize-genesis-reservation")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("terminal genesis materializer must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode terminal workflow_dispatch: %v", err)
	}
	if len(dispatch.Inputs) != 1 {
		t.Fatalf("terminal materializer input inventory drifted: %+v", dispatch.Inputs)
	}
	inputNode, ok := dispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("terminal materializer expected_sha input is absent")
	}
	var input releaseWorkflowDispatchInput
	if err := inputNode.Decode(&input); err != nil {
		t.Fatalf("decode expected_sha input: %v", err)
	}
	if !input.Required || input.Type != "string" || input.Default != nil {
		t.Fatalf("expected_sha must be a required string without default: %+v", input)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("terminal genesis top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["materialize-genesis-reservation"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 15 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"contents": "write"}) {
		t.Fatalf("terminal genesis job boundary drifted: %+v", job)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"materialize-genesis-reservation": {Steps: job.Steps},
	}, map[string]string{
		"materialize-genesis-reservation/Verify exact genesis materialization authorization":                     "57ff9680836ea16c06849b7a8aa7edb7b2f1e4e73d4b8f8d699b04bdafc57a04",
		"materialize-genesis-reservation/Write genesis materialization intent evidence":                          "7bd16b0c0eb217ed282e15fe87e2c565f5878d35c71d21954b3a5fb983226e2e",
		"materialize-genesis-reservation/Observe unchanged production health before immutable object write":      "cebde1718b247d6d5ca0bad326c5b44aa1695d28905a303aab6f42af26c0cfc9",
		"materialize-genesis-reservation/Materialize canonical genesis reservation objects without moving a ref": "ef9776d3c57145beace07e864de5285e524614678072d47d30e538847563a7e0",
		"materialize-genesis-reservation/Write genesis materialization result evidence":                          "bc1fb83fccc4a769d203e4be4a3d15d034a06d293e78218cdd595179732bac4b",
	})

	wantSteps := []string{
		"Checkout exact terminal-writer policy SHA",
		"Verify exact genesis materialization authorization",
		"Write genesis materialization intent evidence",
		"Upload genesis materialization intent evidence",
		"Observe unchanged production health before immutable object write",
		"Materialize canonical genesis reservation objects without moving a ref",
		"Write genesis materialization result evidence",
		"Upload genesis materialization result evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("terminal genesis step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("terminal genesis step %d drifted: %+v", index, step)
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("terminal genesis checkout drifted: %+v", checkout)
	}
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA": "${{ inputs.expected_sha }}",
		"HEALTH_URL":   "${{ vars.FUGUE_CONTROL_PLANE_RP1_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":     "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("terminal genesis verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`actual_changes_text="$(git diff --no-renames --name-status "${policy_parent}" "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/write-control-plane-release-terminal-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_workflow_test.go`,
		`"repos/${GITHUB_REPOSITORY}/git/ref/heads/main"`,
		`"${remote_main}" == "${EXPECTED_SHA}"`,
		`"${state_count}" == '0'`,
		`terminal_date=%s`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("terminal genesis verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("terminal genesis verifier must not hide source command status through process substitution")
	}

	materialize := job.Steps[5]
	wantMaterializeEnv := map[string]string{
		"EXPECTED_SHA":  "${{ inputs.expected_sha }}",
		"TERMINAL_DATE": "${{ steps.verify.outputs.terminal_date }}",
		"GH_TOKEN":      "${{ github.token }}",
	}
	if materialize.ID != "materialize" || materialize.Uses != "" || materialize.Run == "" ||
		!reflect.DeepEqual(materialize.Env, wantMaterializeEnv) {
		t.Fatalf("terminal genesis materializer execution boundary drifted: %+v", materialize)
	}
	for _, required := range []string{
		`"terminal_mode": "reservation"`,
		`"source_workflow": ".github/workflows/write-control-plane-release-terminal-rp1.yml"`,
		`"previous_terminal_state_oid": "absent"`,
		`git hash-object -w --stdin`,
		`git mktree`,
		`git hash-object -t commit --stdin`,
		`"parents": []`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${blob_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${tree_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${reservation_oid}"`,
		`response.get("parents") != []`,
		`"${after_count}" == '0'`,
		`"${after_main}" == "${EXPECTED_SHA}"`,
	} {
		if !strings.Contains(materialize.Run, required) {
			t.Fatalf("terminal genesis materializer must contain %q", required)
		}
	}
	if strings.Count(materialize.Run, "gh api --method POST") != 3 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/blobs"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/trees"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/commits"`) != 1 ||
		strings.Count(materialize.Run, `git/matching-refs/heads/fugue-control-plane-release-terminal-state`) != 1 {
		t.Fatalf("terminal genesis object/ref API inventory drifted:\n%s", materialize.Run)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		`"repos/${GITHUB_REPOSITORY}/git/refs`, "force=", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("terminal genesis workflow contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP1TerminalGenesisMaterializerObjectReadbackSettlement(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalGenesisWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal genesis workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal genesis workflow: %v", err)
	}
	var materialize releaseWorkflowStep
	for _, step := range workflow.Jobs["materialize-genesis-reservation"].Steps {
		if step.Name == "Materialize canonical genesis reservation objects without moving a ref" {
			materialize = step
		}
	}
	if materialize.Run == "" {
		t.Fatal("terminal genesis materializer run body is absent")
	}

	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatalf("create terminal fixture repo: %v", err)
	}
	runGit := func(input string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		command.Stdin = strings.NewReader(input)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%q", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("", "init", "--quiet")

	const runID = "29670000001"
	const terminalDate = "2026-07-19T00:00:00Z"
	policySHA := strings.Repeat("a", 40)
	document := releaseterminal.Document{
		SchemaVersion:            releaseterminal.SchemaVersion,
		CertificateKind:          releaseterminal.CertificateKindReservation,
		TerminalMode:             releaseterminal.ModeReservation,
		SourceRunID:              runID,
		SourceRunAttempt:         1,
		SourceHeadSHA:            policySHA,
		SourceWorkflow:           releaseterminal.WorkflowTerminalWriter,
		SourceConclusion:         "in_progress",
		PreviousTerminalStateOID: releaseterminal.AbsentOID,
	}
	payload, err := releaseterminal.Encode(document)
	if err != nil {
		t.Fatalf("encode expected terminal reservation: %v", err)
	}
	expectedBlob := runGit(string(payload), "hash-object", "-w", "--stdin")
	expectedTree := runGit(fmt.Sprintf("100644 blob %s\t%s\n", expectedBlob, releaseterminal.CarrierPayloadPath), "mktree")
	instant, err := time.Parse(time.RFC3339, terminalDate)
	if err != nil {
		t.Fatalf("parse fixture terminal date: %v", err)
	}
	message := "fugue release terminal reservation " + runID
	identity := fmt.Sprintf("Fugue Release Terminal <release-terminal@fugue.invalid> %d +0000", instant.Unix())
	commitContent := fmt.Sprintf("tree %s\nauthor %s\ncommitter %s\n\n%s", expectedTree, identity, identity, message)
	expectedReservation := runGit(commitContent, "hash-object", "-t", "commit", "--stdin")
	if withTrailingLF := runGit(commitContent+"\n", "hash-object", "-t", "commit", "--stdin"); withTrailingLF == expectedReservation {
		t.Fatal("terminal fixture does not distinguish a trailing commit-message LF")
	}

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create terminal mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
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
  sha="${arguments##*/}"
  python3 - "${sha}" <<'PY'
import base64, json, os, subprocess, sys
content = subprocess.check_output(["git", "cat-file", "blob", sys.argv[1]])
if os.environ["MODE"] == "blob_drift":
    content += b"drift"
print(json.dumps({"sha": sys.argv[1], "encoding": "base64", "content": base64.b64encode(content).decode("ascii")}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/trees/'* ]]; then
  sha="${arguments##*/}"
  python3 - "${sha}" <<'PY'
import json, os, subprocess, sys
line = subprocess.check_output(["git", "ls-tree", sys.argv[1]], text=True).rstrip("\n")
metadata, path = line.split("\t", 1)
mode, object_type, object_sha = metadata.split()
print(json.dumps({"sha": sys.argv[1], "truncated": os.environ["MODE"] == "tree_drift", "tree": [{"path": path, "mode": mode, "type": object_type, "sha": object_sha}]}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  sha="${arguments##*/}"
  [[ "${sha}" == "${EXPECTED_RESERVATION_OID}" ]] || exit 7
  python3 - <<'PY'
import json, os
identity = {"name": "Fugue Release Terminal", "email": "release-terminal@fugue.invalid", "date": os.environ["TERMINAL_DATE"]}
parents = [] if os.environ["MODE"] != "commit_parent" else [{"sha": "b" * 40}]
print(json.dumps({"sha": os.environ["EXPECTED_RESERVATION_OID"], "message": "fugue release terminal reservation " + os.environ["GITHUB_RUN_ID"], "tree": {"sha": os.environ["EXPECTED_TREE_SHA"]}, "parents": parents, "author": identity, "committer": identity}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-terminal-state'* ]]; then
  [[ "${MODE}" != 'ref_appears' ]] && printf '0\n' || printf '1\n'
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/main'* ]]; then
  [[ "${MODE}" != 'main_drift' ]] && printf '%s\n' "${EXPECTED_SHA}" || printf '%040d\n' 2
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
			t.Fatalf("write terminal %s mock: %v", name, err)
		}
	}

	type result struct {
		posts  int
		output string
		log    string
		err    error
	}
	runMaterializer := func(t *testing.T, mode string) result {
		t.Helper()
		caseDir := t.TempDir()
		outputPath := filepath.Join(caseDir, "github-output")
		logPath := filepath.Join(caseDir, "gh.log")
		command := exec.Command("bash")
		command.Dir = repo
		command.Stdin = strings.NewReader(materialize.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"LOG_FILE="+logPath,
			"GITHUB_EVENT_NAME=workflow_dispatch",
			"GITHUB_RUN_ID="+runID,
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"EXPECTED_SHA="+policySHA,
			"TERMINAL_DATE="+terminalDate,
			"EXPECTED_RESERVATION_OID="+expectedReservation,
			"EXPECTED_TREE_SHA="+expectedTree,
			"GITHUB_REPOSITORY=fugue-test/repository",
			"GITHUB_OUTPUT="+outputPath,
			"GH_TOKEN=test-token",
		)
		combined, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read terminal gh log: %v", err)
		}
		published := ""
		if value, err := os.ReadFile(outputPath); err == nil {
			published = string(value)
		} else if !os.IsNotExist(err) {
			t.Fatalf("read terminal output: %v", err)
		}
		return result{posts: strings.Count(string(log), "--method POST"), output: published, log: string(combined), err: runErr}
	}

	for _, mode := range []string{"success", "blob_lost", "tree_lost", "commit_lost"} {
		t.Run(mode, func(t *testing.T) {
			got := runMaterializer(t, mode)
			if got.err != nil || got.posts != 3 {
				t.Fatalf("terminal object settlement failed: mode=%s err=%v posts=%d output=%q log=%q", mode, got.err, got.posts, got.output, got.log)
			}
			outputs := map[string]string{}
			for _, line := range strings.Split(strings.TrimSpace(got.output), "\n") {
				key, value, ok := strings.Cut(line, "=")
				if !ok || outputs[key] != "" {
					t.Fatalf("terminal output is malformed: %q", got.output)
				}
				outputs[key] = value
			}
			if len(outputs) != 6 || outputs["payload_blob_sha"] != expectedBlob ||
				outputs["payload_tree_sha"] != expectedTree || outputs["reservation_oid"] != expectedReservation {
				t.Fatalf("terminal output topology drifted: mode=%s output=%q", mode, got.output)
			}
			wantStatus := map[string]string{"blob_transport_status": "0", "tree_transport_status": "0", "commit_transport_status": "0"}
			if mode != "success" {
				wantStatus[strings.TrimSuffix(mode, "_lost")+"_transport_status"] = "7"
			}
			for key, want := range wantStatus {
				if outputs[key] != want {
					t.Fatalf("terminal transport status drifted: mode=%s key=%s got=%q want=%q", mode, key, outputs[key], want)
				}
			}
			storedPayload := runGit("", "cat-file", "blob", outputs["payload_blob_sha"])
			resolved, err := releaseterminal.Decode([]byte(storedPayload + "\n"))
			if err != nil || !reflect.DeepEqual(resolved, document) {
				t.Fatalf("materialized terminal payload is not canonical: document=%#v err=%v", resolved, err)
			}
		})
	}
	t.Run("blob absent after failed transport", func(t *testing.T) {
		got := runMaterializer(t, "blob_absent")
		if got.err == nil || got.posts != 1 || got.output != "" {
			t.Fatalf("terminal writer did not fail closed for absent blob: err=%v posts=%d output=%q log=%q", got.err, got.posts, got.output, got.log)
		}
	})
	for _, test := range []struct {
		mode      string
		wantPosts int
	}{
		{mode: "blob_drift", wantPosts: 1},
		{mode: "tree_drift", wantPosts: 2},
		{mode: "commit_parent", wantPosts: 3},
		{mode: "ref_appears", wantPosts: 3},
		{mode: "main_drift", wantPosts: 3},
	} {
		t.Run(test.mode, func(t *testing.T) {
			got := runMaterializer(t, test.mode)
			if got.err == nil || got.posts != test.wantPosts || got.output != "" {
				t.Fatalf("terminal writer accepted drift: mode=%s err=%v posts=%d output=%q log=%q", test.mode, got.err, got.posts, got.output, got.log)
			}
		})
	}
}
