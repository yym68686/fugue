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

const rp1TerminalRefCreateWorkflow = "../../.github/workflows/create-control-plane-release-terminal-ref-rp1.yml"

func TestRP1TerminalRefCreateIsHostedSingleMutationAndWriterLast(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalRefCreateWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal ref creator workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "910806ca9d1a2949e38b46e2f7d819d04bf21828f83ef803871fe34c7af967bb")
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
		t.Fatalf("parse RP1 terminal ref creator workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "create-genesis-reservation-ref")
	jobNode := workflowMappingValue(t, jobsNode, "create-genesis-reservation-ref")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("terminal ref creator must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode terminal ref workflow_dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_sha", "materializer_artifact_digest", "materializer_artifact_id",
		"materializer_run_id", "reservation_oid",
	}
	if len(dispatch.Inputs) != len(wantInputs) {
		t.Fatalf("terminal ref creator input inventory drifted: %+v", dispatch.Inputs)
	}
	for _, name := range wantInputs {
		node, exists := dispatch.Inputs[name]
		if !exists {
			t.Fatalf("terminal ref creator input %s is absent", name)
		}
		var input releaseWorkflowDispatchInput
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode terminal ref input %s: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("terminal ref input %s must be required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("terminal ref creator top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["create-genesis-reservation-ref"]
	wantPermissions := map[string]string{"actions": "read", "contents": "write"}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, wantPermissions) {
		t.Fatalf("terminal ref creator job boundary drifted: %+v", job)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"create-genesis-reservation-ref": {Steps: job.Steps},
	}, map[string]string{
		"create-genesis-reservation-ref/Verify exact terminal ref creation authorization":                 "cf29b7b292621fa697edffe6353b35b8ac047cd3bc1a01ef5b97637bed105ccc",
		"create-genesis-reservation-ref/Write terminal ref creation intent evidence":                      "4f3d339bc1d0d811e38b241f4b4a6b3eb2f5e1a22a8d047bfa962dee6f9cd2ae",
		"create-genesis-reservation-ref/Observe unchanged production health before terminal ref creation": "8f054a87d0d0d191aab4c5162a924e394b7e3f9a7b2a20ba898363876f6a8d29",
		"create-genesis-reservation-ref/Create absent terminal ref at verified genesis reservation":       "1e085b3299c0043a04a671207ce261e649893a6ff5e38a93fe4ad4fd852c5366",
	})
	wantSteps := []string{
		"Checkout exact terminal ref creator policy SHA",
		"Verify exact terminal ref creation authorization",
		"Write terminal ref creation intent evidence",
		"Upload terminal ref creation intent evidence",
		"Observe unchanged production health before terminal ref creation",
		"Create absent terminal ref at verified genesis reservation",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("terminal ref creator step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("terminal ref creator step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("terminal ref creator step %q is not valid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("terminal ref creator checkout drifted: %+v", checkout)
	}

	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                 "${{ inputs.expected_sha }}",
		"RESERVATION_OID":              "${{ inputs.reservation_oid }}",
		"MATERIALIZER_RUN_ID":          "${{ inputs.materializer_run_id }}",
		"MATERIALIZER_ARTIFACT_ID":     "${{ inputs.materializer_artifact_id }}",
		"MATERIALIZER_ARTIFACT_DIGEST": "${{ inputs.materializer_artifact_digest }}",
		"HEALTH_URL":                   "${{ vars.FUGUE_CONTROL_PLANE_RP1_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                     "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("terminal ref verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`actual_changes_text="$(git diff --no-renames --name-status "${policy_parent}" "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/create-control-plane-release-terminal-ref-rp1.yml`,
		`A\tinternal/platformsafety/release_terminal_ref_workflow_test.go`,
		`"${terminal_count}" == '0'`,
		`git merge-base --is-ancestor "${run_head}" "${policy_parent}" || exit 1`,
		`"${artifact_digest}" == "${MATERIALIZER_ARTIFACT_DIGEST}"`,
		`materializer_parent="$(git rev-parse "${run_head}^")" || exit 1`,
		`artifact_values="$(python3 - "${evidence_tmp}/content" "${run_head}"`,
		`set(paths) != {root / "intent.json"}`,
		`"genesis-reservation-object-materialized-ref-absent"`,
		`value["transport_status"] != {"blob": 0, "tree": 0, "commit": 0}`,
		`value["repository"] != sys.argv[5]`,
		`value["policy_parent_sha"] != sys.argv[6]`,
		`commit.get("parents") != []`,
		`raw_tree = b"100644 .fugue-release-terminal-state.json\0"`,
		`hashlib.sha1(b"commit "`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("terminal ref verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("terminal ref verifier must not hide command status through process substitution")
	}

	intentUpload := job.Steps[3]
	if intentUpload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		intentUpload.With["if-no-files-found"] != "error" || intentUpload.With["retention-days"] != "90" {
		t.Fatalf("terminal ref intent upload drifted: %+v", intentUpload)
	}
	create := job.Steps[len(job.Steps)-1]
	if create.Env["MATERIALIZER_HEAD"] != "${{ steps.verify.outputs.materializer_head }}" {
		t.Fatalf("terminal ref creator must bind the verified materializer head: %+v", create.Env)
	}
	for _, required := range []string{
		`"${terminal_count}" == '0'`,
		`"${parent_count}" == '0'`,
		`"${materializer_head}" == "${MATERIALIZER_HEAD}"`,
		`git merge-base --is-ancestor "${materializer_head}" "${policy_parent}" || exit 1`,
		`"${artifact_digest}" == "${MATERIALIZER_ARTIFACT_DIGEST}"`,
		`-f "ref=${terminal_ref}" -f "sha=${RESERVATION_OID}"`,
		`"${observed_ref}" == "${RESERVATION_OID}"`,
		`"${observed_ref}" == 'absent'`,
		`"${settled_ref}" == "${RESERVATION_OID}"`,
		`"${baseline_before}" =~ ^[0-9a-f]{40}$`,
	} {
		if !strings.Contains(create.Run, required) {
			t.Fatalf("terminal ref creator final writer must contain %q", required)
		}
	}
	if strings.Count(create.Run, "gh api --method POST") != 1 ||
		strings.Count(create.Run, `"repos/${GITHUB_REPOSITORY}/git/refs"`) != 1 ||
		strings.Contains(create.Run, "GITHUB_OUTPUT") {
		t.Fatalf("terminal ref creator mutation inventory drifted:\n%s", create.Run)
	}
	settledGuard := `[[ "${settled_ref}" == "${RESERVATION_OID}" ]] || exit 1`
	settledIndex := strings.LastIndex(create.Run, settledGuard)
	if settledIndex < 0 {
		t.Fatal("terminal ref creator exact settlement guard is absent")
	}
	fallibleTail := strings.TrimSpace(create.Run[settledIndex+len(settledGuard):])
	wantTail := `printf 'terminal ref settled at exact genesis reservation %s\n' "${settled_ref}" || true`
	if fallibleTail != wantTail {
		t.Fatalf("terminal ref exact settlement must be the final fallible operation: got tail %q want %q", fallibleTail, wantTail)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH",
		"--method PUT", "--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		"force=true", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("terminal ref creator contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP1TerminalRefCreateReadbackSettlesOneMutation(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp1TerminalRefCreateWorkflow)
	if err != nil {
		t.Fatalf("read RP1 terminal ref creator workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP1 terminal ref creator workflow: %v", err)
	}
	steps := workflow.Jobs["create-genesis-reservation-ref"].Steps
	if len(steps) == 0 {
		t.Fatal("terminal ref creator steps are absent")
	}
	create := steps[len(steps)-1]
	if create.Name != "Create absent terminal ref at verified genesis reservation" || create.Run == "" {
		t.Fatalf("terminal ref creator final step drifted: %+v", create)
	}

	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatalf("create terminal ref fixture repo: %v", err)
	}
	runGit := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Fugue Terminal Ref Test",
			"GIT_AUTHOR_EMAIL=terminal-ref-test@fugue.invalid",
			"GIT_AUTHOR_DATE=2026-07-19T00:00:00Z",
			"GIT_COMMITTER_NAME=Fugue Terminal Ref Test",
			"GIT_COMMITTER_EMAIL=terminal-ref-test@fugue.invalid",
			"GIT_COMMITTER_DATE=2026-07-19T00:00:00Z",
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%q", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("init", "--quiet")
	if err := os.WriteFile(filepath.Join(repo, "parent"), []byte("parent\n"), 0o600); err != nil {
		t.Fatalf("write parent fixture: %v", err)
	}
	runGit("add", "parent")
	runGit("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "parent")
	materializerHead := runGit("rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "candidate"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatalf("write candidate fixture: %v", err)
	}
	runGit("add", "candidate")
	runGit("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "candidate")
	policySHA := runGit("rev-parse", "HEAD")
	unrelatedHead := runGit("commit-tree", runGit("write-tree"), "-m", "unrelated materializer")
	reservationOID := strings.Repeat("a", 40)
	baselineOID := strings.Repeat("b", 40)
	artifactDigest := "sha256:" + strings.Repeat("c", 64)

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create terminal ref mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
if [[ "${arguments}" == *'--method POST'*'/git/refs'* ]]; then
  case "${MODE}" in
    success)
      printf '%s\n' "${RESERVATION_OID}" >"${STATE_FILE}"
      printf '%s\t%s\tcommit\n' 'refs/heads/fugue-control-plane-release-terminal-state' "${RESERVATION_OID}"
      ;;
    mutation_lost)
      printf '%s\n' "${RESERVATION_OID}" >"${STATE_FILE}"
      exit 7
      ;;
    wrong_response)
      printf '%s\n' "${RESERVATION_OID}" >"${STATE_FILE}"
      printf '%s\t%s\tcommit\n' 'refs/heads/wrong' "${RESERVATION_OID}"
      ;;
    no_settle)
      exit 7
      ;;
    divergent)
      printf '%040d\n' 3 >"${STATE_FILE}"
      exit 7
      ;;
    unreadable)
      printf '%s\n' "${RESERVATION_OID}" >"${STATE_FILE}"
      exit 7
      ;;
    *) exit 98 ;;
  esac
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/main'* ]]; then
  printf '%s\n' "${GITHUB_SHA}"
  exit 0
fi
if [[ "${arguments}" == *'/git/ref/heads/fugue-control-plane-release-baseline'* ]]; then
  printf '%s\n' "${BASELINE_OID}"
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-terminal-state'* ]]; then
  if [[ "${arguments}" != *'then "absent"'* ]]; then printf '0\n'; exit 0; fi
  if [[ "${MODE}" == 'unreadable' && -s "${STATE_FILE}" ]]; then exit 7; fi
  [[ -s "${STATE_FILE}" ]] && cat "${STATE_FILE}" || printf 'absent\n'
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  printf '%s\t%s\t0\n' "${RESERVATION_OID}" "${TREE_OID}"
  exit 0
fi
if [[ "${arguments}" == *'/actions/runs/'*'/artifacts'* ]]; then
  printf '%s\n' "${MATERIALIZER_ARTIFACT_DIGEST}"
  exit 0
fi
if [[ "${arguments}" == *'/actions/runs/'* ]]; then
  printf 'completed\tsuccess\t1\t%s\t%s\n' "${MATERIALIZER_HEAD}" '.github/workflows/write-control-plane-release-terminal-rp1.yml'
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
	curlMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' '{"status":"ok"}'
`
	for name, source := range map[string]string{"gh": ghMock, "timeout": timeoutMock, "curl": curlMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write terminal ref %s mock: %v", name, err)
		}
	}

	type result struct {
		posts int
		state string
		log   string
		err   error
	}
	runCreator := func(t *testing.T, mode string) result {
		t.Helper()
		caseDir := t.TempDir()
		statePath := filepath.Join(caseDir, "state")
		logPath := filepath.Join(caseDir, "gh.log")
		materializerRunHead := materializerHead
		if mode == "non_ancestor" {
			materializerRunHead = unrelatedHead
		}
		command := exec.Command("bash")
		command.Dir = repo
		command.Stdin = strings.NewReader(create.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"STATE_FILE="+statePath,
			"LOG_FILE="+logPath,
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"EXPECTED_SHA="+policySHA,
			"RESERVATION_OID="+reservationOID,
			"TREE_OID="+strings.Repeat("d", 40),
			"BASELINE_OID="+baselineOID,
			"MATERIALIZER_RUN_ID=29667919251",
			"MATERIALIZER_ARTIFACT_ID=8436338714",
			"MATERIALIZER_ARTIFACT_DIGEST="+artifactDigest,
			"MATERIALIZER_HEAD="+materializerRunHead,
			"GITHUB_REPOSITORY=fugue-test/repository",
			"HEALTH_URL=https://api.example.test/healthz",
			"GH_TOKEN=test-token",
		)
		combined, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read terminal ref gh log: %v", err)
		}
		state := "absent"
		if value, err := os.ReadFile(statePath); err == nil {
			state = strings.TrimSpace(string(value))
		} else if !os.IsNotExist(err) {
			t.Fatalf("read terminal ref state: %v", err)
		}
		return result{posts: strings.Count(string(log), "--method POST"), state: state, log: string(combined), err: runErr}
	}

	for _, mode := range []string{"success", "mutation_lost", "wrong_response"} {
		t.Run(mode, func(t *testing.T) {
			got := runCreator(t, mode)
			if got.err != nil || got.posts != 1 || got.state != reservationOID ||
				!strings.Contains(got.log, "terminal ref settled at exact genesis reservation") {
				t.Fatalf("terminal ref did not settle: mode=%s err=%v posts=%d state=%q log=%q", mode, got.err, got.posts, got.state, got.log)
			}
		})
	}
	for _, mode := range []string{"no_settle", "divergent", "unreadable"} {
		t.Run(mode, func(t *testing.T) {
			got := runCreator(t, mode)
			if got.err == nil || got.posts != 1 || strings.Contains(got.log, "terminal ref settled at exact genesis reservation") {
				t.Fatalf("terminal ref creator accepted an unsettled mutation: mode=%s err=%v posts=%d state=%q log=%q", mode, got.err, got.posts, got.state, got.log)
			}
		})
	}
	t.Run("non_ancestor", func(t *testing.T) {
		got := runCreator(t, "non_ancestor")
		if got.err == nil || got.posts != 0 || got.state != "absent" ||
			strings.Contains(got.log, "terminal ref settled at exact genesis reservation") {
			t.Fatalf("terminal ref creator accepted a non-ancestor materializer head: err=%v posts=%d state=%q log=%q", got.err, got.posts, got.state, got.log)
		}
	})
}
