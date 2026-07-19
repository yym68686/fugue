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

const rp2TerminalWatchdogReportWorkflow = "../../.github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml"

func TestRP2TerminalWatchdogReportIsHostedReadOnly(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp2TerminalWatchdogReportWorkflow)
	if err != nil {
		t.Fatalf("read RP2 terminal watchdog report workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "b47bd049375e624aad8944df9a2fa462a402090d736a0a33bcbe576def1d6329")
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
		t.Fatalf("parse RP2 terminal watchdog report workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "observe-terminal-watchdog")
	jobNode := workflowMappingValue(t, jobsNode, "observe-terminal-watchdog")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 2 {
		t.Fatalf("terminal watchdog report triggers drifted: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode terminal watchdog report dispatch: %v", err)
	}
	wantInputs := []string{"expected_sha", "expected_terminal_mode", "expected_terminal_oid"}
	if got := sortedMapKeys(dispatch.Inputs); !reflect.DeepEqual(got, wantInputs) {
		t.Fatalf("terminal watchdog report inputs = %v, want %v", got, wantInputs)
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
	var schedule []struct {
		Cron string `yaml:"cron"`
	}
	scheduleNode := workflow.On["schedule"]
	if err := scheduleNode.Decode(&schedule); err != nil {
		t.Fatalf("decode terminal watchdog schedule: %v", err)
	}
	if len(schedule) != 1 || schedule[0].Cron != "17 * * * *" {
		t.Fatalf("terminal watchdog schedule drifted: %+v", schedule)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("terminal watchdog report top-level boundary drifted: %+v", workflow)
	}
	job := workflow.Jobs["observe-terminal-watchdog"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 15 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"actions": "read", "contents": "read"}) {
		t.Fatalf("terminal watchdog report job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact terminal watchdog policy SHA",
		"Setup Go",
		"Verify exact report-only watchdog authorization",
		"Classify stable terminal state without mutation",
		"Upload report-only terminal watchdog evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("terminal watchdog report step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("terminal watchdog report step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("terminal watchdog report step %q is invalid bash: %v output=%q", name, err, output)
			}
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("terminal watchdog report checkout drifted: %+v", checkout)
	}
	setup := job.Steps[1]
	if setup.Uses != "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16" ||
		setup.With["go-version-file"] != "go.mod" || setup.With["cache"] != "false" {
		t.Fatalf("terminal watchdog report Go setup drifted: %+v", setup)
	}
	verify := job.Steps[2]
	if verify.ID != "authorize" {
		t.Fatalf("terminal watchdog verifier ID drifted: %+v", verify)
	}
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":           "${{ inputs.expected_sha }}",
		"EXPECTED_TERMINAL_OID":  "${{ inputs.expected_terminal_oid }}",
		"EXPECTED_TERMINAL_MODE": "${{ inputs.expected_terminal_mode }}",
		"GH_TOKEN":               "${{ github.token }}",
		"GITHUB_TOKEN":           "${{ github.token }}",
	}
	if !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("terminal watchdog verifier environment drifted: %+v", verify.Env)
	}
	for _, required := range []string{
		`workflow_dispatch)`,
		`schedule)`,
		`-z "${EXPECTED_SHA}" && -z "${EXPECTED_TERMINAL_OID}" && -z "${EXPECTED_TERMINAL_MODE}"`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_commit="$(git log --format='%H' -n 1 -- "${workflow_path}" "${test_path}")" || exit 1`,
		`git merge-base --is-ancestor "${policy_commit}" "${GITHUB_SHA}" || exit 1`,
		`"$(git log --format='%H' -n 1 -- "${policy_path}")" == "${policy_commit}"`,
		`policy_identity="$(git rev-list --parents -n 1 "${policy_commit}")" || exit 1`,
		`M\t.github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml`,
		`M\tinternal/platformsafety/release_terminal_watchdog_report_workflow_test.go`,
		`"${main_head}" == "${GITHUB_SHA}"`,
		`fugue-rp2-scheduled-terminal-reader.json`,
		`"${terminal_oid}" == "${effective_terminal_oid}"`,
		`expected_terminal_oid=%s`,
		`expected_terminal_mode=%s`,
		`cmd/fugue-release-terminal-read/main.go`,
		`internal/releaseterminal/resolver.go`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("terminal watchdog verifier must contain %q", required)
		}
	}
	classify := job.Steps[3]
	wantClassifyEnv := map[string]string{
		"EXPECTED_TERMINAL_OID":  "${{ steps.authorize.outputs.expected_terminal_oid }}",
		"EXPECTED_TERMINAL_MODE": "${{ steps.authorize.outputs.expected_terminal_mode }}",
		"GITHUB_TOKEN":           "${{ github.token }}",
	}
	if !reflect.DeepEqual(classify.Env, wantClassifyEnv) {
		t.Fatalf("terminal watchdog classifier environment drifted: %+v", classify.Env)
	}
	readerSource, err := os.ReadFile("../../cmd/fugue-release-terminal-read/main.go")
	if err != nil {
		t.Fatalf("read terminal reader source: %v", err)
	}
	if !strings.Contains(string(readerSource), `os.Getenv("GITHUB_TOKEN")`) {
		t.Fatal("terminal watchdog classifier token must match the production reader contract")
	}
	for _, required := range []string{
		`go run ./cmd/fugue-release-terminal-read`,
		`"freeze_candidate_missing", "freeze_required"`,
		`"settled_frozen", "none"`,
		`"settled_success", "none"`,
		`"reservation_active", "none"`,
		`"reservation_stale_success", "freeze_required"`,
		`"reservation_terminal_failure", "freeze_required"`,
		`"indeterminate_reader_failure"`,
		`"indeterminate_source_run"`,
		`"indeterminate_source_identity"`,
		`"ref_mutation_attempted": False`,
		`"object_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
		`"git_history_rewritten": False`,
	} {
		if !strings.Contains(classify.Run, required) {
			t.Fatalf("terminal watchdog classifier must contain %q", required)
		}
	}
	upload := job.Steps[4]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("terminal watchdog evidence upload drifted: %+v", upload)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ",
		"git push", "git update-ref", "git commit-tree", "updateRefs", "createRef", "deleteRef", "force=true",
		"--method POST", "--method PATCH", "--method PUT", "--method DELETE", "contents: write", "fugue app ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("terminal watchdog report contains out-of-scope capability %q", forbidden)
		}
	}
	if strings.Count(source, "actions/upload-artifact@") != 1 {
		t.Fatal("terminal watchdog report must upload exactly one evidence artifact")
	}
}

func TestRP2TerminalWatchdogReportPolicyIdentitySurvivesUnrelatedDescendant(t *testing.T) {
	t.Parallel()

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

	runGit("init", "--quiet")
	policyPaths := []string{
		".github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml",
		"internal/platformsafety/release_terminal_watchdog_report_workflow_test.go",
	}
	for _, path := range policyPaths {
		write(path, "published\n")
	}
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "published")
	write(".github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml", "forward repair\n")
	write("internal/platformsafety/release_terminal_watchdog_report_workflow_test.go", "forward repair\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "forward repair")
	policyCommit := strings.TrimSpace(runGit("rev-parse", "HEAD"))

	write("README.md", "unrelated descendant\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "unrelated descendant")
	executionCommit := strings.TrimSpace(runGit("rev-parse", "HEAD"))
	if got := strings.TrimSpace(runGit("log", "--format=%H", "-n", "1", "--", policyPaths[0], policyPaths[1])); got != policyCommit {
		t.Fatalf("latest joint policy commit = %q, want %q", got, policyCommit)
	}
	for _, path := range policyPaths {
		if got := strings.TrimSpace(runGit("log", "--format=%H", "-n", "1", "--", path)); got != policyCommit {
			t.Fatalf("latest commit for %s = %q, want %q", path, got, policyCommit)
		}
	}
	runGit("merge-base", "--is-ancestor", policyCommit, executionCommit)

	want := "M\t.github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml\n" +
		"M\tinternal/platformsafety/release_terminal_watchdog_report_workflow_test.go\n"
	if got := runGit("diff", "--no-renames", "--name-status", policyCommit+"^", policyCommit); got != want {
		t.Fatalf("policy changed-file status = %q, want %q", got, want)
	}

	write(policyPaths[0], "independent drift\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "independent policy drift")
	driftCommit := strings.TrimSpace(runGit("rev-parse", "HEAD"))
	if got := strings.TrimSpace(runGit("log", "--format=%H", "-n", "1", "--", policyPaths[0], policyPaths[1])); got != driftCommit {
		t.Fatalf("latest joint commit after drift = %q, want %q", got, driftCommit)
	}
	if got := strings.TrimSpace(runGit("log", "--format=%H", "-n", "1", "--", policyPaths[1])); got == driftCommit {
		t.Fatalf("independently unchanged policy file unexpectedly resolved to drift commit %q", got)
	}
	if got := runGit("diff", "--no-renames", "--name-status", driftCommit+"^", driftCommit); got == want {
		t.Fatalf("one-file policy drift unexpectedly matched joint policy status: %q", got)
	}
}

func TestRP2TerminalWatchdogScheduledAuthorizationHarness(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp2TerminalWatchdogReportWorkflow)
	if err != nil {
		t.Fatalf("read RP2 terminal watchdog report workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP2 terminal watchdog report workflow: %v", err)
	}
	var authorize releaseWorkflowStep
	for _, step := range workflow.Jobs["observe-terminal-watchdog"].Steps {
		if step.Name == "Verify exact report-only watchdog authorization" {
			authorize = step
		}
	}
	if authorize.Run == "" {
		t.Fatal("terminal watchdog authorization step is absent")
	}
	if authorize.Env["GITHUB_TOKEN"] != "${{ github.token }}" || authorize.Env["GH_TOKEN"] != "${{ github.token }}" {
		t.Fatalf("terminal watchdog authorization token contract drifted: %+v", authorize.Env)
	}

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
	policyPaths := []string{
		".github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml",
		"internal/platformsafety/release_terminal_watchdog_report_workflow_test.go",
	}
	runGit("init", "--quiet")
	for _, path := range policyPaths {
		write(path, "published\n")
	}
	write("cmd/fugue-release-terminal-read/main.go", "package main\n")
	write("internal/releaseterminal/resolver.go", "package releaseterminal\n")
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "published")
	for _, path := range policyPaths {
		write(path, "scheduled report only\n")
	}
	runGit("add", ".")
	runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", "scheduled")
	executionSHA := strings.TrimSpace(runGit("rev-parse", "HEAD"))

	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *'/git/ref/heads/main'*) printf '%s\n' "${EXPECTED_MAIN_SHA}" ;;
  *'/git/matching-refs/heads/fugue-control-plane-release-terminal-state'*)
    if [[ "${MODE}" == 'schedule_absent' ]]; then printf '%s\n' 'absent'; else printf '%s\n' 'c5355438136ac167cf921928ceb86306a52b42e3'; fi
    ;;
  *) exit 97 ;;
esac
`
	goMock := `#!/usr/bin/env bash
set -euo pipefail
case "${MODE}" in
  manual) exit 91 ;;
  schedule_absent) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"absent"}' ;;
  schedule_drift) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"present","object_oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","document":{"terminal_mode":"frozen"}}' ;;
  *) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"present","object_oid":"c5355438136ac167cf921928ceb86306a52b42e3","document":{"terminal_mode":"frozen"}}' ;;
esac
`
	timeoutMock := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 125
shift
exec "$@"
`
	for name, source := range map[string]string{"gh": ghMock, "go": goMock, "timeout": timeoutMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write %s mock: %v", name, err)
		}
	}
	bashEnv := filepath.Join(t.TempDir(), "bash-env")
	bashEnvSource := `actual_changes=($'M\t.github/workflows/observe-control-plane-release-terminal-watchdog-rp2.yml' $'M\tinternal/platformsafety/release_terminal_watchdog_report_workflow_test.go')
mapfile() { return 0; }
`
	if err := os.WriteFile(bashEnv, []byte(bashEnvSource), 0o600); err != nil {
		t.Fatalf("write Bash 3 mapfile compatibility shim: %v", err)
	}

	cases := map[string]struct {
		event   string
		oid     string
		mode    string
		success bool
		want    string
	}{
		"schedule_frozen": {"schedule", "", "", true, "expected_terminal_oid=c5355438136ac167cf921928ceb86306a52b42e3\nexpected_terminal_mode=frozen\n"},
		"schedule_absent": {"schedule", "", "", true, "expected_terminal_oid=absent\nexpected_terminal_mode=absent\n"},
		"schedule_drift":  {"schedule", "", "", false, ""},
		"manual":          {"workflow_dispatch", "c5355438136ac167cf921928ceb86306a52b42e3", "frozen", true, "expected_terminal_oid=c5355438136ac167cf921928ceb86306a52b42e3\nexpected_terminal_mode=frozen\n"},
	}
	for name, test := range cases {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "output")
			command := exec.Command("bash")
			command.Dir = repository
			command.Stdin = strings.NewReader(authorize.Run)
			expectedSHA := ""
			if test.event == "workflow_dispatch" {
				expectedSHA = executionSHA
			}
			command.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"BASH_ENV="+bashEnv,
				"MODE="+name,
				"EXPECTED_MAIN_SHA="+executionSHA,
				"EXPECTED_SHA="+expectedSHA,
				"EXPECTED_TERMINAL_OID="+test.oid,
				"EXPECTED_TERMINAL_MODE="+test.mode,
				"GITHUB_EVENT_NAME="+test.event,
				"GITHUB_REF=refs/heads/main",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA="+executionSHA,
				"GITHUB_REPOSITORY=test/repository",
				"GITHUB_TOKEN=test-token",
				"RUNNER_TEMP="+t.TempDir(),
				"GITHUB_OUTPUT="+outputPath,
			)
			output, runErr := command.CombinedOutput()
			if !test.success {
				if runErr == nil {
					t.Fatalf("expected authorization failure, output=%s", output)
				}
				return
			}
			if runErr != nil {
				t.Fatalf("authorization failed: %v output=%s", runErr, output)
			}
			got, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("read authorization output: %v", err)
			}
			if string(got) != test.want {
				t.Fatalf("authorization output = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRP2TerminalWatchdogReportClassificationHarness(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(rp2TerminalWatchdogReportWorkflow)
	if err != nil {
		t.Fatalf("read RP2 terminal watchdog report workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP2 terminal watchdog report workflow: %v", err)
	}
	var classify releaseWorkflowStep
	for _, step := range workflow.Jobs["observe-terminal-watchdog"].Steps {
		if step.Name == "Classify stable terminal state without mutation" {
			classify = step
		}
	}
	if classify.Run == "" {
		t.Fatal("terminal watchdog classifier step is absent")
	}

	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	goMock := `#!/usr/bin/env bash
set -euo pipefail
case "${MODE}" in
  reader_error) exit 7 ;;
  absent) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"absent"}' ;;
  frozen) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"present","object_oid":"c5355438136ac167cf921928ceb86306a52b42e3","document":{"terminal_mode":"frozen","source_run_id":"29677982238","source_run_attempt":1,"source_head_sha":"5446508ecce13c60f1f6b9c5459ba089b9ea26c9","source_workflow":".github/workflows/materialize-control-plane-release-terminal-next-reservation-rp1.yml","source_conclusion":"success"}}' ;;
  success) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"present","object_oid":"8686c3e867dbdbd98bfc77afb374d88007b045d7","document":{"terminal_mode":"success","source_run_id":"100","source_run_attempt":1,"source_head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_workflow":".github/workflows/deploy-control-plane-v2.yml","source_conclusion":"success"}}' ;;
  *) printf '%s\n' '{"schema_version":1,"ref":"refs/heads/fugue-control-plane-release-terminal-state","state":"present","object_oid":"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820","document":{"terminal_mode":"reservation","source_run_id":"123","source_run_attempt":1,"source_head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_workflow":".github/workflows/deploy-control-plane-v2.yml","source_conclusion":"in_progress"}}' ;;
esac
`
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
case "${MODE}" in
  source_error) exit 7 ;;
  identity_drift) printf '%s\t%s\t%s\t%s\t%s\t%s\n' 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' 'in_progress' '' '1' 'workflow_dispatch' '.github/workflows/deploy-control-plane-v2.yml' ;;
  reservation_active) printf '%s\t%s\t%s\t%s\t%s\t%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' 'in_progress' '' '1' 'workflow_dispatch' '.github/workflows/deploy-control-plane-v2.yml' ;;
  reservation_stale_success) printf '%s\t%s\t%s\t%s\t%s\t%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' 'completed' 'success' '1' 'workflow_dispatch' '.github/workflows/deploy-control-plane-v2.yml' ;;
  reservation_failure) printf '%s\t%s\t%s\t%s\t%s\t%s\n' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' 'completed' 'failure' '1' 'workflow_dispatch' '.github/workflows/deploy-control-plane-v2.yml' ;;
  *) exit 97 ;;
esac
`
	timeoutMock := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 125
shift
exec "$@"
`
	for name, source := range map[string]string{"go": goMock, "gh": ghMock, "timeout": timeoutMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write %s mock: %v", name, err)
		}
	}

	type expected struct {
		oid            string
		mode           string
		classification string
		action         string
		success        bool
	}
	cases := map[string]expected{
		"absent":                    {"absent", "absent", "freeze_candidate_missing", "freeze_required", true},
		"frozen":                    {"c5355438136ac167cf921928ceb86306a52b42e3", "frozen", "settled_frozen", "none", true},
		"success":                   {"8686c3e867dbdbd98bfc77afb374d88007b045d7", "success", "settled_success", "none", true},
		"reservation_active":        {"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820", "reservation", "reservation_active", "none", true},
		"reservation_stale_success": {"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820", "reservation", "reservation_stale_success", "freeze_required", true},
		"reservation_failure":       {"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820", "reservation", "reservation_terminal_failure", "freeze_required", true},
		"source_error":              {"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820", "reservation", "indeterminate_source_run", "manual_evidence_required", true},
		"identity_drift":            {"5dc68e4e7a61b791dd460f4b8fbbe0dbd0f7f820", "reservation", "indeterminate_source_identity", "manual_evidence_required", true},
		"reader_error":              {"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "indeterminate", "indeterminate_reader_failure", "manual_evidence_required", true},
		"expectation_drift":         {"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "frozen", "", "", false},
	}
	for mode, want := range cases {
		mode, want := mode, want
		t.Run(mode, func(t *testing.T) {
			runnerTemp := filepath.Join(root, "runner-"+mode)
			if err := os.Mkdir(runnerTemp, 0o700); err != nil {
				t.Fatalf("create runner temp: %v", err)
			}
			command := exec.Command("bash")
			command.Stdin = strings.NewReader(classify.Run)
			command.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"MODE="+mode,
				"RUNNER_TEMP="+runnerTemp,
				"GITHUB_STEP_SUMMARY="+filepath.Join(runnerTemp, "summary"),
				"GITHUB_REPOSITORY=test/repository",
				"GITHUB_RUN_ID=9001",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA="+strings.Repeat("c", 40),
				"EXPECTED_TERMINAL_OID="+want.oid,
				"EXPECTED_TERMINAL_MODE="+want.mode,
				"GITHUB_TOKEN=test-token",
			)
			output, runErr := command.CombinedOutput()
			if !want.success {
				if runErr == nil {
					t.Fatalf("expected failure, output=%s", output)
				}
				return
			}
			if runErr != nil {
				t.Fatalf("classifier failed: %v output=%s", runErr, output)
			}
			evidencePath := filepath.Join(runnerTemp, "fugue-rp2-terminal-watchdog-report", "observation.json")
			data, err := os.ReadFile(evidencePath)
			if err != nil {
				t.Fatalf("read observation: %v", err)
			}
			var value map[string]any
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatalf("decode observation: %v", err)
			}
			if value["classification"] != want.classification || value["action"] != want.action ||
				value["terminal_oid"] != want.oid || value["terminal_mode"] != want.mode {
				t.Fatalf("observation drifted: %+v", value)
			}
			wantKeys := []string{
				"action", "classification", "cluster_mutation_attempted", "expected_terminal_mode",
				"expected_terminal_oid", "git_history_rewritten", "object_mutation_attempted", "observed_state",
				"policy_sha", "reader_status", "recorded_at", "ref_mutation_attempted", "repository",
				"run_attempt", "run_id", "schema_version", "source_document_conclusion", "source_run_conclusion",
				"source_run_id", "source_run_status", "terminal_mode", "terminal_oid", "terminal_ref", "workflow",
			}
			if len(value) != len(wantKeys) {
				t.Fatalf("observation key count drifted: %+v", value)
			}
			for _, key := range wantKeys {
				if _, ok := value[key]; !ok {
					t.Fatalf("observation key %q is absent: %+v", key, value)
				}
			}
			for _, field := range []string{"ref_mutation_attempted", "object_mutation_attempted", "cluster_mutation_attempted", "git_history_rewritten"} {
				if value[field] != false {
					t.Fatalf("%s must be false: %+v", field, value)
				}
			}
		})
	}
}
