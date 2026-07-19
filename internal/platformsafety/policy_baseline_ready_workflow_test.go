package platformsafety

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const policyBaselineReadyActivationWorkflow = "../../.github/workflows/materialize-policy-baseline-ready-sla0b.yml"

type policyBaselineReadyWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	RunsOn          string                `yaml:"runs-on"`
	TimeoutMinutes  int                   `yaml:"timeout-minutes"`
	Environment     string                `yaml:"environment"`
	Permissions     map[string]string     `yaml:"permissions"`
	Outputs         map[string]string     `yaml:"outputs"`
	ContinueOnError bool                  `yaml:"continue-on-error"`
	Steps           []releaseWorkflowStep `yaml:"steps"`
}

func TestPolicyBaselineReadyActivationWorkflowContract(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(policyBaselineReadyActivationWorkflow)
	if err != nil {
		t.Fatalf("read policy baseline activation workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "92adca2ec2172cc2c2fbdd0fe6703183d330d8a7a64909eb08f1b01d7b08768e")
	var workflow struct {
		On          map[string]yaml.Node                      `yaml:"on"`
		Permissions map[string]string                         `yaml:"permissions"`
		Jobs        map[string]policyBaselineReadyWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse policy baseline activation workflow: %v", err)
	}
	root := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, root, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, root, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, root, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "consume", "materialize")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, jobsNode, "materialize"),
		"environment", "outputs", "permissions", "runs-on", "steps", "timeout-minutes")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, jobsNode, "consume"),
		"environment", "needs", "permissions", "runs-on", "steps", "timeout-minutes")

	dispatchNode, ok := workflow.On["repository_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("policy baseline activation must be repository_dispatch-only: %+v", workflow.On)
	}
	var dispatch struct {
		Types []string `yaml:"types"`
	}
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode repository_dispatch trigger: %v", err)
	}
	if !reflect.DeepEqual(dispatch.Types, []string{"fugue_policy_checkpoint_complete"}) {
		t.Fatalf("repository_dispatch types drifted: %v", dispatch.Types)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 2 {
		t.Fatalf("policy baseline activation top-level boundary drifted: %+v", workflow)
	}

	wantPermissions := map[string]string{"actions": "read", "contents": "read"}
	materialize := workflow.Jobs["materialize"]
	if materialize.RunsOn != "ubuntu-latest" || materialize.TimeoutMinutes != 10 ||
		materialize.Environment != "production" || len(materialize.Needs) != 0 ||
		materialize.ContinueOnError || !reflect.DeepEqual(materialize.Permissions, wantPermissions) {
		t.Fatalf("materialize job boundary drifted: %+v", materialize)
	}
	wantOutputs := map[string]string{
		"artifact_name":   "${{ steps.materialize.outputs.artifact_name }}",
		"artifact_digest": "${{ steps.normalize.outputs.artifact_digest }}",
		"baseline_sha256": "${{ steps.materialize.outputs.baseline_sha256 }}",
		"expected_sha":    "${{ steps.materialize.outputs.expected_sha }}",
		"expected_tree":   "${{ steps.materialize.outputs.expected_tree }}",
	}
	if !reflect.DeepEqual(materialize.Outputs, wantOutputs) {
		t.Fatalf("materialize job outputs drifted: %+v", materialize.Outputs)
	}
	wantMaterializeSteps := []string{
		"Checkout exact policy checkpoint SHA",
		"Setup Go",
		"Verify exact automatic activation and decode attested inputs",
		"Materialize canonical exact-main baseline readiness",
		"Upload exact-main baseline readiness artifact",
		"Normalize uploaded artifact digest",
	}
	assertPolicyBaselineReadySteps(t, materialize.Steps, wantMaterializeSteps)
	checkout := materialize.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.event.client_payload.expected_sha }}" ||
		checkout.With["fetch-depth"] != "0" || checkout.With["persist-credentials"] != "false" {
		t.Fatalf("policy baseline checkout drifted: %+v", checkout)
	}
	setup := materialize.Steps[1]
	if setup.Uses != "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16" ||
		setup.With["go-version-file"] != "go.mod" || setup.With["cache"] != "false" {
		t.Fatalf("policy baseline setup-go drifted: %+v", setup)
	}
	verify := materialize.Steps[2]
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'repository_dispatch'`,
		`"${EVENT_ACTION}" == 'fugue_policy_checkpoint_complete'`,
		`"${GITHUB_ACTOR}" == "${REPOSITORY_OWNER}"`,
		`"${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		`"$(git rev-parse HEAD^{tree})" == "${EXPECTED_TREE}"`,
		`activation_commit="$(git log --format='%H' -n 1 -- "${workflow_path}" "${test_path}")"`,
		`materializer_commit="$(git log --format='%H' -n 1 -- "${materializer_path}" "${materializer_test_path}")"`,
		`git merge-base --is-ancestor "${activation_commit}" "${EXPECTED_SHA}"`,
		`git merge-base --is-ancestor "${materializer_commit}" "${EXPECTED_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/ref/heads/main"`,
		`base64.b64decode(payload, validate=True)`,
		`os.O_WRONLY | os.O_CREAT | os.O_EXCL`,
		`attested input files must be distinct`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("activation verifier must contain %q", required)
		}
	}
	materializer := materialize.Steps[3]
	for _, required := range []string{
		"go run ./cmd/fugue-policy-baseline-ready-materialize",
		`--checkpoint-digest "${CHECKPOINT_DIGEST}"`,
		`--environment-digest "${ENVIRONMENT_DIGEST}"`,
		`--timing-digest "${TIMING_DIGEST}"`,
		`--expected-sha "${EXPECTED_SHA}"`,
		`--expected-tree "${EXPECTED_TREE}"`,
		`"production_write": False`,
		`"ref_mutation_attempted": False`,
		`"object_mutation_attempted": False`,
		`"cluster_mutation_attempted": False`,
	} {
		if !strings.Contains(materializer.Run, required) {
			t.Fatalf("materializer step must contain %q", required)
		}
	}
	upload := materialize.Steps[4]
	if upload.ID != "upload" || upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("policy baseline upload drifted: %+v", upload)
	}
	normalize := materialize.Steps[5]
	if normalize.ID != "normalize" || !reflect.DeepEqual(normalize.Env, map[string]string{
		"RAW_ARTIFACT_DIGEST": "${{ steps.upload.outputs.artifact-digest }}",
	}) || !strings.Contains(normalize.Run, `"${RAW_ARTIFACT_DIGEST}" =~ ^[0-9a-f]{64}$`) ||
		!strings.Contains(normalize.Run, `artifact_digest=sha256:%s`) {
		t.Fatalf("artifact digest normalization drifted: %+v", normalize)
	}
	normalizeOutput := filepath.Join(t.TempDir(), "output")
	normalizeCommand := exec.Command("bash")
	normalizeCommand.Stdin = strings.NewReader(normalize.Run)
	normalizeCommand.Env = append(os.Environ(),
		"RAW_ARTIFACT_DIGEST="+strings.Repeat("a", 64),
		"GITHUB_OUTPUT="+normalizeOutput,
	)
	if output, err := normalizeCommand.CombinedOutput(); err != nil {
		t.Fatalf("normalize real bare upload-artifact digest: %v output=%s", err, output)
	}
	normalized, err := os.ReadFile(normalizeOutput)
	if err != nil {
		t.Fatalf("read normalized artifact digest: %v", err)
	}
	if string(normalized) != "artifact_digest=sha256:"+strings.Repeat("a", 64)+"\n" {
		t.Fatalf("normalized artifact digest drifted: %q", normalized)
	}
	rejectPrefixed := exec.Command("bash")
	rejectPrefixed.Stdin = strings.NewReader(normalize.Run)
	rejectPrefixed.Env = append(os.Environ(),
		"RAW_ARTIFACT_DIGEST=sha256:"+strings.Repeat("a", 64),
		"GITHUB_OUTPUT="+filepath.Join(t.TempDir(), "output"),
	)
	if output, err := rejectPrefixed.CombinedOutput(); err == nil {
		t.Fatalf("normalizer unexpectedly accepted a prefixed action output: %s", output)
	}

	consume := workflow.Jobs["consume"]
	if consume.RunsOn != "ubuntu-latest" || consume.TimeoutMinutes != 5 || consume.Environment != "production" ||
		consume.ContinueOnError || !reflect.DeepEqual([]string(consume.Needs), []string{"materialize"}) ||
		!reflect.DeepEqual(consume.Permissions, wantPermissions) || len(consume.Outputs) != 0 {
		t.Fatalf("consume job boundary drifted: %+v", consume)
	}
	wantConsumeSteps := []string{
		"Download exact-main baseline readiness artifact",
		"Consume and verify exact-main baseline readiness artifact",
	}
	assertPolicyBaselineReadySteps(t, consume.Steps, wantConsumeSteps)
	download := consume.Steps[0]
	if download.Uses != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" ||
		download.With["name"] != "${{ needs.materialize.outputs.artifact_name }}" ||
		download.With["path"] != "${{ runner.temp }}/fugue-policy-baseline-ready-sla0b-consume" {
		t.Fatalf("policy baseline download drifted: %+v", download)
	}
	consumeStep := consume.Steps[1]
	for _, required := range []string{
		`"${ARTIFACT_DIGEST}" =~ ^sha256:[0-9a-f]{64}$`,
		`"${BASELINE_SHA256}" =~ ^sha256:[0-9a-f]{64}$`,
		`files != ["BASELINE_READY.json", "activation.json"]`,
		`"repos/${GITHUB_REPOSITORY}/git/ref/heads/main"`,
		`activation.get("baseline_ready_sha256") != baseline_digest`,
		`("production_write", "ref_mutation_attempted", "object_mutation_attempted", "cluster_mutation_attempted")`,
	} {
		if !strings.Contains(consumeStep.Run, required) {
			t.Fatalf("artifact consumer must contain %q", required)
		}
	}

	source := string(data)
	for _, forbidden := range []string{
		"workflow_dispatch", "schedule:", "cron:", "push:", "self-hosted", "${{ secrets.",
		"contents: write", "KUBECONFIG", "kubectl ", "helm ", "ssh ", "docker ", "fugue app ",
		"git push", "git update-ref", "git commit-tree", "updateRefs", "createRef", "deleteRef",
		"--method POST", "--method PATCH", "--method PUT", "--method DELETE", "force=true",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("policy baseline activation contains out-of-scope capability %q", forbidden)
		}
	}
	if strings.Count(source, "actions/upload-artifact@") != 1 || strings.Count(source, "actions/download-artifact@") != 1 ||
		strings.Count(source, "go run ./cmd/fugue-policy-baseline-ready-materialize") != 1 {
		t.Fatal("policy baseline activation must have one materializer, upload, and consumption path")
	}
}

func TestPolicyBaselineReadyActivationRealHistoryAndMaterializerContract(t *testing.T) {
	t.Parallel()

	workflowData, err := os.ReadFile(policyBaselineReadyActivationWorkflow)
	if err != nil {
		t.Fatalf("read policy baseline activation workflow: %v", err)
	}
	verifyStep := policyBaselineReadyStep(t, workflowData, "materialize", "Verify exact automatic activation and decode attested inputs")
	materializeStep := policyBaselineReadyStep(t, workflowData, "materialize", "Materialize canonical exact-main baseline readiness")

	repository := filepath.Join(t.TempDir(), "repository")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatalf("create fixture repository: %v", err)
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
	write := func(name string, data []byte) {
		t.Helper()
		path := filepath.Join(repository, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create %s parent: %v", name, err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	commit := func(message string) string {
		t.Helper()
		runGit("add", ".")
		runGit("-c", "user.name=Fugue Test", "-c", "user.email=fugue-test@example.invalid", "commit", "--quiet", "-m", message)
		return strings.TrimSpace(runGit("rev-parse", "HEAD"))
	}

	runGit("init", "--quiet")
	write("go.mod", []byte("module fixture\n\ngo 1.25\n"))
	write("README.md", []byte("base\n"))
	commit("base")
	for _, path := range []string{
		"cmd/fugue-policy-baseline-ready-materialize/main.go",
		"cmd/fugue-policy-baseline-ready-materialize/main_test.go",
	} {
		data, err := os.ReadFile(filepath.Join("../..", filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read published materializer %s: %v", path, err)
		}
		write(path, data)
	}
	commit("materializer")
	write(".github/workflows/materialize-policy-baseline-ready-sla0b.yml", []byte("activation workflow\n"))
	write("internal/platformsafety/policy_baseline_ready_workflow_test.go", []byte("activation test\n"))
	activationCommit := commit("activation")
	write("README.md", []byte("unrelated descendant\n"))
	executionCommit := commit("unrelated descendant")
	executionTree := strings.TrimSpace(runGit("rev-parse", "HEAD^{tree}"))
	if activationCommit == executionCommit {
		t.Fatal("real history fixture did not create an unrelated descendant")
	}

	checkpoint, environment, timing := policyBaselineReadyValidInputs(t, executionCommit, executionTree)
	runnerTemp := filepath.Join(t.TempDir(), "runner")
	if err := os.Mkdir(runnerTemp, 0o700); err != nil {
		t.Fatalf("create runner temp: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	writeExecutable := func(name, source string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write %s mock: %v", name, err)
		}
	}
	writeExecutable("gh", "#!/usr/bin/env bash\nset -euo pipefail\n[[ \"${1:-}\" == api ]]\nprintf '%s\\n' \"${MOCK_MAIN_SHA:-${EXPECTED_SHA}}\"\n")
	writeExecutable("timeout", "#!/usr/bin/env bash\nset -euo pipefail\n[[ \"${1:-}\" == --kill-after=* ]] && shift\n[[ \"${1:-}\" == 10s ]]\nshift\nexec \"$@\"\n")

	verifyOutput := filepath.Join(runnerTemp, "verify-output")
	baseEnv := map[string]string{
		"CHECKPOINT_B64":     base64.StdEncoding.EncodeToString(checkpoint),
		"CHECKPOINT_DIGEST":  policyBaselineReadyDigest(checkpoint),
		"ENVIRONMENT_B64":    base64.StdEncoding.EncodeToString(environment),
		"ENVIRONMENT_DIGEST": policyBaselineReadyDigest(environment),
		"EVENT_ACTION":       "fugue_policy_checkpoint_complete",
		"EXPECTED_SHA":       executionCommit,
		"EXPECTED_TREE":      executionTree,
		"GH_TOKEN":           "fixture-token",
		"GITHUB_ACTOR":       "owner",
		"GITHUB_EVENT_NAME":  "repository_dispatch",
		"GITHUB_OUTPUT":      verifyOutput,
		"GITHUB_REF":         "refs/heads/main",
		"GITHUB_REPOSITORY":  "owner/repository",
		"GITHUB_RUN_ATTEMPT": "1",
		"GITHUB_SHA":         executionCommit,
		"REPOSITORY_OWNER":   "owner",
		"RUNNER_TEMP":        runnerTemp,
		"TIMING_B64":         base64.StdEncoding.EncodeToString(timing),
		"TIMING_DIGEST":      policyBaselineReadyDigest(timing),
	}
	if output, err := runPolicyBaselineReadyBash(repository, verifyStep.Run, bin, baseEnv); err != nil {
		t.Fatalf("real-history activation verifier failed: %v output=%s", err, output)
	}
	verifyValues := policyBaselineReadyOutputValues(t, verifyOutput)
	evidenceDir := verifyValues["evidence_dir"]
	for _, name := range []string{"checkpoint.json", "environment.json", "timing.json"} {
		info, err := os.Lstat(filepath.Join(evidenceDir, name))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("decoded evidence %s is not a regular file: info=%v err=%v", name, info, err)
		}
	}

	materializeOutput := filepath.Join(runnerTemp, "materialize-output")
	materializeEnv := map[string]string{
		"CHECKPOINT_DIGEST":  policyBaselineReadyDigest(checkpoint),
		"ENVIRONMENT_DIGEST": policyBaselineReadyDigest(environment),
		"EVIDENCE_DIR":       evidenceDir,
		"EXPECTED_SHA":       executionCommit,
		"EXPECTED_TREE":      executionTree,
		"GITHUB_OUTPUT":      materializeOutput,
		"GITHUB_REPOSITORY":  "owner/repository",
		"GITHUB_RUN_ATTEMPT": "1",
		"GITHUB_RUN_ID":      "9001",
		"RUNNER_TEMP":        runnerTemp,
		"TIMING_DIGEST":      policyBaselineReadyDigest(timing),
	}
	if output, err := runPolicyBaselineReadyBash(repository, materializeStep.Run, bin, materializeEnv); err != nil {
		t.Fatalf("real materializer workflow step failed: %v output=%s", err, output)
	}
	materializeValues := policyBaselineReadyOutputValues(t, materializeOutput)
	if materializeValues["expected_sha"] != executionCommit || materializeValues["expected_tree"] != executionTree ||
		!strings.HasPrefix(materializeValues["baseline_sha256"], "sha256:") ||
		materializeValues["artifact_name"] != "fugue-policy-baseline-ready-"+executionCommit+"-9001-1" {
		t.Fatalf("materializer outputs drifted: %+v", materializeValues)
	}
	baselinePath := filepath.Join(runnerTemp, "fugue-policy-baseline-ready-sla0b-artifact", "BASELINE_READY.json")
	baselineData, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("read materialized BASELINE_READY: %v", err)
	}
	if got := policyBaselineReadyDigest(baselineData); got != materializeValues["baseline_sha256"] {
		t.Fatalf("materialized digest = %s, want %s", got, materializeValues["baseline_sha256"])
	}
	var baseline map[string]any
	if err := json.Unmarshal(baselineData, &baseline); err != nil {
		t.Fatalf("decode materialized BASELINE_READY: %v", err)
	}
	if baseline["status"] != "BASELINE_READY" || baseline["base_sha"] != executionCommit || baseline["base_tree"] != executionTree {
		t.Fatalf("materialized BASELINE_READY identity drifted: %+v", baseline)
	}

	negativeCases := map[string]func(map[string]string){
		"bad_base64": func(values map[string]string) {
			values["CHECKPOINT_B64"] = "not+base64==="
		},
		"bad_json": func(values map[string]string) {
			values["CHECKPOINT_B64"] = base64.StdEncoding.EncodeToString([]byte("not-json"))
		},
		"bad_digest": func(values map[string]string) {
			values["CHECKPOINT_DIGEST"] = "sha256:not-a-digest"
		},
		"wrong_actor": func(values map[string]string) {
			values["GITHUB_ACTOR"] = "not-owner"
		},
		"stale_main": func(values map[string]string) {
			values["MOCK_MAIN_SHA"] = strings.Repeat("f", 40)
		},
	}
	for name, mutate := range negativeCases {
		values := make(map[string]string, len(baseEnv))
		for key, value := range baseEnv {
			values[key] = value
		}
		caseTemp := filepath.Join(t.TempDir(), name)
		if err := os.Mkdir(caseTemp, 0o700); err != nil {
			t.Fatalf("create %s temp: %v", name, err)
		}
		values["RUNNER_TEMP"] = caseTemp
		values["GITHUB_OUTPUT"] = filepath.Join(caseTemp, "output")
		mutate(values)
		if output, err := runPolicyBaselineReadyBash(repository, verifyStep.Run, bin, values); err == nil {
			t.Fatalf("negative case %s unexpectedly passed: output=%s", name, output)
		}
	}

	badCheckpoint := append([]byte(nil), checkpoint...)
	var badCheckpointValue map[string]any
	if err := json.Unmarshal(badCheckpoint, &badCheckpointValue); err != nil {
		t.Fatalf("decode checkpoint for negative source gate: %v", err)
	}
	badCheckpointValue["status"] = "not-complete"
	badCheckpoint, err = json.Marshal(badCheckpointValue)
	if err != nil {
		t.Fatalf("marshal negative checkpoint: %v", err)
	}
	badCheckpoint = append(badCheckpoint, '\n')
	badEvidenceDir := filepath.Join(t.TempDir(), "bad-source-gate")
	if err := os.Mkdir(badEvidenceDir, 0o700); err != nil {
		t.Fatalf("create bad source gate evidence dir: %v", err)
	}
	for name, data := range map[string][]byte{"checkpoint.json": badCheckpoint, "environment.json": environment, "timing.json": timing} {
		if err := os.WriteFile(filepath.Join(badEvidenceDir, name), data, 0o600); err != nil {
			t.Fatalf("write bad source gate %s: %v", name, err)
		}
	}
	badMaterializeTemp := filepath.Join(t.TempDir(), "bad-materialize")
	if err := os.Mkdir(badMaterializeTemp, 0o700); err != nil {
		t.Fatalf("create bad materialize temp: %v", err)
	}
	badMaterializeEnv := make(map[string]string, len(materializeEnv))
	for key, value := range materializeEnv {
		badMaterializeEnv[key] = value
	}
	badMaterializeEnv["CHECKPOINT_DIGEST"] = policyBaselineReadyDigest(badCheckpoint)
	badMaterializeEnv["EVIDENCE_DIR"] = badEvidenceDir
	badMaterializeEnv["GITHUB_OUTPUT"] = filepath.Join(badMaterializeTemp, "output")
	badMaterializeEnv["RUNNER_TEMP"] = badMaterializeTemp
	if output, err := runPolicyBaselineReadyBash(repository, materializeStep.Run, bin, badMaterializeEnv); err == nil {
		t.Fatalf("invalid source checkpoint unexpectedly materialized: output=%s", output)
	}

	write(".github/workflows/materialize-policy-baseline-ready-sla0b.yml", []byte("one-file drift\n"))
	driftCommit := commit("one-file activation drift")
	driftTree := strings.TrimSpace(runGit("rev-parse", "HEAD^{tree}"))
	driftEnv := make(map[string]string, len(baseEnv))
	for key, value := range baseEnv {
		driftEnv[key] = value
	}
	driftEnv["EXPECTED_SHA"] = driftCommit
	driftEnv["EXPECTED_TREE"] = driftTree
	driftEnv["GITHUB_SHA"] = driftCommit
	driftEnv["GITHUB_OUTPUT"] = filepath.Join(runnerTemp, "drift-output")
	if output, err := runPolicyBaselineReadyBash(repository, verifyStep.Run, bin, driftEnv); err == nil {
		t.Fatalf("one-file activation drift unexpectedly passed: output=%s", output)
	}
}

func assertPolicyBaselineReadySteps(t *testing.T, steps []releaseWorkflowStep, names []string) {
	t.Helper()
	if len(steps) != len(names) {
		t.Fatalf("step inventory length = %d, want %d: %+v", len(steps), len(names), steps)
	}
	for index, name := range names {
		step := steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("step %d drifted: %+v", index, step)
		}
		if step.Run != "" {
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(step.Run)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("step %q is invalid bash: %v output=%s", name, err, output)
			}
		}
	}
}

func policyBaselineReadyStep(t *testing.T, data []byte, jobName, stepName string) releaseWorkflowStep {
	t.Helper()
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse policy baseline activation workflow: %v", err)
	}
	for _, step := range workflow.Jobs[jobName].Steps {
		if step.Name == stepName {
			return step
		}
	}
	t.Fatalf("step %q/%q is absent", jobName, stepName)
	return releaseWorkflowStep{}
}

func policyBaselineReadyValidInputs(t *testing.T, sha, tree string) ([]byte, []byte, []byte) {
	t.Helper()
	checkpoint := map[string]any{
		"checkpoint": "fixture-checkpoint", "status": "checkpoint_complete",
		"commit":             map[string]any{"sha": sha, "tree": tree},
		"full_gate":          map[string]any{"result": "PASS", "candidate_tree": tree},
		"independent_review": map[string]any{"result": "APPROVE", "ended": true},
		"planner_gate":       map[string]any{"result": "unknown", "domains": []any{}, "exit_code": 2, "zero_runtime_domains": true},
		"publication":        map[string]any{"main_sha": sha, "ci_result": "success", "build_result": "not_required_zero_runtime", "formal_result": "success", "production_write": false},
		"observation":        map[string]any{"result": "PASS", "samples": 5, "main_stable": true, "actions_and_artifacts_stable": true, "api_health": "ok", "central_coredns": "5/5 1/1 1"},
		"recovery_proof":     map[string]any{"result": "PASS", "remote_ref_mutation": false, "production_mutation": false},
	}
	environment := map[string]any{
		"schema_version": 1, "status": "ENVIRONMENT_READY", "qualified_at_utc": "2026-07-19T16:11:31Z",
		"environment_class": "fixture-loopback", "disk_available_kib": 11 * 1024 * 1024,
		"disk_minimum_kib": 10 * 1024 * 1024, "loopback_shared_udp_tcp_port": 56050,
		"residual_processes":   []any{},
		"toolchain":            map[string]any{"go": "go1.25.7", "git": "2.50.1", "helm": "v4.2.0", "gh": "2.57.0"},
		"canonical_user_files": map[string]any{"result": "PASS"},
	}
	timing := map[string]any{
		"checkpoint": "fixture-checkpoint", "candidate_sha": sha, "candidate_tree": tree,
		"safety_status": "PASS", "timing_status": "TIMING_NONCOMPLIANT",
	}
	encode := func(name string, value any) []byte {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		return append(data, '\n')
	}
	return encode("checkpoint", checkpoint), encode("environment", environment), encode("timing", timing)
}

func policyBaselineReadyDigest(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func runPolicyBaselineReadyBash(repository, script, bin string, values map[string]string) ([]byte, error) {
	command := exec.Command("bash")
	command.Dir = repository
	command.Stdin = strings.NewReader(script)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		command.Env = append(command.Env, key+"="+values[key])
	}
	return command.CombinedOutput()
}

func policyBaselineReadyOutputValues(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read GitHub output %s: %v", path, err)
	}
	values := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		name, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
			t.Fatalf("invalid GitHub output line %q", line)
		}
		values[name] = value
	}
	return values
}
