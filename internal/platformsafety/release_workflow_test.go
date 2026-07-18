package platformsafety

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflowNeeds []string

func (n *workflowNeeds) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		*n = workflowNeeds{node.Value}
		return nil
	case yaml.SequenceNode:
		values := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode {
				return fmt.Errorf("workflow need must be a scalar")
			}
			values = append(values, item.Value)
		}
		*n = values
		return nil
	default:
		return fmt.Errorf("workflow needs must be a scalar or sequence")
	}
}

type releaseWorkflow struct {
	On          releaseWorkflowTriggers       `yaml:"on"`
	Permissions map[string]string             `yaml:"permissions"`
	Jobs        map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowTriggers struct {
	Push             releaseWorkflowPushTrigger      `yaml:"push"`
	WorkflowDispatch *releaseWorkflowDispatchTrigger `yaml:"workflow_dispatch"`
}

type releaseWorkflowPushTrigger struct {
	Paths []string `yaml:"paths"`
}

type releaseWorkflowDispatchTrigger struct {
	Inputs map[string]yaml.Node `yaml:"inputs"`
}

type releaseWorkflowDispatchInput struct {
	Required bool       `yaml:"required"`
	Type     string     `yaml:"type"`
	Default  *yaml.Node `yaml:"default"`
}

type releaseWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	If              string                `yaml:"if"`
	Outputs         map[string]string     `yaml:"outputs"`
	Permissions     map[string]string     `yaml:"permissions"`
	ContinueOnError bool                  `yaml:"continue-on-error"`
	Steps           []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	ID              string            `yaml:"id"`
	Name            string            `yaml:"name"`
	If              string            `yaml:"if"`
	Uses            string            `yaml:"uses"`
	Env             map[string]string `yaml:"env"`
	With            map[string]string `yaml:"with"`
	Run             string            `yaml:"run"`
	Shell           string            `yaml:"shell"`
	ContinueOnError bool              `yaml:"continue-on-error"`
}

func workflowDocumentMapping(t *testing.T, data []byte) *yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatalf("parse workflow YAML node: %v", err)
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatalf("workflow must contain exactly one mapping document: %+v", document)
	}
	return document.Content[0]
}

func assertWorkflowSourceDigest(t *testing.T, data []byte, expected string) {
	t.Helper()
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != expected {
		t.Fatalf("workflow source drifted: got sha256:%s want sha256:%s", actual, expected)
	}
}

func workflowMappingValue(t *testing.T, mapping *yaml.Node, key string) *yaml.Node {
	t.Helper()
	if mapping == nil || mapping.Kind != yaml.MappingNode || len(mapping.Content)%2 != 0 {
		t.Fatalf("workflow node for %q is not a mapping", key)
	}
	for index := 0; index < len(mapping.Content); index += 2 {
		candidate := mapping.Content[index]
		if candidate.Kind == yaml.ScalarNode && candidate.Value == key {
			return mapping.Content[index+1]
		}
	}
	t.Fatalf("workflow mapping key %q is absent", key)
	return nil
}

func assertWorkflowMappingKeys(t *testing.T, mapping *yaml.Node, expected ...string) {
	t.Helper()
	if mapping == nil || mapping.Kind != yaml.MappingNode || len(mapping.Content)%2 != 0 {
		t.Fatalf("workflow node is not a mapping: %+v", mapping)
	}
	actual := make([]string, 0, len(mapping.Content)/2)
	for index := 0; index < len(mapping.Content); index += 2 {
		key := mapping.Content[index]
		if key.Kind != yaml.ScalarNode {
			t.Fatalf("workflow mapping key must be scalar: %+v", key)
		}
		actual = append(actual, key.Value)
	}
	sort.Strings(actual)
	want := append([]string(nil), expected...)
	sort.Strings(want)
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("workflow mapping key inventory drifted: got %v want %v", actual, want)
	}
}

type workflowJobNodeContract struct {
	Keys     []string
	StepKeys [][]string
}

func assertWorkflowJobNodeContracts(t *testing.T, jobs *yaml.Node, contracts map[string]workflowJobNodeContract) {
	t.Helper()
	jobNames := make([]string, 0, len(contracts))
	for jobName := range contracts {
		jobNames = append(jobNames, jobName)
	}
	assertWorkflowMappingKeys(t, jobs, jobNames...)

	for jobName, contract := range contracts {
		job := workflowMappingValue(t, jobs, jobName)
		assertWorkflowMappingKeys(t, job, contract.Keys...)
		steps := workflowMappingValue(t, job, "steps")
		if steps.Kind != yaml.SequenceNode || len(steps.Content) != len(contract.StepKeys) {
			t.Fatalf("workflow job %s step inventory drifted: got %d steps want %d", jobName, len(steps.Content), len(contract.StepKeys))
		}
		for index, step := range steps.Content {
			assertWorkflowMappingKeys(t, step, contract.StepKeys[index]...)
		}
	}
}

func assertWorkflowRunDigests(t *testing.T, jobs map[string]releaseWorkflowJob, expected map[string]string) {
	t.Helper()
	seen := make(map[string]struct{}, len(expected))
	for jobName, job := range jobs {
		for _, step := range job.Steps {
			if step.Run == "" {
				continue
			}
			key := jobName + "/" + step.Name
			want, ok := expected[key]
			if !ok {
				t.Fatalf("workflow contains an unreviewed run body %q", key)
			}
			if _, duplicate := seen[key]; duplicate {
				t.Fatalf("workflow contains duplicate run body %q", key)
			}
			seen[key] = struct{}{}
			got := fmt.Sprintf("%x", sha256.Sum256([]byte(step.Run)))
			if got != want {
				t.Fatalf("workflow run body %q drifted: got sha256:%s want sha256:%s", key, got, want)
			}
		}
	}
	if len(seen) != len(expected) {
		missing := make([]string, 0, len(expected)-len(seen))
		for key := range expected {
			if _, ok := seen[key]; !ok {
				missing = append(missing, key)
			}
		}
		sort.Strings(missing)
		t.Fatalf("workflow reviewed run bodies are absent: %v", missing)
	}
}

func TestRP0MetadataObjectMaterializationIsHostedEvidenceBoundAndRefFree(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "migrate-control-plane-release-baseline-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 migration workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "a154d22eeecf2344f37ee4ff36b3462803f911795dcb4333d01d7966cf56d874")
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
		t.Fatalf("parse RP0 migration workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "migrate-forward-baseline")
	jobNode := workflowMappingValue(t, jobsNode, "migrate-forward-baseline")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")
	stepsNode := workflowMappingValue(t, jobNode, "steps")
	if stepsNode.Kind != yaml.SequenceNode || len(stepsNode.Content) != 8 {
		t.Fatalf("RP0 migration step node inventory drifted: %+v", stepsNode)
	}
	wantStepKeys := [][]string{
		{"name", "uses", "with"},
		{"name", "id", "env", "run"},
		{"name", "env", "run"},
		{"name", "uses", "with"},
		{"name", "env", "run"},
		{"name", "id", "env", "run"},
		{"name", "env", "run"},
		{"name", "uses", "with"},
	}
	for index, stepNode := range stepsNode.Content {
		assertWorkflowMappingKeys(t, stepNode, wantStepKeys[index]...)
	}
	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("RP0 migration must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode RP0 workflow_dispatch trigger: %v", err)
	}
	if len(dispatch.Inputs) != 1 {
		t.Fatalf("RP0 migration must expose only expected_sha: %+v", dispatch.Inputs)
	}
	inputNode, ok := dispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("RP0 migration must require expected_sha")
	}
	var input releaseWorkflowDispatchInput
	if err := inputNode.Decode(&input); err != nil {
		t.Fatalf("decode RP0 expected_sha input: %v", err)
	}
	if !input.Required || input.Type != "string" || input.Default != nil {
		t.Fatalf("RP0 expected_sha must be a required string without default: %+v", input)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("RP0 migration must have empty top-level permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["migrate-forward-baseline"]
	if !ok {
		t.Fatal("RP0 migration job is absent")
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"migrate-forward-baseline": {Steps: job.Steps},
	}, map[string]string{
		"migrate-forward-baseline/Verify exact migration authorization and last runtime baseline":      "5d634b19d90645ba234e335c8601fad69996bd17ee4feadf13cbaca3bb843b03",
		"migrate-forward-baseline/Write RP0 migration intent evidence":                                 "854da0bb501bd6179d242f9557768848fefc4d62981bc051d889749388108f5c",
		"migrate-forward-baseline/Observe unchanged production health before baseline migration":       "cebde1718b247d6d5ca0bad326c5b44aa1695d28905a303aab6f42af26c0cfc9",
		"migrate-forward-baseline/Materialize canonical orphan baseline metadata object without a ref": "4fa8d03db5455ccfeb33fae687e46072adf80651745868a106615628829b9ae4",
		"migrate-forward-baseline/Write RP0 metadata object result evidence":                           "7c0f7f5f14fb8e2dcabdc9b9f3c15230aceeafdfe942be123d23a57a1a79e3d1",
	})
	wantPermissions := map[string]string{"actions": "read", "contents": "write"}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, wantPermissions) {
		t.Fatalf("RP0 migration job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact RP0 target without persisted credentials",
		"Verify exact migration authorization and last runtime baseline",
		"Write RP0 migration intent evidence",
		"Upload RP0 migration intent evidence",
		"Observe unchanged production health before baseline migration",
		"Materialize canonical orphan baseline metadata object without a ref",
		"Write RP0 metadata object result evidence",
		"Upload RP0 metadata object result evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("RP0 migration step inventory drifted: %+v", job.Steps)
	}
	for index, want := range wantSteps {
		step := job.Steps[index]
		if step.Name != want || step.If != "" || step.ContinueOnError {
			t.Fatalf("RP0 migration step %d boundary drifted: %+v", index, step)
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" {
		t.Fatalf("RP0 checkout pin drifted: %q", checkout.Uses)
	}
	for key, want := range map[string]string{
		"ref": "${{ github.sha }}", "fetch-depth": "0", "persist-credentials": "false",
	} {
		if got := checkout.With[key]; got != want {
			t.Fatalf("RP0 checkout %s drifted: got %q want %q", key, got, want)
		}
	}
	verify := job.Steps[1]
	if verify.ID != "verify" || verify.Uses != "" {
		t.Fatalf("RP0 evidence verifier boundary drifted: %+v", verify)
	}
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                       "${{ inputs.expected_sha }}",
		"AUTHORIZED_RUNTIME_BASELINE_SHA":    "${{ vars.FUGUE_CONTROL_PLANE_RP0_RUNTIME_BASELINE_SHA }}",
		"AUTHORIZED_RUNTIME_RUN_ID":          "${{ vars.FUGUE_CONTROL_PLANE_RP0_RUNTIME_RUN_ID }}",
		"AUTHORIZED_RUNTIME_ARTIFACT_ID":     "${{ vars.FUGUE_CONTROL_PLANE_RP0_RUNTIME_ARTIFACT_ID }}",
		"AUTHORIZED_RUNTIME_ARTIFACT_DIGEST": "${{ vars.FUGUE_CONTROL_PLANE_RP0_RUNTIME_ARTIFACT_DIGEST }}",
		"HEALTH_URL":                         "${{ vars.FUGUE_CONTROL_PLANE_RP0_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                           "${{ github.token }}",
	}
	if !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("RP0 evidence verifier environment drifted: got %+v want %+v", verify.Env, wantVerifyEnv)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		"git diff --no-renames --name-status",
		"git merge-base --is-ancestor \"${AUTHORIZED_RUNTIME_BASELINE_SHA}\" \"${GITHUB_SHA}\"",
		"fugue-control-plane-release-attribution-${AUTHORIZED_RUNTIME_RUN_ID}-${run_attempt}",
		"sha256:$(sha256sum",
		"missing or ambiguous successful deploy job",
		"[fugue-upgrade] previous Helm revision: 717",
		"[fugue-upgrade] upgrade complete; current Helm revision=718",
		"def parse_rfc3339_nano(value):",
		`r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z"`,
		`.ljust(9, "0")`,
		"180 * 1_000_000_000",
		"runtime baseline continuous observation window is incomplete",
		"central_coredns",
		".updated_at",
		"runtime_completed_at=%s",
		"refs/heads/fugue-control-plane-release-baseline",
		"-F 'force=false'",
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("RP0 evidence verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, "fromisoformat") {
		t.Fatal("RP0 evidence verifier must not truncate or reject RFC3339Nano timestamps through fromisoformat")
	}
	intent := job.Steps[2]
	for _, required := range []string{
		`"baseline_transition": "metadata-object-pending-ref-absent"`,
		`"metadata_ref_created": False`,
		`"cluster_mutation_attempted": False`,
		`"git_history_rewritten": False`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("RP0 intent evidence must contain %q", required)
		}
	}
	upload := job.Steps[3]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" || upload.Run != "" {
		t.Fatalf("RP0 intent evidence upload drifted: %+v", upload)
	}
	observe := job.Steps[4]
	for _, required := range []string{"for sample in 1 2 3 4 5", "sleep 15", `{"status": "ok"}`} {
		if !strings.Contains(observe.Run, required) {
			t.Fatalf("RP0 pre-migration observation must contain %q", required)
		}
	}
	materialize := job.Steps[5]
	if materialize.ID != "materialize" || materialize.If != "" || materialize.Uses != "" ||
		materialize.Shell != "" || materialize.ContinueOnError || materialize.Run == "" {
		t.Fatalf("RP0 metadata materializer execution semantics drifted: %+v", materialize)
	}
	wantMaterializeEnv := map[string]string{
		"EXPECTED_SHA":         "${{ inputs.expected_sha }}",
		"RUNTIME_BASELINE_SHA": "${{ steps.verify.outputs.runtime_baseline_sha }}",
		"RUNTIME_COMPLETED_AT": "${{ steps.verify.outputs.runtime_completed_at }}",
		"GH_TOKEN":             "${{ github.token }}",
	}
	if !reflect.DeepEqual(materialize.Env, wantMaterializeEnv) {
		t.Fatalf("RP0 metadata materializer environment drifted: got %+v want %+v", materialize.Env, wantMaterializeEnv)
	}
	for _, required := range []string{
		"readonly metadata_path='fugue-runtime-baseline.json'",
		`"previous_baseline_object_sha": None`,
		`"schema_version": 1`,
		`"parents": []`,
		`"Fugue Release Baseline"`,
		`"release-baseline@fugue.invalid"`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs"`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${blob_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${tree_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${metadata_commit_sha}"`,
		`response.get("parents") != []`,
		`"${after_status}" == '0' && "${after_count}" == '0'`,
		"metadata_commit_sha=%s",
	} {
		if !strings.Contains(materialize.Run, required) {
			t.Fatalf("RP0 metadata materializer must contain %q", required)
		}
	}
	if strings.Count(materialize.Run, "gh api") != 9 || strings.Count(materialize.Run, "gh api --method POST") != 3 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/matching-refs/heads/fugue-control-plane-release-baseline"`) != 2 {
		t.Fatalf("RP0 metadata materializer API inventory drifted:\n%s", materialize.Run)
	}
	for _, forbidden := range []string{
		"git push", "git update-ref", "--force-with-lease", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		"git/refs", "force=", "curl ", "wget ",
	} {
		if strings.Contains(materialize.Run, forbidden) {
			t.Fatalf("RP0 metadata materializer contains out-of-scope write capability %q", forbidden)
		}
	}
	result := job.Steps[6]
	wantResultEnv := map[string]string{
		"METADATA_BLOB_SHA":   "${{ steps.materialize.outputs.metadata_blob_sha }}",
		"METADATA_TREE_SHA":   "${{ steps.materialize.outputs.metadata_tree_sha }}",
		"METADATA_COMMIT_SHA": "${{ steps.materialize.outputs.metadata_commit_sha }}",
	}
	if !reflect.DeepEqual(result.Env, wantResultEnv) ||
		!strings.Contains(result.Run, `payload["baseline_transition"] = "metadata-object-materialized-ref-absent"`) {
		t.Fatalf("RP0 metadata result evidence drifted: %+v", result)
	}
	resultUpload := job.Steps[7]
	if resultUpload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		resultUpload.With["name"] != "fugue-control-plane-rp0-metadata-object-${{ github.run_id }}-${{ github.run_attempt }}" ||
		resultUpload.With["if-no-files-found"] != "error" || resultUpload.With["retention-days"] != "90" {
		t.Fatalf("RP0 metadata result upload drifted: %+v", resultUpload)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "--kubeconfig",
		"refs/tags/fugue-control-plane-release-baseline", "--force-with-lease",
		"ssh ", "kubectl ", "docker ", "helm ", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", `"repos/${GITHUB_REPOSITORY}/git/refs"`,
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("RP0 migration contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestControlPlaneV2IsExactlyDormantHostedAndPermissionsEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane-v2.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dormant control-plane v2 workflow: %v", err)
	}
	const expectedSource = `name: deploy-control-plane-v2

on:
  workflow_dispatch:
    inputs:
      expected_sha:
        description: Dormant input retained for fail-closed workflow registration
        required: true
        type: string

permissions: {}

jobs:
  dormant:
    runs-on: ubuntu-latest
    timeout-minutes: 1
    permissions: {}
    steps:
      - name: Reject runtime release before Fugue settlement is installed
        run: |
          printf '%s\n' \
            'deploy-control-plane-v2 runtime mutation is intentionally dormant until the separately released Fugue settlement and automatic rollback checkpoint is complete.' >&2
          exit 1
`
	if got := string(data); got != expectedSource {
		t.Fatalf("control-plane v2 must match the reviewed dormant source\ngot:\n%s", got)
	}
}

func TestDisabledWorkflowRerunProbeIsHostedPermissionsEmptyAndZeroWrite(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "probe-disabled-workflow-rerun.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read disabled-workflow rerun probe: %v", err)
	}
	const expectedSource = `name: probe-disabled-workflow-rerun

on:
  workflow_dispatch:
    inputs:
      expected_sha:
        description: Exact lowercase main SHA for the harmless disabled-workflow rerun probe
        required: true
        type: string

permissions: {}

concurrency:
  group: fugue-release-policy-disabled-workflow-rerun-probe-v1
  cancel-in-progress: false

jobs:
  prove-hosted-zero-write-probe:
    runs-on: ubuntu-latest
    timeout-minutes: 3
    permissions: {}
    steps:
      - name: Verify exact SHA and observe unchanged production health
        env:
          EXPECTED_SHA: ${{ inputs.expected_sha }}
        run: |
          set -euo pipefail
          readonly health_url='https://api.fugue.pro/healthz'
          [[ "${GITHUB_EVENT_NAME}" == 'workflow_dispatch' ]]
          [[ "${GITHUB_REF}" == 'refs/heads/main' ]]
          [[ "${GITHUB_RUN_ATTEMPT}" == '1' ]]
          [[ "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]]
          [[ "${EXPECTED_SHA}" == "${GITHUB_SHA}" ]]
          for sample in 1 2 3 4 5; do
            response="$(curl --fail --silent --show-error \
              --connect-timeout 5 --max-time 10 "${health_url}")"
            python3 - "${response}" <<'PY'
          import json, sys
          if json.loads(sys.argv[1]) != {"status": "ok"}:
              raise SystemExit("production health payload drifted")
          PY
            [[ "${sample}" == '5' ]] || sleep 15
          done
          printf '%s\n' 'disabled-workflow rerun probe is exact-SHA, hosted, permissions-empty, and zero-write'
`
	if got := string(data); got != expectedSource {
		t.Fatalf("disabled-workflow rerun probe must match the exact reviewed zero-write source\ngot:\n%s", got)
	}
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Jobs        map[string]struct {
			RunsOn          string                `yaml:"runs-on"`
			TimeoutMinutes  int                   `yaml:"timeout-minutes"`
			Environment     string                `yaml:"environment"`
			Needs           workflowNeeds         `yaml:"needs"`
			If              string                `yaml:"if"`
			Outputs         map[string]string     `yaml:"outputs"`
			Permissions     map[string]string     `yaml:"permissions"`
			ContinueOnError bool                  `yaml:"continue-on-error"`
			Steps           []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse disabled-workflow rerun probe: %v", err)
	}
	workflowDispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("disabled-workflow rerun probe must be dispatch-only: %+v", workflow.On)
	}
	var workflowDispatch releaseWorkflowDispatchTrigger
	if err := workflowDispatchNode.Decode(&workflowDispatch); err != nil {
		t.Fatalf("decode disabled-workflow rerun probe trigger: %v", err)
	}
	if len(workflowDispatch.Inputs) != 1 {
		t.Fatalf("disabled-workflow rerun probe must expose only expected_sha: %+v", workflowDispatch.Inputs)
	}
	expectedSHAInput, ok := workflowDispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("disabled-workflow rerun probe must require expected_sha")
	}
	var expectedSHA releaseWorkflowDispatchInput
	if err := expectedSHAInput.Decode(&expectedSHA); err != nil {
		t.Fatalf("decode disabled-workflow rerun probe expected_sha: %v", err)
	}
	if !expectedSHA.Required || expectedSHA.Type != "string" || expectedSHA.Default != nil {
		t.Fatalf("disabled-workflow rerun probe expected_sha must be required without a default: %+v", expectedSHA)
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("disabled-workflow rerun probe must have empty top-level permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["prove-hosted-zero-write-probe"]
	if !ok {
		t.Fatal("disabled-workflow rerun probe job is absent")
	}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 3 || job.Environment != "" || len(job.Permissions) != 0 {
		t.Fatalf("disabled-workflow rerun probe must be hosted, bounded, environment-free, and permissions-empty: %+v", job)
	}
	if len(job.Needs) != 0 || job.If != "" || len(job.Outputs) != 0 || job.ContinueOnError {
		t.Fatalf("disabled-workflow rerun probe must not depend on, gate, export, or soften another job: %+v", job)
	}
	if len(job.Steps) != 1 {
		t.Fatalf("disabled-workflow rerun probe must contain exactly one step: %+v", job.Steps)
	}
	step := job.Steps[0]
	if step.Name != "Verify exact SHA and observe unchanged production health" || step.Uses != "" || step.If != "" || len(step.With) != 0 || step.ContinueOnError {
		t.Fatalf("disabled-workflow rerun probe must contain one strict shell-only step: %+v", step)
	}
	if len(step.Env) != 1 || step.Env["EXPECTED_SHA"] != "${{ inputs.expected_sha }}" {
		t.Fatalf("disabled-workflow rerun probe expected SHA binding drifted: %+v", step.Env)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`"${GITHUB_RUN_ATTEMPT}" == '1'`,
		`"${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$`,
		`"${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		"for sample in 1 2 3 4 5",
		"sleep 15",
		"https://api.fugue.pro/healthz",
		`{"status": "ok"}`,
	} {
		if !strings.Contains(step.Run, required) {
			t.Fatalf("disabled-workflow rerun probe must contain %q", required)
		}
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "actions/checkout", "uses:", "environment:",
		"contents:", "actions:", "id-token:", "GITHUB_TOKEN", "github.token", "secrets.",
		"kubectl ", "helm ", "ssh ", "scp ", "rsync ", "docker ", "gh ",
		"git push", "git tag", "git update-ref", "curl -X", "curl --request",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("disabled-workflow rerun probe contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestControlPlaneDeployRequiresInternalReleaseGate(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "2761cef511519e8cd887ccac90368cc9db6ed0742807a73184a3e385e3e38f95")
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	workflowRootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, workflowRootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowRunDigests(t, workflow.Jobs, map[string]string{
		"release-input-guard/Guard exact main commit authorization":                      "36817d224982821ad3eb81a44fd42dd50bfa479915e48b339010fae5e19ae1a5",
		"release-baseline/Resolve release-domain baseline":                               "2ee9ad41de7031b7e176a2a6498407a2e966796308ea33083d2653968267fee0",
		"release-baseline/Resolve live image metadata":                                   "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"release-baseline/Compute live-to-target release changed files":                  "3fd4596b94b2bf2cef792ccc89752f72e371fedc51f0953821f341f74d249992",
		"release-gate/Verify generated OpenAPI artifacts":                                "7b93bd9f923a238d19f6aed52847bc1a10000fa5c6fb85fc269f2bf1101dad08",
		"release-gate/Verify release-domain safety contracts":                            "0a71d9858c02ceb5aa8aa188313276dc4a63db5dae5cc856323c533fd1051144",
		"release-gate/Run Go tests":                                                      "1bb497e3e13a1105cf24e3359fa3ef75de08b66ff8a2839cd7f9ea97824d9eb3",
		"build/Compute image metadata":                                                   "12f6dcc38d6f1597416aae34a1c2fa4efda4c6353c5fcbc0eee6c66ee3ccb5b6",
		"build/Compute image build plan":                                                 "e545c87a2385902616eb8fa652954970e0de7e47ffe4c8fea46eb03cb71e5ea0",
		"build/Publish verified control-plane image provenance":                          "6561990b64acc7e6ffe4f97b6f8424edf28154444d579610aa60fb545f15cb07",
		"deploy/Record deploy job budget origin":                                         "752b51a8ce207fa8a0f61a05d9d4deea9990882c5f846f369e916a3be2bfb677",
		"deploy/Build private release-domain tools":                                      "1017c0bb023803233350b68c1b434ca34c01e82d04bc0ad8a80b03f2c437ead2",
		"deploy/Write genesis public release evidence":                                   "6b376d82302f8a146582de5125a6de541bd52d93e3f6696b559b58b1f9990cd5",
		"deploy/Guard stateful component files":                                          "65a7da57e288071328518bc5bd3ee9c0b5726ca97dd9a2b33672fe351eb544c6",
		"deploy/Prepare authoritative DNS DiG runtime":                                   "90038169ec5ef9b2d60a35fa9271e53ee66bdfb1fbaec61ab035674a7b68f6af",
		"deploy/Verify local deploy prerequisites":                                       "e94b5f2811734f45c3ff37be7bf5ef1b85321e8e4b4f2e6821e18e23ff8dff01",
		"deploy/Explain runner and fail closed target":                                   "afab1c1aa3b6305ac3fdf982640fce8d81781c339cea714f11e2bde65a3b4475",
		"deploy/Resolve live image metadata":                                             "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"deploy/Prove explicitly authorized stale pre-Helm release recovery":             "e4af592e5c1cfc427e3f53fa3b2c835bd134019117fc53ffe9e7981944afe312",
		"deploy/Upgrade Fugue control plane":                                             "0390f1a108338e637e594e6e64bb82bcccf3a85ad59f668ee6c1160ddee84e76",
		"deploy/Remove stale release recovery proof":                                     "43203d3cc033dd8ddca207f84eeee8877791c528b99ccae888b7097b2dea077d",
		"record-release-baseline/Advance dedicated forward-only release baseline branch": "9090338e2f90cb9498c42cdf3fb4a3d8da2205ef6b0856760a476a19ee40ea77",
		"freeze-release-lane-on-failure/Record release lane freeze evidence":             "fcf21e0732d091de6e115386f2d55e88de2c0e49110bb7ebf7674c7c8e76e00a",
		"freeze-release-lane-on-failure/Disable release lane and cancel queued runs":     "1e957fb32c9a8c4864c4e43a1bd5878738957696843f4bcfba62d118f7692869",
		"freeze-release-lane-on-failure/Require release lane freeze evidence":            "a583f75fce52b2c2e957c16f290af7ab4367ef35a3b4d22adeef76b2446c6cd4",
	})
	workflowJobsNode := workflowMappingValue(t, workflowRootNode, "jobs")
	assertWorkflowJobNodeContracts(t, workflowJobsNode, map[string]workflowJobNodeContract{
		"release-input-guard": {
			Keys: []string{"runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "env", "run"},
			},
		},
		"release-baseline": {
			Keys: []string{"needs", "outputs", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "id", "run"},
				{"name", "id", "env", "run"},
				{"name", "id", "env", "run"},
			},
		},
		"release-gate": {
			Keys: []string{"needs", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "uses", "with"},
				{"name", "uses", "with"},
				{"name", "run"},
				{"name", "run"},
				{"name", "run"},
			},
		},
		"build": {
			Keys: []string{"needs", "outputs", "permissions", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "uses", "with"},
				{"name", "id", "run"},
				{"name", "id", "env", "run"},
				{"name", "if", "uses"},
				{"name", "if", "uses", "with"},
				{"name", "id", "env", "run"},
			},
		},
		"deploy": {
			Keys: []string{"needs", "if", "runs-on", "timeout-minutes", "environment", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "if", "run"},
				{"name", "uses", "with"},
				{"name", "uses", "with"},
				{"name", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "run"},
				{"name", "if", "run"},
				{"name", "if", "env", "run"},
				{"name", "id", "if", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "run"},
				{"name", "if", "uses", "with"},
			},
		},
		"record-release-baseline": {
			Keys: []string{"needs", "if", "runs-on", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "env", "run"},
			},
		},
		"freeze-release-lane-on-failure": {
			Keys: []string{"needs", "if", "runs-on", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "env", "run"},
				{"name", "id", "if", "continue-on-error", "uses", "with"},
				{"name", "id", "if", "env", "run"},
				{"name", "if", "run"},
			},
		},
	})
	if workflow.On.WorkflowDispatch == nil {
		t.Fatal("control-plane workflow must support workflow_dispatch")
	}
	if len(workflow.On.WorkflowDispatch.Inputs) != 1 {
		t.Fatalf("workflow_dispatch must expose only expected_sha: %+v", workflow.On.WorkflowDispatch.Inputs)
	}
	expectedSHAInput, ok := workflow.On.WorkflowDispatch.Inputs["expected_sha"]
	if !ok {
		t.Fatal("workflow_dispatch must require expected_sha")
	}
	var expectedSHA releaseWorkflowDispatchInput
	if err := expectedSHAInput.Decode(&expectedSHA); err != nil {
		t.Fatalf("decode expected_sha input: %v", err)
	}
	if !expectedSHA.Required || expectedSHA.Type != "string" || expectedSHA.Default != nil {
		t.Fatalf("expected_sha must be a required string without a default: %+v", expectedSHA)
	}
	workflowSource := string(data)
	if strings.Contains(workflowSource, "existing_image_tag") || len(workflow.On.Push.Paths) != 0 {
		t.Fatal("control-plane release must be dispatch-only without an image bypass")
	}

	inputGuard, ok := workflow.Jobs["release-input-guard"]
	if !ok {
		t.Fatal("control-plane workflow must define release-input-guard")
	}
	guard := workflowStepByName(t, inputGuard, "Guard exact main commit authorization")
	for key, want := range map[string]string{
		"EXPECTED_SHA":   "${{ inputs.expected_sha }}",
		"ACTUAL_SHA":     "${{ github.sha }}",
		"EVENT_NAME":     "${{ github.event_name }}",
		"EVENT_REF":      "${{ github.ref }}",
		"EVENT_REF_NAME": "${{ github.ref_name }}",
		"EVENT_REF_TYPE": "${{ github.ref_type }}",
	} {
		if got := guard.Env[key]; got != want {
			t.Fatalf("release input guard env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{"refs/heads/main", "^[0-9a-f]{40}$", `"${EXPECTED_SHA}" == "${ACTUAL_SHA}"`} {
		if !strings.Contains(guard.Run, required) {
			t.Fatalf("release input guard must contain %q", required)
		}
	}

	gate, ok := workflow.Jobs["release-gate"]
	if !ok {
		t.Fatal("control-plane workflow must define release-gate")
	}
	if gate.ContinueOnError {
		t.Fatal("release-gate must fail closed")
	}
	commands := make([]string, 0, len(gate.Steps))
	for _, step := range gate.Steps {
		commands = append(commands, step.Run)
	}
	joinedCommands := strings.Join(commands, "\n")
	for _, required := range []string{
		"make generate-openapi-check",
		"bash scripts/test_release_domain_workflow.sh",
		"bash scripts/test_release_domain_safety.sh",
		"go test ./...",
	} {
		if !strings.Contains(joinedCommands, required) {
			t.Fatalf("release-gate must run %q", required)
		}
	}

	baseline, ok := workflow.Jobs["release-baseline"]
	if !ok {
		t.Fatal("control-plane workflow must define release-baseline")
	}
	for key, want := range map[string]string{
		"domain_base_sha":         "${{ steps.domain_baseline.outputs.domain_base_sha }}",
		"baseline_ref_object_sha": "${{ steps.domain_baseline.outputs.baseline_ref_object_sha }}",
		"changed_files":           "${{ steps.release_changes.outputs.changed_files }}",
		"baseline_refs":           "${{ steps.release_changes.outputs.baseline_refs }}",
		"target_ref":              "${{ steps.release_changes.outputs.target_ref }}",
	} {
		if got := baseline.Outputs[key]; got != want {
			t.Fatalf("release baseline output %s drifted: got %q want %q", key, got, want)
		}
	}
	const checkoutAction = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
	for _, jobName := range []string{"release-baseline", "release-gate", "build", "deploy", "record-release-baseline"} {
		job, exists := workflow.Jobs[jobName]
		if !exists {
			t.Fatalf("control-plane workflow must define %s", jobName)
		}
		checkout := workflowStepByName(t, job, "Checkout")
		if checkout.Uses != checkoutAction {
			t.Fatalf("%s checkout must use the pinned action: got %q want %q", jobName, checkout.Uses, checkoutAction)
		}
		if got, want := checkout.With["ref"], "${{ github.sha }}"; got != want {
			t.Fatalf("%s checkout must bind the exact event commit: got %q want %q", jobName, got, want)
		}
	}
	checkoutCount := 0
	for jobName, job := range workflow.Jobs {
		if strings.Contains(job.If, "workflow_dispatch") {
			t.Fatalf("job %s must not condition behavior on workflow_dispatch: %q", jobName, job.If)
		}
		for _, step := range job.Steps {
			if strings.Contains(step.If, "workflow_dispatch") {
				t.Fatalf("step %s/%s must not condition behavior on workflow_dispatch: %q", jobName, step.Name, step.If)
			}
			if strings.HasPrefix(step.Uses, "actions/checkout@") {
				checkoutCount++
				if step.Uses != checkoutAction {
					t.Fatalf("step %s/%s uses an unapproved checkout action: %q", jobName, step.Name, step.Uses)
				}
				if got, want := step.With["ref"], "${{ github.sha }}"; got != want {
					t.Fatalf("step %s/%s checkout ref drifted: got %q want %q", jobName, step.Name, got, want)
				}
			}
		}
	}
	if checkoutCount != 5 {
		t.Fatalf("control-plane workflow must bind exactly five checkout steps, found %d", checkoutCount)
	}

	if !containsWorkflowNeed(baseline.Needs, "release-input-guard") {
		t.Fatal("release-baseline must wait for the exact input guard")
	}
	domainBaseline := workflowStepByName(t, baseline, "Resolve release-domain baseline")
	if len(domainBaseline.Env) != 0 {
		t.Fatalf("forward-only baseline resolver must not retain genesis inputs: %+v", domainBaseline.Env)
	}
	for _, required := range []string{
		"refs/heads/fugue-control-plane-release-baseline",
		`"${remote_status}" == '0'`,
		`"${fetched_ref_object_sha}" == "${remote_object}"`,
		`"${domain_base_sha}" == "${remote_object}"`,
		"git merge-base --is-ancestor",
		"printf 'is_genesis=false",
		"printf 'genesis_parent_sha=",
	} {
		if !strings.Contains(domainBaseline.Run, required) {
			t.Fatalf("release-domain baseline resolver must contain %q", required)
		}
	}
	for _, forbidden := range []string{
		"refs/tags/", "genesis_base_sha", "force-with-lease", "git push",
		"gh api", "curl ", "--method", "updateRefs",
	} {
		if strings.Contains(domainBaseline.Run, forbidden) {
			t.Fatalf("forward-only baseline resolver retains legacy transport %q", forbidden)
		}
	}

	baselineLiveImages := workflowStepByName(t, baseline, "Resolve live image metadata")
	if baselineLiveImages.ID != "live_images" {
		t.Fatalf("release baseline live image step id drifted: %q", baselineLiveImages.ID)
	}
	if got, want := baselineLiveImages.Env["FUGUE_IMAGE_TAG"], "${{ github.sha }}"; got != want {
		t.Fatalf("release baseline image target must be the dispatched commit: got %q want %q", got, want)
	}
	baselineChanges := workflowStepByName(t, baseline, "Compute live-to-target release changed files")
	if baselineChanges.ID != "release_changes" {
		t.Fatalf("release baseline changed-files step id drifted: %q", baselineChanges.ID)
	}
	if got, want := baselineChanges.Env["FUGUE_RELEASE_TARGET_REF"], "${{ github.sha }}"; got != want {
		t.Fatalf("release baseline diff target must be the dispatched commit: got %q want %q", got, want)
	}
	if got, want := baselineChanges.Env["FUGUE_RELEASE_BASE_REFS"], "${{ steps.live_images.outputs.release_baseline_tags }}"; got != want {
		t.Fatalf("release image diff must retain the live deployed image baselines: got %q want %q", got, want)
	}

	build, ok := workflow.Jobs["build"]
	if !ok || !containsWorkflowNeed(build.Needs, "release-baseline") || !containsWorkflowNeed(build.Needs, "release-gate") {
		t.Fatal("image build must wait for release-baseline and release-gate")
	}
	if strings.TrimSpace(build.If) != "" {
		t.Fatalf("image build must run after the guarded dispatch without a bypass condition: %q", build.If)
	}
	if got, want := build.Permissions, map[string]string{"contents": "read", "packages": "write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("image build permissions drifted: got %v want %v", got, want)
	}
	for key, want := range map[string]string{
		"image_tag":                        "${{ steps.meta.outputs.image_tag }}",
		"api_image_repository":             "${{ steps.meta.outputs.api_image_repository }}",
		"controller_image_repository":      "${{ steps.meta.outputs.controller_image_repository }}",
		"drain_agent_image_repository":     "${{ steps.meta.outputs.drain_agent_image_repository }}",
		"telemetry_agent_image_repository": "${{ steps.meta.outputs.telemetry_agent_image_repository }}",
		"image_cache_image_repository":     "${{ steps.meta.outputs.image_cache_image_repository }}",
		"edge_image_repository":            "${{ steps.meta.outputs.edge_image_repository }}",
		"build_api":                        "${{ steps.plan.outputs.build_api }}",
		"build_controller":                 "${{ steps.plan.outputs.build_controller }}",
		"build_drain_agent":                "${{ steps.plan.outputs.build_drain_agent }}",
		"build_telemetry_agent":            "${{ steps.plan.outputs.build_telemetry_agent }}",
		"build_image_cache":                "${{ steps.plan.outputs.build_image_cache }}",
		"build_edge":                       "${{ steps.plan.outputs.build_edge }}",
	} {
		if got := build.Outputs[key]; got != want {
			t.Fatalf("image build output %s drifted: got %q want %q", key, got, want)
		}
	}
	buildMeta := workflowStepByName(t, build, "Compute image metadata")
	if buildMeta.ID != "meta" {
		t.Fatalf("image metadata step id drifted: %q", buildMeta.ID)
	}
	const imageTagOutput = `echo "image_tag=${GITHUB_SHA}" >> "${GITHUB_OUTPUT}"`
	if strings.Count(buildMeta.Run, "image_tag=") != 1 || !strings.Contains(buildMeta.Run, imageTagOutput) {
		t.Fatalf("image metadata must publish only GITHUB_SHA as image_tag: %q", buildMeta.Run)
	}
	buildPlan := workflowStepByName(t, build, "Compute image build plan")
	if buildPlan.ID != "plan" {
		t.Fatalf("image build-plan step id drifted: %q", buildPlan.ID)
	}
	if got, want := buildPlan.Env["FUGUE_RELEASE_TARGET_REF"], "${{ needs.release-baseline.outputs.target_ref }}"; got != want {
		t.Fatalf("image build plan must use the baseline target ref: got %q want %q", got, want)
	}
	buildProvenance := workflowStepByName(t, build, "Publish verified control-plane image provenance")
	if buildProvenance.ID != "build_images" {
		t.Fatalf("image provenance step id drifted: %q", buildProvenance.ID)
	}
	if strings.TrimSpace(buildProvenance.If) != "" {
		t.Fatalf("image provenance must be published for empty and non-empty build plans: %q", buildProvenance.If)
	}
	if got, want := buildProvenance.Env["FUGUE_IMAGE_TAG"], "${{ steps.meta.outputs.image_tag }}"; got != want {
		t.Fatalf("image provenance tag source drifted: got %q want %q", got, want)
	}
	if got, want := buildProvenance.Env["FUGUE_CONTROL_PLANE_IMAGE_TARGETS"], "${{ steps.plan.outputs.targets }}"; got != want {
		t.Fatalf("image provenance target source drifted: got %q want %q", got, want)
	}

	deploy, ok := workflow.Jobs["deploy"]
	if !ok || !containsWorkflowNeed(deploy.Needs, "release-baseline") || !containsWorkflowNeed(deploy.Needs, "release-gate") || !containsWorkflowNeed(deploy.Needs, "build") {
		t.Fatal("control-plane deploy must wait for release-baseline, release-gate, and build")
	}
	const deployCondition = "${{ always() && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' }}"
	if strings.TrimSpace(deploy.If) != deployCondition {
		t.Fatalf("deploy condition must require every prerequisite success without bypass: got %q want %q", deploy.If, deployCondition)
	}
	if got, want := deploy.Permissions, map[string]string{"actions": "read", "contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deploy permissions drifted: got %v want %v", got, want)
	}
	if deploy.ContinueOnError {
		t.Fatal("deploy job must fail closed")
	}
	buildTools := workflowStepByName(t, deploy, "Build private release-domain tools")
	for _, required := range []string{
		"${RUNNER_TEMP}/fugue-release-tools",
		"for goarch in amd64 arm64; do",
		"CGO_ENABLED=0",
		`GOARCH="${goarch}"`,
		"GOOS=linux",
		"GOFLAGS=-mod=readonly",
		"go list -mod=readonly -buildvcs=false -deps ./cmd/...",
		"go mod verify",
		"GOPROXY=https://proxy.golang.org",
		"'GOVCS=*:off'",
		"git diff --exit-code -- go.mod go.sum",
		"./cmd/fugue-release-domain-evidence",
		"./cmd/fugue-release-domain-dispatch",
		"chmod 0700",
	} {
		if !strings.Contains(buildTools.Run, required) {
			t.Fatalf("deploy release tool build must contain %q", required)
		}
	}
	if strings.Contains(buildTools.Run, "go mod download all") {
		t.Fatal("deploy release tool build must not preload unrelated module versions")
	}
	if strings.Contains(buildTools.Run, "GOPROXY=off") {
		t.Fatal("deploy release tool cache validation must not disable the module proxy")
	}
	preloadIndex := strings.Index(buildTools.Run, "go list -mod=readonly -buildvcs=false -deps ./cmd/...")
	verifyIndex := strings.Index(buildTools.Run, "go mod verify")
	evidenceBuildIndex := strings.Index(buildTools.Run, `go build -trimpath -o "${tools_dir}/fugue-release-domain-evidence"`)
	if preloadIndex < 0 || verifyIndex < 0 || evidenceBuildIndex < 0 || preloadIndex >= verifyIndex || verifyIndex >= evidenceBuildIndex {
		t.Fatal("deploy must preload and verify both command dependency graphs before building evidence")
	}
	genesisEvidence := workflowStepByName(t, deploy, "Write genesis public release evidence")
	if got, want := genesisEvidence.If, "${{ needs.release-baseline.outputs.is_genesis == 'true' }}"; got != want {
		t.Fatalf("genesis evidence condition drifted: got %q want %q", got, want)
	}
	for _, required := range []string{
		"write-genesis-public-evidence",
		`--ownership "${GITHUB_WORKSPACE}/deploy/release-domains/ownership-v1.yaml"`,
		`--expected-head-sha "${GENESIS_SHA}"`,
		`--evidence-base-sha "${DOMAIN_BASE_SHA}"`,
		`--actual-parent-sha "${GENESIS_PARENT_SHA}"`,
	} {
		if !strings.Contains(genesisEvidence.Run, required) {
			t.Fatalf("genesis evidence command must contain %q", required)
		}
	}
	expectedGenesisChanges := []string{
		".github/workflows/deploy-control-plane.yml",
		"cmd/fugue-release-domain-dispatch/classify_files.go",
		"cmd/fugue-release-domain-dispatch/main.go",
		"cmd/fugue-release-domain-dispatch/main_test.go",
		"cmd/fugue-release-domain-dispatch/public_evidence.go",
		"cmd/fugue-release-domain-dispatch/public_evidence_test.go",
		"cmd/fugue-release-domain-dispatch/secure_files.go",
		"cmd/fugue-release-domain-dispatch/stat_times_darwin.go",
		"cmd/fugue-release-domain-dispatch/stat_times_linux.go",
		"cmd/fugue-release-domain-dispatch/stat_times_other.go",
		"cmd/fugue-release-domain-dispatch/strict_json.go",
		"cmd/fugue-release-domain-evidence/evidence.go",
		"cmd/fugue-release-domain-evidence/evidence_test.go",
		"cmd/fugue-release-domain-evidence/main.go",
		"cmd/fugue-release-domain-evidence/manifest.go",
		"cmd/fugue-release-domain-evidence/manifest_test.go",
		"cmd/fugue-release-domain-plan/main.go",
		"cmd/fugue-release-domain-plan/main_test.go",
		"cmd/fugue-release-domain-plan/output.go",
		"cmd/fugue-release-domain-plan/output_test.go",
		"deploy/release-domains/ownership-v1.yaml",
		"docs/runbooks/release-domain-planner.md",
		"internal/api/topology_labeler_test.go",
		"internal/platformsafety/release_workflow_test.go",
		"internal/releaseadapter/adapter.go",
		"internal/releaseadapter/dispatcher.go",
		"internal/releaseadapter/dispatcher_test.go",
		"internal/releaseadapter/trace.go",
		"internal/releaseadapter/transaction.go",
		"internal/releaseadapter/transaction_test.go",
		"internal/releasedomain/changed_file_evidence.go",
		"internal/releasedomain/changed_file_evidence_test.go",
		"internal/releasedomain/file_classifier_test.go",
		"internal/releasedomain/ownership_test.go",
		"internal/releasedomain/plan_artifacts.go",
		"internal/releasedomain/plan_artifacts_test.go",
		"internal/releasedomain/rendered_classifier_test.go",
		"internal/releasedomain/rollback_ownership.go",
		"internal/releasedomain/rollback_ownership_test.go",
		"internal/releasedomain/transaction_envelope.go",
		"internal/releaseevidence/public.go",
		"internal/releaseevidence/public_test.go",
		"scripts/lib/control_plane_release_domain_production.sh",
		"scripts/lib/control_plane_release_domains.sh",
		"scripts/lib/control_plane_release_render.sh",
		"scripts/test_control_plane_release_domain_production.sh",
		"scripts/test_control_plane_release_main_wiring.sh",
		"scripts/test_control_plane_release_render.sh",
		"scripts/test_release_domain_safety.sh",
		"scripts/test_release_domain_workflow.sh",
		"scripts/test_single_domain_release.sh",
		"scripts/upgrade_fugue_control_plane.sh",
	}
	if len(expectedGenesisChanges) != 52 {
		t.Fatalf("genesis expected-change allowlist must contain exactly 52 paths, found %d", len(expectedGenesisChanges))
	}
	seenGenesisChanges := make(map[string]struct{}, len(expectedGenesisChanges))
	for _, path := range expectedGenesisChanges {
		if path == "" {
			t.Fatal("genesis expected-change allowlist contains an empty path")
		}
		if _, duplicate := seenGenesisChanges[path]; duplicate {
			t.Fatalf("genesis expected-change allowlist repeats %q", path)
		}
		seenGenesisChanges[path] = struct{}{}
	}
	const expectedChangePrefix = `--expected-change "`
	const expectedChangeSuffix = "\" \\"
	actualGenesisChanges := make([]string, 0, len(expectedGenesisChanges))
	for _, line := range strings.Split(genesisEvidence.Run, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, "--expected-change") {
			continue
		}
		if !strings.HasPrefix(trimmed, expectedChangePrefix) || !strings.HasSuffix(trimmed, expectedChangeSuffix) {
			t.Fatalf("genesis expected-change must be one literal quoted path per flag: %q", trimmed)
		}
		path := strings.TrimSuffix(strings.TrimPrefix(trimmed, expectedChangePrefix), expectedChangeSuffix)
		actualGenesisChanges = append(actualGenesisChanges, path)
	}
	if !reflect.DeepEqual(actualGenesisChanges, expectedGenesisChanges) {
		t.Fatalf("genesis expected-change allowlist drifted:\n got: %q\nwant: %q", actualGenesisChanges, expectedGenesisChanges)
	}
	for _, line := range strings.Split(genesisEvidence.Run, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "upgrade_fugue_control_plane.sh") && !strings.HasPrefix(trimmed, expectedChangePrefix) {
			t.Fatal("genesis evidence path must never invoke the upgrade script")
		}
	}

	statefulGuard := workflowStepByName(t, deploy, "Guard stateful component files")
	const nonGenesisCondition = "${{ needs.release-baseline.outputs.is_genesis != 'true' }}"
	genesisReachable := map[string]string{
		"Checkout":                              "",
		"Setup Go":                              "",
		"Build private release-domain tools":    "",
		"Write genesis public release evidence": "${{ needs.release-baseline.outputs.is_genesis == 'true' }}",
		"Upload release-domain public evidence": "always()",
	}
	for _, candidate := range deploy.Steps {
		if want, allowed := genesisReachable[candidate.Name]; allowed {
			if candidate.If != want {
				t.Fatalf("genesis-reachable step %s condition drifted: got %q want %q", candidate.Name, candidate.If, want)
			}
			continue
		}
		if !strings.Contains(candidate.If, "needs.release-baseline.outputs.is_genesis != 'true'") {
			t.Fatalf("unreviewed deploy step %s is reachable from genesis: %q", candidate.Name, candidate.If)
		}
	}
	if strings.TrimSpace(statefulGuard.If) != nonGenesisCondition {
		t.Fatalf("stateful component guard must run only for ordinary releases: %q", statefulGuard.If)
	}
	if got, want := statefulGuard.Env["FUGUE_RELEASE_CHANGED_FILES"], "${{ needs.release-baseline.outputs.changed_files }}"; got != want {
		t.Fatalf("stateful component guard must consume the trusted baseline changed files: got %q want %q", got, want)
	}
	if !strings.Contains(statefulGuard.Run, "independent controlled release window") || strings.Contains(statefulGuard.Run, "manual release") {
		t.Fatal("stateful component guard must direct operators to an independent controlled release window")
	}
	const deployImageTag = "${{ needs.build.outputs.image_tag || github.sha }}"
	explain := workflowStepByName(t, deploy, "Explain runner and fail closed target")
	if got := explain.Env["FUGUE_IMAGE_TAG"]; got != deployImageTag {
		t.Fatalf("deploy attribution must use the built image tag chain: got %q want %q", got, deployImageTag)
	}
	deployLiveImages := workflowStepByName(t, deploy, "Resolve live image metadata")
	if deployLiveImages.ID != "live_images" {
		t.Fatalf("deploy live image step id drifted: %q", deployLiveImages.ID)
	}
	if got := deployLiveImages.Env["FUGUE_IMAGE_TAG"]; got != deployImageTag {
		t.Fatalf("deploy live image resolution must use the built image tag chain: got %q want %q", got, deployImageTag)
	}

	upgrade := workflowStepByName(t, deploy, "Upgrade Fugue control plane")
	if strings.TrimSpace(upgrade.If) != nonGenesisCondition {
		t.Fatalf("control-plane upgrade must be unreachable from the genesis evidence path: %q", upgrade.If)
	}
	for key, want := range map[string]string{
		"FUGUE_API_IMAGE_REPOSITORY":             "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.api_image_repository || steps.live_images.outputs.api_image_repository }}",
		"FUGUE_API_IMAGE_TAG":                    "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.api_image_tag }}",
		"FUGUE_CONTROLLER_IMAGE_REPOSITORY":      "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.controller_image_repository || steps.live_images.outputs.controller_image_repository }}",
		"FUGUE_CONTROLLER_IMAGE_TAG":             "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.controller_image_tag }}",
		"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY":     "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.drain_agent_image_repository || steps.live_images.outputs.drain_agent_image_repository }}",
		"FUGUE_DRAIN_AGENT_IMAGE_TAG":            "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.drain_agent_image_tag }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY": "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.telemetry_agent_image_repository || steps.live_images.outputs.telemetry_agent_image_repository }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_TAG":        "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.telemetry_agent_image_tag }}",
		"FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY":     "${{ needs.build.outputs.build_image_cache == 'true' && needs.build.outputs.image_cache_image_repository || steps.live_images.outputs.image_cache_image_repository }}",
		"FUGUE_IMAGE_CACHE_IMAGE_TAG":            "${{ needs.build.outputs.build_image_cache == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.image_cache_image_tag }}",
		"FUGUE_EDGE_IMAGE_REPOSITORY":            "${{ needs.build.outputs.build_edge == 'true' && needs.build.outputs.edge_image_repository || steps.live_images.outputs.edge_image_repository }}",
		"FUGUE_EDGE_IMAGE_TAG":                   "${{ needs.build.outputs.build_edge == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.edge_image_tag }}",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("upgrade image selection %s drifted: got %q want %q", key, got, want)
		}
	}
	if got, want := upgrade.Env["FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE"], "${{ vars.FUGUE_PUBLIC_DATA_PLANE_AUTO_RELEASE_ELIGIBLE || needs.build.outputs.build_edge == 'true' }}"; got != want {
		t.Fatalf("public data-plane auto release must depend only on explicit policy or an edge build: got %q want %q", got, want)
	}
	for key, want := range map[string]string{
		"FUGUE_RELEASE_DOMAIN_BASE_SHA":             "${{ needs.release-baseline.outputs.domain_base_sha }}",
		"FUGUE_RELEASE_DOMAIN_TARGET_SHA":           "${{ github.sha }}",
		"FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL":        "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
		"FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL":        "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
		"FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE": "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("upgrade release-domain input %s drifted: got %q want %q", key, got, want)
		}
	}

	publicUpload := workflowStepByName(t, deploy, "Upload release-domain public evidence")
	if got, want := publicUpload.If, "always()"; got != want {
		t.Fatalf("public evidence must always be uploaded: got %q want %q", got, want)
	}
	if publicUpload.ContinueOnError {
		t.Fatal("public evidence upload must fail closed")
	}
	for key, want := range map[string]string{
		"path":                 "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
		"if-no-files-found":    "error",
		"retention-days":       "90",
		"include-hidden-files": "false",
		"overwrite":            "false",
	} {
		if got := publicUpload.With[key]; got != want {
			t.Fatalf("public evidence upload %s drifted: got %q want %q", key, got, want)
		}
	}

	recordBaseline, ok := workflow.Jobs["record-release-baseline"]
	if !ok {
		t.Fatal("control-plane workflow must define record-release-baseline")
	}
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy"} {
		if !containsWorkflowNeed(recordBaseline.Needs, required) {
			t.Fatalf("record-release-baseline must wait for %s", required)
		}
	}
	if got, want := recordBaseline.Permissions, map[string]string{"contents": "write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("record-release-baseline permissions drifted: got %v want %v", got, want)
	}
	if recordBaseline.ContinueOnError {
		t.Fatal("record-release-baseline must fail closed")
	}
	recordNode := workflowMappingValue(t, workflowJobsNode, "record-release-baseline")
	assertWorkflowMappingKeys(t, recordNode, "needs", "if", "runs-on", "permissions", "steps")
	recordStepsNode := workflowMappingValue(t, recordNode, "steps")
	if recordStepsNode.Kind != yaml.SequenceNode || len(recordStepsNode.Content) != 2 {
		t.Fatalf("record-release-baseline step node inventory drifted: %+v", recordStepsNode)
	}
	assertWorkflowMappingKeys(t, recordStepsNode.Content[0], "name", "uses", "with")
	assertWorkflowMappingKeys(t, recordStepsNode.Content[1], "name", "env", "run")
	const recordBaselineCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' }}"
	if recordBaseline.If != recordBaselineCondition {
		t.Fatalf("record-release-baseline success condition drifted: got %q want %q", recordBaseline.If, recordBaselineCondition)
	}
	if len(recordBaseline.Steps) != 2 {
		t.Fatalf("record-release-baseline must contain exact checkout/writer steps: %+v", recordBaseline.Steps)
	}
	checkout := recordBaseline.Steps[0]
	if checkout.Name != "Checkout" || checkout.With["persist-credentials"] != "false" {
		t.Fatalf("record-release-baseline checkout must not persist credentials: %+v", checkout)
	}
	advanceBaseline := workflowStepByName(t, recordBaseline, "Advance dedicated forward-only release baseline branch")
	if advanceBaseline.If != "" || advanceBaseline.Uses != "" || advanceBaseline.Shell != "" ||
		advanceBaseline.ContinueOnError || advanceBaseline.Run == "" {
		t.Fatalf("release baseline writer execution semantics drifted: %+v", advanceBaseline)
	}
	if recordBaseline.Steps[1].Name != advanceBaseline.Name {
		t.Fatal("release baseline writer must be the final semantic step")
	}
	if got, want := advanceBaseline.Env["EXPECTED_BASE_REF_OBJECT"], "${{ needs.release-baseline.outputs.baseline_ref_object_sha }}"; got != want {
		t.Fatalf("record-release-baseline ref-object binding drifted: got %q want %q", got, want)
	}
	wantAdvanceEnv := map[string]string{
		"EXPECTED_BASE_SHA":        "${{ needs.release-baseline.outputs.domain_base_sha }}",
		"EXPECTED_BASE_REF_OBJECT": "${{ needs.release-baseline.outputs.baseline_ref_object_sha }}",
		"TARGET_SHA":               "${{ github.sha }}",
		"GH_TOKEN":                 "${{ github.token }}",
	}
	if !reflect.DeepEqual(advanceBaseline.Env, wantAdvanceEnv) {
		t.Fatalf("record-release-baseline writer environment drifted: got %+v want %+v", advanceBaseline.Env, wantAdvanceEnv)
	}
	for _, required := range []string{
		"refs/heads/fugue-control-plane-release-baseline",
		`"${remote_object}" == "${EXPECTED_BASE_REF_OBJECT}"`,
		`"${EXPECTED_BASE_REF_OBJECT}" == "${EXPECTED_BASE_SHA}"`,
		"git merge-base --is-ancestor",
		"beforeOid:$beforeOid",
		"afterOid:$afterOid",
		"-F 'force=false'",
		`-f "beforeOid=${EXPECTED_BASE_REF_OBJECT}"`,
		`-f "afterOid=${TARGET_SHA}"`,
		`"${observed}" == "${TARGET_SHA}"`,
	} {
		if !strings.Contains(advanceBaseline.Run, required) {
			t.Fatalf("release baseline advancement must contain %q", required)
		}
	}
	if strings.Count(advanceBaseline.Run, "gh api") != 3 ||
		strings.Count(advanceBaseline.Run, "gh api graphql") != 2 ||
		strings.Count(advanceBaseline.Run, "updateRefs(") != 1 ||
		strings.Count(advanceBaseline.Run, "-F 'force=false'") != 1 {
		t.Fatalf("release baseline writer API inventory drifted:\n%s", advanceBaseline.Run)
	}
	for _, forbidden := range []string{
		"refs/tags/", "git push", "git update-ref", "--force-with-lease", "--method",
		" -X ", "createRef", "deleteRef", "force=true", "curl ", "wget ",
	} {
		if strings.Contains(advanceBaseline.Run, forbidden) {
			t.Fatalf("release baseline writer contains out-of-scope capability %q", forbidden)
		}
	}

	freeze, ok := workflow.Jobs["freeze-release-lane-on-failure"]
	if !ok {
		t.Fatal("control-plane workflow must define the automatic release-lane freeze finalizer")
	}
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline"} {
		if !containsWorkflowNeed(freeze.Needs, required) {
			t.Fatalf("release-lane freeze finalizer must wait for %s", required)
		}
	}
	if len(freeze.Needs) != 6 {
		t.Fatalf("release-lane freeze finalizer has unexpected dependencies: %v", freeze.Needs)
	}
	const freezeCondition = "${{ always() && (needs.release-input-guard.result != 'success' || needs.release-baseline.result != 'success' || needs.release-gate.result != 'success' || needs.build.result != 'success' || needs.deploy.result != 'success' || needs.record-release-baseline.result != 'success') }}"
	if freeze.If != freezeCondition {
		t.Fatalf("release-lane freeze condition drifted: got %q want %q", freeze.If, freezeCondition)
	}
	if got, want := freeze.Permissions["actions"], "write"; got != want {
		t.Fatalf("release-lane freeze finalizer needs actions:write: got %q want %q", got, want)
	}
	if got, want := freeze.Permissions["contents"], "read"; got != want {
		t.Fatalf("release-lane freeze finalizer needs contents:read: got %q want %q", got, want)
	}
	if len(freeze.Permissions) != 2 {
		t.Fatalf("release-lane freeze finalizer has unexpected permissions: %v", freeze.Permissions)
	}
	if got, want := workflow.Permissions, map[string]string{"contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("workflow default permissions must be contents:read only: got %v want %v", got, want)
	}
	for jobName, job := range workflow.Jobs {
		if jobName != "freeze-release-lane-on-failure" && job.Permissions["actions"] == "write" {
			t.Fatalf("job %s must not receive actions:write", jobName)
		}
	}

	freezeRecord := workflowStepByName(t, freeze, "Record release lane freeze evidence")
	for key, want := range map[string]string{
		"RELEASE_INPUT_GUARD_RESULT":     "${{ needs.release-input-guard.result }}",
		"RELEASE_BASELINE_RESULT":        "${{ needs.release-baseline.result }}",
		"RELEASE_GATE_RESULT":            "${{ needs.release-gate.result }}",
		"BUILD_RESULT":                   "${{ needs.build.result }}",
		"DEPLOY_RESULT":                  "${{ needs.deploy.result }}",
		"RECORD_RELEASE_BASELINE_RESULT": "${{ needs.record-release-baseline.result }}",
	} {
		if got := freezeRecord.Env[key]; got != want {
			t.Fatalf("release-lane freeze evidence env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{"lane-freeze.json", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "job_results", "os.replace"} {
		if !strings.Contains(freezeRecord.Run, required) {
			t.Fatalf("release-lane freeze evidence must contain %q", required)
		}
	}

	freezeUpload := workflowStepByName(t, freeze, "Upload release lane freeze evidence")
	const uploadArtifactAction = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
	if freezeUpload.ID != "freeze_evidence_upload" || strings.TrimSpace(freezeUpload.If) != "always()" || freezeUpload.Uses != uploadArtifactAction || !freezeUpload.ContinueOnError {
		t.Fatalf("release-lane freeze evidence upload must be pinned and non-blocking: %#v", freezeUpload)
	}
	if got, want := freezeUpload.With["if-no-files-found"], "error"; got != want {
		t.Fatalf("release-lane freeze evidence upload must reject an absent file: got %q want %q", got, want)
	}

	freezeLane := workflowStepByName(t, freeze, "Disable release lane and cancel queued runs")
	if freezeLane.ID != "freeze_lane" || strings.TrimSpace(freezeLane.If) != "always()" {
		t.Fatalf("release-lane disable step must always run after evidence generation: %#v", freezeLane)
	}
	for _, required := range []string{
		"actions/workflows/${workflow_id}/disable",
		"disabled_manually",
		"for status in queued in_progress requested waiting pending",
		"status=${status}",
		"actions/runs/${run_id}/cancel",
		"CURRENT_RUN_ID",
		"pending_other_runs",
	} {
		if !strings.Contains(freezeLane.Run, required) {
			t.Fatalf("release-lane disable step must contain %q", required)
		}
	}

	requireFreezeEvidence := workflowStepByName(t, freeze, "Require release lane freeze evidence")
	if got, want := requireFreezeEvidence.If, "${{ always() && steps.freeze_evidence_upload.outcome != 'success' }}"; got != want {
		t.Fatalf("release-lane evidence failure condition drifted: got %q want %q", got, want)
	}
}

func workflowStepByName(t *testing.T, job releaseWorkflowJob, name string) releaseWorkflowStep {
	t.Helper()
	var match releaseWorkflowStep
	found := false
	for _, step := range job.Steps {
		if step.Name == name {
			if found {
				t.Fatalf("workflow job defines duplicate step %q", name)
			}
			match = step
			found = true
		}
	}
	if !found {
		t.Fatalf("workflow job does not define step %q", name)
	}
	return match
}

func containsWorkflowNeed(needs workflowNeeds, expected string) bool {
	return containsString([]string(needs), expected)
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
