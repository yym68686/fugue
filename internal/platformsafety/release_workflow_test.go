package platformsafety

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
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
	Required bool   `yaml:"required"`
	Type     string `yaml:"type"`
	Default  any    `yaml:"default"`
}

type releaseWorkflowJob struct {
	Needs           workflowNeeds         `yaml:"needs"`
	If              string                `yaml:"if"`
	RunsOn          yaml.Node             `yaml:"runs-on"`
	TimeoutMinutes  int                   `yaml:"timeout-minutes"`
	Environment     string                `yaml:"environment"`
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

type compositeReleaseAction struct {
	Runs struct {
		Using string                `yaml:"using"`
		Steps []releaseWorkflowStep `yaml:"steps"`
	} `yaml:"runs"`
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
	assertWorkflowSourceDigest(t, data, "76a1e6c6a4d6af1df8516c9c1f418ee0421149d91332188be74fd8a651f99070")
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
		"migrate-forward-baseline/Verify exact migration authorization and last runtime baseline":      "c451c407dae5526825da3e969d827e89d365517fad45a0ba8027416cc626bbd9",
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

func TestRP0CarrierMaterializerIsHostedRefFreeAndReadbackSettled(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "materialize-control-plane-release-baseline-carrier-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 carrier materializer workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "32bda9af4f36164869ed9718648daca5f4357406de5e00a0e10b0923ac14b849")
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
		t.Fatalf("parse RP0 carrier materializer workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "materialize-forward-carrier")
	jobNode := workflowMappingValue(t, jobsNode, "materialize-forward-carrier")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("carrier materializer must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode carrier workflow_dispatch: %v", err)
	}
	wantInputs := []string{"expected_previous_object_sha", "expected_sha", "runtime_sha"}
	if len(dispatch.Inputs) != len(wantInputs) {
		t.Fatalf("carrier materializer input inventory drifted: %+v", dispatch.Inputs)
	}
	for _, name := range wantInputs {
		node, exists := dispatch.Inputs[name]
		if !exists {
			t.Fatalf("carrier materializer input %s is absent", name)
		}
		var input releaseWorkflowDispatchInput
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode carrier input %s: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("carrier input %s must be required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("carrier materializer top-level boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["materialize-forward-carrier"]
	if !ok {
		t.Fatal("carrier materializer job is absent")
	}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 15 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, map[string]string{"contents": "write"}) {
		t.Fatalf("carrier materializer job boundary drifted: %+v", job)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"materialize-forward-carrier": {Steps: job.Steps},
	}, map[string]string{
		"materialize-forward-carrier/Verify exact carrier materialization authorization":                 "29829f3f441e8907d295679f19fa757cefc2d2b28dfa4651a10d4712c105257c",
		"materialize-forward-carrier/Write carrier materialization intent evidence":                      "686ab0004c352ef4b9840be7f75bbe902db1f80ddb789fb82070931318c0a124",
		"materialize-forward-carrier/Observe unchanged production health before carrier object write":    "cebde1718b247d6d5ca0bad326c5b44aa1695d28905a303aab6f42af26c0cfc9",
		"materialize-forward-carrier/Materialize canonical forward carrier objects without moving a ref": "1a28dd68acb853ac7ff8bfdbcb49e159736e50f1be8870c54cb902db4117f7fc",
		"materialize-forward-carrier/Write carrier materialization result evidence":                      "ab548801ade6ea482474ba6a7b1b9c5fff8a6d92e329fb2e4494f9eb7fd22a8f",
	})
	wantSteps := []string{
		"Checkout exact carrier-writer policy SHA",
		"Verify exact carrier materialization authorization",
		"Write carrier materialization intent evidence",
		"Upload carrier materialization intent evidence",
		"Observe unchanged production health before carrier object write",
		"Materialize canonical forward carrier objects without moving a ref",
		"Write carrier materialization result evidence",
		"Upload carrier materialization result evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("carrier materializer step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("carrier materializer step %d drifted: %+v", index, step)
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("carrier materializer checkout drifted: %+v", checkout)
	}
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                 "${{ inputs.expected_sha }}",
		"EXPECTED_PREVIOUS_OBJECT_SHA": "${{ inputs.expected_previous_object_sha }}",
		"RUNTIME_SHA":                  "${{ inputs.runtime_sha }}",
		"HEALTH_URL":                   "${{ vars.FUGUE_CONTROL_PLANE_RP0_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                     "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("carrier verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${GITHUB_REF}" == 'refs/heads/main'`,
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`actual_changes_text="$(git diff --no-renames --name-status "${policy_parent}" "${GITHUB_SHA}")" || exit 1`,
		`mapfile -t actual_changes <<<"${actual_changes_text}"`,
		`M\t.github/workflows/materialize-control-plane-release-baseline-carrier-rp0.yml`,
		`M\tinternal/platformsafety/release_workflow_test.go`,
		`"${baseline_object}" == "${EXPECTED_PREVIOUS_OBJECT_SHA}"`,
		`"${represented_runtime}" == "${RUNTIME_SHA}"`,
		`"${represented_parent}" == "${represented_previous}"`,
		`git merge-base --is-ancestor "${RUNTIME_SHA}" "${GITHUB_SHA}"`,
		`carrier_date=%s`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("carrier verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("carrier verifier must not hide source command status through process substitution")
	}
	materialize := job.Steps[5]
	wantMaterializeEnv := map[string]string{
		"EXPECTED_SHA":        "${{ inputs.expected_sha }}",
		"PREVIOUS_OBJECT_SHA": "${{ steps.verify.outputs.previous_object_sha }}",
		"RUNTIME_SHA":         "${{ steps.verify.outputs.runtime_sha }}",
		"CARRIER_DATE":        "${{ steps.verify.outputs.carrier_date }}",
		"GH_TOKEN":            "${{ github.token }}",
	}
	if materialize.ID != "materialize" || materialize.Uses != "" || materialize.Run == "" ||
		!reflect.DeepEqual(materialize.Env, wantMaterializeEnv) {
		t.Fatalf("carrier materializer execution boundary drifted: %+v", materialize)
	}
	for _, required := range []string{
		`"previous_baseline_object_sha": sys.argv[1]`,
		`git hash-object -w --stdin`,
		`git mktree`,
		`).encode("utf-8") + message.encode("utf-8")`,
		`git hash-object -t commit --stdin`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${blob_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${tree_sha}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${carrier_sha}"`,
		`"${after_object}" == "${PREVIOUS_OBJECT_SHA}"`,
		`blob_transport_status=%s`,
		`tree_transport_status=%s`,
		`commit_transport_status=%s`,
	} {
		if !strings.Contains(materialize.Run, required) {
			t.Fatalf("carrier materializer must contain %q", required)
		}
	}
	if strings.Count(materialize.Run, "gh api --method POST") != 3 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/blobs"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/trees"`) != 1 ||
		strings.Count(materialize.Run, `"repos/${GITHUB_REPOSITORY}/git/commits"`) != 1 {
		t.Fatalf("carrier object write inventory drifted:\n%s", materialize.Run)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ",
		"git push", "git update-ref", "git commit-tree", "--force-with-lease", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", "graphql", "updateRefs", "createRef", "deleteRef",
		`"repos/${GITHUB_REPOSITORY}/git/refs`, "force=", "docker ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("carrier materializer contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP0CarrierMaterializerSourceBindingRejectsValidOutputThenFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		code int
	}{
		{
			name: "commit identity",
			body: `set -euo pipefail
mock_identity() {
  printf '%040d %040d\n' 1 2
  return 7
}
policy_identity="$(mock_identity)" || exit 91
read -r policy_commit policy_parent extra <<<"${policy_identity}" || exit 92
`,
			code: 91,
		},
		{
			name: "changed files",
			body: `set -euo pipefail
mock_diff() {
  printf '%s\n' $'M\t.github/workflows/materialize-control-plane-release-baseline-carrier-rp0.yml'
  printf '%s\n' $'M\tinternal/platformsafety/release_workflow_test.go'
  return 7
}
actual_changes_text="$(mock_diff)" || exit 93
mapfile -t actual_changes <<<"${actual_changes_text}"
`,
			code: 93,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command("bash")
			command.Stdin = strings.NewReader(test.body)
			output, err := command.CombinedOutput()
			exitError, ok := err.(*exec.ExitError)
			if !ok || exitError.ExitCode() != test.code {
				t.Fatalf("valid source output followed by failure was not rejected at capture: err=%v output=%q", err, output)
			}
		})
	}
}

func TestRP0CarrierMaterializerObjectReadbackSettlementMock(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "materialize-control-plane-release-baseline-carrier-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 carrier materializer workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 carrier materializer workflow: %v", err)
	}
	job := workflow.Jobs["materialize-forward-carrier"]
	var materialize releaseWorkflowStep
	for _, step := range job.Steps {
		if step.Name == "Materialize canonical forward carrier objects without moving a ref" {
			materialize = step
		}
	}
	if materialize.Run == "" {
		t.Fatal("carrier materializer run body is absent")
	}

	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatalf("create carrier fixture repo: %v", err)
	}
	runGit := func(input string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		command.Stdin = strings.NewReader(input)
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Fugue Carrier Test",
			"GIT_AUTHOR_EMAIL=carrier-test@fugue.invalid",
			"GIT_AUTHOR_DATE=2026-07-18T00:00:00Z",
			"GIT_COMMITTER_NAME=Fugue Carrier Test",
			"GIT_COMMITTER_EMAIL=carrier-test@fugue.invalid",
			"GIT_COMMITTER_DATE=2026-07-18T00:00:00Z",
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%q", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("", "init", "--quiet")
	runGit("", "symbolic-ref", "HEAD", "refs/heads/main")
	writeCommit := func(name, content string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repo, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write carrier fixture: %v", err)
		}
		runGit("", "add", "--", name)
		runGit("", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", name)
		return runGit("", "rev-parse", "HEAD")
	}
	runtimeSHA := writeCommit("runtime.txt", "runtime\n")
	policySHA := writeCommit("policy.txt", "policy\n")
	rootPayload := fmt.Sprintf(`{"previous_baseline_object_sha":null,"runtime_sha":"%s","schema_version":1}`+"\n", runtimeSHA)
	rootBlob := runGit(rootPayload, "hash-object", "-w", "--stdin")
	rootTree := runGit(fmt.Sprintf("100644 blob %s\tfugue-runtime-baseline.json\n", rootBlob), "mktree")
	previousObject := runGit("", "commit-tree", rootTree, "-m", "fugue runtime baseline")
	carrierPayload := fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`+"\n", previousObject, runtimeSHA)
	carrierBlob := runGit(carrierPayload, "hash-object", "-w", "--stdin")
	carrierTree := runGit(fmt.Sprintf("100644 blob %s\tfugue-runtime-baseline.json\n", carrierBlob), "mktree")
	carrierMessage := "fugue runtime baseline carrier " + runtimeSHA
	carrierContent := fmt.Sprintf(
		"tree %s\nparent %s\nauthor Fugue Release Baseline <release-baseline@fugue.invalid> 1784332800 +0000\ncommitter Fugue Release Baseline <release-baseline@fugue.invalid> 1784332800 +0000\n\n%s",
		carrierTree,
		previousObject,
		carrierMessage,
	)
	expectedCarrierSHA := runGit(carrierContent, "hash-object", "-t", "commit", "--stdin")
	if withTrailingLF := runGit(carrierContent+"\n", "hash-object", "-t", "commit", "--stdin"); withTrailingLF == expectedCarrierSHA {
		t.Fatal("carrier fixture does not distinguish the GitHub REST message bytes from commit-tree's trailing LF")
	}

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create carrier mock bin: %v", err)
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
import base64, json, subprocess, sys
content = subprocess.check_output(["git", "cat-file", "blob", sys.argv[1]])
print(json.dumps({"sha": sys.argv[1], "encoding": "base64", "content": base64.b64encode(content).decode("ascii")}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/trees/'* ]]; then
  sha="${arguments##*/}"
  python3 - "${sha}" <<'PY'
import json, subprocess, sys
line = subprocess.check_output(["git", "ls-tree", sys.argv[1]], text=True).rstrip("\n")
metadata, path = line.split("\t", 1)
mode, object_type, object_sha = metadata.split()
print(json.dumps({"sha": sys.argv[1], "truncated": False, "tree": [{"path": path, "mode": mode, "type": object_type, "sha": object_sha}]}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  sha="${arguments##*/}"
  [[ "${sha}" == "${EXPECTED_CARRIER_SHA}" ]] || exit 7
  python3 - "${sha}" <<'PY'
import json, os, sys
sha = sys.argv[1]
identity = {"name": "Fugue Release Baseline", "email": "release-baseline@fugue.invalid", "date": os.environ["CARRIER_DATE"]}
print(json.dumps({"sha": sha, "message": "fugue runtime baseline carrier " + os.environ["RUNTIME_SHA"], "tree": {"sha": os.environ["EXPECTED_METADATA_TREE_SHA"]}, "parents": [{"sha": os.environ["PREVIOUS_OBJECT_SHA"]}], "author": identity, "committer": identity}))
PY
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-baseline'* ]]; then
  printf '%s\n' "${PREVIOUS_OBJECT_SHA}"
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
			t.Fatalf("write carrier %s mock: %v", name, err)
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
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"EXPECTED_SHA="+policySHA,
			"PREVIOUS_OBJECT_SHA="+previousObject,
			"RUNTIME_SHA="+runtimeSHA,
			"CARRIER_DATE=2026-07-18T00:00:00Z",
			"EXPECTED_CARRIER_SHA="+expectedCarrierSHA,
			"EXPECTED_METADATA_TREE_SHA="+carrierTree,
			"GITHUB_REPOSITORY=fugue-test/repository",
			"GITHUB_OUTPUT="+outputPath,
			"GH_TOKEN=test-token",
		)
		combined, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read carrier gh log: %v", err)
		}
		published := ""
		if value, err := os.ReadFile(outputPath); err == nil {
			published = string(value)
		} else if !os.IsNotExist(err) {
			t.Fatalf("read carrier output: %v", err)
		}
		return result{posts: strings.Count(string(log), "--method POST"), output: published, log: string(combined), err: runErr}
	}

	for _, mode := range []string{"success", "blob_lost", "tree_lost", "commit_lost"} {
		t.Run(mode, func(t *testing.T) {
			got := runMaterializer(t, mode)
			if got.err != nil || got.posts != 3 {
				t.Fatalf("carrier object settlement failed: mode=%s err=%v posts=%d output=%q log=%q", mode, got.err, got.posts, got.output, got.log)
			}
			outputs := map[string]string{}
			for _, line := range strings.Split(strings.TrimSpace(got.output), "\n") {
				key, value, ok := strings.Cut(line, "=")
				if !ok || outputs[key] != "" {
					t.Fatalf("carrier output is malformed: %q", got.output)
				}
				outputs[key] = value
			}
			carrierSHA := outputs["carrier_commit_sha"]
			if len(outputs) != 6 || carrierSHA != expectedCarrierSHA {
				t.Fatalf("carrier output topology drifted: mode=%s output=%q", mode, got.output)
			}
			wantStatus := map[string]string{"blob_transport_status": "0", "tree_transport_status": "0", "commit_transport_status": "0"}
			if mode != "success" {
				wantStatus[strings.TrimSuffix(mode, "_lost")+"_transport_status"] = "7"
			}
			for key, want := range wantStatus {
				if outputs[key] != want {
					t.Fatalf("carrier transport status drifted: mode=%s key=%s got=%q want=%q", mode, key, outputs[key], want)
				}
			}
		})
	}
	t.Run("blob absent after failed transport", func(t *testing.T) {
		got := runMaterializer(t, "blob_absent")
		if got.err == nil || got.posts != 1 || got.output != "" {
			t.Fatalf("carrier writer did not fail closed for absent blob: err=%v posts=%d output=%q log=%q", got.err, got.posts, got.output, got.log)
		}
	})
}

func TestRP0CarrierRefCASIsHostedSingleMutationAndWriterLast(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "advance-control-plane-release-baseline-carrier-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 carrier ref CAS workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "92754dae6a1b8dae6af9dac9bdd0d5a103075de27ff7b105ec34c449f893b95e")
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
		t.Fatalf("parse RP0 carrier ref CAS workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "advance-forward-carrier-ref")
	jobNode := workflowMappingValue(t, jobsNode, "advance-forward-carrier-ref")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")

	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("carrier ref CAS must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode carrier ref CAS workflow_dispatch: %v", err)
	}
	wantInputs := []string{
		"carrier_commit_sha", "carrier_result_artifact_digest", "carrier_result_artifact_id",
		"carrier_result_run_id", "expected_previous_object_sha", "expected_sha",
	}
	if len(dispatch.Inputs) != len(wantInputs) {
		t.Fatalf("carrier ref CAS input inventory drifted: %+v", dispatch.Inputs)
	}
	for _, name := range wantInputs {
		node, exists := dispatch.Inputs[name]
		if !exists {
			t.Fatalf("carrier ref CAS input %s is absent", name)
		}
		var input releaseWorkflowDispatchInput
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode carrier ref CAS input %s: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("carrier ref CAS input %s must be required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("carrier ref CAS top-level boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["advance-forward-carrier-ref"]
	if !ok {
		t.Fatal("carrier ref CAS job is absent")
	}
	wantPermissions := map[string]string{"actions": "read", "contents": "write"}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, wantPermissions) {
		t.Fatalf("carrier ref CAS job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact carrier ref CAS policy SHA",
		"Verify exact carrier ref CAS authorization",
		"Write carrier ref CAS intent evidence",
		"Upload carrier ref CAS intent evidence",
		"Observe unchanged health before carrier ref CAS",
		"Advance baseline ref by one exact forward CAS",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("carrier ref CAS step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		step := job.Steps[index]
		if step.Name != name || step.If != "" || step.ContinueOnError {
			t.Fatalf("carrier ref CAS step %d drifted: %+v", index, step)
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("carrier ref CAS checkout drifted: %+v", checkout)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"advance-forward-carrier-ref": {Steps: job.Steps},
	}, map[string]string{
		"advance-forward-carrier-ref/Verify exact carrier ref CAS authorization":      "6a07281a0f4dc39301172fbdf2d5e1a3591408e77baf0e3b4a171b8dbc7216d9",
		"advance-forward-carrier-ref/Write carrier ref CAS intent evidence":           "66c982f564c8c5dd175e5840e8467e4157657977beea9b32fabb23782f8c6e3c",
		"advance-forward-carrier-ref/Observe unchanged health before carrier ref CAS": "fc5ae03d78d5939860dee55790f46d2ce0b560114cf2a78b5d0fb4ace08c230e",
		"advance-forward-carrier-ref/Advance baseline ref by one exact forward CAS":   "8eae6b19475d7182a1263f5f21cca7d55879396f98c2c5c23e5ff2b767828ef4",
	})

	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                   "${{ inputs.expected_sha }}",
		"EXPECTED_PREVIOUS_OBJECT_SHA":   "${{ inputs.expected_previous_object_sha }}",
		"CARRIER_COMMIT_SHA":             "${{ inputs.carrier_commit_sha }}",
		"CARRIER_RESULT_RUN_ID":          "${{ inputs.carrier_result_run_id }}",
		"CARRIER_RESULT_ARTIFACT_ID":     "${{ inputs.carrier_result_artifact_id }}",
		"CARRIER_RESULT_ARTIFACT_DIGEST": "${{ inputs.carrier_result_artifact_digest }}",
		"HEALTH_URL":                     "${{ vars.FUGUE_CONTROL_PLANE_RP0_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                       "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("carrier ref CAS verifier boundary drifted: %+v", verify)
	}
	for _, required := range []string{
		`policy_identity="$(git rev-list --parents -n 1 "${GITHUB_SHA}")" || exit 1`,
		`actual_changes_text="$(git diff --no-renames --name-status "${policy_parent}" "${GITHUB_SHA}")" || exit 1`,
		`A\t.github/workflows/advance-control-plane-release-baseline-carrier-rp0.yml`,
		`"${writer_state}" == 'disabled_manually'`,
		`"${deploy_state}" == 'disabled_manually'`,
		`"${run_head}" == "${policy_parent}"`,
		`"${artifact_digest}" == "${CARRIER_RESULT_ARTIFACT_DIGEST}"`,
		`names != ["intent.json"]`,
		`"carrier-object-materialized-ref-unchanged"`,
		`payload["transport_status"] != {"blob": 0, "tree": 0, "commit": 0}`,
		`parents[0].get("sha") != previous_sha`,
		`content != expected_content`,
		`git merge-base --is-ancestor "${runtime_sha}" "${GITHUB_SHA}"`,
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("carrier ref CAS verifier must contain %q", required)
		}
	}
	if strings.Contains(verify.Run, `< <(`) {
		t.Fatal("carrier ref CAS verifier must not hide command status through process substitution")
	}

	intentUpload := job.Steps[3]
	if intentUpload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		intentUpload.With["if-no-files-found"] != "error" || intentUpload.With["retention-days"] != "90" {
		t.Fatalf("carrier ref CAS intent upload drifted: %+v", intentUpload)
	}
	advance := job.Steps[len(job.Steps)-1]
	for _, required := range []string{
		`beforeOid:$beforeOid`, `afterOid:$afterOid`, `force:$force`,
		`-f "beforeOid=${PREVIOUS_OBJECT_SHA}"`, `-f "afterOid=${CARRIER_COMMIT_SHA}"`,
		`-F 'force=false'`, `"${writer_state}" == 'disabled_manually'`,
		`"${deploy_state}" == 'disabled_manually'`,
		`"${observed}" == "${CARRIER_COMMIT_SHA}"`, `exit 0`,
	} {
		if !strings.Contains(advance.Run, required) {
			t.Fatalf("carrier ref CAS writer must contain %q", required)
		}
	}
	if strings.Count(advance.Run, "updateRefs(input:") != 1 ||
		strings.Count(advance.Run, "-F 'force=false'") != 1 ||
		strings.Contains(advance.Run, "GITHUB_OUTPUT") {
		t.Fatalf("carrier ref CAS mutation inventory drifted:\n%s", advance.Run)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "kubectl ", "helm ", "ssh ",
		"git push", "git update-ref", "--force-with-lease", "--method PATCH", "--method PUT",
		"--method DELETE", " -X ", "force=true", "createRef", "deleteRef", "docker ",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("carrier ref CAS contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP0CarrierRefCASReadbackSettlesOneMutation(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "advance-control-plane-release-baseline-carrier-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 carrier ref CAS workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 carrier ref CAS workflow: %v", err)
	}
	job := workflow.Jobs["advance-forward-carrier-ref"]
	if len(job.Steps) == 0 {
		t.Fatal("carrier ref CAS steps are absent")
	}
	advance := job.Steps[len(job.Steps)-1]
	if advance.Name != "Advance baseline ref by one exact forward CAS" || advance.Run == "" {
		t.Fatalf("carrier ref CAS terminal step drifted: %+v", advance)
	}

	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create carrier ref CAS mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
if [[ "${arguments}" == *'graphql'*'repository(owner:'* ]]; then
  printf '%s\n' 'repository-node-id'
  exit 0
fi
if [[ "${arguments}" == *'graphql'*'updateRefs(input:'* ]]; then
  case "${MODE}" in
    success)
      printf '%s\n' "${CARRIER_COMMIT_SHA}" >"${STATE_FILE}"
      printf '%s\n' "fugue-rp0-carrier-ref-${PREVIOUS_OBJECT_SHA:0:12}-${CARRIER_COMMIT_SHA:0:12}"
      ;;
    mutation_lost)
      printf '%s\n' "${CARRIER_COMMIT_SHA}" >"${STATE_FILE}"
      exit 7
      ;;
    wrong_echo)
      printf '%s\n' "${CARRIER_COMMIT_SHA}" >"${STATE_FILE}"
      printf '%s\n' 'wrong-echo'
      ;;
    no_settle)
      exit 7
      ;;
    divergent)
      printf '%040d\n' 3 >"${STATE_FILE}"
      exit 7
      ;;
    unreadable)
      printf '%s\n' "${CARRIER_COMMIT_SHA}" >"${STATE_FILE}"
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
  value="$(<"${STATE_FILE}")"
  printf '%s\n' "${value}"
  exit 0
fi
if [[ "${arguments}" == *'/actions/workflows/materialize-control-plane-release-baseline-carrier-rp0.yml'* ]] ||
   [[ "${arguments}" == *'/actions/workflows/deploy-control-plane.yml'* ]]; then
  printf '%s\n' 'disabled_manually'
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-baseline'* ]]; then
  [[ "${MODE}" != 'unreadable' ]] || exit 7
  value="$(<"${STATE_FILE}")"
  printf '%s\n' "${value}"
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
	sleepMock := "#!/usr/bin/env bash\nexit 0\n"
	for name, source := range map[string]string{"gh": ghMock, "timeout": timeoutMock, "sleep": sleepMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write carrier ref CAS %s mock: %v", name, err)
		}
	}

	previous := strings.Repeat("1", 40)
	carrier := strings.Repeat("2", 40)
	policy := strings.Repeat("4", 40)
	type result struct {
		mutations int
		state     string
		err       error
		output    string
	}
	runCAS := func(t *testing.T, mode string) result {
		t.Helper()
		caseDir := t.TempDir()
		statePath := filepath.Join(caseDir, "state")
		logPath := filepath.Join(caseDir, "gh.log")
		if err := os.WriteFile(statePath, []byte(previous+"\n"), 0o600); err != nil {
			t.Fatalf("write carrier ref CAS state: %v", err)
		}
		command := exec.Command("bash")
		command.Stdin = strings.NewReader(advance.Run)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"STATE_FILE="+statePath,
			"LOG_FILE="+logPath,
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policy,
			"EXPECTED_SHA="+policy,
			"GITHUB_REPOSITORY=fugue-test/repository",
			"GITHUB_REPOSITORY_OWNER=fugue-test",
			"PREVIOUS_OBJECT_SHA="+previous,
			"CARRIER_COMMIT_SHA="+carrier,
			"GH_TOKEN=test-token",
		)
		output, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read carrier ref CAS mock log: %v", err)
		}
		state, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatalf("read carrier ref CAS mock state: %v", err)
		}
		return result{
			mutations: strings.Count(string(log), "updateRefs(input:"),
			state:     strings.TrimSpace(string(state)),
			err:       runErr,
			output:    string(output),
		}
	}

	for _, mode := range []string{"success", "mutation_lost", "wrong_echo"} {
		t.Run(mode, func(t *testing.T) {
			got := runCAS(t, mode)
			if got.err != nil || got.mutations != 1 || got.state != carrier {
				t.Fatalf("carrier ref CAS did not settle exact target: mode=%s err=%v mutations=%d state=%q output=%q", mode, got.err, got.mutations, got.state, got.output)
			}
		})
	}
	for _, mode := range []string{"no_settle", "divergent", "unreadable"} {
		t.Run(mode, func(t *testing.T) {
			got := runCAS(t, mode)
			if got.err == nil || got.mutations != 1 {
				t.Fatalf("carrier ref CAS did not fail closed: mode=%s err=%v mutations=%d state=%q output=%q", mode, got.err, got.mutations, got.state, got.output)
			}
		})
	}
}

func TestRP0MetadataReaderIsHostedReadOnlyAndEvidenceBound(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "validate-control-plane-release-baseline-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 metadata reader workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "343665adfa8a23979958b7f9e28936d6209b6ea0cadafa5b8277f7ed563b2cc3")
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
		t.Fatalf("parse RP0 metadata reader workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "validate-metadata-object")
	jobNode := workflowMappingValue(t, jobsNode, "validate-metadata-object")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")
	stepsNode := workflowMappingValue(t, jobNode, "steps")
	if stepsNode.Kind != yaml.SequenceNode || len(stepsNode.Content) != 6 {
		t.Fatalf("RP0 metadata reader step node inventory drifted: %+v", stepsNode)
	}
	wantStepKeys := [][]string{
		{"name", "uses", "with"},
		{"name", "id", "env", "run"},
		{"name", "env", "run"},
		{"name", "env", "run"},
		{"name", "env", "run"},
		{"name", "uses", "with"},
	}
	for index, stepNode := range stepsNode.Content {
		assertWorkflowMappingKeys(t, stepNode, wantStepKeys[index]...)
	}
	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("RP0 metadata reader must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode RP0 metadata reader dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_sha", "metadata_commit_sha", "metadata_result_run_id",
		"metadata_result_artifact_id", "metadata_result_artifact_digest",
	}
	if len(dispatch.Inputs) != len(wantInputs) {
		t.Fatalf("RP0 metadata reader input inventory drifted: %+v", dispatch.Inputs)
	}
	for _, name := range wantInputs {
		node, exists := dispatch.Inputs[name]
		if !exists {
			t.Fatalf("RP0 metadata reader input %s is absent", name)
		}
		var input releaseWorkflowDispatchInput
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode RP0 metadata reader input %s: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("RP0 metadata reader input %s must be required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("RP0 metadata reader must have empty top permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-metadata-object"]
	if !ok {
		t.Fatal("RP0 metadata reader job is absent")
	}
	wantPermissions := map[string]string{"actions": "read", "contents": "read"}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, wantPermissions) {
		t.Fatalf("RP0 metadata reader job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact RP0 reader target without persisted credentials",
		"Verify exact reader authorization and prior metadata result",
		"Validate canonical metadata object chain",
		"Observe unchanged production health after metadata validation",
		"Write RP0 metadata reader evidence",
		"Upload RP0 metadata reader evidence",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("RP0 metadata reader step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		if job.Steps[index].Name != name || job.Steps[index].If != "" || job.Steps[index].ContinueOnError {
			t.Fatalf("RP0 metadata reader step %d drifted: %+v", index, job.Steps[index])
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("RP0 metadata reader checkout drifted: %+v", checkout)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"validate-metadata-object": {Steps: job.Steps},
	}, map[string]string{
		"validate-metadata-object/Verify exact reader authorization and prior metadata result":   "f4991c89f5042d8117b8d0fc5448920c9402f04821e00ff47db30c943cee705c",
		"validate-metadata-object/Validate canonical metadata object chain":                      "eb12f66733ac38727f048109710ee4376a7da8500e811f264811e76cfadc1fa8",
		"validate-metadata-object/Observe unchanged production health after metadata validation": "78d2c64060feeb66255d0004f7c52068a66b305c49527dda984a9752c3a43d7e",
		"validate-metadata-object/Write RP0 metadata reader evidence":                            "9353540efaf7a4c3ca97f76b2b48db62fd6e4e31676c5a993bb97083489abb53",
	})
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":                    "${{ inputs.expected_sha }}",
		"METADATA_COMMIT_SHA":             "${{ inputs.metadata_commit_sha }}",
		"METADATA_RESULT_RUN_ID":          "${{ inputs.metadata_result_run_id }}",
		"METADATA_RESULT_ARTIFACT_ID":     "${{ inputs.metadata_result_artifact_id }}",
		"METADATA_RESULT_ARTIFACT_DIGEST": "${{ inputs.metadata_result_artifact_digest }}",
		"HEALTH_URL":                      "${{ vars.FUGUE_CONTROL_PLANE_RP0_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":                        "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("RP0 metadata reader verifier drifted: %+v", verify)
	}
	for _, required := range []string{
		`$'A\t.github/workflows/validate-control-plane-release-baseline-rp0.yml'`,
		"metadata-object-materialized-ref-absent", "missing or ambiguous metadata result artifact",
		"metadata result artifact inventory drifted", "metadata result commit binding drifted",
		"metadata result schema version drifted", "metadata result recorded_at is not canonical RFC3339 UTC",
		"sha256:$(sha256sum", "runtime_baseline_sha=%s", "runtime_artifact_digest=%s", "metadata_tree_sha=%s",
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("RP0 metadata reader verifier must contain %q", required)
		}
	}
	validate := job.Steps[2]
	wantValidateEnv := map[string]string{
		"RUNTIME_BASELINE_SHA":    "${{ steps.verify.outputs.runtime_baseline_sha }}",
		"RUNTIME_RUN_ID":          "${{ steps.verify.outputs.runtime_run_id }}",
		"RUNTIME_ARTIFACT_ID":     "${{ steps.verify.outputs.runtime_artifact_id }}",
		"RUNTIME_ARTIFACT_DIGEST": "${{ steps.verify.outputs.runtime_artifact_digest }}",
		"METADATA_BLOB_SHA":       "${{ steps.verify.outputs.metadata_blob_sha }}",
		"METADATA_TREE_SHA":       "${{ steps.verify.outputs.metadata_tree_sha }}",
		"METADATA_COMMIT_SHA":     "${{ steps.verify.outputs.metadata_commit_sha }}",
		"GH_TOKEN":                "${{ github.token }}",
	}
	if !reflect.DeepEqual(validate.Env, wantValidateEnv) {
		t.Fatalf("RP0 metadata reader object validator environment drifted: got %+v want %+v", validate.Env, wantValidateEnv)
	}
	for _, required := range []string{
		`"repos/${GITHUB_REPOSITORY}/actions/runs/${RUNTIME_RUN_ID}"`,
		`"repos/${GITHUB_REPOSITORY}/actions/runs/${RUNTIME_RUN_ID}/artifacts"`,
		"missing or ambiguous runtime baseline artifact", "${runtime_head}", "${RUNTIME_ARTIFACT_DIGEST}",
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${METADATA_BLOB_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${METADATA_TREE_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${METADATA_COMMIT_SHA}"`,
		`"previous_baseline_object_sha": None`, `commit.get("parents") != []`,
		"git merge-base --is-ancestor", `"${baseline_count}" == '0'`,
	} {
		if !strings.Contains(validate.Run, required) {
			t.Fatalf("RP0 metadata reader object validator must contain %q", required)
		}
	}
	observe := job.Steps[3]
	for _, required := range []string{"for sample in 1 2 3 4 5", "sleep 15", `{"status": "ok"}`, `"${baseline_count}" == '0'`} {
		if !strings.Contains(observe.Run, required) {
			t.Fatalf("RP0 metadata reader observation must contain %q", required)
		}
	}
	evidence := job.Steps[4]
	wantEvidenceEnv := map[string]string{
		"RUNTIME_BASELINE_SHA":            "${{ steps.verify.outputs.runtime_baseline_sha }}",
		"RUNTIME_RUN_ID":                  "${{ steps.verify.outputs.runtime_run_id }}",
		"RUNTIME_ARTIFACT_ID":             "${{ steps.verify.outputs.runtime_artifact_id }}",
		"RUNTIME_ARTIFACT_DIGEST":         "${{ steps.verify.outputs.runtime_artifact_digest }}",
		"METADATA_BLOB_SHA":               "${{ steps.verify.outputs.metadata_blob_sha }}",
		"METADATA_TREE_SHA":               "${{ steps.verify.outputs.metadata_tree_sha }}",
		"METADATA_COMMIT_SHA":             "${{ steps.verify.outputs.metadata_commit_sha }}",
		"METADATA_RESULT_RUN_ID":          "${{ inputs.metadata_result_run_id }}",
		"METADATA_RESULT_ARTIFACT_ID":     "${{ inputs.metadata_result_artifact_id }}",
		"METADATA_RESULT_ARTIFACT_DIGEST": "${{ inputs.metadata_result_artifact_digest }}",
	}
	if !reflect.DeepEqual(evidence.Env, wantEvidenceEnv) {
		t.Fatalf("RP0 metadata reader evidence environment drifted: got %+v want %+v", evidence.Env, wantEvidenceEnv)
	}
	for _, required := range []string{
		`"runtime_run_id": os.environ["RUNTIME_RUN_ID"]`,
		`"runtime_artifact_digest": os.environ["RUNTIME_ARTIFACT_DIGEST"]`,
		`"metadata_result_run_id": os.environ["METADATA_RESULT_RUN_ID"]`,
		`"metadata_result_artifact_digest": os.environ["METADATA_RESULT_ARTIFACT_DIGEST"]`,
	} {
		if !strings.Contains(evidence.Run, required) {
			t.Fatalf("RP0 metadata reader evidence must contain %q", required)
		}
	}
	upload := job.Steps[5]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["name"] != "fugue-control-plane-rp0-metadata-reader-${{ github.run_id }}-${{ github.run_attempt }}" ||
		upload.With["path"] != "${{ runner.temp }}/fugue-rp0-metadata-reader/rp0-metadata-reader.json" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("RP0 metadata reader upload drifted: %+v", upload)
	}
	source := string(data)
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "--kubeconfig", "ssh ", "kubectl ", "docker ", "helm ",
		"--method", " -X ", "graphql", "git push", "git update-ref", "git/refs", "force=", "curl --request",
		"mapfile", "< <(",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("RP0 metadata reader contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP0MetadataReaderEvidenceValidatorAcceptsPublishedFixtureAndRejectsDrift(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "validate-control-plane-release-baseline-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 metadata reader workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 metadata reader workflow: %v", err)
	}
	steps := workflow.Jobs["validate-metadata-object"].Steps
	if len(steps) < 2 {
		t.Fatalf("RP0 metadata reader verifier step is absent: %+v", steps)
	}
	const commandMarker = `result_fields="$(python3 - `
	const heredocMarker = `<<'PY'` + "\n"
	start := strings.Index(steps[1].Run, commandMarker)
	if start < 0 {
		t.Fatal("RP0 metadata reader validator command is absent")
	}
	heredocOffset := strings.Index(steps[1].Run[start:], heredocMarker)
	if heredocOffset < 0 {
		t.Fatal("RP0 metadata reader validator heredoc is absent")
	}
	start += heredocOffset + len(heredocMarker)
	endOffset := strings.Index(steps[1].Run[start:], "\nPY\n")
	if endOffset < 0 {
		t.Fatal("RP0 metadata reader validator heredoc terminator is absent")
	}
	validator := steps[1].Run[start : start+endOffset]

	fixture := map[string]any{
		"baseline_transition":        "metadata-object-materialized-ref-absent",
		"cluster_mutation_attempted": false,
		"git_history_rewritten":      false,
		"metadata_blob_sha":          "1ab84b0dc7783f6fbd5796ed477005ffa0ead963",
		"metadata_commit_sha":        "0aca9c8869d7ac064d22c9b1e5477f30de4813b4",
		"metadata_ref_created":       false,
		"metadata_tree_sha":          "f5fbfb2758190fbf5fddab701e625ef9046bb812",
		"policy_sha":                 "7b3bf0507926934f102e8baabbaa376453407958",
		"recorded_at":                "2026-07-18T04:11:34.057929+00:00",
		"run_attempt":                "1",
		"run_id":                     "29630134601",
		"runtime_artifact_digest":    "sha256:4ff05d34019da02bc10dd8f465acb9166fb280334717d9f349851ff3bd5001bf",
		"runtime_artifact_id":        "8329699987",
		"runtime_baseline_ref":       "refs/heads/fugue-control-plane-release-baseline",
		"runtime_baseline_sha":       "92805aab5209348932b2c1db060e5c3c56ce4a2c",
		"runtime_run_id":             "29380409275",
		"schema_version":             1,
		"workflow":                   "migrate-control-plane-release-baseline-rp0",
	}
	runValidator := func(t *testing.T, value map[string]any, extraDirectory bool) ([]byte, error) {
		t.Helper()
		root := t.TempDir()
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal RP0 metadata result fixture: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "rp0-migration.json"), encoded, 0o600); err != nil {
			t.Fatalf("write RP0 metadata result fixture: %v", err)
		}
		if extraDirectory {
			if err := os.Mkdir(filepath.Join(root, "unexpected-empty-directory"), 0o700); err != nil {
				t.Fatalf("write RP0 metadata result inventory drift: %v", err)
			}
		}
		command := exec.Command("python3", "-", root, fixture["policy_sha"].(string), fixture["run_id"].(string), fixture["metadata_commit_sha"].(string))
		command.Stdin = strings.NewReader(validator)
		return command.CombinedOutput()
	}

	output, err := runValidator(t, fixture, false)
	if err != nil {
		t.Fatalf("published RP0 metadata result fixture must pass: %v\n%s", err, output)
	}
	wantOutput := strings.Join([]string{
		"92805aab5209348932b2c1db060e5c3c56ce4a2c",
		"29380409275",
		"8329699987",
		"sha256:4ff05d34019da02bc10dd8f465acb9166fb280334717d9f349851ff3bd5001bf",
		"1ab84b0dc7783f6fbd5796ed477005ffa0ead963",
		"f5fbfb2758190fbf5fddab701e625ef9046bb812",
	}, "\t") + "\n"
	if string(output) != wantOutput {
		t.Fatalf("published RP0 metadata result projection drifted: got %q want %q", output, wantOutput)
	}

	tests := []struct {
		name           string
		mutate         func(map[string]any)
		extraDirectory bool
	}{
		{name: "boolean schema", mutate: func(value map[string]any) { value["schema_version"] = true }},
		{name: "integer runtime run ID", mutate: func(value map[string]any) { value["runtime_run_id"] = 29380409275 }},
		{name: "integer runtime artifact ID", mutate: func(value map[string]any) { value["runtime_artifact_id"] = 8329699987 }},
		{name: "noncanonical recorded at", mutate: func(value map[string]any) { value["recorded_at"] = "2026-07-18 04:11:34Z" }},
		{name: "extra empty directory", mutate: func(map[string]any) {}, extraDirectory: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := make(map[string]any, len(fixture))
			for key, value := range fixture {
				mutated[key] = value
			}
			test.mutate(mutated)
			if output, err := runValidator(t, mutated, test.extraDirectory); err == nil {
				t.Fatalf("RP0 metadata result drift must fail; output=%q", output)
			}
		})
	}
}

func TestRP0MetadataReaderCommandCapturesRejectValidOutputFollowedByFailure(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "validate-control-plane-release-baseline-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 metadata reader workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 metadata reader workflow: %v", err)
	}
	steps := workflow.Jobs["validate-metadata-object"].Steps
	if len(steps) < 3 {
		t.Fatalf("RP0 metadata reader command-bearing steps are absent: %+v", steps)
	}
	captures := steps[1].Run + "\n" + steps[2].Run
	if strings.Contains(captures, `<<<"$(`) {
		t.Fatal("RP0 metadata reader must not parse a command substitution directly through a here-string")
	}
	for _, required := range []string{
		`target_parent_fields="$(git rev-list`,
		`result_run_fields="$(`,
		`metadata_artifact_fields="$(`,
		`runtime_run_fields="$(`,
		`runtime_artifact_fields="$(`,
		`)" || exit 1`,
	} {
		if !strings.Contains(captures, required) {
			t.Fatalf("RP0 metadata reader fail-closed capture must contain %q", required)
		}
	}

	const validOutputThenFailure = `set -euo pipefail
mock_command() {
  printf '%s\t%s\n' valid fields
  return 7
}
captured="$(mock_command)" || exit 91
IFS=$'\t' read -r first second extra <<<"${captured}" || exit 92
[[ "${first}" == valid && "${second}" == fields && -z "${extra:-}" ]]
`
	command := exec.Command("bash")
	command.Stdin = strings.NewReader(validOutputThenFailure)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("valid-looking command output followed by failure must be rejected: %q", output)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 91 {
		t.Fatalf("fail-closed capture rejected at the wrong boundary: err=%v output=%q", err, output)
	}
}

func TestRP0BaselineRefCreatorIsHostedEvidenceBoundAndAtomic(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "create-control-plane-release-baseline-ref-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 baseline ref creator workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "1bdba74b763fcd6aa2d3b74e79f5eecca0a8a8f296b994bf75582bbdf9193625")
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
		t.Fatalf("parse RP0 baseline ref creator workflow: %v", err)
	}
	rootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, rootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowMappingKeys(t, workflowMappingValue(t, rootNode, "concurrency"), "group", "cancel-in-progress")
	jobsNode := workflowMappingValue(t, rootNode, "jobs")
	assertWorkflowMappingKeys(t, jobsNode, "create-forward-baseline-ref")
	jobNode := workflowMappingValue(t, jobsNode, "create-forward-baseline-ref")
	assertWorkflowMappingKeys(t, jobNode, "runs-on", "timeout-minutes", "environment", "permissions", "steps")
	stepsNode := workflowMappingValue(t, jobNode, "steps")
	if stepsNode.Kind != yaml.SequenceNode || len(stepsNode.Content) != 7 {
		t.Fatalf("RP0 baseline ref creator step node inventory drifted: %+v", stepsNode)
	}
	wantStepKeys := [][]string{
		{"name", "uses", "with"},
		{"name", "id", "env", "run"},
		{"name", "env", "run"},
		{"name", "env", "run"},
		{"name", "uses", "with"},
		{"name", "env", "run"},
		{"name", "env", "run"},
	}
	for index, stepNode := range stepsNode.Content {
		assertWorkflowMappingKeys(t, stepNode, wantStepKeys[index]...)
	}
	dispatchNode, ok := workflow.On["workflow_dispatch"]
	if !ok || len(workflow.On) != 1 {
		t.Fatalf("RP0 baseline ref creator must be dispatch-only: %+v", workflow.On)
	}
	var dispatch releaseWorkflowDispatchTrigger
	if err := dispatchNode.Decode(&dispatch); err != nil {
		t.Fatalf("decode RP0 baseline ref creator dispatch: %v", err)
	}
	wantInputs := []string{
		"expected_sha", "metadata_commit_sha", "reader_run_id", "reader_artifact_id", "reader_artifact_digest",
	}
	if len(dispatch.Inputs) != len(wantInputs) {
		t.Fatalf("RP0 baseline ref creator input inventory drifted: %+v", dispatch.Inputs)
	}
	for _, name := range wantInputs {
		node, exists := dispatch.Inputs[name]
		if !exists {
			t.Fatalf("RP0 baseline ref creator input %s is absent", name)
		}
		var input releaseWorkflowDispatchInput
		if err := node.Decode(&input); err != nil {
			t.Fatalf("decode RP0 baseline ref creator input %s: %v", name, err)
		}
		if !input.Required || input.Type != "string" || input.Default != nil {
			t.Fatalf("RP0 baseline ref creator input %s must be a required string without default: %+v", name, input)
		}
	}
	if len(workflow.Permissions) != 0 || len(workflow.Jobs) != 1 {
		t.Fatalf("RP0 baseline ref creator must have empty top permissions and one job: %+v", workflow)
	}
	job, ok := workflow.Jobs["create-forward-baseline-ref"]
	if !ok {
		t.Fatal("RP0 baseline ref creator job is absent")
	}
	wantPermissions := map[string]string{"actions": "read", "contents": "write"}
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 20 || job.Environment != "production" ||
		job.ContinueOnError || !reflect.DeepEqual(job.Permissions, wantPermissions) {
		t.Fatalf("RP0 baseline ref creator job boundary drifted: %+v", job)
	}
	wantSteps := []string{
		"Checkout exact RP0 ref writer target without persisted credentials",
		"Verify exact ref writer authorization and hosted reader evidence",
		"Revalidate canonical metadata object chain before ref creation",
		"Write RP0 ref creation intent evidence",
		"Upload RP0 ref creation intent evidence",
		"Observe unchanged production health before ref creation",
		"Create absent forward-only baseline ref at validated metadata root",
	}
	if len(job.Steps) != len(wantSteps) {
		t.Fatalf("RP0 baseline ref creator step inventory drifted: %+v", job.Steps)
	}
	for index, name := range wantSteps {
		if job.Steps[index].Name != name || job.Steps[index].If != "" || job.Steps[index].ContinueOnError {
			t.Fatalf("RP0 baseline ref creator step %d drifted: %+v", index, job.Steps[index])
		}
	}
	checkout := job.Steps[0]
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != "0" ||
		checkout.With["persist-credentials"] != "false" {
		t.Fatalf("RP0 baseline ref creator checkout drifted: %+v", checkout)
	}
	assertWorkflowRunDigests(t, map[string]releaseWorkflowJob{
		"create-forward-baseline-ref": {Steps: job.Steps},
	}, map[string]string{
		"create-forward-baseline-ref/Verify exact ref writer authorization and hosted reader evidence":   "b2f0ff29844f4d63d23363eb52e8a2a6b982c4b5fc293795b3af107ca908353a",
		"create-forward-baseline-ref/Revalidate canonical metadata object chain before ref creation":     "1e0a84fa1ff2c912146c4a7c76849839146fd3bac9a1ff1179352d1f400bd836",
		"create-forward-baseline-ref/Write RP0 ref creation intent evidence":                             "ffbed03dcf8d3a484c68ea48ee2abbe55c7e70fadf48df5e2a2aea79a7b5c9e1",
		"create-forward-baseline-ref/Observe unchanged production health before ref creation":            "8f0f923b1be9e85ba8a5887e35dfe0f5638e0239bba896cbdf748fe9fb3689e1",
		"create-forward-baseline-ref/Create absent forward-only baseline ref at validated metadata root": "540cef0f50e0677cca18ae41b2ddbb91889eeaf9fade80b03de417fced0e589d",
	})
	verify := job.Steps[1]
	wantVerifyEnv := map[string]string{
		"EXPECTED_SHA":           "${{ inputs.expected_sha }}",
		"METADATA_COMMIT_SHA":    "${{ inputs.metadata_commit_sha }}",
		"READER_RUN_ID":          "${{ inputs.reader_run_id }}",
		"READER_ARTIFACT_ID":     "${{ inputs.reader_artifact_id }}",
		"READER_ARTIFACT_DIGEST": "${{ inputs.reader_artifact_digest }}",
		"HEALTH_URL":             "${{ vars.FUGUE_CONTROL_PLANE_RP0_HEALTH_URL || 'https://api.fugue.pro/healthz' }}",
		"GH_TOKEN":               "${{ github.token }}",
	}
	if verify.ID != "verify" || !reflect.DeepEqual(verify.Env, wantVerifyEnv) {
		t.Fatalf("RP0 baseline ref creator verifier drifted: %+v", verify)
	}
	for _, required := range []string{
		`$'A\t.github/workflows/create-control-plane-release-baseline-ref-rp0.yml'`,
		"missing or ambiguous metadata reader artifact", "metadata reader artifact inventory drifted",
		"metadata reader policy attribution drifted", "metadata reader commit binding drifted",
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/validate-control-plane-release-baseline-rp0.yml"`,
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/migrate-control-plane-release-baseline-rp0.yml"`,
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/deploy-control-plane.yml"`,
		"runtime_artifact_digest=%s", "metadata_result_artifact_digest=%s", "health_url=%s",
	} {
		if !strings.Contains(verify.Run, required) {
			t.Fatalf("RP0 baseline ref creator verifier must contain %q", required)
		}
	}
	revalidate := job.Steps[2]
	for _, required := range []string{
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${METADATA_BLOB_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${METADATA_TREE_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${METADATA_COMMIT_SHA}"`,
		`{"previous_baseline_object_sha": None, "runtime_sha": runtime_sha, "schema_version": 1}`,
		`commit.get("parents") != []`, "git merge-base --is-ancestor", `"${baseline_count}" == '0'`,
	} {
		if !strings.Contains(revalidate.Run, required) {
			t.Fatalf("RP0 baseline ref creator object validator must contain %q", required)
		}
	}
	intent := job.Steps[3]
	for _, required := range []string{
		`"baseline_transition": "absent-to-validated-metadata-root-pending"`,
		`"metadata_ref_created": False`, `"cluster_mutation_attempted": False`,
		`"reader_artifact_digest": os.environ["READER_ARTIFACT_DIGEST"]`,
	} {
		if !strings.Contains(intent.Run, required) {
			t.Fatalf("RP0 baseline ref creator intent must contain %q", required)
		}
	}
	upload := job.Steps[4]
	if upload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		upload.With["name"] != "fugue-control-plane-rp0-baseline-ref-create-${{ github.run_id }}-${{ github.run_attempt }}" ||
		upload.With["path"] != "${{ runner.temp }}/fugue-rp0-baseline-ref-create/rp0-baseline-ref-create.json" ||
		upload.With["if-no-files-found"] != "error" || upload.With["retention-days"] != "90" {
		t.Fatalf("RP0 baseline ref creator intent upload drifted: %+v", upload)
	}
	observe := job.Steps[5]
	for _, required := range []string{
		"for sample in 1 2 3 4 5", "sleep 15", `{"status": "ok"}`, `"${baseline_count}" == '0'`,
		`"${reader_state}" == 'disabled_manually'`, `"${deploy_state}" == 'disabled_manually'`,
	} {
		if !strings.Contains(observe.Run, required) {
			t.Fatalf("RP0 baseline ref creator observation must contain %q", required)
		}
	}
	writer := job.Steps[6]
	wantWriterEnv := map[string]string{
		"EXPECTED_SHA":         "${{ inputs.expected_sha }}",
		"RUNTIME_BASELINE_SHA": "${{ steps.verify.outputs.runtime_baseline_sha }}",
		"METADATA_COMMIT_SHA":  "${{ steps.verify.outputs.metadata_commit_sha }}",
		"GH_TOKEN":             "${{ github.token }}",
	}
	if !reflect.DeepEqual(writer.Env, wantWriterEnv) {
		t.Fatalf("RP0 baseline ref creator writer environment drifted: got %+v want %+v", writer.Env, wantWriterEnv)
	}
	for _, required := range []string{
		"readonly baseline_ref='refs/heads/fugue-control-plane-release-baseline'",
		`"repos/${GITHUB_REPOSITORY}/git/commits/${METADATA_COMMIT_SHA}"`,
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/validate-control-plane-release-baseline-rp0.yml"`,
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/migrate-control-plane-release-baseline-rp0.yml"`,
		`"repos/${GITHUB_REPOSITORY}/actions/workflows/deploy-control-plane.yml"`,
		"gh api --method POST", `"repos/${GITHUB_REPOSITORY}/git/refs"`,
		`-f "ref=${baseline_ref}" -f "sha=${METADATA_COMMIT_SHA}"`,
		"create_status=0", `"${main_before_create}" == "${GITHUB_SHA}"`, "for settlement_attempt in 1 2 3 4 5",
		`"${observed_ref}" == "${METADATA_COMMIT_SHA}"`, `"${observed_ref}" == 'absent'`,
		`"${settled_ref}" == "${METADATA_COMMIT_SHA}"`,
	} {
		if !strings.Contains(writer.Run, required) {
			t.Fatalf("RP0 baseline ref creator writer must contain %q", required)
		}
	}
	if strings.Count(writer.Run, "gh api") != 9 || strings.Count(writer.Run, "gh api --method POST") != 1 ||
		strings.Count(writer.Run, `"repos/${GITHUB_REPOSITORY}/git/refs"`) != 1 {
		t.Fatalf("RP0 baseline ref creator writer API inventory drifted:\n%s", writer.Run)
	}
	source := string(data)
	if strings.Count(source, "gh api --method POST") != 1 {
		t.Fatalf("RP0 baseline ref creator must contain exactly one API write")
	}
	for _, forbidden := range []string{
		"self-hosted", "${{ secrets.", "KUBECONFIG", "--kubeconfig", "ssh ", "kubectl ", "docker ", "helm ",
		"--method PATCH", "--method PUT", "--method DELETE", " -X ", "graphql", "git push", "git update-ref",
		"--force-with-lease", "force=", "updateRefs", "createRef", "deleteRef", "mapfile", "< <(",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("RP0 baseline ref creator contains out-of-scope capability %q", forbidden)
		}
	}
}

func TestRP0BaselineRefCreatorWriterMockMatrix(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "create-control-plane-release-baseline-ref-rp0.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RP0 baseline ref creator workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse RP0 baseline ref creator workflow: %v", err)
	}
	steps := workflow.Jobs["create-forward-baseline-ref"].Steps
	if len(steps) != 7 {
		t.Fatalf("RP0 baseline ref creator writer step is absent: %+v", steps)
	}
	writer := steps[6].Run
	const policySHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const runtimeSHA = "92805aab5209348932b2c1db060e5c3c56ce4a2c"
	const metadataSHA = "0aca9c8869d7ac064d22c9b1e5477f30de4813b4"

	runWriter := func(t *testing.T, mode string) (int, bool, string, []byte, error) {
		t.Helper()
		root := t.TempDir()
		bin := filepath.Join(root, "bin")
		if err := os.Mkdir(bin, 0o700); err != nil {
			t.Fatalf("create mock bin: %v", err)
		}
		statePath := filepath.Join(root, "created")
		readbackCountPath := filepath.Join(root, "readback-count")
		logPath := filepath.Join(root, "gh.log")
		ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
endpoint=''
settlement='false'
for argument in "$@"; do
  case "${argument}" in repos/*) endpoint="${argument}"; break ;; esac
done
for argument in "$@"; do
  if [[ "${argument}" == *'then "absent"'* ]]; then settlement='true'; fi
done
case "${endpoint}" in
  */git/ref/heads/main)
    printf '%s\n' "${GITHUB_SHA}"
    ;;
  */git/matching-refs/heads/fugue-control-plane-release-baseline)
    if [[ -e "${STATE_FILE}" ]]; then
      if [[ "${settlement}" == 'true' && "${MODE}" == 'readback_transient' ]]; then
        readback_count=0
        if [[ -e "${READBACK_COUNT_FILE}" ]]; then read -r readback_count <"${READBACK_COUNT_FILE}"; fi
        readback_count=$((readback_count + 1))
        printf '%s\n' "${readback_count}" >"${READBACK_COUNT_FILE}"
        if [[ "${readback_count}" == '1' ]]; then exit 28; fi
      fi
      if [[ "${MODE}" == 'readback_wrong' ]]; then
        printf '%040d\n' 0
      else
        printf '%s\n' "${METADATA_COMMIT_SHA}"
      fi
    elif [[ "${MODE}" == 'baseline_exists' ]]; then
      printf '1\n'
    elif [[ "${settlement}" == 'true' ]]; then
      printf 'absent\n'
    else
      printf '0\n'
    fi
    ;;
  */git/commits/*)
    if [[ "${MODE}" == 'metadata_nonroot' ]]; then
      printf '%s\t1\n' "${METADATA_COMMIT_SHA}"
    else
      printf '%s\t0\n' "${METADATA_COMMIT_SHA}"
    fi
    ;;
  */actions/workflows/*)
    printf 'disabled_manually\n'
    ;;
  */git/refs)
    if [[ "${MODE}" == 'committed_exit7' ]]; then
      : >"${STATE_FILE}"
      printf '%s\t%s\tcommit\n' 'refs/heads/fugue-control-plane-release-baseline' "${METADATA_COMMIT_SHA}"
      exit 7
    fi
    if [[ "${MODE}" == 'post_failed_absent' ]]; then exit 7; fi
    : >"${STATE_FILE}"
    if [[ "${MODE}" == 'response_wrong_sha' ]]; then
      printf '%s\t%040d\tcommit\n' 'refs/heads/fugue-control-plane-release-baseline' 0
    else
      printf '%s\t%s\tcommit\n' 'refs/heads/fugue-control-plane-release-baseline' "${METADATA_COMMIT_SHA}"
    fi
    ;;
  *) exit 98 ;;
esac
`
		timeoutMock := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == '--kill-after=2s' ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 125
shift
exec "$@"
`
		sleepMock := `#!/usr/bin/env bash
set -euo pipefail
exit 0
`
		for name, source := range map[string]string{"gh": ghMock, "timeout": timeoutMock, "sleep": sleepMock} {
			mockPath := filepath.Join(bin, name)
			if err := os.WriteFile(mockPath, []byte(source), 0o700); err != nil {
				t.Fatalf("write %s mock: %v", name, err)
			}
		}
		command := exec.Command("bash")
		command.Stdin = strings.NewReader(writer)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"STATE_FILE="+statePath,
			"READBACK_COUNT_FILE="+readbackCountPath,
			"LOG_FILE="+logPath,
			"GITHUB_RUN_ATTEMPT=1",
			"GITHUB_SHA="+policySHA,
			"EXPECTED_SHA="+policySHA,
			"GITHUB_REPOSITORY=yym68686/fugue",
			"RUNTIME_BASELINE_SHA="+runtimeSHA,
			"METADATA_COMMIT_SHA="+metadataSHA,
			"GH_TOKEN=test-token",
		)
		output, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read gh mock log: %v", err)
		}
		_, stateErr := os.Stat(statePath)
		created := stateErr == nil
		if stateErr != nil && !os.IsNotExist(stateErr) {
			t.Fatalf("inspect mock ref state: %v", stateErr)
		}
		readbackCount := ""
		if count, err := os.ReadFile(readbackCountPath); err == nil {
			readbackCount = strings.TrimSpace(string(count))
		} else if !os.IsNotExist(err) {
			t.Fatalf("read settlement retry count: %v", err)
		}
		return strings.Count(string(log), "--method POST"), created, readbackCount, output, runErr
	}

	positive := []struct {
		mode              string
		wantReadbackCount string
	}{
		{mode: "success"},
		{mode: "committed_exit7"},
		{mode: "readback_transient", wantReadbackCount: "2"},
		{mode: "response_wrong_sha"},
	}
	for _, test := range positive {
		postCount, created, readbackCount, output, err := runWriter(t, test.mode)
		if err != nil || postCount != 1 || !created || readbackCount != test.wantReadbackCount {
			t.Fatalf("RP0 baseline ref creator settlement mock failed: mode=%s err=%v posts=%d created=%t readbacks=%q wantReadbacks=%q output=%q", test.mode, err, postCount, created, readbackCount, test.wantReadbackCount, output)
		}
	}
	tests := []struct {
		name        string
		mode        string
		wantPosts   int
		wantCreated bool
	}{
		{name: "baseline already exists", mode: "baseline_exists", wantPosts: 0, wantCreated: false},
		{name: "metadata commit is not root", mode: "metadata_nonroot", wantPosts: 0, wantCreated: false},
		{name: "POST fails and ref stays absent", mode: "post_failed_absent", wantPosts: 1, wantCreated: false},
		{name: "readback persistently has wrong SHA", mode: "readback_wrong", wantPosts: 1, wantCreated: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			posts, created, _, output, err := runWriter(t, test.mode)
			if err == nil || posts != test.wantPosts || created != test.wantCreated {
				t.Fatalf("RP0 baseline ref creator negative mock drifted: mode=%s err=%v posts=%d want=%d created=%t wantCreated=%t output=%q", test.mode, err, posts, test.wantPosts, created, test.wantCreated, output)
			}
		})
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

func TestControlPlaneMetadataBaselineResolverMockMatrix(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	resolver := workflowStepByName(t, workflow.Jobs["release-baseline"], "Resolve release-domain baseline").Run

	root := t.TempDir()
	origin := filepath.Join(root, "origin.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")
	if err := os.Mkdir(seed, 0o700); err != nil {
		t.Fatalf("create seed repository: %v", err)
	}
	runGit := func(dir, input string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = dir
		command.Stdin = strings.NewReader(input)
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Fugue Resolver Test",
			"GIT_AUTHOR_EMAIL=resolver-test@fugue.invalid",
			"GIT_AUTHOR_DATE=2026-07-18T00:00:00Z",
			"GIT_COMMITTER_NAME=Fugue Resolver Test",
			"GIT_COMMITTER_EMAIL=resolver-test@fugue.invalid",
			"GIT_COMMITTER_DATE=2026-07-18T00:00:00Z",
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%q", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}

	runGit(root, "", "init", "--quiet", "--bare", origin)
	runGit(root, "", "--git-dir="+origin, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(seed, "", "init", "--quiet")
	runGit(seed, "", "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(seed, "", "remote", "add", "origin", origin)
	writeCommit := func(name, contents, message string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(seed, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		runGit(seed, "", "add", "--", name)
		runGit(seed, "", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message)
		return runGit(seed, "", "rev-parse", "HEAD")
	}
	rootSHA := writeCommit("root.txt", "root\n", "root")
	baseSHA := writeCommit("base.txt", "base\n", "base")
	targetSHA := writeCommit("target.txt", "target\n", "target")
	rootTree := runGit(seed, "", "rev-parse", rootSHA+"^{tree}")
	unrelatedSHA := runGit(seed, "", "commit-tree", rootTree, "-m", "unrelated")
	runGit(seed, "", "push", "--quiet", "origin",
		targetSHA+":refs/heads/main", unrelatedSHA+":refs/heads/fixture-unrelated")

	makeMetadataCommit := func(blobContents string, extraFile, extraEmptyTree bool, parents ...string) string {
		t.Helper()
		blob := runGit(root, blobContents, "--git-dir="+origin, "hash-object", "-w", "--stdin")
		treeInput := fmt.Sprintf("100644 blob %s\tfugue-runtime-baseline.json\n", blob)
		if extraFile {
			extraBlob := runGit(root, "extra\n", "--git-dir="+origin, "hash-object", "-w", "--stdin")
			treeInput += fmt.Sprintf("100644 blob %s\textra.txt\n", extraBlob)
		}
		if extraEmptyTree {
			emptyTree := runGit(root, "", "--git-dir="+origin, "mktree")
			treeInput += fmt.Sprintf("040000 tree %s\textra-dir\n", emptyTree)
		}
		tree := runGit(root, treeInput, "--git-dir="+origin, "mktree")
		args := []string{"--git-dir=" + origin, "commit-tree", tree}
		for _, parent := range parents {
			args = append(args, "-p", parent)
		}
		args = append(args, "-m", "fugue runtime baseline")
		return runGit(root, "", args...)
	}
	canonicalPayload := fmt.Sprintf(`{"previous_baseline_object_sha":null,"runtime_sha":"%s","schema_version":1}`, baseSHA)
	canonicalMetadata := makeMetadataCommit(canonicalPayload+"\n", false, false)
	canonicalCarrierPayload := fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`, canonicalMetadata, baseSHA)
	canonicalCarrier := makeMetadataCommit(canonicalCarrierPayload+"\n", false, false, canonicalMetadata)
	secondCarrierPayload := fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`, canonicalCarrier, targetSHA)
	secondCarrier := makeMetadataCommit(secondCarrierPayload+"\n", false, false, canonicalCarrier)
	badSchemaMetadata := makeMetadataCommit(fmt.Sprintf(`{"previous_baseline_object_sha":null,"runtime_sha":"%s","schema_version":2}`+"\n", baseSHA), false, false)
	extraFileMetadata := makeMetadataCommit(canonicalPayload+"\n", true, false)
	extraEmptyTreeMetadata := makeMetadataCommit(canonicalPayload+"\n", false, true)
	missingNewlineMetadata := makeMetadataCommit(canonicalPayload, false, false)
	doubleNewlineMetadata := makeMetadataCommit(canonicalPayload+"\n\n", false, false)
	nulMetadata := makeMetadataCommit(canonicalPayload+"\x00\n", false, false)
	unrelatedMetadata := makeMetadataCommit(fmt.Sprintf(`{"previous_baseline_object_sha":null,"runtime_sha":"%s","schema_version":1}`+"\n", unrelatedSHA), false, false)
	rootWithPrevious := makeMetadataCommit(canonicalCarrierPayload+"\n", false, false)
	carrierWithNullPrevious := makeMetadataCommit(canonicalPayload+"\n", false, false, canonicalMetadata)
	carrierWithWrongPrevious := makeMetadataCommit(
		fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`+"\n", unrelatedSHA, baseSHA),
		false, false, canonicalMetadata,
	)
	carrierWithExtraParent := makeMetadataCommit(canonicalCarrierPayload+"\n", false, false, canonicalMetadata, unrelatedSHA)
	carrierWithInvalidPrevious := makeMetadataCommit(
		fmt.Sprintf(`{"previous_baseline_object_sha":"invalid","runtime_sha":"%s","schema_version":1}`+"\n", baseSHA),
		false, false, canonicalMetadata,
	)
	carrierWithUnrelatedRuntime := makeMetadataCommit(
		fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`+"\n", canonicalMetadata, unrelatedSHA),
		false, false, canonicalMetadata,
	)
	runGit(root, "", "clone", "--quiet", origin, checkout)
	if got := runGit(checkout, "", "rev-parse", "HEAD"); got != targetSHA {
		t.Fatalf("fixture target drifted: got %s want %s", got, targetSHA)
	}

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	timeoutMock := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --kill-after=* ]]; then shift; fi
[[ "${1:-}" =~ ^[0-9]+s$ ]] || exit 125
shift
exec "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "timeout"), []byte(timeoutMock), 0o700); err != nil {
		t.Fatalf("write timeout mock: %v", err)
	}
	const baselineRef = "refs/heads/fugue-control-plane-release-baseline"
	runResolver := func(t *testing.T, refObject string) (string, []byte, error) {
		t.Helper()
		runGit(root, "", "--git-dir="+origin, "update-ref", baselineRef, refObject)
		outputPath := filepath.Join(t.TempDir(), "github-output")
		command := exec.Command("bash")
		command.Dir = checkout
		command.Stdin = strings.NewReader(resolver)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"GITHUB_SHA="+targetSHA,
			"SOURCE_SHA="+targetSHA,
			"TARGET_SHA="+targetSHA,
			"GITHUB_OUTPUT="+outputPath,
		)
		output, runErr := command.CombinedOutput()
		published, readErr := os.ReadFile(outputPath)
		if readErr != nil && !os.IsNotExist(readErr) {
			t.Fatalf("read resolver output: %v", readErr)
		}
		return string(published), output, runErr
	}

	positive := []struct {
		name       string
		refObject  string
		wantDomain string
	}{
		{name: "direct code baseline", refObject: baseSHA, wantDomain: baseSHA},
		{name: "canonical metadata bridge", refObject: canonicalMetadata, wantDomain: baseSHA},
		{name: "canonical forward carrier", refObject: canonicalCarrier, wantDomain: baseSHA},
	}
	for _, test := range positive {
		t.Run(test.name, func(t *testing.T) {
			published, output, err := runResolver(t, test.refObject)
			if err != nil {
				t.Fatalf("resolver rejected valid baseline: err=%v output=%q", err, output)
			}
			want := fmt.Sprintf("domain_base_sha=%s\nbaseline_ref_object_sha=%s\nis_genesis=false\ngenesis_parent_sha=\n", test.wantDomain, test.refObject)
			if published != want {
				t.Fatalf("resolver output drifted: got %q want %q", published, want)
			}
		})
	}
	negative := []struct {
		name      string
		refObject string
	}{
		{name: "metadata schema drift", refObject: badSchemaMetadata},
		{name: "metadata tree has extra file", refObject: extraFileMetadata},
		{name: "metadata root tree has extra empty tree", refObject: extraEmptyTreeMetadata},
		{name: "metadata blob is missing final newline", refObject: missingNewlineMetadata},
		{name: "metadata blob has double final newline", refObject: doubleNewlineMetadata},
		{name: "metadata blob contains NUL", refObject: nulMetadata},
		{name: "metadata runtime is not target ancestor", refObject: unrelatedMetadata},
		{name: "metadata root has non-null previous object", refObject: rootWithPrevious},
		{name: "metadata carrier has null previous object", refObject: carrierWithNullPrevious},
		{name: "metadata carrier previous object mismatches parent", refObject: carrierWithWrongPrevious},
		{name: "metadata carrier has extra parent", refObject: carrierWithExtraParent},
		{name: "metadata carrier has invalid previous object", refObject: carrierWithInvalidPrevious},
		{name: "metadata carrier runtime is not target ancestor", refObject: carrierWithUnrelatedRuntime},
		{name: "baseline already equals target", refObject: secondCarrier},
	}
	for _, test := range negative {
		t.Run(test.name, func(t *testing.T) {
			published, output, err := runResolver(t, test.refObject)
			if err == nil || published != "" {
				identity := runGit(root, "", "--git-dir="+origin, "rev-list", "--parents", "-n", "1", test.refObject)
				tree := runGit(root, "", "--git-dir="+origin, "ls-tree", "--full-tree", test.refObject)
				t.Fatalf("resolver accepted unsafe baseline: err=%v published=%q output=%q identity=%q tree=%q", err, published, output, identity, tree)
			}
		})
	}
}

func TestControlPlaneBaselineRecorderSettlementMockMatrix(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	writer := workflowStepByName(t, workflow.Jobs["record-release-baseline"], "Advance dedicated forward-only release baseline branch").Run

	root := t.TempDir()
	origin := filepath.Join(root, "origin.git")
	seed := filepath.Join(root, "seed")
	checkout := filepath.Join(root, "checkout")
	if err := os.Mkdir(seed, 0o700); err != nil {
		t.Fatalf("create seed repository: %v", err)
	}
	runGit := func(dir, input string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = dir
		command.Stdin = strings.NewReader(input)
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Fugue Recorder Test",
			"GIT_AUTHOR_EMAIL=recorder-test@fugue.invalid",
			"GIT_AUTHOR_DATE=2026-07-18T00:00:00Z",
			"GIT_COMMITTER_NAME=Fugue Recorder Test",
			"GIT_COMMITTER_EMAIL=recorder-test@fugue.invalid",
			"GIT_COMMITTER_DATE=2026-07-18T00:00:00Z",
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v output=%q", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}

	runGit(root, "", "init", "--quiet", "--bare", origin)
	runGit(root, "", "--git-dir="+origin, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(seed, "", "init", "--quiet")
	runGit(seed, "", "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(seed, "", "remote", "add", "origin", origin)
	writeCommit := func(name, contents, message string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(seed, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		runGit(seed, "", "add", "--", name)
		runGit(seed, "", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message)
		return runGit(seed, "", "rev-parse", "HEAD")
	}
	baseSHA := writeCommit("base.txt", "base\n", "base")
	targetSHA := writeCommit("target.txt", "target\n", "target")
	runGit(seed, "", "push", "--quiet", "origin", targetSHA+":refs/heads/main")
	const baselineRef = "refs/heads/fugue-control-plane-release-baseline"
	basePayload := fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`+"\n", baseSHA, baseSHA)
	baseBlob := runGit(root, basePayload, "--git-dir="+origin, "hash-object", "-w", "--stdin")
	baseTree := runGit(root, fmt.Sprintf("100644 blob %s\tfugue-runtime-baseline.json\n", baseBlob), "--git-dir="+origin, "mktree")
	baseCarrier := runGit(root, "", "--git-dir="+origin, "commit-tree", baseTree, "-p", baseSHA, "-m", "fugue runtime baseline carrier "+baseSHA)

	targetPayload := fmt.Sprintf(`{"previous_baseline_object_sha":"%s","runtime_sha":"%s","schema_version":1}`+"\n", baseCarrier, targetSHA)
	targetBlob := runGit(root, targetPayload, "--git-dir="+origin, "hash-object", "-w", "--stdin")
	targetTree := runGit(root, fmt.Sprintf("100644 blob %s\tfugue-runtime-baseline.json\n", targetBlob), "--git-dir="+origin, "mktree")
	const carrierDate = "2026-07-18T00:00:00Z"
	carrierMessage := "fugue runtime baseline carrier " + targetSHA
	identity := "Fugue Release Baseline <release-baseline@fugue.invalid> 1784332800 +0000"
	carrierContent := fmt.Sprintf(
		"tree %s\nparent %s\nauthor %s\ncommitter %s\n\n%s",
		targetTree, baseCarrier, identity, identity, carrierMessage,
	)
	targetCarrier := runGit(root, carrierContent, "--git-dir="+origin, "hash-object", "-w", "-t", "commit", "--stdin")
	runGit(root, "", "--git-dir="+origin, "update-ref", baselineRef, baseCarrier)
	runGit(root, "", "clone", "--quiet", origin, checkout)

	metadataPath := filepath.Join(root, "target-metadata.json")
	if err := os.WriteFile(metadataPath, []byte(targetPayload), 0o600); err != nil {
		t.Fatalf("write recorder target metadata: %v", err)
	}
	commitResponsePath := filepath.Join(root, "target-commit-response.json")
	commitResponse, err := json.Marshal(map[string]any{
		"sha":     targetCarrier,
		"message": carrierMessage,
		"tree":    map[string]string{"sha": targetTree},
		"parents": []map[string]string{{"sha": baseCarrier}},
		"author": map[string]string{
			"name": "Fugue Release Baseline", "email": "release-baseline@fugue.invalid", "date": carrierDate,
		},
		"committer": map[string]string{
			"name": "Fugue Release Baseline", "email": "release-baseline@fugue.invalid", "date": carrierDate,
		},
	})
	if err != nil {
		t.Fatalf("encode recorder commit response: %v", err)
	}
	if err := os.WriteFile(commitResponsePath, commitResponse, 0o600); err != nil {
		t.Fatalf("write recorder commit response: %v", err)
	}

	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create mock bin: %v", err)
	}
	ghMock := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${LOG_FILE}"
arguments="$*"
if [[ "${arguments}" == *'repository(owner:'* ]]; then
  printf '%s\n' 'R_fugue_recorder_test'
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/blobs'* ]]; then
  [[ "${MODE}" != 'blob_post_exit7' ]] || exit 7
  printf '%s\n' '{}'
  exit 0
fi
if [[ "${arguments}" == *'/git/blobs/'* ]]; then
  object_read_count="$(grep -c '/git/blobs/' "${LOG_FILE}")"
  if [[ "${MODE}" == 'blob_readback_transient' && "${object_read_count}" -lt 3 ]]; then exit 7; fi
  if [[ "${MODE}" == 'blob_readback_unavailable' ]]; then exit 7; fi
  if [[ "${MODE}" == 'blob_readback_drift' ]]; then
    printf '%s\n' '{"sha":"drift","encoding":"base64","content":""}'
    exit 0
  fi
  python3 - "${EXPECTED_METADATA_FILE}" "${EXPECTED_BLOB_SHA}" <<'PY'
import base64, json, pathlib, sys
print(json.dumps({"sha": sys.argv[2], "encoding": "base64", "content": base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode("ascii")}, separators=(",", ":")))
PY
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/trees'* ]]; then
  [[ "${MODE}" != 'tree_post_exit7' ]] || exit 7
  printf '%s\n' '{}'
  exit 0
fi
if [[ "${arguments}" == *'/git/trees/'* ]]; then
  object_read_count="$(grep -c '/git/trees/' "${LOG_FILE}")"
  if [[ "${MODE}" == 'tree_readback_transient' && "${object_read_count}" -lt 3 ]]; then exit 7; fi
  if [[ "${MODE}" == 'tree_readback_unavailable' ]]; then exit 7; fi
  printf '{"sha":"%s","truncated":false,"tree":[{"path":"fugue-runtime-baseline.json","mode":"100644","type":"blob","sha":"%s"}]}\n' \
    "${EXPECTED_TREE_SHA}" "${EXPECTED_BLOB_SHA}"
  exit 0
fi
if [[ "${arguments}" == *'--method POST'*'/git/commits'* ]]; then
  [[ "${MODE}" != 'commit_post_exit7' ]] || exit 7
  printf '%s\n' '{}'
  exit 0
fi
if [[ "${arguments}" == *'/git/commits/'* ]]; then
  object_read_count="$(grep -c '/git/commits/' "${LOG_FILE}")"
  if [[ "${MODE}" == 'commit_readback_transient' && "${object_read_count}" -lt 3 ]]; then exit 7; fi
  if [[ "${MODE}" == 'commit_readback_unavailable' ]]; then exit 7; fi
  cat "${EXPECTED_COMMIT_RESPONSE_FILE}"
  exit 0
fi
if [[ "${arguments}" == *'updateRefs('* ]]; then
  case "${MODE}" in
    success|committed_exit7|committed_wrong_echo|readback_transient|readback_unavailable|readback_target_exit7|blob_post_exit7|tree_post_exit7|commit_post_exit7|blob_readback_transient|tree_readback_transient|commit_readback_transient)
      git --git-dir="${ORIGIN_DIR}" update-ref "${BASELINE_REF}" "${TARGET_CARRIER_SHA}" "${BASE_REF_OBJECT}"
      ;;
    failed_no_update|success_no_update) ;;
    *) exit 96 ;;
  esac
  case "${MODE}" in
    committed_exit7) exit 7 ;;
    committed_wrong_echo) printf '%s\n' 'wrong-mutation-echo' ;;
    failed_no_update) exit 7 ;;
    *) printf '%s\n' "${MUTATION_ID}" ;;
  esac
  exit 0
fi
if [[ "${arguments}" == *'/git/matching-refs/heads/fugue-control-plane-release-baseline'* ]]; then
  count=0
  [[ ! -f "${READBACK_COUNT_FILE}" ]] || count="$(<"${READBACK_COUNT_FILE}")"
  count=$((count + 1))
  printf '%s\n' "${count}" >"${READBACK_COUNT_FILE}"
  if [[ "${count}" == '1' ]]; then
    if [[ "${MODE}" == 'pre_cas_ref_drift' ]]; then printf '%s\n' "${TARGET_SHA}"; else printf '%s\n' "${BASE_REF_OBJECT}"; fi
    exit 0
  fi
  if [[ "${MODE}" == 'readback_transient' && "${count}" == '2' ]]; then exit 7; fi
  if [[ "${MODE}" == 'readback_unavailable' ]]; then exit 7; fi
  if [[ "${MODE}" == 'readback_target_exit7' ]]; then printf '%s\n' "${TARGET_CARRIER_SHA}"; exit 7; fi
  git --git-dir="${ORIGIN_DIR}" rev-parse --verify "${BASELINE_REF}"
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
	sleepMock := `#!/usr/bin/env bash
set -euo pipefail
exit 0
`
	for name, source := range map[string]string{"gh": ghMock, "timeout": timeoutMock, "sleep": sleepMock} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatalf("write %s mock: %v", name, err)
		}
	}

	mutationID := fmt.Sprintf("fugue-runtime-baseline-%s-%s", baseCarrier[:12], targetCarrier[:12])
	type result struct {
		blobPosts     int
		treePosts     int
		commitPosts   int
		blobReads     int
		treeReads     int
		commitReads   int
		mutationCalls int
		readbackCalls string
		refObject     string
		log           string
		output        []byte
		err           error
	}
	runWriter := func(t *testing.T, mode string) result {
		t.Helper()
		runGit(root, "", "--git-dir="+origin, "update-ref", baselineRef, baseCarrier)
		caseDir := t.TempDir()
		logPath := filepath.Join(caseDir, "gh.log")
		readbackCountPath := filepath.Join(caseDir, "readback-count")
		command := exec.Command("bash")
		command.Dir = checkout
		command.Stdin = strings.NewReader(writer)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"MODE="+mode,
			"LOG_FILE="+logPath,
			"READBACK_COUNT_FILE="+readbackCountPath,
			"ORIGIN_DIR="+origin,
			"BASELINE_REF="+baselineRef,
			"BASE_REF_OBJECT="+baseCarrier,
			"TARGET_SHA="+targetSHA,
			"TARGET_CARRIER_SHA="+targetCarrier,
			"MUTATION_ID="+mutationID,
			"EXPECTED_BASE_SHA="+baseSHA,
			"EXPECTED_BASE_REF_OBJECT="+baseCarrier,
			"SOURCE_SHA="+targetSHA,
			"EXPECTED_METADATA_FILE="+metadataPath,
			"EXPECTED_BLOB_SHA="+targetBlob,
			"EXPECTED_TREE_SHA="+targetTree,
			"EXPECTED_COMMIT_RESPONSE_FILE="+commitResponsePath,
			"GITHUB_SHA="+targetSHA,
			"GITHUB_REPOSITORY_OWNER=fugue-test",
			"GITHUB_REPOSITORY=fugue-test/repository",
			"GH_TOKEN=test-token",
		)
		output, runErr := command.CombinedOutput()
		log, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read gh mock log: %v", err)
		}
		readbackCalls := ""
		if count, err := os.ReadFile(readbackCountPath); err == nil {
			readbackCalls = strings.TrimSpace(string(count))
		} else if !os.IsNotExist(err) {
			t.Fatalf("read recorder settlement count: %v", err)
		}
		return result{
			blobPosts:     strings.Count(string(log), "--method POST repos/fugue-test/repository/git/blobs"),
			treePosts:     strings.Count(string(log), "--method POST repos/fugue-test/repository/git/trees"),
			commitPosts:   strings.Count(string(log), "--method POST repos/fugue-test/repository/git/commits"),
			blobReads:     strings.Count(string(log), "/git/blobs/"),
			treeReads:     strings.Count(string(log), "/git/trees/"),
			commitReads:   strings.Count(string(log), "/git/commits/"),
			mutationCalls: strings.Count(string(log), "updateRefs("),
			readbackCalls: readbackCalls,
			refObject:     runGit(root, "", "--git-dir="+origin, "rev-parse", "--verify", baselineRef),
			log:           string(log),
			output:        output,
			err:           runErr,
		}
	}

	positive := []struct {
		mode              string
		wantResponseExact string
		wantReadbacks     string
		wantObjectReads   [3]int
	}{
		{mode: "success", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "committed_exit7", wantResponseExact: "false", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "committed_wrong_echo", wantResponseExact: "false", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "readback_transient", wantResponseExact: "true", wantReadbacks: "3", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "blob_post_exit7", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "tree_post_exit7", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "commit_post_exit7", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 1}},
		{mode: "blob_readback_transient", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{3, 1, 1}},
		{mode: "tree_readback_transient", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 3, 1}},
		{mode: "commit_readback_transient", wantResponseExact: "true", wantReadbacks: "2", wantObjectReads: [3]int{1, 1, 3}},
	}
	for _, test := range positive {
		t.Run(test.mode, func(t *testing.T) {
			got := runWriter(t, test.mode)
			settled := fmt.Sprintf("response_exact=%s", test.wantResponseExact)
			if got.err != nil || got.blobPosts != 1 || got.treePosts != 1 || got.commitPosts != 1 ||
				[3]int{got.blobReads, got.treeReads, got.commitReads} != test.wantObjectReads ||
				got.mutationCalls != 1 || got.readbackCalls != test.wantReadbacks || got.refObject != targetCarrier ||
				!strings.Contains(string(got.output), settled) {
				t.Fatalf("recorder failed carrier settlement: mode=%s err=%v posts=%d/%d/%d object_reads=%d/%d/%d mutations=%d ref_readbacks=%q ref=%s output=%q", test.mode, got.err, got.blobPosts, got.treePosts, got.commitPosts, got.blobReads, got.treeReads, got.commitReads, got.mutationCalls, got.readbackCalls, got.refObject, got.output)
			}
		})
	}
	negative := []struct {
		mode          string
		wantRefObject string
	}{
		{mode: "failed_no_update", wantRefObject: baseCarrier},
		{mode: "success_no_update", wantRefObject: baseCarrier},
		{mode: "readback_unavailable", wantRefObject: targetCarrier},
		{mode: "readback_target_exit7", wantRefObject: targetCarrier},
	}
	for _, test := range negative {
		t.Run(test.mode, func(t *testing.T) {
			got := runWriter(t, test.mode)
			if got.err == nil || got.blobPosts != 1 || got.treePosts != 1 || got.commitPosts != 1 ||
				got.mutationCalls != 1 || got.readbackCalls != "6" || got.refObject != test.wantRefObject ||
				strings.Contains(string(got.output), "baseline carrier CAS settled") {
				t.Fatalf("recorder failed closed incorrectly: mode=%s err=%v posts=%d/%d/%d mutations=%d readbacks=%q ref=%s output=%q", test.mode, got.err, got.blobPosts, got.treePosts, got.commitPosts, got.mutationCalls, got.readbackCalls, got.refObject, got.output)
			}
		})
	}

	preCASNegative := []struct {
		mode            string
		wantBlobPosts   int
		wantTreePosts   int
		wantCommitPosts int
		wantObjectReads [3]int
		wantReadbacks   string
	}{
		{mode: "blob_readback_drift", wantBlobPosts: 1, wantObjectReads: [3]int{1, 0, 0}},
		{mode: "blob_readback_unavailable", wantBlobPosts: 1, wantObjectReads: [3]int{15, 0, 0}},
		{mode: "tree_readback_unavailable", wantBlobPosts: 1, wantTreePosts: 1, wantObjectReads: [3]int{1, 15, 0}},
		{mode: "commit_readback_unavailable", wantBlobPosts: 1, wantTreePosts: 1, wantCommitPosts: 1, wantObjectReads: [3]int{1, 1, 15}},
		{mode: "pre_cas_ref_drift", wantBlobPosts: 1, wantTreePosts: 1, wantCommitPosts: 1, wantObjectReads: [3]int{1, 1, 1}, wantReadbacks: "1"},
	}
	for _, test := range preCASNegative {
		t.Run(test.mode, func(t *testing.T) {
			got := runWriter(t, test.mode)
			if got.err == nil || got.blobPosts != test.wantBlobPosts || got.treePosts != test.wantTreePosts ||
				[3]int{got.blobReads, got.treeReads, got.commitReads} != test.wantObjectReads ||
				got.commitPosts != test.wantCommitPosts || got.mutationCalls != 0 || got.readbackCalls != test.wantReadbacks ||
				got.refObject != baseCarrier || strings.Contains(string(got.output), "baseline carrier CAS settled") {
				t.Fatalf("recorder crossed CAS boundary after pre-CAS failure: mode=%s err=%v posts=%d/%d/%d object_reads=%d/%d/%d mutations=%d ref_readbacks=%q ref=%s output=%q log=%q", test.mode, got.err, got.blobPosts, got.treePosts, got.commitPosts, got.blobReads, got.treeReads, got.commitReads, got.mutationCalls, got.readbackCalls, got.refObject, got.output, got.log)
			}
		})
	}
}

func TestControlPlaneDeployRequiresInternalReleaseGate(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	assertWorkflowSourceDigest(t, data, "e64674b2997bc3400eaed2b1eef846baf3dbaf17d6fb0f0cf758c7880906269a")
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	actionPath := filepath.Join("..", "..", ".github", "actions", "operational-domain-guarded-deploy", "action.yml")
	actionData, err := os.ReadFile(actionPath)
	if err != nil {
		t.Fatalf("read operational-domain guarded deploy action: %v", err)
	}
	assertWorkflowSourceDigest(t, actionData, "d05affeb474fa28d4442930114ba0fd7e1ac4b82b527a8750b3b7fdd309fedef")
	var operationalAction compositeReleaseAction
	if err := yaml.Unmarshal(actionData, &operationalAction); err != nil {
		t.Fatalf("parse operational-domain guarded deploy action: %v", err)
	}
	workflowRootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, workflowRootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowRunDigests(t, workflow.Jobs, map[string]string{
		"release-input-guard/Guard exact main commit authorization":                         "de92429028992e91fef3853ab8f70d898b44c21b67c5f4289273f3362a202153",
		"release-baseline/Resolve release-domain baseline":                                  "5ebc563799cb49f189178bbc29bcaee2bc01a2605139e1c12e35106f00fbf927",
		"release-baseline/Verify Stage1 handoff before release planning":                    "309ac2db472e741bdd25a4c5d380f4074d386807f057aec279631a7fececa211",
		"release-baseline/Resolve live image metadata":                                      "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"release-baseline/Compute live-to-target release changed files":                     "3fd4596b94b2bf2cef792ccc89752f72e371fedc51f0953821f341f74d249992",
		"release-gate/Verify exact source CI receipt":                                       "006942ca3f4ccc4d4fdf708219b6acc88ee4a652e70fb1d288899d65a5bba7fd",
		"build/Compute image metadata":                                                      "95dbd02ae09313f4d3e01ac44f7b3bdd99da8fb6302ca85e9efa87cbbd6e189c",
		"build/Compute image build plan":                                                    "ed9f833cf35832ec4d6d3362ebb5844bef787f5554faccabfdf04fa360d7b051",
		"build/Verify exact historical incident image plan":                                 "ea9c8f3100c63075f5e0d7376f6580ba25ba2e32d9ed318d66e2c4634081a8f1",
		"build/Publish verified control-plane image provenance":                             "8f188857beb59ed38aa7c3bb427b4cc4c2a5f1f6aa7df0c91211d23642f3589d",
		"deploy/Record deploy job budget origin":                                            "752b51a8ce207fa8a0f61a05d9d4deea9990882c5f846f369e916a3be2bfb677",
		"deploy/Prepare exact current tooling for historical runtime evidence":              "58356a6014715486b41cb3731208ff69dd840e95cb9885f3aff12ca0a6f8039d",
		"deploy/Build private release-domain tools":                                         "1927cf23030b57763f05b16fe227da645e993df07218783a9dc7a882f9700300",
		"deploy/Restore exact historical runtime checkout":                                  "2a2d9cad5c6caa758954108141911ffe7d0f95fa375be37b3678b42a2b363ee1",
		"deploy/Reverify Stage1 handoff at deploy prewrite":                                 "a95f6099c3affdc2e5176133f3d9a324f8273cdc6adf55ef4d60ed8ed957fbae",
		"deploy/Write genesis public release evidence":                                      "f9cda719ba304a529408a14275a87be590e9fa0422dbfbf2bfecf18c758b401d",
		"deploy/Guard stateful component files":                                             "65a7da57e288071328518bc5bd3ee9c0b5726ca97dd9a2b33672fe351eb544c6",
		"deploy/Synchronize additive ManagedApp CRD schema":                                 "a89dc070599c8f3d24b2da7e237e97730c83881a2324adbd81505e8f832fce5f",
		"deploy/Prepare authoritative DNS DiG runtime":                                      "90038169ec5ef9b2d60a35fa9271e53ee66bdfb1fbaec61ab035674a7b68f6af",
		"deploy/Verify local deploy prerequisites":                                          "e94b5f2811734f45c3ff37be7bf5ef1b85321e8e4b4f2e6821e18e23ff8dff01",
		"deploy/Explain runner and fail closed target":                                      "1731c0653bfe39738df9b79b1deafd74e1454c815ed2aa816454b9b83713f0d0",
		"deploy/Resolve live image metadata":                                                "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"deploy/Prove explicitly authorized stale pre-Helm release recovery":                "e4af592e5c1cfc427e3f53fa3b2c835bd134019117fc53ffe9e7981944afe312",
		"deploy/Remove stale release recovery proof":                                        "43203d3cc033dd8ddca207f84eeee8877791c528b99ccae888b7097b2dea077d",
		"continue-release-convergence/Dispatch exact release convergence successor":         "510c424625aa4b352c3fdacfbca149f8e784760da4971977cbe5f487fddcdfb9",
		"record-release-baseline/Advance dedicated forward-only release baseline branch":    "007dfc7144f11bc1cc0a7b62994c4efee969679de1a185913489ec5d42b592e7",
		"rearm-release-lane-on-success/Disable successful release lane with exact readback": "45c936e0acd042ba3f4e9a88249f49912b4825e52df413e2020d4a2224d1f8d2",
		"freeze-release-lane-on-failure/Record release lane freeze evidence":                "a06aef257a74d0b2029c79bbc175d57f998698edf04bfeb66f11f012f55c0ac1",
		"freeze-release-lane-on-failure/Disable release lane and cancel queued runs":        "1c3e22987871632615f8c74f86e1da5f6675b440a3dbba8c2848056cd045d99a",
		"freeze-release-lane-on-failure/Require release lane freeze evidence":               "a583f75fce52b2c2e957c16f290af7ab4367ef35a3b4d22adeef76b2446c6cd4",
	})
	workflowJobsNode := workflowMappingValue(t, workflowRootNode, "jobs")
	assertWorkflowJobNodeContracts(t, workflowJobsNode, map[string]workflowJobNodeContract{
		"release-input-guard": {
			Keys: []string{"runs-on", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "if", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "env", "run"},
			},
		},
		"release-baseline": {
			Keys: []string{"needs", "outputs", "permissions", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "if", "env", "run"},
				{"name", "id", "env", "run"},
				{"name", "id", "env", "run"},
				{"name", "id", "env", "run"},
			},
		},
		"release-gate": {
			Keys: []string{"needs", "permissions", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "env", "run"},
			},
		},
		"build": {
			Keys: []string{"needs", "outputs", "permissions", "runs-on", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "if", "uses", "with"},
				{"name", "id", "env", "run"},
				{"name", "id", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "uses"},
				{"name", "if", "uses", "with"},
				{"name", "id", "env", "run"},
			},
		},
		"deploy": {
			Keys: []string{"needs", "if", "runs-on", "timeout-minutes", "environment", "permissions", "outputs", "steps"},
			StepKeys: [][]string{
				{"name", "if", "run"},
				{"name", "uses", "with"},
				{"name", "id", "if", "env", "run"},
				{"name", "if", "uses", "with"},
				{"name", "uses", "with"},
				{"name", "run"},
				{"name", "if", "env", "run"},
				{"name", "id", "if", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "if", "run"},
				{"name", "if", "run"},
				{"name", "if", "run"},
				{"name", "if", "env", "run"},
				{"name", "id", "if", "env", "run"},
				{"name", "if", "env", "run"},
				{"name", "id", "if", "env", "uses"},
				{"name", "if", "env", "run"},
				{"name", "if", "run"},
				{"name", "if", "uses", "with"},
			},
		},
		"continue-release-convergence": {
			Keys: []string{"needs", "if", "runs-on", "timeout-minutes", "environment", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "id", "env", "run"},
				{"name", "uses", "with"},
			},
		},
		"record-release-baseline": {
			Keys: []string{"needs", "if", "runs-on", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "uses", "with"},
				{"name", "env", "run"},
			},
		},
		"rearm-release-lane-on-success": {
			Keys: []string{"needs", "if", "runs-on", "timeout-minutes", "environment", "permissions", "steps"},
			StepKeys: [][]string{
				{"name", "id", "env", "run"},
				{"name", "uses", "with"},
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
	if len(workflow.On.WorkflowDispatch.Inputs) != 6 {
		t.Fatalf("workflow_dispatch input inventory drifted: %+v", workflow.On.WorkflowDispatch.Inputs)
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
	targetSHAInput, ok := workflow.On.WorkflowDispatch.Inputs["target_sha"]
	if !ok {
		t.Fatal("workflow_dispatch must require target_sha")
	}
	var targetSHA releaseWorkflowDispatchInput
	if err := targetSHAInput.Decode(&targetSHA); err != nil {
		t.Fatalf("decode target_sha input: %v", err)
	}
	if !targetSHA.Required || targetSHA.Type != "string" || targetSHA.Default != nil {
		t.Fatalf("target_sha must be a required string without a default: %+v", targetSHA)
	}
	imageCacheInput, ok := workflow.On.WorkflowDispatch.Inputs["image_cache_convergence"]
	if !ok {
		t.Fatal("workflow_dispatch must define image_cache_convergence")
	}
	var imageCache releaseWorkflowDispatchInput
	if err := imageCacheInput.Decode(&imageCache); err != nil {
		t.Fatalf("decode image_cache_convergence input: %v", err)
	}
	if !imageCache.Required || imageCache.Type != "boolean" || imageCache.Default != false {
		t.Fatalf("image_cache_convergence must be a required false-default boolean: %+v", imageCache)
	}
	convergenceSourceInput, ok := workflow.On.WorkflowDispatch.Inputs["convergence_source_run_id"]
	if !ok {
		t.Fatal("workflow_dispatch must define convergence_source_run_id")
	}
	var convergenceSource releaseWorkflowDispatchInput
	if err := convergenceSourceInput.Decode(&convergenceSource); err != nil {
		t.Fatalf("decode convergence_source_run_id input: %v", err)
	}
	if convergenceSource.Required || convergenceSource.Type != "string" || convergenceSource.Default != "" {
		t.Fatalf("convergence_source_run_id must be an optional empty-default string: %+v", convergenceSource)
	}
	for _, inputName := range []string{"public_data_plane_adoption_run_id", "public_data_plane_adoption_baseline_digest"} {
		inputNode, exists := workflow.On.WorkflowDispatch.Inputs[inputName]
		if !exists {
			t.Fatalf("workflow_dispatch must define %s", inputName)
		}
		var input releaseWorkflowDispatchInput
		if err := inputNode.Decode(&input); err != nil {
			t.Fatalf("decode %s input: %v", inputName, err)
		}
		if input.Required || input.Type != "string" || input.Default != "" {
			t.Fatalf("%s must be an optional empty-default string: %+v", inputName, input)
		}
	}
	workflowSource := string(data)
	if strings.Contains(workflowSource, "existing_image_tag") || len(workflow.On.Push.Paths) != 0 {
		t.Fatal("control-plane release must be dispatch-only without an image bypass")
	}

	inputGuard, ok := workflow.Jobs["release-input-guard"]
	if !ok {
		t.Fatal("control-plane workflow must define release-input-guard")
	}
	if got, want := inputGuard.Permissions, map[string]string{"actions": "read", "contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("release input guard permissions drifted: got %v want %v", got, want)
	}
	downloadAuthorization := workflowStepByName(t, inputGuard, "Download convergence successor authorization")
	if downloadAuthorization.If != "${{ inputs.image_cache_convergence }}" ||
		downloadAuthorization.Uses != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" {
		t.Fatalf("convergence authorization download boundary drifted: %+v", downloadAuthorization)
	}
	wantAuthorizationWith := map[string]string{
		"name":         "fugue-release-convergence-successor-${{ inputs.convergence_source_run_id }}-1",
		"path":         "${{ runner.temp }}/fugue-release-convergence-authorization",
		"github-token": "${{ github.token }}",
		"repository":   "${{ github.repository }}",
		"run-id":       "${{ inputs.convergence_source_run_id }}",
	}
	if !reflect.DeepEqual(downloadAuthorization.With, wantAuthorizationWith) {
		t.Fatalf("convergence authorization download inputs drifted: got %v want %v", downloadAuthorization.With, wantAuthorizationWith)
	}
	guard := workflowStepByName(t, inputGuard, "Guard exact main commit authorization")
	for key, want := range map[string]string{
		"EXPECTED_SHA":                      "${{ inputs.expected_sha }}",
		"ACTUAL_SHA":                        "${{ github.sha }}",
		"TARGET_SHA":                        "${{ inputs.target_sha }}",
		"IMAGE_CACHE_CONVERGENCE":           "${{ inputs.image_cache_convergence && 'true' || 'false' }}",
		"CONVERGENCE_SOURCE_RUN_ID":         "${{ inputs.convergence_source_run_id }}",
		"CONVERGENCE_AUTHORIZATION_FILE":    "${{ runner.temp }}/fugue-release-convergence-authorization/successor.json",
		"GH_TOKEN":                          "${{ github.token }}",
		"REPOSITORY":                        "${{ github.repository }}",
		"EVENT_NAME":                        "${{ github.event_name }}",
		"EVENT_REF":                         "${{ github.ref }}",
		"EVENT_REF_NAME":                    "${{ github.ref_name }}",
		"EVENT_REF_TYPE":                    "${{ github.ref_type }}",
		"PUBLIC_DATA_PLANE_ADOPTION_RUN_ID": "${{ inputs.public_data_plane_adoption_run_id }}",
		"PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST": "${{ inputs.public_data_plane_adoption_baseline_digest }}",
		"PUBLIC_DATA_PLANE_ADOPTION_BASELINE":        "${{ runner.temp }}/public-data-plane-stage1-handoff/stage1-baseline.json",
		"PUBLIC_DATA_PLANE_ADOPTION_TRACE":           "${{ runner.temp }}/public-data-plane-stage1-handoff/execution-trace.json",
	} {
		if got := guard.Env[key]; got != want {
			t.Fatalf("release input guard env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		"refs/heads/main", "^[0-9a-f]{40}$", `"${EXPECTED_SHA}" == "${ACTUAL_SHA}"`,
		`"${TARGET_SHA}" =~ ^[0-9a-f]{40}$`, `"${remote_main}" == "${EXPECTED_SHA}"`,
		`[[ -z "${CONVERGENCE_SOURCE_RUN_ID}" ]]`, "actions/runs/${CONVERGENCE_SOURCE_RUN_ID}",
		`"${source_status}" == 'completed' && "${source_conclusion}" == 'success'`,
		`"pending_activation_artifacts": ["image_cache"]`, `"source_image_cache_artifact"`,
		`"source_image_cache_artifacts_digest"`, `"schema_version": 2`,
		`"successor_run_id": successor_run_id`,
		`if [[ -z "${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}" && -z "${PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST}" ]]`,
		`"${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}" =~ ^[1-9][0-9]*$`,
		`"${PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$`,
		"if raw != canonical:",
	} {
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
	if got, want := gate.Permissions, map[string]string{"actions": "read", "contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("release-gate permissions drifted: got %v want %v", got, want)
	}
	if len(gate.Steps) != 1 {
		t.Fatalf("release-gate must contain only the exact source CI receipt: %+v", gate.Steps)
	}
	receipt := workflowStepByName(t, gate, "Verify exact source CI receipt")
	if got, want := receipt.Env, map[string]string{
		"EXPECTED_SHA": "${{ inputs.expected_sha }}",
		"GH_TOKEN":     "${{ github.token }}",
		"REPOSITORY":   "${{ github.repository }}",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("release-gate source CI receipt environment drifted: got %v want %v", got, want)
	}
	for _, required := range []string{
		"actions/workflows/ci.yml/runs?branch=main&event=push&status=success&per_page=100",
		`select(.head_sha == \"${EXPECTED_SHA}\")`,
		`"${ci_attempt}" == '1'`,
		`"${ci_sha}" == "${EXPECTED_SHA}"`,
		`"${ci_status}" == 'completed'`,
		`"${ci_conclusion}" == 'success'`,
		`"${ci_path}" == '.github/workflows/ci.yml'`,
	} {
		if !strings.Contains(receipt.Run, required) {
			t.Fatalf("release-gate source CI receipt must contain %q", required)
		}
	}
	for _, forbidden := range []string{"make generate-openapi-check", "test_release_domain_safety.sh", "test_node_local_dns_release.sh", "test_verify_stale_release_recovery.py", "go test ./..."} {
		if strings.Contains(receipt.Run, forbidden) {
			t.Fatalf("release-gate reruns source CI command %q", forbidden)
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
	for _, jobName := range []string{"release-baseline", "build", "deploy", "record-release-baseline"} {
		job, exists := workflow.Jobs[jobName]
		if !exists {
			t.Fatalf("control-plane workflow must define %s", jobName)
		}
		checkout := workflowStepByName(t, job, "Checkout")
		if checkout.Uses != checkoutAction {
			t.Fatalf("%s checkout must use the pinned action: got %q want %q", jobName, checkout.Uses, checkoutAction)
		}
		if got, want := checkout.With["ref"], "${{ inputs.target_sha }}"; got != want {
			t.Fatalf("%s checkout must bind the exact runtime target: got %q want %q", jobName, got, want)
		}
	}
	checkoutCount := 0
	currentToolingCheckoutCount := 0
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
				want := "${{ inputs.target_sha }}"
				if jobName == "build" && step.Name == "Checkout current artifact verifier" {
					want = "${{ inputs.expected_sha }}"
					currentToolingCheckoutCount++
					if step.If != "${{ inputs.target_sha != inputs.expected_sha }}" ||
						step.With["path"] != "current-release-tools" || step.With["fetch-depth"] != "1" {
						t.Fatalf("historical artifact verifier checkout is not exact: %#v", step)
					}
				}
				if got := step.With["ref"]; got != want {
					t.Fatalf("step %s/%s checkout ref drifted: got %q want %q", jobName, step.Name, got, want)
				}
			}
		}
	}
	if checkoutCount != 5 || currentToolingCheckoutCount != 1 {
		t.Fatalf("control-plane workflow checkout closure drifted: total=%d current-tooling=%d", checkoutCount, currentToolingCheckoutCount)
	}

	if !containsWorkflowNeed(baseline.Needs, "release-input-guard") {
		t.Fatal("release-baseline must wait for the exact input guard")
	}
	domainBaseline := workflowStepByName(t, baseline, "Resolve release-domain baseline")
	if got, want := domainBaseline.Env, map[string]string{
		"SOURCE_SHA": "${{ inputs.expected_sha }}",
		"TARGET_SHA": "${{ inputs.target_sha }}",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("forward-only baseline resolver target inputs drifted: got %+v want %+v", got, want)
	}
	for _, required := range []string{
		"refs/heads/fugue-control-plane-release-baseline",
		`"${remote_status}" == '0'`,
		`"${fetched_ref_object_sha}" == "${remote_object}"`,
		`commit_identity="$(git rev-list --parents -n 1 FETCH_HEAD)"`,
		`metadata_candidate='false'`,
		`"${metadata_path}" == 'fugue-runtime-baseline.json'`,
		`metadata_candidate='true'`,
		`git cat-file blob "${metadata_blob}"`,
		`previous_sha = value.get("previous_baseline_object_sha")`,
		`if payload != expected:`,
		`sys.stdout.write(runtime_sha + "\t" + ("null" if previous_sha is None else previous_sha))`,
		`"${metadata_parent}" == "${previous_baseline_object_sha}"`,
		`[[ -n "${parent_shas:-}" ]] || exit 1`,
		`git cat-file -e "${domain_base_sha}^{commit}"`,
		`"${domain_base_sha}" != "${target_sha}"`,
		`git merge-base --is-ancestor "${target_sha}" "${SOURCE_SHA}"`,
		`git merge-base --is-ancestor "${domain_base_sha}" "${target_sha}"`,
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
	for _, line := range strings.Split(domainBaseline.Run, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[[") && !strings.HasSuffix(line, "|| exit 1") {
			t.Fatalf("release-domain baseline resolver check must fail explicitly across supported Bash versions: %q", line)
		}
	}

	baselineLiveImages := workflowStepByName(t, baseline, "Resolve live image metadata")
	if baselineLiveImages.ID != "live_images" {
		t.Fatalf("release baseline live image step id drifted: %q", baselineLiveImages.ID)
	}
	if got, want := baselineLiveImages.Env["GITHUB_SHA"], "${{ inputs.target_sha }}"; got != want {
		t.Fatalf("release baseline script revision must be the exact runtime target: got %q want %q", got, want)
	}
	if got, want := baselineLiveImages.Env["FUGUE_IMAGE_TAG"], "${{ inputs.target_sha }}"; got != want {
		t.Fatalf("release baseline image target must be the exact runtime target: got %q want %q", got, want)
	}
	if got, want := baselineLiveImages.Env["FUGUE_RESOLVE_HELM_IMAGE_BASELINES"], "true"; got != want {
		t.Fatalf("release baseline must compare live images with the Helm revision: got %q want %q", got, want)
	}
	for component, want := range map[string]string{
		"api":             "${{ steps.live_images.outputs.api_image_helm_drift }}",
		"controller":      "${{ steps.live_images.outputs.controller_image_helm_drift }}",
		"drain_agent":     "${{ steps.live_images.outputs.drain_agent_image_helm_drift }}",
		"telemetry_agent": "${{ steps.live_images.outputs.telemetry_agent_image_helm_drift }}",
		"image_cache":     "${{ steps.live_images.outputs.image_cache_image_helm_drift }}",
		"edge":            "${{ steps.live_images.outputs.edge_image_helm_drift }}",
	} {
		if got := baseline.Outputs[component+"_image_helm_drift"]; got != want {
			t.Fatalf("release baseline Helm drift output %s drifted: got %q want %q", component, got, want)
		}
	}
	baselineChanges := workflowStepByName(t, baseline, "Compute live-to-target release changed files")
	if baselineChanges.ID != "release_changes" {
		t.Fatalf("release baseline changed-files step id drifted: %q", baselineChanges.ID)
	}
	if got, want := baselineChanges.Env["FUGUE_RELEASE_TARGET_REF"], "${{ inputs.target_sha }}"; got != want {
		t.Fatalf("release baseline diff target must be the exact runtime target: got %q want %q", got, want)
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
	if got, want := build.Permissions, map[string]string{"actions": "read", "contents": "read", "packages": "write"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("image build permissions drifted: got %v want %v", got, want)
	}
	buildAuthorization := workflowStepByName(t, build, "Download convergence image artifact authorization")
	if buildAuthorization.If != "${{ inputs.image_cache_convergence }}" ||
		buildAuthorization.Uses != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" ||
		!reflect.DeepEqual(buildAuthorization.With, wantAuthorizationWith) {
		t.Fatalf("build convergence authorization download drifted: %+v", buildAuthorization)
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
	if got, want := buildMeta.Env["TARGET_SHA"], "${{ inputs.target_sha }}"; got != want {
		t.Fatalf("image metadata target input drifted: got %q want %q", got, want)
	}
	const imageTagOutput = `echo "image_tag=${TARGET_SHA}" >> "${GITHUB_OUTPUT}"`
	if strings.Count(buildMeta.Run, "image_tag=") != 1 || !strings.Contains(buildMeta.Run, imageTagOutput) {
		t.Fatalf("image metadata must publish only the exact runtime target as image_tag: %q", buildMeta.Run)
	}
	buildPlan := workflowStepByName(t, build, "Compute image build plan")
	if buildPlan.ID != "plan" {
		t.Fatalf("image build-plan step id drifted: %q", buildPlan.ID)
	}
	if got, want := buildPlan.Env["FUGUE_RELEASE_TARGET_REF"], "${{ needs.release-baseline.outputs.target_ref }}"; got != want {
		t.Fatalf("image build plan must use the baseline target ref: got %q want %q", got, want)
	}
	if got, want := buildPlan.Env["FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE"], "${{ inputs.image_cache_convergence && 'true' || 'false' }}"; got != want {
		t.Fatalf("image build plan convergence input drifted: got %q want %q", got, want)
	}
	for component, want := range map[string]string{
		"API":             "${{ needs.release-baseline.outputs.api_image_helm_drift }}",
		"CONTROLLER":      "${{ needs.release-baseline.outputs.controller_image_helm_drift }}",
		"DRAIN_AGENT":     "${{ needs.release-baseline.outputs.drain_agent_image_helm_drift }}",
		"TELEMETRY_AGENT": "${{ needs.release-baseline.outputs.telemetry_agent_image_helm_drift }}",
		"IMAGE_CACHE":     "${{ needs.release-baseline.outputs.image_cache_image_helm_drift }}",
		"EDGE":            "${{ needs.release-baseline.outputs.edge_image_helm_drift }}",
	} {
		key := "FUGUE_" + component + "_IMAGE_HELM_DRIFT"
		if got := buildPlan.Env[key]; got != want {
			t.Fatalf("image build plan Helm drift input %s drifted: got %q want %q", key, got, want)
		}
	}
	historicalPlan := workflowStepByName(t, build, "Verify exact historical incident image plan")
	if historicalPlan.If != "${{ inputs.target_sha != inputs.expected_sha }}" {
		t.Fatalf("historical incident plan condition drifted: %q", historicalPlan.If)
	}
	for key, want := range map[string]string{
		"TARGET_SHA":            "${{ inputs.target_sha }}",
		"BUILD_API":             "${{ steps.plan.outputs.build_api }}",
		"BUILD_CONTROLLER":      "${{ steps.plan.outputs.build_controller }}",
		"BUILD_DRAIN_AGENT":     "${{ steps.plan.outputs.build_drain_agent }}",
		"BUILD_TELEMETRY_AGENT": "${{ steps.plan.outputs.build_telemetry_agent }}",
		"BUILD_IMAGE_CACHE":     "${{ steps.plan.outputs.build_image_cache }}",
		"BUILD_EDGE":            "${{ steps.plan.outputs.build_edge }}",
		"BUILD_APP_SSH":         "${{ steps.plan.outputs.build_app_ssh }}",
		"IMAGE_TARGETS":         "${{ steps.plan.outputs.targets }}",
		"IMAGE_TARGET_COUNT":    "${{ steps.plan.outputs.target_count }}",
	} {
		if got := historicalPlan.Env[key]; got != want {
			t.Fatalf("historical incident plan env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		"d1e7ed9cdedbaa09db9bd78b4e433b94c7357510",
		`"${BUILD_API}" == 'true' && "${BUILD_CONTROLLER}" == 'true'`,
		`"${BUILD_TELEMETRY_AGENT}" == 'true' && "${BUILD_EDGE}" == 'true'`,
		`"${BUILD_DRAIN_AGENT}" == 'false' && "${BUILD_IMAGE_CACHE}" == 'false' && "${BUILD_APP_SSH}" == 'false'`,
		`"${IMAGE_TARGETS}" == 'api controller telemetry_agent edge' && "${IMAGE_TARGET_COUNT}" == '4'`,
	} {
		if !strings.Contains(historicalPlan.Run, required) {
			t.Fatalf("historical incident plan must contain %q", required)
		}
	}
	historicalReceipt := workflowStepByName(t, build, "Download exact historical incident build receipt")
	if historicalReceipt.If != "${{ inputs.target_sha != inputs.expected_sha }}" ||
		historicalReceipt.Uses != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" {
		t.Fatalf("historical incident receipt download is not exact: %#v", historicalReceipt)
	}
	for key, want := range map[string]string{
		"name":       "fugue-control-plane-build-activation-evidence-30741102194-1",
		"run-id":     "30741102194",
		"repository": "${{ github.repository }}",
	} {
		if got := historicalReceipt.With[key]; got != want {
			t.Fatalf("historical incident receipt %s drifted: got %q want %q", key, got, want)
		}
	}
	if setup := workflowStepByName(t, build, "Setup Go"); setup.If != "${{ inputs.target_sha == inputs.expected_sha }}" {
		t.Fatalf("historical reuse must skip Setup Go: %q", setup.If)
	}
	for _, name := range []string{"Setup Docker Buildx", "Login to GHCR"} {
		step := workflowStepByName(t, build, name)
		if step.If != "${{ steps.plan.outputs.target_count != '0' && inputs.target_sha == inputs.expected_sha }}" {
			t.Fatalf("historical reuse must skip %s: %q", name, step.If)
		}
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
	for key, want := range map[string]string{
		"FUGUE_IMAGE_CACHE_IMAGE_BASE_REF":                   "${{ needs.release-baseline.outputs.image_cache_image_baseline_ref }}",
		"FUGUE_CONTROL_PLANE_IMAGE_REUSE_AUTHORIZATION_FILE": "${{ inputs.image_cache_convergence && format('{0}/fugue-release-convergence-authorization/successor.json', runner.temp) || '' }}",
		"FUGUE_CONVERGENCE_SOURCE_RUN_ID":                    "${{ inputs.convergence_source_run_id }}",
		"FUGUE_CONTROL_PLANE_HISTORICAL_INCIDENT_BUILD_PLAN": "${{ inputs.target_sha != inputs.expected_sha && format('{0}/fugue-historical-incident-build/build-artifact-plan.json', runner.temp) || '' }}",
	} {
		if got := buildProvenance.Env[key]; got != want {
			t.Fatalf("image provenance convergence env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		"./current-release-tools/scripts/build_control_plane_images.sh",
		"./scripts/build_control_plane_images.sh",
	} {
		if !strings.Contains(buildProvenance.Run, required) {
			t.Fatalf("image provenance execution is missing %q", required)
		}
	}

	deploy, ok := workflow.Jobs["deploy"]
	if !ok || !containsWorkflowNeed(deploy.Needs, "release-input-guard") ||
		!containsWorkflowNeed(deploy.Needs, "release-baseline") ||
		!containsWorkflowNeed(deploy.Needs, "release-gate") || !containsWorkflowNeed(deploy.Needs, "build") {
		t.Fatal("control-plane deploy must wait for release-input-guard, release-baseline, release-gate, and build")
	}
	const deployCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' }}"
	if strings.TrimSpace(deploy.If) != deployCondition {
		t.Fatalf("deploy condition must require every prerequisite success without bypass: got %q want %q", deploy.If, deployCondition)
	}
	if got, want := deploy.Permissions, map[string]string{"actions": "read", "contents": "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("deploy permissions drifted: got %v want %v", got, want)
	}
	wantDeployOutputs := map[string]string{
		"image_activation_convergence": "${{ needs.release-baseline.outputs.is_genesis == 'true' && 'complete' || steps.guarded_deploy.outputs.image-activation-convergence }}",
		"pending_activation_artifacts": "${{ steps.guarded_deploy.outputs.pending-activation-artifacts }}",
	}
	if !reflect.DeepEqual(deploy.Outputs, wantDeployOutputs) {
		t.Fatalf("deploy convergence outputs drifted: got %v want %v", deploy.Outputs, wantDeployOutputs)
	}
	if deploy.ContinueOnError {
		t.Fatal("deploy job must fail closed")
	}
	setupGo := workflowStepByName(t, deploy, "Setup Go")
	if setupGo.Uses != "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16" ||
		!reflect.DeepEqual(setupGo.With, map[string]string{"go-version-file": "go.mod", "cache": "false"}) {
		t.Fatalf("self-hosted deploy must disable Actions Go cache restore: %+v", setupGo)
	}
	buildTools := workflowStepByName(t, deploy, "Build private release-domain tools")
	for _, required := range []string{
		`source_root="${FUGUE_CURRENT_RELEASE_TOOLS_ROOT:-${GITHUB_WORKSPACE}}"`,
		`cd "${source_root}"`,
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
		"module_files_digest",
		"./cmd/fugue-release-domain-evidence",
		"./cmd/fugue-release-domain-dispatch",
		"chmod 0700",
	} {
		if !strings.Contains(buildTools.Run, required) {
			t.Fatalf("deploy release tool build must contain %q", required)
		}
	}
	currentTooling := workflowStepByName(t, deploy, "Prepare exact current tooling for historical runtime evidence")
	if currentTooling.ID != "current_tooling" || currentTooling.If != "${{ inputs.target_sha != inputs.expected_sha && needs.release-baseline.outputs.is_genesis != 'true' }}" {
		t.Fatalf("historical current-tooling gate drifted: %#v", currentTooling)
	}
	for key, want := range map[string]string{
		"SOURCE_SHA":        "${{ inputs.expected_sha }}",
		"TARGET_SHA":        "${{ inputs.target_sha }}",
		"API_DIGEST":        "${{ needs.build.outputs.api_image_digest }}",
		"CONTROLLER_DIGEST": "${{ needs.build.outputs.controller_image_digest }}",
		"TELEMETRY_DIGEST":  "${{ needs.build.outputs.telemetry_agent_image_digest }}",
	} {
		if got := currentTooling.Env[key]; got != want {
			t.Fatalf("historical current-tooling env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		`git archive "${SOURCE_SHA}"`,
		"deploy/release-domains/ownership-v1.yaml",
		"git update-index --assume-unchanged",
		"FUGUE_API_IMAGE_DIGEST",
		"FUGUE_CONTROLLER_IMAGE_DIGEST",
		"FUGUE_TELEMETRY_AGENT_IMAGE_DIGEST",
	} {
		if !strings.Contains(currentTooling.Run, required) {
			t.Fatalf("historical current-tooling proof is missing %q", required)
		}
	}
	restoreRuntime := workflowStepByName(t, deploy, "Restore exact historical runtime checkout")
	if !strings.Contains(restoreRuntime.If, "steps.current_tooling.outcome == 'success'") ||
		!strings.Contains(restoreRuntime.Run, "git update-index --no-assume-unchanged") ||
		!strings.Contains(restoreRuntime.Run, "git diff --quiet --ignore-submodules --") {
		t.Fatalf("historical runtime restoration is incomplete: %#v", restoreRuntime)
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
	if got, want := genesisEvidence.ID, "genesis_evidence"; got != want {
		t.Fatalf("genesis evidence id drifted: got %q want %q", got, want)
	}
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
		".github/actions/operational-domain-guarded-deploy/action.yml",
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
	if len(expectedGenesisChanges) != 53 {
		t.Fatalf("genesis expected-change allowlist must contain exactly 53 paths, found %d", len(expectedGenesisChanges))
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
	crdSync := workflowStepByName(t, deploy, "Synchronize additive ManagedApp CRD schema")
	if strings.TrimSpace(crdSync.If) != nonGenesisCondition {
		t.Fatalf("managed app CRD sync must run only for ordinary releases: %q", crdSync.If)
	}
	if strings.TrimSpace(crdSync.Run) != "bash ./scripts/sync_managed_app_crd.sh" {
		t.Fatalf("managed app CRD sync must use the reviewed bounded script: %q", crdSync.Run)
	}
	genesisReachable := map[string]string{
		"Checkout":                              "",
		"Setup Go":                              "",
		"Build private release-domain tools":    "",
		"Write genesis public release evidence": "${{ needs.release-baseline.outputs.is_genesis == 'true' }}",
		"Upload release-domain public evidence": "${{ always() && (steps.genesis_evidence.outcome == 'success' || steps.guarded_deploy.outcome == 'success') }}",
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
	const deployImageTag = "${{ needs.build.outputs.image_tag || inputs.target_sha }}"
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
	if got, want := deployLiveImages.Env["GITHUB_SHA"], "${{ inputs.target_sha }}"; got != want {
		t.Fatalf("deploy live image resolver runtime revision drifted: got %q want %q", got, want)
	}

	upgrade := workflowStepByName(t, deploy, "Upgrade Fugue control plane through uploaded operational evidence")
	if got, want := upgrade.ID, "guarded_deploy"; got != want {
		t.Fatalf("guarded deploy step id drifted: got %q want %q", got, want)
	}
	if strings.TrimSpace(upgrade.If) != nonGenesisCondition {
		t.Fatalf("control-plane upgrade must be unreachable from the genesis evidence path: %q", upgrade.If)
	}
	if got, want := upgrade.Uses, "./.github/actions/operational-domain-guarded-deploy"; got != want {
		t.Fatalf("control-plane upgrade must use the guarded composite action: got %q want %q", got, want)
	}
	if strings.TrimSpace(upgrade.Run) != "" {
		t.Fatal("guarded deploy workflow step must not define a run body")
	}
	for key, want := range map[string]string{
		"GITHUB_SHA":                             "${{ inputs.target_sha }}",
		"FUGUE_API_IMAGE_REPOSITORY":             "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.api_image_repository || steps.live_images.outputs.api_image_repository }}",
		"FUGUE_API_IMAGE_TAG":                    "${{ needs.build.outputs.build_api == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.api_image_tag }}",
		"FUGUE_CONTROLLER_IMAGE_REPOSITORY":      "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.controller_image_repository || steps.live_images.outputs.controller_image_repository }}",
		"FUGUE_CONTROLLER_IMAGE_TAG":             "${{ needs.build.outputs.build_controller == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.controller_image_tag }}",
		"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY":     "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.drain_agent_image_repository || steps.live_images.outputs.drain_agent_image_repository }}",
		"FUGUE_DRAIN_AGENT_IMAGE_TAG":            "${{ needs.build.outputs.build_drain_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.drain_agent_image_tag }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY": "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.telemetry_agent_image_repository || steps.live_images.outputs.telemetry_agent_image_repository }}",
		"FUGUE_TELEMETRY_AGENT_IMAGE_TAG":        "${{ needs.build.outputs.build_telemetry_agent == 'true' && needs.build.outputs.image_tag || steps.live_images.outputs.telemetry_agent_image_tag }}",
		"FUGUE_APP_SSH_IMAGE_REPOSITORY":         "${{ needs.build.outputs.app_ssh_image_repository }}",
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
	if got, want := upgrade.Env["FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE"], "preserve"; got != want {
		t.Fatalf("control-plane release must preserve the public Edge data plane: got %q want %q", got, want)
	}
	for key, want := range map[string]string{
		"FUGUE_EDGE_ACTIVATION_ENABLED":             "${{ vars.FUGUE_EDGE_ACTIVATION_ENABLED || 'false' }}",
		"FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME": "${{ vars.FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME || '' }}",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("edge activation Helm wiring %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, forbidden := range []string{
		"FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY",
		"FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_ID",
		"FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_GENERATION",
	} {
		if _, exists := upgrade.Env[forbidden]; exists {
			t.Fatalf("edge activation key material must not enter the deploy workflow environment: %s", forbidden)
		}
	}
	for key, want := range map[string]string{
		"FUGUE_RELEASE_DOMAIN_BASE_SHA":                        "${{ needs.release-baseline.outputs.domain_base_sha }}",
		"FUGUE_RELEASE_DOMAIN_TARGET_SHA":                      "${{ inputs.target_sha }}",
		"FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL":                   "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
		"FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL":                   "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
		"FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE":            "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE":         "${{ runner.temp }}/fugue-release-domain-public/operational-domain-evidence.json",
		"FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR":     "${{ runner.temp }}/fugue-release-domain-public/build-activation-evidence",
		"FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST": "${{ needs.build.outputs.verified_image_artifacts_digest }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS":                   "${{ needs.build.outputs.image_targets }}",
		"FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE":                "${{ inputs.image_cache_convergence && 'true' || 'false' }}",
		"FUGUE_RELEASE_DOMAIN_API_IMAGE_BASE_SHA":              "${{ needs.release-baseline.outputs.api_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_API_IMAGE_DIGEST":                "${{ needs.build.outputs.api_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_BASE_SHA":       "${{ needs.release-baseline.outputs.controller_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_DIGEST":         "${{ needs.build.outputs.controller_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_BASE_SHA":      "${{ needs.release-baseline.outputs.drain_agent_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_DIGEST":        "${{ needs.build.outputs.drain_agent_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_BASE_SHA":  "${{ needs.release-baseline.outputs.telemetry_agent_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_DIGEST":    "${{ needs.build.outputs.telemetry_agent_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_BASE_SHA":      "${{ needs.release-baseline.outputs.image_cache_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_DIGEST":        "${{ needs.build.outputs.image_cache_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_BASE_SHA":             "${{ needs.release-baseline.outputs.edge_image_baseline_ref }}",
		"FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_DIGEST":               "${{ needs.build.outputs.edge_image_digest }}",
		"FUGUE_RELEASE_DOMAIN_APP_SSH_IMAGE_DIGEST":            "${{ needs.build.outputs.app_ssh_image_digest }}",
	} {
		if got := upgrade.Env[key]; got != want {
			t.Fatalf("upgrade release-domain input %s drifted: got %q want %q", key, got, want)
		}
	}
	if got, want := operationalAction.Runs.Using, "composite"; got != want {
		t.Fatalf("operational deploy action runtime drifted: got %q want %q", got, want)
	}
	wantActionSteps := []string{
		"Prepare operational-domain report-only evidence",
		"Upload operational-domain report-only evidence",
		"Upload build-vs-activation report-only evidence",
		"Apply exact authorized control-plane release",
		"Verify image activation convergence",
	}
	gotActionSteps := make([]string, 0, len(operationalAction.Runs.Steps))
	for _, step := range operationalAction.Runs.Steps {
		gotActionSteps = append(gotActionSteps, step.Name)
	}
	if !reflect.DeepEqual(gotActionSteps, wantActionSteps) {
		t.Fatalf("operational deploy action order drifted: got %q want %q", gotActionSteps, wantActionSteps)
	}
	prepare := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Prepare operational-domain report-only evidence")
	if got, want := prepare.ID, "prepare"; got != want {
		t.Fatalf("operational prepare id drifted: got %q want %q", got, want)
	}
	if got, want := prepare.Env["FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE"], "prepare"; got != want {
		t.Fatalf("operational prepare phase drifted: got %q want %q", got, want)
	}
	if got, want := strings.TrimSpace(prepare.Run), "./scripts/upgrade_fugue_control_plane.sh"; got != want {
		t.Fatalf("operational prepare entrypoint drifted: got %q want %q", got, want)
	}
	operationalUpload := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Upload operational-domain report-only evidence")
	if got, want := operationalUpload.ID, "operational-report-upload"; got != want {
		t.Fatalf("operational report upload id drifted: got %q want %q", got, want)
	}
	if got, want := operationalUpload.If, "${{ always() && steps.prepare.outcome == 'success' }}"; got != want {
		t.Fatalf("operational report upload condition drifted: got %q want %q", got, want)
	}
	if operationalUpload.ContinueOnError {
		t.Fatal("operational report upload must fail closed")
	}
	if got, want := operationalUpload.Uses, "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"; got != want {
		t.Fatalf("operational report upload pin drifted: got %q want %q", got, want)
	}
	for key, want := range map[string]string{
		"path":                 "${{ env.FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE }}",
		"if-no-files-found":    "error",
		"retention-days":       "90",
		"include-hidden-files": "false",
		"overwrite":            "false",
	} {
		if got := operationalUpload.With[key]; got != want {
			t.Fatalf("operational report upload %s drifted: got %q want %q", key, got, want)
		}
	}
	activationUpload := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Upload build-vs-activation report-only evidence")
	if got, want := activationUpload.ID, "image-activation-report-upload"; got != want {
		t.Fatalf("build-activation report upload id drifted: got %q want %q", got, want)
	}
	if got, want := activationUpload.If, "${{ always() && steps.prepare.outcome == 'success' }}"; got != want {
		t.Fatalf("build-activation report upload condition drifted: got %q want %q", got, want)
	}
	if activationUpload.ContinueOnError {
		t.Fatal("build-activation report upload must fail closed")
	}
	if got, want := activationUpload.Uses, "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"; got != want {
		t.Fatalf("build-activation report upload pin drifted: got %q want %q", got, want)
	}
	for key, want := range map[string]string{
		"path":                 "${{ env.FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR }}",
		"if-no-files-found":    "error",
		"retention-days":       "90",
		"include-hidden-files": "false",
		"overwrite":            "false",
	} {
		if got := activationUpload.With[key]; got != want {
			t.Fatalf("build-activation report upload %s drifted: got %q want %q", key, got, want)
		}
	}
	apply := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Apply exact authorized control-plane release")
	for key, want := range map[string]string{
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE":                "apply",
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID":          "${{ steps.operational-report-upload.outputs.artifact-id }}",
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST":      "${{ steps.operational-report-upload.outputs.artifact-digest }}",
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL":         "${{ steps.operational-report-upload.outputs.artifact-url }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID":     "${{ steps.image-activation-report-upload.outputs.artifact-id }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST": "${{ steps.image-activation-report-upload.outputs.artifact-digest }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL":    "${{ steps.image-activation-report-upload.outputs.artifact-url }}",
	} {
		if got := apply.Env[key]; got != want {
			t.Fatalf("operational apply %s drifted: got %q want %q", key, got, want)
		}
	}
	if got, want := strings.TrimSpace(apply.Run), "./scripts/upgrade_fugue_control_plane.sh"; got != want {
		t.Fatalf("operational apply entrypoint drifted: got %q want %q", got, want)
	}
	convergenceStep := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Verify image activation convergence")
	if got, want := convergenceStep.ID, "image-activation-convergence"; got != want {
		t.Fatalf("image activation convergence step id drifted: got %q want %q", got, want)
	}
	for _, required := range []string{
		"image-activation-convergence",
		`--build-artifact-plan "${evidence_dir}/build-artifact-plan.json"`,
		`--image-activation-plan "${evidence_dir}/image-activation-plan.json"`,
		`--image-activation-evidence "${evidence_dir}/image-activation-evidence.json"`,
		"complete)", "pending)", `printf 'status=%s\n'`, `printf 'pending_artifacts=%s\n'`,
	} {
		if !strings.Contains(convergenceStep.Run, required) {
			t.Fatalf("image activation convergence step must contain %q", required)
		}
	}

	publicUpload := workflowStepByName(t, deploy, "Upload release-domain public evidence")
	if got, want := publicUpload.If, "${{ always() && (steps.genesis_evidence.outcome == 'success' || steps.guarded_deploy.outcome == 'success') }}"; got != want {
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

	continuation, ok := workflow.Jobs["continue-release-convergence"]
	if !ok {
		t.Fatal("control-plane workflow must define release convergence continuation")
	}
	wantContinuationNeeds := workflowNeeds{"release-input-guard", "release-baseline", "release-gate", "build", "deploy"}
	if !reflect.DeepEqual(continuation.Needs, wantContinuationNeeds) {
		t.Fatalf("release convergence continuation dependencies drifted: got %v want %v", continuation.Needs, wantContinuationNeeds)
	}
	const continuationCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' && needs.deploy.outputs.image_activation_convergence == 'pending' }}"
	if continuation.If != continuationCondition {
		t.Fatalf("release convergence continuation condition drifted: got %q want %q", continuation.If, continuationCondition)
	}
	var continuationRunner string
	if err := continuation.RunsOn.Decode(&continuationRunner); err != nil {
		t.Fatalf("decode release convergence runner: %v", err)
	}
	if continuationRunner != "ubuntu-latest" || continuation.TimeoutMinutes != 10 || continuation.Environment != "production" ||
		!reflect.DeepEqual(continuation.Permissions, map[string]string{"actions": "write", "contents": "read"}) {
		t.Fatalf("release convergence continuation boundary drifted: runner=%q job=%+v", continuationRunner, continuation)
	}
	successor := workflowStepByName(t, continuation, "Dispatch exact release convergence successor")
	if successor.ID != "convergence_successor" {
		t.Fatalf("release convergence successor id drifted: %+v", successor)
	}
	for key, want := range map[string]string{
		"EXPECTED_SHA":                           "${{ inputs.expected_sha }}",
		"TARGET_SHA":                             "${{ inputs.target_sha }}",
		"PENDING_ACTIVATION_ARTIFACTS":           "${{ needs.deploy.outputs.pending_activation_artifacts }}",
		"SOURCE_IMAGE_CACHE_BASE_REF":            "${{ needs.release-baseline.outputs.image_cache_image_baseline_ref }}",
		"SOURCE_IMAGE_CACHE_IMAGE_DIGEST":        "${{ needs.build.outputs.image_cache_image_digest }}",
		"SOURCE_IMAGE_CACHE_IMAGE_REPOSITORY":    "${{ needs.build.outputs.image_cache_image_repository }}",
		"SOURCE_IMAGE_TARGETS":                   "${{ needs.build.outputs.image_targets }}",
		"SOURCE_VERIFIED_IMAGE_ARTIFACTS_JSON":   "${{ needs.build.outputs.verified_image_artifacts_json }}",
		"SOURCE_VERIFIED_IMAGE_ARTIFACTS_DIGEST": "${{ needs.build.outputs.verified_image_artifacts_digest }}",
		"GH_TOKEN":                               "${{ github.token }}",
		"REPOSITORY":                             "${{ github.repository }}",
	} {
		if got := successor.Env[key]; got != want {
			t.Fatalf("release convergence successor env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		`"${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		`"${PENDING_ACTIVATION_ARTIFACTS}" == 'image_cache'`,
		`"${state}" == 'active'`,
		`"${main_head}" == "${EXPECTED_SHA}"`,
		`[[ -z "${before}" ]] || exit 1`,
		"actions/workflows/${workflow_id}/dispatches",
		`-f "inputs[expected_sha]=${main_head}"`,
		`-f "inputs[target_sha]=${TARGET_SHA}"`,
		`-f 'inputs[image_cache_convergence]=true'`,
		`-f "inputs[convergence_source_run_id]=${GITHUB_RUN_ID}"`,
		"successor_number > GITHUB_RUN_NUMBER",
		`"${successor_sha}" == "${main_head}"`,
		`"schema_version": 2`,
		`"source_image_cache_artifact": image_cache_artifact`,
		`"source_image_cache_artifacts_digest": bound_digest`,
		`"baseline_advanced": False`,
		`"workflow_dispatch_attempted": True`,
	} {
		if !strings.Contains(successor.Run, required) {
			t.Fatalf("release convergence successor must contain %q", required)
		}
	}
	for _, forbidden := range []string{"/enable", "/disable", "/cancel", "git push", "updateRefs", "helm ", "kubectl "} {
		if strings.Contains(successor.Run, forbidden) {
			t.Fatalf("release convergence successor contains out-of-scope capability %q", forbidden)
		}
	}
	continuationUpload := workflowStepByName(t, continuation, "Upload release convergence successor evidence")
	if continuationUpload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		continuationUpload.With["path"] != "${{ runner.temp }}/fugue-release-convergence-successor/successor.json" ||
		continuationUpload.With["if-no-files-found"] != "error" || continuationUpload.With["retention-days"] != "90" ||
		continuationUpload.With["include-hidden-files"] != "false" || continuationUpload.With["overwrite"] != "false" {
		t.Fatalf("release convergence successor upload drifted: %+v", continuationUpload)
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
	const recordBaselineCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' && needs.deploy.outputs.image_activation_convergence == 'complete' }}"
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
		"SOURCE_SHA":               "${{ inputs.expected_sha }}",
		"TARGET_SHA":               "${{ inputs.target_sha }}",
		"GH_TOKEN":                 "${{ github.token }}",
	}
	if !reflect.DeepEqual(advanceBaseline.Env, wantAdvanceEnv) {
		t.Fatalf("record-release-baseline writer environment drifted: got %+v want %+v", advanceBaseline.Env, wantAdvanceEnv)
	}
	for _, required := range []string{
		"refs/heads/fugue-control-plane-release-baseline",
		`"${remote_object}" == "${EXPECTED_BASE_REF_OBJECT}"`,
		`"${EXPECTED_BASE_REF_OBJECT}" =~ ^[0-9a-f]{40}$`,
		`"${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ && "${SOURCE_SHA}" == "${GITHUB_SHA}"`,
		`"${remote_main}" == "${SOURCE_SHA}"`,
		`git merge-base --is-ancestor "${TARGET_SHA}" "${SOURCE_SHA}"`,
		`"${EXPECTED_BASE_SHA}" != "${TARGET_SHA}"`,
		`"${EXPECTED_BASE_REF_OBJECT}" != "${EXPECTED_BASE_SHA}"`,
		`"${represented_runtime}" == "${EXPECTED_BASE_SHA}"`,
		`"${represented_parent}" == "${represented_previous}"`,
		"git merge-base --is-ancestor",
		`readonly metadata_path='fugue-runtime-baseline.json'`,
		`"previous_baseline_object_sha": sys.argv[1]`,
		`"runtime_sha": sys.argv[2]`,
		`bounded_git_object_readback() {`,
		`for attempt in $(seq 1 15)`,
		`"${attempt}" == '15' ]] || sleep 2`,
		`carrier %s readback did not settle after 15 attempts`,
		`blob_sha="$(git hash-object -w --stdin`,
		`"repos/${GITHUB_REPOSITORY}/git/blobs/${blob_sha}"`,
		`tree_sha="$(git mktree`,
		`"repos/${GITHUB_REPOSITORY}/git/trees/${tree_sha}"`,
		`carrier_message="fugue runtime baseline carrier ${TARGET_SHA}"`,
		`).encode("utf-8") + message.encode("utf-8")`,
		`carrier_sha="$(git hash-object -t commit --stdin`,
		`"repos/${GITHUB_REPOSITORY}/git/commits/${carrier_sha}"`,
		`"${before_cas_object}" == "${EXPECTED_BASE_REF_OBJECT}" ]] || exit 1`,
		`rm -rf "${object_tmp}" || exit 1`,
		`trap - EXIT`,
		"beforeOid:$beforeOid",
		"afterOid:$afterOid",
		"-F 'force=false'",
		`-f "beforeOid=${EXPECTED_BASE_REF_OBJECT}"`,
		`-f "afterOid=${carrier_sha}"`,
		`settled='false'`,
		`"${observe_status}" == '0' && "${observed}" == "${carrier_sha}"`,
		`settled='true'`,
		`[[ "${settled}" == 'true' ]] || exit 1`,
		`response_exact='false'`,
		`"${mutation_status}" == '0' && "${echoed}" == "${mutation_id}"`,
		"baseline carrier CAS settled by exact bounded readback",
		`"${response_exact}" "${carrier_sha}" >&2 || true`,
	} {
		if !strings.Contains(advanceBaseline.Run, required) {
			t.Fatalf("release baseline advancement must contain %q", required)
		}
	}
	if strings.Count(advanceBaseline.Run, "gh api") != 8 ||
		strings.Count(advanceBaseline.Run, "gh api graphql") != 2 ||
		strings.Count(advanceBaseline.Run, "--method POST") != 3 ||
		strings.Count(advanceBaseline.Run, "bounded_git_object_readback") != 4 ||
		strings.Count(advanceBaseline.Run, "updateRefs(") != 1 ||
		strings.Count(advanceBaseline.Run, "-F 'force=false'") != 1 {
		t.Fatalf("release baseline writer API inventory drifted:\n%s", advanceBaseline.Run)
	}
	for _, forbidden := range []string{
		"refs/tags/", "git push", "git update-ref", "--force-with-lease",
		" -X ", "createRef", "deleteRef", "force=true", "curl ", "wget ",
		`-f "afterOid=${TARGET_SHA}"`, "--method PATCH", "--method DELETE",
	} {
		if strings.Contains(advanceBaseline.Run, forbidden) {
			t.Fatalf("release baseline writer contains out-of-scope capability %q", forbidden)
		}
	}

	successRearm, ok := workflow.Jobs["rearm-release-lane-on-success"]
	if !ok {
		t.Fatal("control-plane workflow must define the successful release-lane rearm finalizer")
	}
	wantSuccessNeeds := []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline"}
	for _, required := range wantSuccessNeeds {
		if !containsWorkflowNeed(successRearm.Needs, required) {
			t.Fatalf("successful lane rearm must wait for %s", required)
		}
	}
	if len(successRearm.Needs) != len(wantSuccessNeeds) {
		t.Fatalf("successful lane rearm has unexpected dependencies: %v", successRearm.Needs)
	}
	const successRearmCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' && needs.deploy.outputs.image_activation_convergence == 'complete' && needs.record-release-baseline.result == 'success' }}"
	if successRearm.If != successRearmCondition {
		t.Fatalf("successful lane rearm condition drifted: got %q want %q", successRearm.If, successRearmCondition)
	}
	var successRunner string
	if err := successRearm.RunsOn.Decode(&successRunner); err != nil {
		t.Fatalf("decode successful lane rearm runner: %v", err)
	}
	if successRunner != "ubuntu-latest" || successRearm.TimeoutMinutes != 10 || successRearm.Environment != "production" ||
		!reflect.DeepEqual(successRearm.Permissions, map[string]string{"actions": "write", "contents": "read"}) {
		t.Fatalf("successful lane rearm boundary drifted: runner=%q job=%+v", successRunner, successRearm)
	}
	successRearmStep := workflowStepByName(t, successRearm, "Disable successful release lane with exact readback")
	if successRearmStep.ID != "rearm_lane" {
		t.Fatalf("successful lane rearm step id drifted: %+v", successRearmStep)
	}
	for key, want := range map[string]string{
		"EXPECTED_SHA":                   "${{ inputs.expected_sha }}",
		"RELEASE_INPUT_GUARD_RESULT":     "${{ needs.release-input-guard.result }}",
		"RELEASE_BASELINE_RESULT":        "${{ needs.release-baseline.result }}",
		"RELEASE_GATE_RESULT":            "${{ needs.release-gate.result }}",
		"BUILD_RESULT":                   "${{ needs.build.result }}",
		"DEPLOY_RESULT":                  "${{ needs.deploy.result }}",
		"RECORD_RELEASE_BASELINE_RESULT": "${{ needs.record-release-baseline.result }}",
		"GH_TOKEN":                       "${{ github.token }}",
		"REPOSITORY":                     "${{ github.repository }}",
	} {
		if got := successRearmStep.Env[key]; got != want {
			t.Fatalf("successful lane rearm env %s drifted: got %q want %q", key, got, want)
		}
	}
	for _, required := range []string{
		`"${GITHUB_EVENT_NAME}" == 'workflow_dispatch'`,
		`"${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ && "${EXPECTED_SHA}" == "${GITHUB_SHA}"`,
		`"${main_head}" =~ ^[0-9a-f]{40}$`,
		`"main_matches_release_sha": main_matches == "true"`,
		`"observed_main_sha": main_head`,
		"git/ref/heads/fugue-control-plane-release-baseline",
		"for run_status in queued in_progress waiting pending requested",
		"actions/workflows/${workflow_id}/runs?status=${run_status}",
		`run_number <= current_run_number or attempt != 1`,
		`event != "workflow_dispatch" or branch != "main"`,
		`workflow_path != ".github/workflows/deploy-control-plane.yml"`,
		`"successor_run_count": len(successors)`,
		`"successor_runs": successors`,
		`"settlement_mode": settlement_mode`,
		`"${state_before}" == 'active' || "${state_before}" == 'disabled_manually'`,
		"actions/workflows/${workflow_id}/disable",
		"mutation_status=$?",
		"for attempt in 1 2 3 4 5",
		`"${state_after}" == 'disabled_manually'`,
		`"${settled}" == 'true'`,
		`"rearm_ref_mutation_attempted": False`,
		`"rearm_runtime_mutation_attempted": False`,
		`"rearm_cluster_mutation_attempted": False`,
		`"rearm_production_write": False`,
	} {
		if !strings.Contains(successRearmStep.Run, required) {
			t.Fatalf("successful lane rearm must contain %q", required)
		}
	}
	for _, forbidden := range []string{
		"/enable", "/dispatches", "/cancel", "git push", "git update-ref", "updateRefs", "createRef", "deleteRef",
		"--method POST", "--method PATCH", "--method DELETE", "helm ", "kubectl ", "k3s kubectl", "fugue app ",
		`[[ "${main_head}" == "${EXPECTED_SHA}" ]] || exit 1`,
		`[[ -z "${other_runs}" ]] || exit 1`,
	} {
		if strings.Contains(successRearmStep.Run, forbidden) {
			t.Fatalf("successful lane rearm contains out-of-scope capability %q", forbidden)
		}
	}
	successRearmUpload := workflowStepByName(t, successRearm, "Upload successful release lane rearm evidence")
	if successRearmUpload.Uses != "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" ||
		successRearmUpload.With["if-no-files-found"] != "error" || successRearmUpload.With["retention-days"] != "90" ||
		successRearmUpload.With["include-hidden-files"] != "false" || successRearmUpload.With["overwrite"] != "false" {
		t.Fatalf("successful lane rearm upload drifted: %+v", successRearmUpload)
	}

	freeze, ok := workflow.Jobs["freeze-release-lane-on-failure"]
	if !ok {
		t.Fatal("control-plane workflow must define the automatic release-lane freeze finalizer")
	}
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy", "continue-release-convergence", "record-release-baseline", "rearm-release-lane-on-success"} {
		if !containsWorkflowNeed(freeze.Needs, required) {
			t.Fatalf("release-lane freeze finalizer must wait for %s", required)
		}
	}
	if len(freeze.Needs) != 8 {
		t.Fatalf("release-lane freeze finalizer has unexpected dependencies: %v", freeze.Needs)
	}
	const freezeCondition = "${{ always() && (needs.release-input-guard.result != 'success' || needs.release-baseline.result != 'success' || needs.release-gate.result != 'success' || needs.build.result != 'success' || needs.deploy.result != 'success' || (needs.deploy.outputs.image_activation_convergence == 'complete' && (needs.record-release-baseline.result != 'success' || needs.rearm-release-lane-on-success.result != 'success')) || (needs.deploy.outputs.image_activation_convergence == 'pending' && needs.continue-release-convergence.result != 'success') || (needs.deploy.outputs.image_activation_convergence != 'complete' && needs.deploy.outputs.image_activation_convergence != 'pending')) }}"
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
		if jobName != "freeze-release-lane-on-failure" && jobName != "rearm-release-lane-on-success" &&
			jobName != "continue-release-convergence" && job.Permissions["actions"] == "write" {
			t.Fatalf("job %s must not receive actions:write", jobName)
		}
	}

	freezeRecord := workflowStepByName(t, freeze, "Record release lane freeze evidence")
	for key, want := range map[string]string{
		"RELEASE_INPUT_GUARD_RESULT":          "${{ needs.release-input-guard.result }}",
		"RELEASE_BASELINE_RESULT":             "${{ needs.release-baseline.result }}",
		"RELEASE_GATE_RESULT":                 "${{ needs.release-gate.result }}",
		"BUILD_RESULT":                        "${{ needs.build.result }}",
		"DEPLOY_RESULT":                       "${{ needs.deploy.result }}",
		"IMAGE_ACTIVATION_CONVERGENCE":        "${{ needs.deploy.outputs.image_activation_convergence }}",
		"CONTINUE_RELEASE_CONVERGENCE_RESULT": "${{ needs.continue-release-convergence.result }}",
		"RECORD_RELEASE_BASELINE_RESULT":      "${{ needs.record-release-baseline.result }}",
		"REARM_RELEASE_LANE_RESULT":           "${{ needs.rearm-release-lane-on-success.result }}",
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

func TestControlPlaneSuccessfulReleaseLaneRearmSettlementHarness(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	rearm := workflowStepByName(t, workflow.Jobs["rearm-release-lane-on-success"], "Disable successful release lane with exact readback")
	const (
		expectedSHA      = "1111111111111111111111111111111111111111"
		expectedBaseline = "2222222222222222222222222222222222222222"
		driftedOID       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	tests := []struct {
		name           string
		initialState   string
		mutate         string
		putExit        string
		mainDrift      bool
		invalidMain    bool
		otherRunRows   string
		otherRunStatus string
		deployResult   string
		wantPass       bool
		wantState      string
		wantWrites     string
		wantSuccessors int
		wantSettlement string
		wantMutation   bool
	}{
		{name: "successful response settles", initialState: "active", mutate: "true", putExit: "0", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n", wantSettlement: "disabled", wantMutation: true},
		{name: "lost response settles by readback", initialState: "active", mutate: "true", putExit: "23", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n", wantSettlement: "disabled", wantMutation: true},
		{name: "unsettled disable fails closed", initialState: "active", mutate: "false", putExit: "23", wantPass: false, wantState: "active", wantWrites: "PUT\n"},
		{name: "already disabled is idempotently settled", initialState: "disabled_manually", mutate: "false", putExit: "0", wantPass: true, wantState: "disabled_manually", wantSettlement: "already_disabled"},
		{name: "main advancement still closes one-shot lane", initialState: "active", mutate: "true", putExit: "0", mainDrift: true, wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n", wantSettlement: "disabled", wantMutation: true},
		{name: "invalid main ref blocks before disable", initialState: "active", mutate: "false", putExit: "0", invalidMain: true, wantPass: false, wantState: "active"},
		{name: "validated successor survives lane closure", initialState: "active", mutate: "true", putExit: "0", otherRunRows: "999\t11\t1\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n", otherRunStatus: "pending", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n", wantSuccessors: 1, wantSettlement: "disabled", wantMutation: true},
		{name: "duplicate successor snapshot is deduplicated", initialState: "active", mutate: "true", putExit: "0", otherRunRows: "999\t11\t1\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n999\t11\t1\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n", otherRunStatus: "pending", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n", wantSuccessors: 1, wantSettlement: "disabled", wantMutation: true},
		{name: "older active run fails closed", initialState: "active", mutate: "false", putExit: "0", otherRunRows: "444\t9\t1\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n", otherRunStatus: "pending", wantPass: false, wantState: "active"},
		{name: "non-dispatch active run fails closed", initialState: "active", mutate: "false", putExit: "0", otherRunRows: "999\t11\t1\tpush\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n", otherRunStatus: "pending", wantPass: false, wantState: "active"},
		{name: "retried successor fails closed", initialState: "active", mutate: "false", putExit: "0", otherRunRows: "999\t11\t2\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/deploy-control-plane.yml\n", otherRunStatus: "pending", wantPass: false, wantState: "active"},
		{name: "wrong workflow path fails closed", initialState: "active", mutate: "false", putExit: "0", otherRunRows: "999\t11\t1\tworkflow_dispatch\tmain\taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tpending\t.github/workflows/other.yml\n", otherRunStatus: "pending", wantPass: false, wantState: "active"},
		{name: "failed release result blocks before disable", initialState: "active", mutate: "false", putExit: "0", deployResult: "failure", wantPass: false, wantState: "active"},
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
			if err := os.WriteFile(stateFile, []byte(test.initialState+"\n"), 0o600); err != nil {
				t.Fatalf("write initial workflow state: %v", err)
			}
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sleep"), "#!/usr/bin/env bash\nexit 0\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\n"+
				"set -euo pipefail\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/disable\"* ]]; then\n"+
				"  printf 'PUT\\n' >>\"${MUTATION_LOG}\"\n"+
				"  if [[ \"${MUTATE}\" == 'true' ]]; then printf 'disabled_manually\\n' >\"${STATE_FILE}\"; fi\n"+
				"  exit \"${PUT_EXIT}\"\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/main\"* ]]; then printf '%s\\n' \"${OBSERVED_MAIN_SHA}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/fugue-control-plane-release-baseline\"* ]]; then printf '%s\\n' \"${OBSERVED_BASELINE_OID}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/runs?status=\"* ]]; then\n"+
				"  if [[ -n \"${OTHER_RUN_STATUS}\" && \"$*\" == *\"status=${OTHER_RUN_STATUS}&\"* ]]; then printf '%s' \"${OTHER_RUN_ROWS}\"; fi\n"+
				"  exit 0\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml\"* ]]; then cat \"${STATE_FILE}\"; exit 0; fi\n"+
				"exit 91\n")
			observedMain := expectedSHA
			if test.mainDrift {
				observedMain = driftedOID
			}
			if test.invalidMain {
				observedMain = "invalid-main-ref"
			}
			deployResult := test.deployResult
			if deployResult == "" {
				deployResult = "success"
			}
			command := exec.Command("bash", "-c", rearm.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STATE_FILE="+stateFile,
				"MUTATION_LOG="+mutationLog,
				"MUTATE="+test.mutate,
				"PUT_EXIT="+test.putExit,
				"EXPECTED_SHA="+expectedSHA,
				"RELEASE_INPUT_GUARD_RESULT=success",
				"RELEASE_BASELINE_RESULT=success",
				"RELEASE_GATE_RESULT=success",
				"BUILD_RESULT=success",
				"DEPLOY_RESULT="+deployResult,
				"RECORD_RELEASE_BASELINE_RESULT=success",
				"OBSERVED_MAIN_SHA="+observedMain,
				"OBSERVED_BASELINE_OID="+expectedBaseline,
				"OTHER_RUN_ROWS="+test.otherRunRows,
				"OTHER_RUN_STATUS="+test.otherRunStatus,
				"GITHUB_EVENT_NAME=workflow_dispatch",
				"GITHUB_REF=refs/heads/main",
				"GITHUB_RUN_ID=555",
				"GITHUB_RUN_NUMBER=10",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA="+expectedSHA,
				"GITHUB_WORKFLOW=deploy-control-plane",
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_OUTPUT="+filepath.Join(tempDir, "outputs"),
				"RUNNER_TEMP="+tempDir,
				"REPOSITORY=example/fugue",
				"GH_TOKEN=test",
			)
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("successful lane rearm settlement failed: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("successful lane rearm settlement unexpectedly passed: output=%s", output)
			}
			finalState, readErr := os.ReadFile(stateFile)
			if readErr != nil {
				t.Fatalf("read final workflow state: %v", readErr)
			}
			if strings.TrimSpace(string(finalState)) != test.wantState {
				t.Fatalf("final state = %q, want %q", finalState, test.wantState)
			}
			writes, readErr := os.ReadFile(mutationLog)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatalf("read mutation log: %v", readErr)
			}
			if string(writes) != test.wantWrites {
				t.Fatalf("mutation calls = %q, want %q", writes, test.wantWrites)
			}
			if test.wantPass {
				evidencePath := filepath.Join(tempDir, "fugue-release-lane-success-rearm", "success-rearm.json")
				evidenceData, readErr := os.ReadFile(evidencePath)
				if readErr != nil {
					t.Fatalf("read successful lane rearm evidence: %v", readErr)
				}
				var evidence map[string]any
				if err := json.Unmarshal(evidenceData, &evidence); err != nil {
					t.Fatalf("decode successful lane rearm evidence: %v", err)
				}
				if evidence["state_before"] != test.initialState || evidence["state_after"] != "disabled_manually" ||
					evidence["workflow_mutation_attempted"] != test.wantMutation || evidence["rearm_production_write"] != false ||
					evidence["baseline_ref_object"] != expectedBaseline || evidence["observed_main_sha"] != observedMain ||
					evidence["main_matches_release_sha"] != !test.mainDrift ||
					evidence["successor_run_count"] != float64(test.wantSuccessors) || evidence["settlement_mode"] != test.wantSettlement {
					t.Fatalf("successful lane rearm evidence drifted: %+v", evidence)
				}
				successors, ok := evidence["successor_runs"].([]any)
				if !ok || len(successors) != test.wantSuccessors {
					t.Fatalf("successor evidence drifted: %+v", evidence["successor_runs"])
				}
				if test.wantSuccessors == 1 {
					successor, ok := successors[0].(map[string]any)
					if !ok || successor["id"] != "999" || successor["run_number"] != float64(11) ||
						successor["run_attempt"] != float64(1) || successor["head_sha"] != driftedOID {
						t.Fatalf("validated successor identity drifted: %+v", successors[0])
					}
					statuses, ok := successor["observed_statuses"].([]any)
					if !ok || !reflect.DeepEqual(statuses, []any{"pending"}) {
						t.Fatalf("validated successor statuses drifted: %+v", successor)
					}
				}
			}
		})
	}
}

func TestControlPlaneFailureFreezeSettlementHarness(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	freeze := workflowStepByName(
		t,
		workflow.Jobs["freeze-release-lane-on-failure"],
		"Disable release lane and cancel queued runs",
	)
	tests := []struct {
		name         string
		initialState string
		mutate       string
		putExit      string
		wantPass     bool
		wantWrites   string
	}{
		{name: "already disabled is settled without a rejected mutation", initialState: "disabled_manually", mutate: "false", putExit: "1", wantPass: true},
		{name: "active lane is disabled", initialState: "active", mutate: "true", putExit: "0", wantPass: true, wantWrites: "PUT\n"},
		{name: "lost disable response settles by readback", initialState: "active", mutate: "true", putExit: "23", wantPass: true, wantWrites: "PUT\n"},
		{name: "unsettled disable fails closed", initialState: "active", mutate: "false", putExit: "23", wantPass: false, wantWrites: "PUT\nPUT\nPUT\nPUT\nPUT\n"},
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
			if err := os.WriteFile(stateFile, []byte(test.initialState+"\n"), 0o600); err != nil {
				t.Fatalf("write initial workflow state: %v", err)
			}
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sleep"), "#!/usr/bin/env bash\nexit 0\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\n"+
				"set -euo pipefail\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/disable\"* ]]; then\n"+
				"  printf 'PUT\\n' >>\"${MUTATION_LOG}\"\n"+
				"  if [[ \"${MUTATE}\" == 'true' ]]; then printf 'disabled_manually\\n' >\"${STATE_FILE}\"; fi\n"+
				"  exit \"${PUT_EXIT}\"\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/runs?status=\"* ]]; then exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml\"* ]]; then cat \"${STATE_FILE}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"/actions/runs/\"*\"/cancel\"* ]]; then exit 0; fi\n"+
				"exit 91\n")
			command := exec.Command("bash", "-c", freeze.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"STATE_FILE="+stateFile,
				"MUTATION_LOG="+mutationLog,
				"MUTATE="+test.mutate,
				"PUT_EXIT="+test.putExit,
				"CURRENT_RUN_ID=555",
				"REPOSITORY=example/fugue",
				"GH_TOKEN=test",
			)
			output, err := command.CombinedOutput()
			if test.wantPass && err != nil {
				t.Fatalf("failure-lane freeze settlement failed: %v output=%s", err, output)
			}
			if !test.wantPass && err == nil {
				t.Fatalf("failure-lane freeze settlement unexpectedly passed: output=%s", output)
			}
			finalState, readErr := os.ReadFile(stateFile)
			if readErr != nil {
				t.Fatalf("read final workflow state: %v", readErr)
			}
			wantState := "disabled_manually"
			if !test.wantPass {
				wantState = test.initialState
			}
			if strings.TrimSpace(string(finalState)) != wantState {
				t.Fatalf("final state = %q, want %q", finalState, wantState)
			}
			writes, readErr := os.ReadFile(mutationLog)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatalf("read mutation log: %v", readErr)
			}
			if string(writes) != test.wantWrites {
				t.Fatalf("mutation calls = %q, want %q", writes, test.wantWrites)
			}
		})
	}
}

func TestControlPlaneReleaseConvergenceSuccessorHarness(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	successor := workflowStepByName(t, workflow.Jobs["continue-release-convergence"], "Dispatch exact release convergence successor")
	const (
		expectedSHA = "1111111111111111111111111111111111111111"
		driftedSHA  = "2222222222222222222222222222222222222222"
	)
	sourceArtifact := map[string]any{
		"component":                "image_cache",
		"config_digest":            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"immutable_ref":            "ghcr.io/example/fugue-image-cache@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"oci_revision":             expectedSHA,
		"platform_manifest_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"repository":               "ghcr.io/example/fugue-image-cache",
		"source_tag":               expectedSHA,
		"top_digest":               "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"verification":             "registry_manifest_config_and_layer_get",
	}
	sourceArtifactsJSON, err := json.Marshal([]any{sourceArtifact})
	if err != nil {
		t.Fatalf("marshal source image artifacts: %v", err)
	}
	sourceArtifactsDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(sourceArtifactsJSON))
	tests := []struct {
		name               string
		workflowState      string
		preexisting        bool
		mainSHA            string
		successorSHA       string
		driftAfterDispatch bool
		wantPass           bool
		wantDispatches     string
	}{
		{name: "dispatches one exact successor", workflowState: "active", successorSHA: expectedSHA, wantPass: true, wantDispatches: "POST\n"},
		{name: "disabled lane fails before dispatch", workflowState: "disabled_manually", successorSHA: expectedSHA},
		{name: "advanced main fails before dispatch", workflowState: "active", mainSHA: driftedSHA, successorSHA: expectedSHA},
		{name: "preexisting successor fails before dispatch", workflowState: "active", preexisting: true, successorSHA: expectedSHA},
		{name: "wrong successor SHA fails closed", workflowState: "active", successorSHA: driftedSHA, wantDispatches: "POST\n"},
		{name: "main drift after dispatch fails closed", workflowState: "active", successorSHA: expectedSHA, driftAfterDispatch: true, wantDispatches: "POST\n"},
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
			dispatchedFile := filepath.Join(tempDir, "dispatched")
			mutationLog := filepath.Join(tempDir, "mutations")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "sleep"), "#!/usr/bin/env bash\nexit 0\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\n"+
				"set -euo pipefail\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/dispatches\"* ]]; then\n"+
				"  printf 'POST\\n' >>\"${MUTATION_LOG}\"\n"+
				"  : >\"${DISPATCHED_FILE}\"\n"+
				"  exit 0\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/runs?status=\"* ]]; then\n"+
				"  if [[ \"$*\" == *\"status=queued&\"* && ( \"${PREEXISTING}\" == 'true' || -f \"${DISPATCHED_FILE}\" ) ]]; then\n"+
				"    printf '999\\t11\\t1\\tworkflow_dispatch\\tmain\\t%s\\tqueued\\t.github/workflows/deploy-control-plane.yml\\n' \"${SUCCESSOR_SHA}\"\n"+
				"  fi\n"+
				"  exit 0\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"git/ref/heads/main\"* ]]; then\n"+
				"  if [[ \"${DRIFT_AFTER_DISPATCH}\" == 'true' && -f \"${DISPATCHED_FILE}\" ]]; then printf '%s\\n' \"${DRIFTED_SHA}\"; else printf '%s\\n' \"${MAIN_SHA}\"; fi\n"+
				"  exit 0\n"+
				"fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml\"* ]]; then printf '%s\\n' \"${WORKFLOW_STATE}\"; exit 0; fi\n"+
				"exit 91\n")
			mainSHA := test.mainSHA
			if mainSHA == "" {
				mainSHA = expectedSHA
			}
			command := exec.Command("bash", "-c", successor.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"EXPECTED_SHA="+expectedSHA,
				"TARGET_SHA="+expectedSHA,
				"PENDING_ACTIVATION_ARTIFACTS=image_cache",
				"SOURCE_IMAGE_CACHE_BASE_REF="+driftedSHA,
				"SOURCE_IMAGE_CACHE_IMAGE_DIGEST=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				"SOURCE_IMAGE_CACHE_IMAGE_REPOSITORY=ghcr.io/example/fugue-image-cache",
				"SOURCE_IMAGE_TARGETS=image_cache",
				"SOURCE_VERIFIED_IMAGE_ARTIFACTS_JSON="+string(sourceArtifactsJSON),
				"SOURCE_VERIFIED_IMAGE_ARTIFACTS_DIGEST="+sourceArtifactsDigest,
				"REPOSITORY=example/fugue",
				"GH_TOKEN=test",
				"WORKFLOW_STATE="+test.workflowState,
				"MAIN_SHA="+mainSHA,
				"SUCCESSOR_SHA="+test.successorSHA,
				"DRIFTED_SHA="+driftedSHA,
				"PREEXISTING="+strconv.FormatBool(test.preexisting),
				"DRIFT_AFTER_DISPATCH="+strconv.FormatBool(test.driftAfterDispatch),
				"DISPATCHED_FILE="+dispatchedFile,
				"MUTATION_LOG="+mutationLog,
				"GITHUB_EVENT_NAME=workflow_dispatch",
				"GITHUB_REF=refs/heads/main",
				"GITHUB_RUN_ID=555",
				"GITHUB_RUN_NUMBER=10",
				"GITHUB_RUN_ATTEMPT=1",
				"GITHUB_SHA="+expectedSHA,
				"GITHUB_WORKFLOW=deploy-control-plane",
				"GITHUB_REPOSITORY=example/fugue",
				"GITHUB_OUTPUT="+filepath.Join(tempDir, "outputs"),
				"RUNNER_TEMP="+tempDir,
			)
			output, runErr := command.CombinedOutput()
			if test.wantPass && runErr != nil {
				t.Fatalf("release convergence successor failed: %v output=%s", runErr, output)
			}
			if !test.wantPass && runErr == nil {
				t.Fatalf("release convergence successor unexpectedly passed: output=%s", output)
			}
			writes, readErr := os.ReadFile(mutationLog)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatalf("read mutation log: %v", readErr)
			}
			if string(writes) != test.wantDispatches {
				t.Fatalf("dispatch calls = %q, want %q", writes, test.wantDispatches)
			}
			if test.wantPass {
				evidenceData, readErr := os.ReadFile(filepath.Join(tempDir, "fugue-release-convergence-successor", "successor.json"))
				if readErr != nil {
					t.Fatalf("read convergence successor evidence: %v", readErr)
				}
				var evidence map[string]any
				if err := json.Unmarshal(evidenceData, &evidence); err != nil {
					t.Fatalf("decode convergence successor evidence: %v", err)
				}
				if evidence["baseline_advanced"] != false || evidence["workflow_dispatch_attempted"] != true ||
					evidence["successor_target_sha"] != expectedSHA || evidence["schema_version"] != float64(2) ||
					evidence["source_image_cache_artifacts_digest"] != sourceArtifactsDigest {
					t.Fatalf("convergence successor evidence drifted: %+v", evidence)
				}
			}
		})
	}
}

func TestControlPlaneReleaseConvergenceAuthorizationHarness(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control-plane workflow: %v", err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	guard := workflowStepByName(t, workflow.Jobs["release-input-guard"], "Guard exact main commit authorization")
	const (
		expectedSHA  = "1111111111111111111111111111111111111111"
		imageBaseSHA = "2222222222222222222222222222222222222222"
		sourceRunID  = "555"
		successorRun = "777"
		successorNum = 11
		sourceRunNum = 10
	)
	tests := []struct {
		name              string
		convergence       string
		sourceID          string
		sourceConclusion  string
		wrongSuccessor    bool
		badArtifactDigest bool
		noncanonical      bool
		wantPass          bool
	}{
		{name: "ordinary dispatch needs no successor proof", convergence: "false", wantPass: true},
		{name: "ordinary dispatch rejects a source run", convergence: "false", sourceID: sourceRunID},
		{name: "verified successor authorization passes", convergence: "true", sourceID: sourceRunID, sourceConclusion: "success", wantPass: true},
		{name: "proof bound to another successor fails", convergence: "true", sourceID: sourceRunID, sourceConclusion: "success", wrongSuccessor: true},
		{name: "proof with a drifted image artifact digest fails", convergence: "true", sourceID: sourceRunID, sourceConclusion: "success", badArtifactDigest: true},
		{name: "failed source run is rejected", convergence: "true", sourceID: sourceRunID, sourceConclusion: "failure"},
		{name: "noncanonical proof is rejected", convergence: "true", sourceID: sourceRunID, sourceConclusion: "success", noncanonical: true},
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
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "timeout"), "#!/usr/bin/env bash\nset -euo pipefail\nshift 2\nexec \"$@\"\n")
			writeRP5PromotionExecutable(t, filepath.Join(mockBin, "gh"), "#!/usr/bin/env bash\n"+
				"set -euo pipefail\n"+
				"if [[ \"$*\" == *\"git/ref/heads/main\"* ]]; then printf '%s\\n' \"${EXPECTED_SHA}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/runs/${SOURCE_RUN_ID}\"* ]]; then\n"+
				"  printf '%s\\t%s\\t1\\tworkflow_dispatch\\tmain\\t%s\\tcompleted\\t%s\\t.github/workflows/deploy-control-plane.yml\\n' \"${SOURCE_RUN_ID}\" \"${SOURCE_RUN_NUMBER}\" \"${EXPECTED_SHA}\" \"${SOURCE_CONCLUSION}\"\n"+
				"  exit 0\n"+
				"fi\n"+
				"exit 91\n")

			proofPath := filepath.Join(tempDir, "authorization", "successor.json")
			if test.convergence == "true" {
				if err := os.MkdirAll(filepath.Dir(proofPath), 0o700); err != nil {
					t.Fatalf("create proof directory: %v", err)
				}
				boundSuccessor := successorRun
				if test.wrongSuccessor {
					boundSuccessor = "778"
				}
				artifact := map[string]any{
					"component":                "image_cache",
					"config_digest":            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"immutable_ref":            "ghcr.io/example/fugue-image-cache@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
					"oci_revision":             expectedSHA,
					"platform_manifest_digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"repository":               "ghcr.io/example/fugue-image-cache",
					"source_tag":               expectedSHA,
					"top_digest":               "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
					"verification":             "registry_manifest_config_and_layer_get",
				}
				artifactBytes, err := json.Marshal([]any{artifact})
				if err != nil {
					t.Fatalf("marshal image-cache artifact: %v", err)
				}
				artifactDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(artifactBytes))
				if test.badArtifactDigest {
					artifactDigest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
				}
				proof := map[string]any{
					"schema_version":                      2,
					"workflow":                            "deploy-control-plane",
					"repository":                          "example/fugue",
					"source_run_id":                       sourceRunID,
					"source_run_attempt":                  1,
					"source_head_sha":                     expectedSHA,
					"source_image_cache_artifact":         artifact,
					"source_image_cache_artifacts_digest": artifactDigest,
					"source_image_cache_base_ref":         imageBaseSHA,
					"pending_activation_artifacts":        []string{"image_cache"},
					"successor_run_id":                    boundSuccessor,
					"successor_run_number":                successorNum,
					"successor_status":                    "queued",
					"successor_target_sha":                expectedSHA,
					"baseline_advanced":                   false,
					"cluster_mutation_attempted":          false,
					"workflow_dispatch_attempted":         true,
					"recorded_at":                         "2026-07-29T04:00:00+00:00",
				}
				encoded, err := json.Marshal(proof)
				if err != nil {
					t.Fatalf("marshal proof: %v", err)
				}
				if test.noncanonical {
					encoded = append([]byte(" "), encoded...)
				}
				encoded = append(encoded, '\n')
				if err := os.WriteFile(proofPath, encoded, 0o600); err != nil {
					t.Fatalf("write proof: %v", err)
				}
			}

			command := exec.Command("bash", "-c", guard.Run)
			command.Env = append(os.Environ(),
				"PATH="+mockBin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"EXPECTED_SHA="+expectedSHA,
				"ACTUAL_SHA="+expectedSHA,
				"TARGET_SHA="+expectedSHA,
				"IMAGE_CACHE_CONVERGENCE="+test.convergence,
				"PUBLIC_DATA_PLANE_ADOPTION_RUN_ID=",
				"PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST=",
				"PUBLIC_DATA_PLANE_ADOPTION_BASELINE="+filepath.Join(tempDir, "stage1-baseline.json"),
				"PUBLIC_DATA_PLANE_ADOPTION_TRACE="+filepath.Join(tempDir, "stage1-trace.json"),
				"CONVERGENCE_SOURCE_RUN_ID="+test.sourceID,
				"CONVERGENCE_AUTHORIZATION_FILE="+proofPath,
				"GH_TOKEN=test",
				"REPOSITORY=example/fugue",
				"EVENT_NAME=workflow_dispatch",
				"EVENT_REF=refs/heads/main",
				"EVENT_REF_NAME=main",
				"EVENT_REF_TYPE=branch",
				"GITHUB_RUN_ID="+successorRun,
				"GITHUB_RUN_NUMBER="+strconv.Itoa(successorNum),
				"GITHUB_RUN_ATTEMPT=1",
				"SOURCE_RUN_ID="+sourceRunID,
				"SOURCE_RUN_NUMBER="+strconv.Itoa(sourceRunNum),
				"SOURCE_CONCLUSION="+test.sourceConclusion,
			)
			output, runErr := command.CombinedOutput()
			if test.wantPass && runErr != nil {
				t.Fatalf("release convergence authorization failed: %v output=%s", runErr, output)
			}
			if !test.wantPass && runErr == nil {
				t.Fatalf("release convergence authorization unexpectedly passed: output=%s", output)
			}
		})
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
