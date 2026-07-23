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
		{name: "second canonical forward carrier", refObject: secondCarrier, wantDomain: targetSHA},
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
  cat "${EXPECTED_COMMIT_RESPONSE_FILE}"
  exit 0
fi
if [[ "${arguments}" == *'updateRefs('* ]]; then
  case "${MODE}" in
    success|committed_exit7|committed_wrong_echo|readback_transient|readback_unavailable|readback_target_exit7|blob_post_exit7|tree_post_exit7|commit_post_exit7)
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
	}{
		{mode: "success", wantResponseExact: "true", wantReadbacks: "2"},
		{mode: "committed_exit7", wantResponseExact: "false", wantReadbacks: "2"},
		{mode: "committed_wrong_echo", wantResponseExact: "false", wantReadbacks: "2"},
		{mode: "readback_transient", wantResponseExact: "true", wantReadbacks: "3"},
		{mode: "blob_post_exit7", wantResponseExact: "true", wantReadbacks: "2"},
		{mode: "tree_post_exit7", wantResponseExact: "true", wantReadbacks: "2"},
		{mode: "commit_post_exit7", wantResponseExact: "true", wantReadbacks: "2"},
	}
	for _, test := range positive {
		t.Run(test.mode, func(t *testing.T) {
			got := runWriter(t, test.mode)
			settled := fmt.Sprintf("response_exact=%s", test.wantResponseExact)
			if got.err != nil || got.blobPosts != 1 || got.treePosts != 1 || got.commitPosts != 1 ||
				got.mutationCalls != 1 || got.readbackCalls != test.wantReadbacks || got.refObject != targetCarrier ||
				!strings.Contains(string(got.output), settled) {
				t.Fatalf("recorder failed carrier settlement: mode=%s err=%v posts=%d/%d/%d mutations=%d readbacks=%q ref=%s output=%q", test.mode, got.err, got.blobPosts, got.treePosts, got.commitPosts, got.mutationCalls, got.readbackCalls, got.refObject, got.output)
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
		wantReadbacks   string
	}{
		{mode: "blob_readback_drift", wantBlobPosts: 1},
		{mode: "pre_cas_ref_drift", wantBlobPosts: 1, wantTreePosts: 1, wantCommitPosts: 1, wantReadbacks: "1"},
	}
	for _, test := range preCASNegative {
		t.Run(test.mode, func(t *testing.T) {
			got := runWriter(t, test.mode)
			if got.err == nil || got.blobPosts != test.wantBlobPosts || got.treePosts != test.wantTreePosts ||
				got.commitPosts != test.wantCommitPosts || got.mutationCalls != 0 || got.readbackCalls != test.wantReadbacks ||
				got.refObject != baseCarrier || strings.Contains(string(got.output), "baseline carrier CAS settled") {
				t.Fatalf("recorder crossed CAS boundary after pre-CAS failure: mode=%s err=%v posts=%d/%d/%d mutations=%d readbacks=%q ref=%s output=%q log=%q", test.mode, got.err, got.blobPosts, got.treePosts, got.commitPosts, got.mutationCalls, got.readbackCalls, got.refObject, got.output, got.log)
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
	assertWorkflowSourceDigest(t, data, "1ed163622569afd38a3bd2535e2f350656290db49c952e8ff8cac542d5aef013")
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse control-plane workflow: %v", err)
	}
	actionPath := filepath.Join("..", "..", ".github", "actions", "operational-domain-guarded-deploy", "action.yml")
	actionData, err := os.ReadFile(actionPath)
	if err != nil {
		t.Fatalf("read operational-domain guarded deploy action: %v", err)
	}
	assertWorkflowSourceDigest(t, actionData, "bb52d445d98b0f5e6d10a42b2bfabb4c22a30f88e28ab0bf38177c4c8c151057")
	var operationalAction compositeReleaseAction
	if err := yaml.Unmarshal(actionData, &operationalAction); err != nil {
		t.Fatalf("parse operational-domain guarded deploy action: %v", err)
	}
	workflowRootNode := workflowDocumentMapping(t, data)
	assertWorkflowMappingKeys(t, workflowRootNode, "name", "on", "permissions", "concurrency", "jobs")
	assertWorkflowRunDigests(t, workflow.Jobs, map[string]string{
		"release-input-guard/Guard exact main commit authorization":                         "36817d224982821ad3eb81a44fd42dd50bfa479915e48b339010fae5e19ae1a5",
		"release-baseline/Resolve release-domain baseline":                                  "4a510777f17f06c60e8abb6900cfb15a90b430844ad05effeee84a0c37392151",
		"release-baseline/Resolve live image metadata":                                      "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"release-baseline/Compute live-to-target release changed files":                     "3fd4596b94b2bf2cef792ccc89752f72e371fedc51f0953821f341f74d249992",
		"release-gate/Prepare pinned ripgrep for release safety contracts":                  "fd3284573ed17f45090180e1d168e8c0f143e088586882168e5cf60637390761",
		"release-gate/Verify generated OpenAPI artifacts":                                   "7b93bd9f923a238d19f6aed52847bc1a10000fa5c6fb85fc269f2bf1101dad08",
		"release-gate/Verify release-domain safety contracts":                               "0a71d9858c02ceb5aa8aa188313276dc4a63db5dae5cc856323c533fd1051144",
		"release-gate/Run Go tests":                                                         "1bb497e3e13a1105cf24e3359fa3ef75de08b66ff8a2839cd7f9ea97824d9eb3",
		"build/Compute image metadata":                                                      "12f6dcc38d6f1597416aae34a1c2fa4efda4c6353c5fcbc0eee6c66ee3ccb5b6",
		"build/Compute image build plan":                                                    "e545c87a2385902616eb8fa652954970e0de7e47ffe4c8fea46eb03cb71e5ea0",
		"build/Publish verified control-plane image provenance":                             "6561990b64acc7e6ffe4f97b6f8424edf28154444d579610aa60fb545f15cb07",
		"deploy/Record deploy job budget origin":                                            "752b51a8ce207fa8a0f61a05d9d4deea9990882c5f846f369e916a3be2bfb677",
		"deploy/Build private release-domain tools":                                         "1017c0bb023803233350b68c1b434ca34c01e82d04bc0ad8a80b03f2c437ead2",
		"deploy/Write genesis public release evidence":                                      "f9cda719ba304a529408a14275a87be590e9fa0422dbfbf2bfecf18c758b401d",
		"deploy/Guard stateful component files":                                             "65a7da57e288071328518bc5bd3ee9c0b5726ca97dd9a2b33672fe351eb544c6",
		"deploy/Prepare authoritative DNS DiG runtime":                                      "90038169ec5ef9b2d60a35fa9271e53ee66bdfb1fbaec61ab035674a7b68f6af",
		"deploy/Verify local deploy prerequisites":                                          "e94b5f2811734f45c3ff37be7bf5ef1b85321e8e4b4f2e6821e18e23ff8dff01",
		"deploy/Explain runner and fail closed target":                                      "afab1c1aa3b6305ac3fdf982640fce8d81781c339cea714f11e2bde65a3b4475",
		"deploy/Resolve live image metadata":                                                "7c2b32da72eb0a2020df38e40afcf99cf9e778d60e158a36960ac4ff4ac65267",
		"deploy/Prove explicitly authorized stale pre-Helm release recovery":                "e4af592e5c1cfc427e3f53fa3b2c835bd134019117fc53ffe9e7981944afe312",
		"deploy/Remove stale release recovery proof":                                        "43203d3cc033dd8ddca207f84eeee8877791c528b99ccae888b7097b2dea077d",
		"record-release-baseline/Advance dedicated forward-only release baseline branch":    "54ed82f5027c66a622a0033be71b7d1b9182de690e431a3572bb48201123d7af",
		"rearm-release-lane-on-success/Disable successful release lane with exact readback": "47f3662d1f73fd307553e0249301f60b4248e9dbe952ef522428204fced61e7e",
		"freeze-release-lane-on-failure/Record release lane freeze evidence":                "647f2abd75678bcf08439bbb465cc0fc976c2d6c8949f82bcd3a045fbfbd7022",
		"freeze-release-lane-on-failure/Disable release lane and cancel queued runs":        "1e957fb32c9a8c4864c4e43a1bd5878738957696843f4bcfba62d118f7692869",
		"freeze-release-lane-on-failure/Require release lane freeze evidence":               "a583f75fce52b2c2e957c16f290af7ab4367ef35a3b4d22adeef76b2446c6cd4",
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
				{"name", "env", "run"},
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
				{"name", "if", "env", "uses"},
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

	upgrade := workflowStepByName(t, deploy, "Upgrade Fugue control plane through uploaded operational evidence")
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
	for key, want := range map[string]string{
		"FUGUE_RELEASE_DOMAIN_BASE_SHA":                        "${{ needs.release-baseline.outputs.domain_base_sha }}",
		"FUGUE_RELEASE_DOMAIN_TARGET_SHA":                      "${{ github.sha }}",
		"FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL":                   "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
		"FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL":                   "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
		"FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE":            "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
		"FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE":         "${{ runner.temp }}/fugue-release-domain-public/operational-domain-evidence.json",
		"FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR":     "${{ runner.temp }}/fugue-release-domain-public/build-activation-evidence",
		"FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST": "${{ needs.build.outputs.verified_image_artifacts_digest }}",
		"FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS":                   "${{ needs.build.outputs.image_targets }}",
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
	}
	gotActionSteps := make([]string, 0, len(operationalAction.Runs.Steps))
	for _, step := range operationalAction.Runs.Steps {
		gotActionSteps = append(gotActionSteps, step.Name)
	}
	if !reflect.DeepEqual(gotActionSteps, wantActionSteps) {
		t.Fatalf("operational deploy action order drifted: got %q want %q", gotActionSteps, wantActionSteps)
	}
	prepare := workflowStepByName(t, releaseWorkflowJob{Steps: operationalAction.Runs.Steps}, "Prepare operational-domain report-only evidence")
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
	if got, want := operationalUpload.If, "always()"; got != want {
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
	if got, want := activationUpload.If, "always()"; got != want {
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
		`"${EXPECTED_BASE_REF_OBJECT}" =~ ^[0-9a-f]{40}$`,
		`"${TARGET_SHA}" == "${GITHUB_SHA}"`,
		`"${EXPECTED_BASE_REF_OBJECT}" != "${EXPECTED_BASE_SHA}"`,
		`"${represented_runtime}" == "${EXPECTED_BASE_SHA}"`,
		`"${represented_parent}" == "${represented_previous}"`,
		"git merge-base --is-ancestor",
		`readonly metadata_path='fugue-runtime-baseline.json'`,
		`"previous_baseline_object_sha": sys.argv[1]`,
		`"runtime_sha": sys.argv[2]`,
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
	if strings.Count(advanceBaseline.Run, "gh api") != 10 ||
		strings.Count(advanceBaseline.Run, "gh api graphql") != 2 ||
		strings.Count(advanceBaseline.Run, "--method POST") != 3 ||
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
	const successRearmCondition = "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' && needs.record-release-baseline.result == 'success' }}"
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
		`"${main_head}" == "${EXPECTED_SHA}"`,
		"git/ref/heads/fugue-control-plane-release-baseline",
		"for run_status in queued in_progress waiting pending requested",
		"actions/workflows/${workflow_id}/runs?status=${run_status}",
		`"${state_before}" == 'active'`,
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
	for _, required := range []string{"release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline", "rearm-release-lane-on-success"} {
		if !containsWorkflowNeed(freeze.Needs, required) {
			t.Fatalf("release-lane freeze finalizer must wait for %s", required)
		}
	}
	if len(freeze.Needs) != 7 {
		t.Fatalf("release-lane freeze finalizer has unexpected dependencies: %v", freeze.Needs)
	}
	const freezeCondition = "${{ always() && (needs.release-input-guard.result != 'success' || needs.release-baseline.result != 'success' || needs.release-gate.result != 'success' || needs.build.result != 'success' || needs.deploy.result != 'success' || needs.record-release-baseline.result != 'success' || needs.rearm-release-lane-on-success.result != 'success') }}"
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
		if jobName != "freeze-release-lane-on-failure" && jobName != "rearm-release-lane-on-success" && job.Permissions["actions"] == "write" {
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
		"REARM_RELEASE_LANE_RESULT":      "${{ needs.rearm-release-lane-on-success.result }}",
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
		name         string
		initialState string
		mutate       string
		putExit      string
		mainDrift    bool
		otherRuns    string
		deployResult string
		wantPass     bool
		wantState    string
		wantWrites   string
	}{
		{name: "successful response settles", initialState: "active", mutate: "true", putExit: "0", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n"},
		{name: "lost response settles by readback", initialState: "active", mutate: "true", putExit: "23", wantPass: true, wantState: "disabled_manually", wantWrites: "PUT\n"},
		{name: "unsettled disable fails closed", initialState: "active", mutate: "false", putExit: "23", wantPass: false, wantState: "active", wantWrites: "PUT\n"},
		{name: "already disabled cannot replay", initialState: "disabled_manually", mutate: "false", putExit: "0", wantPass: false, wantState: "disabled_manually"},
		{name: "main drift blocks before disable", initialState: "active", mutate: "false", putExit: "0", mainDrift: true, wantPass: false, wantState: "active"},
		{name: "active deploy run blocks before disable", initialState: "active", mutate: "false", putExit: "0", otherRuns: "999\n", wantPass: false, wantState: "active"},
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
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml/runs?status=\"* ]]; then printf '%s' \"${OTHER_RUNS}\"; exit 0; fi\n"+
				"if [[ \"$*\" == *\"actions/workflows/deploy-control-plane.yml\"* ]]; then cat \"${STATE_FILE}\"; exit 0; fi\n"+
				"exit 91\n")
			observedMain := expectedSHA
			if test.mainDrift {
				observedMain = driftedOID
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
				"OTHER_RUNS="+test.otherRuns,
				"GITHUB_EVENT_NAME=workflow_dispatch",
				"GITHUB_REF=refs/heads/main",
				"GITHUB_RUN_ID=555",
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
				if evidence["state_before"] != "active" || evidence["state_after"] != "disabled_manually" ||
					evidence["workflow_mutation_attempted"] != true || evidence["rearm_production_write"] != false ||
					evidence["baseline_ref_object"] != expectedBaseline {
					t.Fatalf("successful lane rearm evidence drifted: %+v", evidence)
				}
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
