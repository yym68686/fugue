package declarativerelease

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCIHasOneDeclarativeProductionEntryPoint(t *testing.T) {
	entries, err := os.ReadDir("../../.github/workflows")
	if err != nil {
		t.Fatal(err)
	}
	workflowFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		workflowFiles = append(workflowFiles, entry.Name())
	}
	sort.Strings(workflowFiles)
	if !reflect.DeepEqual(workflowFiles, []string{"build-cli.yml", "ci.yml", "release-cli.yml"}) {
		t.Fatalf("legacy or parallel production workflow remains reachable: %v", workflowFiles)
	}

	filename := "../../.github/workflows/ci.yml"
	raw, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode CI workflow: %v", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatal("CI workflow root is invalid")
	}
	root := document.Content[0]
	on := yamlMappingValue(t, root, "on")
	triggerKeys := yamlMappingKeys(t, on)
	if !reflect.DeepEqual(triggerKeys, []string{"pull_request", "push", "schedule"}) {
		t.Fatalf("CI triggers are not push/PR/audit only: %v", triggerKeys)
	}
	jobs := yamlMappingValue(t, root, "jobs")
	jobKeys := yamlMappingKeys(t, jobs)
	if !reflect.DeepEqual(jobKeys, []string{
		"audit", "component-build", "deploy_api", "deploy_controller", "deploy_edge_client", "deploy_edge_control", "deploy_edge_worker",
		"deploy_image_cache", "deploy_release_guardian", "deploy_schema", "deploy_telemetry", "monitor-plan", "monitor-production", "prepush",
	}) {
		t.Fatalf("CI job inventory is not the single component pipeline: %v", jobKeys)
	}
	source := string(raw)
	for _, required := range []string{
		"\"${RELEASE_TOOL}\" plan",
		"\"${RELEASE_TOOL}\" build",
		"uses: ./.github/actions/deploy-declarative-component",
		"group: fugue-production-api",
		"group: fugue-production-controller",
		"group: '${{ matrix.concurrency }}'",
		"group: fugue-production-image-cache",
		"group: fugue-production-release-guardian",
		"group: fugue-production-schema",
		"group: fugue-production-telemetry",
		"group: fugue-build-${{ matrix.build_lane }}-${{ github.sha }}",
		"environment: production",
		"needs: [prepush, component-build, deploy_schema]",
		"needs: [prepush, component-build, deploy_api]",
		"needs: [prepush, component-build, deploy_api, deploy_edge_control]",
		"edge_control_matrix",
		"edge_client_matrix",
		"edge_worker_matrix",
		"needs: [prepush, component-build, deploy_controller]",
		"'*/5 * * * *'",
		"\"${RELEASE_TOOL}\" emit-monitor-output",
		"\"${RELEASE_TOOL}\" monitor",
		"needs: monitor-plan",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("CI workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"deploy_edge_client_de", "deploy_edge_client_us", "deploy_edge_control_de", "deploy_edge_control_us", "deploy_edge_worker_de", "deploy_edge_worker_us",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("CI workflow retained fixed group job %q", forbidden)
		}
	}
	actionRaw, err := os.ReadFile("../../.github/actions/deploy-declarative-component/action.yml")
	if err != nil {
		t.Fatal(err)
	}
	var actionDocument yaml.Node
	if err := yaml.Unmarshal(actionRaw, &actionDocument); err != nil {
		t.Fatalf("decode declarative component action: %v", err)
	}
	action := string(actionRaw)
	if strings.Count(action, "FUGUE_REGISTRY_PASSWORD: ${{ inputs.registry-token }}") != 3 {
		t.Fatal("prepare, execute, and reconcile must share the masked read-only registry credential")
	}
	for _, required := range []string{
		"\"${RELEASE_TOOL}\" prepare", "\"${RELEASE_TOOL}\" emit-delivery", "\"${RELEASE_TOOL}\" execute", "\"${RELEASE_TOOL}\" guardian-submit", "\"${RELEASE_TOOL}\" reconcile",
		"Upload the durable prewrite plan before mutation", "Upload terminal component receipt",
		"continue-on-error: true", "if: steps.delivery.outputs.writer == 'direct' && steps.execute_direct.outcome == 'failure'",
		"if: steps.delivery.outputs.writer == 'guardian'",
	} {
		if strings.Count(action, required) != 1 {
			t.Fatalf("declarative component action must contain %q exactly once", required)
		}
	}
	for _, forbidden := range []string{
		"workflow_" + "dispatch", "deploy-" + "control-plane", "api_" + "hotfix", "historical_" + "controller",
		"controller_" + "m16", "RP" + "5", "release_" + "recovery", "post-" + "renderer",
		"apply_controller_" + "declarative.sh", "apply_telemetry_" + "declarative.sh",
	} {
		if strings.Contains(source+"\n"+action, forbidden) {
			t.Fatalf("CI workflow retained legacy release token %q", forbidden)
		}
	}
}

func yamlMappingValue(t *testing.T, mapping *yaml.Node, key string) *yaml.Node {
	t.Helper()
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		t.Fatalf("YAML value for %q is not a mapping", key)
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	t.Fatalf("YAML mapping has no key %q", key)
	return nil
}

func yamlMappingKeys(t *testing.T, mapping *yaml.Node) []string {
	t.Helper()
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		t.Fatal("YAML value is not a mapping")
	}
	keys := make([]string, 0, len(mapping.Content)/2)
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		keys = append(keys, mapping.Content[index].Value)
	}
	sort.Strings(keys)
	return keys
}
