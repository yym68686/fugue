package declarativerelease

import (
	"os"
	"reflect"
	"regexp"
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
	var checkActionPins func(*yaml.Node)
	checkActionPins = func(node *yaml.Node) {
		if node.Kind == yaml.MappingNode {
			for i := 0; i+1 < len(node.Content); i += 2 {
				if node.Content[i].Value == "uses" {
					value := node.Content[i+1].Value
					if !strings.HasPrefix(value, "./") && !regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$`).MatchString(value) {
						t.Fatalf("external Action must pin a complete commit SHA: %s", value)
					}
				}
			}
		}
		for _, child := range node.Content {
			checkActionPins(child)
		}
	}
	checkActionPins(root)
	on := yamlMappingValue(t, root, "on")
	triggerKeys := yamlMappingKeys(t, on)
	if !reflect.DeepEqual(triggerKeys, []string{"pull_request", "push", "schedule"}) {
		t.Fatalf("CI triggers are not push/PR/audit only: %v", triggerKeys)
	}
	jobs := yamlMappingValue(t, root, "jobs")
	jobKeys := yamlMappingKeys(t, jobs)
	if !reflect.DeepEqual(jobKeys, []string{
		"agent_edge_activation", "agent_edge_shadow_policy", "agent_edge_trust", "audit", "cell_inventory_enrollment", "cell_inventory_plan", "cell_member_enrollment", "cell_producer_plan", "cell_producer_reconfiguration", "cell_producer_shadow", "cell_route_promotion", "cell_route_promotion_plan", "cell_trust", "cell_trust_plan", "cnpg_candidate_artifact", "component-build", "deploy_api", "deploy_controller", "deploy_edge_client", "deploy_edge_control", "deploy_edge_image_gc", "deploy_edge_worker",
		"deploy_image_cache", "deploy_release_guardian", "deploy_runtime_agent", "deploy_schema", "deploy_telemetry", "diagnostic_packages", "diagnostics_configuration", "diagnostics_package_activation", "dns_authority_stage", "dns_authority_stage_plan", "dns_probe_egress", "dns_probe_egress_plan", "dns_transport", "drain_observation_access", "drain_observer_artifact", "external_controller_release", "front_drain_observation", "front_external_observation", "front_observation", "front_probe_transport", "front_public_recovery", "front_public_verification", "front_restricted_egress_observation", "front_serving_handoff", "front_serving_stage", "observability_configuration", "postgres_protection", "prepush", "runtime_agent_identity", "static_edge_observability", "traffic_safety_stage0", "worker_standby_observation", "workload_memory_policy",
	}) {
		t.Fatalf("CI job inventory is not the single component pipeline: %v", jobKeys)
	}
	source := string(raw)
	dnsProbeEgress := yamlMappingValue(t, jobs, "dns_probe_egress")
	dnsProbePlan := yamlMappingValue(t, jobs, "dns_probe_egress_plan")
	if yamlMappingValue(t, dnsProbePlan, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, dnsProbeEgress, "needs").Value != "dns_probe_egress_plan" || !strings.Contains(yamlMappingValue(t, dnsProbeEgress, "if").Value, "needs.dns_probe_egress_plan.outputs.selected == 'true'") || yamlMappingValue(t, dnsProbeEgress, "environment").Value != "production" {
		t.Fatal("DNS probe egress requires independent changed-intent admission and production scope")
	}
	for _, key := range yamlMappingKeys(t, dnsProbePlan) {
		if key == "needs" {
			t.Fatal("DNS network configuration recovery cannot depend on code release jobs")
		}
	}
	dnsProbeRaw, err := yaml.Marshal(dnsProbeEgress)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git fetch origin main", "scripts.test_reconcile_dns_probe_egress", "scripts.reconcile_dns_probe_egress", "--validate-only", "--apply --evidence", "fugue-production-dns-probe-egress", "dns-probe-egress.json"} {
		if !strings.Contains(string(dnsProbeRaw), required) {
			t.Fatal("DNS egress configuration lost validation or retained evidence", required)
		}
	}
	dnsStage := yamlMappingValue(t, jobs, "dns_authority_stage")
	dnsStagePlan := yamlMappingValue(t, jobs, "dns_authority_stage_plan")
	if yamlMappingValue(t, dnsStage, "needs").Value != "dns_authority_stage_plan" || yamlMappingValue(t, dnsStagePlan, "runs-on").Value != "ubuntu-latest" || !strings.Contains(yamlMappingValue(t, dnsStage, "if").Value, "needs.dns_authority_stage_plan.outputs.selected == 'true'") {
		t.Fatal("DNS configuration must independently select one changed declaration without depending on code builds")
	}
	if yamlMappingValue(t, dnsStage, "environment").Value != "production" {
		t.Fatal("DNS staging bypasses production environment")
	}
	dnsStageRaw, err := yaml.Marshal(dnsStage)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy/environments/production/dns-authority-stage", "one DNS staging declaration per atom", "scripts.stage_dns_authority_transition", "fugue-production-dns-authority-configuration", "git fetch origin main", "selected == 'true'", "FUGUE_API_KEY", "dns-authority-stage.json"} {
		if !strings.Contains(string(dnsStageRaw), required) {
			t.Fatal("DNS staging lost explicit input/serialization/evidence gate", required)
		}
	}
	memberEnrollment := yamlMappingValue(t, jobs, "cell_member_enrollment")
	for _, key := range yamlMappingKeys(t, memberEnrollment) {
		if key == "needs" {
			t.Fatal("member inventory configuration must remain independent of code builds")
		}
	}
	memberRaw, err := yaml.Marshal(memberEnrollment)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy/environments/production/cell-member-enrollment", "one Cell member per enrollment atom", "scripts.enroll_cell_member", "selected == 'true'", "fugue-production-cell-member-enrollment", "git fetch origin main"} {
		if !strings.Contains(string(memberRaw), required) {
			t.Fatal("member enrollment lost explicit declaration gate", required)
		}
	}
	if yamlMappingValue(t, memberEnrollment, "environment").Value != "production" {
		t.Fatal("member enrollment bypasses production environment")
	}
	reconfiguration := yamlMappingValue(t, jobs, "cell_producer_reconfiguration")
	producerPlan := yamlMappingValue(t, jobs, "cell_producer_plan")
	if yamlMappingValue(t, producerPlan, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, reconfiguration, "needs").Value != "cell_producer_plan" || !strings.Contains(yamlMappingValue(t, reconfiguration, "if").Value, "needs.cell_producer_plan.outputs.reconfiguration == 'true'") {
		t.Fatal("only changed producer reconfigurations may enter the shared production queue")
	}
	for _, key := range yamlMappingKeys(t, producerPlan) {
		if key == "needs" {
			t.Fatal("existing Cell configuration must remain independent of code builds")
		}
	}
	producerPlanRaw, err := yaml.Marshal(producerPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy/environments/production/cell-producers", "deploy/environments/production/cell-producer-reconfiguration", "deploy/environments/production/cell-membership-expansion", "one Cell producer declaration per configuration atom", "shadow=true", "reconfiguration=true"} {
		if !strings.Contains(string(producerPlanRaw), required) {
			t.Fatal("producer queue admission lost shared one-declaration selection", required)
		}
	}
	reconfigurationRaw, err := yaml.Marshal(reconfiguration)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy/environments/production/cell-producer-reconfiguration", "deploy/environments/production/cell-membership-expansion", "one existing Cell producer per configuration atom", "scripts.reconfigure_cell_producer", "scripts.expand_cell_membership", "selected == 'true'", "fugue-production-cell-producer-shadow"} {
		if !strings.Contains(string(reconfigurationRaw), required) {
			t.Fatalf("existing Cell configuration lacks scoped selection, CAS or serialization: %s", required)
		}
	}
	if yamlMappingValue(t, reconfiguration, "environment").Value != "production" {
		t.Fatal("existing Cell configuration requires production environment")
	}
	cellPromotion := yamlMappingValue(t, jobs, "cell_route_promotion")
	cellPromotionPlan := yamlMappingValue(t, jobs, "cell_route_promotion_plan")
	if yamlMappingValue(t, cellPromotion, "needs").Value != "cell_route_promotion_plan" || yamlMappingValue(t, cellPromotionPlan, "runs-on").Value != "ubuntu-latest" || !strings.Contains(yamlMappingValue(t, cellPromotion, "if").Value, "needs.cell_route_promotion_plan.outputs.selected == 'true'") {
		t.Fatal("private Cell promotion must independently select an explicit changed declaration")
	}
	for _, key := range yamlMappingKeys(t, cellPromotionPlan) {
		if key == "needs" {
			t.Fatal("private Cell publication cannot depend on a code-release job")
		}
	}
	if yamlMappingValue(t, cellPromotion, "environment").Value != "production" || !strings.Contains(source, "python3 -m scripts.promote_cell_routes") {
		t.Fatal("private Cell promotion requires bounded production observation")
	}
	cellInventory := yamlMappingValue(t, jobs, "cell_inventory_enrollment")
	cellInventoryPlan := yamlMappingValue(t, jobs, "cell_inventory_plan")
	if yamlMappingValue(t, cellInventory, "needs").Value != "cell_inventory_plan" || yamlMappingValue(t, cellInventoryPlan, "runs-on").Value != "ubuntu-latest" || !strings.Contains(yamlMappingValue(t, cellInventory, "if").Value, "needs.cell_inventory_plan.outputs.selected == 'true'") {
		t.Fatal("only changed inventory declarations may enter the shared production queue")
	}
	for _, key := range yamlMappingKeys(t, cellInventoryPlan) {
		if key == "needs" {
			t.Fatal("initial cell inventory configuration cannot depend on code-release jobs")
		}
	}
	if yamlMappingValue(t, cellInventory, "environment").Value != "production" || !strings.Contains(source, "python3 -m scripts.bootstrap_cell_inventory") || !strings.Contains(source, "deploy/environments/production/cell-inventory") {
		t.Fatal("initial cell inventory requires explicit production inputs and the bounded verifier")
	}
	cellProducer := yamlMappingValue(t, jobs, "cell_producer_shadow")
	if yamlMappingValue(t, cellProducer, "needs").Value != "cell_producer_plan" || !strings.Contains(yamlMappingValue(t, cellProducer, "if").Value, "needs.cell_producer_plan.outputs.shadow == 'true'") {
		t.Fatal("only changed shadow declarations may enter the shared production queue")
	}
	if yamlMappingValue(t, cellProducer, "environment").Value != "production" || !strings.Contains(source, "python3 -m scripts.bootstrap_cell_producer") || !strings.Contains(source, "deploy/environments/production/cell-producers") {
		t.Fatal("cell shadow enrollment must use explicit production inputs and the bounded publisher")
	}
	cellTrust := yamlMappingValue(t, jobs, "cell_trust")
	cellTrustPlan := yamlMappingValue(t, jobs, "cell_trust_plan")
	if yamlMappingValue(t, cellTrust, "needs").Value != "cell_trust_plan" || yamlMappingValue(t, cellTrustPlan, "runs-on").Value != "ubuntu-latest" || !strings.Contains(yamlMappingValue(t, cellTrust, "if").Value, "needs.cell_trust_plan.outputs.selected == 'true'") {
		t.Fatal("only changed trust declarations may enter the shared production queue")
	}
	for _, key := range yamlMappingKeys(t, cellTrustPlan) {
		if key == "needs" {
			t.Fatal("cell trust configuration recovery cannot depend on a code release")
		}
	}
	if yamlMappingValue(t, cellTrust, "environment").Value != "production" || !strings.Contains(source, "scripts/reconcile_cell_trust.py") || !strings.Contains(source, "secrets[steps.trust.outputs.material_secret]") {
		t.Fatal("cell trust must use the explicit encrypted production declaration")
	}
	cellTrustRaw, err := yaml.Marshal(cellTrust)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"fugue.cell-trust/v2", "'git', 'show'", "digest(previous) != config['previousDigest']", "previous['generation'] != config['previousGeneration']", "--previous", "cell-trust-previous.json", "git diff --quiet HEAD origin/main"} {
		if !strings.Contains(string(cellTrustRaw), required) {
			t.Fatal("reader addition lost its exact Git predecessor or current-main gate", required)
		}
	}
	staticObservation := yamlMappingValue(t, jobs, "static_edge_observability")
	if yamlMappingValue(t, staticObservation, "needs").Value != "prepush" || yamlMappingValue(t, staticObservation, "runs-on").Value != "ubuntu-latest" || !strings.Contains(source, "make test-static-edge-observability") || !strings.Contains(source, "scripts/package_static_edge_observability.sh") {
		t.Fatal("independent static edge artifacts require isolated verification and the normal prepush gate")
	}
	agentActivation := yamlMappingValue(t, jobs, "agent_edge_activation")
	for _, key := range yamlMappingKeys(t, agentActivation) {
		if key == "needs" {
			t.Fatal("Agent policy activation and recovery cannot depend on code-release jobs")
		}
	}
	if yamlMappingValue(t, agentActivation, "environment").Value != "production" || !strings.Contains(source, "Retain activation evidence before publication") || !strings.Contains(source, "Retain selected-traffic verification witness") {
		t.Fatal("Agent activation requires retained evidence before publication and LKG verification")
	}
	if strings.Contains(source, `scripts/test_activate_agent_edge_policy.py cmd/fugue-agent-edge-keyring .github/workflows/ci.yml; then`) {
		t.Fatal("unrelated workflow jobs must not republish or re-attest an unchanged Agent policy")
	}
	agentPolicy := yamlMappingValue(t, jobs, "agent_edge_shadow_policy")
	for _, key := range yamlMappingKeys(t, agentPolicy) {
		if key == "needs" {
			t.Fatal("Agent policy recovery must remain independent from serving code releases")
		}
	}
	if yamlMappingValue(t, agentPolicy, "environment").Value != "production" || !strings.Contains(source, "scripts/publish_agent_edge_shadow.py deploy/environments/production/agent-edge-policy/shadow.json") {
		t.Fatal("Agent observation policy must use its independent declarative publisher")
	}
	agentTrust := yamlMappingValue(t, jobs, "agent_edge_trust")
	for _, key := range yamlMappingKeys(t, agentTrust) {
		if key == "needs" {
			t.Fatal("Agent trust recovery must not depend on a serving component build")
		}
	}
	trustSource, err := yaml.Marshal(agentTrust)
	if err != nil {
		t.Fatal(err)
	}
	if yamlMappingValue(t, agentTrust, "environment").Value != "production" ||
		!strings.Contains(string(trustSource), "secrets.FUGUE_AGENT_EDGE_SIGNING_KEYRING") ||
		!strings.Contains(string(trustSource), "scripts/reconcile_agent_edge_trust.py") ||
		!strings.Contains(string(trustSource), "--check") || strings.Contains(string(trustSource), " generate ") {
		t.Fatal("Agent trust requires explicit encrypted material, independent reconciliation and read-back; no implicit root generation")
	}
	dnsTransport := yamlMappingValue(t, jobs, "dns_transport")
	for _, key := range yamlMappingKeys(t, dnsTransport) {
		if key == "needs" {
			t.Fatal("DNS transport recovery must not depend on rebuilding consumer code")
		}
	}
	if yamlMappingValue(t, dnsTransport, "environment").Value != "production" || !strings.Contains(source, `scripts/reconcile_dns_transport.py "$config" --apply`) {
		t.Fatal("DNS transport must use the production declarative reconciler")
	}
	externalController := yamlMappingValue(t, jobs, "external_controller_release")
	if yamlMappingValue(t, externalController, "environment").Value != "production" || yamlMappingValue(t, externalController, "needs").Value != "prepush" {
		t.Fatal("external controller release must use the production CI gate")
	}
	if !strings.Contains(source, "steps.selected.outputs.apply == 'true'") || !strings.Contains(source, "Persist the controller recovery witness before mutation") {
		t.Fatal("external controller release needs an explicit intent and durable prewrite witness")
	}
	cnpgCandidate := yamlMappingValue(t, jobs, "cnpg_candidate_artifact")
	if yamlMappingValue(t, cnpgCandidate, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, cnpgCandidate, "needs").Value != "prepush" {
		t.Fatal("third-party candidate verification requires prepush and an isolated hosted runner")
	}
	if !strings.Contains(source, "scripts/verify_cnpg_candidate.py \"$IMAGE_REPOSITORY@$digest\" \"$SOURCE_SHA\"") {
		t.Fatal("CNPG candidate must verify retained manager binaries by immutable image identity")
	}
	drainAccess := yamlMappingValue(t, jobs, "drain_observation_access")
	for _, key := range yamlMappingKeys(t, drainAccess) {
		if key == "needs" {
			t.Fatal("drain observation access recovery must not wait for code builds")
		}
	}
	if yamlMappingValue(t, drainAccess, "environment").Value != "production" || !strings.Contains(source, "get pods --subresource=proxy") {
		t.Fatal("drain access requires the production environment and exact subresource validation")
	}
	drainArtifact := yamlMappingValue(t, jobs, "drain_observer_artifact")
	if yamlMappingValue(t, drainArtifact, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, drainArtifact, "needs").Value != "prepush" {
		t.Fatal("drain artifact verification requires prepush and an isolated hosted runner")
	}
	if !strings.Contains(source, "scripts/smoke_drain_agent.py \"$IMAGE_REPOSITORY@$digest\" \"$SOURCE_SHA\"") {
		t.Fatal("drain observer artifact must exercise the exact immutable image")
	}
	memoryPolicy := yamlMappingValue(t, jobs, "workload_memory_policy")
	for _, key := range yamlMappingKeys(t, memoryPolicy) {
		if key == "needs" {
			t.Fatal("memory admission policy recovery must not depend on a code build")
		}
	}
	if yamlMappingValue(t, memoryPolicy, "environment").Value != "production" {
		t.Fatal("memory policy requires the production environment")
	}
	if !strings.Contains(source, "scripts/reconcile_workload_memory.py deploy/environments/production/workload-memory/policy.json --apply") {
		t.Fatal("memory policy must use the versioned configuration reconciler")
	}
	// Database configuration recovery must not depend on building application code.
	protection := yamlMappingValue(t, jobs, "postgres_protection")
	for _, key := range yamlMappingKeys(t, protection) {
		if key == "needs" {
			t.Fatal("database protection is coupled to another release job")
		}
	}
	if yamlMappingValue(t, protection, "environment").Value != "production" {
		t.Fatal("database protection must use the protected production environment")
	}
	observabilityConfig := yamlMappingValue(t, jobs, "observability_configuration")
	for _, key := range yamlMappingKeys(t, observabilityConfig) {
		if key == "needs" {
			t.Fatal("observability configuration must remain recoverable independently of code builds")
		}
	}
	if yamlMappingValue(t, observabilityConfig, "environment").Value != "production" {
		t.Fatal("configuration lane requires production environment")
	}
	diagnosticConfig := yamlMappingValue(t, jobs, "diagnostics_configuration")
	if yamlMappingValue(t, diagnosticConfig, "environment").Value != "production" {
		t.Fatal("diagnostic configuration requires the production environment")
	}
	for _, key := range yamlMappingKeys(t, diagnosticConfig) {
		if key == "needs" {
			t.Fatal("diagnostic configuration recovery must not depend on any code build")
		}
	}
	activation := yamlMappingValue(t, jobs, "diagnostics_package_activation")
	if yamlMappingValue(t, activation, "needs").Value != "diagnostic_packages" || !strings.Contains(yamlMappingValue(t, activation, "if").Value, "runner_image != ''") {
		t.Fatal("diagnostic package activation requires a verified immutable package output")
	}
	if !strings.Contains(source, "scripts/publish_diagnostic_catalog.py deploy/environments/production/diagnostics/catalog.json --runner-image") || !strings.Contains(source, "--tag \"$repository:diagnostic-git-$SOURCE_SHA\"") {
		t.Fatal("diagnostic package publication must use an independent tag and configuration path")
	}
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
		"group: fugue-build-${{ matrix.build_lane }}-${{ matrix.component }}-${{ github.sha }}",
		"environment: production",
		"needs: [prepush, component-build, deploy_schema]",
		"needs: [prepush, component-build, deploy_schema, deploy_release_guardian]",
		"needs: [prepush, component-build, deploy_api]",
		"needs: [prepush, component-build, deploy_api, deploy_edge_control, deploy_release_guardian]",
		"edge_control_matrix",
		"edge_client_matrix",
		"edge_worker_matrix",
		"needs: [prepush, component-build, deploy_controller]",
		"Download Go modules before the bounded test budget",
		"'23 18 * * *'",
		"Run the full asynchronous audit",
		"needs.prepush.outputs.traffic_safety_changed == 'true'",
		"FUGUE_TRAFFIC_SAFETY_STAGE0_CONFIG_JSON",
		"Setup Go for the public DNS verifier",
		"scripts/apply_fugue_traffic_safety.sh --apply",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("CI workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"deploy_edge_client_de", "deploy_edge_client_us", "deploy_edge_control_de", "deploy_edge_control_us", "deploy_edge_worker_de", "deploy_edge_worker_us",
		"monitor-plan", "monitor-production", "emit-monitor-output", "'*/5 * * * *'",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("CI workflow retained fixed group job %q", forbidden)
		}
	}
	cliRaw, err := os.ReadFile("../../cmd/fugue-declarative-release/main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{`"emit-monitor-output"`, `case "monitor":`} {
		if strings.Contains(string(cliRaw), forbidden) {
			t.Fatalf("declarative release CLI retained retired cron writer command %q", forbidden)
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
	if strings.Count(action, "FUGUE_REGISTRY_PASSWORD: ${{ inputs.registry-token }}") != 4 {
		t.Fatal("prepare, execute, reconcile, and committed Guardian adoption must share the masked read-only registry credential")
	}
	for _, required := range []string{
		"\"${RELEASE_TOOL}\" prepare", "\"${RELEASE_TOOL}\" emit-delivery", "\"${RELEASE_TOOL}\" execute", "\"${RELEASE_TOOL}\" guardian-submit", "\"${RELEASE_TOOL}\" reconcile", "\"${RELEASE_TOOL}\" adopt-committed-monitor",
		"Upload the durable prewrite plan before mutation", "Upload terminal component receipt",
		"cache: false",
		"if: steps.delivery.outputs.writer == 'direct' && steps.execute_direct.outcome == 'failure'",
		"if: steps.delivery.outputs.writer == 'guardian' && steps.execute_guardian.outcome == 'failure'",
	} {
		if strings.Count(action, required) != 1 {
			t.Fatalf("declarative component action must contain %q exactly once", required)
		}
	}
	if strings.Count(action, "continue-on-error: true") != 2 || strings.Count(action, "if: steps.delivery.outputs.writer == 'guardian'\n") != 1 {
		t.Fatal("direct and Guardian writers must expose failure to their bounded reconciliation steps")
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

func TestEveryRegisteredComponentHasDeploymentJob(t *testing.T) {
	file, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	registry, err := DecodeRegistry(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			If    string `yaml:"if"`
			Steps []struct {
				Uses string            `yaml:"uses"`
				With map[string]string `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	deployed := map[string]bool{}
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if step.Uses == "./.github/actions/deploy-declarative-component" {
				component := step.With["component"]
				if strings.Contains(job.If, "'"+component+"'") {
					deployed[component] = true
				}
			}
		}
	}
	for _, component := range registry.Components {
		if strings.HasPrefix(component.ID, "edge-worker-") || strings.HasPrefix(component.ID, "edge-control-") || strings.HasPrefix(component.ID, "edge-client-") {
			continue
		} // Existing typed matrices cover these lanes.
		if !deployed[component.ID] {
			t.Fatalf("component %q builds without a selected deployment job", component.ID)
		}
	}
}
