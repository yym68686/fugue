package declarativerelease

import (
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAPIDeploymentHasGuardianRecoveryBudget(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	job := yamlMappingValue(t, jobs, "deploy_api")
	minutes, err := strconv.Atoi(yamlMappingValue(t, job, "timeout-minutes").Value)
	if err != nil || minutes < 45 {
		t.Fatalf("API deployment must leave time for Guardian recovery, worker continuity checks, and receipt upload; got timeout=%d err=%v", minutes, err)
	}
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "deploy-declarative-component") {
		t.Fatal("API release bypasses the standard declarative component workflow")
	}
}

func TestControllerReleaseSynchronizesManagedAppSchemaBeforeRollout(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	job := yamlMappingValue(t, yamlMappingValue(t, document.Content[0], "jobs"), "deploy_controller")
	steps := yamlMappingValue(t, job, "steps")
	syncIndex, rolloutIndex := -1, -1
	for index, step := range steps.Content {
		encoded, err := yaml.Marshal(step)
		if err != nil {
			t.Fatal(err)
		}
		source := string(encoded)
		if strings.Contains(source, "bash scripts/sync_managed_app_crd.sh") {
			syncIndex = index
			for _, required := range []string{"set -euo pipefail", "git diff --exit-code HEAD origin/main", "scripts/validate_managed_app_crd_transition.py", "python3 scripts/test_validate_managed_app_crd_transition.py"} {
				if !strings.Contains(source, required) {
					t.Fatalf("schema sync lost its fail-closed release gate: %s", required)
				}
			}
			if strings.Contains(source, "continue-on-error") || strings.Contains(source, "|| true") {
				t.Fatal("schema synchronization failure must block rollout")
			}
		}
		if strings.Contains(source, "deploy-declarative-component") {
			rolloutIndex = index
		}
	}
	if syncIndex < 0 || rolloutIndex <= syncIndex {
		t.Fatal("controller rollout must follow verified additive schema synchronization")
	}
}

func TestPhysicalBackupExecutorInstallsThroughIndependentDeclarativeLane(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	job := yamlMappingValue(t, yamlMappingValue(t, document.Content[0], "jobs"), "cnpg_backup_count_retention")
	if got := yamlMappingValue(t, job, "needs").Value; got != "production_maintenance_plan" {
		t.Fatalf("backup policy executor must depend only on declaration selection, not a database-backed build: %s", got)
	}
	when := yamlMappingValue(t, job, "if").Value
	if !strings.Contains(when, "github.event_name == 'push' && github.ref == 'refs/heads/main'") ||
		!strings.Contains(when, "needs.production_maintenance_plan.outputs.cnpg_retention == 'true'") {
		t.Fatalf("unrelated pushes or schedule ticks must not reinstall the backup policy executor: %s", when)
	}
	if yamlMappingValue(t, job, "environment").Value != "production" {
		t.Fatal("backup policy executor requires production environment authorization")
	}
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"scripts.test_cnpg_backup_retention", "--install", "git diff --exit-code HEAD origin/main", "control-plane-backup-retention/install.json"} {
		if !strings.Contains(string(encoded), required) {
			t.Fatalf("backup executor lost release gate: %s", required)
		}
	}
}

func TestK3sTransportVerificationIsIsolatedFromProduction(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	job := yamlMappingValue(t, yamlMappingValue(t, document.Content[0], "jobs"), "k3s_transport_integration")
	if yamlMappingValue(t, job, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, job, "needs").Value != "prepush" {
		t.Fatal("upstream verification must use a disposable runner after repository checks")
	}
	for _, key := range yamlMappingKeys(t, job) {
		if key == "environment" || key == "concurrency" {
			t.Fatal("dependency verification must not acquire production environment access or rollout locks")
		}
	}
	permissions := yamlMappingValue(t, job, "permissions")
	if !reflect.DeepEqual(yamlMappingKeys(t, permissions), []string{"contents"}) || yamlMappingValue(t, permissions, "contents").Value != "read" {
		t.Fatal("dependency verification only requires repository read permission")
	}
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for _, required := range []string{"scripts.test_verify_k3s_transport", "verify_k3s_transport.py prepare", "verify_k3s_transport.py verify", "k3s-transport-evidence", "if-no-files-found: error"} {
		if !strings.Contains(source, required) {
			t.Fatalf("missing transport verification gate: %s", required)
		}
	}
	for _, forbidden := range []string{"secrets.", "self-hosted", "systemctl", "kubectl", "--push"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("isolated verification gained production or publication capability: %s", forbidden)
		}
	}
}

func TestFrontObservationSelectsOnlyExplicitChangedProfiles(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	job := yamlMappingValue(t, jobs, "front_observation")
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for _, required := range []string{"git diff --name-only --diff-filter=AM", "front-observation-profiles.txt", "while IFS= read -r config", `[[ "$count" -le 32 ]]`, "git diff --quiet HEAD origin/main"} {
		if !strings.Contains(source, required) {
			t.Fatalf("Front observation lost bounded explicit selection: %s", required)
		}
	}
	if strings.Contains(source, "for config in deploy/environments/production/front-observation/*.json") {
		t.Fatal("unselected historical Front profiles must not block a new observation")
	}
}

func TestFrontHandoffCollectsSameRunWitnessWithoutProbeMutation(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	probe := yamlMappingValue(t, jobs, "front_probe_transport")
	outputs := yamlMappingValue(t, probe, "outputs")
	if !strings.Contains(yamlMappingValue(t, outputs, "observation_target").Value, "steps.transport.outputs.observation_target") {
		t.Fatal("read-only handoff witness must be independent of probe mutation")
	}
	external := yamlMappingValue(t, jobs, "front_external_observation")
	condition := yamlMappingValue(t, external, "if").Value
	if !strings.Contains(condition, "outputs.observation_target != ''") || strings.Contains(condition, "outputs.selected == 'true'") {
		t.Fatal("handoff cannot require a probe mutation to obtain fresh external evidence")
	}
	restricted, err := yaml.Marshal(yamlMappingValue(t, jobs, "front_restricted_egress_observation"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(restricted), `--listener "$PROBE_LISTENER"`) || strings.Contains(string(restricted), "while IFS=") {
		t.Fatal("restricted egress observation must select the exact declared listener")
	}
	drain := yamlMappingValue(t, jobs, "front_drain_observation")
	handoff := yamlMappingValue(t, jobs, "front_serving_handoff")
	handoffOutputs := yamlMappingValue(t, handoff, "outputs")
	if !strings.Contains(yamlMappingValue(t, handoffOutputs, "observation_target").Value, "needs.front_probe_transport.outputs.observation_target") {
		t.Fatal("public acceptance must retain this run's exact ingress witness target")
	}
	verification := yamlMappingValue(t, jobs, "front_public_verification")
	if !strings.Contains(yamlMappingValue(t, yamlMappingValue(t, verification, "env"), "PROBE_LISTENER").Value, "needs.front_serving_handoff.outputs.observation_target") {
		t.Fatal("public verification must select the handed-off ingress only")
	}
	verificationSource, err := yaml.Marshal(verification)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`--listener "$PROBE_LISTENER"`, `test -n "$PROBE_LISTENER"`} {
		if !strings.Contains(string(verificationSource), required) {
			t.Fatalf("public verification lost exact nonempty target: %s", required)
		}
	}
	if !strings.Contains(yamlMappingValue(t, yamlMappingValue(t, drain, "env"), "HANDOFF_CONFIG").Value, "needs.front_serving_handoff.outputs.target") {
		t.Fatal("drain observation requires the exact verified handoff target")
	}
	drainCondition := yamlMappingValue(t, drain, "if").Value
	for _, required := range []string{"needs.front_serving_handoff.outputs.selected == 'true'", "needs.front_public_verification.result == 'success'"} {
		if !strings.Contains(drainCondition, required) {
			t.Fatal("drain observation requires a verified handoff in this run")
		}
	}
	drainSource, err := yaml.Marshal(drain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(drainCondition, "skipped") || strings.Contains(string(drainSource), "for config in") || !strings.Contains(string(drainSource), `scripts/observe_front_drain.py "$HANDOFF_CONFIG"`) {
		t.Fatal("drain observation must not select historical or uncommitted public transports")
	}
}

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
	if !reflect.DeepEqual(workflowFiles, []string{"build-cli.yml", "ci.yml", "daily-audit.yml", "release-cli.yml"}) {
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
	policyQueue := yamlMappingValue(t, yamlMappingValue(t, jobs, "storage_policy"), "concurrency")
	maintenanceQueue := yamlMappingValue(t, yamlMappingValue(t, jobs, "compiler_input_storage"), "concurrency")
	if yamlMappingValue(t, policyQueue, "group").Value == yamlMappingValue(t, maintenanceQueue, "group").Value {
		t.Fatal("empty maintenance jobs must not cancel pending storage policy publication")
	}
	for _, queue := range []*yaml.Node{policyQueue, maintenanceQueue} {
		if yamlMappingValue(t, queue, "queue").Value != "max" || yamlMappingValue(t, queue, "cancel-in-progress").Value != "false" {
			t.Fatal("reviewed storage maintenance and policy operations must queue without cancellation")
		}
	}
	jobKeys := yamlMappingKeys(t, jobs)
	expectedJobs := []string{
		"agent_edge_activation", "agent_edge_shadow_policy", "agent_edge_trust", "app_tcp_transport", "audit", "cell_inventory_enrollment", "cell_inventory_plan", "cell_member_enrollment", "cell_producer_plan", "cell_producer_reconfiguration", "cell_producer_shadow", "cell_route_promotion", "cell_route_promotion_plan", "cell_trust", "cell_trust_plan", "cnpg_backup_count_retention", "cnpg_candidate_artifact", "compiler_input_storage", "component-build", "deploy_api", "deploy_controller", "deploy_edge_client", "deploy_edge_control", "deploy_edge_image_gc", "deploy_edge_worker",
		"deploy_image_cache", "deploy_release_guardian", "deploy_runtime_agent", "deploy_schema", "deploy_ssh_control", "deploy_telemetry", "diagnostic_packages", "diagnostics_configuration", "diagnostics_package_activation", "dns_authority_promotion", "dns_authority_promotion_plan", "dns_authority_stage", "dns_authority_stage_plan", "dns_configuration_plan", "dns_port_retirement", "dns_probe_egress", "dns_probe_egress_plan", "dns_public_observation", "dns_selector_retirement", "dns_selector_retirement_plan", "dns_slot_retirement", "dns_transport", "drain_observation_access", "drain_observer_artifact", "dynamic_quality_plan", "dynamic_quality_reconfiguration", "edge_capacity_external_observation", "edge_member_recovery", "edge_member_recovery_plan", "edge_offline_configuration", "edge_release_hold", "edge_release_hold_plan", "external_controller_release", "fixed_node_retention", "front_drain_observation", "front_external_observation", "front_observation", "front_probe_transport", "front_public_recovery", "front_public_verification", "front_replacement", "front_replacement_external", "front_replacement_plan", "front_replacement_recovery", "front_restricted_egress_observation", "front_serving_handoff", "front_serving_stage", "k3s_transport_integration", "longhorn_gc_build", "longhorn_gc_install", "managed_postgres_cpu_policy", "managed_postgres_cpu_policy_plan", "managed_ssh_artifact", "managed_ssh_front_artifact", "managed_ssh_front_stage", "managed_ssh_identity_maintenance", "managed_ssh_keyrings", "managed_ssh_runtime", "managed_ssh_runtime_plan", "managed_ssh_transport", "node_resource_policy", "node_resource_policy_tasks", "observability_configuration", "physical_dns_plan", "physical_dns_reconfiguration", "postgres_bootstrap_cpu", "postgres_bootstrap_cpu_plan", "postgres_protection", "postgres_volume_recovery", "prepush", "producer_publication_renewal", "runtime_agent_identity", "static_edge_observability", "storage_policy", "tls_issuance_policy", "traffic_safety_stage0", "worker_standby_observation", "workload_memory_policy",
	}
	expectedJobs = append(expectedJobs, "longhorn_cleanup_executor_release", "dns_observation_policy", "automatic_node_participation", "production_maintenance_plan", "edge_origin_relay_trust", "edge_origin_relay_policy")
	expectedJobs = append(expectedJobs, "longhorn_cleanup_observer_release", "deploy_observation_worker", "observation_writer_handoff", "quality_evidence_retention_handoff")
	expectedJobs = append(expectedJobs, "observation_sample_retention_handoff", "app_mail_reconcile_handoff", "app_mail_standby_observation")
	expectedJobs = append(expectedJobs, "deploy_hosted_dns_worker", "hosted_dns_writer_handoff", "deploy_storage_audit_worker", "storage_audit_activation")
	expectedJobs = append(expectedJobs, "node_worker_handoff", "node_worker_handoff_plan", "front_hostport_retirement", "idle_front_standby_release")
	sort.Strings(expectedJobs)
	if !reflect.DeepEqual(jobKeys, expectedJobs) {
		t.Fatalf("CI job inventory is not the single component pipeline: %v", jobKeys)
	}
	observationJobs := []string{"observation_writer_handoff", "quality_evidence_retention_handoff", "observation_sample_retention_handoff"}
	for _, name := range append(observationJobs, "app_mail_reconcile_handoff") {
		job := yamlMappingValue(t, jobs, name)
		for _, required := range []string{"github.event_name == 'push' && github.ref == 'refs/heads/main'", "needs.prepush.result == 'success'", "needs.prepush.outputs." + name + " == 'true'"} {
			if !strings.Contains(yamlMappingValue(t, job, "if").Value, required) {
				t.Fatalf("%s is missing a production admission gate: %s", name, required)
			}
		}
	}
	for _, name := range append(observationJobs, "deploy_observation_worker") {
		queue := yamlMappingValue(t, yamlMappingValue(t, jobs, name), "concurrency")
		if yamlMappingValue(t, queue, "group").Value != "fugue-production-observation-worker" ||
			yamlMappingValue(t, queue, "cancel-in-progress").Value != "false" || yamlMappingValue(t, queue, "queue").Value != "max" {
			t.Fatalf("%s must share the non-canceling observation release queue", name)
		}
	}
	storageAudit := yamlMappingValue(t, jobs, "deploy_storage_audit_worker")
	storageAuditWhen := yamlMappingValue(t, storageAudit, "if").Value
	for _, gate := range []string{
		"github.event_name == 'push' && github.ref == 'refs/heads/main'",
		"needs.prepush.result == 'success'", "needs.prepush.outputs.storage_audit_worker_rehearsed == 'true'",
		"needs.component-build.result == 'success'", "needs.observability_configuration.result == 'success'",
		"needs.storage_policy.result == 'success'", "'storage-audit-worker')",
	} {
		if !strings.Contains(storageAuditWhen, gate) {
			t.Fatalf("storage audit release is missing its admission gate: %s", gate)
		}
	}
	storageActivation := yamlMappingValue(t, jobs, "storage_audit_activation")
	for _, gate := range []string{
		"github.event_name == 'push' && github.ref == 'refs/heads/main'",
		"needs.prepush.result == 'success'", "needs.prepush.outputs.storage_audit_activation == 'true'",
	} {
		if !strings.Contains(yamlMappingValue(t, storageActivation, "if").Value, gate) {
			t.Fatalf("storage audit activation is missing its admission gate: %s", gate)
		}
	}
	for _, job := range []*yaml.Node{storageAudit, storageActivation} {
		queue := yamlMappingValue(t, job, "concurrency")
		if yamlMappingValue(t, queue, "group").Value != "fugue-production-storage-audit-worker" ||
			yamlMappingValue(t, queue, "cancel-in-progress").Value != "false" || yamlMappingValue(t, queue, "queue").Value != "max" {
			t.Fatal("storage audit deployment and activation must share a non-canceling queue")
		}
	}
	nodeWorker := yamlMappingValue(t, jobs, "node_worker_handoff")
	nodeWorkerWhen := yamlMappingValue(t, nodeWorker, "if").Value
	for _, required := range []string{"github.event_name == 'push' && github.ref == 'refs/heads/main'", "needs.prepush.result == 'success'", "needs.node_worker_handoff_plan.outputs.config != ''"} {
		if !strings.Contains(nodeWorkerWhen, required) {
			t.Fatalf("node Worker publication is missing its main, test or exact declaration gate: %s", required)
		}
	}
	for _, name := range []string{"node_worker_handoff", "front_replacement", "front_replacement_recovery", "front_serving_handoff", "front_public_recovery", "front_hostport_retirement"} {
		queue := yamlMappingValue(t, yamlMappingValue(t, jobs, name), "concurrency")
		if yamlMappingValue(t, queue, "group").Value != "fugue-production-front-executor-replacement" ||
			yamlMappingValue(t, queue, "cancel-in-progress").Value != "false" || yamlMappingValue(t, queue, "queue").Value != "max" {
			t.Fatalf("%s must share the non-canceling Front writer queue", name)
		}
	}
	relayTrust := yamlMappingValue(t, jobs, "edge_origin_relay_trust")
	relayWhen := yamlMappingValue(t, relayTrust, "if").Value
	relayQueue := yamlMappingValue(t, relayTrust, "concurrency")
	if yamlMappingValue(t, relayTrust, "needs").Value != "prepush" ||
		yamlMappingValue(t, relayTrust, "environment").Value != "production-edge-origin-relay" ||
		!strings.Contains(relayWhen, "github.event_name == 'push' && github.ref == 'refs/heads/main'") ||
		!strings.Contains(relayWhen, "needs.prepush.outputs.edge_origin_relay_trust == 'true'") ||
		yamlMappingValue(t, relayQueue, "group").Value != "fugue-production-edge-origin-relay-trust" ||
		yamlMappingValue(t, relayQueue, "cancel-in-progress").Value != "false" || yamlMappingValue(t, relayQueue, "queue").Value != "max" {
		t.Fatal("origin relay trust admission lost its main-only scoped environment, exact intent or queue")
	}
	relaySource, err := yaml.Marshal(relayTrust)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git diff --quiet HEAD origin/main", "scripts.stage_edge_origin_relay_trust", "secrets.FUGUE_ORIGIN_RELAY_LEAF_PACKAGES", "edge-origin-relay-trust-receipt.json"} {
		if !strings.Contains(string(relaySource), required) {
			t.Fatalf("origin relay trust lane is missing declaration, leaf-only input or evidence: %s", required)
		}
	}
	relayPolicy := yamlMappingValue(t, jobs, "edge_origin_relay_policy")
	policyWhen := yamlMappingValue(t, relayPolicy, "if").Value
	policyConcurrency := yamlMappingValue(t, relayPolicy, "concurrency")
	if yamlMappingValue(t, relayPolicy, "needs").Value != "prepush" ||
		yamlMappingValue(t, relayPolicy, "environment").Value != "production-edge-origin-relay" ||
		!strings.Contains(policyWhen, "github.event_name == 'push' && github.ref == 'refs/heads/main'") ||
		!strings.Contains(policyWhen, "needs.prepush.outputs.edge_origin_relay_policy == 'true'") ||
		yamlMappingValue(t, policyConcurrency, "group").Value != yamlMappingValue(t, relayQueue, "group").Value ||
		yamlMappingValue(t, policyConcurrency, "cancel-in-progress").Value != "false" ||
		yamlMappingValue(t, policyConcurrency, "queue").Value != "max" {
		t.Fatal("relay lease publication must obey the tested main declaration and serialize with trust staging")
	}
	relayPolicySource, err := yaml.Marshal(relayPolicy)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git diff --quiet HEAD origin/main", "scripts.release_edge_origin_relay_policy", "secrets.FUGUE_ORIGIN_RELAY_SIGNED_LEASE", "edge-origin-relay-policy-receipt.json"} {
		if !strings.Contains(string(relayPolicySource), required) {
			t.Fatalf("relay policy lane is missing exact declaration, signed envelope or evidence: %s", required)
		}
	}
	if strings.Contains(string(relayPolicySource), "SIGNING_KEY") || strings.Contains(string(relayPolicySource), "LEAF_PACKAGES") {
		t.Fatal("relay policy publication must not receive renewable private material")
	}
	cleanup := yamlMappingValue(t, jobs, "longhorn_cleanup_executor_release")
	cleanupWhen := yamlMappingValue(t, cleanup, "if").Value
	if !strings.Contains(cleanupWhen, "github.event_name == 'push' && github.ref == 'refs/heads/main'") ||
		!strings.Contains(cleanupWhen, "needs.prepush.result == 'success'") ||
		!strings.Contains(cleanupWhen, "needs.production_maintenance_plan.outputs.cleanup_executor == 'true'") ||
		strings.Contains(cleanupWhen, "schedule") {
		t.Fatal("online cleanup executor publication requires tested main release intent")
	}
	cleanupQueue := yamlMappingValue(t, cleanup, "concurrency")
	if yamlMappingValue(t, cleanupQueue, "group").Value != "fugue-production-postgres-protection" ||
		yamlMappingValue(t, cleanupQueue, "cancel-in-progress").Value != "false" ||
		yamlMappingValue(t, cleanupQueue, "queue").Value != "max" {
		t.Fatal("executor publication must queue with database recovery without cancellation")
	}
	cleanupYAML, err := yaml.Marshal(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git diff --quiet HEAD origin/main", "scripts.install_longhorn_snapshot_cleanup", "tests/snapshot-native-fold", "if: always()", "longhorn-cleanup-executor.json"} {
		if !strings.Contains(string(cleanupYAML), required) {
			t.Fatalf("cleanup lane lost its release, freshness or evidence gate: %s", required)
		}
	}
	if strings.Contains(string(cleanupYAML), "--apply") || strings.Contains(string(cleanupYAML), "scripts.reconcile_longhorn_snapshot_cleanup") {
		t.Fatal("the publication job must leave native reclamation to the online service")
	}
	externalObservation := yamlMappingValue(t, jobs, "edge_capacity_external_observation")
	if got := yamlMappingValue(t, externalObservation, "if").Value; got != "${{ github.event_name == 'schedule' }}" {
		t.Fatalf("external edge observation must remain scheduled: %s", got)
	}
	for _, key := range yamlMappingKeys(t, externalObservation) {
		if key == "needs" {
			t.Fatal("external edge observation must be independently scheduled")
		}
	}
	observerRelease := yamlMappingValue(t, jobs, "longhorn_cleanup_observer_release")
	if got := yamlMappingValue(t, observerRelease, "needs").Content; len(got) != 2 || got[0].Value != "prepush" || got[1].Value != "production_maintenance_plan" {
		t.Fatal("the snapshot observer must depend on tested, explicitly selected intent")
	}
	observerWhen := yamlMappingValue(t, observerRelease, "if").Value
	if !strings.Contains(observerWhen, "github.event_name == 'push' && github.ref == 'refs/heads/main'") ||
		!strings.Contains(observerWhen, "needs.production_maintenance_plan.outputs.cleanup_observer == 'true'") {
		t.Fatal("the in-cluster snapshot observer must be installed only by tested main releases")
	}
	observerYAML, err := yaml.Marshal(observerRelease)
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range []string{"git diff --exit-code HEAD origin/main", "--dry-run=server", "create pods --subresource=exec", "delete snapshots.longhorn.io", "authorizes_deletion == false", "metadata.creationTimestamp > $published", "spec.timeZone", ".phase == \"blocked\""} {
		if !strings.Contains(string(observerYAML), gate) {
			t.Fatalf("snapshot observer release lacks the observation-only gate %s", gate)
		}
	}
	for _, name := range []string{"automatic_node_participation", "postgres_volume_recovery", "postgres_protection"} {
		if yamlMappingValue(t, yamlMappingValue(t, jobs, name), "needs").Value != "production_maintenance_plan" {
			t.Fatalf("%s must follow declaration selection rather than the release lane", name)
		}
	}
	recovery := yamlMappingValue(t, jobs, "postgres_volume_recovery")
	recoverySource, err := yaml.Marshal(recovery)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(recoverySource), `"$REQUEST/review.json" "$REQUEST/approval.json" --apply`) ||
		strings.Contains(string(recoverySource), "local-snapshot-review/approval.json") {
		t.Fatal("snapshot apply must consume only the newly selected approval")
	}
	dnsWhen := yamlMappingValue(t, yamlMappingValue(t, jobs, "dns_observation_policy"), "if").Value
	if !strings.Contains(dnsWhen, "needs.dns_configuration_plan.outputs.public_observation == 'true'") {
		t.Fatal("DNS public verification must not run on unrelated pushes")
	}
	dnsPromotion := yamlMappingValue(t, jobs, "dns_authority_promotion")
	releaseHold := yamlMappingValue(t, jobs, "edge_release_hold")
	if yamlMappingValue(t, yamlMappingValue(t, jobs, "dynamic_quality_reconfiguration"), "needs").Value != "dynamic_quality_plan" {
		t.Fatal("DNS availability configuration must not depend on code compilation or component deployment")
	}
	if yamlMappingValue(t, releaseHold, "needs").Value != "edge_release_hold_plan" ||
		yamlMappingValue(t, yamlMappingValue(t, releaseHold, "concurrency"), "group").Value != "${{ needs.edge_release_hold_plan.outputs.concurrency }}" ||
		yamlMappingValue(t, yamlMappingValue(t, releaseHold, "concurrency"), "cancel-in-progress").Value != "false" {
		t.Fatal("release hold configuration must remain independent of builds and share the affected component fence")
	}
	if got := yamlMappingValue(t, dnsPromotion, "needs").Value; got != "dns_authority_promotion_plan" {
		t.Fatalf("private DNS configuration must not depend on a code rollout: %s", got)
	}
	if got := yamlMappingValue(t, yamlMappingValue(t, dnsPromotion, "concurrency"), "group").Value; got != "fugue-production-dns-authority-configuration" {
		t.Fatal("private DNS promotion must serialize with its staging authority")
	}
	if !strings.Contains(string(raw), `scripts/build_dns_observation_verifier.sh "$revision" "$RUNNER_TEMP/private-dns-verifier"`) {
		t.Fatal("private DNS recovery must select an explicit compatible verifier revision")
	}
	nodePolicy := yamlMappingValue(t, jobs, "node_resource_policy")
	if got := yamlMappingValue(t, nodePolicy, "needs").Value; got != "production_maintenance_plan" {
		t.Fatalf("local node capacity policy publication must remain independent of code builds: %s", got)
	}
	localNodePolicyWhen := yamlMappingValue(t, nodePolicy, "if").Value
	if strings.Contains(localNodePolicyWhen, "'schedule'") ||
		!strings.Contains(localNodePolicyWhen, "needs.production_maintenance_plan.outputs.local_node_resources == 'true'") {
		t.Fatalf("the control-plane host timer must reconcile independently of Actions: %s", localNodePolicyWhen)
	}
	remoteNodePolicyWhen := yamlMappingValue(t, yamlMappingValue(t, jobs, "node_resource_policy_tasks"), "if").Value
	if !strings.Contains(remoteNodePolicyWhen, "github.event_name == 'schedule'") {
		t.Fatal("preserve remote policy recovery until updater rehydration is verified")
	}
	cpuPolicy := yamlMappingValue(t, jobs, "managed_postgres_cpu_policy")
	// Both lanes mutate the controller but are independent release intents.
	// GitHub's default single pending slot lets a no-op configuration job
	// cancel an already-built executable release before it gets a runner.
	for _, name := range []string{"managed_postgres_cpu_policy", "deploy_controller", "deploy_api", "app_mail_reconcile_handoff"} {
		concurrency := yamlMappingValue(t, yamlMappingValue(t, jobs, name), "concurrency")
		if yamlMappingValue(t, concurrency, "group").Value != "fugue-production-controller" ||
			yamlMappingValue(t, concurrency, "cancel-in-progress").Value != "false" ||
			yamlMappingValue(t, concurrency, "queue").Value != "max" {
			t.Fatal("controller configuration and executable lanes must serialize without replacing pending intents")
		}
	}
	if yamlMappingValue(t, cpuPolicy, "needs").Value != "managed_postgres_cpu_policy_plan" ||
		yamlMappingValue(t, cpuPolicy, "if").Value != "${{ needs.managed_postgres_cpu_policy_plan.outputs.changed == 'true' }}" {
		t.Fatal("CPU policy must acquire production serialization only for changed configuration")
	}
	for _, key := range yamlMappingKeys(t, yamlMappingValue(t, jobs, "managed_postgres_cpu_policy_plan")) {
		if key == "needs" || key == "concurrency" {
			t.Fatal("CPU intent selection must remain independent of builds and production serialization")
		}
	}
	source := string(raw)
	offlineConfiguration := yamlMappingValue(t, jobs, "edge_offline_configuration")
	for _, key := range yamlMappingKeys(t, offlineConfiguration) {
		if key == "needs" {
			t.Fatal("offline intent must remain independently recoverable from executable release")
		}
	}
	if yamlMappingValue(t, offlineConfiguration, "environment").Value != "production" {
		t.Fatal("offline intent bypasses production environment")
	}
	sshMaintenance := yamlMappingValue(t, jobs, "managed_ssh_identity_maintenance")
	for _, key := range yamlMappingKeys(t, sshMaintenance) {
		if key == "needs" {
			t.Fatal("SSH identity maintenance must remain independent of executable deployment")
		}
	}
	if yamlMappingValue(t, sshMaintenance, "environment").Value != "production" {
		t.Fatal("SSH identity maintenance bypasses production environment")
	}
	sshTransport := yamlMappingValue(t, jobs, "managed_ssh_transport")
	for _, key := range yamlMappingKeys(t, sshTransport) {
		if key == "needs" {
			t.Fatal("SSH transport recovery must be independent of executable rollout")
		}
	}
	if yamlMappingValue(t, sshTransport, "environment").Value != "production" {
		t.Fatal("SSH transport bypasses production environment")
	}
	if yamlMappingValue(t, sshTransport, "timeout-minutes").Value != "40" {
		t.Fatal("SSH transport budget must accommodate eight sequential bounded observation windows and compensation")
	}
	sshTransportRaw, err := yaml.Marshal(sshTransport)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"transport-operation.json", "git fetch origin main", "git diff --quiet HEAD origin/main", "scripts.run_managed_ssh_transport", "scripts.test_select_managed_ssh_transport", "scripts.test_observe_managed_ssh_front", "FUGUE_MANAGED_SSH_CANARY_KEY", "cmd/fugue-ssh-probe"} {
		if !strings.Contains(string(sshTransportRaw), required) {
			t.Fatal("SSH transport lost independently declared verification", required)
		}
	}
	sshFrontStage := yamlMappingValue(t, jobs, "managed_ssh_front_stage")
	for _, key := range yamlMappingKeys(t, sshFrontStage) {
		if key == "needs" {
			t.Fatal("SSH front staging must not depend on executable rollout")
		}
	}
	if yamlMappingValue(t, sshFrontStage, "environment").Value != "production" {
		t.Fatal("SSH front staging bypasses production environment")
	}
	sshFrontStageRaw, err := yaml.Marshal(sshFrontStage)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"front-stage.json", "git fetch origin main", "git diff --quiet HEAD origin/main", "scripts.stage_managed_ssh_fronts", "--verify-artifact-run", "actions/download-artifact@", "scripts.test_stage_managed_ssh_fronts", "--validate-only"} {
		if !strings.Contains(string(sshFrontStageRaw), required) {
			t.Fatal("SSH front staging lost independent intent or artifact gates", required)
		}
	}
	fixedNodeRetention := yamlMappingValue(t, jobs, "fixed_node_retention")
	fixedNodeRetentionRaw, err := yaml.Marshal(fixedNodeRetention)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"scripts/reconcile_fixed_node_retention.py", "scripts.test_reconcile_fixed_node_retention", "deploy/environments/production/fixed-node-retention", "--apply"} {
		if !strings.Contains(string(fixedNodeRetentionRaw), required) {
			t.Fatal("fixed-node retention changes must rerun the production reconciler", required)
		}
	}
	sshFrontArtifact := yamlMappingValue(t, jobs, "managed_ssh_front_artifact")
	if yamlMappingValue(t, sshFrontArtifact, "needs").Value != "prepush" || yamlMappingValue(t, sshFrontArtifact, "runs-on").Value != "ubuntu-latest" {
		t.Fatal("SSH front artifact must be verified away from production")
	}
	sshFrontArtifactRaw, err := yaml.Marshal(sshFrontArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Dockerfile.ssh-front", "scripts.verify_ssh_front_image", "scripts.test_verify_ssh_front_image", "--metadata-file", "org.opencontainers.image.revision", "fugue-ssh-front-artifact-"} {
		if !strings.Contains(string(sshFrontArtifactRaw), required) {
			t.Fatal("SSH front artifact lost its provenance or acceptance gate", required)
		}
	}
	for _, forbidden := range []string{"KUBECONFIG", "kubectl apply", "self-hosted", "deploy-declarative-component"} {
		if strings.Contains(string(sshFrontArtifactRaw), forbidden) {
			t.Fatal("SSH front build changed production ingress", forbidden)
		}
	}
	sshRuntime := yamlMappingValue(t, jobs, "managed_ssh_runtime")
	sshRuntimePlan := yamlMappingValue(t, jobs, "managed_ssh_runtime_plan")
	if yamlMappingValue(t, sshRuntime, "needs").Value != "managed_ssh_runtime_plan" || !strings.Contains(yamlMappingValue(t, sshRuntime, "if").Value, "needs.managed_ssh_runtime_plan.outputs.changed == 'true'") {
		t.Fatal("SSH runtime must select configuration before acquiring its concurrency fence")
	}
	if yamlMappingValue(t, sshRuntimePlan, "runs-on").Value != "ubuntu-latest" {
		t.Fatal("SSH runtime selection must not occupy the production runner")
	}
	for _, key := range yamlMappingKeys(t, sshRuntimePlan) {
		if key == "needs" || key == "concurrency" || key == "environment" {
			t.Fatal("SSH runtime selection must remain independent of code builds and writer fences")
		}
	}
	if yamlMappingValue(t, sshRuntime, "environment").Value != "production" {
		t.Fatal("SSH runtime configuration bypasses production environment")
	}
	sshRuntimeRaw, err := yaml.Marshal(sshRuntime)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git fetch origin main", "git diff --quiet HEAD origin/main", "artifactSourceCommit", "artifactRunID", "actions/download-artifact@", "--verify-artifact-run", "scripts.test_reconcile_managed_ssh_runtime", "--validate-only", "fugue-production-managed-ssh-runtime", "managed-ssh-receipt.json"} {
		if !strings.Contains(string(sshRuntimeRaw), required) {
			t.Fatal("SSH runtime publication lost its verified artifact gate", required)
		}
	}
	storagePolicy := yamlMappingValue(t, jobs, "storage_policy")
	for _, key := range yamlMappingKeys(t, storagePolicy) {
		if key == "needs" {
			t.Fatal("storage format/budget recovery must not depend on code release")
		}
	}
	if yamlMappingValue(t, storagePolicy, "environment").Value != "production" {
		t.Fatal("storage policy bypasses production environment")
	}
	sshArtifact := yamlMappingValue(t, jobs, "managed_ssh_artifact")
	if yamlMappingValue(t, sshArtifact, "needs").Value != "prepush" || yamlMappingValue(t, sshArtifact, "runs-on").Value != "ubuntu-latest" {
		t.Fatal("managed SSH artifact must be built and tested off production")
	}
	sshArtifactRaw, err := yaml.Marshal(sshArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Dockerfile.managed-ssh", "scripts.verify_managed_ssh_image", "org.opencontainers.image.revision", "managed-ssh-receipt.json", "--platform linux/amd64", "--push"} {
		if !strings.Contains(string(sshArtifactRaw), required) {
			t.Fatal("managed SSH image lost immutable build or real runtime verification", required)
		}
	}
	sshControl := yamlMappingValue(t, jobs, "deploy_ssh_control")
	sshControlRaw, err := yaml.Marshal(sshControl)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy_schema", "fugue-production-ssh-control", "./.github/actions/deploy-declarative-component", "component: ssh-control"} {
		if !strings.Contains(string(sshControlRaw), required) {
			t.Fatal("managed SSH controller bypasses normal component release", required)
		}
	}
	sshKeys := yamlMappingValue(t, jobs, "managed_ssh_keyrings")
	for _, key := range yamlMappingKeys(t, sshKeys) {
		if key == "needs" {
			t.Fatal("SSH keyring recovery must not depend on code releases")
		}
	}
	if yamlMappingValue(t, sshKeys, "environment").Value != "production" {
		t.Fatal("SSH keyring publication requires the production environment")
	}
	sshKeysRaw, err := yaml.Marshal(sshKeys)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"deploy/environments/production/managed-ssh/keys.json", "git fetch origin main", "git diff --quiet HEAD origin/main", "scripts.test_reconcile_managed_ssh_keys", "--validate-only", "fugue-production-managed-ssh-keyrings", "selected == 'true'", "secrets.FUGUE_MANAGED_SSH_KEYRINGS_JSON"} {
		if !strings.Contains(string(sshKeysRaw), required) {
			t.Fatal("SSH keyring recovery lost its explicit configuration gate", required)
		}
	}
	if strings.Contains(source, `git diff --quiet "$base" HEAD -- "$config" scripts/publish_agent_edge_shadow.py scripts/test_publish_agent_edge_shadow.py; then`) {
		t.Fatal("shared API client code must not replay an initial Agent shadow configuration")
	}
	renewal := yamlMappingValue(t, jobs, "producer_publication_renewal")
	if yamlMappingValue(t, renewal, "needs").Value != "cell_producer_plan" || !strings.Contains(yamlMappingValue(t, renewal, "if").Value, "needs.cell_producer_plan.outputs.renewal == 'true'") {
		t.Fatal("producer renewal must select changed configuration before entering the shared queue")
	}
	renewalSelectionPlan := yamlMappingValue(t, jobs, "cell_producer_plan")
	for _, key := range yamlMappingKeys(t, renewalSelectionPlan) {
		if key == "needs" || key == "environment" || key == "concurrency" {
			t.Fatal("producer configuration selection cannot depend on builds or acquire production locks")
		}
	}
	if yamlMappingValue(t, renewalSelectionPlan, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, yamlMappingValue(t, renewalSelectionPlan, "permissions"), "contents").Value != "read" {
		t.Fatal("producer configuration selection requires only a disposable read-only runner")
	}
	producerSelectionRaw, err := yaml.Marshal(renewalSelectionPlan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(producerSelectionRaw), "secrets.") || !strings.Contains(string(producerSelectionRaw), "producer-renewal/renewal.json") || !strings.Contains(string(producerSelectionRaw), "renewal=true") {
		t.Fatal("producer renewal selection lost its read-only declaration boundary")
	}
	renewalRaw, err := yaml.Marshal(renewal)
	if err != nil {
		t.Fatal(err)
	}
	if yamlMappingValue(t, renewal, "environment").Value != "production" {
		t.Fatal("producer recovery bypasses production environment")
	}
	for _, required := range []string{"deploy/environments/production/producer-renewal/renewal.json", "git fetch origin main", "scripts.test_renew_producer_publication", "scripts.renew_producer_publication", "fugue-production-cell-producer-shadow", "--evidence", "selected == 'true'"} {
		if !strings.Contains(string(renewalRaw), required) {
			t.Fatal("producer recovery lost declaration gate", required)
		}
	}
	retirement := yamlMappingValue(t, jobs, "dns_selector_retirement")
	if yamlMappingValue(t, retirement, "needs").Value != "dns_selector_retirement_plan" || yamlMappingValue(t, retirement, "environment").Value != "production" {
		t.Fatal("retirement configuration must remain independent of code release")
	}
	retirementRaw, err := yaml.Marshal(retirement)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"git fetch origin main", "scripts.test_retire_dns_selector", "scripts.retire_dns_selector", "fugue-production-cell-producer-shadow", "--evidence", "selected == 'true'"} {
		if !strings.Contains(string(retirementRaw), required) {
			t.Fatal("retirement lost transactional declaration gate", required)
		}
	}
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
		if key == "needs" && yamlMappingValue(t, dnsTransport, key).Value != "dns_configuration_plan" {
			t.Fatal("DNS transport recovery must not depend on rebuilding consumer code")
		}
	}
	if yamlMappingValue(t, dnsTransport, "environment").Value != "production" || !strings.Contains(source, `scripts.reconcile_dns_transport "$config" --apply`) {
		t.Fatal("DNS transport must use the production declarative reconciler")
	}
	frontObservation := yamlMappingValue(t, jobs, "front_external_observation")
	if !strings.Contains(yamlMappingValue(t, frontObservation, "if").Value, "needs.front_probe_transport.outputs.observation_target != ''") {
		t.Fatal("external probe verification must bind to the exact declared transition witness")
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
		"group: fugue-production-controller",
		"group: '${{ matrix.concurrency }}'",
		"group: fugue-production-image-cache",
		"group: fugue-production-release-guardian",
		"group: fugue-production-schema",
		"group: fugue-production-telemetry",
		"group: fugue-build-${{ matrix.build_lane }}-${{ matrix.component }}-${{ github.sha }}",
		"environment: production",
		"needs: [prepush, component-build, deploy_schema]",
		"needs: [prepush, component-build, deploy_schema, deploy_release_guardian, deploy_controller]",
		"needs: [prepush, component-build, deploy_api, deploy_release_guardian]",
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
		"monitor-plan", "monitor-production", "emit-monitor-output",
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
		"if: steps.delivery.outputs.writer == 'guardian' && steps.execute_guardian.outcome == 'failure' && steps.execute_guardian.outputs.worker_drain_wait != 'true'",
		"if: steps.execute_guardian.outputs.worker_drain_wait == 'true'",
		".workerDrainPlanDigest == $plan[0].planDigest",
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

func TestCapacityRecoveryScheduleIsExternalObservationOnly(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	observer := yamlMappingValue(t, jobs, "edge_capacity_external_observation")
	if yamlMappingValue(t, observer, "runs-on").Value != "ubuntu-latest" || yamlMappingValue(t, observer, "timeout-minutes").Value != "3" {
		t.Fatal("capacity recovery must run on a bounded external runner")
	}
	queue := yamlMappingValue(t, observer, "concurrency")
	if yamlMappingValue(t, queue, "cancel-in-progress").Value != "false" {
		t.Fatal("observation must not be canceled between claim and report")
	}
	encoded, _ := yaml.Marshal(observer)
	for _, forbidden := range []string{"kubectl", "--apply", "deploy-declarative-component", "BUNDLE_SIGNING"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("external observation cannot release code or access signing keys: %s", forbidden)
		}
	}
	for _, name := range []string{"dns_public_observation", "node_resource_policy_tasks", "workload_memory_policy"} {
		if !strings.Contains(yamlMappingValue(t, yamlMappingValue(t, jobs, name), "if").Value, "github.event.schedule == '23 18 * * *'") {
			t.Fatalf("frequent observation must not reschedule daily work: %s", name)
		}
	}
	audit := yamlMappingValue(t, jobs, "audit")
	if got := yamlMappingValue(t, audit, "if").Value; got != "github.event_name == 'push' && github.ref == 'refs/heads/main'" {
		t.Fatalf("production workflow must not run the daily audit: %s", got)
	}
}

func TestDailyAuditHasNoProductionAccess(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/daily-audit.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	root := document.Content[0]
	on := yamlMappingValue(t, root, "on")
	if got := yamlMappingKeys(t, on); !reflect.DeepEqual(got, []string{"schedule", "workflow_dispatch"}) {
		t.Fatalf("daily audit triggers: %v", got)
	}
	choices := yamlMappingValue(t, yamlMappingValue(t, on, "workflow_dispatch"), "inputs")
	shadowCases := yamlMappingValue(t, choices, "shadow_case")
	options := yamlMappingValue(t, shadowCases, "options")
	gotCases := make([]string, 0, len(options.Content))
	for _, choice := range options.Content {
		gotCases = append(gotCases, choice.Value)
	}
	wantCases := []string{
		"none", "queue-runtime", "queue-test", "host-policy-test", "backup-runtime", "retirement-test", "builder-runtime", "builder-test",
		"oom-runtime", "oom-test", "cross-domain",
		"contract-backup-checkpoint", "contract-builder-checkpoint", "contract-oom-admission", "contract-tracking-scope",
		"contract-generator-consumer", "contract-retirement-scope",
	}
	if !reflect.DeepEqual(gotCases, wantCases) {
		t.Fatalf("daily audit shadow cases: %v", gotCases)
	}
	incremental := yamlMappingValue(t, choices, "shadow_incremental")
	if yamlMappingValue(t, incremental, "type").Value != "boolean" ||
		yamlMappingValue(t, incremental, "default").Value != "false" {
		t.Fatal("incremental replay must require explicit manual selection")
	}
	schedule := yamlMappingValue(t, on, "schedule").Content
	if len(schedule) != 1 || yamlMappingValue(t, schedule[0], "cron").Value != "23 18 * * *" {
		t.Fatal("daily audit cadence changed")
	}
	if got := yamlMappingKeys(t, yamlMappingValue(t, root, "permissions")); !reflect.DeepEqual(got, []string{"contents"}) ||
		yamlMappingValue(t, yamlMappingValue(t, root, "permissions"), "contents").Value != "read" {
		t.Fatal("daily audit should have only repository read permission")
	}
	jobs := yamlMappingValue(t, root, "jobs")
	if got := yamlMappingKeys(t, jobs); !reflect.DeepEqual(got, []string{"shadow-replay", "test"}) {
		t.Fatalf("daily audit jobs: %v", got)
	}
	test := yamlMappingValue(t, jobs, "test")
	if got := yamlMappingKeys(t, test); !reflect.DeepEqual(got, []string{"runs-on", "steps", "timeout-minutes"}) ||
		yamlMappingValue(t, test, "runs-on").Value != "ubuntu-latest" {
		t.Fatal("daily audit must use only a bounded, hosted test job")
	}
	shadow := yamlMappingValue(t, jobs, "shadow-replay")
	if got := yamlMappingKeys(t, shadow); !reflect.DeepEqual(got, []string{"env", "if", "runs-on", "steps", "timeout-minutes"}) ||
		yamlMappingValue(t, shadow, "runs-on").Value != "ubuntu-latest" ||
		yamlMappingValue(t, shadow, "if").Value != "github.event_name == 'workflow_dispatch' && inputs.shadow_case != 'none'" ||
		yamlMappingValue(t, shadow, "timeout-minutes").Value != "120" {
		t.Fatal("shadow audits must be manual, bounded, hosted and inherit repository-only read access")
	}
	for _, job := range []*yaml.Node{test, shadow} {
		steps := yamlMappingValue(t, job, "steps")
		if len(steps.Content) == 0 ||
			yamlMappingValue(t, steps.Content[0], "uses").Value != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
			yamlMappingValue(t, yamlMappingValue(t, steps.Content[0], "with"), "persist-credentials").Value != "false" {
			t.Fatal("daily audit jobs must check out without persisting credentials")
		}
	}
	for _, s := range []string{"secrets.", "environment:", "self-hosted", "kubectl", "uses: ./", "deploy-declarative-component", "github.token", "id-token:"} {
		if strings.Contains(string(raw), s) {
			t.Fatalf("daily audit retained production access: %s", s)
		}
	}
	if !strings.Contains(string(raw), "run: make test") || !strings.Contains(string(raw), "persist-credentials: false") {
		t.Fatal("daily audit must run the full tests without persisting credentials")
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

// The controller delivery lane builds the release command and uses the same
// TLS route probe client as DNS. Changes to either dependency must be owned by
// the selected controller intent without selecting unrelated components.
func TestControllerPlanIncludesDeliveryAndProbeDependencies(t *testing.T) {
	file, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	registry, err := DecodeRegistry(file)
	if err != nil {
		t.Fatal(err)
	}
	changed := []string{"cmd/fugue-declarative-release/main.go", "cmd/fugue-declarative-release/postgres_cpu_policy.go", "internal/controller/safe_rollout_observers.go", "internal/routeprobe/probe.go", "deploy/releases/controller/intent.json"}
	plan, err := BuildPlan(registry, strings.Repeat("a", 40), strings.Repeat("b", 40), changed)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != "controller" {
		t.Fatalf("unexpected release selection: %#v", plan.Releases)
	}
	for _, path := range changed[:len(changed)-1] {
		found := false
		for _, selected := range plan.Releases[0].ChangedPaths {
			if selected == path {
				found = true
			}
		}
		if !found {
			t.Fatalf("controller plan omits dependency %s", path)
		}
	}
}

func TestDNSSlotRetirementIsIndependentAndSharesTransportFence(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	jobs := yamlMappingValue(t, document.Content[0], "jobs")
	job := yamlMappingValue(t, jobs, "dns_slot_retirement")
	dependencies := yamlMappingValue(t, job, "needs")
	if dependencies.Kind != yaml.SequenceNode || len(dependencies.Content) != 2 || dependencies.Content[0].Value != "dns_configuration_plan" || dependencies.Content[1].Value != "dns_transport" {
		t.Fatal("retirement must follow independent transport configuration, never code builds")
	}
	if !strings.Contains(yamlMappingValue(t, job, "if").Value, "needs.dns_transport.result == 'success'") {
		t.Fatal("retirement cannot run after a failed selected handoff")
	}
	encoded, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"fugue-production-dns-transport", "git diff --exit-code HEAD origin/main", "--evidence", "--apply", "scripts.test_retire_dns_slots", "scripts.verify_dns_configuration_revision", "production"} {
		if !strings.Contains(string(encoded), required) {
			t.Fatal("retirement lost a required release fence", required)
		}
	}
}
