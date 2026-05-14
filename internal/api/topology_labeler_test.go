package api

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestTopologyLabelerTemplateDoesNotManageSharedPoolMembership(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "deploy", "helm", "fugue", "templates", "topology-labeler-daemonset.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read topology labeler template: %v", err)
	}
	if strings.Contains(string(data), "fugue.io/shared-pool") {
		t.Fatalf("expected topology labeler template to leave shared-pool membership to policy/runtime reconciliation: %s", path)
	}
}

func TestUpgradeScriptDoesNotReapplySharedPoolLabels(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	if strings.Contains(string(data), "fugue.io/shared-pool=internal") {
		t.Fatalf("expected upgrade script to avoid reapplying shared-pool labels: %s", path)
	}
	for _, want := range []string{
		"fugue.install/role=primary",
		"fugue.io/shared-pool-",
		"fugue.io/build-",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("expected upgrade script to remove primary pool label %q: %s", want, path)
		}
	}
}

func TestUpgradeScriptPinsTopologyLabelerTolerations(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	script := string(data)
	blockStart := strings.Index(script, "topologyLabeler:\n  tolerations:")
	if blockStart == -1 {
		t.Fatalf("expected upgrade script to override topology-labeler tolerations: %s", path)
	}
	blockEnd := strings.Index(script[blockStart:], "\nedge:")
	if blockEnd == -1 {
		t.Fatalf("expected topology-labeler toleration override before edge block: %s", path)
	}
	block := script[blockStart : blockStart+blockEnd]
	for _, want := range []string{
		"key: node-role.kubernetes.io/control-plane",
		"key: node-role.kubernetes.io/master",
		"key: fugue.io/dedicated",
		"value: internal",
		"operator: Equal",
		"effect: NoSchedule",
	} {
		if !strings.Contains(block, want) {
			t.Fatalf("expected topology-labeler override to contain %q:\n%s", want, block)
		}
	}
	for _, unwanted := range []string{
		"operator: Exists",
		"node.kubernetes.io/disk-pressure",
	} {
		if strings.Contains(block, unwanted) {
			t.Fatalf("topology-labeler override should not contain %q:\n%s", unwanted, block)
		}
	}
}

func TestUpgradeScriptPinsEdgeDNSHealthyNodeSelectors(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	script := string(data)
	for _, want := range []string{
		"edge:\n  nodeSelector:\n    fugue.io/role.edge: \"true\"\n    fugue.io/schedulable: \"true\"",
		"key: fugue.io/tenant\n      operator: Exists\n      effect: NoSchedule",
		"fugue.io/role.dns: \"true\"",
		"fugue.io/schedulable: \"true\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected upgrade script to pin healthy node selector fragment %q: %s", want, path)
		}
	}
}

func TestUpgradeScriptPrunesOnlyStatelessReleasePodsOnUnhealthyNodes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	script := string(data)
	for _, want := range []string{
		"force_delete_release_pods_on_unhealthy_nodes()",
		"force deleting ${count} stateless Fugue release pods on unhealthy node ${node_name}",
		"force_delete_release_pods_on_unhealthy_nodes",
		"--force",
		"--grace-period=0",
		"api|controller|node-janitor|topology-labeler|shared-workspace-provisioner|edge|dns|edge-*|dns-*",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected upgrade script to contain %q: %s", want, path)
		}
	}
	blockStart := strings.Index(script, "is_stateless_release_component()")
	if blockStart == -1 {
		t.Fatalf("expected stateless component helper in upgrade script: %s", path)
	}
	blockEnd := strings.Index(script[blockStart:], "\nstateless_release_pod_names_on_node()")
	if blockEnd == -1 {
		t.Fatalf("expected stateless component helper before pod listing helper: %s", path)
	}
	block := script[blockStart : blockStart+blockEnd]
	for _, unwanted := range []string{
		"postgres|",
		"registry|",
		"headscale|",
		"shared-workspace-nfs",
	} {
		if strings.Contains(block, unwanted) {
			t.Fatalf("upgrade script stateless prune should not target stateful component %q: %s", unwanted, path)
		}
	}
}

func TestUpgradeScriptDoesNotBlockHAUpgradeOnNotReadyPrimary(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	script := string(data)
	for _, want := range []string{
		"HA upgrade will continue on remaining Ready control-plane nodes",
		"skip primary mesh restore because primary node ${primary_node_name} is NotReady",
		"skip primary disk-pressure recovery because primary node ${primary_node_name} is NotReady",
		"skip Route A edge proxy sync because primary node ${primary_node_name} is NotReady",
		"warning: Route A edge proxy sync failed; continuing because edge/API rollout already completed",
		"operator: NotIn",
		"- primary",
		"maxUnavailable: 0",
		"maxSurge: 2",
		"force_delete_release_pods_on_unhealthy_nodes",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected upgrade script to contain primary-down HA guard %q: %s", want, path)
		}
	}
	for _, unwanted := range []string{
		"primary node ${primary_node_name} is NotReady; restarting k3s on the primary host",
		"primary node ${primary_node_name} remained NotReady after cleanup and SSH restart fallback",
	} {
		if strings.Contains(script, unwanted) {
			t.Fatalf("upgrade script should not block HA deploy on %q: %s", unwanted, path)
		}
	}
}

func TestUpgradeScriptSupportsNonControlPlaneSingletonAnchor(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	script := string(data)
	for _, want := range []string{
		"append_control_plane_singleton_values()",
		"FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED",
		"FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR",
		"validate_control_plane_singleton_anchor",
		"skip legacy postgres recovery because legacy postgres is disabled",
		"must match exactly one node",
		"must not be a control-plane node",
		"FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR",
		"patch_control_plane_singleton_deployments()",
		"patch_singleton_deployment_node_selector()",
		"selector_json()",
		"nodeSelector: null",
		"patch deploy",
		"FUGUE_REGISTRY_DEPLOYMENT_NAME",
		"FUGUE_HEADSCALE_DEPLOYMENT_NAME",
		"FUGUE_SHARED_WORKSPACE_NFS_DEPLOYMENT_NAME",
		"FUGUE_SHARED_WORKSPACE_PROVISIONER_DEPLOYMENT_NAME",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected upgrade script to contain singleton anchor support %q: %s", want, path)
		}
	}
}

func TestDeployWorkflowDefaultsLegacyPostgresOffWhenCNPGIsAPIStore(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), ".github", "workflows", "deploy-control-plane.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read deploy workflow: %v", err)
	}
	workflow := string(data)
	for _, want := range []string{
		"FUGUE_POSTGRES_ENABLED: ${{ vars.FUGUE_POSTGRES_ENABLED || (vars.FUGUE_CONTROL_PLANE_POSTGRES_USE_FOR_API == 'true' && 'false' || 'true') }}",
		"FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED: ${{ vars.FUGUE_CONTROL_PLANE_SINGLETONS_ENABLED || 'false' }}",
		"FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR: ${{ vars.FUGUE_CONTROL_PLANE_SINGLETON_NODE_SELECTOR || '' }}",
		"FUGUE_CONTROL_PLANE_KUBE_API_FALLBACK_SERVERS: ${{ vars.FUGUE_CONTROL_PLANE_KUBE_API_FALLBACK_SERVERS || 'https://100.64.0.2:6443,https://100.64.0.3:6443' }}",
		"FUGUE_SYNC_EDGE_PROXY: ${{ vars.FUGUE_SYNC_EDGE_PROXY || 'false' }}",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("expected deploy workflow to contain %q: %s", want, path)
		}
	}
}
