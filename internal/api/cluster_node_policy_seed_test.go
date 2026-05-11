package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestListClusterNodesSeedsBootstrapControlPlaneMachineForPlatformAdmin(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServer(t)
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.ClusterNodes) != 1 {
		t.Fatalf("expected one cluster node, got %d", len(response.ClusterNodes))
	}

	node := response.ClusterNodes[0]
	if node.Machine == nil {
		t.Fatalf("expected bootstrap control-plane node to expose machine metadata, got %#v", node)
	}
	if node.Machine.Scope != model.MachineScopePlatformNode {
		t.Fatalf("expected machine scope %q, got %q", model.MachineScopePlatformNode, node.Machine.Scope)
	}
	if strings.TrimSpace(node.Machine.NodeKeyID) != "" {
		t.Fatalf("expected bootstrap machine to start without node key id, got %q", node.Machine.NodeKeyID)
	}
	if node.Policy == nil {
		t.Fatalf("expected bootstrap control-plane node policy, got %#v", node)
	}
	if node.Policy.AllowBuilds {
		t.Fatalf("expected bootstrap control-plane node builds to default disabled, got %#v", node.Policy)
	}
	if node.Policy.AllowSharedPool {
		t.Fatalf("expected bootstrap control-plane node shared-pool to default disabled, got %#v", node.Policy)
	}
	if node.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected desired role %q, got %q", model.MachineControlPlaneRoleMember, node.Policy.DesiredControlPlaneRole)
	}
	if node.Policy.EffectiveControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected effective role %q, got %q", model.MachineControlPlaneRoleMember, node.Policy.EffectiveControlPlaneRole)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("expected seeded platform machine in store, got %v", err)
	}
	if machine.Scope != model.MachineScopePlatformNode {
		t.Fatalf("expected persisted machine scope %q, got %q", model.MachineScopePlatformNode, machine.Scope)
	}
	if strings.TrimSpace(machine.NodeKeyID) != "" {
		t.Fatalf("expected persisted bootstrap machine to have no node key id, got %q", machine.NodeKeyID)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected stored machine role %q, got %q", model.MachineControlPlaneRoleMember, machine.Policy.DesiredControlPlaneRole)
	}
}

func TestClusterNodePolicyStatusReportsDesiredActualAndDrift(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServer(t)
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	statusRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/node-policies/status", "bootstrap-secret", nil)
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, statusRecorder.Code, statusRecorder.Body.String())
	}
	var statusResponse struct {
		Summary      model.ClusterNodePolicyStatusSummary `json:"summary"`
		NodePolicies []model.ClusterNodePolicyStatus      `json:"node_policies"`
	}
	mustDecodeJSON(t, statusRecorder, &statusResponse)
	if statusResponse.Summary.Total != 1 || statusResponse.Summary.Drifted != 1 || statusResponse.Summary.Ready != 1 || statusResponse.Summary.DiskPressure != 0 {
		t.Fatalf("unexpected policy status summary: %+v", statusResponse.Summary)
	}
	if len(statusResponse.NodePolicies) != 1 {
		t.Fatalf("expected one node policy status, got %+v", statusResponse.NodePolicies)
	}
	nodePolicy := statusResponse.NodePolicies[0]
	if nodePolicy.NodeName != "gcp1" || nodePolicy.Policy == nil {
		t.Fatalf("unexpected node policy status: %+v", nodePolicy)
	}
	if nodePolicy.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected desired control-plane role backfill, got %+v", nodePolicy.Policy)
	}
	if nodePolicy.Reconciled || !strings.Contains(strings.Join(nodePolicy.ReconcileReasons, ";"), "node policy labels drift") {
		t.Fatalf("expected unreconciled label drift, got %+v", nodePolicy)
	}
	if nodePolicy.Labels["node-role.kubernetes.io/control-plane"] != "" {
		t.Fatalf("expected actual labels to include control-plane label, got %+v", nodePolicy.Labels)
	}

	getRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/node-policies/gcp1", "bootstrap-secret", nil)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, getRecorder.Code, getRecorder.Body.String())
	}
	var getResponse struct {
		NodePolicy model.ClusterNodePolicyStatus `json:"node_policy"`
	}
	mustDecodeJSON(t, getRecorder, &getResponse)
	if getResponse.NodePolicy.NodeName != "gcp1" {
		t.Fatalf("unexpected node policy get response: %+v", getResponse.NodePolicy)
	}
}

func TestListClusterNodesSeedsBootstrapControlPlaneMachineBuildPolicyFromLiveLabels(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		runtimepkg.BuildNodeLabelKey:  runtimepkg.BuildNodeLabelValue,
		legacyBuildTierLabelKey:       "large",
		runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue,
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.ClusterNodes) != 1 {
		t.Fatalf("expected one cluster node, got %d", len(response.ClusterNodes))
	}

	node := response.ClusterNodes[0]
	if node.Policy == nil {
		t.Fatalf("expected bootstrap control-plane node policy, got %#v", node)
	}
	if !node.Policy.AllowBuilds {
		t.Fatalf("expected desired builds to inherit live labels, got %#v", node.Policy)
	}
	if !node.Policy.EffectiveBuilds {
		t.Fatalf("expected effective builds enabled, got %#v", node.Policy)
	}
	if !node.Policy.AllowSharedPool {
		t.Fatalf("expected desired shared-pool to inherit live labels, got %#v", node.Policy)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("expected seeded platform machine in store, got %v", err)
	}
	if !machine.Policy.AllowBuilds {
		t.Fatalf("expected stored machine builds enabled, got %#v", machine.Policy)
	}
	if !machine.Policy.AllowSharedPool {
		t.Fatalf("expected stored machine shared-pool enabled, got %#v", machine.Policy)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected stored machine role %q, got %q", model.MachineControlPlaneRoleMember, machine.Policy.DesiredControlPlaneRole)
	}
}

func TestReconcileLegacyBuildTierLabelsFromSnapshotsRemovesLabelWithoutMachine(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		legacyBuildTierLabelKey: "large",
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	refreshed, changed, err := server.reconcileLegacyBuildTierLabelsFromSnapshots([]clusterNodeSnapshot{
		{
			node: model.ClusterNode{
				Name: "gcp1",
			},
			labels: map[string]string{
				legacyBuildTierLabelKey: "large",
			},
		},
	})
	if err != nil {
		t.Fatalf("reconcile legacy build-tier labels: %v", err)
	}
	if !changed {
		t.Fatal("expected legacy build-tier cleanup to report changes")
	}
	if len(refreshed) != 1 {
		t.Fatalf("expected one refreshed snapshot, got %d", len(refreshed))
	}
	if got := strings.TrimSpace(firstNodeLabel(refreshed[0].labels, legacyBuildTierLabelKey)); got != "" {
		t.Fatalf("expected legacy build-tier label removed, got %q", got)
	}
}

func TestStartBackgroundWarmersBackfillsLegacyBootstrapControlPlaneMachinePolicy(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	seeded := legacyBootstrapControlPlaneMachine()
	if err := rewriteBootstrapControlPlaneMachine(storePath, func(machine *model.Machine) {
		*machine = seeded
	}); err != nil {
		t.Fatalf("seed legacy bootstrap control-plane machine: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		runtimepkg.BuildNodeLabelKey:  runtimepkg.BuildNodeLabelValue,
		runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue,
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.StartBackgroundWarmers(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
		snapshots, snapshotErr := server.fetchClusterNodeInventory(context.Background())
		legacyLabelPresent := true
		if snapshotErr == nil && len(snapshots) > 0 {
			legacyLabelPresent = strings.TrimSpace(firstNodeLabel(snapshots[0].labels, legacyBuildTierLabelKey)) != ""
		}
		if err == nil &&
			machine.ID == seeded.ID &&
			machine.Policy.AllowBuilds &&
			machine.Policy.AllowSharedPool &&
			machine.Policy.DesiredControlPlaneRole == model.MachineControlPlaneRoleMember &&
			!legacyLabelPresent {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("load backfilled bootstrap control-plane machine: %v", err)
	}
	snapshots, snapshotErr := server.fetchClusterNodeInventory(context.Background())
	if snapshotErr != nil {
		t.Fatalf("load refreshed cluster node inventory: %v", snapshotErr)
	}
	legacyLabel := ""
	if len(snapshots) > 0 {
		legacyLabel = strings.TrimSpace(firstNodeLabel(snapshots[0].labels, legacyBuildTierLabelKey))
	}
	t.Fatalf("expected background warmers to backfill legacy machine policy and remove build-tier label, got policy=%#v legacy_label=%q", machine.Policy, legacyLabel)
}

func TestSyncBootstrapControlPlaneMachinesBackfillsAuditlessLegacyControlPlaneRole(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	updatedAt := time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC)
	if err := rewriteBootstrapControlPlaneMachine(storePath, func(machine *model.Machine) {
		*machine = legacyBootstrapControlPlaneMachine()
		machine.Policy.AllowBuilds = true
		machine.Policy.AllowSharedPool = true
		machine.UpdatedAt = updatedAt
		machine.LastSeenAt = &updatedAt
	}); err != nil {
		t.Fatalf("seed partially backfilled bootstrap control-plane machine: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		runtimepkg.BuildNodeLabelKey:  runtimepkg.BuildNodeLabelValue,
		runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue,
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	snapshots, err := server.fetchClusterNodeInventory(context.Background())
	if err != nil {
		t.Fatalf("fetch cluster node inventory: %v", err)
	}
	if err := server.syncBootstrapControlPlaneMachinesFromSnapshots(snapshots); err != nil {
		t.Fatalf("sync bootstrap control-plane machines: %v", err)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("load backfilled bootstrap control-plane machine: %v", err)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleMember {
		t.Fatalf("expected control-plane role backfill %q, got %q", model.MachineControlPlaneRoleMember, machine.Policy.DesiredControlPlaneRole)
	}
}

func TestSyncBootstrapControlPlaneMachinesPreservesAuditedControlPlaneRole(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	updatedAt := time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC)
	if err := rewriteBootstrapControlPlaneMachine(storePath, func(machine *model.Machine) {
		*machine = legacyBootstrapControlPlaneMachine()
		machine.Policy.AllowBuilds = true
		machine.Policy.AllowSharedPool = true
		machine.UpdatedAt = updatedAt
		machine.LastSeenAt = &updatedAt
	}); err != nil {
		t.Fatalf("seed audited bootstrap control-plane machine: %v", err)
	}
	if err := stateStore.AppendAuditEvent(model.AuditEvent{
		ID:         model.NewID("audit"),
		ActorType:  model.ActorTypeSystem,
		ActorID:    "system",
		Action:     "cluster.node.policy",
		TargetType: "cluster_node",
		TargetID:   "gcp1",
		Metadata:   map[string]string{"cluster_node_name": "gcp1"},
		CreatedAt:  updatedAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("append cluster node policy audit event: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		runtimepkg.BuildNodeLabelKey:  runtimepkg.BuildNodeLabelValue,
		runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue,
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	snapshots, err := server.fetchClusterNodeInventory(context.Background())
	if err != nil {
		t.Fatalf("fetch cluster node inventory: %v", err)
	}
	if err := server.syncBootstrapControlPlaneMachinesFromSnapshots(snapshots); err != nil {
		t.Fatalf("sync bootstrap control-plane machines: %v", err)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("load preserved bootstrap control-plane machine: %v", err)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleNone {
		t.Fatalf("expected audited control-plane role to remain %q, got %q", model.MachineControlPlaneRoleNone, machine.Policy.DesiredControlPlaneRole)
	}
}

func TestSetClusterNodePolicySeedsBootstrapControlPlaneMachine(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServer(t)
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/cluster/nodes/gcp1/policy", "bootstrap-secret", map[string]any{
		"allow_app_runtime":          true,
		"allow_builds":               true,
		"allow_shared_pool":          true,
		"allow_edge":                 true,
		"allow_dns":                  true,
		"allow_internal_maintenance": true,
		"desired_control_plane_role": model.MachineControlPlaneRoleCandidate,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNode    model.ClusterNode `json:"cluster_node"`
		NodeReconciled bool              `json:"node_reconciled"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.NodeReconciled {
		t.Fatalf("expected node reconciliation to succeed, got %#v", response)
	}
	if response.ClusterNode.Machine == nil {
		t.Fatalf("expected patched bootstrap control-plane node to expose machine metadata, got %#v", response.ClusterNode)
	}
	if response.ClusterNode.Policy == nil {
		t.Fatalf("expected patched bootstrap control-plane node policy, got %#v", response.ClusterNode)
	}
	if !response.ClusterNode.Policy.AllowBuilds {
		t.Fatalf("expected desired builds enabled after patch, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.AllowAppRuntime {
		t.Fatalf("expected desired app-runtime enabled after patch, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.AllowSharedPool {
		t.Fatalf("expected desired shared-pool enabled after patch, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.AllowEdge || !response.ClusterNode.Policy.AllowDNS || !response.ClusterNode.Policy.AllowInternalMaintenance {
		t.Fatalf("expected desired edge/dns/internal maintenance enabled after patch, got %#v", response.ClusterNode.Policy)
	}
	if response.ClusterNode.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleCandidate {
		t.Fatalf("expected desired role %q, got %q", model.MachineControlPlaneRoleCandidate, response.ClusterNode.Policy.DesiredControlPlaneRole)
	}
	if !response.ClusterNode.Policy.EffectiveBuilds {
		t.Fatalf("expected effective builds enabled after node reconcile, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.EffectiveSharedPool {
		t.Fatalf("expected effective shared-pool enabled after node reconcile, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.EffectiveEdge || !response.ClusterNode.Policy.EffectiveDNS || !response.ClusterNode.Policy.EffectiveInternalMaintenance {
		t.Fatalf("expected effective edge/dns/internal maintenance enabled after node reconcile, got %#v", response.ClusterNode.Policy)
	}
	if !response.ClusterNode.Policy.EffectiveSchedulable {
		t.Fatalf("expected healthy node to be effectively schedulable, got %#v", response.ClusterNode.Policy)
	}

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("expected patched bootstrap control-plane machine in store, got %v", err)
	}
	if !machine.Policy.AllowBuilds {
		t.Fatalf("expected stored machine builds enabled, got %#v", machine.Policy)
	}
	if !machine.Policy.AllowAppRuntime {
		t.Fatalf("expected stored machine app-runtime enabled, got %#v", machine.Policy)
	}
	if !machine.Policy.AllowSharedPool {
		t.Fatalf("expected stored machine shared-pool enabled, got %#v", machine.Policy)
	}
	if !machine.Policy.AllowEdge || !machine.Policy.AllowDNS || !machine.Policy.AllowInternalMaintenance {
		t.Fatalf("expected stored machine edge/dns/internal maintenance enabled, got %#v", machine.Policy)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleCandidate {
		t.Fatalf("expected stored machine role %q, got %q", model.MachineControlPlaneRoleCandidate, machine.Policy.DesiredControlPlaneRole)
	}

	statusRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/node-policies/gcp1", "bootstrap-secret", nil)
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, statusRecorder.Code, statusRecorder.Body.String())
	}
	var statusResponse struct {
		NodePolicy model.ClusterNodePolicyStatus `json:"node_policy"`
	}
	mustDecodeJSON(t, statusRecorder, &statusResponse)
	if !statusResponse.NodePolicy.Reconciled || len(statusResponse.NodePolicy.ReconcileReasons) != 0 {
		t.Fatalf("expected patched healthy node policy to be reconciled, got %+v", statusResponse.NodePolicy)
	}
}

func TestBuildMachineNodeMergePatchAppliesNodePolicyRolesAndHealthGate(t *testing.T) {
	t.Parallel()

	node := kubeNode{}
	node.Metadata.Labels = map[string]string{}
	node.Status.Conditions = []kubeNodeCondition{
		{Type: clusterNodeConditionReady, Status: "True"},
		{Type: clusterNodeConditionDisk, Status: "False"},
	}
	machine := model.Machine{
		ID:              "machine_edge",
		Scope:           model.MachineScopePlatformNode,
		ClusterNodeName: "edge-1",
		Policy: model.MachinePolicy{
			AllowBuilds: true,
			AllowEdge:   true,
			AllowDNS:    true,
		},
	}

	patch, changed := buildMachineNodeMergePatch(node, machine, nil)
	if !changed {
		t.Fatal("expected node policy patch to change empty node")
	}
	metadata := patch["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	for key, want := range map[string]string{
		runtimepkg.EdgeRoleLabelKey:        runtimepkg.NodeRoleLabelValue,
		runtimepkg.DNSRoleLabelKey:         runtimepkg.NodeRoleLabelValue,
		runtimepkg.BuilderRoleLabelKey:     runtimepkg.NodeRoleLabelValue,
		runtimepkg.BuildNodeLabelKey:       runtimepkg.BuildNodeLabelValue,
		runtimepkg.NodeSchedulableLabelKey: "true",
		runtimepkg.NodeHealthLabelKey:      runtimepkg.NodeHealthReadyValue,
	} {
		if got := labels[key]; got != want {
			t.Fatalf("expected label %s=%q, got %#v in %#v", key, want, got, labels)
		}
	}
	spec := patch["spec"].(map[string]any)
	taints := spec["taints"].([]kubeNodeTaint)
	if len(taints) != 1 || taints[0].Key != runtimepkg.DedicatedTaintKey || taints[0].Value != runtimepkg.DedicatedEdgeValue {
		t.Fatalf("expected edge dedicated taint for edge+dns node, got %#v", taints)
	}
}

func TestBuildMachineNodeMergePatchBlocksDiskPressureNodes(t *testing.T) {
	t.Parallel()

	node := kubeNode{}
	node.Metadata.Labels = map[string]string{
		runtimepkg.EdgeRoleLabelKey:        runtimepkg.NodeRoleLabelValue,
		runtimepkg.NodeSchedulableLabelKey: "true",
		runtimepkg.NodeHealthLabelKey:      runtimepkg.NodeHealthReadyValue,
	}
	node.Spec.Taints = []kubeNodeTaint{{
		Key:    runtimepkg.DedicatedTaintKey,
		Value:  runtimepkg.DedicatedEdgeValue,
		Effect: "NoSchedule",
	}}
	node.Status.Conditions = []kubeNodeCondition{
		{Type: clusterNodeConditionReady, Status: "True"},
		{Type: clusterNodeConditionDisk, Status: "True"},
	}
	machine := model.Machine{
		ID:              "machine_edge",
		Scope:           model.MachineScopePlatformNode,
		ClusterNodeName: "edge-1",
		Policy: model.MachinePolicy{
			AllowEdge: true,
		},
	}

	patch, changed := buildMachineNodeMergePatch(node, machine, nil)
	if !changed {
		t.Fatal("expected disk-pressure node to receive blocked health gate")
	}
	metadata := patch["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if got := labels[runtimepkg.NodeSchedulableLabelKey]; got != "false" {
		t.Fatalf("expected schedulable=false, got %#v in %#v", got, labels)
	}
	if got := labels[runtimepkg.NodeHealthLabelKey]; got != runtimepkg.NodeHealthBlockedValue {
		t.Fatalf("expected blocked health, got %#v in %#v", got, labels)
	}
	spec := patch["spec"].(map[string]any)
	taints := spec["taints"].([]kubeNodeTaint)
	foundHealthTaint := false
	for _, taint := range taints {
		if taint.Key == runtimepkg.NodeUnhealthyTaintKey && taint.Value == runtimepkg.NodeUnhealthyTaintValue {
			foundHealthTaint = true
		}
	}
	if !foundHealthTaint {
		t.Fatalf("expected node-unhealthy NoSchedule taint, got %#v", taints)
	}
}

func TestReconcileSharedPoolPolicyDriftFromSnapshotsRemovesBootstrapControlPlaneLabel(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := rewriteBootstrapControlPlaneMachine(storePath, func(machine *model.Machine) {
		*machine = legacyBootstrapControlPlaneMachine()
		machine.Policy.AllowSharedPool = false
	}); err != nil {
		t.Fatalf("seed bootstrap control-plane machine: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServerWithLabels(t, map[string]string{
		runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue,
	})
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	refreshed, changed, err := server.reconcileSharedPoolPolicyDriftFromSnapshots([]clusterNodeSnapshot{
		{
			node:       model.ClusterNode{Name: "gcp1"},
			sharedPool: true,
		},
	})
	if err != nil {
		t.Fatalf("reconcile shared-pool policy drift: %v", err)
	}
	if !changed {
		t.Fatal("expected shared-pool policy drift reconciliation to report changes")
	}
	if len(refreshed) != 1 {
		t.Fatalf("expected one refreshed snapshot, got %d", len(refreshed))
	}
	if got := strings.TrimSpace(firstNodeLabel(refreshed[0].labels, runtimepkg.SharedPoolLabelKey)); got != "" {
		t.Fatalf("expected shared-pool label removed after reconciliation, got %q", got)
	}
}

func TestReconcileSharedPoolPolicyDriftFromSnapshotsAddsBootstrapControlPlaneLabel(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := rewriteBootstrapControlPlaneMachine(storePath, func(machine *model.Machine) {
		*machine = legacyBootstrapControlPlaneMachine()
		machine.Policy.AllowSharedPool = true
	}); err != nil {
		t.Fatalf("seed bootstrap control-plane machine: %v", err)
	}

	kubeServer := newBootstrapControlPlaneKubeServer(t)
	defer kubeServer.Close()

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	refreshed, changed, err := server.reconcileSharedPoolPolicyDriftFromSnapshots([]clusterNodeSnapshot{
		{
			node:       model.ClusterNode{Name: "gcp1"},
			sharedPool: false,
		},
	})
	if err != nil {
		t.Fatalf("reconcile shared-pool policy drift: %v", err)
	}
	if !changed {
		t.Fatal("expected shared-pool policy drift reconciliation to report changes")
	}
	if len(refreshed) != 1 {
		t.Fatalf("expected one refreshed snapshot, got %d", len(refreshed))
	}
	if got := strings.TrimSpace(firstNodeLabel(refreshed[0].labels, runtimepkg.SharedPoolLabelKey)); got != runtimepkg.SharedPoolLabelValue {
		t.Fatalf("expected shared-pool label restored after reconciliation, got %q", got)
	}
}

func newBootstrapControlPlaneKubeServer(t *testing.T) *httptest.Server {
	return newBootstrapControlPlaneKubeServerWithLabels(t, nil)
}

func legacyBootstrapControlPlaneMachine() model.Machine {
	now := time.Date(2026, time.April, 20, 0, 0, 0, 0, time.UTC)
	return model.Machine{
		ID:              model.NewID("machine"),
		Name:            "gcp1",
		Scope:           model.MachineScopePlatformNode,
		ConnectionMode:  model.MachineConnectionModeCluster,
		Status:          model.RuntimeStatusActive,
		Endpoint:        "203.0.113.10",
		Labels:          map[string]string{"node-role.kubernetes.io/control-plane": ""},
		ClusterNodeName: "gcp1",
		Policy: model.MachinePolicy{
			AllowBuilds:             false,
			AllowSharedPool:         false,
			DesiredControlPlaneRole: model.MachineControlPlaneRoleNone,
		},
		CreatedAt:  now,
		UpdatedAt:  now,
		LastSeenAt: &now,
	}
}

func rewriteBootstrapControlPlaneMachine(storePath string, mutate func(machine *model.Machine)) error {
	data, err := os.ReadFile(storePath)
	if err != nil {
		return err
	}
	var state model.State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	found := false
	for idx := range state.Machines {
		if strings.EqualFold(strings.TrimSpace(state.Machines[idx].ClusterNodeName), "gcp1") {
			mutate(&state.Machines[idx])
			found = true
			break
		}
	}
	if !found {
		machine := legacyBootstrapControlPlaneMachine()
		mutate(&machine)
		state.Machines = append(state.Machines, machine)
	}

	next, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(storePath, next, 0o600)
}

func newBootstrapControlPlaneKubeServerWithLabels(t *testing.T, extraLabels map[string]string) *httptest.Server {
	t.Helper()

	var (
		mu     = sync.Mutex{}
		labels = map[string]string{
			"node-role.kubernetes.io/control-plane": "",
			"topology.kubernetes.io/region":         "us-central1",
			"topology.kubernetes.io/zone":           "us-central1-a",
		}
		taints []map[string]string
	)
	for key, value := range extraLabels {
		labels[key] = value
	}

	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
			mu.Lock()
			node := bootstrapControlPlaneKubeNodeJSON(labels, taints)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{node},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/gcp1":
			mu.Lock()
			node := bootstrapControlPlaneKubeNodeJSON(labels, taints)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(node)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/gcp1/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": "gcp1",
					"cpu": map[string]any{
						"usageNanoCores": 500_000_000,
					},
					"memory": map[string]any{
						"workingSetBytes": 2 * 1024 * 1024 * 1024,
					},
					"fs": map[string]any{
						"capacityBytes": 100 * 1024 * 1024 * 1024,
						"usedBytes":     25 * 1024 * 1024 * 1024,
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/nodes/gcp1":
			var payload struct {
				Metadata struct {
					Labels map[string]*string `json:"labels"`
				} `json:"metadata"`
				Spec struct {
					Taints []map[string]string `json:"taints"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode node patch: %v", err)
			}
			mu.Lock()
			for key, value := range payload.Metadata.Labels {
				if value == nil {
					delete(labels, key)
					continue
				}
				labels[key] = *value
			}
			if payload.Spec.Taints != nil {
				taints = payload.Spec.Taints
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
}

func bootstrapControlPlaneKubeNodeJSON(labels map[string]string, taints ...[]map[string]string) map[string]any {
	clonedLabels := make(map[string]string, len(labels))
	for key, value := range labels {
		clonedLabels[key] = value
	}
	node := map[string]any{
		"metadata": map[string]any{
			"name":              "gcp1",
			"creationTimestamp": "2026-04-20T00:00:00Z",
			"labels":            clonedLabels,
		},
		"status": map[string]any{
			"addresses": []map[string]string{
				{"type": "InternalIP", "address": "10.0.0.10"},
				{"type": "ExternalIP", "address": "203.0.113.10"},
				{"type": "Hostname", "address": "gcp1-host"},
			},
			"conditions": []map[string]string{
				{
					"type":               "Ready",
					"status":             "True",
					"reason":             "KubeletReady",
					"message":            "kubelet is posting ready status",
					"lastTransitionTime": "2026-04-20T00:01:00Z",
				},
				{
					"type":               "DiskPressure",
					"status":             "False",
					"reason":             "KubeletHasNoDiskPressure",
					"message":            "kubelet has no disk pressure",
					"lastTransitionTime": "2026-04-20T00:01:00Z",
				},
			},
			"capacity": map[string]string{
				"cpu":               "4",
				"memory":            "16Gi",
				"ephemeral-storage": "100Gi",
			},
			"allocatable": map[string]string{
				"cpu":               "3900m",
				"memory":            "15Gi",
				"ephemeral-storage": "90Gi",
			},
			"nodeInfo": map[string]string{
				"kubeletVersion":          "v1.32.2",
				"osImage":                 "Ubuntu 24.04.1 LTS",
				"kernelVersion":           "6.8.0",
				"containerRuntimeVersion": "containerd://2.0.0",
				"machineID":               "machine-id-gcp1",
				"systemUUID":              "uuid-gcp1",
			},
		},
	}
	if len(taints) > 0 && taints[0] != nil {
		node["spec"] = map[string]any{"taints": taints[0]}
	}
	return node
}
