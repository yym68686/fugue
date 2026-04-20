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
		"allow_builds":               true,
		"allow_shared_pool":          true,
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
	if !response.ClusterNode.Policy.AllowSharedPool {
		t.Fatalf("expected desired shared-pool enabled after patch, got %#v", response.ClusterNode.Policy)
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

	machine, err := stateStore.GetMachineByClusterNodeName("gcp1")
	if err != nil {
		t.Fatalf("expected patched bootstrap control-plane machine in store, got %v", err)
	}
	if !machine.Policy.AllowBuilds {
		t.Fatalf("expected stored machine builds enabled, got %#v", machine.Policy)
	}
	if !machine.Policy.AllowSharedPool {
		t.Fatalf("expected stored machine shared-pool enabled, got %#v", machine.Policy)
	}
	if machine.Policy.DesiredControlPlaneRole != model.MachineControlPlaneRoleCandidate {
		t.Fatalf("expected stored machine role %q, got %q", model.MachineControlPlaneRoleCandidate, machine.Policy.DesiredControlPlaneRole)
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
			node := bootstrapControlPlaneKubeNodeJSON(labels)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{node},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/gcp1":
			mu.Lock()
			node := bootstrapControlPlaneKubeNodeJSON(labels)
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
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
}

func bootstrapControlPlaneKubeNodeJSON(labels map[string]string) map[string]any {
	clonedLabels := make(map[string]string, len(labels))
	for key, value := range labels {
		clonedLabels[key] = value
	}
	return map[string]any{
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
}
