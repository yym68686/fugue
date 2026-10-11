package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestManagedEdgeEnrollmentPreflightAndCredentialRetry(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("edge enrollment fixture")
	if err != nil {
		t.Fatal(err)
	}
	_, secret, err := s.CreateNodeKey(tenant.ID, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	_, runtime, err := s.BootstrapClusterNode(secret, "node-new", "https://node.example.test", nil, "node-new", "fixture-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	machine, err := s.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, "bootstrap-fixture"), nil, ServerConfig{})
	if err := server.validateManagedEdgeEnrollment(machine, "edge-group-missing"); err == nil {
		t.Fatal("missing authority accepted")
	}
	edge, _, err := s.CreateEdgeNodeToken(model.EdgeNode{ID: machine.ClusterNodeName, EdgeGroupID: "edge-group-fixture", WorkloadMode: model.EdgeWorkloadModeStatic, CanaryState: model.EdgeCanaryStateJoined})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.validateManagedEdgeEnrollment(machine, edge.EdgeGroupID); err == nil {
		t.Fatal("missing updater accepted")
	}
	updater, _, err := s.EnrollNodeUpdater(secret, runtime.ClusterNodeName, runtime.Endpoint, nil, "node-new", "fixture-fingerprint", "test", "test", []string{model.NodeUpdateTaskTypeRefreshDesiredState})
	if err != nil {
		t.Fatal(err)
	}
	updater, err = s.UpdateNodeUpdaterHeartbeat(updater.ID, model.NodeUpdater{Capabilities: []string{model.NodeUpdateTaskTypeRefreshDesiredState}})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.validateManagedEdgeEnrollment(machine, edge.EdgeGroupID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	intent := model.EdgeNodeEnrollment{ID: "enroll-fixture", NodeID: machine.ClusterNodeName, EdgeGroupID: edge.EdgeGroupID, AuthorityRecordDigest: "sha256:" + strings.Repeat("a", 64), Slot: "a", FenceSequence: 1, BundleVersion: "fixture.p1.r0", WorkerSourceSHA: strings.Repeat("b", 40), WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	savedPolicy := machine.Policy
	savedPolicy.AllowEdge = true
	machine, err = s.SetMachineEdgeEnrollment(machine.ClusterNodeName, savedPolicy, intent)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := server.managedEdgeCredentialInstalled(machine); err != nil || ready {
		t.Fatal("node was scheduled before credential installation", err)
	}
	// Kubernetes labels intentionally remain absent until the credential is
	// installed; explicit intent must still drive authenticated updater setup.
	policy := &model.ClusterNodePolicyStatus{NodeName: machine.ClusterNodeName, Labels: map[string]string{}, Policy: &model.ClusterNodePolicy{AllowEdge: true}}
	// A partially enrolled static identity must become node-managed even when
	// the old token prefix has already been acknowledged by the updater.
	updater.EdgeEnvGeneration = "v2:" + edge.TokenPrefix + ":fixture"
	req := httptest.NewRequest("GET", "https://api.example.test/v1/node-updaters/desired-state", nil)
	credential, _, err := server.nodeUpdaterEdgeCredential(req, updater, policy)
	if err != nil {
		t.Fatal(err)
	}
	if credential == nil || credential.Token == "" || credential.WorkloadMode != "dynamic" || credential.TokenPrefix == edge.TokenPrefix {
		t.Fatalf("partial enrollment did not issue the intended credential: %+v", credential)
	}
	updater.EdgeEnvGeneration = "v2:" + credential.TokenPrefix + ":fixture"
	updater, err = s.UpdateNodeUpdaterHeartbeat(updater.ID, updater)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := server.managedEdgeCredentialInstalled(machine); err != nil || !ready {
		t.Fatal("acknowledged credential did not release scheduling gate", err)
	}
	retry, _, err := server.nodeUpdaterEdgeCredential(req, updater, policy)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Token != "" || retry.TokenPrefix != credential.TokenPrefix {
		t.Fatal("idempotent retry rotated an acknowledged credential")
	}
	// An established static node must not be silently converted by enrollment.
	_, _, err = s.CreateEdgeNodeToken(model.EdgeNode{ID: machine.ClusterNodeName, EdgeGroupID: edge.EdgeGroupID, WorkloadMode: model.EdgeWorkloadModeStatic})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.UpdateEdgeHeartbeat(model.EdgeNode{ID: machine.ClusterNodeName, EdgeGroupID: edge.EdgeGroupID, Status: model.EdgeHealthHealthy, Healthy: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.validateManagedEdgeEnrollment(machine, edge.EdgeGroupID); err == nil {
		t.Fatal("established static credentials accepted for implicit migration")
	}
}

func TestEdgeEnrollmentLeaseIsExplicitStableAndCannotFollowAuthorityChange(t *testing.T) {
	now := time.Now().UTC()
	group := "edge-group-fixture"
	authority := edgeEnrollmentAuthority{GroupID: group, Record: "sha256:" + strings.Repeat("a", 64), Slot: "a", Fence: 5, Bundle: "fixture.p8.r0", Source: strings.Repeat("b", 40), Image: "sha256:" + strings.Repeat("c", 64)}
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/system-fixture/configmaps/fugue-current-authority-"+group {
			t.Errorf("unexpected Kubernetes mutation/read: %s %s", r.Method, r.URL.Path)
		}
		raw, _ := json.Marshal(authority)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"authority.json": string(raw)}})
	}))
	defer kube.Close()
	server := &Server{controlPlaneNamespace: "system-fixture", newClusterNodeClient: func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "fixture"}, nil
	}}
	machine := model.Machine{ClusterNodeName: "node-new", Labels: map[string]string{}}
	first, err := server.prepareManagedEdgeEnrollment(context.Background(), machine, group)
	if err != nil {
		t.Fatal(err)
	}
	if first.Validate(machine.ClusterNodeName, group, now) != nil {
		t.Fatal("invalid initial lease")
	}
	raw, _ := json.Marshal(first)
	machine.Labels[model.MachineEdgeEnrollmentLabel] = string(raw)
	retry, err := server.prepareManagedEdgeEnrollment(context.Background(), machine, group)
	if err != nil || retry != first {
		t.Fatal("retry renewed or changed its existing lease", err)
	}
	authority.Record = "sha256:" + strings.Repeat("d", 64)
	next, err := server.prepareManagedEdgeEnrollment(context.Background(), machine, group)
	if err != nil || next.ID == first.ID {
		t.Fatal("changed authority reused prior permission", err)
	}
	// A desired-state read only returns the stored permission if it still
	// equals current authority; it never stores this newly prepared successor.
	if machine.Labels[model.MachineEdgeEnrollmentLabel] != string(raw) {
		t.Fatal("read rewrote stored intent")
	}
}
