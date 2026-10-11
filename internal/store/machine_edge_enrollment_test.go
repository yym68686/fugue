package store

import (
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestMachineEdgeEnrollmentPreservesTenantAccessAndRejectsAuthorityMigration(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("enrollment fixture")
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
	policy := machine.Policy
	policy.AllowEdge = true
	for range 2 {
		got, err := s.SetMachineEdgePolicyByClusterNodeName(runtime.ClusterNodeName, policy, "edge-group-fixture")
		if err != nil {
			t.Fatal(err)
		}
		if got.Labels[runtimepkg.EdgeGroupIDLabelKey] != "edge-group-fixture" || got.Labels[runtimepkg.EdgeWorkloadLabelKey] != "dynamic" || !reflect.DeepEqual(got.Policy, policy) || got.TenantID != tenant.ID || got.RuntimeID != runtime.ID {
			t.Fatalf("enrollment changed identity or role intent: %+v", got)
		}
	}
	before, _ := s.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	rediscovered, err := s.EnsureMachineForRuntime(runtime.ID)
	if err != nil || rediscovered.Labels[runtimepkg.EdgeGroupIDLabelKey] != "edge-group-fixture" || rediscovered.Labels[runtimepkg.EdgeWorkloadLabelKey] != "dynamic" {
		t.Fatalf("runtime refresh lost enrollment: %+v, %v", rediscovered, err)
	}
	before, _ = s.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	policy.AllowBuilds = true
	if _, err := s.SetMachineEdgePolicyByClusterNodeName(runtime.ClusterNodeName, policy, "edge-group-other"); err == nil {
		t.Fatal("authority migration accepted")
	}
	after, _ := s.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected enrollment changed saved machine")
	}
	gotRuntime, err := s.GetRuntime(runtime.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRuntime.AccessMode != model.RuntimeAccessModePrivate || gotRuntime.PoolMode != model.RuntimePoolModeDedicated {
		t.Fatalf("changed tenant access: %+v", gotRuntime)
	}
}
