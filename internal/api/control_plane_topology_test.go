package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestControlPlaneTopologySingleNodeIsRiskNotFailure(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	topology := buildControlPlaneTopology([]model.ControlPlaneTopologyNode{{
		NodeName:                  "cp-1",
		Ready:                     true,
		ControlPlaneCapable:       true,
		EtcdVoterCapable:          true,
		EffectiveControlPlaneRole: model.MachineControlPlaneRoleMember,
		FailureDomain:             "provider:ovh/region:us-west/zone:a",
	}}, now)
	if topology.Mode != model.ControlPlaneTopologyModeSingle || topology.Status != model.ControlPlaneTopologyStatusSingleNodeRisk {
		t.Fatalf("expected single node risk, got %+v", topology)
	}
	if topology.HAReady || topology.QuorumReady {
		t.Fatalf("single node must not report HA/quorum ready: %+v", topology)
	}
	check := controlPlaneTopologyCheck(topology)
	if !check.Pass || check.Severity != model.RobustnessSeverityWarning {
		t.Fatalf("single physical control plane should be warning, not release failure: %+v", check)
	}
}

func TestControlPlaneTopologyFromClusterNodesUsesKubernetesControlPlaneRole(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	topology := buildControlPlaneTopologyFromClusterNodes([]model.ClusterNode{{
		Name:   "cp-1",
		Status: "ready",
		Roles:  []string{"control-plane", "etcd"},
		Region: "us-west",
		Policy: &model.ClusterNodePolicy{
			DesiredControlPlaneRole:   model.MachineControlPlaneRoleMember,
			EffectiveControlPlaneRole: model.MachineControlPlaneRoleMember,
		},
	}}, now)

	if topology.Mode != model.ControlPlaneTopologyModeSingle || topology.Status != model.ControlPlaneTopologyStatusSingleNodeRisk {
		t.Fatalf("expected Kubernetes control-plane node to report single-control-plane risk, got %+v", topology)
	}
	if topology.ControlPlaneCapableNodes != 1 || topology.EtcdVoterCapableNodes != 1 {
		t.Fatalf("expected one control-plane/etcd capable node, got %+v", topology)
	}
	check := controlPlaneTopologyCheck(topology)
	if !check.Pass || check.Severity != model.RobustnessSeverityWarning {
		t.Fatalf("single Kubernetes control-plane node should be warning, not failure: %+v", check)
	}
}

func TestControlPlaneTopologyThreeNodesHAReady(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	topology := buildControlPlaneTopology([]model.ControlPlaneTopologyNode{
		{NodeName: "cp-1", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, ReleaseRunnerCapable: true, FailureDomain: "provider:a/region:us/zone:1"},
		{NodeName: "cp-2", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, ReleaseRunnerCapable: true, FailureDomain: "provider:b/region:eu/zone:1"},
		{NodeName: "cp-3", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, FailureDomain: "provider:c/region:jp/zone:1"},
	}, now)
	if topology.Mode != model.ControlPlaneTopologyModeHAQuorum || topology.Status != model.ControlPlaneTopologyStatusHAReady {
		t.Fatalf("expected HA quorum, got %+v", topology)
	}
	if !topology.HAReady || !topology.QuorumReady {
		t.Fatalf("expected HA/quorum ready: %+v", topology)
	}
	if len(topology.MissingRedundancy) != 0 || len(topology.RiskWarnings) != 0 {
		t.Fatalf("expected no redundancy gaps, got warnings=%v missing=%v", topology.RiskWarnings, topology.MissingRedundancy)
	}
}

func TestControlPlaneTopologyThreeNodesSameFailureDomainWarns(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	topology := buildControlPlaneTopology([]model.ControlPlaneTopologyNode{
		{NodeName: "cp-1", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, ReleaseRunnerCapable: true, FailureDomain: "provider:a/region:us/zone:1"},
		{NodeName: "cp-2", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, ReleaseRunnerCapable: true, FailureDomain: "provider:a/region:us/zone:1"},
		{NodeName: "cp-3", Ready: true, ControlPlaneCapable: true, EtcdVoterCapable: true, FailureDomain: "provider:a/region:us/zone:1"},
	}, now)
	if topology.Status != model.ControlPlaneTopologyStatusPartialRedundancy {
		t.Fatalf("same failure domain should not be HA ready, got %+v", topology)
	}
	if topology.HAReady {
		t.Fatalf("same failure domain should not be HA ready: %+v", topology)
	}
}
