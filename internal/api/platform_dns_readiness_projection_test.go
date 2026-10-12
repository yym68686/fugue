package api

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestDNSReadinessProjectionSeparatesTopologyFromHeartbeatHealth(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-24 * time.Hour)
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Healthy: false, LastHeartbeatAt: &old}, {ID: "edge-b", EdgeGroupID: "edge-group-b", PublicIPv4: "9.9.9.9", Healthy: true, LastHeartbeatAt: &now}}
	result := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := projectDNSReadiness(&result, nodes, now); err != nil {
		t.Fatal(err)
	}
	if len(result.RuntimeSnapshot.DNSEdgeEndpoints) != 2 || result.Policy.DNSReadiness == nil || result.Policy.TLSReadiness == nil || result.Policy.TLSReadiness == result.Policy.DNSReadiness || result.RuntimeSnapshot.PolicyGeneration != result.Policy.Generation {
		t.Fatal("missing topology or policy binding")
	}
	prior := result
	nodes[0].Healthy = true
	nodes[1].Healthy = false
	if err := projectDNSReadiness(&result, nodes, now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prior, result) {
		t.Fatal("health changed endpoint authorization or desired policy")
	}
	nodes[0].PublicIPv4 = "10.0.0.1"
	if err := projectDNSReadiness(&result, nodes, now); err == nil {
		t.Fatal("private endpoint captured")
	}
	if !reflect.DeepEqual(prior, result) {
		t.Fatal("failed capture partially modified projection")
	}
}

func TestDNSCandidateProjectionAppliesSignedOfflineIntent(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Healthy: true}, {ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9", Healthy: false}}
	result := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := projectDNSReadiness(&result, nodes, now); err != nil {
		t.Fatal(err)
	}
	policy := &platformproducer.ProjectionPolicyInput{DNSReadiness: result.Policy.DNSReadiness, TLSReadiness: result.Policy.TLSReadiness, Cohorts: result.Policy.TrafficRolloutCohorts,
		OfflineEdges: []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "maintenance", ExpectedRecoveryDate: "2020-01-01"}}}
	if err := projectDNSReadinessWithPolicy(&result, nodes, now, policy); err != nil {
		t.Fatal(err)
	}
	if len(result.RuntimeSnapshot.DNSEdgeEndpoints) != 1 || result.RuntimeSnapshot.DNSEdgeEndpoints[0].EdgeID != "edge-b" {
		t.Fatal("held healthy endpoint still eligible", result.RuntimeSnapshot.DNSEdgeEndpoints)
	}
	previous := result
	nodes[0].PublicIPv4 = "1.1.1.1"
	if err := projectDNSReadinessWithPolicy(&result, nodes, now, policy); err == nil || !reflect.DeepEqual(previous, result) {
		t.Fatal("identity substitution must reject without changing LKG candidate")
	}
}
