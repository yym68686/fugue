package api

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSReadinessProjectionSeparatesTopologyFromHeartbeatHealth(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-24 * time.Hour)
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Healthy: false, LastHeartbeatAt: &old}, {ID: "edge-b", EdgeGroupID: "edge-group-b", PublicIPv4: "9.9.9.9", Healthy: true, LastHeartbeatAt: &now}}
	result := platformIntentProjectionResponse{Policy: platformconfig.PolicySnapshot{Generation: "old", Scope: "global"}}
	if err := projectDNSReadiness(&result, nodes, now); err != nil {
		t.Fatal(err)
	}
	if len(result.RuntimeSnapshot.DNSEdgeEndpoints) != 2 || result.Policy.DNSReadiness == nil || result.RuntimeSnapshot.PolicyGeneration != result.Policy.Generation {
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
