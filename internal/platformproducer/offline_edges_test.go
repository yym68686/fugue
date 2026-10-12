package platformproducer

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestOfflineIntentSurvivesDateAndHeartbeatRecovery(t *testing.T) {
	now := time.Now().UTC()
	nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Healthy: true, LastHeartbeatAt: &now}, {ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9", Healthy: false}}
	holds := []OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", ExpectedRecoveryDate: "2020-01-01"}}
	selected, err := SelectDNSCandidateEndpoints(nodes, holds)
	if err != nil || !reflect.DeepEqual(selected, nodes[1:]) || !nodes[0].Healthy {
		t.Fatal("configuration must retain hold without falsifying live facts", selected, err)
	}
	selected, err = SelectDNSCandidateEndpoints(nodes, nil)
	if err != nil || !reflect.DeepEqual(selected, nodes) || selected[1].Healthy {
		t.Fatal("explicit resume restores only candidates, not serving health", selected, err)
	}
	for _, scenario := range []string{"identity", "missing", "duplicate", "all", "date"} {
		t.Run(scenario, func(t *testing.T) {
			inventory := append([]model.EdgeNode(nil), nodes...)
			declaration := append([]OfflineEdge(nil), holds...)
			switch scenario {
			case "identity":
				inventory[0].PublicIPv4 = "1.1.1.1"
			case "missing":
				inventory = inventory[1:]
			case "duplicate":
				inventory = append(inventory, inventory[0])
			case "all":
				inventory = inventory[:1]
			case "date":
				declaration[0].ExpectedRecoveryDate = "2026-02-30"
			}
			if _, err := SelectDNSCandidateEndpoints(inventory, declaration); err == nil {
				t.Fatal("unsafe offline projection accepted")
			}
		})
	}
}
