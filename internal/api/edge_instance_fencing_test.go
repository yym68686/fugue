package api

import (
	"testing"

	"fugue/internal/model"
)

func TestRouteInventoryConsumesOnlyCentrallyFencedActiveEpoch(t *testing.T) {
	t.Parallel()
	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID: "edge-shared", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthUnknown,
	}); err != nil {
		t.Fatalf("create control identity: %v", err)
	}
	a := model.EdgeNodeInstance{
		EdgeID: "edge-shared", EdgeGroupID: "edge-group-country-de", Slot: model.EdgeSlotA,
		InstanceUID: "pod-a", ReleaseEpoch: "release-a",
		Node: model.EdgeNode{ID: "edge-shared", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-a", CaddyRouteCount: 2},
	}
	b := a
	b.Slot, b.InstanceUID, b.ReleaseEpoch = model.EdgeSlotB, "pod-b", "release-b"
	b.Node.RouteBundleVersion = "route-b"
	for _, instance := range []model.EdgeNodeInstance{a, a, b, b} {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			t.Fatalf("seed instance heartbeat: %v", err)
		}
	}
	if _, err := storeState.PutEdgeActiveEpoch(model.EdgeActiveEpoch{
		EdgeGroupID: "edge-group-country-de", Slot: model.EdgeSlotB, ReleaseEpoch: "release-b", FenceSequence: 7,
	}); err != nil {
		t.Fatalf("fence B active: %v", err)
	}

	a.Node.Status, a.Node.Healthy = model.EdgeHealthUnhealthy, false
	for index := 0; index < 4; index++ {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(a); err != nil {
			t.Fatalf("late A heartbeat: %v", err)
		}
	}
	healthy, nodeIDs, _, _, err := server.edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("route inventory: %v", err)
	}
	if !healthy["edge-group-country-de"] || len(nodeIDs["edge-group-country-de"]) != 1 || nodeIDs["edge-group-country-de"][0] != "edge-shared" {
		t.Fatalf("inactive A polluted healthy B: healthy=%v node_ids=%v", healthy, nodeIDs)
	}

	b.FailureClass = model.EdgeInstanceFailureSignatureInvalid
	if _, err := storeState.UpdateEdgeInstanceHeartbeat(b); err != nil {
		t.Fatalf("record active signature failure: %v", err)
	}
	healthy, _, _, _, err = server.edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("route inventory after active failure: %v", err)
	}
	if healthy["edge-group-country-de"] {
		t.Fatalf("active signature failure must be immediately unavailable: %v", healthy)
	}
}

func TestRouteInventoryFailsUnavailableWhenActiveEpochIsMissing(t *testing.T) {
	t.Parallel()
	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID: "edge-shared", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthUnknown,
	}); err != nil {
		t.Fatalf("create control identity: %v", err)
	}
	instance := model.EdgeNodeInstance{
		EdgeID: "edge-shared", EdgeGroupID: "edge-group-country-de", Slot: model.EdgeSlotB,
		InstanceUID: "pod-b", ReleaseEpoch: "release-b",
		Node: model.EdgeNode{ID: "edge-shared", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthHealthy, Healthy: true, RouteBundleVersion: "route-b", CaddyRouteCount: 2},
	}
	for index := 0; index < 2; index++ {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			t.Fatalf("seed instance heartbeat: %v", err)
		}
	}
	healthy, nodeIDs, expected, minimum, err := server.edgeRouteGroupInventory()
	if err != nil {
		t.Fatalf("missing active epoch must be represented as unavailable, not serving error: %v", err)
	}
	if len(healthy) != 0 || len(nodeIDs) != 0 || len(expected) != 0 || len(minimum) != 0 {
		t.Fatalf("missing active epoch must fail closed: healthy=%v node_ids=%v expected=%v minimum=%v", healthy, nodeIDs, expected, minimum)
	}
}
