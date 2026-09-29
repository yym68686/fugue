package edgecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAggregatedBootstrapBindsAuthenticatedProducerObservations(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	group := "cell-bootstrap-test"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range []string{"node-one", "node-two"} {
		h := authorityInventoryHeartbeatFixture(group, node, uint64(i), uint64(i+1), now, "bootstrap-authenticated-"+node)
		serving := false
		h.Inventory.ActiveEpoch.MinHealthyInstances = 2
		instance := &h.Inventory.Instances[0]
		instance.EffectiveHealthy, instance.ServingHealthy = false, &serving
		instance.BootstrapEligibility = &GroupBootstrapEligibility{GroupID: group, ReleaseEpoch: instance.ReleaseEpoch, ProducerGeneration: uint64(i + 1), ValidUntil: now.Add(time.Minute)}
		id := GroupInventoryProducerIdentity{CredentialID: "credential-" + node, TokenID: "token-" + node, NodeID: node, GroupID: group}
		if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, id, h, now); err != nil {
			t.Fatal(err)
		}
	}
	restarted, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.ReadGroupInventory(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	view, err := validateGroupInventory(group, snapshot, true, now.Add(time.Second))
	if err != nil || len(view.bootstrapEdgeIDs) != 2 || len(view.servingEdgeIDs) != 0 {
		t.Fatalf("authenticated aggregation lost bootstrap: %+v, %v", view, err)
	}
	if _, err := validateGroupInventory(group, snapshot, false, now); err == nil {
		t.Fatal("bootstrap was usable after the first publication")
	}
	if _, err := validateGroupInventory(group, snapshot, true, now.Add(time.Minute)); err == nil {
		t.Fatal("read extended the original bootstrap deadline")
	}
	for _, mutate := range []func(*GroupInventorySnapshot){
		func(s *GroupInventorySnapshot) { s.Instances[0].BootstrapEligibility.ProducerGeneration++ },
		func(s *GroupInventorySnapshot) { s.Instances[0].InstanceUID = "replacement" },
		func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].Instance.InstanceUID = "foreign" },
		func(s *GroupInventorySnapshot) { s.Generation = "inventory-tampered" },
	} {
		changed := cloneGroupInventorySnapshot(snapshot)
		mutate(&changed)
		if _, err := validateGroupInventory(group, changed, true, now); err == nil {
			t.Fatal("changed aggregate or producer provenance authorized bootstrap")
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var networkCopy GroupInventorySnapshot
	if err := json.Unmarshal(raw, &networkCopy); err != nil {
		t.Fatal(err)
	}
	if networkCopy.verifiedProducer != nil {
		t.Fatal("local producer provenance leaked into the wire format")
	}
	if _, err := validateGroupInventory(group, networkCopy, true, now); err == nil {
		t.Fatal("an aggregate from outside the authenticated store authorized bootstrap")
	}
	if _, err := validateGroupInventory(group, snapshot, true, now); err != nil {
		t.Fatal("mutating a cloned read changed the original provenance")
	}
}

func TestInventoryProducerAcceptsReleaseAuditChangeAtSameServingEpoch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 16, 23, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	nodeID := "vps-84c8f0a9"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	identity := GroupInventoryProducerIdentity{CredentialID: "edge-inventory-producer", TokenID: "token-de-1", NodeID: nodeID, GroupID: groupID}
	first := authorityInventoryHeartbeatFixture(groupID, nodeID, 0, 1, now, "release-audit-first-0001")
	first.Inventory.ActiveEpoch.ReleaseEpoch = strings.Repeat("1", 40)
	first.Inventory.Instances[0].ReleaseEpoch = first.Inventory.ActiveEpoch.ReleaseEpoch
	if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, first, now); err != nil {
		t.Fatalf("store first heartbeat: %v", err)
	}

	second := authorityInventoryHeartbeatFixture(groupID, nodeID, 1, 2, now.Add(time.Second), "release-audit-second-0002")
	second.Inventory.ActiveEpoch.ReleaseEpoch = strings.Repeat("2", 40)
	second.Inventory.Instances[0].ReleaseEpoch = second.Inventory.ActiveEpoch.ReleaseEpoch
	stored, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, second, now.Add(time.Second))
	if err != nil {
		t.Fatalf("store same serving epoch with new release audit: %v", err)
	}
	if stored.Sequence != 2 || stored.ActiveEpoch.ReleaseEpoch != second.Inventory.ActiveEpoch.ReleaseEpoch ||
		len(stored.Instances) != 1 || stored.Instances[0].ReleaseEpoch != second.Inventory.ActiveEpoch.ReleaseEpoch {
		t.Fatalf("stored inventory did not advance release audit: %+v", stored)
	}
	producer, exists, err := store.ReadGroupInventoryProducerState(ctx, groupID)
	if err != nil || !exists || producer.Generation != 2 || producer.ActiveEpoch.ReleaseEpoch != second.Inventory.ActiveEpoch.ReleaseEpoch {
		t.Fatalf("producer state did not advance release audit: state=%+v exists=%t err=%v", producer, exists, err)
	}
}

func TestInventoryProducerRejectsSlotChangeAtSameServingFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 16, 23, 5, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	nodeID := "vps-84c8f0a9"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	identity := GroupInventoryProducerIdentity{CredentialID: "edge-inventory-producer", TokenID: "token-de-1", NodeID: nodeID, GroupID: groupID}
	first := authorityInventoryHeartbeatFixture(groupID, nodeID, 0, 1, now, "serving-fence-first-0001")
	if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, first, now); err != nil {
		t.Fatalf("store first heartbeat: %v", err)
	}

	second := authorityInventoryHeartbeatFixture(groupID, nodeID, 1, 2, now.Add(time.Second), "serving-fence-second-0002")
	second.Inventory.ActiveEpoch.Slot = model.EdgeSlotA
	second.Inventory.Instances[0].Slot = model.EdgeSlotA
	if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, second, now.Add(time.Second)); !errors.Is(err, ErrGroupInventoryProducerEpoch) {
		t.Fatalf("same-fence slot change error = %v, want %v", err, ErrGroupInventoryProducerEpoch)
	}
}
