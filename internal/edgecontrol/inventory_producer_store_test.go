package edgecontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

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
