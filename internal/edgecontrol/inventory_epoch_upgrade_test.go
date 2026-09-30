package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInventoryEpochDeferredWriterKeepsLegacyEncodingAndPositiveLKG(t *testing.T) {
	ctx, now, group := context.Background(), time.Now().UTC().Truncate(time.Second), "cell-format-upgrade"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStoreWithOptions(root, PersistentGroupStoreOptions{DeferInventoryEpochWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := storeInventoryEpoch(t, store, group, "node-a", strings.Repeat("a", 40), 7, 1, now)
	if _, err := validateGroupInventory(group, snapshot, false, now); err != nil {
		t.Fatal("single authenticated legacy envelope lost readiness", err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	publisher := GroupAuthorityPublisher{Store: store, Signer: &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x58}, 32)}, validFor: time.Hour}, Now: func() time.Time { return now }}
	intent := routeIntentFixture()
	intent.Routes, intent.TLSAllowlist = intent.Routes[:1], intent.TLSAllowlist[1:]
	batch, err := compiler.Reconcile(ctx, intent, []string{group})
	if err != nil || batch.Succeeded != 1 {
		t.Fatal(batch, err)
	}
	if result, err := publisher.Publish(ctx, batch); err != nil || result.Published != 1 {
		t.Fatal(result, err)
	}
	before, err := store.ReadGroupServingAuthority(ctx, group)
	if err != nil || !before.PublishedExists {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(store.groupStatePath(group))
	if err != nil || bytes.Contains(raw, []byte("serving_epoch")) || bytes.Contains(raw, []byte("allowLegacy")) {
		t.Fatal("compatibility stage changed legacy encoding", err)
	}

	// The normal writer can recover the old positive state before fresh epoch
	// evidence arrives, but cannot count a missing epoch as a current member.
	writer, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	checkPositive := func(s *PersistentGroupStore) {
		t.Helper()
		after, err := s.ReadGroupServingAuthority(ctx, group)
		if err != nil || !reflect.DeepEqual(before.Published, after.Published) {
			t.Fatal("format transition lost positive LKG", err)
		}
	}
	checkPositive(writer)
	legacy, err := writer.ReadGroupInventory(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateGroupInventory(group, legacy, false, now); err == nil {
		t.Fatal("normal writer inferred a missing original epoch")
	}
	storeInventoryEpoch(t, writer, group, "node-a", strings.Repeat("a", 40), 7, 1, now.Add(time.Second))
	checkPositive(writer)

	// A code rollback to the compatibility stage reads and continues the new
	// format, rather than deleting epoch evidence or invalidating the LKG.
	rollback, err := OpenPersistentGroupStoreWithOptions(root, PersistentGroupStoreOptions{DeferInventoryEpochWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	checkPositive(rollback)
	snapshot = storeInventoryEpoch(t, rollback, group, "node-a", strings.Repeat("a", 40), 7, 1, now.Add(2*time.Second))
	if _, err := validateGroupInventory(group, snapshot, false, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	producer, _, err := rollback.ReadGroupInventoryProducerState(ctx, group)
	if err != nil || producer.Observations[0].ServingEpoch == nil {
		t.Fatal("rollback stripped durable epoch", err)
	}
	checkPositive(rollback)
}

func TestInventoryEpochDeferredWriterRejectsMembershipExpansionWithoutChangingState(t *testing.T) {
	ctx, now, group := context.Background(), time.Now().UTC().Truncate(time.Second), "cell-format-upgrade"
	store, err := OpenPersistentGroupStoreWithOptions(privateStateDir(t), PersistentGroupStoreOptions{DeferInventoryEpochWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := storeInventoryEpoch(t, store, group, "node-a", strings.Repeat("a", 40), 7, 1, now)
	before, err := os.ReadFile(store.groupStatePath(group))
	if err != nil {
		t.Fatal(err)
	}
	for _, minimum := range []int{1, 2} {
		heartbeat := authorityInventoryHeartbeatFixture(group, "node-b", snapshot.Sequence, 2, now, "second-producer-not-admitted")
		heartbeat.Inventory.ActiveEpoch.MinHealthyInstances = minimum
		identity := GroupInventoryProducerIdentity{CredentialID: "credential-b", TokenID: "token-b", NodeID: "node-b", GroupID: group}
		if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, heartbeat, now); err == nil {
			t.Fatal("compatibility writer admitted another producer")
		}
	}
	after, err := os.ReadFile(store.groupStatePath(group))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed admission changed durable state", err)
	}

	// Only the single latest envelope is bound. Older/future/foreign evidence
	// cannot borrow its fence, nor can a wire-decoded inventory enable this mode.
	for name, change := range map[string]func(*GroupInventorySnapshot){
		"older observation":      func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ProducerGeneration-- },
		"different receipt time": func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ObservedAt = now.Add(-time.Second) },
		"new minimum":            func(s *GroupInventorySnapshot) { s.ActiveEpoch.MinHealthyInstances = 2 },
		"new fence":              func(s *GroupInventorySnapshot) { s.ActiveEpoch.FenceSequence++ },
		"older writer":           func(s *GroupInventorySnapshot) { s.verifiedProducer.Generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneGroupInventorySnapshot(snapshot)
			change(&copy)
			if _, bound := inventoryInstanceProducerBound(copy, copy.Instances[0]); bound {
				t.Fatal("unbound observation counted")
			}
		})
	}
	raw, _ := json.Marshal(snapshot)
	var decoded GroupInventorySnapshot
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.allowLegacySingleProducer || decoded.verifiedProducer != nil {
		t.Fatal("untrusted JSON enabled compatibility", err)
	}
	if _, err := validateGroupInventory(group, snapshot, false, now.Add(maxInventoryHeartbeatTTL)); err == nil {
		t.Fatal("stale legacy evidence counted")
	}
}
