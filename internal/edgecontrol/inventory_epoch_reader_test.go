package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestInventoryOriginalEpochEncodingRemainsOptional(t *testing.T) {
	observation := GroupInventoryProducerObservation{NodeID: "node-a"}
	raw, err := json.Marshal(observation)
	if err != nil || bytes.Contains(raw, []byte("serving_epoch")) {
		t.Fatal("absent epoch changed legacy persisted encoding", string(raw), err)
	}
	if err := json.Unmarshal(raw, &observation); err != nil || observation.ServingEpoch != nil {
		t.Fatal("reader invented an original epoch", err)
	}
}

func TestInventoryEpochReaderPreservesPositivePublicationAcrossRestart(t *testing.T) {
	ctx, now, group := context.Background(), time.Now().UTC().Truncate(time.Second), "cell-reader-test"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	h := authorityInventoryHeartbeatFixture(group, "node-a", 0, 1, now, "reader-compatibility-heartbeat")
	id := GroupInventoryProducerIdentity{CredentialID: "credential", TokenID: "token", NodeID: "node-a", GroupID: group}
	if _, err := store.StoreGroupInventoryProducerHeartbeat(ctx, id, h, now); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	signer := &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x56}, 32)}, validFor: time.Hour}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	intent := routeIntentFixture()
	intent.Routes = intent.Routes[:1]
	intent.TLSAllowlist = intent.TLSAllowlist[1:]
	compiled, err := compiler.Reconcile(ctx, intent, []string{group})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatal(compiled, err)
	}
	if published, err := publisher.Publish(ctx, compiled); err != nil || published.Published != 1 {
		t.Fatal(published, err)
	}
	before, err := store.ReadGroupAuthority(ctx, group)
	if err != nil || !before.PublishedExists {
		t.Fatal(err)
	}
	if err := store.withGroupState(ctx, group, true, func(state *persistentGroupState) error {
		epoch := h.Inventory.ActiveEpoch
		state.InventoryProducer.Observations[0].ServingEpoch = &epoch
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	after, err := restarted.ReadGroupAuthority(ctx, group)
	if err != nil || !reflect.DeepEqual(before.Published, after.Published) {
		t.Fatal("extended inventory prevented positive publication recovery", err)
	}
	producer, found, err := restarted.ReadGroupInventoryProducerState(ctx, group)
	if err != nil || !found || !reflect.DeepEqual(producer.Observations[0].ServingEpoch, &h.Inventory.ActiveEpoch) {
		t.Fatal("original epoch was lost", err)
	}
	producer.Observations[0].ServingEpoch.FenceSequence++
	again, _, err := restarted.ReadGroupInventoryProducerState(ctx, group)
	if err != nil || !reflect.DeepEqual(again.Observations[0].ServingEpoch, &h.Inventory.ActiveEpoch) {
		t.Fatal("clone mutated cached original epoch", err)
	}
	for _, change := range []string{"cell", "slot", "code", "domain", "pool", "future fence", "minimum"} {
		t.Run(change, func(t *testing.T) {
			copy := cloneGroupInventoryProducerState(again)
			epoch := copy.Observations[0].ServingEpoch
			switch change {
			case "cell":
				epoch.GroupID = "cell-other"
			case "slot":
				epoch.Slot = "a"
			case "code":
				epoch.ReleaseEpoch = "other"
			case "domain":
				epoch.FaultDomainID = "other-host"
			case "pool":
				epoch.EdgePoolID = "other-pool"
			case "future fence":
				epoch.FenceSequence++
			case "minimum":
				epoch.MinHealthyInstances++
			}
			if err := validateGroupInventoryProducerState(copy, group); err == nil {
				t.Fatal("invalid original epoch accepted")
			}
		})
	}
}
