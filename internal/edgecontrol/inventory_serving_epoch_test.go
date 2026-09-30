package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInventoryServingEpochRejectsUnboundOrMutatedObservations(t *testing.T) {
	now, group := time.Now().UTC().Truncate(time.Second), "cell-provenance-test"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	storeInventoryEpoch(t, store, group, "node-a", strings.Repeat("a", 40), 7, 2, now)
	snapshot := storeInventoryEpoch(t, store, group, "node-b", strings.Repeat("b", 40), 7, 2, now)
	for name, change := range map[string]func(*GroupInventorySnapshot){
		"missing original epoch": func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ServingEpoch = nil },
		"old fence":              func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ServingEpoch.FenceSequence-- },
		"future fence":           func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ServingEpoch.FenceSequence++ },
		"foreign slot":           func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ServingEpoch.Slot = "a" },
		"minimum":                func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ServingEpoch.MinHealthyInstances-- },
		"foreign cell": func(s *GroupInventorySnapshot) {
			s.verifiedProducer.Observations[0].ServingEpoch.GroupID = "cell-other"
		},
		"code audit": func(s *GroupInventorySnapshot) {
			s.verifiedProducer.Observations[0].ServingEpoch.ReleaseEpoch = "other-code"
		},
		"domain": func(s *GroupInventorySnapshot) {
			s.verifiedProducer.Observations[0].ServingEpoch.FaultDomainID = "other-host"
		},
		"pool": func(s *GroupInventorySnapshot) {
			s.verifiedProducer.Observations[0].ServingEpoch.EdgePoolID = "other-pool"
		},
		"instance":   func(s *GroupInventorySnapshot) { s.Instances[0].InstanceUID = "foreign" },
		"generation": func(s *GroupInventorySnapshot) { s.Generation = "unbound" },
		"expired observation": func(s *GroupInventorySnapshot) {
			s.verifiedProducer.Observations[0].ObservedAt = now.Add(-maxInventoryHeartbeatTTL)
		},
		"future observation": func(s *GroupInventorySnapshot) { s.verifiedProducer.Observations[0].ObservedAt = now.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneGroupInventorySnapshot(snapshot)
			change(&copy)
			if _, err := validateGroupInventory(group, copy, false, now); err == nil {
				t.Fatal("mutated evidence supplied quorum")
			}
		})
	}
	if view, err := validateGroupInventory(group, snapshot, false, now); err != nil || len(view.servingEdgeIDs) != 2 {
		t.Fatal("mutating a read corrupted retained original epochs", view, err)
	}
}

func TestInventoryMissingPersistedEpochWaitsForFreshProducersAndPreservesLKG(t *testing.T) {
	ctx, now, group := context.Background(), time.Now().UTC().Truncate(time.Second), "cell-upgrade-test"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	codeA, codeB := strings.Repeat("a", 40), strings.Repeat("b", 40)
	storeInventoryEpoch(t, store, group, "node-a", codeA, 7, 2, now)
	storeInventoryEpoch(t, store, group, "node-b", codeB, 7, 2, now)
	signer := &fixtureGroupSigner{keys: map[string][]byte{group: bytes.Repeat([]byte{0x45}, 32)}, validFor: time.Hour}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }, InventoryMaxAge: GroupInventoryHeartbeatMaxAge}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	intent := routeIntentFixture()
	intent.Routes = intent.Routes[:1]
	intent.TLSAllowlist = intent.TLSAllowlist[1:]
	compiled, err := compiler.Reconcile(ctx, intent, []string{group})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatal("initial compilation failed", compiled, err)
	}
	if result, err := publisher.Publish(ctx, compiled); err != nil || result.Published != 1 {
		t.Fatal("initial publication failed", result, err)
	}
	before, err := store.ReadGroupAuthority(ctx, group)
	if err != nil || !before.PublishedExists {
		t.Fatal(err)
	}
	// This is the old on-disk schema: the optional original epoch is absent.
	if err := store.withGroupState(ctx, group, true, func(state *persistentGroupState) error {
		for i := range state.InventoryProducer.Observations {
			state.InventoryProducer.Observations[i].ServingEpoch = nil
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store, err = OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal("old state no longer opens", err)
	}
	compiler.Inventory, compiler.Ledger, publisher.Store = store, store, store
	for reports := 0; reports < 2; reports++ {
		if reports == 1 {
			storeInventoryEpoch(t, store, group, "node-a", codeA, 7, 2, now)
		}
		compiled, err = compiler.Reconcile(ctx, intent, []string{group})
		if err != nil || compiled.Failed != 1 {
			t.Fatal("missing original epoch inferred from latest writer", compiled, err)
		}
		if result, err := publisher.Publish(ctx, compiled); err != nil || result.Published != 0 || result.Failed != 1 {
			t.Fatal("incomplete inventory published", result, err)
		}
		after, err := store.ReadGroupAuthority(ctx, group)
		if err != nil || !reflect.DeepEqual(after.Published, before.Published) {
			t.Fatal("waiting for fresh producers changed positive LKG", err)
		}
	}
	storeInventoryEpoch(t, store, group, "node-b", codeB, 7, 2, now)
	compiled, err = compiler.Reconcile(ctx, intent, []string{group})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatal("fresh evidence failed recovery", compiled, err)
	}
	if result, err := publisher.Publish(ctx, compiled); err != nil || result.Failed != 0 {
		t.Fatal("fresh publication did not recover", result, err)
	}
}

func storeInventoryEpoch(t *testing.T, store *PersistentGroupStore, group, node, code string, fence uint64, minimum int, now time.Time) GroupInventorySnapshot {
	t.Helper()
	ctx := context.Background()
	sequence, generation := uint64(0), uint64(0)
	if previous, err := store.ReadGroupInventory(ctx, group); err == nil {
		sequence = previous.Sequence
	}
	if producer, found, err := store.ReadGroupInventoryProducerState(ctx, group); err != nil {
		t.Fatal(err)
	} else if found {
		generation = producer.Generation
	}
	h := authorityInventoryHeartbeatFixture(group, node, sequence, generation+1, now, fmt.Sprintf("original-serving-epoch-%s-%d", node, generation+1))
	h.FaultDomainID, h.EdgePoolID = "host-"+node, "pool-"+node
	h.Inventory.FaultDomainID, h.Inventory.EdgePoolID = h.FaultDomainID, h.EdgePoolID
	h.Inventory.ActiveEpoch.FaultDomainID, h.Inventory.ActiveEpoch.EdgePoolID = h.FaultDomainID, h.EdgePoolID
	h.Inventory.ActiveEpoch.ReleaseEpoch, h.Inventory.ActiveEpoch.FenceSequence, h.Inventory.ActiveEpoch.MinHealthyInstances = code, fence, minimum
	h.Inventory.Instances[0].ReleaseEpoch = code
	h.Inventory.Instances[0].FaultDomainID, h.Inventory.Instances[0].EdgePoolID = h.FaultDomainID, h.EdgePoolID
	id := GroupInventoryProducerIdentity{CredentialID: "credential-" + node, TokenID: "token-" + node, NodeID: node, GroupID: group}
	stored, err := store.StoreGroupInventoryProducerHeartbeat(ctx, id, h, now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ReadGroupInventory(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if groupInventorySemanticDigest(stored) != groupInventorySemanticDigest(snapshot) {
		t.Fatal("heartbeat receipt and compiler disagree about authenticated inventory")
	}
	return snapshot
}

func TestInventoryServingEpochCountsMixedCodeWithoutHeartbeatOrderChurn(t *testing.T) {
	ctx, now, group := context.Background(), time.Now().UTC().Truncate(time.Second), "cell-epoch-test"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	codeA, codeB := strings.Repeat("a", 40), strings.Repeat("b", 40)
	storeInventoryEpoch(t, store, group, "node-a", codeA, 7, 2, now)
	snapshot := storeInventoryEpoch(t, store, group, "node-b", codeB, 7, 2, now)
	view, err := validateGroupInventory(group, snapshot, false, now)
	if err != nil || !reflect.DeepEqual(view.servingEdgeIDs, []string{"node-a", "node-b"}) {
		t.Fatal("mixed code lost physical quorum", view, err)
	}
	digest := groupInventorySemanticDigest(snapshot)
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now.Add(time.Second) }}
	intent := routeIntentFixture()
	intent.Routes = intent.Routes[:1]
	intent.TLSAllowlist = intent.TLSAllowlist[1:]
	initial, err := compiler.Reconcile(ctx, intent, []string{group})
	if err != nil || initial.Succeeded != 1 {
		t.Fatal("initial mixed-code compilation failed", initial, err)
	}
	for _, node := range []string{"node-a", "node-b", "node-a"} {
		code := codeA
		if node == "node-b" {
			code = codeB
		}
		snapshot = storeInventoryEpoch(t, store, group, node, code, 7, 2, now.Add(time.Second))
		if got := groupInventorySemanticDigest(snapshot); got != digest {
			t.Fatal("latest envelope changed semantic identity", node, digest, got)
		}
		compiled, err := compiler.Reconcile(ctx, intent, []string{group})
		if err != nil || compiled.Succeeded != 1 || compiled.Results[0].BundleGeneration != initial.Results[0].BundleGeneration || compiled.Results[0].LedgerSequence != initial.Results[0].LedgerSequence {
			t.Fatal("heartbeat order changed immutable route candidate", compiled, err)
		}
	}
	restarted, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = restarted.ReadGroupInventory(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if view, err := validateGroupInventory(group, snapshot, false, now.Add(2*time.Second)); err != nil || len(view.servingEdgeIDs) != 2 || groupInventorySemanticDigest(snapshot) != digest {
		t.Fatal("restart lost authenticated original epochs", view, err)
	}
	status, err := restarted.ReadGroupAuthorityStatus(ctx, group)
	if err != nil || groupInventorySemanticDigest(status.Inventory) != digest {
		t.Fatal("cached status lost original producer provenance", err)
	}
	stage, err := restarted.ReadGroupCandidateStage(ctx, group)
	if err != nil || groupInventorySemanticDigest(stage.Inventory) != digest {
		t.Fatal("candidate stage lost original producer provenance", err)
	}
	raw, _ := json.Marshal(snapshot)
	var network GroupInventorySnapshot
	if err := json.Unmarshal(raw, &network); err != nil {
		t.Fatal(err)
	}
	if _, err := validateGroupInventory(group, network, false, now.Add(2*time.Second)); err == nil {
		t.Fatal("network copy supplied mixed-code quorum without local provenance")
	}
	if _, err := validateGroupInventory(group, snapshot, false, now.Add(maxInventoryHeartbeatTTL+2*time.Second)); err == nil {
		t.Fatal("reading retained snapshot renewed producer evidence")
	}
	// A genuine per-node code update remains an observable inventory change.
	snapshot = storeInventoryEpoch(t, store, group, "node-a", strings.Repeat("c", 40), 7, 2, now.Add(3*time.Second))
	if _, err := validateGroupInventory(group, snapshot, false, now.Add(3*time.Second)); err != nil || groupInventorySemanticDigest(snapshot) == digest {
		t.Fatal("new node revision was hidden or lost quorum", err)
	}
}

func TestInventoryServingEpochRejectsOtherMembersOriginalFence(t *testing.T) {
	now, group := time.Now().UTC().Truncate(time.Second), "cell-fence-test"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	code := strings.Repeat("a", 40)
	storeInventoryEpoch(t, store, group, "node-a", code, 7, 2, now)
	snapshot := storeInventoryEpoch(t, store, group, "node-b", code, 8, 2, now.Add(time.Second))
	if _, err := validateGroupInventory(group, snapshot, false, now.Add(time.Second)); err == nil {
		t.Fatal("fence7 evidence counted toward fence8 quorum")
	}
	snapshot = storeInventoryEpoch(t, store, group, "node-a", code, 8, 2, now.Add(2*time.Second))
	if view, err := validateGroupInventory(group, snapshot, false, now.Add(2*time.Second)); err != nil || len(view.servingEdgeIDs) != 2 {
		t.Fatal("fresh original-fence observations did not restore quorum", view, err)
	}
}
