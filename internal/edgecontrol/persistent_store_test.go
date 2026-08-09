package edgecontrol

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPersistentGroupStoreCompactsCandidateBundlesWithoutLosingSequence(t *testing.T) {
	t.Parallel()

	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	state := persistentGroupState{Schema: persistentGroupStateSchemaV1, GroupID: groupID, Revision: 1}
	for index := 0; index < retainedGroupCandidateBundles+4; index++ {
		generation := fmt.Sprintf("generation-%02d", index+1)
		bundle := model.EdgeRouteBundle{EdgeGroupID: groupID, Generation: generation, Issuer: groupShadowIssuer}
		entry := GroupShadowLedgerEntry{
			Schema: GroupShadowLedgerSchemaV1, GroupID: groupID, Status: GroupShadowStatusCompiled,
			RouteIntentGeneration: generation, InputDigest: "sha256:" + strings.Repeat(fmt.Sprintf("%x", index%16), 64),
			BundleGeneration: generation, LastSuccessfulBundleGeneration: generation, Authority: "none",
			RecordedAt: time.Date(2026, 8, 7, 0, index, 0, 0, time.UTC), Bundle: &bundle,
		}
		appended, appendErr := prepareGroupShadowLedgerAppend(groupID, uint64(index), state.Ledger, entry)
		if appendErr != nil {
			t.Fatalf("append candidate %d: %v", index+1, appendErr)
		}
		state.Ledger = append(state.Ledger, appended)
	}
	compactPersistentGroupState(&state)
	retained := 0
	for index, entry := range state.Ledger {
		if entry.Bundle != nil {
			retained++
			if index < 4 {
				t.Fatalf("old candidate %d retained its full bundle", index+1)
			}
		} else if !entry.BundleArchived {
			t.Fatalf("candidate %d lost its bundle without an archive marker", index+1)
		}
	}
	if retained != retainedGroupCandidateBundles {
		t.Fatalf("retained bundles=%d want=%d", retained, retainedGroupCandidateBundles)
	}
	state.Revision++
	if err := store.writeGroupState(store.groupStatePath(groupID), state); err != nil {
		t.Fatalf("persist compacted state: %v", err)
	}
	head, exists, err := store.Head(context.Background(), groupID)
	if err != nil || !exists || head.Sequence != uint64(len(state.Ledger)) || head.Bundle == nil || head.BundleArchived {
		t.Fatalf("compacted head=%+v exists=%v err=%v", head, exists, err)
	}
	history, err := store.History(context.Background(), groupID)
	if err != nil || len(history) != len(state.Ledger) || !history[0].BundleArchived || history[0].Bundle != nil {
		t.Fatalf("compacted history did not preserve archived identity: len=%d err=%v first=%+v", len(history), err, history[0])
	}
}

func TestPersistentGroupStoreAcceptsFirstProducerInventoryAfterMissingInventoryLedger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 7, 7, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	nodeID := "vps-84c8f0a9"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	failed, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || failed.Failed != 1 || failed.Results[0].FailureCode != GroupShadowFailureInventoryRead {
		t.Fatalf("missing-inventory reconcile = %+v, %v", failed, err)
	}
	published, err := (GroupAuthorityPublisher{
		Store: store, Signer: &fixtureGroupSigner{keys: map[string][]byte{groupID: []byte("0123456789abcdef0123456789abcdef")}},
		Now: func() time.Time { return now },
	}).Publish(ctx, failed)
	if err != nil || published.Failed != 1 || published.Results[0].FailureCode != GroupShadowFailureInventoryRead {
		t.Fatalf("missing-inventory authority publication = %+v, %v", published, err)
	}
	for _, path := range []string{store.groupStatePath(groupID), store.groupStatePath(groupID) + ".lock"} {
		if err := os.Chmod(path, 0o660); err != nil {
			t.Fatal(err)
		}
	}
	store, err = OpenPersistentGroupStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := authorityInventoryHeartbeatFixture(groupID, nodeID, 0, 1, now, "heartbeat-de-bootstrap-000001")
	heartbeat.Inventory.ActiveEpoch.ReleaseEpoch = strings.Repeat("5", 40)
	heartbeat.Inventory.Instances[0].ReleaseEpoch = heartbeat.Inventory.ActiveEpoch.ReleaseEpoch
	identity := GroupInventoryProducerIdentity{CredentialID: "edge-inventory-producer", TokenID: "token-de-1", NodeID: nodeID, GroupID: groupID}
	stored, err := store.StoreGroupInventoryProducerHeartbeat(ctx, identity, heartbeat, now)
	if err != nil {
		t.Fatalf("store first producer inventory after missing-inventory ledger: %v", err)
	}
	if stored.Sequence != 1 || stored.Generation == "" || len(stored.Instances) != 1 {
		t.Fatalf("stored first producer inventory = %+v", stored)
	}
}

func TestPersistentGroupStoreSurvivesRestartAndPreservesLKG(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-country-us"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", true)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatalf("store initial inventory: %v", err)
	}
	persisted, err := os.ReadFile(store.groupStatePath(groupID))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"token_hash", "token_prefix", "public_ipv4", "last_error", "caddy_"} {
		if strings.Contains(string(persisted), forbidden) {
			t.Fatalf("persistent Edge Control inventory leaked Core field %q", forbidden)
		}
	}
	first, err := reconcileRouteIntents(ctx, staticRouteIntentSource{snapshot: routeIntentFixture()}, GroupShadowCompiler{Inventory: store, Ledger: store}, []string{groupID})
	if err != nil || first.Succeeded != 1 || first.Results[0].BundleGeneration == "" {
		t.Fatalf("initial RunOnce() = %+v, %v", first, err)
	}
	lastSuccess := first.Results[0].BundleGeneration

	restarted, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatalf("reopen persistent store: %v", err)
	}
	restoredInventory, err := restarted.ReadGroupInventory(ctx, groupID)
	if err != nil || restoredInventory.Sequence != 1 || restoredInventory.Generation != inventory.Generation {
		t.Fatalf("restored inventory = %+v, %v", restoredInventory, err)
	}
	restoredHead, exists, err := restarted.Head(ctx, groupID)
	if err != nil || !exists || restoredHead.Sequence != 1 || restoredHead.Bundle == nil || restoredHead.BundleGeneration != lastSuccess {
		t.Fatalf("restored ledger head = %+v, exists=%v, err=%v", restoredHead, exists, err)
	}

	badInventory := inventory
	badInventory.Sequence = 2
	badInventory.Generation = "inventory-us-2"
	badInventory.ActiveEpoch = GroupActiveEpoch{}
	badInventory.Instances = nil
	if err := restarted.StoreGroupInventoryCAS(ctx, groupID, 1, badInventory); err != nil {
		t.Fatalf("store bad group-local inventory: %v", err)
	}
	second, err := reconcileRouteIntents(ctx, staticRouteIntentSource{snapshot: routeIntentFixture()}, GroupShadowCompiler{Inventory: restarted, Ledger: restarted}, []string{groupID})
	if err != nil {
		t.Fatalf("failed group RunOnce() returned global error: %v", err)
	}
	if second.Failed != 1 || second.Results[0].FailureCode != GroupShadowFailureInventoryInvalid || second.Results[0].LastSuccessfulBundleGeneration != lastSuccess {
		t.Fatalf("failed group lost persisted LKG: %+v", second)
	}

	secondRestart, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	finalHead, exists, err := secondRestart.Head(ctx, groupID)
	if err != nil || !exists || finalHead.Sequence != 2 || finalHead.Status != GroupShadowStatusFailed || finalHead.LastSuccessfulBundleGeneration != lastSuccess {
		t.Fatalf("second restart ledger head = %+v, exists=%v, err=%v", finalHead, exists, err)
	}
	history, err := secondRestart.History(ctx, groupID)
	if err != nil || len(history) != 2 || history[0].Bundle == nil || history[1].Bundle != nil {
		t.Fatalf("persistent history = %+v, %v", history, err)
	}
}

func TestPersistentGroupStoreCorruptionIsGroupScoped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	groups := []string{"edge-group-country-us", "edge-group-country-de"}
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatalf("store %s inventory: %v", groupID, err)
		}
	}
	first, err := reconcileRouteIntents(ctx, staticRouteIntentSource{snapshot: routeIntentFixture()}, GroupShadowCompiler{Inventory: store, Ledger: store}, groups)
	if err != nil || first.Succeeded != 2 {
		t.Fatalf("initial RunOnce() = %+v, %v", first, err)
	}
	if err := os.WriteFile(store.groupStatePath("edge-group-country-de"), []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt DE fixture: %v", err)
	}

	second, err := reconcileRouteIntents(ctx, staticRouteIntentSource{snapshot: routeIntentFixture()}, GroupShadowCompiler{Inventory: store, Ledger: store}, groups)
	if err != nil {
		t.Fatalf("group corruption returned global error: %v", err)
	}
	byGroup := shadowResultsByGroup(second.Results)
	if byGroup["edge-group-country-us"].Status != GroupShadowStatusCompiled || byGroup["edge-group-country-us"].LedgerSequence != 1 {
		t.Fatalf("US was contaminated by DE state corruption: %+v", byGroup)
	}
	if byGroup["edge-group-country-de"].FailureCode != GroupShadowFailureLedgerRead {
		t.Fatalf("DE corruption classification = %+v", byGroup["edge-group-country-de"])
	}
	if history, err := store.History(ctx, "edge-group-country-us"); err != nil || len(history) != 1 {
		t.Fatalf("US history after DE corruption = %+v, %v", history, err)
	}
}

func TestPersistentGroupStoreKeysAndRevisionsAreGroupScoped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	groups := []string{"edge-group-country-de", "edge-group-country-us", "edge-group-region-test"}
	paths := make(map[string]string, len(groups))
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatalf("store %s inventory: %v", groupID, err)
		}
		path := store.groupStatePath(groupID)
		for existingGroup, existingPath := range paths {
			if path == existingPath {
				t.Fatalf("groups %s and %s share state key %s", groupID, existingGroup, path)
			}
		}
		paths[groupID] = path
	}

	usGroup := "edge-group-country-us"
	usInventory, err := store.ReadGroupInventory(ctx, usGroup)
	if err != nil {
		t.Fatal(err)
	}
	usInventory.Sequence++
	usInventory.Generation = "inventory-us-2"
	if err := store.StoreGroupInventoryCAS(ctx, usGroup, usInventory.Sequence-1, usInventory); err != nil {
		t.Fatalf("advance US inventory: %v", err)
	}

	for _, groupID := range groups {
		state, err := store.readGroupState(paths[groupID], groupID)
		if err != nil {
			t.Fatalf("read %s state: %v", groupID, err)
		}
		wantRevision := uint64(1)
		if groupID == usGroup {
			wantRevision = 2
		}
		if state.Revision != wantRevision {
			t.Fatalf("%s revision = %d, want %d", groupID, state.Revision, wantRevision)
		}
	}
}

func TestPersistentGroupStoreRejectsInventoryCASRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", false)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); !errors.Is(err, ErrGroupInventoryCASConflict) {
		t.Fatalf("stale inventory CAS error = %v, want %v", err, ErrGroupInventoryCASConflict)
	}
	skipped := inventory
	skipped.Sequence = 3
	skipped.Generation = "inventory-us-3"
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 1, skipped); !errors.Is(err, ErrGroupInventorySequence) {
		t.Fatalf("skipped inventory sequence error = %v, want %v", err, ErrGroupInventorySequence)
	}
}

func TestPersistentGroupStoreAcceptsFSGroupPrivateVolumeAndRejectsWorldAccess(t *testing.T) {
	t.Parallel()

	groupPrivate := t.TempDir()
	if err := os.Chmod(groupPrivate, 0o770); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistentGroupStore(groupPrivate); err != nil {
		t.Fatalf("fsGroup-private state directory rejected: %v", err)
	}
	worldAccessible := t.TempDir()
	if err := os.Chmod(worldAccessible, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistentGroupStore(worldAccessible); err == nil {
		t.Fatal("world-accessible state directory unexpectedly accepted")
	}
}

func TestPersistentGroupStoreRejectsWorldAccessibleStateAndLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotA, "epoch-de-a", "inventory-de-1", false)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	statePath := store.groupStatePath(groupID)
	lockPath := statePath + ".lock"
	if err := os.Chmod(statePath, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadGroupInventory(ctx, groupID); err == nil || !strings.Contains(err.Error(), "private regular file") {
		t.Fatalf("world-accessible state file was accepted: %v", err)
	}
	if err := os.Chmod(statePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(lockPath, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadGroupInventory(ctx, groupID); err == nil || !strings.Contains(err.Error(), "private regular file") {
		t.Fatalf("world-accessible lock file was accepted: %v", err)
	}
}

func TestPersistentGroupStoreRejectsCandidateFromStaleInventory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	first := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", false)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Sequence = 2
	second.Generation = "inventory-us-2"
	reader := advancingInventoryReader{store: store, next: second}
	compiler := GroupShadowCompiler{Inventory: reader, Ledger: store}
	batch, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatalf("stale candidate returned global error: %v", err)
	}
	if batch.Failed != 1 || batch.Results[0].FailureCode != GroupShadowFailureLedgerCAS {
		t.Fatalf("stale inventory candidate result = %+v", batch)
	}
	if history, err := store.History(ctx, groupID); err != nil || len(history) != 0 {
		t.Fatalf("stale inventory candidate entered ledger: %+v, %v", history, err)
	}
	current, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil || current.Sequence != 2 || current.Generation != second.Generation {
		t.Fatalf("advanced inventory = %+v, %v", current, err)
	}
}

type staticRouteIntentSource struct {
	snapshot model.EdgeRouteIntentSnapshot
	err      error
}

type advancingInventoryReader struct {
	store *PersistentGroupStore
	next  GroupInventorySnapshot
}

func (reader advancingInventoryReader) ReadGroupInventory(ctx context.Context, groupID string) (GroupInventorySnapshot, error) {
	current, err := reader.store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		return GroupInventorySnapshot{}, err
	}
	if err := reader.store.StoreGroupInventoryCAS(ctx, groupID, current.Sequence, reader.next); err != nil {
		return GroupInventorySnapshot{}, err
	}
	return current, nil
}

func privateStateDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func (source staticRouteIntentSource) FetchRouteIntents(context.Context) (model.EdgeRouteIntentSnapshot, error) {
	return source.snapshot, source.err
}
