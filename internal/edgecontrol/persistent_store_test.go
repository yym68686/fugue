package edgecontrol

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

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
	runtime := ShadowRuntime{
		RouteIntents: staticRouteIntentSource{snapshot: routeIntentFixture()},
		Compiler:     GroupShadowCompiler{Inventory: store, Ledger: store},
		GroupIDs:     []string{groupID},
	}
	first, err := runtime.RunOnce(ctx)
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
	restartedRuntime := ShadowRuntime{
		RouteIntents: staticRouteIntentSource{snapshot: routeIntentFixture()},
		Compiler:     GroupShadowCompiler{Inventory: restarted, Ledger: restarted},
		GroupIDs:     []string{groupID},
	}
	second, err := restartedRuntime.RunOnce(ctx)
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
	runtime := ShadowRuntime{
		RouteIntents: staticRouteIntentSource{snapshot: routeIntentFixture()},
		Compiler:     GroupShadowCompiler{Inventory: store, Ledger: store},
		GroupIDs:     groups,
	}
	first, err := runtime.RunOnce(ctx)
	if err != nil || first.Succeeded != 2 {
		t.Fatalf("initial RunOnce() = %+v, %v", first, err)
	}
	if err := os.WriteFile(store.groupStatePath("edge-group-country-de"), []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt DE fixture: %v", err)
	}

	second, err := runtime.RunOnce(ctx)
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
