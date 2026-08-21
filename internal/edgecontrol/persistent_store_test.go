package edgecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPersistentGroupStoreMigratesOnlyDigestBoundLegacyCandidateSequence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, _, candidate, current := groupPromotionFixture(t, groupID, now)
	path := store.groupStatePath(groupID)
	state, err := store.readGroupState(path, groupID)
	if err != nil {
		t.Fatal(err)
	}
	state.AuthorityLedger = state.AuthorityLedger[:state.Published.PublicationSequence]
	state.Candidate.AuthorityLedgerSequence = 0
	state.Digest = legacyCandidatePersistentGroupStateDigest(state)
	legacy := legacyCandidatePersistentGroupState{
		Schema: state.Schema, GroupID: state.GroupID, Revision: state.Revision, Inventory: state.Inventory,
		InventoryProducer: state.InventoryProducer, Ledger: state.Ledger, AuthorityLedger: state.AuthorityLedger,
		Published: state.Published, Digest: state.Digest,
	}
	legacyCandidate := state.Candidate
	legacy.Candidate = &legacyGroupCandidateBundle{
		Schema: legacyCandidate.Schema, GroupID: legacyCandidate.GroupID, Epoch: legacyCandidate.Epoch,
		CandidateLedgerSequence: legacyCandidate.CandidateLedgerSequence, RouteIntentGeneration: legacyCandidate.RouteIntentGeneration,
		InventoryGeneration: legacyCandidate.InventoryGeneration, ReleaseRecordDigest: legacyCandidate.ReleaseRecordDigest,
		WorkerSlot: legacyCandidate.WorkerSlot, PublishedAt: legacyCandidate.PublishedAt,
		CurrentRecord: legacyCandidate.CurrentRecord, CurrentBundle: legacyCandidate.CurrentBundle,
		CurrentWorkerSlot: legacyCandidate.CurrentWorkerSlot, Record: legacyCandidate.Record, Bundle: legacyCandidate.Bundle,
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenPersistentGroupStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	migrated, exists, err := restarted.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || migrated.AuthorityLedgerSequence != current.Published.PublicationSequence ||
		migrated.Record.RecordDigest != candidate.Record.RecordDigest {
		t.Fatalf("legacy candidate was not migrated in memory: candidate=%+v exists=%v err=%v", migrated, exists, err)
	}
	migratedState, err := restarted.readGroupState(path, groupID)
	if err != nil {
		t.Fatal(err)
	}
	migratedState.Revision++
	if err := restarted.writeGroupState(path, migratedState); err != nil {
		t.Fatalf("persist migrated state: %v", err)
	}
	persisted, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(persisted), `"authority_ledger_sequence":`) {
		t.Fatalf("migrated state did not persist the new witness: err=%v", err)
	}

	for name, mutate := range map[string]func(*legacyCandidatePersistentGroupState){
		"digest": func(value *legacyCandidatePersistentGroupState) { value.Digest = "sha256:" + strings.Repeat("0", 64) },
		"authority-head": func(value *legacyCandidatePersistentGroupState) {
			value.AuthorityLedger = append(value.AuthorityLedger, GroupAuthorityLedgerEntry{
				Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Sequence: uint64(len(value.AuthorityLedger) + 1),
				Status: GroupAuthorityStatusFailed, CandidateLedgerSequence: current.Published.CandidateLedgerSequence,
				RouteIntentGeneration: "legacy-migration-drift", LastPublishedBundleGeneration: current.Published.Bundle.Generation,
				FailureCode: GroupAuthorityFailureSigning, Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(time.Minute),
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := legacy
			copy.AuthorityLedger = append([]GroupAuthorityLedgerEntry(nil), legacy.AuthorityLedger...)
			mutate(&copy)
			bad, _ := json.Marshal(copy)
			if err := os.WriteFile(path, bad, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := restarted.ReadGroupCandidate(ctx, groupID); err == nil {
				t.Fatalf("legacy %s drift was accepted", name)
			}
		})
	}
}

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

func TestOpenPersistentGroupStoreRemovesStaleStateTemporaries(t *testing.T) {
	root := privateStateDir(t)
	stale := filepath.Join(root, ".group-state-crash.tmp")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistentGroupStore(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale state temporary still exists: %v", err)
	}
}

func TestPersistentGroupStoreRehydratesArchivedPublishedRecoveryTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 16, 1, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, published := groupPromotionFixture(t, groupID, now)
	path := store.groupStatePath(groupID)
	state, err := store.readGroupState(path, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Published == nil || state.Published.Bundle.Generation != candidate.Bundle.Generation {
		t.Fatalf("fixture is not a published candidate: candidate=%+v published=%+v", candidate, state.Published)
	}
	sequence := state.Published.CandidateLedgerSequence
	if sequence == 0 {
		t.Fatalf("published fixture has no candidate sequence: published=%+v", state.Published)
	}
	archived := state.Ledger[sequence-1]
	archived.Bundle = nil
	archived.BundleArchived = true
	state.Ledger[sequence-1] = archived
	state.Candidate = nil
	state.CandidateHistory = nil
	state.Revision++
	if err := store.writeGroupState(path, state); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenPersistentGroupStore(store.root)
	if err != nil {
		t.Fatal(err)
	}
	authority, recovered, _, err := restarted.ReadGroupRecoveryTarget(ctx, groupID, published.Published.Bundle.Generation)
	if err != nil {
		t.Fatalf("archived published recovery target rejected: %v", err)
	}
	if recovered.Bundle == nil || recovered.Bundle.Generation != published.Published.Bundle.Generation ||
		!recovered.BundleArchived || authority.Published.Bundle.Generation != recovered.Bundle.Generation {
		t.Fatalf("archived published recovery target was not rehydrated exactly: authority=%+v candidate=%+v", authority, recovered)
	}
	refreshedAt := published.Published.Bundle.ValidUntil.Add(time.Second)
	bundle := cloneEdgeRouteBundle(*recovered.Bundle)
	bundle.Issuer = groupAuthorityIssuer
	bundle.GeneratedAt = refreshedAt
	bundle.ValidUntil = time.Time{}
	bundle.KeyID = ""
	bundle.Signature = ""
	bundle.Signatures = nil
	bundle.PreviousGeneration = ""
	bundle.Version = groupPublicationVersion(bundle.Generation, authority.LedgerHead.Sequence+1, 1)
	signed, err := signer.SignGroupBundle(ctx, groupID, bundle)
	if err != nil {
		t.Fatal(err)
	}
	entry := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusPublished,
		CandidateLedgerSequence: recovered.Sequence, RouteIntentGeneration: recovered.RouteIntentGeneration,
		InventoryGeneration: recovered.InventoryGeneration, BundleGeneration: signed.Generation,
		LastPublishedBundleGeneration: signed.Generation, PublishedBundleDigest: signedGroupBundleDigest(signed),
		SigningKeyID: signed.KeyID, RecoveryEpoch: 1, RecoveryReason: "test archived published refresh",
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: refreshedAt}
	if _, err := restarted.RecoverGroupAuthorityCAS(ctx, groupID, authority.Published.PublicationSequence, 0, entry, signed); err != nil {
		t.Fatalf("direct archived recovery CAS: %v", err)
	}
	// Repeat from the newly recovered publication to exercise the public
	// maintenance path as production does.
	refreshedAt = signed.ValidUntil.Add(time.Second)
	publisher := GroupAuthorityPublisher{Store: restarted, Signer: signer, Now: func() time.Time { return refreshedAt }}
	result, err := publisher.RefreshPublishedLKG(ctx, groupID, refreshedAt)
	if err != nil || result.Status != GroupAuthorityStatusPublished {
		t.Fatalf("refresh archived published recovery target: result=%+v err=%v", result, err)
	}
	after, err := restarted.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != published.Published.Bundle.Generation ||
		after.Published.RecoveryEpoch != published.Published.RecoveryEpoch+2 || !after.Published.Bundle.ValidUntil.After(refreshedAt) {
		t.Fatalf("archived published recovery target did not advance exactly: after=%+v err=%v", after, err)
	}
}

func TestPersistentGroupStoreCompactsLargeRecoveryWindowBelowDurableLimit(t *testing.T) {
	t.Parallel()

	groupID := "edge-group-country-us"
	state := persistentGroupState{Schema: persistentGroupStateSchemaV1, GroupID: groupID, Revision: 1}
	for index := 0; index < retainedGroupCandidateBundles+2; index++ {
		generation := fmt.Sprintf("generation-%02d", index+1)
		bundle := model.EdgeRouteBundle{
			EdgeGroupID: groupID,
			Generation:  generation,
			Issuer:      groupShadowIssuer,
			Routes: []model.EdgeRouteBinding{{
				Hostname:     "api.example.com",
				StatusReason: strings.Repeat(fmt.Sprintf("%x", index%16), 7<<20),
			}},
		}
		entry := GroupShadowLedgerEntry{
			Schema: GroupShadowLedgerSchemaV1, GroupID: groupID, Status: GroupShadowStatusCompiled,
			RouteIntentGeneration: generation, InputDigest: "sha256:" + strings.Repeat(fmt.Sprintf("%x", index%16), 64),
			BundleGeneration: generation, LastSuccessfulBundleGeneration: generation, Authority: "none",
			RecordedAt: time.Date(2026, 8, 16, 0, index, 0, 0, time.UTC), Bundle: &bundle,
		}
		appended, err := prepareGroupShadowLedgerAppend(groupID, uint64(index), state.Ledger, entry)
		if err != nil {
			t.Fatalf("append candidate %d: %v", index+1, err)
		}
		state.Ledger = append(state.Ledger, appended)
	}
	compactPersistentGroupState(&state)
	if err := compactPersistentGroupStateForSize(&state); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > targetPersistentGroupStateBytes {
		t.Fatalf("compacted state bytes=%d target=%d", len(encoded), targetPersistentGroupStateBytes)
	}
	retained := 0
	for _, entry := range state.Ledger {
		if entry.Bundle != nil {
			retained++
		}
	}
	if retained >= retainedGroupCandidateBundles {
		t.Fatalf("large recovery window retained %d full bundles", retained)
	}
	if state.Ledger[len(state.Ledger)-1].Bundle == nil {
		t.Fatal("newest candidate bundle was archived")
	}
}

func TestPersistentGroupStoreCachesOnlyValidatedCurrentSummary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-country-de"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	first := GroupInventorySnapshot{Schema: GroupInventorySchemaV1, GroupID: groupID, Sequence: 1,
		Generation: "inventory-1", ObservedAt: time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)}
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, first); err != nil {
		t.Fatal(err)
	}
	status, err := store.ReadGroupAuthorityStatus(ctx, groupID)
	if err != nil || !status.InventoryExists || status.Inventory.Sequence != 1 {
		t.Fatalf("initial summary=%+v err=%v", status, err)
	}
	store.summaryMu.RLock()
	summary := store.summaries[groupID]
	store.summaryMu.RUnlock()
	if summary.stage.Inventory.Sequence != 1 || summary.status.Inventory.Sequence != 1 {
		t.Fatalf("validated current projection was not cached: %+v", summary)
	}

	second := cloneGroupInventorySnapshot(first)
	second.Sequence, second.Generation = 2, "inventory-2"
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 1, second); err != nil {
		t.Fatal(err)
	}
	status, err = store.ReadGroupAuthorityStatus(ctx, groupID)
	if err != nil || status.Inventory.Sequence != 2 || status.Inventory.Generation != second.Generation {
		t.Fatalf("write-refreshed summary=%+v err=%v", status, err)
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

func TestPersistentGroupStoreAuthorityStatusSnapshotIsDefensive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	groupID := "edge-group-region-test"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-test-b", "inventory-test-1", false)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}

	first, err := store.ReadGroupAuthorityStatus(ctx, groupID)
	if err != nil || !first.InventoryExists || first.Inventory.Generation != inventory.Generation || first.ProducerExists || first.Authority.LedgerExists || first.Authority.PublishedExists {
		t.Fatalf("first authority status snapshot = %+v, %v", first, err)
	}
	first.Inventory.Generation = "forged"
	first.Inventory.Instances[0].EdgeID = "forged"
	second, err := store.ReadGroupAuthorityStatus(ctx, groupID)
	if err != nil || second.Inventory.Generation != inventory.Generation || second.Inventory.Instances[0].EdgeID != inventory.Instances[0].EdgeID {
		t.Fatalf("authority status snapshot exposed mutable state: %+v, %v", second, err)
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
