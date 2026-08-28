package edgecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func TestGroupAuthorityPublishesAndPreservesLKGWithoutCrossGroupTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 5, 2, 0, 0, 0, time.UTC)
	groups := []string{"edge-group-country-de", "edge-group-country-us"}
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, "b", "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatal(err)
		}
	}

	signingDir := privateFixtureDir(t)
	writeGroupSigningFixture(t, signingDir, groups[1], bytes.Repeat([]byte{0x22}, 32), now)
	if err := os.WriteFile(filepath.Join(signingDir, groups[0]+".json"), []byte("{malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := NewProjectedGroupBundleSigner(signingDir, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), groups)
	if err != nil {
		t.Fatal(err)
	}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	first, err := publisher.Publish(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	byGroup := authorityResultsByGroup(first.Results)
	if first.Published != 1 || first.Failed != 1 || byGroup[groups[0]].FailureCode != GroupAuthorityFailureSigning || byGroup[groups[1]].Status != GroupAuthorityStatusPublished {
		t.Fatalf("isolated signing batch = %+v", first)
	}
	de, err := store.ReadGroupAuthority(ctx, groups[0])
	if err != nil || de.PublishedExists || !de.LedgerExists || de.LedgerHead.Status != GroupAuthorityStatusFailed {
		t.Fatalf("DE authority state = %+v, %v", de, err)
	}
	us, err := store.ReadGroupAuthority(ctx, groups[1])
	if err != nil || !us.PublishedExists || us.Published.Bundle.EdgeGroupID != groups[1] || us.Published.Bundle.KeyID == "" {
		t.Fatalf("US authority state = %+v, %v", us, err)
	}
	if err := bundleauth.VerifyEdgeRouteBundle(us.Published.Bundle, string(bytes.Repeat([]byte{0x22}, 32)), us.Published.Bundle.KeyID, now); err != nil {
		t.Fatalf("US published bundle signature: %v", err)
	}
	statusHandler, err := NewAuthorityStatusHandler(store, groups, NewAuthorityRuntimeState(func() time.Time { return now }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	globalReady := httptest.NewRecorder()
	statusHandler.ServeHTTP(globalReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if globalReady.Code != http.StatusOK || !strings.Contains(globalReady.Body.String(), `"status":"ready"`) {
		t.Fatalf("healthy US was removed by unavailable DE: status=%d body=%s", globalReady.Code, globalReady.Body.String())
	}
	authorityStatus := httptest.NewRecorder()
	statusHandler.ServeHTTP(authorityStatus, httptest.NewRequest(http.MethodGet, AuthorityStatusPathV1, nil))
	if authorityStatus.Code != http.StatusOK || !strings.Contains(authorityStatus.Body.String(), `"status":"degraded"`) || !strings.Contains(authorityStatus.Body.String(), `"serving_ready":true`) {
		t.Fatalf("partial group authority status=%d body=%s", authorityStatus.Code, authorityStatus.Body.String())
	}
	for groupID, wantCode := range map[string]int{groups[0]: http.StatusServiceUnavailable, groups[1]: http.StatusOK} {
		recorder := httptest.NewRecorder()
		statusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, authorityGroupReadyPath(groupID), nil))
		if recorder.Code != wantCode {
			t.Fatalf("group %s readiness=%d want=%d body=%s", groupID, recorder.Code, wantCode, recorder.Body.String())
		}
	}

	writeGroupSigningFixture(t, signingDir, groups[0], bytes.Repeat([]byte{0x11}, 32), now)
	now = now.Add(time.Minute)
	secondIntent := routeIntentFixture()
	secondIntent.Generation = "route-intents-43"
	secondIntent.Routes[0].Generation = "route-all-2"
	compiled, err = compiler.Reconcile(ctx, secondIntent, groups)
	if err != nil {
		t.Fatal(err)
	}
	second, err := publisher.Publish(ctx, compiled)
	if err != nil || second.Published != 2 || second.Failed != 0 {
		t.Fatalf("second publication = %+v, %v", second, err)
	}
	for _, groupID := range groups {
		state, err := store.ReadGroupAuthority(ctx, groupID)
		if err != nil || !state.PublishedExists || state.LedgerHead.Sequence != 2 || state.Published.PublicationSequence != 2 || state.Published.Bundle.EdgeGroupID != groupID {
			t.Fatalf("group %s did not advance independently: %+v, %v", groupID, state, err)
		}
		for _, route := range state.Published.Bundle.Routes {
			if route.EdgeGroupID != groupID || route.SelectedEdgeGroup != groupID {
				t.Fatalf("group %s publication contains cross-group route: %+v", groupID, route)
			}
		}
	}
}

func TestGroupAuthorityConsumesBootstrapEligibilityOnlyForFirstPublicationAndRestartsFromLKG(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 10, 14, 40, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	root := privateStateDir(t)
	store, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	serving := false
	faultDomainID := "fault-domain-primary"
	edgePoolID := "edge-pool-public"
	bootstrapInventory := GroupInventorySnapshot{
		Schema: GroupInventorySchemaV1, GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Sequence: 1, Generation: ProducerInventoryEnvelopeGeneration(1), ObservedAt: now,
		ActiveEpoch: GroupActiveEpoch{GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Slot: "a", ReleaseEpoch: "worker-epoch-1", FenceSequence: 1, MinHealthyInstances: 1},
		Instances: []GroupInstance{{
			EdgeID: "edge-us-1", GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Slot: "a", InstanceUID: "worker-us-1", ReleaseEpoch: "worker-epoch-1",
			ServingHealthy: &serving, NodeHealthy: true, NodeStatus: model.EdgeHealthHealthy,
			BootstrapEligibility: &GroupBootstrapEligibility{GroupID: groupID, ReleaseEpoch: "worker-epoch-1", ProducerGeneration: 1, ValidUntil: now.Add(time.Minute)},
		}},
	}
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, bootstrapInventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }, InventoryMaxAge: GroupInventoryHeartbeatMaxAge}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Succeeded != 1 || compiled.Results[0].Status != GroupShadowStatusCompiled {
		t.Fatalf("first bootstrap compilation = %+v, %v", compiled, err)
	}
	head, exists, err := store.Head(ctx, groupID)
	if err != nil || !exists || head.ActiveHealthyInstances != 0 || head.ActiveBootstrapInstances != 1 {
		t.Fatalf("bootstrap and serving evidence were conflated: %+v exists=%t err=%v", head, exists, err)
	}
	key := bytes.Repeat([]byte{0x31}, 32)
	publisher := GroupAuthorityPublisher{Store: store, Signer: &fixtureGroupSigner{keys: map[string][]byte{groupID: key}, validFor: 30 * time.Minute}, Now: func() time.Time { return now }}
	published, err := publisher.Publish(ctx, compiled)
	if err != nil || published.Published != 1 || published.Failed != 0 {
		t.Fatalf("first bootstrap publication = %+v, %v", published, err)
	}
	firstState, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !firstState.PublishedExists {
		t.Fatalf("first group LKG missing: %+v, %v", firstState, err)
	}

	now = now.Add(10 * time.Second)
	bootstrapInventory.Sequence = 2
	bootstrapInventory.Generation = ProducerInventoryEnvelopeGeneration(2)
	bootstrapInventory.ObservedAt = now
	bootstrapInventory.Instances[0].BootstrapEligibility.ProducerGeneration = 2
	bootstrapInventory.Instances[0].BootstrapEligibility.ValidUntil = now.Add(time.Minute)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 1, bootstrapInventory); err != nil {
		t.Fatal(err)
	}
	compiled, err = compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Failed != 1 || compiled.Results[0].FailureCode != GroupShadowFailureNoHealthyActive {
		t.Fatalf("bootstrap eligibility was reused after first publication: %+v, %v", compiled, err)
	}
	failed, err := publisher.Publish(ctx, compiled)
	if err != nil || failed.Published != 0 || failed.Failed != 1 {
		t.Fatalf("post-bootstrap failure was not recorded: %+v, %v", failed, err)
	}

	// Once the first publication exists, bootstrap eligibility is never
	// consumed again. If that signed bundle later expires, Control refreshes
	// the exact persisted LKG instead of compiling current intent from a
	// non-serving worker inventory.
	now = now.Add(31 * time.Minute)
	bootstrapInventory.Sequence = 3
	bootstrapInventory.Generation = ProducerInventoryEnvelopeGeneration(3)
	bootstrapInventory.ObservedAt = now
	bootstrapInventory.Instances[0].BootstrapEligibility.ProducerGeneration = 3
	bootstrapInventory.Instances[0].BootstrapEligibility.ValidUntil = now.Add(time.Minute)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 2, bootstrapInventory); err != nil {
		t.Fatal(err)
	}
	compiled, err = compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Failed != 1 || compiled.Results[0].FailureCode != GroupShadowFailureNoHealthyActive {
		t.Fatalf("expired LKG precondition = %+v, %v", compiled, err)
	}
	refreshed, err := publisher.Publish(ctx, compiled)
	if err != nil || refreshed.Published != 1 || refreshed.Failed != 0 {
		t.Fatalf("persisted LKG refresh = %+v, %v", refreshed, err)
	}

	restarted, err := OpenPersistentGroupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	restartedState, err := restarted.ReadGroupAuthority(ctx, groupID)
	if err != nil || !restartedState.PublishedExists || restartedState.Published.Digest == firstState.Published.Digest ||
		restartedState.Published.Bundle.Generation != firstState.Published.Bundle.Generation || restartedState.Published.RecoveryEpoch != 1 ||
		!restartedState.Published.Bundle.ValidUntil.After(now) {
		t.Fatalf("restarted control lost its signed group LKG: %+v, %v", restartedState, err)
	}
	statusHandler, err := NewAuthorityStatusHandler(restarted, []string{groupID}, NewAuthorityRuntimeState(func() time.Time { return now }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	statusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, authorityGroupReadyPath(groupID), nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"status":"publication_ready"`) ||
		!strings.Contains(recorder.Body.String(), `"ready":false`) {
		t.Fatalf("refreshed LKG was confused with serving health: status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	serving = true
	bootstrapInventory.Sequence = 4
	bootstrapInventory.Generation = ProducerInventoryEnvelopeGeneration(4)
	bootstrapInventory.ObservedAt = now.Add(time.Second)
	bootstrapInventory.Instances[0].ServingHealthy = &serving
	bootstrapInventory.Instances[0].EffectiveHealthy = true
	bootstrapInventory.Instances[0].BootstrapEligibility = nil
	if err := restarted.StoreGroupInventoryCAS(ctx, groupID, 3, bootstrapInventory); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	statusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, authorityGroupReadyPath(groupID), nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"status":"ready"`) ||
		!strings.Contains(recorder.Body.String(), `"ready":true`) {
		t.Fatalf("worker bundle sync did not establish serving health: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestGroupAuthorityReconcileIsIdempotentUntilSignatureRefreshWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 5, 2, 45, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "b", "epoch-us-b", "inventory-us-1", false)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x29}, 32)}, validFor: 30 * time.Minute}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	for iteration := 0; iteration < 2; iteration++ {
		compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
		if err != nil {
			t.Fatal(err)
		}
		if published, err := publisher.Publish(ctx, compiled); err != nil || published.Published != 1 || published.Failed != 0 {
			t.Fatalf("iteration %d publication=%+v err=%v", iteration, published, err)
		}
		now = now.Add(time.Minute)
	}
	shadowHistory, _ := store.History(ctx, groupID)
	authorityHistory, _ := store.AuthorityHistory(ctx, groupID)
	if len(shadowHistory) != 1 || len(authorityHistory) != 1 {
		t.Fatalf("unchanged reconcile grew durable ledgers: shadow=%d authority=%d", len(shadowHistory), len(authorityHistory))
	}
}

func TestGroupAuthorityConfigurationPublicationRetiresSupersededWorkerCandidate(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 5, 35, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, before := groupPromotionFixture(t, groupID, now)
	if candidate.Bundle.Generation != before.Published.Bundle.Generation {
		t.Fatalf("fixture candidate generation=%s published=%s", candidate.Bundle.Generation, before.Published.Bundle.Generation)
	}

	inventory, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Sequence++
	inventory.Generation = "inventory-configuration-successor"
	inventory.ObservedAt = now.Add(2 * time.Minute)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	intent := routeIntentFixture()
	intent.Generation = "route-intents-configuration-successor"
	intent.Routes[0].Generation = "route-configuration-successor"
	compiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now.Add(2 * time.Minute) }}).Reconcile(ctx, intent, []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("compile configuration successor: batch=%+v err=%v", compiled, err)
	}
	published, err := (GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now.Add(2 * time.Minute) }}).Publish(ctx, compiled)
	if err != nil || published.Published != 1 || published.Failed != 0 {
		t.Fatalf("publish configuration successor: batch=%+v err=%v", published, err)
	}

	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation == before.Published.Bundle.Generation ||
		after.Published.CandidateLedgerSequence != compiled.Results[0].LedgerSequence {
		t.Fatalf("configuration authority did not advance: before=%+v after=%+v err=%v", before, after, err)
	}
	if _, exists, err := store.ReadGroupCandidate(ctx, groupID); err != nil || exists {
		t.Fatalf("superseded Worker candidate still blocks configuration authority: exists=%t err=%v", exists, err)
	}
	persisted, err := store.readGroupState(store.groupStatePath(groupID), groupID)
	if err != nil || len(persisted.CandidateHistory) != 0 {
		t.Fatalf("superseded Worker candidate history was retained: count=%d err=%v", len(persisted.CandidateHistory), err)
	}
}

func TestGroupAuthorityClassifiesPublishedLKGRecoveryFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 18, 1, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-de-healthy", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("seed compile: batch=%+v err=%v", compiled, err)
	}
	goodSigner := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x44}, 32)}, validFor: 30 * time.Minute}
	if batch, err := (GroupAuthorityPublisher{Store: store, Signer: goodSigner, Now: func() time.Time { return now }}).Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed publication: batch=%+v err=%v", batch, err)
	}

	now = now.Add(31 * time.Minute)
	serving := false
	inventory.Sequence++
	inventory.Generation = "inventory-de-not-serving"
	inventory.ObservedAt = now
	inventory.Instances[0].ServingHealthy = &serving
	inventory.Instances[0].EffectiveHealthy = false
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	compiled, err = compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Failed != 1 || compiled.Results[0].FailureCode != GroupShadowFailureNoHealthyActive {
		t.Fatalf("recovery precondition: batch=%+v err=%v", compiled, err)
	}
	badSigner := failingGroupSigner{}
	result, err := (GroupAuthorityPublisher{Store: store, Signer: badSigner, Now: func() time.Time { return now }}).Publish(ctx, compiled)
	if err != nil || result.Failed != 1 || result.Results[0].FailureCode != GroupAuthorityFailureSigning {
		t.Fatalf("recovery failure classification: batch=%+v err=%v", result, err)
	}
}

func TestGroupAuthorityClassifiesPublishedLKGValidationFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 18, 1, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-de-healthy", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("seed compile: batch=%+v err=%v", compiled, err)
	}
	goodSigner := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x44}, 32)}, validFor: 30 * time.Minute}
	if batch, err := (GroupAuthorityPublisher{Store: store, Signer: goodSigner, Now: func() time.Time { return now }}).Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed publication: batch=%+v err=%v", batch, err)
	}

	now = now.Add(31 * time.Minute)
	serving := false
	inventory.Sequence++
	inventory.Generation = "inventory-de-not-serving"
	inventory.ObservedAt = now
	inventory.Instances[0].ServingHealthy = &serving
	inventory.Instances[0].EffectiveHealthy = false
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	compiled, err = compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Failed != 1 {
		t.Fatalf("recovery precondition: batch=%+v err=%v", compiled, err)
	}
	cases := []struct {
		name string
		err  error
		code string
	}{
		{name: "published pointer", err: ErrGroupAuthorityPublishedPointerCAS, code: GroupAuthorityFailurePublishedPointerCAS},
		{name: "recovery epoch", err: ErrGroupAuthorityRecoveryEpochCAS, code: GroupAuthorityFailureRecoveryEpochCAS},
		{name: "audit tail", err: ErrGroupAuthorityAuditTailCAS, code: GroupAuthorityFailureAuditTailCAS},
		{name: "validation", err: errors.New("wrapped publication validation failure"), code: GroupAuthorityFailurePublicationValidation},
	}
	for _, testCase := range cases {
		wrappedStore := &validationFailureRecoveryStore{PersistentGroupStore: store, err: testCase.err}
		result, err := (GroupAuthorityPublisher{Store: wrappedStore, Signer: goodSigner, Now: func() time.Time { return now }}).Publish(ctx, compiled)
		if err != nil || result.Failed != 1 || result.Results[0].FailureCode != testCase.code {
			t.Fatalf("%s failure classification: batch=%+v err=%v", testCase.name, result, err)
		}
	}
}

type validationFailureRecoveryStore struct {
	*PersistentGroupStore
	err error
}

func (store *validationFailureRecoveryStore) RecoverPublishedLKG(context.Context, string, uint64, uint64, string, model.EdgeRouteBundle, string, time.Time) (GroupAuthorityLedgerEntry, error) {
	return GroupAuthorityLedgerEntry{}, store.err
}

func TestGroupCompilerAndPublisherLetHealthyGroupProgressWhilePeerStalls(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 5, 2, 55, 0, 0, time.UTC)
	de, us := "edge-group-country-de", "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, groupID := range []string{de, us} {
		inventory := groupInventoryFixture(groupID, "b", "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatal(err)
		}
	}

	readerRelease := make(chan struct{})
	compiler := GroupShadowCompiler{
		Inventory: &blockingGroupInventoryReader{delegate: store, blockedGroup: de, release: readerRelease},
		Ledger:    store, Now: func() time.Time { return now },
	}
	compileDone := make(chan GroupShadowBatch, 1)
	go func() {
		batch, _ := compiler.Reconcile(ctx, routeIntentFixture(), []string{de, us})
		compileDone <- batch
	}()
	waitForCondition(t, func() bool {
		history, err := store.History(ctx, us)
		return err == nil && len(history) == 1
	}, "US compiler progress while DE inventory read is stalled")
	close(readerRelease)
	compiled := <-compileDone
	if compiled.Succeeded != 2 {
		t.Fatalf("compiled batch after release=%+v", compiled)
	}

	signerRelease := make(chan struct{})
	signer := &blockingGroupSigner{
		fixtureGroupSigner: fixtureGroupSigner{keys: map[string][]byte{
			de: bytes.Repeat([]byte{0x61}, 32), us: bytes.Repeat([]byte{0x62}, 32),
		}, validFor: time.Hour},
		blockedGroup: de, release: signerRelease,
	}
	publishDone := make(chan GroupAuthorityBatch, 1)
	go func() {
		batch, _ := (GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}).Publish(ctx, compiled)
		publishDone <- batch
	}()
	waitForCondition(t, func() bool {
		state, err := store.ReadGroupAuthority(ctx, us)
		return err == nil && state.PublishedExists
	}, "US publication progress while DE signer is stalled")
	close(signerRelease)
	published := <-publishDone
	if published.Published != 2 || published.Failed != 0 {
		t.Fatalf("published batch after release=%+v", published)
	}
}

func TestGroupAuthorityFailureServesOnlyThatGroupsDurableLKGAfterRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 5, 3, 0, 0, 0, time.UTC)
	groups := []string{"edge-group-country-de", "edge-group-country-us"}
	stateDir := privateStateDir(t)
	store, err := OpenPersistentGroupStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, "b", "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatal(err)
		}
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{
		groups[0]: bytes.Repeat([]byte{0x31}, 32), groups[1]: bytes.Repeat([]byte{0x32}, 32),
	}, validFor: time.Hour}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), groups)
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := publisher.Publish(ctx, compiled); err != nil || batch.Published != 2 {
		t.Fatalf("initial publication = %+v, %v", batch, err)
	}
	usBefore, _ := store.ReadGroupAuthority(ctx, groups[1])
	deBefore, _ := store.ReadGroupAuthority(ctx, groups[0])

	now = now.Add(time.Minute)
	failingInventory := &selectiveInventoryReader{delegate: store, failures: map[string]bool{groups[1]: true}}
	compiler.Inventory = failingInventory
	secondIntent := routeIntentFixture()
	secondIntent.Generation = "route-intents-43"
	secondIntent.Routes[0].Generation = "route-all-2"
	compiled, err = compiler.Reconcile(ctx, secondIntent, groups)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := publisher.Publish(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	results := authorityResultsByGroup(batch.Results)
	if results[groups[1]].Status != GroupAuthorityStatusFailed || results[groups[0]].Status != GroupAuthorityStatusPublished {
		t.Fatalf("group failure contaminated publication: %+v", batch)
	}
	usAfter, _ := store.ReadGroupAuthority(ctx, groups[1])
	deAfter, _ := store.ReadGroupAuthority(ctx, groups[0])
	if !usAfter.PublishedExists || usAfter.Published.Digest != usBefore.Published.Digest || usAfter.LedgerHead.Status != GroupAuthorityStatusFailed ||
		deAfter.Published.Digest == deBefore.Published.Digest || deAfter.LedgerHead.Status != GroupAuthorityStatusPublished {
		t.Fatalf("group-local LKG transition: DE before=%+v after=%+v US before=%+v after=%+v", deBefore, deAfter, usBefore, usAfter)
	}

	restarted, err := OpenPersistentGroupStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := NewAuthorityRuntimeState(func() time.Time { return now })
	statusHandler, err := NewAuthorityStatusHandler(restarted, groups, runtimeState, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	statusHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, AuthorityStatusPathV1, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("restart readiness=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var status AuthorityStatusSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	statusByGroup := authorityStatusByGroup(status.Groups)
	if status.Status != "degraded" || !status.Ready || !status.ServingReady || statusByGroup[groups[1]].Status != GroupAuthorityHealthServingLKG ||
		statusByGroup[groups[1]].LKGState != GroupAuthorityLKGPreserved || statusByGroup[groups[0]].Status != GroupAuthorityHealthReady {
		t.Fatalf("restart group-aware status = %+v", status)
	}
}

func TestGroupAuthorityRefreshesExpiredPublishedLKGWhenInventoryHeartbeatIsStale(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-de-healthy", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }, InventoryMaxAge: GroupInventoryHeartbeatMaxAge}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("seed compile: batch=%+v err=%v", compiled, err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x45}, 32)}, validFor: 30 * time.Minute}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := publisher.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed publication: batch=%+v err=%v", batch, err)
	}
	before, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !before.PublishedExists {
		t.Fatalf("seed authority: state=%+v err=%v", before, err)
	}

	now = now.Add(31 * time.Minute)
	compiled, err = compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Failed != 1 || compiled.Results[0].FailureCode != GroupShadowFailureInventoryInvalid {
		t.Fatalf("stale inventory precondition: batch=%+v err=%v", compiled, err)
	}
	refreshed, err := publisher.Publish(ctx, compiled)
	if err != nil || refreshed.Published != 1 || refreshed.Failed != 0 {
		t.Fatalf("expired LKG refresh: batch=%+v err=%v", refreshed, err)
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !after.PublishedExists || after.Published.Bundle.Generation != before.Published.Bundle.Generation ||
		after.Published.Digest == before.Published.Digest || after.Published.RecoveryEpoch != before.Published.RecoveryEpoch+1 ||
		!after.Published.Bundle.ValidUntil.After(now) {
		t.Fatalf("refresh changed or lost the exact durable LKG: before=%+v after=%+v err=%v", before.Published, after.Published, err)
	}
}

func TestGroupBundleReaderAndRecoveryAreAuthenticatedGroupScopedCAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 5, 4, 0, 0, 0, time.UTC)
	groups := []string{"edge-group-country-de", "edge-group-country-us"}
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, "b", "epoch-"+groupID+"-b", "inventory-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatal(err)
		}
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{
		groups[0]: bytes.Repeat([]byte{0x41}, 32), groups[1]: bytes.Repeat([]byte{0x42}, 32),
	}, validFor: time.Hour}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	firstCompiled, _ := compiler.Reconcile(ctx, routeIntentFixture(), groups)
	firstPublished, err := publisher.Publish(ctx, firstCompiled)
	if err != nil || firstPublished.Published != 2 {
		t.Fatalf("initial publication = %+v, %v", firstPublished, err)
	}
	firstUS := authorityResultsByGroup(firstPublished.Results)[groups[1]]

	now = now.Add(time.Minute)
	secondIntent := routeIntentFixture()
	secondIntent.Generation = "route-intents-43"
	secondIntent.Routes[0].Generation = "route-all-2"
	secondCompiled, _ := compiler.Reconcile(ctx, secondIntent, groups)
	secondPublished, err := publisher.Publish(ctx, secondCompiled)
	if err != nil || secondPublished.Published != 2 {
		t.Fatalf("second publication = %+v, %v", secondPublished, err)
	}
	usCurrent, _ := store.ReadGroupAuthority(ctx, groups[1])
	deCurrent, _ := store.ReadGroupAuthority(ctx, groups[0])
	if usCurrent.Published.Bundle.Generation == firstUS.BundleGeneration {
		t.Fatal("fixture did not produce a recoverable prior generation")
	}

	readerDir := privateFixtureDir(t)
	readerToken := strings.Repeat("r", 48)
	writeGroupReaderFixture(t, readerDir, groups[1], readerToken, now)
	writeGroupReaderFixture(t, readerDir, groups[0], strings.Repeat("d", 48), now)
	reader, err := NewGroupBundleHandler(GroupBundleHandlerConfig{Store: store, GroupIDs: groups, KeyringDir: readerDir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groups[1]+"&edge_id=edge-us-1", nil)
	request.Header.Set("Authorization", "Bearer "+readerToken)
	recorder := httptest.NewRecorder()
	reader.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Header().Get(GroupBundleGroupHeader) != groups[1] || recorder.Header().Get("ETag") == "" ||
		recorder.Header().Get(GroupBundlePublicationHeader) != "2" || recorder.Header().Get(GroupBundleRecoveryEpochHeader) != "0" {
		t.Fatalf("group bundle read status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	queryCredential := httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groups[1]+"&edge_id=edge-us-1&token="+readerToken, nil)
	queryCredential.Header.Set("Authorization", "Bearer "+readerToken)
	queryCredentialRecorder := httptest.NewRecorder()
	reader.ServeHTTP(queryCredentialRecorder, queryCredential)
	if queryCredentialRecorder.Code != http.StatusBadRequest {
		t.Fatalf("reader accepted URL credential: status=%d body=%s", queryCredentialRecorder.Code, queryCredentialRecorder.Body.String())
	}
	missingBearerRecorder := httptest.NewRecorder()
	reader.ServeHTTP(missingBearerRecorder, httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groups[1]+"&edge_id=edge-us-1", nil))
	if missingBearerRecorder.Code != http.StatusBadRequest {
		t.Fatalf("reader accepted missing bearer credential: status=%d body=%s", missingBearerRecorder.Code, missingBearerRecorder.Body.String())
	}
	var served struct {
		EdgeGroupID string `json:"edge_group_id"`
		Generation  string `json:"generation"`
		Version     string `json:"version"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &served); err != nil || served.EdgeGroupID != groups[1] || served.Generation != usCurrent.Published.Bundle.Generation ||
		served.Version != groupPublicationVersion(served.Generation, 2, 0) {
		t.Fatalf("served group bundle=%+v err=%v", served, err)
	}
	wrong := httptest.NewRecorder()
	wrongRequest := httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groups[0]+"&edge_id=edge-de-1", nil)
	wrongRequest.Header.Set("Authorization", "Bearer "+readerToken)
	reader.ServeHTTP(wrong, wrongRequest)
	if wrong.Code != http.StatusUnauthorized {
		t.Fatalf("cross-group reader token status=%d body=%s", wrong.Code, wrong.Body.String())
	}
	wrongEdge := httptest.NewRecorder()
	wrongEdgeRequest := httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groups[1]+"&edge_id=edge-us-2", nil)
	wrongEdgeRequest.Header.Set("Authorization", "Bearer "+readerToken)
	reader.ServeHTTP(wrongEdge, wrongEdgeRequest)
	if wrongEdge.Code != http.StatusUnauthorized {
		t.Fatalf("cross-edge reader token status=%d body=%s", wrongEdge.Code, wrongEdge.Body.String())
	}

	recoveryDir := privateFixtureDir(t)
	recoverySecret := bytes.Repeat([]byte{0x51}, 32)
	writeGroupRecoveryFixture(t, recoveryDir, groups[1], recoverySecret, now)
	writeGroupRecoveryFixture(t, recoveryDir, groups[0], bytes.Repeat([]byte{0x52}, 32), now)
	recovery, err := NewGroupRecoveryHandler(GroupRecoveryHandlerConfig{
		Store: store, Signer: signer, GroupIDs: groups, KeyringDir: recoveryDir, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	failedAudit := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groups[1], Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: usCurrent.Published.CandidateLedgerSequence, RouteIntentGeneration: "recovery-audit-tail",
		LastPublishedBundleGeneration: usCurrent.Published.Bundle.Generation, FailureCode: GroupShadowFailureInventoryInvalid,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(time.Second)}
	if _, err := store.AppendGroupAuthorityCAS(ctx, groups[1], usCurrent.LedgerHead.Sequence,
		usCurrent.Published.CandidateLedgerSequence, failedAudit, nil); err != nil {
		t.Fatalf("seed recovery audit tail: %v", err)
	}
	recoveryRequest := GroupRecoveryRequest{
		Schema: GroupRecoveryRequestSchemaV1, KeyID: "recovery-us-1", GroupID: groups[1],
		ExpectedPublicationSequence: usCurrent.Published.PublicationSequence, ExpectedRecoveryEpoch: 0,
		TargetBundleGeneration: firstUS.BundleGeneration, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
		Nonce: "recovery-nonce-00000001", Reason: "rollback after failed semantic probe",
	}
	if err := SignGroupRecoveryRequest(&recoveryRequest, recoverySecret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(recoveryRequest)
	httpRequest := httptest.NewRequest(http.MethodPost, GroupRecoveryPathV1, bytes.NewReader(raw))
	httpRequest.Header.Set("Content-Type", "application/json")
	recoveryRecorder := httptest.NewRecorder()
	recovery.ServeHTTP(recoveryRecorder, httpRequest)
	if recoveryRecorder.Code != http.StatusOK {
		t.Fatalf("recovery status=%d body=%s", recoveryRecorder.Code, recoveryRecorder.Body.String())
	}
	usRecovered, _ := store.ReadGroupAuthority(ctx, groups[1])
	deAfter, _ := store.ReadGroupAuthority(ctx, groups[0])
	if usRecovered.Published.Bundle.Generation != firstUS.BundleGeneration || usRecovered.LedgerHead.RecoveryEpoch != 1 ||
		usRecovered.Published.RecoveryEpoch != 1 || usRecovered.Published.Bundle.Version != groupPublicationVersion(firstUS.BundleGeneration, usCurrent.LedgerHead.Sequence+2, 1) ||
		usRecovered.LedgerHead.Sequence != usCurrent.LedgerHead.Sequence+2 || deAfter.Published.Digest != deCurrent.Published.Digest || deAfter.LedgerHead.Sequence != deCurrent.LedgerHead.Sequence {
		t.Fatalf("group recovery escaped scope: US=%+v DE before=%+v after=%+v", usRecovered, deCurrent, deAfter)
	}
	replay := httptest.NewRecorder()
	replayRequest := httptest.NewRequest(http.MethodPost, GroupRecoveryPathV1, bytes.NewReader(raw))
	replayRequest.Header.Set("Content-Type", "application/json")
	recovery.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusConflict {
		t.Fatalf("replayed recovery status=%d body=%s", replay.Code, replay.Body.String())
	}
}

type prunedCurrentRecoveryStore struct {
	*PersistentGroupStore
}

func (store *prunedCurrentRecoveryStore) ReadGroupRecoveryTarget(context.Context, string, string) (GroupAuthorityState, GroupShadowLedgerEntry, uint64, error) {
	return GroupAuthorityState{}, GroupShadowLedgerEntry{}, 0, errors.New("fixture candidate history was pruned")
}

func TestGroupRecoveryRenewsCurrentPublishedBundleWithoutCandidateHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 25, 13, 45, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	persistent, signer, _, _ := groupPromotionFixture(t, groupID, now.Add(-time.Minute))
	before, err := persistent.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x61}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, now)
	handler, err := NewGroupRecoveryHandler(GroupRecoveryHandlerConfig{
		Store: &prunedCurrentRecoveryStore{PersistentGroupStore: persistent}, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	value := GroupRecoveryRequest{
		Schema: GroupRecoveryRequestSchemaV1, KeyID: "recovery-us-1", GroupID: groupID,
		ExpectedPublicationSequence: before.Published.PublicationSequence, ExpectedRecoveryEpoch: before.Published.RecoveryEpoch,
		TargetBundleGeneration: before.Published.Bundle.Generation, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
		Nonce: "current-recovery-nonce-01", Reason: "refresh current published LKG after pruned candidate history",
	}
	if err := SignGroupRecoveryRequest(&value, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	request := httptest.NewRequest(http.MethodPost, GroupRecoveryPathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("current published recovery status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	after, err := persistent.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != before.Published.Bundle.Generation ||
		after.Published.PublicationSequence != before.LedgerHead.Sequence+1 ||
		after.Published.RecoveryEpoch != before.Published.RecoveryEpoch+1 ||
		after.LedgerHead.Sequence != before.LedgerHead.Sequence+1 {
		t.Fatalf("current published recovery did not advance exact bundle: before=%+v after=%+v err=%v", before, after, err)
	}
}

type fixtureGroupSigner struct {
	keys     map[string][]byte
	validFor time.Duration
}

type failingGroupSigner struct{}

func (failingGroupSigner) SignGroupBundle(context.Context, string, model.EdgeRouteBundle) (model.EdgeRouteBundle, error) {
	return model.EdgeRouteBundle{}, errors.New("fixture signing unavailable")
}

func (signer *fixtureGroupSigner) SignGroupBundle(_ context.Context, groupID string, bundle model.EdgeRouteBundle) (model.EdgeRouteBundle, error) {
	key := signer.keys[groupID]
	return bundleauth.SignEdgeRouteBundle(bundle, string(key), "signing-"+strings.TrimPrefix(groupID, "edge-group-country-")+"-1", signer.validFor), nil
}

type selectiveInventoryReader struct {
	delegate GroupInventoryReader
	failures map[string]bool
}

type blockingGroupInventoryReader struct {
	delegate     GroupInventoryReader
	blockedGroup string
	release      <-chan struct{}
}

func (reader *blockingGroupInventoryReader) ReadGroupInventory(ctx context.Context, groupID string) (GroupInventorySnapshot, error) {
	if groupID == reader.blockedGroup {
		select {
		case <-reader.release:
		case <-ctx.Done():
			return GroupInventorySnapshot{}, ctx.Err()
		}
	}
	return reader.delegate.ReadGroupInventory(ctx, groupID)
}

type blockingGroupSigner struct {
	fixtureGroupSigner
	blockedGroup string
	release      <-chan struct{}
}

func (signer *blockingGroupSigner) SignGroupBundle(ctx context.Context, groupID string, bundle model.EdgeRouteBundle) (model.EdgeRouteBundle, error) {
	if groupID == signer.blockedGroup {
		select {
		case <-signer.release:
		case <-ctx.Done():
			return model.EdgeRouteBundle{}, ctx.Err()
		}
	}
	return signer.fixtureGroupSigner.SignGroupBundle(ctx, groupID, bundle)
}

func waitForCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func (reader *selectiveInventoryReader) ReadGroupInventory(ctx context.Context, groupID string) (GroupInventorySnapshot, error) {
	if reader.failures[groupID] {
		return GroupInventorySnapshot{}, os.ErrNotExist
	}
	return reader.delegate.ReadGroupInventory(ctx, groupID)
}

func privateFixtureDir(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "projected")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func writeGroupSigningFixture(t *testing.T, directory, groupID string, key []byte, _ time.Time) {
	t.Helper()
	value := groupBundleSigningKeyringFile{
		Schema: GroupBundleSigningKeyringSchemaV1, Generation: 1,
		Group: groupBundleSigningKey{GroupID: groupID, PrimaryKeyID: "signing-" + strings.TrimPrefix(groupID, "edge-group-country-") + "-1", PrimaryKey: base64.RawURLEncoding.EncodeToString(key)},
	}
	writePrivateJSONFixture(t, filepath.Join(directory, groupID+".json"), value)
}

func writeGroupReaderFixture(t *testing.T, directory, groupID, token string, now time.Time) {
	t.Helper()
	digest := sha256.Sum256([]byte(token))
	value := groupBundleReaderKeyring{
		Schema: GroupBundleReaderKeyringSchemaV1, Generation: 1, GroupID: groupID,
		Credentials: []groupBundleReaderCredential{{
			CredentialID: "reader-" + strings.TrimPrefix(groupID, "edge-group-country-") + "-1",
			EdgeID:       "edge-" + strings.TrimPrefix(groupID, "edge-group-country-") + "-1",
			TokenDigest:  "sha256:" + hex.EncodeToString(digest[:]), NotBeforeUnix: now.Add(-time.Minute).Unix(), NotAfterUnix: now.Add(time.Hour).Unix(),
		}},
	}
	writePrivateJSONFixture(t, filepath.Join(directory, groupID+".json"), value)
}

func writeGroupRecoveryFixture(t *testing.T, directory, groupID string, secret []byte, now time.Time) {
	t.Helper()
	value := groupRecoveryKeyring{
		Schema: GroupRecoveryKeyringSchemaV1, Generation: 1, GroupID: groupID,
		Keys: []groupRecoveryKey{{
			KeyID: "recovery-" + strings.TrimPrefix(groupID, "edge-group-country-") + "-1", Secret: base64.RawURLEncoding.EncodeToString(secret),
			NotBeforeUnix: now.Add(-time.Minute).Unix(), NotAfterUnix: now.Add(time.Hour).Unix(),
		}},
	}
	writePrivateJSONFixture(t, filepath.Join(directory, groupID+".json"), value)
}

func writePrivateJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func authorityResultsByGroup(values []GroupAuthorityResult) map[string]GroupAuthorityResult {
	out := make(map[string]GroupAuthorityResult, len(values))
	for _, value := range values {
		out[value.GroupID] = value
	}
	return out
}

func authorityStatusByGroup(values []AuthorityGroupStatus) map[string]AuthorityGroupStatus {
	out := make(map[string]AuthorityGroupStatus, len(values))
	for _, value := range values {
		out[value.GroupID] = value
	}
	return out
}
