package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCandidatePublisherPersistsInactiveBundleWithoutChangingCurrent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-de-1", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x51}, 32)}, validFor: 30 * time.Minute}
	legacy := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := legacy.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed current publication: batch=%+v err=%v", batch, err)
	}
	currentBefore, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	identity := CandidateReleaseIdentity{
		SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64),
	}
	publisher := GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &legacy, Identity: identity, Now: func() time.Time { return now.Add(time.Minute) }}
	batch, err := publisher.Publish(ctx, compiled)
	if err != nil || batch.Published != 1 || batch.Failed != 0 {
		t.Fatalf("candidate publication: batch=%+v err=%v", batch, err)
	}
	currentAfter, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !reflect.DeepEqual(currentAfter, currentBefore) {
		t.Fatalf("candidate changed current authority: before=%+v after=%+v err=%v", currentBefore, currentAfter, err)
	}
	candidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || validateGroupCandidateBundle(groupID, candidate) != nil ||
		candidate.Record.SourceSHA != identity.SourceSHA || candidate.Record.ControlImageDigest != identity.ControlImageDigest ||
		candidate.ReleaseRecordDigest != identity.ReleaseRecordDigest || candidate.Epoch <= currentBefore.Published.PublicationSequence ||
		candidate.CurrentRecord == nil || candidate.CurrentRecord.BundleDigest != currentBefore.Published.Digest ||
		candidate.CurrentBundle == nil || !reflect.DeepEqual(*candidate.CurrentBundle, currentBefore.Published.Bundle) ||
		candidate.CurrentRecord.SourceSHA != identity.SourceSHA || candidate.CurrentWorkerSlot != "a" || candidate.WorkerSlot != "b" {
		t.Fatalf("durable candidate is invalid: candidate=%+v exists=%v err=%v", candidate, exists, err)
	}

	readerDir := privateFixtureDir(t)
	readerToken := strings.Repeat("r", 48)
	writeGroupReaderFixture(t, readerDir, groupID, readerToken, now.Add(time.Minute))
	handler, err := NewGroupBundleHandler(GroupBundleHandlerConfig{Store: store, GroupIDs: []string{groupID}, KeyringDir: readerDir, Now: func() time.Time { return now.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, GroupCandidateBundleReadPathV1+"?edge_group_id="+groupID+"&edge_id=edge-de-1", nil)
	request.Header.Set("Authorization", "Bearer "+readerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Header().Get(GroupCandidateRecordHeader) != candidate.Record.RecordDigest ||
		recorder.Header().Get(GroupCandidateReleaseHeader) != identity.ReleaseRecordDigest ||
		recorder.Header().Get(GroupCandidateSlotHeader) != "b" || recorder.Header().Get(GroupBundlePublicationHeader) != "2" {
		t.Fatalf("candidate endpoint is unbound: status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	var served struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &served); err != nil || served.Version != groupPublicationVersion(candidate.Bundle.Generation, candidate.Epoch, 0) {
		t.Fatalf("candidate endpoint served wrong bundle: %+v err=%v", served, err)
	}
	var bundleBody map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &bundleBody); err != nil || bundleBody["record"] != nil || bundleBody["release_record_digest"] != nil {
		t.Fatalf("candidate bundle endpoint exposed envelope fields: body=%s err=%v", recorder.Body.String(), err)
	}
	envelopeRequest := httptest.NewRequest(http.MethodGet, GroupCandidateEnvelopeReadPathV1+"?edge_group_id="+groupID+"&edge_id=edge-de-1", nil)
	envelopeRequest.Header.Set("Authorization", "Bearer "+readerToken)
	envelopeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(envelopeRecorder, envelopeRequest)
	var envelope GroupCandidateBundle
	if envelopeRecorder.Code != http.StatusOK || envelopeRecorder.Header().Get("ETag") != `"`+candidate.Record.RecordDigest+`"` ||
		envelopeRecorder.Header().Get(GroupCandidateRecordHeader) != candidate.Record.RecordDigest ||
		json.Unmarshal(envelopeRecorder.Body.Bytes(), &envelope) != nil || !reflect.DeepEqual(envelope, candidate) ||
		validateGroupCandidateBundle(groupID, envelope) != nil {
		t.Fatalf("candidate envelope is not the exact durable record: status=%d headers=%v envelope=%+v body=%s", envelopeRecorder.Code, envelopeRecorder.Header(), envelope, envelopeRecorder.Body.String())
	}
	currentRequest := httptest.NewRequest(http.MethodGet, GroupBundleReadPathV1+"?edge_group_id="+groupID+"&edge_id=edge-de-1", nil)
	currentRequest.Header.Set("Authorization", "Bearer "+readerToken)
	currentRecorder := httptest.NewRecorder()
	handler.ServeHTTP(currentRecorder, currentRequest)
	if currentRecorder.Code != http.StatusOK || currentRecorder.Header().Get(GroupCandidateRecordHeader) != "" ||
		currentRecorder.Header().Get("ETag") != `"`+currentBefore.Published.Digest+`"` {
		t.Fatalf("current endpoint changed after candidate publish: status=%d headers=%v", currentRecorder.Code, currentRecorder.Header())
	}
}

func TestCandidatePublisherRejectsUnboundReleaseIdentity(t *testing.T) {
	publisher := GroupCandidatePublisher{Identity: CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40)}}
	if _, err := publisher.Publish(context.Background(), GroupShadowBatch{}); err == nil {
		t.Fatal("candidate publisher accepted incomplete release provenance")
	}
}

func TestCandidateCurrentAuthorityBindingRejectsCrossReleaseAndSameSlot(t *testing.T) {
	candidate := candidateWithCurrentRecordFixture(t, "edge-group-country-de")
	current := candidate.Record
	current.Epoch = candidate.Record.Epoch - 1
	current.BundleDigest = "sha256:" + strings.Repeat("b", 64)
	current.Signature = strings.Repeat("B", 43)
	current, err := current.Seal()
	if err != nil {
		t.Fatal(err)
	}
	candidate.CurrentRecord = &current
	currentBundle := candidate.Bundle
	currentBundle.Version = "current-version"
	currentBundle.Generation = "current-generation"
	currentBundle.PreviousGeneration = ""
	currentBundle.GeneratedAt = candidate.Bundle.GeneratedAt.Add(-time.Minute)
	currentBundle.ValidUntil = candidate.Bundle.ValidUntil.Add(-time.Minute)
	currentBundle.Signature = strings.Repeat("B", 43)
	candidate.CurrentBundle = &currentBundle
	current.BundleDigest = signedGroupBundleDigest(currentBundle)
	current, err = current.Seal()
	if err != nil {
		t.Fatal(err)
	}
	candidate.CurrentRecord = &current
	candidate.CurrentWorkerSlot = candidate.WorkerSlot
	if err := validateGroupCandidateBundle(candidate.GroupID, candidate); err == nil {
		t.Fatal("candidate accepted the same current and inactive slot")
	}
	candidate.CurrentWorkerSlot = "a"
	if candidate.WorkerSlot == "a" {
		candidate.CurrentWorkerSlot = "b"
	}
	changed := current
	changed.SourceSHA = strings.Repeat("c", 40)
	changed, err = changed.Seal()
	if err != nil {
		t.Fatal(err)
	}
	candidate.CurrentRecord = &changed
	if err := validateGroupCandidateBundle(candidate.GroupID, candidate); err == nil {
		t.Fatal("candidate accepted a current record from another release")
	}
}

func candidateWithCurrentRecordFixture(t *testing.T, groupID string) GroupCandidateBundle {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 1, 15, 0, 0, time.UTC)
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-current-record", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}).Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x53}, 32)}, validFor: 30 * time.Minute}
	current := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := current.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed current publication: batch=%+v err=%v", batch, err)
	}
	statePath := store.groupStatePath(groupID)
	state, err := store.readGroupState(statePath, groupID)
	if err != nil {
		t.Fatal(err)
	}
	state.Inventory.ObservedAt = now.Add(31 * time.Minute)
	state.Revision++
	if err := store.writeGroupState(statePath, state); err != nil {
		t.Fatal(err)
	}
	identity := CandidateReleaseIdentity{
		SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64),
	}
	publisher := GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &current, Identity: identity, Now: func() time.Time { return now.Add(31 * time.Minute) }}
	if batch, err := publisher.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("publish candidate: batch=%+v err=%v", batch, err)
	}
	candidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists {
		t.Fatalf("read candidate: exists=%v err=%v", exists, err)
	}
	return candidate
}

func TestCandidateObservationDoesNotClaimCurrentPublication(t *testing.T) {
	state := NewAuthorityRuntimeState(func() time.Time { return time.Date(2026, 8, 12, 1, 30, 0, 0, time.UTC) })
	state.Observe(AuthorityRuntimeObservation{RouteIntentGeneration: "candidate-1", CandidatePublished: 1})
	handler := &authorityStatusHandler{state: state}
	snapshot := handler.processReadySnapshot()
	if snapshot.LastPublished != 0 || snapshot.LastCandidatePublished != 1 {
		t.Fatalf("candidate was exposed as current publication: %+v", snapshot)
	}
}

func TestCandidateModeRefreshesOnlyTheExactPersistedCurrentLKG(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 2, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-de-a", "inventory-de-2", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x52}, 32)}, validFor: 30 * time.Minute}
	legacy := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := legacy.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed current publication: batch=%+v err=%v", batch, err)
	}
	before, _ := store.ReadGroupAuthority(ctx, groupID)
	identity := CandidateReleaseIdentity{
		SourceSHA: strings.Repeat("6", 40), ControlImageDigest: "sha256:" + strings.Repeat("7", 64),
		ManifestDigest: "sha256:" + strings.Repeat("8", 64), HealthContractDigest: "sha256:" + strings.Repeat("9", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("a", 64),
	}
	refreshAt := now.Add(16 * time.Minute)
	publisher := GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &legacy, Identity: identity, Now: func() time.Time { return refreshAt }}
	for refresh := 1; refresh <= 6; refresh++ {
		statePath := store.groupStatePath(groupID)
		state, stateErr := store.readGroupState(statePath, groupID)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		state.Inventory.ObservedAt = refreshAt
		state.Revision++
		if stateErr := store.writeGroupState(statePath, state); stateErr != nil {
			t.Fatal(stateErr)
		}
		publisher.Now = func() time.Time { return refreshAt }
		if batch, err := publisher.Publish(ctx, compiled); err != nil || batch.Published != 1 {
			t.Fatalf("candidate publication with LKG refresh %d: batch=%+v err=%v", refresh, batch, err)
		}
		if refresh < 6 {
			refreshAt = refreshAt.Add(16 * time.Minute)
		}
	}
	after, _ := store.ReadGroupAuthority(ctx, groupID)
	if after.Published.Bundle.Generation != before.Published.Bundle.Generation ||
		groupAuthorityCandidateDigest(after.Published.Bundle) != groupAuthorityCandidateDigest(before.Published.Bundle) ||
		after.Published.PublicationSequence != before.Published.PublicationSequence+6 || after.Published.RecoveryEpoch != before.Published.RecoveryEpoch+6 ||
		!after.Published.Bundle.ValidUntil.After(before.Published.Bundle.ValidUntil) {
		t.Fatalf("candidate mode did not refresh only exact current LKG: before=%+v after=%+v", before.Published, after.Published)
	}
	candidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || candidate.Epoch <= after.Published.PublicationSequence {
		t.Fatalf("candidate epoch did not remain inactive and above current: candidate=%+v err=%v", candidate, err)
	}
}

func TestCandidateModeRebuildsInactiveEnvelopeFromExactCurrentLKGWhenWorkersServeLKG(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 12, 3, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "b", "epoch-de-b", "inventory-current", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}
	compiled, err := compiler.Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x54}, 32)}, validFor: 30 * time.Minute}
	current := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := current.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed current publication: batch=%+v err=%v", batch, err)
	}
	statePath := store.groupStatePath(groupID)
	state, err := store.readGroupState(statePath, groupID)
	if err != nil {
		t.Fatal(err)
	}
	last := state.Ledger[len(state.Ledger)-1]
	last.Sequence = 0
	last.Status = GroupShadowStatusFailed
	last.Bundle = nil
	last.BundleGeneration = ""
	last.FailureCode = GroupShadowFailureNoHealthyActive
	last.RecordedAt = now.Add(30 * time.Second)
	last.LastSuccessfulBundleGeneration = state.Published.Bundle.Generation
	last.ActiveSlot = ""
	last.ActiveReleaseEpoch = ""
	last.ActiveFenceSequence = 0
	last.ActiveHealthyInstances = 0
	last.ActiveBootstrapInstances = 0
	appended, err := prepareGroupShadowLedgerAppend(groupID, uint64(len(state.Ledger)), state.Ledger, last)
	if err != nil {
		t.Fatal(err)
	}
	state.Ledger = append(state.Ledger, appended)
	state.Inventory.ActiveEpoch.Slot = "a"
	state.Inventory.ActiveEpoch.ReleaseEpoch = "epoch-de-a"
	state.Inventory.ObservedAt = now.Add(31 * time.Minute)
	state.Revision++
	if err := store.writeGroupState(statePath, state); err != nil {
		t.Fatal(err)
	}
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("6", 40), ControlImageDigest: "sha256:" + strings.Repeat("7", 64),
		ManifestDigest: "sha256:" + strings.Repeat("8", 64), HealthContractDigest: "sha256:" + strings.Repeat("9", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("a", 64)}
	publisher := GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &current, Identity: identity, Now: func() time.Time { return now.Add(31 * time.Minute) }}
	degraded := GroupShadowBatch{Schema: GroupShadowBatchSchemaV1, RouteIntentGeneration: compiled.RouteIntentGeneration,
		Results: []GroupShadowResult{{GroupID: groupID, Status: GroupShadowStatusFailed, FailureCode: GroupShadowFailureNoHealthyActive}}, Failed: 1}
	batch, err := publisher.Publish(ctx, degraded)
	if err != nil || batch.Published != 1 || batch.Failed != 0 {
		t.Fatalf("rebuild candidate from exact current LKG: batch=%+v err=%v", batch, err)
	}
	candidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	authority, authorityErr := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || authorityErr != nil || !exists || validateGroupCandidateBundle(groupID, candidate) != nil ||
		candidate.Record.SourceSHA != identity.SourceSHA || candidate.CurrentRecord == nil ||
		candidate.CurrentRecord.BundleDigest != authority.Published.Digest || candidate.CandidateLedgerSequence != authority.Published.CandidateLedgerSequence ||
		candidate.Epoch <= authority.Published.PublicationSequence || candidate.WorkerSlot == candidate.CurrentWorkerSlot {
		t.Fatalf("rebuilt LKG candidate is invalid: candidate=%+v authority=%+v err=%v/%v", candidate, authority, err, authorityErr)
	}
	if candidate.CurrentWorkerSlot != "a" || candidate.WorkerSlot != "b" {
		t.Fatalf("candidate reused historical ledger slot instead of live Front inventory: candidate=%+v", candidate)
	}
	if !candidate.CurrentBundle.ValidUntil.After(now.Add(31*time.Minute)) || candidate.CurrentBundle.GeneratedAt.Before(now.Add(31*time.Minute)) ||
		candidate.CurrentRecord.BundleDigest != authority.Published.Digest {
		t.Fatalf("rebuilt candidate retained an expired current LKG: candidate=%+v authority=%+v", candidate, authority)
	}
	if _, err := store.PutGroupCurrentLKGCandidateCAS(ctx, groupID, candidate.Epoch, candidate.CandidateLedgerSequence,
		authority.Published.PublicationSequence+1, authority.Published.Digest, candidate); !errors.Is(err, ErrGroupAuthorityCandidateCAS) {
		t.Fatalf("candidate CAS accepted a changed CurrentAuthority epoch: %v", err)
	}
	repeated, err := publisher.Publish(ctx, degraded)
	if err != nil || repeated.Published != 1 || repeated.Results[0].Epoch != candidate.Epoch || repeated.Results[0].RecordDigest != candidate.Record.RecordDigest {
		t.Fatalf("fresh fallback candidate was replaced during reconcile: batch=%+v candidate=%+v err=%v", repeated, candidate, err)
	}
	state, err = store.readGroupState(statePath, groupID)
	if err != nil {
		t.Fatal(err)
	}
	state.Inventory.ActiveEpoch.Slot = "b"
	state.Inventory.ActiveEpoch.ReleaseEpoch = "epoch-de-b-next"
	state.Inventory.ObservedAt = now.Add(32 * time.Minute)
	state.Revision++
	if err := store.writeGroupState(statePath, state); err != nil {
		t.Fatal(err)
	}
	publisher.Now = func() time.Time { return now.Add(32 * time.Minute) }
	replaced, err := publisher.Publish(ctx, degraded)
	updated, updatedExists, readErr := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || readErr != nil || !updatedExists || replaced.Published != 1 || updated.Epoch <= candidate.Epoch ||
		updated.CurrentWorkerSlot != "b" || updated.WorkerSlot != "a" || updated.Record.RecordDigest == candidate.Record.RecordDigest {
		t.Fatalf("stale candidate was not atomically rebound after Front slot change: batch=%+v candidate=%+v err=%v/%v", replaced, updated, err, readErr)
	}
}
