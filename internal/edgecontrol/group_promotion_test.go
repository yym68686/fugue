package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGroupPromotionAtomicallyReissuesExactCandidateAsCurrent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, current := groupPromotionFixture(t, groupID, now)
	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x71}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	request := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence:   current.LedgerHead.Sequence,
		ExpectedPublicationSequence: current.Published.PublicationSequence, ExpectedRecoveryEpoch: current.Published.RecoveryEpoch,
		ExpectedPublishedBundleDigest: current.Published.Digest, ExpectedCandidateEpoch: candidate.Epoch,
		CandidateRecordDigest: candidate.Record.RecordDigest, CandidateWorkerSlot: candidate.WorkerSlot,
		CandidateBundleGeneration: candidate.Bundle.Generation, IssuedAtUnix: now.Add(2 * time.Minute).Unix(),
		ExpiresAtUnix: now.Add(3 * time.Minute).Unix(), Nonce: "promotion-nonce-00000001", Reason: "promote independently verified candidate"}
	if err := SignGroupPromotionRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(request)
	httpRequest := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	httpRequest.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("promotion status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var receipt GroupPromotionReceipt
	if json.Unmarshal(recorder.Body.Bytes(), &receipt) != nil || receipt.Schema != GroupPromotionReceiptSchemaV1 ||
		receipt.PreviousAuthoritySequence != current.LedgerHead.Sequence ||
		receipt.PreviousPublicationSequence != current.Published.PublicationSequence ||
		receipt.PreviousPublishedBundleDigest != current.Published.Digest || receipt.CandidateRecordDigest != candidate.Record.RecordDigest ||
		receipt.WorkerSlot != candidate.WorkerSlot || receipt.PublicationSequence != current.LedgerHead.Sequence+1 ||
		receipt.BundleGeneration != candidate.Bundle.Generation || receipt.PublishedBundleDigest == candidate.Record.BundleDigest {
		t.Fatalf("promotion receipt is not fully bound: %+v", receipt)
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != candidate.Bundle.Generation ||
		after.Published.Bundle.PreviousGeneration != current.Published.Bundle.Generation || after.Published.Digest != receipt.PublishedBundleDigest ||
		after.Published.Bundle.Version != groupPublicationVersion(candidate.Bundle.Generation, current.LedgerHead.Sequence+1, current.Published.RecoveryEpoch) {
		t.Fatalf("promoted current=%+v err=%v", after, err)
	}
	replay := httptest.NewRecorder()
	replayRequest := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	replayRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusOK || replay.Body.String() != recorder.Body.String() {
		t.Fatalf("promotion replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	history, err := store.AuthorityHistory(ctx, groupID)
	if err != nil || len(history) != int(current.LedgerHead.Sequence+1) {
		t.Fatalf("replay changed authority ledger: len=%d err=%v", len(history), err)
	}
}

func TestGroupPromotionDoesNotBlockFollowingRouteIntentPublication(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	store, signer, candidate, current := groupPromotionFixture(t, groupID, now)
	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x76}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	promotion := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-us-1", GroupID: groupID,
		ExpectedAuthoritySequence: current.LedgerHead.Sequence, ExpectedPublicationSequence: current.Published.PublicationSequence,
		ExpectedRecoveryEpoch: current.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: current.Published.Digest,
		ExpectedCandidateEpoch: candidate.Epoch, CandidateRecordDigest: candidate.Record.RecordDigest,
		CandidateWorkerSlot: candidate.WorkerSlot, CandidateBundleGeneration: candidate.Bundle.Generation,
		IssuedAtUnix: now.Add(2 * time.Minute).Unix(), ExpiresAtUnix: now.Add(3 * time.Minute).Unix(),
		Nonce: "promotion-followed-by-config-0001", Reason: "promote before independent config recovery"}
	if err := SignGroupPromotionRequest(&promotion, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(promotion)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("promotion status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	inventory, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Sequence++
	inventory.Generation = "inventory-after-promotion"
	inventory.ObservedAt = now.Add(3 * time.Minute)
	inventory.ActiveEpoch.Slot = candidate.WorkerSlot
	inventory.ActiveEpoch.ReleaseEpoch = "epoch-after-promotion"
	inventory.ActiveEpoch.FenceSequence++
	for index := range inventory.Instances {
		inventory.Instances[index].Slot = candidate.WorkerSlot
		inventory.Instances[index].ReleaseEpoch = inventory.ActiveEpoch.ReleaseEpoch
		inventory.Instances[index].EffectiveHealthy = true
		serving := true
		inventory.Instances[index].ServingHealthy = &serving
	}
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	nextIntent := routeIntentFixture()
	nextIntent.Generation = "route-intents-after-promotion"
	nextIntent.Routes[0].Generation = "route-after-promotion"
	compiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now.Add(3 * time.Minute) }}).
		Reconcile(ctx, nextIntent, []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("compile next config: batch=%+v err=%v", compiled, err)
	}
	published, err := (GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now.Add(3 * time.Minute) }}).
		Publish(ctx, compiled)
	if err != nil || published.Published != 1 || published.Failed != 0 {
		t.Fatalf("publish next config after promotion: batch=%+v err=%v", published, err)
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != compiled.Results[0].BundleGeneration ||
		after.Published.PublicationSequence <= current.Published.PublicationSequence {
		t.Fatalf("next config did not become current: authority=%+v err=%v", after, err)
	}
}

func TestGroupPromotionAllowsOnlyFailedAuditTailAfterCandidateCanary(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 8, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, current := groupPromotionFixture(t, groupID, now)
	canaryAuthoritySequence := candidate.AuthorityLedgerSequence
	for index := 0; index < 2; index++ {
		authority, err := store.ReadGroupAuthority(ctx, groupID)
		if err != nil {
			t.Fatal(err)
		}
		failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
			CandidateLedgerSequence: authority.Published.CandidateLedgerSequence, RouteIntentGeneration: "post-canary-audit",
			LastPublishedBundleGeneration: authority.Published.Bundle.Generation, FailureCode: GroupShadowFailureInventoryInvalid,
			Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(time.Duration(index+2) * time.Minute)}
		if _, err := store.AppendGroupAuthorityCAS(ctx, groupID, authority.LedgerHead.Sequence,
			authority.Published.CandidateLedgerSequence, failed, nil); err != nil {
			t.Fatalf("append failed audit %d: %v", index, err)
		}
	}
	afterAudits, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || afterAudits.LedgerHead.Sequence != canaryAuthoritySequence+2 ||
		afterAudits.Published.Digest != current.Published.Digest {
		t.Fatalf("audit tail changed publication: authority=%+v err=%v", afterAudits, err)
	}

	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x75}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(4 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	promotion := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: canaryAuthoritySequence, ExpectedPublicationSequence: current.Published.PublicationSequence,
		ExpectedRecoveryEpoch: current.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: current.Published.Digest,
		ExpectedCandidateEpoch: candidate.Epoch, CandidateRecordDigest: candidate.Record.RecordDigest,
		CandidateWorkerSlot: candidate.WorkerSlot, CandidateBundleGeneration: candidate.Bundle.Generation,
		IssuedAtUnix: now.Add(4 * time.Minute).Unix(), ExpiresAtUnix: now.Add(5 * time.Minute).Unix(),
		Nonce: "promotion-audit-tail-0001", Reason: "promote after harmless failed audit tail"}
	if err := SignGroupPromotionRequest(&promotion, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(promotion)
	request := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var receipt GroupPromotionReceipt
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &receipt) != nil ||
		receipt.PreviousAuthoritySequence != afterAudits.LedgerHead.Sequence ||
		receipt.PublicationSequence != afterAudits.LedgerHead.Sequence+1 {
		t.Fatalf("promotion after audit tail status=%d receipt=%+v body=%s", recorder.Code, receipt, recorder.Body.String())
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != candidate.Bundle.Generation ||
		after.Published.Bundle.Version != groupPublicationVersion(candidate.Bundle.Generation, receipt.PublicationSequence, receipt.RecoveryEpoch) {
		t.Fatalf("audit-tail promotion was not exact: authority=%+v err=%v", after, err)
	}
	failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: after.Published.CandidateLedgerSequence, RouteIntentGeneration: "post-promotion-audit",
		LastPublishedBundleGeneration: after.Published.Bundle.Generation, FailureCode: GroupShadowFailureInventoryInvalid,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(6 * time.Minute)}
	if _, err := store.AppendGroupAuthorityCAS(ctx, groupID, after.LedgerHead.Sequence,
		after.Published.CandidateLedgerSequence, failed, nil); err != nil {
		t.Fatal(err)
	}
	replay := httptest.NewRecorder()
	replayRequest := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	replayRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusOK || replay.Body.String() != recorder.Body.String() {
		t.Fatalf("promotion replay after audit status=%d body=%s", replay.Code, replay.Body.String())
	}
}

func TestGroupPromotionRejectsCandidateAndCurrentCASDriftWithoutWriting(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	store, signer, candidate, current := groupPromotionFixture(t, groupID, now)
	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x72}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	base := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-us-1", GroupID: groupID,
		ExpectedAuthoritySequence:   current.LedgerHead.Sequence,
		ExpectedPublicationSequence: current.Published.PublicationSequence, ExpectedRecoveryEpoch: current.Published.RecoveryEpoch,
		ExpectedPublishedBundleDigest: current.Published.Digest, ExpectedCandidateEpoch: candidate.Epoch,
		CandidateRecordDigest: candidate.Record.RecordDigest, CandidateWorkerSlot: candidate.WorkerSlot,
		CandidateBundleGeneration: candidate.Bundle.Generation, IssuedAtUnix: now.Add(2 * time.Minute).Unix(),
		ExpiresAtUnix: now.Add(3 * time.Minute).Unix(), Nonce: "promotion-nonce-00000002", Reason: "reject stale authority witness"}
	for name, mutate := range map[string]func(*GroupPromotionRequest){
		"authority": func(value *GroupPromotionRequest) { value.ExpectedAuthoritySequence-- },
		"current": func(value *GroupPromotionRequest) {
			value.ExpectedPublishedBundleDigest = "sha256:" + strings.Repeat("a", 64)
		},
		"candidate": func(value *GroupPromotionRequest) { value.CandidateRecordDigest = "sha256:" + strings.Repeat("b", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			if err := SignGroupPromotionRequest(&request, secret); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(request)
			httpRequest := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
			httpRequest.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httpRequest)
			if recorder.Code != http.StatusConflict {
				t.Fatalf("drift status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	after, _ := store.ReadGroupAuthority(context.Background(), groupID)
	if after.LedgerHead.Sequence != current.LedgerHead.Sequence || after.Published.Digest != current.Published.Digest {
		t.Fatalf("rejected promotion changed current: before=%+v after=%+v", current, after)
	}
}

func TestGroupPromotionPromotesExactCanariedEpochAfterCandidatePointerAdvances(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 9, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, canaried, current := groupPromotionFixture(t, groupID, now)

	inventory, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Sequence++
	inventory.Generation = "inventory-after-canary"
	inventory.ActiveEpoch.FenceSequence++
	inventory.ObservedAt = now.Add(90 * time.Second)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatalf("advance inventory: %v", err)
	}
	compiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now.Add(90 * time.Second) }}).
		Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil || compiled.Succeeded != 1 {
		t.Fatalf("compile replacement candidate: batch=%+v err=%v", compiled, err)
	}
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	publisher := GroupCandidatePublisher{Store: store, Signer: signer,
		CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now.Add(90 * time.Second) }},
		Identity:   identity, Now: func() time.Time { return now.Add(90 * time.Second) }}
	if batch, publishErr := publisher.Publish(ctx, compiled); publishErr != nil || batch.Published != 1 {
		t.Fatalf("publish replacement candidate: batch=%+v err=%v", batch, publishErr)
	}
	latest, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || latest.Epoch <= canaried.Epoch || latest.Record.RecordDigest == canaried.Record.RecordDigest {
		t.Fatalf("candidate pointer did not advance: latest=%+v exists=%v err=%v", latest, exists, err)
	}
	retained, exists, err := store.ReadGroupCandidateByEpoch(ctx, groupID, canaried.Epoch)
	if err != nil || !exists || retained.Record.RecordDigest != canaried.Record.RecordDigest {
		t.Fatalf("canaried epoch was not retained: retained=%+v exists=%v err=%v", retained, exists, err)
	}

	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x74}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	promotion := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: current.LedgerHead.Sequence, ExpectedPublicationSequence: current.Published.PublicationSequence,
		ExpectedRecoveryEpoch: current.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: current.Published.Digest,
		ExpectedCandidateEpoch: canaried.Epoch, CandidateRecordDigest: canaried.Record.RecordDigest,
		CandidateWorkerSlot: canaried.WorkerSlot, CandidateBundleGeneration: canaried.Bundle.Generation,
		IssuedAtUnix: now.Add(2 * time.Minute).Unix(), ExpiresAtUnix: now.Add(3 * time.Minute).Unix(),
		Nonce: "promotion-retained-epoch-0001", Reason: "promote exact independently canaried epoch"}
	if err := SignGroupPromotionRequest(&promotion, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(promotion)
	request := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("retained epoch promotion status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != canaried.Bundle.Generation || after.Published.CandidateLedgerSequence != canaried.CandidateLedgerSequence {
		t.Fatalf("retained epoch was not promoted exactly: authority=%+v err=%v", after, err)
	}
}

func TestGroupPromotionAcceptsSameGenerationPublicationRefresh(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, before := groupPromotionFixture(t, groupID, now)
	// Refresh the exact published bundle after the candidate was staged. This
	// advances publication/recovery CAS metadata but must not invalidate the
	// immutable candidate predecessor generation.
	refreshed, ok := (GroupAuthorityPublisher{Store: store, Signer: signer}).refreshPublishedLKG(ctx, groupID, before, now.Add(20*time.Minute), "test same-generation publication refresh")
	if !ok || refreshed.Status != GroupAuthorityStatusPublished {
		t.Fatalf("refresh exact published generation: result=%+v ok=%v", refreshed, ok)
	}
	keyDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x76}, 32)
	writeGroupRecoveryFixture(t, keyDir, groupID, secret, now)
	handler, err := NewGroupPromotionHandler(GroupPromotionHandlerConfig{Store: store, Signer: signer,
		GroupIDs: []string{groupID}, KeyringDir: keyDir, Now: func() time.Time { return now.Add(21 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	promotion := GroupPromotionRequest{Schema: GroupPromotionRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: before.LedgerHead.Sequence, ExpectedPublicationSequence: before.Published.PublicationSequence,
		ExpectedRecoveryEpoch: before.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: before.Published.Digest,
		ExpectedCandidateEpoch: candidate.Epoch, CandidateRecordDigest: candidate.Record.RecordDigest,
		CandidateWorkerSlot: candidate.WorkerSlot, CandidateBundleGeneration: candidate.Bundle.Generation,
		IssuedAtUnix: now.Add(21 * time.Minute).Unix(), ExpiresAtUnix: now.Add(22 * time.Minute).Unix(),
		Nonce: "promotion-refresh-0001", Reason: "promote after same-generation publication refresh"}
	if err := SignGroupPromotionRequest(&promotion, secret); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(promotion)
	request := httptest.NewRequest(http.MethodPost, GroupPromotionPathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("same-generation refresh promotion status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var receipt GroupPromotionReceipt
	if err := json.Unmarshal(recorder.Body.Bytes(), &receipt); err != nil || receipt.BundleGeneration != candidate.Bundle.Generation {
		t.Fatalf("same-generation refresh promotion receipt=%+v err=%v", receipt, err)
	}
	after, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || after.Published.Bundle.Generation != candidate.Bundle.Generation || after.LedgerHead.Sequence != receipt.PublicationSequence {
		t.Fatalf("same-generation refresh changed wrong authority: after=%+v receipt=%+v err=%v", after, receipt, err)
	}
}

func groupPromotionFixture(t *testing.T, groupID string, now time.Time) (*PersistentGroupStore, *fixtureGroupSigner, GroupCandidateBundle, GroupAuthorityState) {
	t.Helper()
	ctx := context.Background()
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "a", "epoch-a", "inventory-promotion", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	compiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}).Reconcile(ctx, routeIntentFixture(), []string{groupID})
	if err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x73}, 32)}, validFor: 30 * time.Minute}
	currentPublisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, err := currentPublisher.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("seed current publication: batch=%+v err=%v", batch, err)
	}
	current, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: current.Published.CandidateLedgerSequence, RouteIntentGeneration: "promotion-audit-failure",
		LastPublishedBundleGeneration: current.Published.Bundle.Generation, FailureCode: GroupAuthorityFailureSigning,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(30 * time.Second)}
	if _, err := store.AppendGroupAuthorityCAS(ctx, groupID, current.LedgerHead.Sequence, current.Published.CandidateLedgerSequence, failed, nil); err != nil {
		t.Fatalf("seed failed authority audit: %v", err)
	}
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	candidatePublisher := GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &currentPublisher, Identity: identity, Now: func() time.Time { return now.Add(time.Minute) }}
	if batch, err := candidatePublisher.Publish(ctx, compiled); err != nil || batch.Published != 1 {
		t.Fatalf("publish candidate: batch=%+v err=%v", batch, err)
	}
	candidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists {
		t.Fatalf("read candidate: exists=%v err=%v", exists, err)
	}
	current, err = store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LedgerHead.Sequence == current.Published.PublicationSequence || candidate.AuthorityLedgerSequence != current.LedgerHead.Sequence {
		t.Fatalf("fixture did not separate authority head from published pointer: candidate=%+v current=%+v", candidate, current)
	}
	return store, signer, candidate, current
}
