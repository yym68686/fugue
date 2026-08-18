package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type failedAuditTailCandidateStore struct {
	GroupCandidateStore
	store             *PersistentGroupStore
	groupID           string
	candidateSequence uint64
	recordedAt        time.Time
}

func (store *failedAuditTailCandidateStore) PutGroupStagedCurrentLKGCandidateCAS(ctx context.Context, groupID string,
	expectedEpoch, expectedAuthoritySequence, expectedPublicationSequence, expectedRecoveryEpoch uint64, expectedPublishedDigest string,
	serving *GroupServingAuthorityWitness, candidate GroupCandidateBundle) (GroupCandidateBundle, error) {
	authority, err := store.store.ReadGroupAuthority(ctx, store.groupID)
	if err != nil {
		return GroupCandidateBundle{}, err
	}
	failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: store.groupID, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: store.candidateSequence, RouteIntentGeneration: "cas-race-audit-tail",
		LastPublishedBundleGeneration: authority.Published.Bundle.Generation, FailureCode: GroupAuthorityFailureSigning,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: store.recordedAt}
	if _, err := store.store.AppendGroupAuthorityCAS(ctx, store.groupID, authority.LedgerHead.Sequence,
		store.candidateSequence, failed, nil); err != nil {
		return GroupCandidateBundle{}, err
	}
	return store.GroupCandidateStore.PutGroupStagedCurrentLKGCandidateCAS(ctx, groupID, expectedEpoch, expectedAuthoritySequence,
		expectedPublicationSequence, expectedRecoveryEpoch, expectedPublishedDigest, serving, candidate)
}

func TestWorkerCandidateStageBindsExactReleaseAndPreservesCurrentAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, existing, authority := groupPromotionFixture(t, groupID, now)
	inventory, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ActiveEpoch.Slot = "a"
	inventory.ActiveEpoch.ReleaseEpoch = strings.Repeat("a", 40)
	inventory.ObservedAt = now.Add(time.Minute)
	expectedInventorySequence := inventory.Sequence
	inventory.Sequence++
	if err := store.StoreGroupInventoryCAS(ctx, groupID, expectedInventorySequence, inventory); err != nil {
		t.Fatal(err)
	}
	authorityBefore, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x57}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, now.Add(time.Minute))
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	handler, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: GroupCandidatePublisher{
		Store: store, Signer: signer, CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: signer}, Identity: identity,
	}, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return now.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	request := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: authority.LedgerHead.Sequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: existing.Epoch, ExpectedCurrentWorkerSlot: "a", TargetWorkerSlot: "b",
		WorkerSourceSHA: strings.Repeat("6", 40), WorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64), IssuedAtUnix: now.Add(time.Minute).Unix(),
		ExpiresAtUnix: now.Add(2 * time.Minute).Unix(), Nonce: strings.Repeat("n", 24), Reason: "stage exact inactive Worker release"}
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	recorder := postGroupCandidateStage(t, handler, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("stage status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var receipt GroupCandidateStageReceipt
	if json.Unmarshal(recorder.Body.Bytes(), &receipt) != nil || receipt.Schema != GroupCandidateStageReceiptSchemaV1 ||
		receipt.GroupID != groupID || receipt.WorkerSlot != "b" || receipt.CurrentWorkerSlot != "a" ||
		receipt.WorkerSourceSHA != request.WorkerSourceSHA || receipt.WorkerImageDigest != request.WorkerImageDigest ||
		receipt.ReleaseRecordDigest != request.ReleaseRecordDigest || receipt.OrdinaryTrafficMutation ||
		receipt.CurrentPublishedBundleDigest != authority.Published.Digest {
		t.Fatalf("stage receipt=%+v", receipt)
	}
	authorityAfter, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !reflect.DeepEqual(authorityAfter, authorityBefore) {
		t.Fatalf("stage changed current authority: before=%+v after=%+v err=%v", authorityBefore, authorityAfter, err)
	}
	staged, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || staged.Epoch <= existing.Epoch || staged.ReleaseRecordDigest != request.ReleaseRecordDigest ||
		staged.WorkerSourceSHA != request.WorkerSourceSHA || staged.WorkerImageDigest != request.WorkerImageDigest ||
		staged.WorkerSlot != "b" || staged.CurrentWorkerSlot != "a" || staged.Bundle.Generation != authority.Published.Bundle.Generation {
		t.Fatalf("staged candidate=%+v exists=%v err=%v", staged, exists, err)
	}
	readerDir, readerToken := privateFixtureDir(t), strings.Repeat("q", 48)
	writeGroupReaderFixture(t, readerDir, groupID, readerToken, now.Add(time.Minute))
	reader, err := NewGroupBundleHandler(GroupBundleHandlerConfig{Store: store, GroupIDs: []string{groupID}, KeyringDir: readerDir, Now: func() time.Time { return now.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	readRequest := httptest.NewRequest(http.MethodGet, GroupCandidateEnvelopeReadPathV1+"?edge_group_id="+groupID+"&edge_id=edge-de-1", nil)
	readRequest.Header.Set("Authorization", "Bearer "+readerToken)
	readRecorder := httptest.NewRecorder()
	reader.ServeHTTP(readRecorder, readRequest)
	var exposed GroupCandidateBundle
	if readRecorder.Code != http.StatusOK || json.Unmarshal(readRecorder.Body.Bytes(), &exposed) != nil ||
		exposed.WorkerSourceSHA != request.WorkerSourceSHA || exposed.WorkerImageDigest != request.WorkerImageDigest ||
		exposed.ReleaseRecordDigest != request.ReleaseRecordDigest || exposed.Record.RecordDigest != staged.Record.RecordDigest {
		t.Fatalf("explicit Worker candidate was not exposed exactly: status=%d candidate=%+v body=%s", readRecorder.Code, exposed, readRecorder.Body.String())
	}
	// Exact replay is receipt-idempotent and cannot advance either pointer.
	request.ExpectedCandidateEpoch = staged.Epoch
	request.Nonce = strings.Repeat("r", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	replayed := postGroupCandidateStage(t, handler, request)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	afterReplay, _, _ := store.ReadGroupCandidate(ctx, groupID)
	if !reflect.DeepEqual(afterReplay, staged) {
		t.Fatalf("exact replay changed candidate: before=%+v after=%+v", staged, afterReplay)
	}
}

func TestWorkerCandidateStageAcceptsMonotonicLKGRefreshForSameGeneration(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	store, _, _, _ := groupPromotionFixture(t, "edge-group-country-de", now)
	before, err := store.ReadGroupAuthority(ctx, "edge-group-country-de")
	if err != nil {
		t.Fatal(err)
	}
	serving := &GroupServingAuthorityWitness{
		CurrentRecordDigest: before.Published.Digest, AuthorityEpoch: 1, CurrentAuthorityUID: "uid-1", CurrentAuthorityRV: "rv-1",
		FrontGeneration: 1, BundleVersion: before.Published.Bundle.Version, WorkerSlot: "a",
		WorkerSourceSHA: strings.Repeat("a", 40), WorkerImageDigest: "sha256:" + strings.Repeat("b", 64),
	}
	request := GroupCandidateStageRequest{ExpectedPublicationSequence: before.Published.PublicationSequence, ExpectedRecoveryEpoch: before.Published.RecoveryEpoch,
		ExpectedPublishedBundleDigest: before.Published.Digest}
	renewed := before.Published
	renewed.PublicationSequence++
	renewed.RecoveryEpoch++
	renewed.Bundle.Version = groupPublicationVersion(renewed.Bundle.Generation, renewed.PublicationSequence, renewed.RecoveryEpoch)
	if !stagePublicationMatchesAuthority(renewed, request, serving) {
		t.Fatal("same-generation monotonic LKG refresh was rejected")
	}
	renewed.Bundle.Generation = renewed.Bundle.Generation + "-changed"
	if stagePublicationMatchesAuthority(renewed, request, serving) {
		t.Fatal("route generation change was accepted as an LKG refresh")
	}
	if !servingAuthorityCanUsePrunedCurrentGeneration(serving.BundleVersion, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch) {
		t.Fatal("same-generation serving witness was rejected after candidate history pruning")
	}
	crossGenerationWitness := groupPublicationVersion(renewed.Bundle.Generation, renewed.PublicationSequence, renewed.RecoveryEpoch)
	if servingAuthorityCanUsePrunedCurrentGeneration(crossGenerationWitness, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch) {
		t.Fatal("cross-generation serving witness was accepted by fallback")
	}
	futureWitness := groupPublicationVersion(before.Published.Bundle.Generation, renewed.PublicationSequence+1, renewed.RecoveryEpoch)
	if servingAuthorityCanUsePrunedCurrentGeneration(futureWitness, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch) {
		t.Fatal("future serving publication was accepted by pruned-history fallback")
	}
}

func TestWorkerCandidateStageAcceptsFailedAuditTailThatPreservesPublication(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 8, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, existing, authority := groupPromotionFixture(t, groupID, now)
	inventory, err := store.ReadGroupInventory(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ActiveEpoch.Slot = "a"
	inventory.ActiveEpoch.ReleaseEpoch = strings.Repeat("a", 40)
	inventory.ObservedAt = now.Add(time.Minute)
	expectedInventorySequence := inventory.Sequence
	inventory.Sequence++
	if err := store.StoreGroupInventoryCAS(ctx, groupID, expectedInventorySequence, inventory); err != nil {
		t.Fatal(err)
	}
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	request := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, GroupID: groupID,
		ExpectedAuthoritySequence: authority.LedgerHead.Sequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: existing.Epoch, ExpectedCurrentWorkerSlot: "a", TargetWorkerSlot: "b",
		WorkerSourceSHA: strings.Repeat("6", 40), WorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64), Reason: "stage after publication-preserving audit failure"}
	failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: authority.Published.CandidateLedgerSequence, RouteIntentGeneration: "audit-tail",
		LastPublishedBundleGeneration: authority.Published.Bundle.Generation, FailureCode: GroupAuthorityFailureSigning,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: now.Add(30 * time.Second)}
	if _, err := store.AppendGroupAuthorityCAS(ctx, groupID, authority.LedgerHead.Sequence,
		authority.Published.CandidateLedgerSequence, failed, nil); err != nil {
		t.Fatal(err)
	}
	authorityAfterAudit, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || authorityAfterAudit.LedgerHead.Sequence != request.ExpectedAuthoritySequence+1 {
		t.Fatalf("failed audit tail was not appended: authority=%+v err=%v", authorityAfterAudit, err)
	}
	stagingStore := &failedAuditTailCandidateStore{GroupCandidateStore: store, store: store, groupID: groupID,
		candidateSequence: authority.Published.CandidateLedgerSequence, recordedAt: now.Add(45 * time.Second)}
	publisher := GroupCandidatePublisher{Store: stagingStore, Signer: signer,
		CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: signer}, Identity: identity}
	staged, err := publisher.stageWorkerCurrentLKG(ctx, request, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("stage across failed audit tail: %v", err)
	}
	authorityAfterStage, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || authorityAfterStage.LedgerHead.Sequence != authorityAfterAudit.LedgerHead.Sequence+1 ||
		staged.AuthorityLedgerSequence != authorityAfterAudit.LedgerHead.Sequence || !candidateBindsCurrentAuthority(staged, authorityAfterStage) {
		t.Fatalf("candidate lost publication binding across failed audit CAS race: candidate=%+v before=%+v after=%+v err=%v",
			staged, authorityAfterAudit, authorityAfterStage, err)
	}
}

func TestWorkerCandidateStageRejectsCrossGroupStaleCASAndInvalidSignature(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, existing, authority := groupPromotionFixture(t, groupID, now)
	inventory, _ := store.ReadGroupInventory(context.Background(), groupID)
	inventory.ActiveEpoch.Slot, inventory.ObservedAt = "a", now.Add(time.Minute)
	expectedInventorySequence := inventory.Sequence
	inventory.Sequence++
	if err := store.StoreGroupInventoryCAS(context.Background(), groupID, expectedInventorySequence, inventory); err != nil {
		t.Fatal(err)
	}
	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x58}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, now.Add(time.Minute))
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	handler, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: GroupCandidatePublisher{
		Store: store, Signer: signer, CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: signer}, Identity: identity,
	}, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return now.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	base := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: authority.LedgerHead.Sequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: existing.Epoch, ExpectedCurrentWorkerSlot: "a", TargetWorkerSlot: "b",
		WorkerSourceSHA: strings.Repeat("6", 40), WorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64), IssuedAtUnix: now.Add(time.Minute).Unix(),
		ExpiresAtUnix: now.Add(2 * time.Minute).Unix(), Nonce: strings.Repeat("n", 24), Reason: "stage exact inactive Worker release"}
	for name, test := range map[string]struct {
		mutate func(*GroupCandidateStageRequest)
		want   int
	}{
		"stale-candidate":                    {func(v *GroupCandidateStageRequest) { v.ExpectedCandidateEpoch++ }, http.StatusConflict},
		"same-slot":                          {func(v *GroupCandidateStageRequest) { v.TargetWorkerSlot = "a" }, http.StatusBadRequest},
		"degraded-without-serving-authority": {func(v *GroupCandidateStageRequest) { v.AllowDegradedPrevious = true }, http.StatusBadRequest},
		"standby-without-serving-authority":  {func(v *GroupCandidateStageRequest) { v.StandbyOnly = true }, http.StatusBadRequest},
		"cross-group":                        {func(v *GroupCandidateStageRequest) { v.GroupID = "edge-group-country-us" }, http.StatusForbidden},
		"bad-signature":                      {func(v *GroupCandidateStageRequest) {}, http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			if name != "bad-signature" {
				if err := SignGroupCandidateStageRequest(&value, secret); err != nil {
					t.Fatal(err)
				}
			} else {
				value.Signature = strings.Repeat("x", 43)
			}
			recorder := postGroupCandidateStage(t, handler, value)
			if recorder.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.want, recorder.Body.String())
			}
		})
	}
}

func TestWorkerCandidateStageWithoutServingAuthorityRejectsExpiredBootstrapWindow(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 30, 0, 0, time.UTC)
	stageAt := now.Add(maxGroupBundleValidity + time.Minute)
	groupID := "edge-group-country-de"
	store, signer, existing, authority := groupPromotionFixture(t, groupID, now)
	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x5a}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, stageAt.Add(time.Minute))
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	handler, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: GroupCandidatePublisher{
		Store: store, Signer: signer, CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: signer}, Identity: identity,
	}, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return stageAt }})
	if err != nil {
		t.Fatal(err)
	}
	request := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: authority.LedgerHead.Sequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: existing.Epoch, ExpectedCurrentWorkerSlot: "a", TargetWorkerSlot: "b",
		WorkerSourceSHA: strings.Repeat("6", 40), WorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64), IssuedAtUnix: stageAt.Unix(),
		ExpiresAtUnix: stageAt.Add(time.Minute).Unix(), Nonce: strings.Repeat("u", 24), Reason: "reject stale unbound bootstrap publication"}
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	recorder := postGroupCandidateStage(t, handler, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("expired unbound publication status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestWorkerCandidateStageCanBindExactServingHistoricalPublicationWithoutChangingCurrentAuthority(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	stageAt := now.Add(maxGroupBundleValidity + 2*time.Minute)
	groupID := "edge-group-country-de"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, "b", "epoch-b", "inventory-serving-b", false)
	inventory.ObservedAt = now
	if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
		t.Fatal(err)
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x73}, 32)}, validFor: time.Hour}
	firstIntent := routeIntentFixture()
	firstCompiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }}).
		Reconcile(ctx, firstIntent, []string{groupID})
	if err != nil || firstCompiled.Succeeded != 1 {
		t.Fatalf("compile historical serving publication: batch=%+v err=%v", firstCompiled, err)
	}
	authorityPublisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	if batch, publishErr := authorityPublisher.Publish(ctx, firstCompiled); publishErr != nil || batch.Published != 1 {
		t.Fatalf("publish historical serving publication: batch=%+v err=%v", batch, publishErr)
	}
	servingAuthority, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	servingVersion := servingAuthority.Published.Bundle.Version

	inventory.Sequence++
	inventory.Generation = "inventory-control-a"
	inventory.ActiveEpoch.Slot = "a"
	inventory.ActiveEpoch.ReleaseEpoch = "epoch-a"
	inventory.ActiveEpoch.FenceSequence++
	inventory.Instances[0].Slot = "a"
	inventory.Instances[0].ReleaseEpoch = "epoch-a"
	inventory.Instances[0].InstanceUID = "uid-" + groupID + "-a"
	inventory.ObservedAt = now.Add(time.Minute)
	if err := store.StoreGroupInventoryCAS(ctx, groupID, inventory.Sequence-1, inventory); err != nil {
		t.Fatal(err)
	}
	secondIntent := routeIntentFixture()
	secondIntent.Generation = "route-intents-43"
	secondIntent.Routes[0].Generation = "route-all-2"
	secondIntent.Routes[0].UpstreamURL = "http://runtime-next.mesh:8080"
	secondIntent.Routes[0].UpdatedAt = now.Add(time.Minute)
	secondCompiled, err := (GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now.Add(time.Minute) }}).
		Reconcile(ctx, secondIntent, []string{groupID})
	if err != nil || secondCompiled.Succeeded != 1 {
		t.Fatalf("compile current Control publication: batch=%+v err=%v", secondCompiled, err)
	}
	authorityPublisher.Now = func() time.Time { return now.Add(time.Minute) }
	if batch, publishErr := authorityPublisher.Publish(ctx, secondCompiled); publishErr != nil || batch.Published != 1 {
		t.Fatalf("publish current Control publication: batch=%+v err=%v", batch, publishErr)
	}
	if _, refreshErr := authorityPublisher.RefreshPublishedLKG(ctx, groupID, stageAt); refreshErr != nil {
		t.Fatalf("refresh current Control publication into recovery: %v", refreshErr)
	}
	current, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || current.Published.Bundle.Generation == servingAuthority.Published.Bundle.Generation || current.Published.RecoveryEpoch == 0 {
		t.Fatalf("fixture did not diverge Control and serving LKG: current=%+v serving=%+v err=%v", current, servingAuthority, err)
	}
	authorityBefore := current

	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x59}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, stageAt.Add(time.Minute))
	identity := CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64),
		ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)}
	handler, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: GroupCandidatePublisher{
		Store: store, Signer: signer, CurrentLKG: &authorityPublisher, Identity: identity,
	}, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return stageAt }})
	if err != nil {
		t.Fatal(err)
	}
	witness := &GroupServingAuthorityWitness{CurrentRecordDigest: "sha256:" + strings.Repeat("9", 64), AuthorityEpoch: 42,
		CurrentAuthorityUID: "11111111-2222-3333-4444-555555555555", CurrentAuthorityRV: "12345", FrontGeneration: 124,
		BundleVersion: servingVersion, WorkerSlot: "b", WorkerSourceSHA: strings.Repeat("a", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("b", 64)}
	request := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: current.LedgerHead.Sequence, ExpectedPublicationSequence: current.Published.PublicationSequence,
		ExpectedRecoveryEpoch: current.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: current.Published.Digest,
		ExpectedCandidateEpoch: 0, ExpectedCurrentWorkerSlot: "b", TargetWorkerSlot: "a", ServingAuthority: witness,
		AllowDegradedPrevious: true,
		WorkerSourceSHA:       strings.Repeat("6", 40), WorkerImageDigest: "sha256:" + strings.Repeat("7", 64),
		ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64), IssuedAtUnix: stageAt.Unix(),
		ExpiresAtUnix: stageAt.Add(time.Minute).Unix(), Nonce: strings.Repeat("s", 24), Reason: "stage from exact serving historical publication"}
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	recorder := postGroupCandidateStage(t, handler, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("stage historical serving publication status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	staged, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || staged.Bundle.Generation != servingAuthority.Published.Bundle.Generation ||
		staged.Bundle.Generation == current.Published.Bundle.Generation || staged.CandidateLedgerSequence != servingAuthority.Published.CandidateLedgerSequence ||
		!servingAuthorityWitnessesEqual(staged.ServingAuthority, witness) || !staged.AllowDegradedPrevious || staged.CurrentRecord == nil ||
		staged.CurrentRecord.BundleDigest != current.Published.Digest || staged.CurrentBundle == nil ||
		signedGroupBundleDigest(*staged.CurrentBundle) != current.Published.Digest {
		t.Fatalf("historical serving candidate was not bound exactly: staged=%+v exists=%v err=%v", staged, exists, err)
	}
	authorityAfter, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !reflect.DeepEqual(authorityAfter, authorityBefore) {
		t.Fatalf("historical staging changed current Control authority: before=%+v after=%+v err=%v", authorityBefore, authorityAfter, err)
	}
	var receipt GroupCandidateStageReceipt
	if json.Unmarshal(recorder.Body.Bytes(), &receipt) != nil || !receipt.AllowDegradedPrevious {
		t.Fatalf("historical staging receipt omitted degraded previous authorization: %+v", receipt)
	}

	request.ExpectedCandidateEpoch = staged.Epoch
	request.AllowDegradedPrevious = false
	request.StandbyOnly = true
	request.Nonce = strings.Repeat("u", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	standbyRecorder := postGroupCandidateStage(t, handler, request)
	if standbyRecorder.Code != http.StatusOK {
		t.Fatalf("stage standby status=%d body=%s", standbyRecorder.Code, standbyRecorder.Body.String())
	}
	standbyCandidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || !standbyCandidate.StandbyOnly || standbyCandidate.AllowDegradedPrevious {
		t.Fatalf("standby candidate=%+v exists=%v err=%v", standbyCandidate, exists, err)
	}
	var standbyReceipt GroupCandidateStageReceipt
	if json.Unmarshal(standbyRecorder.Body.Bytes(), &standbyReceipt) != nil || !standbyReceipt.StandbyOnly || standbyReceipt.AllowDegradedPrevious {
		t.Fatalf("standby receipt omitted non-promotable authorization: %+v", standbyReceipt)
	}

	request.ExpectedCandidateEpoch = standbyCandidate.Epoch
	request.StandbyOnly = false
	request.AllowDegradedPrevious = true
	request.ServingAuthority = &GroupServingAuthorityWitness{CurrentRecordDigest: witness.CurrentRecordDigest, AuthorityEpoch: witness.AuthorityEpoch,
		CurrentAuthorityUID: witness.CurrentAuthorityUID, CurrentAuthorityRV: witness.CurrentAuthorityRV, FrontGeneration: witness.FrontGeneration,
		BundleVersion: groupPublicationVersion("pruned-serving-generation", current.Published.PublicationSequence-1, current.Published.RecoveryEpoch),
		WorkerSlot:    witness.WorkerSlot, WorkerSourceSHA: witness.WorkerSourceSHA, WorkerImageDigest: witness.WorkerImageDigest}
	request.Nonce = strings.Repeat("v", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	fallbackRecorder := postGroupCandidateStage(t, handler, request)
	if fallbackRecorder.Code != http.StatusOK {
		t.Fatalf("pruned serving publication fallback status=%d body=%s", fallbackRecorder.Code, fallbackRecorder.Body.String())
	}
	fallbackCandidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || !fallbackCandidate.AllowDegradedPrevious || fallbackCandidate.StandbyOnly ||
		fallbackCandidate.Bundle.Generation != current.Published.Bundle.Generation ||
		fallbackCandidate.CandidateLedgerSequence != current.Published.CandidateLedgerSequence ||
		!servingAuthorityWitnessesEqual(fallbackCandidate.ServingAuthority, request.ServingAuthority) {
		t.Fatalf("pruned serving publication fallback candidate=%+v exists=%v err=%v", fallbackCandidate, exists, err)
	}

	request.ExpectedCandidateEpoch = fallbackCandidate.Epoch
	request.ServingAuthority = &GroupServingAuthorityWitness{CurrentRecordDigest: witness.CurrentRecordDigest, AuthorityEpoch: witness.AuthorityEpoch,
		CurrentAuthorityUID: witness.CurrentAuthorityUID, CurrentAuthorityRV: witness.CurrentAuthorityRV, FrontGeneration: witness.FrontGeneration,
		BundleVersion: groupPublicationVersion("normalized-serving-generation", current.Published.PublicationSequence+377, 0),
		WorkerSlot:    witness.WorkerSlot, WorkerSourceSHA: witness.WorkerSourceSHA, WorkerImageDigest: witness.WorkerImageDigest}
	request.Nonce = strings.Repeat("n", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	normalizedRecorder := postGroupCandidateStage(t, handler, request)
	if normalizedRecorder.Code != http.StatusOK {
		t.Fatalf("normalized serving publication fallback status=%d body=%s", normalizedRecorder.Code, normalizedRecorder.Body.String())
	}
	normalizedCandidate, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || !exists || !normalizedCandidate.AllowDegradedPrevious || normalizedCandidate.StandbyOnly ||
		normalizedCandidate.Bundle.Generation != current.Published.Bundle.Generation ||
		normalizedCandidate.CandidateLedgerSequence != current.Published.CandidateLedgerSequence ||
		!servingAuthorityWitnessesEqual(normalizedCandidate.ServingAuthority, request.ServingAuthority) {
		t.Fatalf("normalized serving publication fallback candidate=%+v exists=%v err=%v", normalizedCandidate, exists, err)
	}

	request.ExpectedCandidateEpoch = normalizedCandidate.Epoch
	request.AllowDegradedPrevious = false
	request.StandbyOnly = true
	request.Nonce = strings.Repeat("w", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	standbyFallbackRejected := postGroupCandidateStage(t, handler, request)
	if standbyFallbackRejected.Code != http.StatusConflict {
		t.Fatalf("standby used pruned serving publication fallback status=%d body=%s", standbyFallbackRejected.Code, standbyFallbackRejected.Body.String())
	}

	request.AllowDegradedPrevious = true
	request.StandbyOnly = false
	request.ServingAuthority = &GroupServingAuthorityWitness{CurrentRecordDigest: witness.CurrentRecordDigest, AuthorityEpoch: witness.AuthorityEpoch,
		CurrentAuthorityUID: witness.CurrentAuthorityUID, CurrentAuthorityRV: witness.CurrentAuthorityRV, FrontGeneration: witness.FrontGeneration,
		BundleVersion: groupPublicationVersion("future-serving-generation", current.Published.PublicationSequence+1, current.Published.RecoveryEpoch),
		WorkerSlot:    witness.WorkerSlot, WorkerSourceSHA: witness.WorkerSourceSHA, WorkerImageDigest: witness.WorkerImageDigest}
	request.Nonce = strings.Repeat("t", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	rejected := postGroupCandidateStage(t, handler, request)
	if rejected.Code != http.StatusConflict {
		t.Fatalf("future serving publication status=%d body=%s", rejected.Code, rejected.Body.String())
	}

	request.ServingAuthority.BundleVersion = groupPublicationVersion("previous-recovery-generation", current.Published.PublicationSequence-1, current.Published.RecoveryEpoch-1)
	request.Nonce = strings.Repeat("r", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	crossRecoveryRejected := postGroupCandidateStage(t, handler, request)
	if crossRecoveryRejected.Code != http.StatusConflict {
		t.Fatalf("cross-recovery serving publication status=%d body=%s", crossRecoveryRejected.Code, crossRecoveryRejected.Body.String())
	}
}

func TestServingAuthorityCanUseCurrentPublishedFallback(t *testing.T) {
	currentGeneration := "edgegroupbundle_" + strings.Repeat("a", 64)
	currentPublicationSequence := uint64(12244)
	tests := []struct {
		name       string
		version    string
		allowOlder bool
		want       bool
	}{
		{name: "stored current bundle publication", version: groupPublicationVersion(currentGeneration, 12238, 142), allowOlder: true, want: true},
		{name: "older publication", version: groupPublicationVersion("pruned-generation", 12237, 142), allowOlder: true, want: true},
		{name: "older publication without authorization", version: groupPublicationVersion("pruned-generation", 12237, 142)},
		{name: "exact republished current generation", version: groupPublicationVersion(currentGeneration, currentPublicationSequence, 142), want: true},
		{name: "same sequence different generation", version: groupPublicationVersion("other-generation", currentPublicationSequence, 142)},
		{name: "future publication", version: groupPublicationVersion(currentGeneration, currentPublicationSequence+1, 142)},
		{name: "normalized serving publication ahead", version: groupPublicationVersion("normalized-serving-generation", currentPublicationSequence+388, 0), allowOlder: true, want: true},
		{name: "normalized serving publication ahead without authorization", version: groupPublicationVersion("normalized-serving-generation", currentPublicationSequence+388, 0)},
		{name: "same generation normalized serving publication behind refreshed LKG", version: groupPublicationVersion(currentGeneration, currentPublicationSequence-388, 0), allowOlder: true, want: true},
		{name: "different generation normalized serving publication behind", version: groupPublicationVersion("old-serving-generation", currentPublicationSequence-388, 0), allowOlder: true},
		{name: "same generation normalized serving publication behind without authorization", version: groupPublicationVersion(currentGeneration, currentPublicationSequence-388, 0)},
		{name: "future nonzero recovery", version: groupPublicationVersion("future-recovery-generation", currentPublicationSequence+388, 1), allowOlder: true},
		{name: "same generation previous recovery with authorization", version: groupPublicationVersion(currentGeneration, 12237, 141), allowOlder: true, want: true},
		{name: "same generation previous recovery without authorization", version: groupPublicationVersion(currentGeneration, 12237, 141)},
		{name: "malformed", version: "not-a-publication"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := servingAuthorityCanUseCurrentPublishedFallback(test.version, currentGeneration, currentPublicationSequence, 142, test.allowOlder); got != test.want {
				t.Fatalf("servingAuthorityCanUseCurrentPublishedFallback(%q)=%t want %t", test.version, got, test.want)
			}
		})
	}
	if servingAuthorityCanUseCurrentPublishedFallback(groupPublicationVersion(currentGeneration, currentPublicationSequence, 142), "", currentPublicationSequence, 142, false) {
		t.Fatal("empty current generation was accepted")
	}
}

func postGroupCandidateStage(t *testing.T, handler http.Handler, value GroupCandidateStageRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, GroupCandidateStagePathV1, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestWorkerCandidateStageKeyringPathIsExact(t *testing.T) {
	publisher := GroupCandidatePublisher{Store: nil}
	if _, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: publisher, GroupIDs: []string{"edge-group-country-de"}, KeyringDir: filepath.Join("relative", "keys")}); err == nil {
		t.Fatal("relative candidate staging keyring was accepted")
	}
}
