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
		receipt.AuthoritySequence != authority.LedgerHead.Sequence || receipt.CandidateBundleGeneration != authority.Published.Bundle.Generation ||
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
	// A transport timeout can leave the candidate committed while the caller
	// retries with the pre-write epoch. Matching the immutable request identity
	// must still reconcile the already committed candidate.
	request.ExpectedCandidateEpoch = existing.Epoch
	request.Nonce = strings.Repeat("s", 24)
	request.Signature = ""
	if err := SignGroupCandidateStageRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	staleReplay := postGroupCandidateStage(t, handler, request)
	if staleReplay.Code != http.StatusOK {
		t.Fatalf("stale replay status=%d body=%s", staleReplay.Code, staleReplay.Body.String())
	}
	afterStaleReplay, _, _ := store.ReadGroupCandidate(ctx, groupID)
	if !reflect.DeepEqual(afterStaleReplay, staged) {
		t.Fatalf("stale replay changed candidate: before=%+v after=%+v", staged, afterStaleReplay)
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
	if !servingAuthorityCanUsePublicationRefresh(serving.BundleVersion, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch, "a") {
		t.Fatal("same-generation serving witness was rejected after candidate history pruning")
	}
	crossGenerationWitness := groupPublicationVersion(renewed.Bundle.Generation, renewed.PublicationSequence, renewed.RecoveryEpoch)
	if servingAuthorityCanUsePublicationRefresh(crossGenerationWitness, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch, "a") {
		t.Fatal("cross-generation serving witness was accepted by fallback")
	}
	futureWitness := groupPublicationVersion(before.Published.Bundle.Generation, renewed.PublicationSequence+1, renewed.RecoveryEpoch)
	if servingAuthorityCanUsePublicationRefresh(futureWitness, before.Published.Bundle.Generation,
		renewed.PublicationSequence, renewed.RecoveryEpoch, "a") {
		t.Fatal("future serving publication was accepted by pruned-history fallback")
	}
}

func TestWorkerCandidateStageAllowsBoundedDegradedPublicationRefresh(t *testing.T) {
	now := time.Date(2026, 8, 13, 10, 15, 0, 0, time.UTC)
	store, _, _, _ := groupPromotionFixture(t, "edge-group-country-de", now)
	authority, err := store.ReadGroupAuthority(context.Background(), "edge-group-country-de")
	if err != nil {
		t.Fatal(err)
	}

	published := authority.Published
	published.PublicationSequence += 2
	published.RecoveryEpoch += 2
	published.Bundle.Version = groupPublicationVersion(published.Bundle.Generation, published.PublicationSequence, published.RecoveryEpoch)
	request := GroupCandidateStageRequest{
		ExpectedPublicationSequence: published.PublicationSequence - 1,
		ExpectedRecoveryEpoch:       published.RecoveryEpoch - 1,
		AllowDegradedPrevious:       true,
	}
	serving := &GroupServingAuthorityWitness{
		BundleVersion: groupPublicationVersion(published.Bundle.Generation, published.PublicationSequence-2, published.RecoveryEpoch-2),
	}

	if !stagePublicationMatchesAuthority(published, request, serving) {
		t.Fatal("explicit degraded recovery rejected an older publication of the same immutable generation")
	}
	request.AllowDegradedPrevious = false
	if stagePublicationMatchesAuthority(published, request, serving) {
		t.Fatal("ordinary transition accepted the degraded publication refresh fallback")
	}
	request.AllowDegradedPrevious = true
	serving.BundleVersion = groupPublicationVersion(published.Bundle.Generation+"-changed", published.PublicationSequence-2, published.RecoveryEpoch-2)
	if stagePublicationMatchesAuthority(published, request, serving) {
		t.Fatal("degraded publication refresh accepted a different route generation")
	}
	serving.BundleVersion = groupPublicationVersion(published.Bundle.Generation, published.PublicationSequence+1, published.RecoveryEpoch)
	if stagePublicationMatchesAuthority(published, request, serving) {
		t.Fatal("degraded publication refresh accepted a future serving publication")
	}
}

func TestServingAuthorityPublicationRefreshSurvivesPrunedHistory(t *testing.T) {
	const generation = "edgegroupbundle_same-generation"
	if !servingAuthorityCanUsePublicationRefresh(groupPublicationVersion(generation, 26702, 513), generation, 26746, 513, "a") {
		t.Fatal("same-generation validity refresh with pruned history was rejected")
	}
	for name, version := range map[string]string{
		"different generation": groupPublicationVersion("edgegroupbundle_other", 26702, 513),
		"future publication":   groupPublicationVersion(generation, 26747, 513),
		"future recovery":      groupPublicationVersion(generation, 26702, 514),
	} {
		t.Run(name, func(t *testing.T) {
			if servingAuthorityCanUsePublicationRefresh(version, generation, 26746, 513, "a") {
				t.Fatalf("unsafe publication refresh accepted: %s", version)
			}
		})
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

func TestWorkerCandidateStageRejectsHistoricalServingPublication(t *testing.T) {
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
	if recorder.Code != http.StatusConflict {
		t.Fatalf("historical serving publication status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	staged, exists, err := store.ReadGroupCandidate(ctx, groupID)
	if err != nil || exists {
		t.Fatalf("historical serving candidate was stored: staged=%+v exists=%v err=%v", staged, exists, err)
	}
	authorityAfter, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || !reflect.DeepEqual(authorityAfter, authorityBefore) {
		t.Fatalf("rejected historical staging changed current authority: before=%+v after=%+v err=%v", authorityBefore, authorityAfter, err)
	}
}

func TestServingAuthorityFallbacksRejectHistoricalPublications(t *testing.T) {
	currentGeneration := "edgegroupbundle_" + strings.Repeat("a", 64)
	currentPublicationSequence, currentRecoveryEpoch := uint64(22855), uint64(388)
	for name, version := range map[string]string{
		"cross generation":    groupPublicationVersion("pruned-generation", 19710, 328),
		"normalized ahead":    groupPublicationVersion("normalized-generation", currentPublicationSequence+388, 0),
		"previous recovery":   groupPublicationVersion("previous-recovery-generation", currentPublicationSequence-1, currentRecoveryEpoch-1),
		"future publication":  groupPublicationVersion(currentGeneration, currentPublicationSequence+1, currentRecoveryEpoch),
		"future recovery":     groupPublicationVersion(currentGeneration, currentPublicationSequence, currentRecoveryEpoch+1),
		"malformed authority": "not-a-publication",
	} {
		t.Run(name, func(t *testing.T) {
			if servingAuthorityCanUsePublicationRefresh(version, currentGeneration, currentPublicationSequence, currentRecoveryEpoch, "a") {
				t.Fatalf("historical serving publication was accepted: %s", version)
			}
		})
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
