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
	current, err := store.ReadGroupAuthority(ctx, groupID)
	if err != nil || current.Published.Bundle.Generation == servingAuthority.Published.Bundle.Generation {
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

func TestServingAuthorityWithinCurrentRecovery(t *testing.T) {
	currentGeneration := "edgegroupbundle_" + strings.Repeat("a", 64)
	currentVersion := groupPublicationVersion(currentGeneration, 12238, 142)
	currentPublicationSequence := uint64(12244)
	tests := []struct {
		name    string
		version string
		want    bool
	}{
		{name: "stored current bundle publication", version: currentVersion, want: true},
		{name: "older publication", version: groupPublicationVersion("pruned-generation", 12237, 142), want: true},
		{name: "exact republished current generation", version: groupPublicationVersion(currentGeneration, currentPublicationSequence, 142), want: true},
		{name: "same sequence different generation", version: groupPublicationVersion("other-generation", currentPublicationSequence, 142)},
		{name: "future publication", version: groupPublicationVersion(currentGeneration, currentPublicationSequence+1, 142)},
		{name: "previous recovery", version: groupPublicationVersion(currentGeneration, 12237, 141)},
		{name: "malformed", version: "not-a-publication"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := servingAuthorityWithinCurrentRecovery(test.version, currentVersion, currentPublicationSequence, 142); got != test.want {
				t.Fatalf("servingAuthorityWithinCurrentRecovery(%q)=%t want %t", test.version, got, test.want)
			}
		})
	}
	if servingAuthorityWithinCurrentRecovery(currentVersion, groupPublicationVersion(currentGeneration, currentPublicationSequence+1, 142), currentPublicationSequence, 142) {
		t.Fatal("current version metadata mismatch was accepted")
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
