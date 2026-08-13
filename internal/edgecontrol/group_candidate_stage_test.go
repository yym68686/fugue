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
		"stale-candidate": {func(v *GroupCandidateStageRequest) { v.ExpectedCandidateEpoch++ }, http.StatusConflict},
		"same-slot":       {func(v *GroupCandidateStageRequest) { v.TargetWorkerSlot = "a" }, http.StatusBadRequest},
		"cross-group":     {func(v *GroupCandidateStageRequest) { v.GroupID = "edge-group-country-us" }, http.StatusForbidden},
		"bad-signature":   {func(v *GroupCandidateStageRequest) {}, http.StatusUnauthorized},
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
