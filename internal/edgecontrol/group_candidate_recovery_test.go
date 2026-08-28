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

func TestGroupCandidateRecoveryFencesOnlyExplicitFailedCandidate(t *testing.T) {
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	store, signer, candidate, authority := groupPromotionFixture(t, groupID, now)
	keyringDir := privateFixtureDir(t)
	secret := bytes.Repeat([]byte{0x62}, 32)
	writeGroupRecoveryFixture(t, keyringDir, groupID, secret, now.Add(time.Minute))
	stageHandler, err := NewGroupCandidateStageHandler(GroupCandidateStageHandlerConfig{Publisher: GroupCandidatePublisher{
		Store: store, Signer: &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x73}, 32)}, validFor: 30 * time.Minute},
		CurrentLKG: &GroupAuthorityPublisher{Store: store, Signer: &fixtureGroupSigner{keys: map[string][]byte{groupID: bytes.Repeat([]byte{0x73}, 32)}, validFor: 30 * time.Minute}},
		Identity:   CandidateReleaseIdentity{SourceSHA: strings.Repeat("1", 40), ControlImageDigest: "sha256:" + strings.Repeat("2", 64), ManifestDigest: "sha256:" + strings.Repeat("3", 64), HealthContractDigest: "sha256:" + strings.Repeat("4", 64), ReleaseRecordDigest: "sha256:" + strings.Repeat("5", 64)},
	}, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Now: func() time.Time { return now.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	stageRequest := GroupCandidateStageRequest{Schema: GroupCandidateStageRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: authority.LedgerHead.Sequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: candidate.Epoch, ExpectedCurrentWorkerSlot: "a", TargetWorkerSlot: "b",
		WorkerSourceSHA: strings.Repeat("e", 40), WorkerImageDigest: "sha256:" + strings.Repeat("f", 64), ReleaseRecordDigest: "sha256:" + strings.Repeat("8", 64),
		IssuedAtUnix: now.Add(time.Minute).Unix(), ExpiresAtUnix: now.Add(2 * time.Minute).Unix(), Nonce: strings.Repeat("s", 24), Reason: "stage failed candidate fixture"}
	if err := SignGroupCandidateStageRequest(&stageRequest, secret); err != nil {
		t.Fatal(err)
	}
	stageRaw, _ := json.Marshal(stageRequest)
	stageHTTP := httptest.NewRequest(http.MethodPost, GroupCandidateStagePathV1, bytes.NewReader(stageRaw))
	stageHTTP.Header.Set("Content-Type", "application/json")
	stageRecorder := httptest.NewRecorder()
	stageHandler.ServeHTTP(stageRecorder, stageHTTP)
	if stageRecorder.Code != http.StatusOK {
		t.Fatalf("seed staged candidate status=%d body=%s", stageRecorder.Code, stageRecorder.Body.String())
	}
	candidate, _, err = store.ReadGroupCandidate(context.Background(), groupID)
	if err != nil {
		t.Fatal(err)
	}
	authority, err = store.ReadGroupAuthority(context.Background(), groupID)
	if err != nil {
		t.Fatal(err)
	}
	refreshedAt := now.Add(16 * time.Minute)
	if result, err := (GroupAuthorityPublisher{Store: store, Signer: signer}).RefreshPublishedLKG(context.Background(), groupID, refreshedAt); err != nil || result.Status != GroupAuthorityStatusPublished {
		t.Fatalf("refresh published LKG: result=%+v err=%v", result, err)
	}
	authority, err = store.ReadGroupAuthority(context.Background(), groupID)
	if err != nil || candidate.Epoch > authority.Published.PublicationSequence {
		t.Fatalf("fixture candidate was not superseded by the refreshed publication: candidate=%+v authority=%+v err=%v", candidate, authority, err)
	}
	requestAuthoritySequence := authority.LedgerHead.Sequence
	failed := GroupAuthorityLedgerEntry{Schema: GroupAuthorityLedgerSchemaV1, GroupID: groupID, Status: GroupAuthorityStatusFailed,
		CandidateLedgerSequence: authority.Published.CandidateLedgerSequence, RouteIntentGeneration: "candidate-recovery-audit-tail",
		LastPublishedBundleGeneration: authority.Published.Bundle.Generation, FailureCode: GroupAuthorityFailureSigning,
		Authority: "edge-control", PublicationEnabled: true, RecordedAt: refreshedAt.Add(time.Minute)}
	if _, err := store.AppendGroupAuthorityCAS(context.Background(), groupID, requestAuthoritySequence,
		authority.Published.CandidateLedgerSequence, failed, nil); err != nil {
		t.Fatalf("append publication-preserving audit tail: %v", err)
	}
	recoveryMetrics := NewGroupCandidateRecoveryMetrics()
	handler, err := NewGroupCandidateRecoveryHandler(GroupCandidateRecoveryHandlerConfig{
		Store: store, GroupIDs: []string{groupID}, KeyringDir: keyringDir, Metrics: recoveryMetrics, Now: func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := GroupCandidateRecoveryRequest{
		Schema: GroupCandidateRecoveryRequestSchemaV1, KeyID: "recovery-de-1", GroupID: groupID,
		ExpectedAuthoritySequence: requestAuthoritySequence, ExpectedPublicationSequence: authority.Published.PublicationSequence,
		ExpectedRecoveryEpoch: authority.Published.RecoveryEpoch, ExpectedPublishedBundleDigest: authority.Published.Digest,
		ExpectedCandidateEpoch: candidate.Epoch, ExpectedWorkerSourceSHA: candidate.WorkerSourceSHA,
		IssuedAtUnix: now.Add(time.Minute).Unix(), ExpiresAtUnix: now.Add(2 * time.Minute).Unix(), Nonce: strings.Repeat("n", 24), Reason: "fence failed candidate before retry",
	}
	if err := SignGroupCandidateRecoveryRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	raw, _ := json.Marshal(request)
	fenceRequest := httptest.NewRequest(http.MethodPost, GroupCandidateRecoveryPathV1, bytes.NewReader(raw))
	fenceRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recorder, fenceRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("fence status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var receipt GroupCandidateRecoveryReceipt
	if err := json.Unmarshal(recorder.Body.Bytes(), &receipt); err != nil || !receipt.CandidateCleared || receipt.FencedCandidateEpoch != candidate.Epoch || receipt.FencedWorkerSourceSHA != candidate.WorkerSourceSHA {
		t.Fatalf("fence receipt=%+v err=%v", receipt, err)
	}
	if _, exists, err := store.ReadGroupCandidate(context.Background(), groupID); err != nil || exists {
		t.Fatalf("candidate pointer remains after fence: exists=%t err=%v", exists, err)
	}
	current, err := store.ReadGroupAuthority(context.Background(), groupID)
	if err != nil || current.Published.Digest != authority.Published.Digest || current.Published.RecoveryEpoch != authority.Published.RecoveryEpoch {
		t.Fatalf("fence changed published authority: current=%+v err=%v", current, err)
	}
	// A lost fence response is idempotently recoverable from bounded history.
	replay := httptest.NewRecorder()
	request.Nonce = strings.Repeat("r", 24)
	request.Signature = ""
	if err := SignGroupCandidateRecoveryRequest(&request, secret); err != nil {
		t.Fatal(err)
	}
	replayRaw, _ := json.Marshal(request)
	replayRequest := httptest.NewRequest(http.MethodPost, GroupCandidateRecoveryPathV1, bytes.NewReader(replayRaw))
	replayRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusOK {
		t.Fatalf("fence replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if metrics := recoveryMetrics.Snapshot(); metrics.Accepted != 2 || metrics.Conflict != 0 || metrics.Rejected != 0 || metrics.Unavailable != 0 {
		t.Fatalf("candidate recovery metrics after accepted request and replay = %+v", metrics)
	}
}

func TestGroupCandidateRecoveryMetricsTrackAllOutcomes(t *testing.T) {
	metrics := NewGroupCandidateRecoveryMetrics()
	for _, result := range []string{"accepted", "conflict", "rejected", "unavailable"} {
		metrics.observe(result)
	}
	if snapshot := metrics.Snapshot(); snapshot.Accepted != 1 || snapshot.Conflict != 1 || snapshot.Rejected != 1 || snapshot.Unavailable != 1 {
		t.Fatalf("candidate recovery metrics = %+v", snapshot)
	}
}
