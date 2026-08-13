package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/releaseguardian"
)

func TestGroupAuthorityPromotionReplaysOnlyUnknownExactRequest(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 0, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	requests := 0
	var firstRaw []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests++
		raw := make([]byte, request.ContentLength)
		_, _ = request.Body.Read(raw)
		if requests == 1 {
			firstRaw = append([]byte(nil), raw...)
			panic(http.ErrAbortHandler)
		}
		if !bytes.Equal(raw, firstRaw) {
			t.Fatal("unknown promotion did not replay exact signed bytes")
		}
		_ = json.NewEncoder(w).Encode(edgecontrol.GroupPromotionReceipt{Schema: edgecontrol.GroupPromotionReceiptSchemaV1,
			GroupID: target.GroupID, PreviousAuthoritySequence: target.AuthoritySequence,
			PreviousPublicationSequence: target.PublicationSequence, PreviousRecoveryEpoch: target.RecoveryEpoch,
			PreviousBundleGeneration: target.PreviousServingGeneration, PreviousPublishedBundleDigest: target.PublishedBundleDigest,
			PublicationSequence: target.AuthoritySequence + 1, RecoveryEpoch: target.RecoveryEpoch,
			BundleGeneration: target.ServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("9", 64),
			CandidateRecordDigest: target.CandidateRecordDigest, WorkerSlot: string(target.TargetSlot), Authority: "edge-control"})
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	receipt, err := activator.promoteControl(context.Background(), target)
	if err != nil || requests != 2 || receipt.PublicationSequence != target.AuthoritySequence+1 {
		t.Fatalf("promotion replay requests=%d receipt=%+v err=%v", requests, receipt, err)
	}
}

func TestGroupAuthorityPromotionAcceptsFailedAuditTailReceipt(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 15, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(edgecontrol.GroupPromotionReceipt{Schema: edgecontrol.GroupPromotionReceiptSchemaV1,
			GroupID: target.GroupID, PreviousAuthoritySequence: target.AuthoritySequence + 3,
			PreviousPublicationSequence: target.PublicationSequence, PreviousRecoveryEpoch: target.RecoveryEpoch,
			PreviousBundleGeneration: target.PreviousServingGeneration, PreviousPublishedBundleDigest: target.PublishedBundleDigest,
			PublicationSequence: target.AuthoritySequence + 4, RecoveryEpoch: target.RecoveryEpoch,
			BundleGeneration: target.ServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("9", 64),
			CandidateRecordDigest: target.CandidateRecordDigest, WorkerSlot: string(target.TargetSlot), Authority: "edge-control"})
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	receipt, err := activator.promoteControl(context.Background(), target)
	if err != nil || receipt.PreviousAuthoritySequence != target.AuthoritySequence+3 ||
		receipt.PublicationSequence != target.AuthoritySequence+4 {
		t.Fatalf("audit-tail receipt=%+v err=%v", receipt, err)
	}
}

func TestGroupAuthorityPromotionTypesOnlyExplicitConflictAsPrewriteCAS(t *testing.T) {
	for name, status := range map[string]int{"sequence_conflict": http.StatusConflict, "candidate_conflict": http.StatusConflict, "unavailable": http.StatusServiceUnavailable} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": name})
			}))
			defer server.Close()
			activator := &groupAuthorityActivator{config: groupAuthorityConfig{Endpoint: server.URL}, client: server.Client()}
			err := activator.post(context.Background(), edgecontrol.GroupPromotionPathV1, map[string]string{"value": "test"}, &struct{}{})
			if (name != "unavailable") != errors.Is(err, errAuthorityPrewriteCASChanged) {
				t.Fatalf("status=%d error=%v", status, err)
			}
		})
	}
}

func TestGroupAuthorityRecoveryUnknownUsesReadOnlyReconcile(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 30, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	promotion := edgecontrol.GroupPromotionReceipt{GroupID: target.GroupID, PublicationSequence: target.AuthoritySequence + 1,
		RecoveryEpoch: target.RecoveryEpoch, PreviousBundleGeneration: target.PreviousServingGeneration}
	posts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			posts++
			panic(http.ErrAbortHandler)
		case http.MethodGet:
			gets++
			_ = json.NewEncoder(w).Encode(edgecontrol.AuthorityGroupStatus{GroupID: target.GroupID,
				PublicationSequence: promotion.PublicationSequence + 1, RecoveryEpoch: promotion.RecoveryEpoch + 1,
				BundleGeneration: target.PreviousServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("8", 64)})
		}
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	if err := activator.recoverControl(context.Background(), promotion, target.PreviousServingGeneration); err != nil || posts != 1 || gets != 1 {
		t.Fatalf("recovery reconcile posts=%d gets=%d err=%v", posts, gets, err)
	}
}

func TestEdgeControlCompensationSettlementRequiresExactLKGAndMonotonicRecovery(t *testing.T) {
	target := groupAuthorityTargetFixture()
	candidate := releaseguardian.CandidateAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind,
		GroupID: target.GroupID, RecordDigest: target.CandidateRecordDigest, BundleGeneration: target.CandidateBundleGeneration,
		ServingGeneration: target.ServingGeneration, AuthoritySequence: target.AuthoritySequence, CandidateSequence: 9,
		CurrentPublicationSequence: target.PublicationSequence, CurrentRecoveryEpoch: target.RecoveryEpoch,
		CurrentBundleDigest: target.PublishedBundleDigest, CurrentServingGeneration: target.PreviousServingGeneration,
		CandidateEpoch: target.CandidateEpoch, WorkerSlot: target.TargetSlot, ReleaseRecordDigest: target.CanaryResultDigest,
		State: releaseguardian.CandidateAuthorityVerified, Generation: 2, CanaryResultDigest: target.CanaryResultDigest}
	journal, err := (releaseguardian.AuthorityTransitionJournal{GroupID: target.GroupID, Phase: releaseguardian.AuthorityTransitionActivated,
		CurrentUID: "current-uid", CurrentRV: "20", Before: releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion,
			Kind: releaseguardian.CurrentAuthorityKind, GroupID: target.GroupID, CurrentRecordDigest: target.PublishedBundleDigest,
			CurrentWorkerSlot: releaseguardian.AuthoritySlotB, AuthorityEpoch: 7}, Candidate: candidate,
		CanaryResultDigest: target.CanaryResultDigest, PreviousNodes: []releaseguardian.AuthorityBaselineNodeWitness{{NodeName: "edge-node-a",
			FrontPodUID: "front-pod-uid", FrontResourceVersion: "10", WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11",
			ActivationGeneration: 7, BundleGeneration: "previous-bundle-7", ServingGeneration: "previous-serving-7",
			WorkerSourceSHA: target.WorkerSourceSHA, WorkerImageDigest: target.WorkerImageDigest}},
		Activation: &releaseguardian.FrontAuthorityReceipt{GroupID: target.GroupID, PreviousSlot: releaseguardian.AuthoritySlotB,
			PreviousGeneration: 7, PreviousBundleGeneration: "previous-bundle-7", PreviousWorkerSourceSHA: target.WorkerSourceSHA,
			PreviousWorkerImageDigest: target.WorkerImageDigest, TargetSlot: target.TargetSlot, TargetGeneration: 8,
			TargetBundleGeneration: target.ServingGeneration + ".p12.r3", TargetWorkerSourceSHA: target.WorkerSourceSHA,
			TargetWorkerImageDigest: target.WorkerImageDigest}, CreatedAt: time.Date(2026, 8, 13, 5, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	status := edgecontrol.AuthorityGroupStatus{GroupID: target.GroupID, PublicationSequence: target.AuthoritySequence + 3,
		RecoveryEpoch: target.RecoveryEpoch + 2, BundleGeneration: target.PreviousServingGeneration,
		PublishedBundleDigest: "sha256:" + strings.Repeat("8", 64), LKGState: edgecontrol.GroupAuthorityLKGCurrent}
	if !edgeControlCompensationSettled(status, journal) {
		t.Fatal("monotonically refreshed exact LKG was not settled")
	}
	for name, mutate := range map[string]func(*edgecontrol.AuthorityGroupStatus){
		"stale publication": func(value *edgecontrol.AuthorityGroupStatus) {
			value.PublicationSequence = target.AuthoritySequence + 1
		},
		"stale recovery":   func(value *edgecontrol.AuthorityGroupStatus) { value.RecoveryEpoch = target.RecoveryEpoch },
		"wrong generation": func(value *edgecontrol.AuthorityGroupStatus) { value.BundleGeneration = target.ServingGeneration },
		"missing lkg":      func(value *edgecontrol.AuthorityGroupStatus) { value.LKGState = edgecontrol.GroupAuthorityLKGMissing },
	} {
		t.Run(name, func(t *testing.T) {
			changed := status
			mutate(&changed)
			if edgeControlCompensationSettled(changed, journal) {
				t.Fatal("invalid recovery witness was settled")
			}
		})
	}
}

func groupAuthorityTargetFixture() releaseguardian.FrontAuthorityTarget {
	return releaseguardian.FrontAuthorityTarget{GroupID: "edge-group-country-de", TargetSlot: releaseguardian.AuthoritySlotA,
		CandidateBundleGeneration: "candidate-full.p20.r0", ServingGeneration: "candidate-base", FrontBundleGeneration: "candidate-base.p12.r3",
		AuthoritySequence: 11, PublicationSequence: 10, RecoveryEpoch: 3, PublishedBundleDigest: "sha256:" + strings.Repeat("1", 64),
		PreviousServingGeneration: "previous-base", CandidateEpoch: 20, WorkerSourceSHA: strings.Repeat("2", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("3", 64), WorkerCohortDigest: "sha256:" + strings.Repeat("4", 64),
		CandidateRecordDigest: "sha256:" + strings.Repeat("5", 64), CanaryResultDigest: "sha256:" + strings.Repeat("6", 64)}
}

func groupAuthorityActivatorFixture(t *testing.T, endpoint, group string, now time.Time) *groupAuthorityActivator {
	t.Helper()
	directory := t.TempDir()
	keyring := authorityRecoveryKeyring{Schema: edgecontrol.GroupRecoveryKeyringSchemaV1, Generation: 1, GroupID: group,
		Keys: []authorityRecoveryKey{{KeyID: "recovery-de-1", Secret: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)),
			NotBeforeUnix: now.Add(-time.Hour).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()}}}
	raw, _ := json.Marshal(keyring)
	path := filepath.Join(directory, "keyring.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	activator := &groupAuthorityActivator{config: groupAuthorityConfig{GroupID: group, Endpoint: endpoint, KeyringFile: path},
		client: &http.Client{Timeout: time.Second}, now: func() time.Time { return now }}
	activator.config.SlotA, activator.config.SlotB = "192.0.2.10:18443", "192.0.2.10:28443"
	if _, _, err := activator.activeKey(now); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return activator
}
