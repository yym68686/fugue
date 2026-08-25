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
	"k8s.io/client-go/kubernetes/fake"
)

func TestAuthorityRouteMatchesAttestedCurrentOrExactLegacyLKG(t *testing.T) {
	body := []byte("route-ok")
	bodyDigest := shaDigest(body)
	record := "sha256:" + strings.Repeat("a", 64)
	headers := http.Header{"X-Fugue-Candidate-Record-Digest": []string{record}, "X-Fugue-Candidate-Worker-Slot": []string{"b"}}
	if !authorityRouteMatches(http.StatusOK, body, headers, nil, bodyDigest, record, releaseguardian.AuthoritySlotB, false) {
		t.Fatal("exact attested route was rejected")
	}
	if authorityRouteMatches(http.StatusOK, body, http.Header{}, nil, bodyDigest, record, releaseguardian.AuthoritySlotB, false) {
		t.Fatal("current route accepted without attestation")
	}
	if !authorityRouteMatches(http.StatusOK, body, http.Header{}, nil, bodyDigest, record, releaseguardian.AuthoritySlotB, true) {
		t.Fatal("legacy LKG route without candidate headers was rejected")
	}
	for name, changed := range map[string]http.Header{
		"partial record": {"X-Fugue-Candidate-Record-Digest": []string{record}},
		"partial slot":   {"X-Fugue-Candidate-Worker-Slot": []string{"b"}},
		"wrong record":   {"X-Fugue-Candidate-Record-Digest": []string{"sha256:" + strings.Repeat("c", 64)}, "X-Fugue-Candidate-Worker-Slot": []string{"b"}},
		"wrong slot":     {"X-Fugue-Candidate-Record-Digest": []string{record}, "X-Fugue-Candidate-Worker-Slot": []string{"a"}},
	} {
		t.Run(name, func(t *testing.T) {
			if authorityRouteMatches(http.StatusOK, body, changed, nil, bodyDigest, record, releaseguardian.AuthoritySlotB, true) {
				t.Fatal("invalid LKG attestation was accepted")
			}
		})
	}
}

func TestCurrentCodeRouteAcceptsIndependentConfigurationRecord(t *testing.T) {
	body := []byte("route-ok")
	bodyDigest := shaDigest(body)
	headers := http.Header{"X-Fugue-Candidate-Record-Digest": []string{"sha256:" + strings.Repeat("c", 64)},
		"X-Fugue-Candidate-Worker-Slot": []string{"a"}}
	if !authorityCurrentRouteMatches(http.StatusOK, body, headers, nil, bodyDigest, releaseguardian.AuthoritySlotA) {
		t.Fatal("independent signed configuration record was treated as code drift")
	}
	for name, mutate := range map[string]func(http.Header){
		"missing record": func(value http.Header) { value.Del("X-Fugue-Candidate-Record-Digest") },
		"wrong slot":     func(value http.Header) { value.Set("X-Fugue-Candidate-Worker-Slot", "b") },
	} {
		t.Run(name, func(t *testing.T) {
			changed := headers.Clone()
			mutate(changed)
			if authorityCurrentRouteMatches(http.StatusOK, body, changed, nil, bodyDigest, releaseguardian.AuthoritySlotA) {
				t.Fatal("invalid code route identity was accepted")
			}
		})
	}
}

func TestAuthorityWorkerHealthAcceptsMonotonicControlPublication(t *testing.T) {
	group := "edge-group-country-de"
	bundle := "serving-generation.p11481.r129"
	health := baselineWorkerHealth{Healthy: true, EdgeGroupID: group, BundleVersion: bundle, PublicationSequence: 11481, ServingGeneration: "serving-generation"}
	if !authorityWorkerHealthAtOrAfter(health, group, bundle) {
		t.Fatal("exact Worker route generation was rejected")
	}
	advanced := health
	advanced.BundleVersion = "serving-generation.p11486.r131"
	advanced.PublicationSequence = 11486
	if !authorityWorkerHealthAtOrAfter(advanced, group, bundle) {
		t.Fatal("monotonic Edge Control publication was rejected")
	}
	if authorityWorkerHealthMatches(advanced, group, bundle) {
		t.Fatal("candidate activation accepted a non-exact configuration publication")
	}
	advanced.BundleVersion = "new-route-intent.p11487.r0"
	advanced.PublicationSequence = 11487
	advanced.ServingGeneration = "new-route-intent"
	if !authorityWorkerHealthAtOrAfter(advanced, group, bundle) {
		t.Fatal("newer independent configuration generation was rejected")
	}
	for name, mutate := range map[string]func(*baselineWorkerHealth){
		"unhealthy":    func(value *baselineWorkerHealth) { value.Healthy = false },
		"wrong group":  func(value *baselineWorkerHealth) { value.EdgeGroupID = "edge-group-other" },
		"wrong bundle": func(value *baselineWorkerHealth) { value.BundleVersion = "other-generation.p11481.r129" },
		"stale publication": func(value *baselineWorkerHealth) {
			value.BundleVersion = "serving-generation.p11480.r129"
			value.PublicationSequence = 11480
		},
		"stale recovery": func(value *baselineWorkerHealth) { value.BundleVersion = "serving-generation.p11481.r128" },
		"wrong epoch":    func(value *baselineWorkerHealth) { value.PublicationSequence++ },
		"wrong serving":  func(value *baselineWorkerHealth) { value.ServingGeneration += "-other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := health
			mutate(&changed)
			if authorityWorkerHealthAtOrAfter(changed, group, bundle) {
				t.Fatal("invalid Worker route generation was accepted")
			}
		})
	}
}

func TestGroupAuthorityRestoreNeverMutatesControlPublication(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	group := "edge-group-country-us"
	front, err := newFrontAuthorityActivator(fake.NewSimpleClientset(), baselineActivationExecutor{}, frontAuthorityConfig{
		GroupID: group, Namespace: "fugue-system", ExpectedNodes: 1,
	}, "guardian-pod:edge-group-country-us")
	if err != nil {
		t.Fatal(err)
	}
	activator := &groupAuthorityActivator{front: front, config: groupAuthorityConfig{GroupID: group, Endpoint: server.URL}}
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: group, CurrentRecordDigest: "sha256:" + strings.Repeat("1", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 29, CurrentBundleGeneration: "routes.p39713.r124", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("3", 64), PreviousRecordDigest: "sha256:" + strings.Repeat("4", 64),
		PreviousWorkerSlot: releaseguardian.AuthoritySlotB, PreviousFrontGeneration: 28, PreviousBundleGeneration: "routes.p39680.r120",
		PreviousWorkerSourceSHA: strings.Repeat("5", 40), PreviousWorkerImageDigest: "sha256:" + strings.Repeat("6", 64), AuthorityEpoch: 8}
	if _, err := activator.BeginRestore(context.Background(), current); err == nil {
		t.Fatal("restore unexpectedly found a Front cohort")
	}
	if requests != 0 {
		t.Fatalf("code rollback mutated Edge Control publication: requests=%d", requests)
	}
}

func TestNewGroupAuthorityActivatorBudgetsPersistentMutationAndReplay(t *testing.T) {
	group := "edge-group-country-de"
	front := &frontAuthorityActivator{config: frontAuthorityConfig{GroupID: group}}
	activator, err := newGroupAuthorityActivator(front, groupAuthorityConfig{GroupID: group,
		Endpoint: "http://127.0.0.1:8080", KeyringFile: filepath.Join(t.TempDir(), "keyring.json"),
		SlotA: "192.0.2.10:18443", SlotB: "192.0.2.10:28443"})
	if err != nil {
		t.Fatal(err)
	}
	if activator.client.Timeout != authorityMutationTimeout || authorityMutationTimeout+authorityReconcileTimeout >= authorityRequestTTL {
		t.Fatalf("authority mutation budget cannot complete an exact signed replay: timeout=%s reconcile=%s ttl=%s",
			activator.client.Timeout, authorityReconcileTimeout, authorityRequestTTL)
	}
}

func TestGroupAuthorityPromotionReplaysOnlyUnknownExactRequest(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 0, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	requests := 0
	var firstRaw []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
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

func TestGroupAuthorityPromotionReconcilesCommittedResponseLoss(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 5, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	requests, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			gets++
			validUntil := now.Add(time.Minute)
			_ = json.NewEncoder(w).Encode(edgecontrol.AuthorityGroupStatus{GroupID: target.GroupID,
				AuthoritySequence: target.AuthoritySequence + 1, CurrentPublicationSequence: target.AuthoritySequence + 1, RecoveryEpoch: target.RecoveryEpoch,
				CandidateEpoch: target.CandidateEpoch, CandidateWorkerSourceSHA: target.WorkerSourceSHA,
				BundleGeneration: target.ServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("9", 64),
				BundleValidUntil: &validUntil})
			return
		}
		requests++
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	receipt, err := activator.promoteControl(context.Background(), target)
	if err != nil || requests != 1 || gets != 1 || receipt.PublicationSequence != target.AuthoritySequence+1 ||
		receipt.BundleGeneration != target.ServingGeneration {
		t.Fatalf("promotion response-loss reconciliation requests=%d gets=%d receipt=%+v err=%v", requests, gets, receipt, err)
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

func TestGroupAuthorityRecoveryUsesReadOnlyReconcileBeforePosting(t *testing.T) {
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
			validUntil := now.Add(time.Minute)
			_ = json.NewEncoder(w).Encode(edgecontrol.AuthorityGroupStatus{GroupID: target.GroupID,
				PublicationSequence: promotion.PublicationSequence + 9, CurrentPublicationSequence: promotion.PublicationSequence + 1,
				RecoveryEpoch: promotion.RecoveryEpoch + 1, BundleGeneration: target.PreviousServingGeneration,
				PublishedBundleDigest: "sha256:" + strings.Repeat("8", 64), BundleValidUntil: &validUntil})
		}
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	if err := activator.recoverControl(context.Background(), promotion, target.PreviousServingGeneration); err != nil || posts != 0 || gets != 1 {
		t.Fatalf("recovery reconcile posts=%d gets=%d err=%v", posts, gets, err)
	}
}

func TestGroupAuthorityRecoveryRefreshesExpiredExactLKGAtPublishedCAS(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 35, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	promotion := edgecontrol.GroupPromotionReceipt{GroupID: target.GroupID, PublicationSequence: target.AuthoritySequence + 1,
		RecoveryEpoch: target.RecoveryEpoch, PreviousBundleGeneration: target.PreviousServingGeneration}
	posts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			gets++
			expired := now.Add(-time.Minute)
			_ = json.NewEncoder(w).Encode(edgecontrol.AuthorityGroupStatus{GroupID: target.GroupID,
				PublicationSequence: promotion.PublicationSequence + 7, CurrentPublicationSequence: promotion.PublicationSequence + 4,
				RecoveryEpoch: promotion.RecoveryEpoch + 2, BundleGeneration: target.PreviousServingGeneration,
				PublishedBundleDigest: "sha256:" + strings.Repeat("8", 64), BundleValidUntil: &expired})
		case http.MethodPost:
			posts++
			var recovery edgecontrol.GroupRecoveryRequest
			if json.NewDecoder(request.Body).Decode(&recovery) != nil || recovery.ExpectedPublicationSequence != promotion.PublicationSequence+4 ||
				recovery.ExpectedRecoveryEpoch != promotion.RecoveryEpoch+2 {
				t.Fatal("expired recovery did not bind the current published CAS")
			}
			_ = json.NewEncoder(w).Encode(edgecontrol.GroupRecoveryReceipt{Schema: edgecontrol.GroupRecoveryReceiptSchemaV1,
				GroupID: target.GroupID, PublicationSequence: promotion.PublicationSequence + 8, RecoveryEpoch: promotion.RecoveryEpoch + 3,
				BundleGeneration: target.PreviousServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("9", 64),
				Authority: "edge-control", PublicationEnabled: true})
		}
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	receipt, err := activator.recoverControlReceipt(context.Background(), promotion, target.PreviousServingGeneration)
	if err != nil || gets != 2 || posts != 1 || receipt.PublicationSequence != promotion.PublicationSequence+8 {
		t.Fatalf("expired recovery refresh gets=%d posts=%d receipt=%+v err=%v", gets, posts, receipt, err)
	}
}

func TestGroupAuthorityRecoveryAcceptsMonotonicAuditTailReceipt(t *testing.T) {
	now := time.Date(2026, 8, 13, 5, 45, 0, 0, time.UTC)
	target := groupAuthorityTargetFixture()
	promotion := edgecontrol.GroupPromotionReceipt{GroupID: target.GroupID, PublicationSequence: target.AuthoritySequence + 1,
		RecoveryEpoch: target.RecoveryEpoch, PreviousBundleGeneration: target.PreviousServingGeneration}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(edgecontrol.GroupRecoveryReceipt{Schema: edgecontrol.GroupRecoveryReceiptSchemaV1,
			GroupID: target.GroupID, PublicationSequence: promotion.PublicationSequence + 4, RecoveryEpoch: promotion.RecoveryEpoch + 1,
			BundleGeneration: target.PreviousServingGeneration, PublishedBundleDigest: "sha256:" + strings.Repeat("8", 64),
			Authority: "edge-control", PublicationEnabled: true})
	}))
	defer server.Close()
	activator := groupAuthorityActivatorFixture(t, server.URL, target.GroupID, now)
	receipt, err := activator.recoverControlReceipt(context.Background(), promotion, target.PreviousServingGeneration)
	if err != nil || receipt.PublicationSequence != promotion.PublicationSequence+4 {
		t.Fatalf("audit-tail recovery receipt=%+v err=%v", receipt, err)
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
