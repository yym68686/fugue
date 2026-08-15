package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgeauthority"
	"fugue/internal/model"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCandidateImporterBootstrapsExactGroupPointersIdempotently(t *testing.T) {
	now := time.Date(2026, 8, 12, 4, 0, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != edgeCandidateEnvelopePathV1 || request.URL.Query().Get("edge_group_id") != groupID ||
			request.URL.Query().Get("edge_id") != "edge-node-a" || request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "rejected", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(envelope)
	}))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}}
	client := fake.NewSimpleClientset(pod)
	store, err := releaseguardian.NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	config := candidateImportConfig{GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile}
	changed, err := importCandidateOnce(context.Background(), store, client, config, now)
	if err != nil || !changed {
		t.Fatalf("first import: changed=%v err=%v", changed, err)
	}
	objects, err := client.CoreV1().ConfigMaps("fugue-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for index := range objects.Items {
		object := &objects.Items[index]
		if _, mutableCurrent := object.Data["authority.json"]; !mutableCurrent {
			if _, mutableCandidate := object.Data["candidate.json"]; !mutableCandidate {
				continue
			}
		}
		object.UID = types.UID("test-" + object.Name)
		object.ResourceVersion = "10"
		if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	current, _, _, err := store.LoadCurrent(context.Background(), groupID)
	if err != nil || current.CurrentRecordDigest != envelope.CurrentRecord.RecordDigest || current.CurrentWorkerSlot != envelope.CurrentWorkerSlot ||
		current.AuthorityEpoch != envelope.CurrentRecord.Epoch || current.PreviousRecordDigest != "" {
		t.Fatalf("current authority=%+v err=%v", current, err)
	}
	candidate, _, _, err := store.LoadCandidate(context.Background(), groupID)
	if err != nil || candidate.RecordDigest != envelope.Record.RecordDigest || candidate.WorkerSlot != envelope.WorkerSlot ||
		candidate.BundleGeneration != envelope.Bundle.Version ||
		candidate.AuthoritySequence != envelope.AuthorityLedgerSequence || candidate.CandidateSequence != envelope.CandidateLedgerSequence ||
		candidate.CurrentPublicationSequence != uint64(envelope.CurrentRecord.Epoch) || candidate.CurrentRecoveryEpoch != 0 ||
		candidate.CurrentBundleDigest != envelope.CurrentRecord.BundleDigest || candidate.CandidateEpoch != envelope.Epoch ||
		candidate.ReleaseRecordDigest != envelope.ReleaseRecordDigest || candidate.State != releaseguardian.CandidateAuthorityLoaded || candidate.Generation != 1 {
		t.Fatalf("candidate authority=%+v err=%v", candidate, err)
	}
	changed, err = importCandidateOnce(context.Background(), store, client, config, now.Add(time.Second))
	if err != nil || changed {
		t.Fatalf("idempotent import: changed=%v err=%v", changed, err)
	}
}

func TestCandidateImporterNeverRefreshesAnExistingCurrentAuthority(t *testing.T) {
	now := time.Date(2026, 8, 12, 4, 15, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}}
	client := fake.NewSimpleClientset(pod)
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: groupID, CurrentRecordDigest: "sha256:" + strings.Repeat("8", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotB,
		PreviousRecordDigest: "sha256:" + strings.Repeat("9", 64), PreviousWorkerSlot: releaseguardian.AuthoritySlotA, AuthorityEpoch: 20}
	if _, _, err := store.SwitchCurrent(context.Background(), current, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-current-authority-"+groupID, "current-authority", "20")
	config := candidateImportConfig{GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile}
	if changed, err := importCandidateOnce(context.Background(), store, client, config, now); err != nil || !changed {
		t.Fatalf("candidate import: changed=%v err=%v", changed, err)
	}
	loaded, _, _, err := store.LoadCurrent(context.Background(), groupID)
	if err != nil || loaded != current {
		t.Fatalf("importer changed Guardian-owned current: current=%+v err=%v", loaded, err)
	}
}

func TestCandidateImporterAcceptsExactServingAuthorityWitness(t *testing.T) {
	now := time.Date(2026, 8, 14, 20, 0, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: groupID, CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 8, CurrentBundleGeneration: "routes-candidate.p3.r1", CurrentWorkerSourceSHA: strings.Repeat("b", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("c", 64), AuthorityEpoch: 23}
	envelope.ServingAuthority = &candidateServingAuthorityWitness{CurrentRecordDigest: current.CurrentRecordDigest, AuthorityEpoch: current.AuthorityEpoch,
		CurrentAuthorityUID: "current-authority", CurrentAuthorityRV: "41", FrontGeneration: current.CurrentFrontGeneration,
		BundleVersion: current.CurrentBundleGeneration, WorkerSlot: current.CurrentWorkerSlot, WorkerSourceSHA: current.CurrentWorkerSourceSHA,
		WorkerImageDigest: current.CurrentWorkerImageDigest}
	envelope.AllowDegradedPrevious = true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}})
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	if _, _, err := store.SwitchCurrent(context.Background(), current, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-current-authority-"+groupID, envelope.ServingAuthority.CurrentAuthorityUID, envelope.ServingAuthority.CurrentAuthorityRV)
	changed, err := importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now)
	if err != nil || !changed {
		t.Fatalf("serving-witness import: changed=%v err=%v", changed, err)
	}
	loaded, uid, rv, err := store.LoadCurrent(context.Background(), groupID)
	if err != nil || loaded != current || string(uid) != envelope.ServingAuthority.CurrentAuthorityUID || rv != envelope.ServingAuthority.CurrentAuthorityRV {
		t.Fatalf("importer changed serving authority: current=%+v uid=%s rv=%s err=%v", loaded, uid, rv, err)
	}
	setMutableAuthorityFixture(t, client, "fugue-candidate-authority-"+groupID, "candidate-authority", "42")
	candidate, _, _, err := store.LoadCandidate(context.Background(), groupID)
	if err != nil || !candidate.AllowDegradedPrevious {
		t.Fatalf("importer omitted degraded previous authorization: candidate=%+v err=%v", candidate, err)
	}
	envelope.AllowDegradedPrevious = false
	envelope.StandbyOnly = true
	changed, err = importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now.Add(time.Second))
	if err != nil || changed {
		t.Fatalf("standby import changed promotable state: changed=%v err=%v", changed, err)
	}
	unchanged, _, _, err := store.LoadCandidate(context.Background(), groupID)
	if err != nil || unchanged != candidate {
		t.Fatalf("standby import replaced candidate: before=%+v after=%+v err=%v", candidate, unchanged, err)
	}
}

func TestCandidateImporterRejectsServingAuthorityCASDriftBeforeWritingRecords(t *testing.T) {
	now := time.Date(2026, 8, 14, 20, 15, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: groupID, CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA,
		CurrentFrontGeneration: 8, CurrentBundleGeneration: "routes-candidate.p3.r1", CurrentWorkerSourceSHA: strings.Repeat("b", 40),
		CurrentWorkerImageDigest: "sha256:" + strings.Repeat("c", 64), AuthorityEpoch: 23}
	envelope.ServingAuthority = &candidateServingAuthorityWitness{CurrentRecordDigest: current.CurrentRecordDigest, AuthorityEpoch: current.AuthorityEpoch,
		CurrentAuthorityUID: "current-authority", CurrentAuthorityRV: "stale-rv", FrontGeneration: current.CurrentFrontGeneration,
		BundleVersion: current.CurrentBundleGeneration, WorkerSlot: current.CurrentWorkerSlot, WorkerSourceSHA: current.CurrentWorkerSourceSHA,
		WorkerImageDigest: current.CurrentWorkerImageDigest}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}})
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	if _, _, err := store.SwitchCurrent(context.Background(), current, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-current-authority-"+groupID, "current-authority", "42")
	changed, err := importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now)
	if err == nil || changed {
		t.Fatalf("stale serving witness was imported: changed=%v err=%v", changed, err)
	}
	if _, _, _, err := store.LoadCandidate(context.Background(), groupID); !apierrors.IsNotFound(err) {
		t.Fatalf("stale serving witness wrote candidate authority: %v", err)
	}
	if _, err := store.LoadRouteBundleRecord(context.Background(), groupID, envelope.Record.RecordDigest); !apierrors.IsNotFound(err) {
		t.Fatalf("stale serving witness wrote immutable candidate record: %v", err)
	}
}

func TestCandidateImporterTreatsPromotedServingWitnessAsSettled(t *testing.T) {
	now := time.Date(2026, 8, 15, 22, 0, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	previousSource := strings.Repeat("a", 40)
	previousImage := "sha256:" + strings.Repeat("b", 64)
	envelope.ServingAuthority = &candidateServingAuthorityWitness{
		CurrentRecordDigest: envelope.CurrentRecord.RecordDigest, AuthorityEpoch: 23,
		CurrentAuthorityUID: "previous-current", CurrentAuthorityRV: "41", FrontGeneration: 8,
		BundleVersion: envelope.Bundle.Generation + ".p4.r0", WorkerSlot: envelope.CurrentWorkerSlot,
		WorkerSourceSHA: previousSource, WorkerImageDigest: previousImage,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}})
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: groupID, CurrentRecordDigest: envelope.Record.RecordDigest, CurrentWorkerSlot: envelope.WorkerSlot,
		CurrentFrontGeneration: envelope.ServingAuthority.FrontGeneration + 1, CurrentBundleGeneration: envelope.Bundle.Version,
		CurrentWorkerSourceSHA: envelope.WorkerSourceSHA, CurrentWorkerImageDigest: envelope.WorkerImageDigest,
		PreviousRecordDigest: envelope.ServingAuthority.CurrentRecordDigest, PreviousWorkerSlot: envelope.ServingAuthority.WorkerSlot,
		PreviousFrontGeneration: envelope.ServingAuthority.FrontGeneration, PreviousBundleGeneration: envelope.ServingAuthority.BundleVersion,
		PreviousWorkerSourceSHA: previousSource, PreviousWorkerImageDigest: previousImage,
		AuthorityEpoch: envelope.ServingAuthority.AuthorityEpoch + 1}
	if _, _, err := store.SwitchCurrent(context.Background(), current, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-current-authority-"+groupID, "settled-current", "52")
	changed, err := importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now)
	if err != nil || changed {
		t.Fatalf("settled candidate import changed=%v err=%v", changed, err)
	}
	if _, _, _, err := store.LoadCandidate(context.Background(), groupID); !apierrors.IsNotFound(err) {
		t.Fatalf("settled candidate recreated promotable state: %v", err)
	}
	if _, err := store.LoadRouteBundleRecord(context.Background(), groupID, envelope.Record.RecordDigest); !apierrors.IsNotFound(err) {
		t.Fatalf("settled candidate rewrote immutable records: %v", err)
	}
}

func TestCandidateEnvelopeSettledCurrentRequiresExactTransitionIdentity(t *testing.T) {
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", time.Date(2026, 8, 15, 22, 15, 0, 0, time.UTC))
	envelope.ServingAuthority = &candidateServingAuthorityWitness{CurrentRecordDigest: envelope.CurrentRecord.RecordDigest, AuthorityEpoch: 23,
		FrontGeneration: 8, BundleVersion: envelope.CurrentBundle.Version, WorkerSlot: envelope.CurrentWorkerSlot,
		WorkerSourceSHA: strings.Repeat("a", 40), WorkerImageDigest: "sha256:" + strings.Repeat("b", 64)}
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind,
		GroupID: envelope.GroupID, CurrentRecordDigest: envelope.Record.RecordDigest, CurrentWorkerSlot: envelope.WorkerSlot,
		CurrentFrontGeneration: 9, CurrentBundleGeneration: envelope.Bundle.Version,
		CurrentWorkerSourceSHA: envelope.WorkerSourceSHA, CurrentWorkerImageDigest: envelope.WorkerImageDigest,
		PreviousRecordDigest: envelope.ServingAuthority.CurrentRecordDigest, PreviousWorkerSlot: envelope.ServingAuthority.WorkerSlot,
		PreviousFrontGeneration: envelope.ServingAuthority.FrontGeneration, PreviousBundleGeneration: envelope.ServingAuthority.BundleVersion,
		PreviousWorkerSourceSHA: envelope.ServingAuthority.WorkerSourceSHA, PreviousWorkerImageDigest: envelope.ServingAuthority.WorkerImageDigest,
		AuthorityEpoch: 24}
	if !candidateEnvelopeMatchesSettledCurrent(envelope, current) {
		t.Fatal("exact settled transition was rejected")
	}
	for name, mutate := range map[string]func(*releaseguardian.CurrentAuthority){
		"wrong epoch": func(value *releaseguardian.CurrentAuthority) { value.AuthorityEpoch++ },
		"wrong current slot": func(value *releaseguardian.CurrentAuthority) {
			value.CurrentWorkerSlot = releaseguardian.AuthoritySlotA
		},
		"wrong current record": func(value *releaseguardian.CurrentAuthority) {
			value.CurrentRecordDigest = "sha256:" + strings.Repeat("d", 64)
		},
		"wrong current bundle": func(value *releaseguardian.CurrentAuthority) { value.CurrentBundleGeneration = "other.p5.r0" },
		"wrong previous record": func(value *releaseguardian.CurrentAuthority) {
			value.PreviousRecordDigest = "sha256:" + strings.Repeat("e", 64)
		},
		"wrong previous generation": func(value *releaseguardian.CurrentAuthority) { value.PreviousFrontGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := current
			mutate(&changed)
			if candidateEnvelopeMatchesSettledCurrent(envelope, changed) {
				t.Fatal("drifted settled transition was accepted")
			}
		})
	}
}

func TestCandidateImporterRejectsDigestDriftWithoutChangingPointers(t *testing.T) {
	now := time.Date(2026, 8, 12, 4, 30, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	envelope.Record.BundleDigest = "sha256:" + strings.Repeat("f", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(strings.Repeat("t", 48)), 0o600); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}})
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	config := candidateImportConfig{GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile}
	if changed, err := importCandidateOnce(context.Background(), store, client, config, now); err == nil || changed {
		t.Fatalf("digest drift was imported: changed=%v err=%v", changed, err)
	}
	objects, err := client.CoreV1().ConfigMaps("fugue-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(objects.Items) != 0 {
		t.Fatalf("invalid import wrote authority objects: count=%d err=%v", len(objects.Items), err)
	}
}

func TestCandidateImporterAcceptsExpiredSignedCurrentLKGButRejectsExpiredCandidate(t *testing.T) {
	now := time.Date(2026, 8, 13, 19, 0, 0, 0, time.UTC)
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", now)
	// Current authority is an immutable rollback anchor. Its routing TTL may
	// expire while Edge Control serves the persisted LKG and stages a fresh,
	// non-traffic candidate. The candidate itself must remain fresh.
	envelope.CurrentBundle.GeneratedAt = now.Add(-5 * time.Hour)
	envelope.CurrentBundle.ValidUntil = now.Add(-4 * time.Hour)
	envelope.CurrentRecord.BundleDigest = candidateBundleDigest(*envelope.CurrentBundle)
	current, err := envelope.CurrentRecord.Seal()
	if err != nil {
		t.Fatal(err)
	}
	envelope.CurrentRecord = &current
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err != nil {
		t.Fatalf("signed expired current LKG was rejected: %v", err)
	}
	envelope.Bundle.ValidUntil = now.Add(-time.Second)
	envelope.Record.BundleDigest = candidateBundleDigest(envelope.Bundle)
	candidate, err := envelope.Record.Seal()
	if err != nil {
		t.Fatal(err)
	}
	envelope.Record = candidate
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("expired candidate authority was accepted")
	}
}

func TestCandidateImporterRejectsMissingCurrentBundleWithoutPanic(t *testing.T) {
	now := time.Date(2026, 8, 14, 20, 30, 0, 0, time.UTC)
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", now)
	envelope.CurrentBundle = nil
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("candidate envelope without current bundle was accepted")
	}
}

func TestCandidateImporterRejectsDegradedPreviousAuthorizationWithoutServingWitness(t *testing.T) {
	now := time.Date(2026, 8, 14, 20, 45, 0, 0, time.UTC)
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", now)
	envelope.AllowDegradedPrevious = true
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("degraded previous authorization without serving witness was accepted")
	}
}

func TestCandidateImporterAcceptsDegradedServingAuthorityWithinCurrentRecovery(t *testing.T) {
	now := time.Date(2026, 8, 14, 20, 50, 0, 0, time.UTC)
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", now)
	envelope.AllowDegradedPrevious = true
	envelope.Bundle.Generation = envelope.CurrentBundle.Generation
	envelope.Bundle.Version = envelope.Bundle.Generation + ".p5.r0"
	envelope.Bundle.PreviousGeneration = envelope.CurrentBundle.Generation
	envelope.Record.BundleDigest = candidateBundleDigest(envelope.Bundle)
	record, err := envelope.Record.Seal()
	if err != nil {
		t.Fatal(err)
	}
	envelope.Record = record
	envelope.ServingAuthority = &candidateServingAuthorityWitness{CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), AuthorityEpoch: 23,
		CurrentAuthorityUID: "current-authority", CurrentAuthorityRV: "41", FrontGeneration: 8,
		BundleVersion: "routes-pruned.p3.r0", WorkerSlot: envelope.CurrentWorkerSlot, WorkerSourceSHA: strings.Repeat("b", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("c", 64)}
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err != nil {
		t.Fatalf("degraded serving publication in current recovery window was rejected: %v", err)
	}

	envelope.ServingAuthority.BundleVersion = "routes-future.p4.r0"
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("non-historical degraded serving publication was accepted")
	}
	envelope.ServingAuthority.BundleVersion = "routes-previous-recovery.p3.r1"
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("cross-recovery degraded serving publication was accepted")
	}
	envelope.ServingAuthority.BundleVersion = "routes-pruned.p3.r0"
	envelope.AllowDegradedPrevious = false
	envelope.StandbyOnly = true
	if err := validateCandidateEnvelope(envelope.GroupID, envelope, now); err == nil {
		t.Fatal("standby envelope used degraded serving publication fallback")
	}
}

func TestCandidateImporterAcceptsTheSharedEdgeControlRecordIdentity(t *testing.T) {
	now := time.Date(2026, 8, 12, 5, 0, 0, 0, time.UTC)
	envelope := candidateImporterEnvelopeFixture(t, "edge-pool-a", now)
	producer, err := (edgeauthority.RouteBundleRecord{
		GroupID: envelope.Record.GroupID, Epoch: envelope.Record.Epoch, BundleDigest: envelope.Record.BundleDigest,
		SourceSHA: envelope.Record.SourceSHA, ControlImageDigest: envelope.Record.ControlImageDigest,
		InventoryDigest: envelope.Record.InventoryDigest, ManifestDigest: envelope.Record.ManifestDigest,
		HealthContractDigest: envelope.Record.HealthContractDigest, IssuedAt: envelope.Record.IssuedAt,
		KeyID: envelope.Record.KeyID, Signature: envelope.Record.Signature,
	}).Seal()
	if err != nil || producer.RecordDigest != envelope.Record.RecordDigest || envelope.Record.Validate() != nil {
		t.Fatalf("Guardian diverged from the Edge Control record identity: producer=%+v consumer=%+v err=%v", producer, envelope.Record, err)
	}
}

func TestCandidateImporterMigratesLoadedPointerToSignedBundleIdentity(t *testing.T) {
	now := time.Date(2026, 8, 12, 5, 30, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}}
	client := fake.NewSimpleClientset(pod)
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	legacy := releaseguardian.CandidateAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind, GroupID: groupID,
		RecordDigest: envelope.Record.RecordDigest, WorkerSlot: envelope.WorkerSlot, ReleaseRecordDigest: envelope.ReleaseRecordDigest,
		State: releaseguardian.CandidateAuthorityLoaded, Generation: 11,
	}
	if _, _, err := store.PutCandidate(context.Background(), legacy, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-candidate-authority-"+groupID, "legacy-candidate", "40")
	changed, err := importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now)
	if err != nil || !changed {
		t.Fatalf("legacy candidate migration: changed=%v err=%v", changed, err)
	}
	migrated, _, _, err := store.LoadCandidate(context.Background(), groupID)
	if err != nil || migrated.BundleGeneration != envelope.Bundle.Version || migrated.Generation != legacy.Generation+1 ||
		migrated.AuthoritySequence != envelope.AuthorityLedgerSequence || migrated.CandidateSequence != envelope.CandidateLedgerSequence ||
		migrated.RecordDigest != legacy.RecordDigest || migrated.WorkerSlot != legacy.WorkerSlot {
		t.Fatalf("migrated candidate=%+v err=%v", migrated, err)
	}
}

func TestCandidateImporterMigratesLoadedPromotionWitnessMissingCurrentGeneration(t *testing.T) {
	now := time.Date(2026, 8, 12, 5, 45, 0, 0, time.UTC)
	groupID := "edge-pool-a"
	token := strings.Repeat("t", 48)
	envelope := candidateImporterEnvelopeFixture(t, groupID, now)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(envelope) }))
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tokenFile, []byte(token), 0o600)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "fugue-system", Labels: map[string]string{
		"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true",
	}}, Spec: corev1.PodSpec{NodeName: "edge-node-a"}}
	client := fake.NewSimpleClientset(pod)
	store, _ := releaseguardian.NewAuthorityStore(client, "fugue-system")
	legacy := releaseguardian.CandidateAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind,
		GroupID: groupID, RecordDigest: envelope.Record.RecordDigest, BundleGeneration: envelope.Bundle.Version, ServingGeneration: envelope.Bundle.Generation,
		AuthoritySequence: envelope.AuthorityLedgerSequence, CandidateSequence: envelope.CandidateLedgerSequence,
		CurrentPublicationSequence: uint64(envelope.CurrentRecord.Epoch), CurrentBundleDigest: envelope.CurrentRecord.BundleDigest,
		CandidateEpoch: envelope.Epoch, WorkerSlot: envelope.WorkerSlot, ReleaseRecordDigest: envelope.ReleaseRecordDigest,
		State: releaseguardian.CandidateAuthorityLoaded, Generation: 19}
	if _, _, err := store.PutCandidate(context.Background(), legacy, "", ""); err != nil {
		t.Fatal(err)
	}
	setMutableAuthorityFixture(t, client, "fugue-candidate-authority-"+groupID, "legacy-witness", "45")
	changed, err := importCandidateOnce(context.Background(), store, client, candidateImportConfig{
		GroupID: groupID, Endpoint: server.URL + edgeCandidateEnvelopePathV1, TokenFile: tokenFile,
	}, now)
	if err != nil || !changed {
		t.Fatalf("promotion witness migration: changed=%v err=%v", changed, err)
	}
	migrated, _, _, err := store.LoadCandidate(context.Background(), groupID)
	if err != nil || migrated.CurrentServingGeneration != envelope.CurrentBundle.Generation || migrated.Generation != 20 || !migrated.HasPromotionWitness() {
		t.Fatalf("migrated promotion witness=%+v err=%v", migrated, err)
	}
}

func setMutableAuthorityFixture(t *testing.T, client *fake.Clientset, name, uid, rv string) {
	t.Helper()
	object, err := client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.UID, object.ResourceVersion = types.UID(uid), rv
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestParseCandidateImportsRequiresExactEndpointAndPrivateTokenPath(t *testing.T) {
	valid := "edge-pool-a,http://edge-control-a.fugue-system.svc:8092/v1/edge/candidate-envelope,/var/run/secrets/fugue-candidate-import-a/token"
	configs, err := parseCandidateImports(valid)
	if err != nil || len(configs) != 1 || configs[0].GroupID != "edge-pool-a" {
		t.Fatalf("valid candidate import rejected: configs=%+v err=%v", configs, err)
	}
	for _, invalid := range []string{
		"edge-pool-a,http://edge-control-a/v1/edge/candidate-routes,/var/run/token",
		"edge-pool-a,http://edge-control-a/v1/edge/candidate-envelope?slot=a,/var/run/token",
		"edge-pool-a,http://edge-control-a/v1/edge/candidate-envelope,relative/token",
		valid + ";" + valid,
	} {
		if _, err := parseCandidateImports(invalid); err == nil {
			t.Fatalf("invalid candidate import accepted: %s", invalid)
		}
	}
}

func TestParseAuthorityBundleVersionBindsPublicationAndRecovery(t *testing.T) {
	sequence, recovery, err := parseAuthorityBundleVersion("edgegroupbundle_abc", "edgegroupbundle_abc.p11314.r79")
	if err != nil || sequence != 11314 || recovery != 79 {
		t.Fatalf("authority version parsed as sequence=%d recovery=%d err=%v", sequence, recovery, err)
	}
	for _, invalid := range []string{"edgegroupbundle_abc.p0.r1", "edgegroupbundle_other.p1.r0", "edgegroupbundle_abc.p1", "edgegroupbundle_abc.px.r0"} {
		if _, _, err := parseAuthorityBundleVersion("edgegroupbundle_abc", invalid); err == nil {
			t.Fatalf("invalid authority version accepted: %s", invalid)
		}
	}
}

func candidateImporterEnvelopeFixture(t *testing.T, groupID string, now time.Time) candidateEnvelope {
	t.Helper()
	currentBundle := model.EdgeRouteBundle{SchemaVersion: model.BundleSchemaVersionV1, Version: "routes-current.p4.r0", Generation: "routes-current", GeneratedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour), Issuer: "fugue-edge-control", KeyID: "edge-key-a", Signature: strings.Repeat("A", 43), EdgeGroupID: groupID, Routes: []model.EdgeRouteBinding{}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{}}
	candidateBundle := currentBundle
	candidateBundle.Version = "routes-candidate.p5.r0"
	candidateBundle.Generation = "routes-candidate"
	candidateBundle.PreviousGeneration = currentBundle.Generation
	candidateBundle.Signature = strings.Repeat("B", 43)
	source := strings.Repeat("1", 40)
	controlImage := "sha256:" + strings.Repeat("2", 64)
	manifest := "sha256:" + strings.Repeat("3", 64)
	health := "sha256:" + strings.Repeat("4", 64)
	current, err := (releaseguardian.RouteBundleRecord{GroupID: groupID, Epoch: 4, BundleDigest: candidateBundleDigest(currentBundle), SourceSHA: source, ControlImageDigest: controlImage, InventoryDigest: "sha256:" + strings.Repeat("5", 64), ManifestDigest: manifest, HealthContractDigest: health, IssuedAt: now.Add(-time.Minute).Format(time.RFC3339Nano), KeyID: currentBundle.KeyID, Signature: currentBundle.Signature}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := (releaseguardian.RouteBundleRecord{GroupID: groupID, Epoch: 5, BundleDigest: candidateBundleDigest(candidateBundle), SourceSHA: source, ControlImageDigest: controlImage, InventoryDigest: "sha256:" + strings.Repeat("6", 64), ManifestDigest: manifest, HealthContractDigest: health, IssuedAt: now.Format(time.RFC3339Nano), KeyID: candidateBundle.KeyID, Signature: candidateBundle.Signature}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return candidateEnvelope{Schema: edgeCandidateEnvelopeSchemaV1, GroupID: groupID, Epoch: 5, AuthorityLedgerSequence: 7, CandidateLedgerSequence: 9, RouteIntentGeneration: "route-intent-5", InventoryGeneration: "inventory-5", ReleaseRecordDigest: "sha256:" + strings.Repeat("7", 64), WorkerSourceSHA: strings.Repeat("8", 40), WorkerImageDigest: "sha256:" + strings.Repeat("9", 64), WorkerSlot: releaseguardian.AuthoritySlotB, PublishedAt: now, CurrentRecord: &current, CurrentBundle: &currentBundle, CurrentWorkerSlot: releaseguardian.AuthoritySlotA, Record: candidate, Bundle: candidateBundle}
}
