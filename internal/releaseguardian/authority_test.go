package releaseguardian

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

const testSignature = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

const testCandidateBundle = "candidate-bundle-generation-1"

func bindCandidatePromotionWitness(candidate CandidateAuthority) CandidateAuthority {
	candidate.AuthoritySequence = 7
	candidate.CandidateSequence = 9
	candidate.CurrentPublicationSequence = 6
	candidate.CurrentBundleDigest = testDigest
	candidate.CurrentServingGeneration = "current-serving-generation-1"
	candidate.CandidateEpoch = 8
	if candidate.ServingGeneration == "" {
		candidate.ServingGeneration = candidate.BundleGeneration
	}
	if candidate.WorkerSourceSHA == "" {
		candidate.WorkerSourceSHA = testSHA
	}
	if candidate.WorkerImageDigest == "" {
		candidate.WorkerImageDigest = testDigest
	}
	return candidate
}

func candidateResultFixture(candidate CandidateAuthority, now time.Time, route, dependency HealthState) CandidateCanaryResult {
	return CandidateCanaryResult{
		GroupID: candidate.GroupID, CandidateRecordDigest: candidate.RecordDigest, BundleGeneration: candidate.BundleGeneration, ServingGeneration: candidate.ServingGeneration,
		AuthoritySequence: candidate.AuthoritySequence, CandidateSequence: candidate.CandidateSequence,
		CurrentPublicationSequence: candidate.CurrentPublicationSequence, CurrentRecoveryEpoch: candidate.CurrentRecoveryEpoch,
		CurrentBundleDigest: candidate.CurrentBundleDigest, CurrentServingGeneration: candidate.CurrentServingGeneration, CandidateEpoch: candidate.CandidateEpoch,
		WorkerSlot: candidate.WorkerSlot, WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest, WorkerCohortDigest: otherDigest,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, RouteState: route, DependencyState: dependency,
		AllowDegradedPrevious: candidate.AllowDegradedPrevious,
		EvidenceDigest:        testDigest, ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(30 * time.Second).Format(time.RFC3339Nano),
		KeyID: "candidate-canary-v1",
	}
}

func sealedRouteRecord(t *testing.T, groupID string, epoch int64) RouteBundleRecord {
	t.Helper()
	record, err := (RouteBundleRecord{
		GroupID: groupID, Epoch: epoch, BundleDigest: testDigest, SourceSHA: testSHA,
		ControlImageDigest: testDigest, InventoryDigest: testDigest, ManifestDigest: testDigest,
		HealthContractDigest: testDigest, IssuedAt: time.Unix(epoch, 0).UTC().Format(time.RFC3339Nano),
		KeyID: "authority-key-1", Signature: testSignature,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestAuthorityModelsBindCandidateCurrentAndLKGIdentity(t *testing.T) {
	record := sealedRouteRecord(t, "edge-pool-a", 7)
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	tampered := record
	tampered.GroupID = "edge-pool-b"
	if err := tampered.Validate(); err == nil {
		t.Fatal("cross-group route record tampering was accepted")
	}

	for _, authority := range []CurrentAuthority{
		{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a", CurrentRecordDigest: record.RecordDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 1},
		{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a", CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, PreviousRecordDigest: record.RecordDigest, PreviousWorkerSlot: AuthoritySlotA, AuthorityEpoch: 2},
	} {
		if err := authority.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	invalid := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a", CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, PreviousRecordDigest: otherDigest, PreviousWorkerSlot: AuthoritySlotA, AuthorityEpoch: 2}
	if err := invalid.Validate(); err == nil {
		t.Fatal("current authority accepted a non-distinct LKG")
	}
}

func TestCandidateCanaryIsImmutableCandidateBoundAndFresh(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	candidate := CandidateAuthority{GroupID: "edge-pool-a", RecordDigest: testDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: otherDigest}
	candidate = bindCandidatePromotionWitness(candidate)
	unsigned := candidateResultFixture(candidate, now, HealthHealthy, HealthHealthy)
	unsigned.KeyID, unsigned.Signature = "canary-key-1", testableSignaturePlaceholder
	result, err := unsigned.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(29 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(now.Add(31 * time.Second)); err == nil {
		t.Fatal("expired candidate canary was accepted")
	}
	result.CandidateRecordDigest = otherDigest
	if err := result.Validate(now); err == nil {
		t.Fatal("candidate canary was reusable for another record")
	}
}

func TestCandidateCanaryStoreScopesLookupAndPrunesOnlyExpiredValidResults(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_000, 0).UTC()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	candidate := CandidateAuthority{
		APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: "edge-pool-a",
		RecordDigest: testDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: otherDigest,
		State: CandidateAuthorityLoaded, Generation: 1,
	}
	candidate = bindCandidatePromotionWitness(candidate)
	result, err := SignCandidateCanaryResult(candidateResultFixture(candidate, now, HealthHealthy, HealthHealthy), candidateCanaryTestKey)
	if err != nil || store.CreateCandidateCanaryResult(ctx, result, now) != nil {
		t.Fatalf("create candidate canary: result=%+v err=%v", result, err)
	}
	object, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, candidateCanaryResultName(result.GroupID, result.ResultDigest), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if object.Labels["fugue.pro/candidate-record"] != candidateRecordLabel(candidate.RecordDigest) {
		t.Fatalf("candidate label=%q", object.Labels["fugue.pro/candidate-record"])
	}
	object.UID, object.ResourceVersion = types.UID("candidate-canary-result"), "30"
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	latest, err := store.LoadLatestCandidateCanaryResult(ctx, candidate, now.Add(29*time.Second))
	if err != nil || latest.ResultDigest != result.ResultDigest {
		t.Fatalf("latest result=%+v err=%v", latest, err)
	}
	authorizedCandidate := candidate
	authorizedCandidate.AllowDegradedPrevious = true
	if _, err := store.LoadLatestCandidateCanaryResult(ctx, authorizedCandidate, now); !errors.Is(err, ErrCandidateCanaryUnavailable) {
		t.Fatalf("canary result with mismatched degraded previous authorization was selected: %v", err)
	}
	otherCandidate := candidate
	otherCandidate.RecordDigest = otherDigest
	if _, err := store.LoadLatestCandidateCanaryResult(ctx, otherCandidate, now); !errors.Is(err, ErrCandidateCanaryUnavailable) {
		t.Fatalf("cross-candidate lookup err=%v", err)
	}
	if err := store.PruneExpiredCandidateCanaryResults(ctx, candidate.GroupID, now.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, object.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired result remains: %v", err)
	}
}

func TestLegacyCandidateCanaryCanOnlyBeRecognizedForBoundedExpiryCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000, 0).UTC()
	groupID := "edge-pool-a"
	legacy := legacyCandidateCanaryResultV1{
		APIVersion: APIVersion, Kind: CandidateCanaryResultKind, GroupID: groupID,
		CandidateRecordDigest: testDigest, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: otherDigest,
		RouteState: HealthHealthy, DependencyState: HealthHealthy, EvidenceDigest: testDigest,
		ObservedAt: now.Add(-40 * time.Second).Format(time.RFC3339Nano), ExpiresAt: now.Add(-10 * time.Second).Format(time.RFC3339Nano),
		KeyID: "candidate-canary-v1", Signature: testableSignaturePlaceholder,
	}
	raw, err := declarativerelease.CanonicalJSON(legacy)
	if err != nil {
		t.Fatal(err)
	}
	legacy.ResultDigest = digest(raw)
	raw, _ = declarativerelease.CanonicalJSON(legacy)
	immutable := true
	name := candidateCanaryResultName(groupID, legacy.ResultDigest)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fugue-system", UID: "legacy-result", ResourceVersion: "10", Labels: map[string]string{
		"fugue.pro/group": groupID, "fugue.pro/authority-kind": "candidate-canary", "fugue.pro/candidate-record": candidateRecordLabel(legacy.CandidateRecordDigest),
	}}, Immutable: &immutable, Data: map[string]string{"result.json": string(raw)}}
	client := fake.NewSimpleClientset(object)
	store, _ := NewAuthorityStore(client, "fugue-system")
	candidate := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: groupID,
		RecordDigest: legacy.CandidateRecordDigest, BundleGeneration: testCandidateBundle, WorkerSlot: legacy.WorkerSlot,
		ReleaseRecordDigest: legacy.ReleaseRecordDigest, State: CandidateAuthorityLoaded, Generation: 1}
	candidate = bindCandidatePromotionWitness(candidate)
	if _, err := store.LoadLatestCandidateCanaryResult(ctx, candidate, now); err == nil {
		t.Fatal("legacy result was accepted as authority evidence")
	}
	if err := store.PruneExpiredCandidateCanaryResults(ctx, groupID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired legacy result remains: %v", err)
	}

	tampered := legacy
	tampered.EvidenceDigest = otherDigest
	tamperedRaw, _ := declarativerelease.CanonicalJSON(tampered)
	if _, err := decodeCandidateCanaryForCleanup(string(tamperedRaw)); err == nil {
		t.Fatal("tampered legacy result was eligible for cleanup")
	}
}

func TestPreviousPromotionWitnessCanaryCanOnlyBeRecognizedForBoundedExpiryCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_500, 0).UTC()
	groupID := "edge-pool-a"
	previous := legacyCandidateCanaryResultV2{
		APIVersion: APIVersion, Kind: CandidateCanaryResultKind, GroupID: groupID,
		CandidateRecordDigest: testDigest, WorkerSlot: AuthoritySlotB, AuthoritySequence: 12, CandidateSequence: 9,
		CurrentPublicationSequence: 11, CurrentRecoveryEpoch: 2, CurrentBundleDigest: otherDigest, CandidateEpoch: 13,
		BundleGeneration: "candidate-bundle.p13.r0", ServingGeneration: "candidate-bundle",
		WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest, WorkerCohortDigest: otherDigest,
		ReleaseRecordDigest: otherDigest, RouteState: HealthHealthy, DependencyState: HealthHealthy,
		EvidenceDigest: testDigest, ObservedAt: now.Add(-40 * time.Second).Format(time.RFC3339Nano),
		ExpiresAt: now.Add(-10 * time.Second).Format(time.RFC3339Nano), KeyID: "candidate-canary-v1",
		Signature: testableSignaturePlaceholder,
	}
	raw, _ := declarativerelease.CanonicalJSON(previous)
	previous.ResultDigest = digest(raw)
	raw, _ = declarativerelease.CanonicalJSON(previous)
	immutable := true
	name := candidateCanaryResultName(groupID, previous.ResultDigest)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fugue-system", UID: "previous-result", ResourceVersion: "11", Labels: map[string]string{
		"fugue.pro/group": groupID, "fugue.pro/authority-kind": "candidate-canary", "fugue.pro/candidate-record": candidateRecordLabel(previous.CandidateRecordDigest),
	}}, Immutable: &immutable, Data: map[string]string{"result.json": string(raw)}}
	client := fake.NewSimpleClientset(object)
	store, _ := NewAuthorityStore(client, "fugue-system")
	candidate := bindCandidatePromotionWitness(CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: groupID,
		RecordDigest: previous.CandidateRecordDigest, BundleGeneration: testCandidateBundle, WorkerSlot: previous.WorkerSlot,
		ReleaseRecordDigest: previous.ReleaseRecordDigest, State: CandidateAuthorityLoaded, Generation: 1})
	if _, err := store.LoadLatestCandidateCanaryResult(ctx, candidate, now); err == nil {
		t.Fatal("previous witness result was accepted as authority evidence")
	}
	if err := store.PruneExpiredCandidateCanaryResults(ctx, groupID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired previous witness remains: %v", err)
	}
	tampered := previous
	tampered.CurrentBundleDigest = testDigest
	tamperedRaw, _ := declarativerelease.CanonicalJSON(tampered)
	if _, err := decodeCandidateCanaryForCleanup(string(tamperedRaw)); err == nil {
		t.Fatal("tampered previous witness was eligible for cleanup")
	}
}

func TestAuthorityStoreKeepsGroupsImmutableAndCASIsolated(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	a := sealedRouteRecord(t, "edge-pool-a", 1)
	b := sealedRouteRecord(t, "edge-pool-b", 1)
	for _, record := range []RouteBundleRecord{a, b} {
		if err := store.CreateRouteBundleRecord(ctx, record); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateRouteBundleRecord(ctx, record); err != nil {
			t.Fatalf("byte-identical immutable create is not idempotent: %v", err)
		}
	}
	if routeBundleRecordName(a.GroupID, a.RecordDigest) == routeBundleRecordName(b.GroupID, b.RecordDigest) ||
		candidateAuthorityName(a.GroupID) == candidateAuthorityName(b.GroupID) ||
		currentAuthorityName(a.GroupID) == currentAuthorityName(b.GroupID) {
		t.Fatal("group authority object names collided")
	}

	candidateA := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: a.GroupID, RecordDigest: a.RecordDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest, State: CandidateAuthorityLoaded, Generation: 1}
	candidateA = bindCandidatePromotionWitness(candidateA)
	_, _, err = store.PutCandidate(ctx, candidateA, "", "")
	if err != nil {
		t.Fatal(err)
	}
	// The fake client does not assign server metadata, so bind exact test CAS
	// metadata before exercising update semantics.
	object, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, candidateAuthorityName(a.GroupID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.UID, object.ResourceVersion = types.UID("candidate-a"), "10"
	object, err = client.CoreV1().ConfigMaps("fugue-system").Update(ctx, object, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	uid, rv := object.UID, object.ResourceVersion
	loaded, loadedUID, loadedRV, err := store.LoadCandidate(ctx, a.GroupID)
	if err != nil || loaded != candidateA || loadedUID != uid || loadedRV != rv {
		t.Fatalf("load candidate=%+v uid=%s rv=%s err=%v", loaded, loadedUID, loadedRV, err)
	}
	verified := candidateA
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, 2, testDigest
	if _, _, err := store.PutCandidate(ctx, verified, uid, rv); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PutCandidate(ctx, verified, uid, "stale"); err == nil {
		t.Fatal("stale candidate resourceVersion CAS was accepted")
	}
	crossGroup := verified
	crossGroup.GroupID = b.GroupID
	if _, _, err := store.PutCandidate(ctx, crossGroup, uid, rv); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("cross-group candidate CAS was accepted: %v", err)
	}
}

func TestCurrentAuthorityCASRequiresExactPreviousAndSlotSwitch(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	initial := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a", CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 1}
	if _, _, err := store.SwitchCurrent(ctx, initial, "", ""); err != nil {
		t.Fatal(err)
	}
	object, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, currentAuthorityName(initial.GroupID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.UID, object.ResourceVersion = types.UID("current-a"), "20"
	object, err = client.CoreV1().ConfigMaps("fugue-system").Update(ctx, object, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: initial.GroupID, CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, PreviousRecordDigest: testDigest, PreviousWorkerSlot: AuthoritySlotA, AuthorityEpoch: 2}
	if _, _, err := store.SwitchCurrent(ctx, next, object.UID, object.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	object, err = client.CoreV1().ConfigMaps("fugue-system").Get(ctx, currentAuthorityName(initial.GroupID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, loadedUID, loadedRV, err := store.LoadCurrent(ctx, initial.GroupID)
	if err != nil || loaded != next || loadedUID != object.UID || loadedRV != object.ResourceVersion {
		t.Fatalf("load current=%+v uid=%s rv=%s err=%v", loaded, loadedUID, loadedRV, err)
	}
	wrongLKG := next
	wrongLKG.AuthorityEpoch = 3
	wrongLKG.CurrentRecordDigest, wrongLKG.CurrentWorkerSlot = testDigest, AuthoritySlotA
	wrongLKG.PreviousRecordDigest = strings.Replace(otherDigest, "b", "c", 1)
	if _, _, err := store.SwitchCurrent(ctx, wrongLKG, object.UID, object.ResourceVersion); err == nil {
		t.Fatal("authority switch with the wrong previous record was accepted")
	}
}

func TestNormalizeCurrentBaselineCanReplaceAValidatedPreviousReceipt(t *testing.T) {
	ctx := context.Background()
	groupID := "edge-pool-a"
	baseline, err := (AuthorityBaselineReceipt{
		GroupID: groupID, BeforeRecordDigest: otherDigest, BeforeWorkerSlot: AuthoritySlotB, BeforeAuthorityEpoch: 1,
		RecordDigest: testDigest, WorkerSlot: AuthoritySlotA, AuthorityEpoch: 2,
		Nodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-old-uid", FrontResourceVersion: "10",
			WorkerPodUID: "worker-old-uid", WorkerResourceVersion: "11", ActivationGeneration: 1,
			BundleGeneration: "baseline-bundle-1", ServingGeneration: "baseline-serving-1", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		ObservedAt: time.Unix(1_000, 0).UTC().Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	previousBefore := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, CurrentFrontGeneration: 1,
		CurrentBundleGeneration: "legacy-bundle-1", CurrentWorkerSourceSHA: testSHA, CurrentWorkerImageDigest: testDigest,
		PreviousRecordDigest: testDigest, PreviousWorkerSlot: AuthoritySlotA, PreviousFrontGeneration: 1,
		PreviousBundleGeneration: "legacy-bundle-0", PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: otherDigest,
		AuthorityEpoch: 3, BaselineReceiptDigest: baseline.ReceiptDigest}
	previousAfter := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, CurrentFrontGeneration: 2,
		CurrentBundleGeneration: "normalized-bundle-2", CurrentWorkerSourceSHA: testSHA, CurrentWorkerImageDigest: testDigest,
		AuthorityEpoch: 4, BaselineReceiptDigest: baseline.ReceiptDigest}
	previousReceipt, err := (AuthorityNormalizationReceipt{GroupID: groupID, BaselineReceiptDigest: baseline.ReceiptDigest,
		Before: previousBefore, After: previousAfter,
		Nodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid-1", FrontResourceVersion: "20",
			WorkerPodUID: "worker-pod-uid-1", WorkerResourceVersion: "21", ActivationGeneration: 2,
			BundleGeneration: previousAfter.CurrentBundleGeneration, ServingGeneration: "normalized-serving-2",
			WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		ObservedAt: time.Unix(2_000, 0).UTC().Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	before := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, CurrentFrontGeneration: 3,
		CurrentBundleGeneration: "legacy-bundle-3", CurrentWorkerSourceSHA: testSHA, CurrentWorkerImageDigest: otherDigest,
		PreviousRecordDigest: testDigest, PreviousWorkerSlot: AuthoritySlotA, PreviousFrontGeneration: 2,
		PreviousBundleGeneration: previousAfter.CurrentBundleGeneration, PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: testDigest,
		AuthorityEpoch: 5, BaselineReceiptDigest: baseline.ReceiptDigest}
	after := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: groupID,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, CurrentFrontGeneration: 4,
		CurrentBundleGeneration: "normalized-bundle-4", CurrentWorkerSourceSHA: testSHA, CurrentWorkerImageDigest: testDigest,
		AuthorityEpoch: 6, BaselineReceiptDigest: baseline.ReceiptDigest}
	receipt, err := (AuthorityNormalizationReceipt{GroupID: groupID, BaselineReceiptDigest: baseline.ReceiptDigest,
		Before: before, After: after,
		Nodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid-2", FrontResourceVersion: "30",
			WorkerPodUID: "worker-pod-uid-2", WorkerResourceVersion: "31", ActivationGeneration: 4,
			BundleGeneration: after.CurrentBundleGeneration, ServingGeneration: "normalized-serving-4",
			WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		ObservedAt: time.Unix(3_000, 0).UTC().Format(time.RFC3339Nano),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	authorityRaw, _ := declarativerelease.CanonicalJSON(before)
	baselineRaw, _ := declarativerelease.CanonicalJSON(baseline)
	previousReceiptRaw, _ := declarativerelease.CanonicalJSON(previousReceipt)

	newStore := func(extraKey string, normalizationRaw []byte) (*AuthorityStore, *fake.Clientset) {
		data := map[string]string{"authority.json": string(authorityRaw), "baseline-receipt.json": string(baselineRaw),
			"normalization-receipt.json": string(normalizationRaw)}
		if extraKey != "" {
			data[extraKey] = "unexpected"
		}
		object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: currentAuthorityName(groupID), Namespace: "fugue-system",
			UID: types.UID("current-authority-uid"), ResourceVersion: "40", Labels: authorityLabels(groupID)}, Data: data}
		client := fake.NewSimpleClientset(object)
		store, _ := NewAuthorityStore(client, "fugue-system")
		return store, client
	}

	t.Run("validated previous receipt", func(t *testing.T) {
		store, client := newStore("", previousReceiptRaw)
		if _, _, err := store.NormalizeCurrentBaseline(ctx, after, receipt, "current-authority-uid", "40"); err != nil {
			t.Fatal(err)
		}
		loaded, _, _, err := store.LoadCurrent(ctx, groupID)
		if err != nil || loaded != after {
			t.Fatalf("loaded authority=%+v err=%v", loaded, err)
		}
		object, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, currentAuthorityName(groupID), metav1.GetOptions{})
		if err != nil || object.Data["normalization-receipt.json"] == string(previousReceiptRaw) {
			t.Fatalf("normalization receipt was not replaced: %v", err)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		store, _ := newStore("unexpected.json", previousReceiptRaw)
		if _, _, err := store.NormalizeCurrentBaseline(ctx, after, receipt, "current-authority-uid", "40"); err == nil ||
			!strings.Contains(err.Error(), "CAS changed") {
			t.Fatalf("unknown field was accepted: %v", err)
		}
	})

	t.Run("tampered previous receipt", func(t *testing.T) {
		tampered := append([]byte(nil), previousReceiptRaw...)
		tampered[len(tampered)-2] ^= 1
		store, _ := newStore("", tampered)
		if _, _, err := store.NormalizeCurrentBaseline(ctx, after, receipt, "current-authority-uid", "40"); err == nil ||
			!strings.Contains(err.Error(), "predecessor changed") {
			t.Fatalf("tampered receipt was accepted: %v", err)
		}
	})
}

func TestLoadedCandidateReplacementIsBounded(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	oldCurrent := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a", CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotB, AuthorityEpoch: 1}
	if _, _, err := store.SwitchCurrent(ctx, oldCurrent, "", ""); err != nil {
		t.Fatal(err)
	}
	candidate := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: oldCurrent.GroupID, RecordDigest: testDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest, State: CandidateAuthorityLoaded, Generation: 1}
	candidate = bindCandidatePromotionWitness(candidate)
	if _, _, err := store.PutCandidate(ctx, candidate, "", ""); err != nil {
		t.Fatal(err)
	}
	candidateObject, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, candidateAuthorityName(oldCurrent.GroupID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	candidateObject.UID, candidateObject.ResourceVersion = types.UID("candidate-import"), "20"
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(ctx, candidateObject, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := candidate
	replacement.RecordDigest = otherDigest
	replacement.WorkerSlot = AuthoritySlotA
	replacement.Generation = 2
	replacedUID, replacedRV, err := store.ReplaceLoadedCandidate(ctx, replacement, candidateObject.UID, candidateObject.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	loadedCandidate, _, _, err := store.LoadCandidate(ctx, oldCurrent.GroupID)
	if err != nil || loadedCandidate != replacement {
		t.Fatalf("replaced candidate=%+v err=%v", loadedCandidate, err)
	}
	terminal := replacement
	terminal.State, terminal.Generation, terminal.CanaryResultDigest = CandidateAuthorityVerified, 3, testDigest
	terminalUID, terminalRV, err := store.PutCandidate(ctx, terminal, replacedUID, replacedRV)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReplaceLoadedCandidate(ctx, candidate, terminalUID, terminalRV); err == nil {
		t.Fatal("terminal candidate was replaceable by importer")
	}
	settledReplacement := candidate
	settledReplacement.RecordDigest, settledReplacement.WorkerSlot, settledReplacement.Generation = testDigest, AuthoritySlotB, terminal.Generation+1
	if _, _, err := store.ReplaceSettledCandidate(ctx, settledReplacement, terminalUID, terminalRV); err != nil {
		t.Fatalf("settled terminal candidate was not replaceable: %v", err)
	}
}

func TestAuthorityTransitionJournalAdvancesImmutablePhasesAndDeletesByCAS(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, _ := NewAuthorityStore(client, "fugue-system")
	now := time.Unix(8_000, 0).UTC()
	candidate := bindCandidatePromotionWitness(CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: "edge-pool-a",
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1})
	candidate.State, candidate.Generation, candidate.CanaryResultDigest = CandidateAuthorityVerified, 2, testDigest
	before := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: candidate.GroupID,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 1}
	prepared, err := (AuthorityTransitionJournal{GroupID: candidate.GroupID, Phase: AuthorityTransitionPrepared,
		CurrentUID: "current-uid", CurrentRV: "20", Before: before, Candidate: candidate, CanaryResultDigest: testDigest,
		PreviousNodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid", FrontResourceVersion: "10",
			WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11", ActivationGeneration: 7, BundleGeneration: "previous-bundle-7",
			ServingGeneration: "previous-serving-7", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		CreatedAt: now.Format(time.RFC3339Nano)}).Seal()
	if err != nil || store.CreateTransitionJournal(ctx, prepared) != nil {
		t.Fatalf("create prepared journal: %v", err)
	}
	activation := FrontAuthorityReceipt{GroupID: candidate.GroupID, PreviousSlot: AuthoritySlotA, PreviousGeneration: 7,
		PreviousBundleGeneration: "previous-bundle-7", PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: testDigest,
		TargetSlot: AuthoritySlotB, TargetGeneration: 8, TargetBundleGeneration: "candidate-serving.p8.r0",
		TargetWorkerSourceSHA: testSHA, TargetWorkerImageDigest: otherDigest}
	activated := prepared
	activated.Phase, activated.Activation = AuthorityTransitionActivated, &activation
	activated, err = activated.Seal()
	if err != nil || store.UpdateTransitionJournal(ctx, prepared, activated) != nil {
		t.Fatalf("activate journal: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, transitionJournalName(candidate.GroupID), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("prepared journal remained after activated witness")
	}
	if err := store.DeleteTransitionJournal(ctx, activated); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, transitionActivatedJournalName(candidate.GroupID), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("activated journal remained after terminal delete")
	}
}

func TestExpiredCanaryReferencedByActiveJournalIsNotPruned(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(9_000, 0).UTC()
	client := fake.NewSimpleClientset()
	store, _ := NewAuthorityStore(client, "fugue-system")
	candidate := bindCandidatePromotionWitness(CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: "edge-pool-a",
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1})
	result, err := SignCandidateCanaryResult(candidateResultFixture(candidate, now, HealthHealthy, HealthHealthy), candidateCanaryTestKey)
	if err != nil || store.CreateCandidateCanaryResult(ctx, result, now) != nil {
		t.Fatal(err)
	}
	candidate.State, candidate.Generation, candidate.CanaryResultDigest = CandidateAuthorityVerified, 2, result.ResultDigest
	before := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: candidate.GroupID,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 1}
	journal, err := (AuthorityTransitionJournal{GroupID: candidate.GroupID, Phase: AuthorityTransitionPrepared,
		CurrentUID: "current-uid", CurrentRV: "20", Before: before, Candidate: candidate, CanaryResultDigest: result.ResultDigest,
		PreviousNodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid", FrontResourceVersion: "10",
			WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11", ActivationGeneration: 7, BundleGeneration: "previous-bundle-7",
			ServingGeneration: "previous-serving-7", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		CreatedAt: now.Format(time.RFC3339Nano)}).Seal()
	if err != nil || store.CreateTransitionJournal(ctx, journal) != nil {
		t.Fatal(err)
	}
	if err := store.PruneExpiredCandidateCanaryResults(ctx, candidate.GroupID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	name := candidateCanaryResultName(candidate.GroupID, result.ResultDigest)
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("journal canary was pruned: %v", err)
	}
	if err := store.DeleteTransitionJournal(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if err := store.PruneExpiredCandidateCanaryResults(ctx, candidate.GroupID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unreferenced expired canary remains: %v", err)
	}
}
