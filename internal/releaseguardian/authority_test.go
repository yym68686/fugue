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

func candidateResultFixture(candidate CandidateAuthority, now time.Time, route, dependency HealthState) CandidateCanaryResult {
	return CandidateCanaryResult{
		GroupID: candidate.GroupID, CandidateRecordDigest: candidate.RecordDigest, BundleGeneration: candidate.BundleGeneration,
		WorkerSlot: candidate.WorkerSlot, WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest, WorkerCohortDigest: otherDigest,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, RouteState: route, DependencyState: dependency,
		EvidenceDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(30 * time.Second).Format(time.RFC3339Nano),
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

func TestImportedAuthorityRefreshAndLoadedCandidateReplacementAreBounded(t *testing.T) {
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
	currentObject, err := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, currentAuthorityName(oldCurrent.GroupID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	currentObject.UID, currentObject.ResourceVersion = types.UID("current-import"), "10"
	if _, err := client.CoreV1().ConfigMaps("fugue-system").Update(ctx, currentObject, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	refreshed := oldCurrent
	refreshed.CurrentRecordDigest = otherDigest
	refreshed.CurrentWorkerSlot = AuthoritySlotA
	refreshed.AuthorityEpoch = 2
	if _, _, err := store.RefreshImportedCurrent(ctx, refreshed, currentObject.UID, currentObject.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	loadedCurrent, _, _, err := store.LoadCurrent(ctx, oldCurrent.GroupID)
	if err != nil || loadedCurrent != refreshed {
		t.Fatalf("refreshed current=%+v err=%v", loadedCurrent, err)
	}

	candidate := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: oldCurrent.GroupID, RecordDigest: testDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest, State: CandidateAuthorityLoaded, Generation: 1}
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
}
