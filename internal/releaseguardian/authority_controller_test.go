package releaseguardian

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAuthorityControllerSwitchesAndRevertsOneGroupWithExactCAS(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000, 0).UTC()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	group := "edge-pool-a"
	candidate := CandidateAuthority{
		APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: group,
		RecordDigest: otherDigest, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1,
	}
	if _, _, err := store.PutCandidate(ctx, candidate, "", ""); err != nil {
		t.Fatal(err)
	}
	setAuthorityObjectCAS(t, client, candidateAuthorityName(group), "candidate-a", "10")
	current := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 4,
	}
	if _, _, err := store.SwitchCurrent(ctx, current, "", ""); err != nil {
		t.Fatal(err)
	}
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-a", "20")
	otherGroup := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-b", CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, AuthorityEpoch: 3}
	if _, _, err := store.SwitchCurrent(ctx, otherGroup, "", ""); err != nil {
		t.Fatal(err)
	}
	setAuthorityObjectCAS(t, client, currentAuthorityName(otherGroup.GroupID), "current-b", "21")
	result, err := SignCandidateCanaryResult(CandidateCanaryResult{
		GroupID: group, CandidateRecordDigest: candidate.RecordDigest, WorkerSlot: candidate.WorkerSlot,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, RouteState: HealthHealthy, DependencyState: HealthHealthy,
		EvidenceDigest: testDigest, ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(30 * time.Second).Format(time.RFC3339Nano),
		KeyID: "candidate-canary-v1",
	}, candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCandidateCanaryResult(ctx, result, now); err != nil {
		t.Fatal(err)
	}
	publicKey, err := CandidateCanaryPublicKey(candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewAuthorityController(store, map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: publicKey}})
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now.Add(time.Second) }
	switched, changed, err := controller.Reconcile(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("fresh immutable candidate canary did not trigger an authority switch")
	}
	if switched.Action != AuthorityCurrentSwitched || switched.Before != current || switched.After.CurrentRecordDigest != candidate.RecordDigest ||
		switched.After.CurrentWorkerSlot != AuthoritySlotB || switched.After.PreviousRecordDigest != current.CurrentRecordDigest || switched.Validate() != nil {
		t.Fatalf("switch receipt is invalid: %+v", switched)
	}
	live, _, _, err := store.LoadCurrent(ctx, group)
	if err != nil || live != switched.After {
		t.Fatalf("current authority did not switch: live=%+v err=%v", live, err)
	}
	reverted, err := controller.Revert(ctx, group, candidate.RecordDigest, result.ResultDigest)
	if err != nil {
		t.Fatal(err)
	}
	if reverted.Action != AuthorityCurrentReverted || reverted.After.CurrentRecordDigest != current.CurrentRecordDigest ||
		reverted.After.CurrentWorkerSlot != AuthoritySlotA || reverted.After.AuthorityEpoch != current.AuthorityEpoch+2 || reverted.Validate() != nil {
		t.Fatalf("revert receipt is invalid: %+v", reverted)
	}
	untouched, _, _, err := store.LoadCurrent(ctx, otherGroup.GroupID)
	if err != nil || untouched != otherGroup {
		t.Fatalf("group-local A->B->A changed another group: %+v err=%v", untouched, err)
	}
}

func TestAuthorityControllerWaitsForBoundCandidateCanary(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	group := "edge-pool-c"
	candidate := CandidateAuthority{
		APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: group,
		RecordDigest: otherDigest, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1,
	}
	if _, _, err := store.PutCandidate(ctx, candidate, "", ""); err != nil {
		t.Fatal(err)
	}
	setAuthorityObjectCAS(t, client, candidateAuthorityName(group), "candidate-c", "50")
	current := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 2,
	}
	if _, _, err := store.SwitchCurrent(ctx, current, "", ""); err != nil {
		t.Fatal(err)
	}
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-c", "60")
	staleAt := time.Unix(3_900, 0).UTC()
	stale, err := SignCandidateCanaryResult(CandidateCanaryResult{
		GroupID: group, CandidateRecordDigest: candidate.RecordDigest, WorkerSlot: candidate.WorkerSlot,
		ReleaseRecordDigest: candidate.ReleaseRecordDigest, RouteState: HealthHealthy, DependencyState: HealthHealthy,
		EvidenceDigest: testDigest, ObservedAt: staleAt.Format(time.RFC3339Nano), ExpiresAt: staleAt.Add(30 * time.Second).Format(time.RFC3339Nano),
		KeyID: "candidate-canary-v1",
	}, candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCandidateCanaryResult(ctx, stale, staleAt); err != nil {
		t.Fatal(err)
	}
	publicKey, err := CandidateCanaryPublicKey(candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewAuthorityController(store, map[string]CandidateCanaryVerifier{
		group: {KeyID: "candidate-canary-v1", Key: publicKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return time.Unix(4_000, 0).UTC() }
	if receipt, changed, err := controller.Reconcile(ctx, group); err != nil || changed || receipt.ReceiptDigest != "" {
		t.Fatalf("missing candidate canary was not a no-op: receipt=%+v changed=%v err=%v", receipt, changed, err)
	}
	live, _, _, err := store.LoadCurrent(ctx, group)
	if err != nil || live != current {
		t.Fatalf("missing candidate canary changed current authority: live=%+v err=%v", live, err)
	}
}

func TestAuthorityControllerRejectsBadCandidateWithoutChangingCurrent(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(3_000, 0).UTC()
	client := fake.NewSimpleClientset()
	store, err := NewAuthorityStore(client, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	group := "edge-pool-b"
	candidate := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: group, RecordDigest: otherDigest, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest, State: CandidateAuthorityLoaded, Generation: 1}
	_, _, _ = store.PutCandidate(ctx, candidate, "", "")
	setAuthorityObjectCAS(t, client, candidateAuthorityName(group), "candidate-b", "30")
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group, CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 8}
	_, _, _ = store.SwitchCurrent(ctx, current, "", "")
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-b", "40")
	result, err := SignCandidateCanaryResult(CandidateCanaryResult{
		GroupID: group, CandidateRecordDigest: candidate.RecordDigest, WorkerSlot: candidate.WorkerSlot, ReleaseRecordDigest: candidate.ReleaseRecordDigest,
		RouteState: HealthDegraded, DependencyState: HealthHealthy, EvidenceDigest: testDigest,
		ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(30 * time.Second).Format(time.RFC3339Nano), KeyID: "candidate-canary-v1",
	}, candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCandidateCanaryResult(ctx, result, now); err != nil {
		t.Fatal(err)
	}
	wrongKey, err := CandidateCanaryPublicKey([]byte("wrong-candidate-canary-signing-key-material-32"))
	if err != nil {
		t.Fatal(err)
	}
	wrongController, _ := NewAuthorityController(store, map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: wrongKey}})
	wrongController.now = func() time.Time { return now.Add(time.Second) }
	if _, err := wrongController.VerifyAndSwitch(ctx, group, result.ResultDigest); err == nil {
		t.Fatal("candidate canary signed by another key was accepted")
	}
	stillLoaded, _, _, err := store.LoadCandidate(ctx, group)
	if err != nil || stillLoaded.State != CandidateAuthorityLoaded {
		t.Fatalf("bad signature changed candidate state: %+v err=%v", stillLoaded, err)
	}
	publicKey, err := CandidateCanaryPublicKey(candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	controller, _ := NewAuthorityController(store, map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: publicKey}})
	controller.now = func() time.Time { return now.Add(time.Second) }
	receipt, err := controller.VerifyAndSwitch(ctx, group, result.ResultDigest)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Action != AuthorityCandidateRejected || receipt.Before != current || receipt.After != current || receipt.Validate() != nil {
		t.Fatalf("candidate rejection receipt is invalid: %+v", receipt)
	}
	live, _, _, err := store.LoadCurrent(ctx, group)
	if err != nil || live != current {
		t.Fatalf("rejected candidate changed current authority: live=%+v err=%v", live, err)
	}
	rejected, _, _, err := store.LoadCandidate(ctx, group)
	if err != nil || rejected.State != CandidateAuthorityRejected || rejected.CanaryResultDigest != result.ResultDigest {
		t.Fatalf("candidate was not rejected exactly: %+v err=%v", rejected, err)
	}
}

func setAuthorityObjectCAS(t *testing.T, client *fake.Clientset, name, uid, rv string) {
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
