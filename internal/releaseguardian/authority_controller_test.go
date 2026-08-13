package releaseguardian

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type testFrontActivator struct {
	beginErr    error
	transaction *testFrontTransaction
}
type testFrontTransaction struct {
	receipt     FrontAuthorityReceipt
	commitErr   error
	rollbackErr error
	committed   bool
	rolledBack  bool
}
type testAuthorityDecisionStore struct {
	*AuthorityStore
	baseline  AuthorityBaselineReceipt
	switchErr error
}

func (store testAuthorityDecisionStore) LoadBaselineReceipt(context.Context, string) (AuthorityBaselineReceipt, error) {
	return store.baseline, nil
}
func (store testAuthorityDecisionStore) LoadTransitionJournal(context.Context, string) (AuthorityTransitionJournal, bool, error) {
	return AuthorityTransitionJournal{}, false, nil
}

func (store testAuthorityDecisionStore) SwitchCurrent(ctx context.Context, authority CurrentAuthority, uid types.UID, rv string) (types.UID, string, error) {
	if store.switchErr != nil {
		return "", "", store.switchErr
	}
	return store.AuthorityStore.SwitchCurrent(ctx, authority, uid, rv)
}
func (store testAuthorityDecisionStore) CreateTransitionJournal(context.Context, AuthorityTransitionJournal) error {
	return nil
}
func (store testAuthorityDecisionStore) UpdateTransitionJournal(context.Context, AuthorityTransitionJournal, AuthorityTransitionJournal) error {
	return nil
}
func (store testAuthorityDecisionStore) DeleteTransitionJournal(context.Context, AuthorityTransitionJournal) error {
	return nil
}

func (activator testFrontActivator) BeginPromote(_ context.Context, target FrontAuthorityTarget) (FrontAuthorityTransaction, error) {
	if activator.beginErr != nil {
		return nil, activator.beginErr
	}
	transaction := activator.transaction
	if transaction == nil {
		transaction = &testFrontTransaction{}
	}
	transaction.receipt = FrontAuthorityReceipt{GroupID: target.GroupID, PreviousSlot: AuthoritySlotA, PreviousGeneration: 7,
		PreviousBundleGeneration: "previous-bundle-7", PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: testDigest,
		TargetSlot: target.TargetSlot, TargetGeneration: 8, TargetBundleGeneration: target.ServingGeneration + ".p8.r0",
		TargetWorkerSourceSHA: target.WorkerSourceSHA, TargetWorkerImageDigest: target.WorkerImageDigest}
	return transaction, nil
}
func (testFrontActivator) BeginRestore(_ context.Context, current CurrentAuthority) (FrontAuthorityTransaction, error) {
	return &testFrontTransaction{receipt: FrontAuthorityReceipt{GroupID: current.GroupID, PreviousSlot: current.CurrentWorkerSlot, PreviousGeneration: current.CurrentFrontGeneration,
		PreviousBundleGeneration: current.CurrentBundleGeneration, PreviousWorkerSourceSHA: current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: current.CurrentWorkerImageDigest,
		TargetSlot: current.PreviousWorkerSlot, TargetGeneration: current.CurrentFrontGeneration + 1, TargetBundleGeneration: current.PreviousBundleGeneration,
		TargetWorkerSourceSHA: current.PreviousWorkerSourceSHA, TargetWorkerImageDigest: current.PreviousWorkerImageDigest}}, nil
}
func (transaction *testFrontTransaction) Receipt() FrontAuthorityReceipt { return transaction.receipt }
func (transaction *testFrontTransaction) Commit(context.Context) error {
	transaction.committed = true
	return transaction.commitErr
}
func (transaction *testFrontTransaction) Rollback(context.Context) error {
	transaction.rolledBack = true
	return transaction.rollbackErr
}

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
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1,
	}
	candidate = bindCandidatePromotionWitness(candidate)
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
	result, err := SignCandidateCanaryResult(candidateResultFixture(candidate, now, HealthHealthy, HealthHealthy), candidateCanaryTestKey)
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
	decisionStore := testAuthorityDecisionStore{AuthorityStore: store, baseline: AuthorityBaselineReceipt{WorkerSlot: AuthoritySlotA,
		Nodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid", FrontResourceVersion: "10",
			WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11", ActivationGeneration: 7, BundleGeneration: "previous-bundle-7",
			ServingGeneration: "previous-serving-7", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}}}}
	controller, err := NewAuthorityControllerWithActivators(decisionStore, map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: publicKey}}, map[string]FrontAuthorityActivator{group: testFrontActivator{}})
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
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1,
	}
	candidate = bindCandidatePromotionWitness(candidate)
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
	stale, err := SignCandidateCanaryResult(candidateResultFixture(candidate, staleAt, HealthHealthy, HealthHealthy), candidateCanaryTestKey)
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
	candidate := CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: group, RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest, State: CandidateAuthorityLoaded, Generation: 1}
	candidate = bindCandidatePromotionWitness(candidate)
	_, _, _ = store.PutCandidate(ctx, candidate, "", "")
	setAuthorityObjectCAS(t, client, candidateAuthorityName(group), "candidate-b", "30")
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group, CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 8}
	_, _, _ = store.SwitchCurrent(ctx, current, "", "")
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-b", "40")
	result, err := SignCandidateCanaryResult(candidateResultFixture(candidate, now, HealthDegraded, HealthHealthy), candidateCanaryTestKey)
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

func TestAuthorityControllerDoesNotSwitchWhenProductionActivationFails(t *testing.T) {
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginErr: errors.New("prewrite drift")})
	if _, err := fixture.controller.VerifyAndSwitch(context.Background(), fixture.group, fixture.result.ResultDigest); err == nil || !strings.Contains(err.Error(), "prewrite drift") {
		t.Fatalf("activation failure was not returned: %v", err)
	}
	current, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || current != fixture.current {
		t.Fatalf("activation failure changed current: %+v err=%v", current, err)
	}
}

func TestAuthorityControllerRollsBackActivationWhenCurrentCASChanges(t *testing.T) {
	transaction := &testFrontTransaction{}
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{transaction: transaction})
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, switchErr: errors.New("current CAS changed")}
	if _, err := fixture.controller.VerifyAndSwitch(context.Background(), fixture.group, fixture.result.ResultDigest); err == nil {
		t.Fatal("CurrentAuthority CAS drift was accepted")
	}
	if !transaction.rolledBack || transaction.committed {
		t.Fatalf("CurrentAuthority CAS drift did not compensate activation: %+v", transaction)
	}
}

type authoritySwitchFixture struct {
	group      string
	client     *fake.Clientset
	store      *AuthorityStore
	controller *AuthorityController
	current    CurrentAuthority
	result     CandidateCanaryResult
	baseline   AuthorityBaselineReceipt
}

func newAuthoritySwitchFixture(t *testing.T, activator FrontAuthorityActivator) authoritySwitchFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Unix(5_000, 0).UTC()
	group := "edge-pool-failure"
	client := fake.NewSimpleClientset()
	store, _ := NewAuthorityStore(client, "fugue-system")
	candidate := bindCandidatePromotionWitness(CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: group,
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1})
	_, _, _ = store.PutCandidate(ctx, candidate, "", "")
	setAuthorityObjectCAS(t, client, candidateAuthorityName(group), "candidate-failure", "70")
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 4,
		CurrentFrontGeneration: 7, CurrentBundleGeneration: "previous-bundle-7", CurrentWorkerSourceSHA: testSHA,
		CurrentWorkerImageDigest: testDigest}
	_, _, _ = store.SwitchCurrent(ctx, current, "", "")
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-failure", "80")
	baseline := AuthorityBaselineReceipt{
		APIVersion: APIVersion, Kind: AuthorityBaselineReceiptKind, GroupID: group,
		BeforeRecordDigest: otherDigest, BeforeWorkerSlot: AuthoritySlotB, BeforeAuthorityEpoch: 3,
		RecordDigest: testDigest, WorkerSlot: AuthoritySlotA, AuthorityEpoch: 4, ObservedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		Nodes: []AuthorityBaselineNodeWitness{{ActivationGeneration: 7, BundleGeneration: "previous-bundle-7",
			NodeName: "edge-node-a", FrontPodUID: "front-pod-uid", FrontResourceVersion: "10", WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11",
			ServingGeneration: "previous-serving-7", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}}}
	baseline, _ = baseline.Seal()
	current.BaselineReceiptDigest = baseline.ReceiptDigest
	currentObject, _ := client.CoreV1().ConfigMaps("fugue-system").Get(ctx, currentAuthorityName(group), metav1.GetOptions{})
	currentObject.Data["authority.json"] = mustCanonicalJSON(t, current)
	currentObject.Data["baseline-receipt.json"] = mustCanonicalJSON(t, baseline)
	_, _ = client.CoreV1().ConfigMaps("fugue-system").Update(ctx, currentObject, metav1.UpdateOptions{})
	decisionStore := testAuthorityDecisionStore{AuthorityStore: store, baseline: baseline}
	result, _ := SignCandidateCanaryResult(candidateResultFixture(candidate, now, HealthHealthy, HealthHealthy), candidateCanaryTestKey)
	_ = store.CreateCandidateCanaryResult(ctx, result, now)
	publicKey, _ := CandidateCanaryPublicKey(candidateCanaryTestKey)
	controller, err := NewAuthorityControllerWithActivators(decisionStore,
		map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: publicKey}},
		map[string]FrontAuthorityActivator{group: activator})
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now.Add(time.Second) }
	return authoritySwitchFixture{group: group, client: client, store: store, controller: controller, current: current, result: result, baseline: decisionStore.baseline}
}

func mustCanonicalJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
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
