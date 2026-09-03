package releaseguardian

import (
	"context"
	"errors"
	"strconv"
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
	beginCount  *int
	transaction *testFrontTransaction
	settled     bool
	settleErr   error
}

var errTestAuthorityPrewriteCAS = errors.New("test authority prewrite CAS changed")

type testAuthorityHealthObserver struct {
	currentHealthy bool
	lkgHealthy     bool
	digest         string
	err            error
}

func (observer testAuthorityHealthObserver) ObserveCurrentAndLKG(context.Context, CurrentAuthority) (bool, bool, string, error) {
	return observer.currentHealthy, observer.lkgHealthy, observer.digest, observer.err
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
	baseline    AuthorityBaselineReceipt
	switchErr   error
	createErr   error
	deleteCount *int
	journal     *AuthorityTransitionJournal
	claimErr    error
}

func (store testAuthorityDecisionStore) ClaimVerifiedCandidate(ctx context.Context, candidate CandidateAuthority, uid types.UID, rv string) error {
	if store.claimErr != nil {
		return store.claimErr
	}
	return store.AuthorityStore.ClaimVerifiedCandidate(ctx, candidate, uid, rv)
}

func (store testAuthorityDecisionStore) LoadBaselineReceipt(context.Context, string) (AuthorityBaselineReceipt, error) {
	return store.baseline, nil
}

func (store testAuthorityDecisionStore) LoadTransitionJournal(context.Context, string) (AuthorityTransitionJournal, bool, error) {
	if store.journal != nil && store.journal.JournalDigest != "" {
		return *store.journal, true, nil
	}
	return AuthorityTransitionJournal{}, false, nil
}

func (store testAuthorityDecisionStore) SwitchCurrent(ctx context.Context, authority CurrentAuthority, uid types.UID, rv string) (types.UID, string, error) {
	if store.switchErr != nil {
		return "", "", store.switchErr
	}
	return store.AuthorityStore.SwitchCurrent(ctx, authority, uid, rv)
}
func (store testAuthorityDecisionStore) CreateTransitionJournal(_ context.Context, journal AuthorityTransitionJournal) error {
	if store.journal != nil {
		*store.journal = journal
	}
	return store.createErr
}
func (store testAuthorityDecisionStore) UpdateTransitionJournal(context.Context, AuthorityTransitionJournal, AuthorityTransitionJournal) error {
	return nil
}
func (store testAuthorityDecisionStore) DeleteTransitionJournal(_ context.Context, journal AuthorityTransitionJournal) error {
	if store.deleteCount != nil {
		*store.deleteCount++
	}
	if store.journal != nil && store.journal.JournalDigest == journal.JournalDigest {
		*store.journal = AuthorityTransitionJournal{}
	}
	return nil
}

func (activator testFrontActivator) BeginPromote(_ context.Context, target FrontAuthorityTarget) (FrontAuthorityTransaction, error) {
	if activator.beginCount != nil {
		*activator.beginCount++
	}
	if activator.beginErr != nil {
		return nil, activator.beginErr
	}
	transaction := activator.transaction
	if transaction == nil {
		transaction = &testFrontTransaction{}
	}
	transaction.receipt = FrontAuthorityReceipt{GroupID: target.GroupID, PreviousSlot: target.PreviousSlot, PreviousGeneration: target.PreviousFrontGeneration,
		PreviousBundleGeneration: target.PreviousBundleGeneration, PreviousWorkerSourceSHA: target.PreviousWorkerSourceSHA, PreviousWorkerImageDigest: target.PreviousWorkerImageDigest,
		TargetSlot: target.TargetSlot, TargetGeneration: target.PreviousFrontGeneration + 1, TargetBundleGeneration: target.ServingGeneration + ".p8.r0",
		TargetWorkerSourceSHA: target.WorkerSourceSHA, TargetWorkerImageDigest: target.WorkerImageDigest}
	return transaction, nil
}
func (activator testFrontActivator) CompensationSettled(context.Context, AuthorityTransitionJournal) (bool, error) {
	return activator.settled, activator.settleErr
}
func (testFrontActivator) IsPrewriteCASChanged(err error) bool {
	return errors.Is(err, errTestAuthorityPrewriteCAS)
}
func (testFrontActivator) BeginRestore(_ context.Context, current CurrentAuthority) (FrontAuthorityTransaction, error) {
	targetBundle := current.PreviousBundleGeneration
	if base, publication, recovery, ok := splitAuthorityBundleGeneration(targetBundle); ok {
		targetBundle = base + ".p" + strconv.FormatUint(publication+1, 10) + ".r" + strconv.FormatUint(recovery+1, 10)
	}
	return &testFrontTransaction{receipt: FrontAuthorityReceipt{GroupID: current.GroupID, PreviousSlot: current.CurrentWorkerSlot, PreviousGeneration: current.CurrentFrontGeneration,
		PreviousBundleGeneration: current.CurrentBundleGeneration, PreviousWorkerSourceSHA: current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: current.CurrentWorkerImageDigest,
		TargetSlot: current.PreviousWorkerSlot, TargetGeneration: current.CurrentFrontGeneration + 1, TargetBundleGeneration: targetBundle,
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

func TestAuthorityControllerRevertsOnlyAfterThreeCandidateOnlyRouteFailures(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_500, 0).UTC()
	client := fake.NewSimpleClientset()
	store, _ := NewAuthorityStore(client, "fugue-system")
	group := "edge-pool-health-a"
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
		CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, CurrentFrontGeneration: 8,
		CurrentBundleGeneration: "candidate-bundle.p8.r0", CurrentWorkerSourceSHA: strings.Repeat("2", 40),
		CurrentWorkerImageDigest: otherDigest, PreviousRecordDigest: testDigest, PreviousWorkerSlot: AuthoritySlotA,
		PreviousFrontGeneration: 7, PreviousBundleGeneration: "previous-bundle.p7.r0", PreviousWorkerSourceSHA: testSHA,
		PreviousWorkerImageDigest: testDigest, AuthorityEpoch: 5}
	_, _, _ = store.SwitchCurrent(ctx, current, "", "")
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-health-a", "90")
	observer := testAuthorityHealthObserver{currentHealthy: false, lkgHealthy: true, digest: testDigest}
	controller, err := NewAuthorityControllerWithActivators(testAuthorityDecisionStore{AuthorityStore: store},
		map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: make([]byte, 32)}},
		map[string]FrontAuthorityActivator{group: testFrontActivator{}})
	if err != nil || controller.SetHealthObservers(map[string]AuthorityHealthObserver{group: observer}) != nil {
		t.Fatalf("create health controller: %v", err)
	}
	controller.now = func() time.Time { return now }
	for attempt := 1; attempt <= 2; attempt++ {
		if receipt, changed, err := controller.ObserveAndRevert(ctx, group); err != nil || changed || receipt.Action != "" {
			t.Fatalf("premature rollback attempt=%d receipt=%+v changed=%v err=%v", attempt, receipt, changed, err)
		}
	}
	receipt, changed, err := controller.ObserveAndRevert(ctx, group)
	if err != nil || !changed || receipt.Action != AuthorityCurrentReverted || receipt.Before != current ||
		receipt.After.CurrentRecordDigest != current.PreviousRecordDigest || receipt.After.CurrentFrontGeneration != current.CurrentFrontGeneration+1 {
		t.Fatalf("bounded rollback receipt=%+v changed=%v err=%v", receipt, changed, err)
	}
}

func TestAuthorityControllerDoesNotRevertSharedDependencyFailure(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store, _ := NewAuthorityStore(client, "fugue-system")
	group := "edge-pool-health-b"
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: group,
		CurrentRecordDigest: otherDigest, CurrentWorkerSlot: AuthoritySlotB, PreviousRecordDigest: testDigest,
		PreviousWorkerSlot: AuthoritySlotA, AuthorityEpoch: 5}
	_, _, _ = store.SwitchCurrent(ctx, current, "", "")
	setAuthorityObjectCAS(t, client, currentAuthorityName(group), "current-health-b", "91")
	controller, err := NewAuthorityControllerWithActivators(testAuthorityDecisionStore{AuthorityStore: store},
		map[string]CandidateCanaryVerifier{group: {KeyID: "candidate-canary-v1", Key: make([]byte, 32)}},
		map[string]FrontAuthorityActivator{group: testFrontActivator{}})
	if err != nil || controller.SetHealthObservers(map[string]AuthorityHealthObserver{group: testAuthorityHealthObserver{
		currentHealthy: false, lkgHealthy: false, digest: testDigest,
	}}) != nil {
		t.Fatalf("create dependency health controller: %v", err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		if _, changed, err := controller.ObserveAndRevert(ctx, group); err != nil || changed {
			t.Fatalf("shared dependency failure triggered rollback: changed=%v err=%v", changed, err)
		}
	}
	live, _, _, _ := store.LoadCurrent(ctx, group)
	if live != current {
		t.Fatalf("shared dependency failure changed current authority: %+v", live)
	}
}

func TestRestoredBundleGenerationAllowsNewerSignedConfiguration(t *testing.T) {
	previous := "route-generation.p11377.r127"
	if !restoredBundleGenerationMatches("route-generation.p11486.r131", previous) {
		t.Fatal("monotonically refreshed exact LKG was rejected")
	}
	if !restoredBundleGenerationMatches("new-route-generation.p11486.r127", previous) {
		t.Fatal("newer signed configuration for the same LKG code was rejected")
	}
	if !restoredBundleGenerationMatches(previous, previous) {
		t.Fatal("unchanged exact LKG was rejected")
	}
	for name, value := range map[string]string{
		"changed same publication": "other-generation.p11377.r131",
		"stale publication":        "route-generation.p11376.r131", "stale recovery": "route-generation.p11486.r127",
		"older changed recovery": "other-generation.p11486.r126", "malformed": "route-generation",
	} {
		t.Run(name, func(t *testing.T) {
			if restoredBundleGenerationMatches(value, previous) {
				t.Fatal("invalid restored LKG generation was accepted")
			}
		})
	}
}

func TestAuthorityControllerRetiresPreparedJournalAfterCommittedCurrentCAS(t *testing.T) {
	beginCount, deleted := 0, 0
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginCount: &beginCount})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	journal, err := (AuthorityTransitionJournal{GroupID: fixture.group, Phase: AuthorityTransitionPrepared,
		CurrentUID: "current-failure", CurrentRV: "80", Before: fixture.current, Candidate: verified,
		CanaryResultDigest: fixture.result.ResultDigest, PreviousNodes: append([]AuthorityBaselineNodeWitness(nil), fixture.baseline.Nodes...),
		CreatedAt: time.Unix(5_000, 0).UTC().Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	committed := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: fixture.group,
		CurrentRecordDigest: verified.RecordDigest, CurrentWorkerSlot: verified.WorkerSlot,
		CurrentFrontGeneration: 100, CurrentBundleGeneration: verified.ServingGeneration + ".p8.r0",
		CurrentWorkerSourceSHA: verified.WorkerSourceSHA, CurrentWorkerImageDigest: verified.WorkerImageDigest,
		PreviousRecordDigest: fixture.current.CurrentRecordDigest, PreviousWorkerSlot: fixture.current.CurrentWorkerSlot,
		PreviousFrontGeneration: 99, PreviousBundleGeneration: fixture.current.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: fixture.current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: fixture.current.CurrentWorkerImageDigest,
		AuthorityEpoch: fixture.current.AuthorityEpoch + 20, BaselineReceiptDigest: fixture.current.BaselineReceiptDigest}
	changed := committed
	changed.PreviousBundleGeneration = "unrelated-bundle.p98.r1"
	if preparedJournalMatchesCommittedAuthority(changed, journal) {
		t.Fatal("prepared journal with unrelated predecessor was accepted")
	}
	changed = committed
	changed.CurrentFrontGeneration++
	if preparedJournalMatchesCommittedAuthority(changed, journal) {
		t.Fatal("prepared journal with non-adjacent current generation was accepted")
	}
	object, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), currentAuthorityName(fixture.group), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Data["authority.json"] = mustCanonicalJSON(t, committed)
	if _, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline,
		journal: &journal, deleteCount: &deleted}
	if receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group); err != nil || changed ||
		receipt.ReceiptDigest != "" || deleted != 1 || beginCount != 0 || journal.JournalDigest != "" {
		t.Fatalf("committed prepared journal was not retired: receipt=%+v changed=%v deleted=%d begins=%d journal=%+v err=%v",
			receipt, changed, deleted, beginCount, journal, err)
	}
}

func TestAuthorityControllerDoesNotReplayRevertedVerifiedCandidate(t *testing.T) {
	beginCount, deleted := 0, 0
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginCount: &beginCount})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	reverted := fixture.current
	reverted.PreviousRecordDigest, reverted.PreviousWorkerSlot = verified.RecordDigest, verified.WorkerSlot
	reverted.PreviousFrontGeneration = reverted.CurrentFrontGeneration - 1
	reverted.PreviousBundleGeneration = verified.ServingGeneration + ".p8.r0"
	reverted.PreviousWorkerSourceSHA, reverted.PreviousWorkerImageDigest = verified.WorkerSourceSHA, verified.WorkerImageDigest
	object, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), currentAuthorityName(fixture.group), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Data["authority.json"] = mustCanonicalJSON(t, reverted)
	if _, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group); err != nil || changed ||
		receipt.ReceiptDigest != "" || beginCount != 0 {
		t.Fatalf("reverted verified candidate was replayed: receipt=%+v changed=%v begins=%d err=%v", receipt, changed, beginCount, err)
	}

	journal, err := (AuthorityTransitionJournal{GroupID: fixture.group, Phase: AuthorityTransitionPrepared,
		CurrentUID: "current-failure", CurrentRV: "80", Before: reverted, Candidate: verified,
		CanaryResultDigest: fixture.result.ResultDigest, PreviousNodes: append([]AuthorityBaselineNodeWitness(nil), fixture.baseline.Nodes...),
		CreatedAt: time.Unix(5_000, 0).UTC().Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline,
		journal: &journal, deleteCount: &deleted}
	if receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group); err != nil || changed ||
		receipt.ReceiptDigest != "" || deleted != 1 || beginCount != 0 || journal.JournalDigest != "" {
		t.Fatalf("reverted candidate journal was not retired: receipt=%+v changed=%v deleted=%d begins=%d journal=%+v err=%v",
			receipt, changed, deleted, beginCount, journal, err)
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

func TestAuthorityControllerRejectsLegacyControlSelfCandidateBeforeJournalOrTraffic(t *testing.T) {
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{})
	candidate, uid, rv, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	legacy := candidate
	legacy.WorkerSourceSHA, legacy.WorkerImageDigest = "", ""
	object, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), candidateAuthorityName(fixture.group), metav1.GetOptions{})
	if err != nil || object.UID != uid || object.ResourceVersion != rv {
		t.Fatal(err)
	}
	object.Data["candidate.json"] = mustCanonicalJSON(t, legacy)
	if _, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	before := fixture.current
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err != nil || changed || receipt.ReceiptDigest != "" {
		t.Fatalf("legacy Control self-candidate was not inert: receipt=%+v changed=%v err=%v", receipt, changed, err)
	}
	after, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || after != before {
		t.Fatalf("legacy Control self-candidate changed authority: before=%+v after=%+v err=%v", before, after, err)
	}
	if _, exists, err := fixture.store.LoadTransitionJournal(context.Background(), fixture.group); err != nil || exists {
		t.Fatalf("legacy Control self-candidate created a journal: exists=%v err=%v", exists, err)
	}
}

func TestAuthorityControllerPreservesHistoricalJournalWithoutWorkerIdentity(t *testing.T) {
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{})
	candidate, uid, rv, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	candidate.WorkerSourceSHA, candidate.WorkerImageDigest = "", ""
	candidate.State, candidate.Generation, candidate.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	object, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), candidateAuthorityName(fixture.group), metav1.GetOptions{})
	if err != nil || object.UID != uid || object.ResourceVersion != rv {
		t.Fatal(err)
	}
	object.Data["candidate.json"] = mustCanonicalJSON(t, candidate)
	if _, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	journal, err := (AuthorityTransitionJournal{GroupID: fixture.group, Phase: AuthorityTransitionPrepared,
		CurrentUID: "current-uid", CurrentRV: "80", Before: fixture.current, Candidate: candidate,
		CanaryResultDigest: fixture.result.ResultDigest, PreviousNodes: fixture.baseline.Nodes,
		CreatedAt: time.Unix(5_001, 0).UTC().Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, journal: &journal}
	before := fixture.current
	if _, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group); err == nil || changed ||
		!strings.Contains(err.Error(), "lacks Worker release identity") {
		t.Fatalf("historical journal was not rejected: changed=%v err=%v", changed, err)
	}
	after, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || after != before || journal.JournalDigest == "" {
		t.Fatalf("historical journal rejection changed evidence: before=%+v after=%+v journal=%+v err=%v", before, after, journal, err)
	}
}

func TestAuthorityControllerPersistsPreparedJournalBeforeTerminalCandidate(t *testing.T) {
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{})
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, createErr: errors.New("journal unavailable")}
	if _, err := fixture.controller.VerifyAndSwitch(context.Background(), fixture.group, fixture.result.ResultDigest); err == nil || !strings.Contains(err.Error(), "journal unavailable") {
		t.Fatalf("missing journal error=%v", err)
	}
	candidate, _, _, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil || candidate.State != CandidateAuthorityLoaded || candidate.CanaryResultDigest != "" {
		t.Fatalf("journal failure terminalized candidate: %+v err=%v", candidate, err)
	}
}

func TestAuthorityControllerDropsPreparedJournalAfterTypedPrewriteCASWithoutTrafficChange(t *testing.T) {
	journal := AuthorityTransitionJournal{}
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginErr: errTestAuthorityPrewriteCAS})
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, journal: &journal}
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err != nil || changed || receipt.ReceiptDigest != "" || journal.JournalDigest != "" {
		t.Fatalf("typed prewrite CAS was not settled: receipt=%+v changed=%v journal=%+v err=%v", receipt, changed, journal, err)
	}
	current, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || current != fixture.current {
		t.Fatalf("typed prewrite CAS changed traffic authority: current=%+v err=%v", current, err)
	}
	candidate, _, _, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil || candidate.State != CandidateAuthorityVerified {
		t.Fatalf("typed prewrite CAS did not retain terminal candidate: candidate=%+v err=%v", candidate, err)
	}
}

func TestAuthorityControllerReplaysVerifiedCandidateAfterJournalWasSettled(t *testing.T) {
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := fixture.store.LoadTransitionJournal(context.Background(), fixture.group); err != nil || exists {
		t.Fatalf("verified candidate unexpectedly has a transition journal: exists=%v err=%v", exists, err)
	}
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err != nil || !changed || receipt.Action != AuthorityCurrentSwitched {
		t.Fatalf("verified candidate was not replayed: receipt=%+v changed=%v err=%v", receipt, changed, err)
	}
	current, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || current.CurrentRecordDigest != verified.RecordDigest || current.CurrentWorkerSlot != verified.WorkerSlot {
		t.Fatalf("replayed candidate did not become current: current=%+v err=%v", current, err)
	}
}

func TestAuthorityControllerLeavesVerifiedCurrentCandidateSettled(t *testing.T) {
	beginCount := 0
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginCount: &beginCount})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	settled := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: fixture.group,
		CurrentRecordDigest: verified.RecordDigest, CurrentWorkerSlot: verified.WorkerSlot,
		CurrentFrontGeneration: fixture.current.CurrentFrontGeneration + 1, CurrentBundleGeneration: verified.ServingGeneration + ".p8.r0",
		CurrentWorkerSourceSHA: verified.WorkerSourceSHA, CurrentWorkerImageDigest: verified.WorkerImageDigest,
		PreviousRecordDigest: fixture.current.CurrentRecordDigest, PreviousWorkerSlot: fixture.current.CurrentWorkerSlot,
		PreviousFrontGeneration: fixture.current.CurrentFrontGeneration, PreviousBundleGeneration: fixture.current.CurrentBundleGeneration,
		PreviousWorkerSourceSHA: fixture.current.CurrentWorkerSourceSHA, PreviousWorkerImageDigest: fixture.current.CurrentWorkerImageDigest,
		AuthorityEpoch: fixture.current.AuthorityEpoch + 1, BaselineReceiptDigest: fixture.current.BaselineReceiptDigest}
	object, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Get(context.Background(), currentAuthorityName(fixture.group), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Data["authority.json"] = mustCanonicalJSON(t, settled)
	if _, err := fixture.client.CoreV1().ConfigMaps("fugue-system").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err != nil || changed || receipt.ReceiptDigest != "" || beginCount != 0 {
		t.Fatalf("settled current candidate was replayed: receipt=%+v changed=%v begins=%d err=%v", receipt, changed, beginCount, err)
	}
}

func TestAuthorityControllerDoesNotReplayCandidateLostDuringClaim(t *testing.T) {
	beginCount := 0
	journal := AuthorityTransitionJournal{}
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginCount: &beginCount})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, journal: &journal, claimErr: errors.New("candidate replacement won CAS")}
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err == nil || changed || receipt.ReceiptDigest != "" || !strings.Contains(err.Error(), "candidate replacement won CAS") {
		t.Fatalf("lost candidate claim was accepted: receipt=%+v changed=%v err=%v", receipt, changed, err)
	}
	if beginCount != 0 || journal.JournalDigest != "" {
		t.Fatalf("lost candidate claim reached traffic or retained its journal: begins=%d journal=%+v", beginCount, journal)
	}
	current, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || current != fixture.current {
		t.Fatalf("lost candidate claim changed current authority: current=%+v err=%v", current, err)
	}
}

func TestAuthorityControllerDropsResumedPreparedJournalAfterTypedPrewriteCASWithoutTrafficChange(t *testing.T) {
	journal := AuthorityTransitionJournal{}
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{beginErr: errTestAuthorityPrewriteCAS})
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline, journal: &journal}
	if _, err := fixture.controller.VerifyAndSwitch(context.Background(), fixture.group, fixture.result.ResultDigest); !errors.Is(err, errTestAuthorityPrewriteCAS) {
		t.Fatalf("prepared fixture did not stop at the typed prewrite CAS: %v", err)
	}
	if journal.Phase != AuthorityTransitionPrepared || journal.JournalDigest == "" {
		t.Fatalf("prepared fixture did not retain its journal: %+v", journal)
	}
	receipt, changed, err := fixture.controller.Reconcile(context.Background(), fixture.group)
	if err != nil || changed || receipt.ReceiptDigest != "" || journal.JournalDigest != "" {
		t.Fatalf("resumed typed prewrite CAS was not settled: receipt=%+v changed=%v journal=%+v err=%v", receipt, changed, journal, err)
	}
	current, _, _, err := fixture.store.LoadCurrent(context.Background(), fixture.group)
	if err != nil || current != fixture.current {
		t.Fatalf("resumed typed prewrite CAS changed traffic authority: current=%+v err=%v", current, err)
	}
	candidate, _, _, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil || candidate.State != CandidateAuthorityVerified {
		t.Fatalf("resumed typed prewrite CAS did not retain terminal candidate: candidate=%+v err=%v", candidate, err)
	}
}

func TestAuthorityControllerRetiresOnlyProvenCompensatedActivatedJournal(t *testing.T) {
	deleted := 0
	controller := &AuthorityController{store: testAuthorityDecisionStore{deleteCount: &deleted},
		activators: map[string]FrontAuthorityActivator{"edge-pool-a": testFrontActivator{settled: true}}, now: time.Now}
	current := CurrentAuthority{APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: "edge-pool-a",
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 7}
	candidate := bindCandidatePromotionWitness(CandidateAuthority{APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: current.GroupID,
		RecordDigest: otherDigest, BundleGeneration: testCandidateBundle, WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 1})
	candidate.State, candidate.Generation, candidate.CanaryResultDigest = CandidateAuthorityVerified, 2, testDigest
	journal, err := (AuthorityTransitionJournal{GroupID: current.GroupID, Phase: AuthorityTransitionActivated,
		CurrentUID: "current-uid", CurrentRV: "20", Before: current, Candidate: candidate, CanaryResultDigest: testDigest,
		PreviousNodes: []AuthorityBaselineNodeWitness{{NodeName: "edge-node-a", FrontPodUID: "front-pod-uid", FrontResourceVersion: "10",
			WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11", ActivationGeneration: 7, BundleGeneration: "previous-bundle-7",
			ServingGeneration: "previous-serving-7", WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest}},
		Activation: &FrontAuthorityReceipt{GroupID: current.GroupID, PreviousSlot: AuthoritySlotA, PreviousGeneration: 7,
			PreviousBundleGeneration: "previous-bundle-7", PreviousWorkerSourceSHA: testSHA, PreviousWorkerImageDigest: testDigest,
			TargetSlot: AuthoritySlotB, TargetGeneration: 8, TargetBundleGeneration: "candidate-serving.p8.r0",
			TargetWorkerSourceSHA: testSHA, TargetWorkerImageDigest: otherDigest},
		CreatedAt: time.Unix(8_000, 0).UTC().Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := controller.resumeTransition(context.Background(), current, journal); err != nil || changed || deleted != 1 {
		t.Fatalf("settled journal changed=%v deleted=%d err=%v", changed, deleted, err)
	}
	controller.activators[current.GroupID] = testFrontActivator{settled: false}
	if _, _, err := controller.resumeTransition(context.Background(), current, journal); err == nil || deleted != 1 {
		t.Fatalf("unproven journal was retired: deleted=%d err=%v", deleted, err)
	}
}

func TestAuthorityControllerActivatedResumeUsesJournalFrontGeneration(t *testing.T) {
	transaction := &testFrontTransaction{}
	fixture := newAuthoritySwitchFixture(t, testFrontActivator{transaction: transaction})
	candidate, candidateUID, candidateRV, err := fixture.store.LoadCandidate(context.Background(), fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	verified := candidate
	verified.State, verified.Generation, verified.CanaryResultDigest = CandidateAuthorityVerified, candidate.Generation+1, fixture.result.ResultDigest
	if _, _, err := fixture.store.PutCandidate(context.Background(), verified, candidateUID, candidateRV); err != nil {
		t.Fatal(err)
	}
	journal, err := (AuthorityTransitionJournal{GroupID: fixture.group, Phase: AuthorityTransitionActivated,
		CurrentUID: "current-failure", CurrentRV: "80", Before: fixture.current, Candidate: verified,
		CanaryResultDigest: fixture.result.ResultDigest, PreviousNodes: append([]AuthorityBaselineNodeWitness(nil), fixture.baseline.Nodes...),
		Activation: &FrontAuthorityReceipt{GroupID: fixture.group, PreviousSlot: fixture.current.CurrentWorkerSlot, PreviousGeneration: 9,
			PreviousBundleGeneration: fixture.current.CurrentBundleGeneration, PreviousWorkerSourceSHA: fixture.current.CurrentWorkerSourceSHA,
			PreviousWorkerImageDigest: fixture.current.CurrentWorkerImageDigest, TargetSlot: verified.WorkerSlot, TargetGeneration: 10,
			TargetBundleGeneration: verified.ServingGeneration + ".p8.r0", TargetWorkerSourceSHA: testSHA, TargetWorkerImageDigest: testDigest},
		CreatedAt: time.Unix(5_000, 0).UTC().Format(time.RFC3339Nano)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller.store = testAuthorityDecisionStore{AuthorityStore: fixture.store, baseline: fixture.baseline}
	if _, err := fixture.controller.verifyAndSwitch(context.Background(), fixture.group, fixture.result.ResultDigest, &journal); err != nil {
		t.Fatalf("%v expected=%+v actual=%+v", err, *journal.Activation, transaction.receipt)
	}
	if transaction.receipt.PreviousGeneration != 9 || transaction.receipt.TargetGeneration != 10 {
		t.Fatalf("activated resume ignored journal generation: %+v", transaction.receipt)
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
