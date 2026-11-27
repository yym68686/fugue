package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func nodeWorkerFenceFixture(t *testing.T) edgegroupfront.NodeWorkerJournal {
	t.Helper()
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	digest := func(s string) string { return "sha256:" + strings.Repeat(s, 64) }
	uid := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }
	j, err := (edgegroupfront.NodeWorkerJournal{
		Schema:    edgegroupfront.NodeWorkerJournalSchema,
		Recipient: edgegroupfront.NativeRecipient{PodUID: uid(1), Node: "pilot", Group: "edge-group-test", Incarnation: digest("a")},
		NodeUID:   uid(2), ServiceUID: uid(3), PredecessorWorkerUID: uid(4), CandidateWorkerUID: uid(5),
		PredecessorProcess: digest("e"), CandidateProcess: digest("f"), CandidateComponent: "test-candidate",
		Expected: edgegroupfront.ActivationState{
			Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: "edge-group-test", Generation: 912, ActiveSlot: "b",
			BundleGeneration: "previous.p45.r1", WorkerSourceCommit: strings.Repeat("b", 40), WorkerImageDigest: digest("b"),
			Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationInit,
			UpdatedAt: now, Reason: "verified physical predecessor",
		},
		Candidate: edgegroupfront.ActivationRepairRelease{
			Slot: "a", BundleGeneration: "current.p46.r1", WorkerSourceCommit: strings.Repeat("c", 40), WorkerImageDigest: digest("c"),
		},
		ReleaseDigest: digest("d"), CreatedAt: now.Add(time.Second),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestNativeNodeWorkerFenceBindsDurableClaimAndInputs(t *testing.T) {
	ctx := t.Context()
	const namespace = "fugue-system"
	j := nodeWorkerFenceFixture(t)
	start := j.CreatedAt.Add(time.Hour)
	claim := productioncontrol.Claim{
		Scope: productioncontrol.Scope{ClusterID: "production_1", ResourceID: productioncontrol.FrontWorkerResource(namespace, j.Recipient.Group)},
		Fence: 3, IntentDigest: digest([]byte("intent")), ApprovalDigest: digest([]byte("approval")),
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID, object.ResourceVersion, object.CreationTimestamp = types.UID(object.Name), "23", metav1.NewTime(start)
		return false, nil, nil
	})
	store, err := NewAuthorityStore(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.ClaimNativeNodeWorker(ctx, j, claim)
	if err != nil || fence.Claim != claim || !fence.ClaimedAt.Equal(start) {
		t.Fatalf("wrong native fence: %+v %v", fence, err)
	}
	restart, _ := NewAuthorityStore(client, namespace)
	restored, found, err := restart.LoadNodeWorker(ctx, j.Recipient.Group)
	if err != nil || !found || restored != fence {
		t.Fatalf("lost the native claim across restart: %+v %v", restored, err)
	}
	if _, err := restart.ClaimNodeWorker(ctx, j); err == nil {
		t.Fatal("legacy claim adopted an already native transaction")
	}
	wrong := claim
	wrong.Fence++
	if _, err := restart.ClaimNativeNodeWorker(ctx, j, wrong); err == nil {
		t.Fatal("a different PostgreSQL claim adopted the same Kubernetes fence")
	}
	arm := NodeWorkerArm{
		Schema: NodeWorkerArmSchema, JournalDigest: j.Digest, OpenedAt: start, Deadline: start.Add(2 * time.Minute),
		Held: NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "retained", StartedAt: start, Slot: j.Expected.ActiveSlot, Target: "192.0.2.7:28443"},
	}
	otherFence := fence
	otherFence.Claim = wrong
	if err := restart.RecordNativeNodeWorkerArm(ctx, otherFence, arm, start); err == nil {
		t.Fatal("a mismatched claim armed the Front")
	}
	if err := restart.RecordNativeNodeWorkerArm(ctx, fence, arm, start); err != nil {
		t.Fatal(err)
	}
	if saved, ok, err := restart.ReadNodeWorkerArm(ctx, j, start); err != nil || !ok || !saved.Held.Same(arm.Held) {
		t.Fatalf("missing native arm: %+v %v", saved, err)
	}
	if _, err := restart.ClaimNativeNodeWorker(ctx, j, claim); err == nil {
		t.Fatal("a second dispatch adopted an already armed journal")
	}
	if err := restart.RecordNodeWorkerResult(ctx, fence, j.Expected, "expired_without_cutover", nil); err != nil {
		t.Fatal(err)
	}
	result, found, err := restart.ReadNodeWorkerResult(ctx, j)
	if err != nil || !found || !result.MatchesExecution(claim) || result.MatchesExecution(wrong) {
		t.Fatalf("terminal result did not keep the exact production claim: %+v, %v", result, err)
	}
	if err := restart.FinalizeNodeWorker(ctx, fence); err != nil {
		t.Fatal(err)
	}
}

func TestNodeWorkerFenceRetainsItsOnlyOwnerAcrossGuardianRestart(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID, object.ResourceVersion = "api-node-fence", "23"
		return false, nil, nil
	})
	const ns = "fugue-system"
	store, err := NewAuthorityStore(client, ns)
	if err != nil {
		t.Fatal(err)
	}
	j := nodeWorkerFenceFixture(t)
	if _, found, err := store.LoadNodeWorker(ctx, j.Recipient.Group); err != nil || found {
		t.Fatalf("unexpected preexisting node fence: %v", err)
	}
	fence, err := store.ClaimNodeWorker(ctx, j)
	if err != nil || fence.UID == "" || fence.Journal.Digest != j.Digest {
		t.Fatalf("failed to claim the exact node journal: %+v err=%v", fence, err)
	}
	restarted, _ := NewAuthorityStore(client, ns)
	resumed, found, err := restarted.LoadNodeWorker(ctx, j.Recipient.Group)
	if err != nil || !found || resumed != fence {
		t.Fatalf("restart lost the immutable projection: %+v err=%v", resumed, err)
	}
	if replay, err := restarted.ClaimNodeWorker(ctx, j); err != nil || replay != resumed {
		t.Fatalf("create replay did not recover the same owner: %+v, %v", replay, err)
	}
	if err := RequireNoFrontRepair(ctx, client, ns, j.Recipient.Group); !errors.Is(err, ErrNodeWorkerInProgress) {
		t.Fatalf("ordinary mutation was not fenced: %v", err)
	}
	other := j
	other.Recipient.Node = "different-node"
	other, err = other.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ClaimNodeWorker(ctx, other); err == nil {
		t.Fatal("another node was allowed to take over the existing group mutation")
	}
}

func TestNodeWorkerFenceRejectsActivatedGroupTransitionAndMalformedProjection(t *testing.T) {
	ctx := context.Background()
	j := nodeWorkerFenceFixture(t)
	const ns = "fugue-system"
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: transitionActivatedJournalName(j.Recipient.Group)}}
	client := fake.NewSimpleClientset(object)
	store, _ := NewAuthorityStore(client, ns)
	if _, err := store.ClaimNodeWorker(ctx, j); err == nil {
		t.Fatal("node writer proceeded while ordinary activated journal was still present")
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: FrontRepairFenceName(j.Recipient.Group),
			Labels: map[string]string{"fugue.pro/group": j.Recipient.Group, "fugue.pro/authority-kind": nodeWorkerJournalKind}},
		Data: map[string]string{"journal.json": "{invalid}"},
	}
	if _, err := client.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadNodeWorker(ctx, j.Recipient.Group); found || err == nil {
		t.Fatal("Guardian resumed a malformed journal")
	}
	if err := RequireNoFrontRepair(ctx, client, ns, j.Recipient.Group); !errors.Is(err, ErrNodeWorkerInProgress) {
		t.Fatalf("invalid node projection allowed ordinary mutation: %v", err)
	}
}

func TestNodeWorkerShadowFenceRejectsAnyExistingOwner(t *testing.T) {
	ctx := context.Background()
	j := nodeWorkerFenceFixture(t)
	group := j.Recipient.Group
	const ns = "fugue-system"
	for _, name := range []string{"", transitionActivatedJournalName(group), FrontRepairFenceName(group)} {
		var objects []runtime.Object
		if name != "" {
			objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}})
		}
		client := fake.NewSimpleClientset(objects...)
		store, err := NewAuthorityStore(client, ns)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckNodeWorkerShadowFence(ctx, group); (err != nil) != (name != "") {
			t.Fatalf("unexpected fence observation for %q: %v", name, err)
		}
		for _, action := range client.Actions() {
			if action.GetVerb() != "get" {
				t.Fatalf("shadow acquired or mutated the fence: %+v", action)
			}
		}
	}
}

func TestNodeWorkerResultRequiresARealHandoffAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	j := nodeWorkerFenceFixture(t)
	clock := j.CreatedAt.Add(2 * time.Second)
	activation := j.Expected
	activation.Generation++
	activation.PreviousSlot, activation.ActiveSlot = j.Expected.ActiveSlot, j.Candidate.Slot
	activation.BundleGeneration, activation.WorkerSourceCommit, activation.WorkerImageDigest = j.Candidate.BundleGeneration, j.Candidate.WorkerSourceCommit, j.Candidate.WorkerImageDigest
	activation.Operation, activation.Reason = edgegroupfront.ActivationOperationPromote, "advance node Worker using journal "+j.Digest
	activation.UpdatedAt = clock
	held := NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "old-connection", Slot: j.Expected.ActiveSlot, Target: "192.0.2.7:28443", StartedAt: j.Expected.UpdatedAt}
	fresh := NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "new-connection", Slot: j.Candidate.Slot, Target: "192.0.2.7:38443", StartedAt: clock.Add(time.Second)}
	arm := NodeWorkerArm{NodeWorkerArmSchema, j.Digest, held, j.CreatedAt, j.CreatedAt.Add(time.Minute)}
	w := NodeWorkerWitness{Schema: NodeWorkerWitnessSchema, JournalDigest: j.Digest, Held: held, Fresh: fresh, ObservedAt: clock.Add(2 * time.Second)}
	now := clock.Add(3 * time.Second)
	if err := arm.Validate(j, now); err != nil {
		t.Fatal(err)
	}
	if err := w.Validate(j, arm, activation, now); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*NodeWorkerWitness){
		func(w *NodeWorkerWitness) { w.Held.ID = "reopened" },
		func(w *NodeWorkerWitness) { w.Fresh.StartedAt = j.Expected.UpdatedAt },
		func(w *NodeWorkerWitness) { w.Fresh.Target = w.Held.Target },
		func(w *NodeWorkerWitness) { w.ObservedAt = arm.Deadline },
	} {
		other := w
		change(&other)
		if other.Validate(j, arm, activation, now) == nil {
			t.Fatal("invalid handoff witness was accepted")
		}
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID, object.ResourceVersion = types.UID("api-"+object.Name), "23"
		return false, nil, nil
	})
	store, _ := NewAuthorityStore(client, "fugue-system")
	f, err := store.ClaimNodeWorker(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordNodeWorkerResult(ctx, f, j.Expected, "verified", &w); err == nil {
		t.Fatal("positive receipt without a durable activation")
	}
	if err := store.RecordNodeWorkerResult(ctx, f, activation, "verified", nil); err == nil {
		t.Fatal("positive receipt without connection witness")
	}
	if err := store.RecordNodeWorkerResult(ctx, f, activation, "verified", &w); err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewAuthorityStore(client, "fugue-system")
	got, exists, err := restarted.ReadNodeWorkerResult(ctx, j)
	if err != nil || !exists || got.Outcome != "verified" || got.Activation != activation || got.Witness == nil || !got.Witness.Fresh.Same(fresh) {
		t.Fatalf("lost handoff result on restart: %+v %v", got, err)
	}
	if err := restarted.EnsureNodeWorkerProjection(ctx, f); err != nil {
		t.Fatal(err)
	}
	name := NodeWorkerProjectionName(j.Recipient.Group, j.Recipient.Node)
	configs := client.CoreV1().ConfigMaps("fugue-system")
	projection, err := configs.Get(ctx, name, metav1.GetOptions{})
	if err != nil || projection.Immutable != nil && *projection.Immutable || projection.Data["journal.json"] == "" {
		t.Fatalf("live Front could not reload a mutable projection: %+v %v", projection, err)
	}
	var failOnce = true
	client.PrependReactor("delete", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		del := action.(ktesting.DeleteAction)
		if del.GetName() != FrontRepairFenceName(j.Recipient.Group) {
			t.Fatalf("refusing to delete unexpected object: %s", del.GetName())
		}
		p := del.GetDeleteOptions().Preconditions
		if p == nil || *p.UID != f.UID || *p.ResourceVersion != f.RV {
			t.Fatal("terminal fence deletion was not bound to the exact object")
		}
		if failOnce {
			failOnce = false
			return true, nil, errors.New("fixture simulates a dropped delete request")
		}
		return false, nil, nil
	})
	if err := restarted.FinalizeNodeWorker(ctx, f); err == nil || !strings.Contains(err.Error(), "fixture simulates a dropped delete request") {
		t.Fatalf("retirement did not reach the expected fenced deletion: %v", err)
	}
	if mode, err := restarted.NodeWorkerMode(ctx, j.Recipient.Group); err != nil || !mode {
		t.Fatalf("legacy writers were not fenced before retirement: %t %v", mode, err)
	}
	indexObject, err := configs.Get(ctx, NodeWorkerIndexName(j.Recipient.Group), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("committed head must precede fence deletion: %v", err)
	}
	index, err := ReadNodeWorkerIndex(indexObject, j.Recipient.Group)
	if want := (NodeWorkerIndexHead{JournalDigest: j.Digest, Generation: activation.Generation}); err != nil || index.Nodes[j.NodeUID] != want {
		t.Fatalf("committed head does not bind the outcome: %+v %v", index, err)
	}
	if err := restarted.FinalizeNodeWorker(ctx, f); err != nil {
		t.Fatalf("retirement could not resume: %v", err)
	}
	if _, err := configs.Get(ctx, FrontRepairFenceName(j.Recipient.Group), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed transaction still owns the group: %v", err)
	}
	cleared, err := configs.Get(ctx, name, metav1.GetOptions{})
	if err != nil || cleared.UID != projection.UID || len(cleared.Data) != 0 {
		t.Fatalf("retirement did not preserve and empty the volume object: %+v %v", cleared, err)
	}
	if err := RequireNoFrontRepair(ctx, client, "fugue-system", j.Recipient.Group); !errors.Is(err, ErrNodeWorkerGroupMode) {
		t.Fatalf("legacy writer could touch the mixed group: %v", err)
	}
	if _, _, err := restarted.LoadTransitionJournal(ctx, j.Recipient.Group); !errors.Is(err, ErrNodeWorkerGroupMode) {
		t.Fatalf("legacy authority reconciler ignored node mode: %v", err)
	}
	if _, err := restarted.ClaimNodeWorker(ctx, j); err == nil {
		t.Fatal("terminal pilot claim was replayed")
	}
	other := j
	other.Recipient.Node = "other-node"
	other, err = other.Seal()
	if err != nil {
		t.Fatal(err)
	}
	g, err := restarted.ClaimNodeWorker(ctx, other)
	if err != nil {
		t.Fatalf("old terminal transaction prevented second node from resuming: %v", err)
	}
	if err := restarted.FinalizeNodeWorker(ctx, g); err == nil {
		t.Fatal("a new node's journal was retired without a result")
	}
	if err := restarted.EnsureNodeWorkerProjection(ctx, g); err != nil {
		t.Fatal(err)
	}
	if got, err := configs.Get(ctx, name, metav1.GetOptions{}); err != nil || len(got.Data) != 0 {
		t.Fatalf("second node touched first node's projection: %+v %v", got, err)
	}
}

func TestNodeWorkerUnarmedExpiryKeepsLegacyWritesAvailable(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID, object.ResourceVersion = types.UID("api-"+object.Name), "23"
		return false, nil, nil
	})
	const ns = "fugue-system"
	store, _ := NewAuthorityStore(client, ns)
	j := nodeWorkerFenceFixture(t)
	f, err := store.ClaimNodeWorker(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordNodeWorkerResult(ctx, f, j.Expected, "expired_without_cutover", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeNodeWorker(ctx, f); err != nil {
		t.Fatalf("unarmed claim could not retire without a mounted projection: %v", err)
	}
	if err := RequireNoFrontRepair(ctx, client, ns, j.Recipient.Group); err != nil {
		t.Fatalf("expiry with an unchanged predecessor disabled legacy writes: %v", err)
	}
}
