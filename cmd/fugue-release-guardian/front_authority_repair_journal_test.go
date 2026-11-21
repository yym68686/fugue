package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stest "k8s.io/client-go/testing"
)

func TestFrontRepairJournalResumesPartialDiskWritesAndLostPointerReply(t *testing.T) {
	ctx := context.Background()
	before, cohort, witnesses := partialFrontRollbackFixture()
	paths := map[string]string{}
	now := time.Now().UTC()
	for i, w := range witnesses {
		path := filepath.Join(t.TempDir(), "activation.json")
		paths[w.Node.Name] = path
		init := edgegroupfront.ActivationCASRequest{GroupID: before.GroupID, InitialGeneration: before.PreviousFrontGeneration,
			ExpectedSlot: string(before.PreviousWorkerSlot), TargetSlot: string(before.PreviousWorkerSlot),
			BundleGeneration: before.PreviousBundleGeneration, WorkerSourceCommit: before.PreviousWorkerSourceSHA, WorkerImageDigest: before.PreviousWorkerImageDigest,
			Operation: edgegroupfront.ActivationOperationInit, Reason: "initialize fixture group member"}
		if _, err := edgegroupfront.ApplyActivationCAS(path, init, now.Add(-3*time.Second)); err != nil {
			t.Fatal(err)
		}
		promote := edgegroupfront.ActivationCASRequest{GroupID: before.GroupID, ExpectedGeneration: before.PreviousFrontGeneration,
			ExpectedSlot: string(before.PreviousWorkerSlot), TargetSlot: string(before.CurrentWorkerSlot), BundleGeneration: before.CurrentBundleGeneration,
			WorkerSourceCommit: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest,
			Operation: edgegroupfront.ActivationOperationPromote, Reason: "apply fixture committed member"}
		receipt, err := edgegroupfront.ApplyActivationCAS(path, promote, now.Add(-2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if w.Activation.Operation == edgegroupfront.ActivationOperationRollback {
			rollback := init
			rollback.InitialGeneration, rollback.ExpectedGeneration = 0, before.CurrentFrontGeneration
			rollback.ExpectedSlot, rollback.BundleGeneration = string(before.CurrentWorkerSlot), before.CurrentBundleGeneration
			rollback.Operation, rollback.RollbackOfGeneration, rollback.Reason = edgegroupfront.ActivationOperationRollback, before.CurrentFrontGeneration, "fixture partial rollback before repair"
			receipt, err = edgegroupfront.ApplyActivationCAS(path, rollback, now.Add(-time.Second))
			if err != nil {
				t.Fatal(err)
			}
		}
		witnesses[i].Activation, witnesses[i].LiveFront = receipt.Current, liveFrontRepairFixture(receipt.Current)
		witnesses[i].ObservedAt = now
	}
	plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, witnesses)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := sealFrontRepairJournal(plan, "sha256:"+strings.Repeat("d", 64), now)
	if err != nil {
		t.Fatal(err)
	}
	const namespace = "fixture-system"
	pointerName := "fugue-current-authority-" + before.GroupID
	payload, err := declarativerelease.CanonicalJSON(before)
	if err != nil {
		t.Fatal(err)
	}
	release, componentLease, groupLease := frontRepairLeaseFixtures(namespace, before.GroupID)
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: pointerName, UID: "pointer-uid", ResourceVersion: "101",
		Labels: map[string]string{"fugue.pro/group": before.GroupID, "fugue.pro/authority-store": "true"}}, Data: map[string]string{"authority.json": string(payload)}},
		componentLease, groupLease)
	trackFrontRepairLeaseCAS(t, client, namespace)
	fenceName := releaseguardian.FrontRepairFenceName(before.GroupID)
	client.PrependReactor("create", "configmaps", func(action k8stest.Action) (bool, runtime.Object, error) {
		object := action.(k8stest.CreateAction).GetObject().(*corev1.ConfigMap)
		if object.Name == fenceName {
			object.UID, object.ResourceVersion = "prepared-fence-uid", "41"
		}
		return false, nil, nil
	})
	pointerWrites := 0
	client.PrependReactor("update", "configmaps", func(action k8stest.Action) (bool, runtime.Object, error) {
		object := action.(k8stest.UpdateAction).GetObject().(*corev1.ConfigMap)
		if object.Name != pointerName {
			return false, nil, nil
		}
		pointerWrites++
		if object.ResourceVersion != "101" {
			t.Fatalf("CAS lost exact pointer resourceVersion: %s", object.ResourceVersion)
		}
		object = object.DeepCopy()
		object.ResourceVersion = "102"
		if err := client.Tracker().Update(action.GetResource(), object, namespace); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("simulated reply loss after pointer write")
	})
	var locks *frontRepairLeasePair
	acquire := func() {
		t.Helper()
		var err error
		locks, err = acquireFrontRepairLeases(ctx, client, release, before.GroupID, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	newStore := func() frontRepairStore {
		t.Helper()
		store, err := newFrontRepairStore(locks)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	if err := (frontRepairStore{}).create(ctx, journal); err == nil {
		t.Fatal("created a journal without locks")
	}
	if _, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, fenceName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unauthorized journal create left state: %v", err)
	}
	acquire()
	store := newStore()
	if err := store.create(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if err := store.create(ctx, journal); err != nil {
		t.Fatalf("creation was not idempotent: %v", err)
	}
	if _, _, err := store.authority.LoadTransitionJournal(ctx, before.GroupID); !errors.Is(err, releaseguardian.ErrFrontRepairInProgress) {
		t.Fatalf("ordinary reconciler did not see the durable repair fence: %v", err)
	}
	ordinary, err := newFrontAuthorityActivator(client, activationFixtureExecutor{}, frontAuthorityConfig{Namespace: namespace, GroupID: before.GroupID, ExpectedNodes: len(cohort)}, "ordinary-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if err := locks.release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.BeginRestore(ctx, before); !errors.Is(err, releaseguardian.ErrFrontRepairInProgress) {
		t.Fatalf("rollback could proceed with a repair fence after the lease was reacquired: %v", err)
	}
	acquire()
	store = newStore()
	other, err := sealFrontRepairJournal(plan, "sha256:"+strings.Repeat("e", 64), now)
	if err != nil || store.create(ctx, other) == nil {
		t.Fatalf("another authorization replaced the existing journal: err=%v", err)
	}
	if _, err := store.writePlan(ctx, before.GroupID, other.Digest, witnesses, nil); err == nil {
		t.Fatal("another authorization could resume the durable journal")
	}
	initial, err := journal.resume(witnesses, now)
	if err != nil || len(initial.Remaining) != 3 || initial.Converged {
		t.Fatalf("unstarted repair misclassified: %+v, err=%v", initial, err)
	}
	prepareRequests, err := frontRepairPublicationRequests(plan)
	if err != nil || len(prepareRequests) != 1 {
		t.Fatalf("unexpected prepare requests: %+v, err=%v", prepareRequests, err)
	}
	preparations := make(map[string]edgegroupfront.PublicationPreparation)
	if _, err := store.writePlan(ctx, before.GroupID, journal.Digest, witnesses, preparations); err == nil {
		t.Fatal("journal authorized traffic before missing preparation")
	}
	for node, request := range prepareRequests {
		p, err := edgegroupfront.PreparePublication(paths[node], request.request, now)
		if err != nil {
			t.Fatal(err)
		}
		preparations[node] = p
	}
	initial, err = store.writePlan(ctx, before.GroupID, journal.Digest, witnesses, preparations)
	if err != nil || len(initial.Remaining) != 3 {
		t.Fatalf("exact preparation did not authorize node requests: %+v, err=%v", initial, err)
	}
	apply := func(node string, request edgegroupfront.ActivationRepairCASRequest) {
		t.Helper()
		if _, err := edgegroupfront.ApplyActivationRepairCAS(paths[node], request, time.Now().Add(10*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	observe := func() []frontRepairWitness {
		t.Helper()
		result := append([]frontRepairWitness(nil), witnesses...)
		for i := range result {
			state, exists, err := edgegroupfront.ReadActivationState(paths[result[i].Node.Name])
			if err != nil || !exists {
				t.Fatalf("fixture member evidence is absent: error=%v", err)
			}
			result[i].Activation, result[i].LiveFront = state, liveFrontRepairFixture(state)
			result[i].ObservedAt = time.Now().UTC()
		}
		return result
	}
	first := cohort[1].Name
	apply(first, initial.Remaining[first]) // discard receipt as if the process exited
	restarted := newStore()
	loaded, err := restarted.load(ctx, before.GroupID)
	if err != nil || loaded.Digest != journal.Digest {
		t.Fatalf("durable journal lost on restart: error=%v", err)
	}
	partial := observe()
	remaining, err := restarted.writePlan(ctx, before.GroupID, loaded.Digest, partial, preparations)
	if err != nil || len(remaining.Remaining) != 2 || remaining.Remaining[first].JournalDigest != "" || remaining.Converged {
		t.Fatalf("partial repair rescheduled a committed member: %+v, err=%v", remaining, err)
	}
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, partial); err == nil || pointerWrites != 0 {
		t.Fatalf("partial recovery advanced pointer: writes=%d, error=%v", pointerWrites, err)
	}
	if err := restarted.finish(ctx, journal, partial); err == nil {
		t.Fatal("partial recovery released the fence")
	}
	for node, request := range remaining.Remaining {
		apply(node, request)
	}
	final := observe()
	unhealthy := append([]frontRepairWitness(nil), final...)
	unhealthy[0].TargetWorkerHealth.Status = "degraded"
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, unhealthy); err == nil || pointerWrites != 0 {
		t.Fatalf("unhealthy cohort advanced pointer: writes=%d, error=%v", pointerWrites, err)
	}
	replaced := append([]frontRepairWitness(nil), final...)
	replaced[0].FrontPodUID = "replacement-front-uid"
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, replaced); err == nil || pointerWrites != 0 {
		t.Fatalf("replacement Pod advanced pointer: writes=%d, error=%v", pointerWrites, err)
	}
	if err := locks.release(ctx); err != nil {
		t.Fatal(err)
	}
	acquire()
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, final); err == nil || pointerWrites != 0 {
		t.Fatalf("superseded locks did not prevent pointer commit: writes=%d, error=%v", pointerWrites, err)
	}
	restarted = newStore()
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, final); err != nil {
		t.Fatalf("cannot reconcile lost update reply: %v", err)
	}
	if err := newStore().commit(ctx, before.GroupID, journal.Digest, final); err != nil || pointerWrites != 1 {
		t.Fatalf("resume replayed CurrentAuthority CAS: writes=%d, error=%v", pointerWrites, err)
	}
	got, _, _, err := restarted.authority.LoadCurrent(ctx, before.GroupID)
	want := before
	want.CurrentFrontGeneration, want.AuthorityEpoch = plan.TargetGeneration, before.AuthorityEpoch+1
	if err != nil || got != want {
		t.Fatalf("repair changed other authority fields: got=%+v, want=%+v, error=%v", got, want, err)
	}
	fenceDeletes := 0
	client.PrependReactor("delete", "configmaps", func(action k8stest.Action) (bool, runtime.Object, error) {
		deleting := action.(k8stest.DeleteAction)
		if deleting.GetName() != fenceName {
			return false, nil, nil
		}
		fenceDeletes++
		p := deleting.GetDeleteOptions().Preconditions
		if p == nil || p.UID == nil || *p.UID != "prepared-fence-uid" || p.ResourceVersion == nil || *p.ResourceVersion != "41" {
			t.Fatalf("fence release without exact UID/resourceVersion: %+v", p)
		}
		if err := client.Tracker().Delete(action.GetResource(), namespace, fenceName); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("simulated reply loss after fence removal")
	})
	if err := restarted.finish(ctx, journal, final); err != nil {
		t.Fatalf("converged recovery could not release its exact fence: %v", err)
	}
	if err := restarted.finish(ctx, journal, nil); err != nil || fenceDeletes != 1 {
		t.Fatalf("completion was not recoverable without replay: deletes=%d err=%v", fenceDeletes, err)
	}
	archived, err := restarted.loadCompletion(ctx, before.GroupID, journal.AuthorizationDigest)
	if err != nil || archived.Digest != journal.Digest {
		t.Fatalf("could not recover by approved intent after cleanup: %v", err)
	}
	if _, err := restarted.loadCompletion(ctx, before.GroupID, other.AuthorizationDigest); !apierrors.IsNotFound(err) {
		t.Fatalf("unrelated approval reused completion receipt: %v", err)
	}
	if err := restarted.create(ctx, journal); err == nil || !strings.Contains(err.Error(), "already has a completion archive") {
		t.Fatalf("completed approval could start again: %v", err)
	}
	if err := locks.release(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := ordinary.acquireOrdinaryAuthorityLease(ctx)
	if err != nil {
		t.Fatalf("finished repair still blocks normal operations: %v", err)
	}
	if err := lease.release(ctx); err != nil {
		t.Fatal(err)
	}
}
