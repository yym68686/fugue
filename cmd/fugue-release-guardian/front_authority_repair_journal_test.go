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
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: pointerName, UID: "pointer-uid", ResourceVersion: "101",
		Labels: map[string]string{"fugue.pro/group": before.GroupID, "fugue.pro/authority-store": "true"}}, Data: map[string]string{"authority.json": string(payload)}})
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
	locksHeld := false
	newStore := func() frontRepairStore {
		store, err := releaseguardian.NewAuthorityStore(client, namespace)
		if err != nil {
			t.Fatal(err)
		}
		return frontRepairStore{client: client, namespace: namespace, authority: store, held: func(context.Context) error {
			if !locksHeld {
				return errors.New("fixture lease pair is not held")
			}
			return nil
		}}
	}
	store := newStore()
	if err := store.create(ctx, journal); err == nil {
		t.Fatal("created a journal without locks")
	}
	if _, err := store.load(ctx, before.GroupID); !apierrors.IsNotFound(err) {
		t.Fatalf("unauthorized journal create left state: %v", err)
	}
	locksHeld = true
	if err := store.create(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if err := store.create(ctx, journal); err != nil {
		t.Fatalf("creation was not idempotent: %v", err)
	}
	other, err := sealFrontRepairJournal(plan, "sha256:"+strings.Repeat("e", 64), now)
	if err != nil || store.create(ctx, other) == nil {
		t.Fatalf("another authorization replaced the existing journal: err=%v", err)
	}
	if _, err := store.writePlan(ctx, before.GroupID, other.Digest, witnesses, nil, now); err == nil {
		t.Fatal("another authorization could resume the durable journal")
	}
	initial, err := journal.resume(witnesses)
	if err != nil || len(initial.Remaining) != 3 || initial.Converged {
		t.Fatalf("unstarted repair misclassified: %+v, err=%v", initial, err)
	}
	prepareRequests, err := frontRepairPublicationRequests(plan)
	if err != nil || len(prepareRequests) != 1 {
		t.Fatalf("unexpected prepare requests: %+v, err=%v", prepareRequests, err)
	}
	preparations := make(map[string]edgegroupfront.PublicationPreparation)
	if _, err := store.writePlan(ctx, before.GroupID, journal.Digest, witnesses, preparations, now); err == nil {
		t.Fatal("journal authorized traffic before missing preparation")
	}
	for node, request := range prepareRequests {
		p, err := edgegroupfront.PreparePublication(paths[node], request.request, now)
		if err != nil {
			t.Fatal(err)
		}
		preparations[node] = p
	}
	initial, err = store.writePlan(ctx, before.GroupID, journal.Digest, witnesses, preparations, now)
	if err != nil || len(initial.Remaining) != 3 {
		t.Fatalf("exact preparation did not authorize node requests: %+v, err=%v", initial, err)
	}
	apply := func(node string, request edgegroupfront.ActivationRepairCASRequest) {
		t.Helper()
		if _, err := edgegroupfront.ApplyActivationRepairCAS(paths[node], request); err != nil {
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
	remaining, err := restarted.writePlan(ctx, before.GroupID, loaded.Digest, partial, preparations, time.Now())
	if err != nil || len(remaining.Remaining) != 2 || remaining.Remaining[first].JournalDigest != "" || remaining.Converged {
		t.Fatalf("partial repair rescheduled a committed member: %+v, err=%v", remaining, err)
	}
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, partial); err == nil || pointerWrites != 0 {
		t.Fatalf("partial recovery advanced pointer: writes=%d, error=%v", pointerWrites, err)
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
	locksHeld = false
	if err := restarted.commit(ctx, before.GroupID, journal.Digest, final); err == nil || pointerWrites != 0 {
		t.Fatalf("lost locks did not prevent pointer commit: writes=%d, error=%v", pointerWrites, err)
	}
	locksHeld = true
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
}
