package releaseguardian

import (
	"context"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestNodeWorkerGrantRequiresVerifiedProbeAndKeepsDeadlineAcrossRestart(t *testing.T) {
	ctx := context.Background()
	j := nodeWorkerFenceFixture(t)
	j.ProbeServiceUID = "00000000-0000-4000-8000-000000000006"
	j, err := j.Seal()
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.UID, cm.ResourceVersion = types.UID("api-"+cm.Name), "1"
		return false, nil, nil
	})
	store, _ := NewAuthorityStore(client, "test-system")
	f, err := store.ClaimNodeWorker(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	start := j.CreatedAt.Add(3 * time.Second)
	if _, err := store.ProjectNodeWorkerDrain(ctx, f, start); err == nil {
		t.Fatal("projected an authorization without the cutover checkpoint")
	}
	active := j.Expected
	active.Generation++
	active.PreviousSlot, active.ActiveSlot = active.ActiveSlot, j.Candidate.Slot
	active.BundleGeneration = j.Candidate.BundleGeneration
	active.WorkerSourceCommit, active.WorkerImageDigest = j.Candidate.WorkerSourceCommit, j.Candidate.WorkerImageDigest
	active.UpdatedAt, active.Operation, active.Reason = j.CreatedAt, edgegroupfront.ActivationOperationPromote, "advance node Worker using journal "+j.Digest
	old := NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "old", Slot: j.Expected.ActiveSlot, Target: "192.0.2.7:28443", StartedAt: j.Expected.UpdatedAt}
	new := NodeWorkerConnection{FrontUID: j.Recipient.PodUID, ID: "new", Slot: active.ActiveSlot, Target: "192.0.2.7:38443", StartedAt: j.CreatedAt.Add(time.Second)}
	w := NodeWorkerWitness{Schema: NodeWorkerWitnessSchema, JournalDigest: j.Digest, Held: old, Fresh: new, ObservedAt: start.Add(-time.Second)}
	for _, invalid := range []*NodeWorkerConnection{nil, &new} {
		w.ProbeFresh = invalid
		if err := store.RecordNodeWorkerDrainStart(ctx, f, active, w, start); err == nil {
			t.Fatal("accepted missing or reused probe connection")
		}
	}
	p := new
	p.ID = "probe"
	w.ProbeFresh = &p
	if err := store.RecordNodeWorkerDrainStart(ctx, f, active, w, start); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordNodeWorkerDrainStart(ctx, f, active, w, start.Add(time.Second)); err == nil {
		t.Fatal("cutover start was rewritten")
	}
	first, err := store.ProjectNodeWorkerDrain(ctx, f, start)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewAuthorityStore(client, "test-system")
	now := start.Add(time.Minute)
	fresh, err := restarted.ProjectNodeWorkerDrain(ctx, f, now)
	if err != nil || fresh.Start != first.Start || fresh.Deadline != first.Deadline || fresh.HealthyAt != now ||
		first.Previous != j.Expected || first.Serving != active || !first.Deadline.Equal(start.Add(10*time.Minute)) {
		t.Fatalf("wrong grant after restart: first=%+v refreshed=%+v err=%v", first, fresh, err)
	}
	if _, err := restarted.ProjectNodeWorkerDrain(ctx, f, start); err == nil {
		t.Fatal("older health overwrote a fresh witness")
	}
}
