package releaseguardian

import (
	"context"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestNodeWorkerComponentLeaseExcludesAnAlreadyRunningGroupWriter(t *testing.T) {
	ctx := context.Background()
	j := nodeWorkerFenceFixture(t)
	now := j.CreatedAt
	component := NodeWorkerComponent{"fugue-system", "edge-worker-test", "fugue-production-edge-worker-test"}
	const oldHolder = "guardian:old-pod:0123456789abcdef:edge-worker-test"
	l := NewComponentLease(component.Namespace, component.LeaseName, component.Component, oldHolder, now)
	l.UID, l.ResourceVersion = "lease-uid", "23"
	client := fake.NewSimpleClientset(l)
	client.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease)
		switch object.ResourceVersion {
		case "23":
			object.ResourceVersion = "24"
		case "24":
			object.ResourceVersion = "25"
		default:
			t.Fatalf("unexpected lease update version: %s", object.ResourceVersion)
		}
		return false, nil, nil
	})
	if _, err := AcquireNodeWorkerComponent(ctx, client, component, "pilot-pod", j.Digest, now.Add(time.Minute)); err == nil {
		t.Fatal("node writer took an ordinary release's live Lease")
	}
	if got := len(client.Actions()); got != 1 || client.Actions()[0].GetVerb() != "get" {
		t.Fatalf("busy node writer should not touch the Lease: %+v", client.Actions())
	}
	now = now.Add(ComponentLeaseDurationSeconds * time.Second)
	held, err := AcquireNodeWorkerComponent(ctx, client, component, "pilot-pod", j.Digest, now)
	if err != nil {
		t.Fatalf("exact expired Lease was not recoverable: %v", err)
	}
	if _, err := AcquireNodeWorkerComponent(ctx, client, component, "other-pod", j.Digest, now.Add(time.Second)); err == nil {
		t.Fatal("a second node writer entered while the first held the Lease")
	}
	if err := held.Release(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	l, err = client.CoordinationV1().Leases(component.Namespace).Get(ctx, component.LeaseName, metav1.GetOptions{})
	holder, _, _, parseErr := ReadComponentLease(l)
	if err != nil || parseErr != nil || holder != "" || l.ResourceVersion != "25" {
		t.Fatalf("legacy group release could not acquire the now-empty component Lease: %q %v %v", holder, err, parseErr)
	}
	if err := held.Release(ctx, now.Add(2*time.Second)); err == nil {
		t.Fatal("a stale holder was allowed to release its prior Lease")
	}
}

func TestNodeWorkerComponentLeaseFailsClosedOnUnknownOwnership(t *testing.T) {
	j := nodeWorkerFenceFixture(t)
	component := NodeWorkerComponent{"fugue-system", "edge-worker-test", "fugue-production-edge-worker-test"}
	for _, scenario := range []struct {
		name   string
		mutate func(*coordinationv1.Lease)
	}{
		{"no renew time", func(l *coordinationv1.Lease) { l.Spec.RenewTime = nil }},
		{"zero renew time", func(l *coordinationv1.Lease) { l.Spec.RenewTime = &metav1.MicroTime{} }},
		{"foreign component", func(l *coordinationv1.Lease) { l.Labels["fugue.pro/component"] = "other" }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			lease := NewComponentLease(component.Namespace, component.LeaseName, component.Component, "old-holder", j.CreatedAt)
			lease.UID, lease.ResourceVersion = "lease-uid", "23"
			scenario.mutate(lease)
			client := fake.NewSimpleClientset(lease)
			if _, err := AcquireNodeWorkerComponent(context.Background(), client, component, "pilot-pod", j.Digest, j.CreatedAt.Add(time.Hour)); err == nil {
				t.Fatal("acquired a Lease with unproven ownership")
			}
		})
	}
}
