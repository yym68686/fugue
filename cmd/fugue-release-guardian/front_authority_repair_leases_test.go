package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stest "k8s.io/client-go/testing"
)

func frontRepairLeaseFixtures(namespace, group string) (declarativerelease.PlanRelease, *coordinationv1.Lease, *coordinationv1.Lease) {
	const component = "edge-worker-fixture"
	release := declarativerelease.PlanRelease{ComponentID: component, Concurrency: "fugue-production-" + component,
		Workload: declarativerelease.Workload{Namespace: namespace}, Transition: &declarativerelease.Transition{
			Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: group}}}
	newLease := func(name, label, value string, duration int32) *coordinationv1.Lease {
		return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(name + "-uid"), ResourceVersion: "41",
			Labels: map[string]string{label: value}}, Spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &duration}}
	}
	return release, newLease(release.Concurrency, "fugue.pro/component", component, frontRepairComponentLeaseSeconds),
		newLease("fugue-authority-"+group, "fugue.pro/group", group, authorityLeaseSeconds)
}

func trackFrontRepairLeaseCAS(t *testing.T, client *fake.Clientset, namespace string) {
	t.Helper()
	client.PrependReactor("update", "leases", func(action k8stest.Action) (bool, runtime.Object, error) {
		value := action.(k8stest.UpdateAction).GetObject().(*coordinationv1.Lease).DeepCopy()
		previous, err := client.Tracker().Get(action.GetResource(), namespace, value.Name)
		if err != nil {
			return true, nil, err
		}
		lease := previous.(*coordinationv1.Lease)
		if value.UID != lease.UID || value.ResourceVersion != lease.ResourceVersion {
			return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), value.Name, errors.New("fixture CAS failed"))
		}
		value.ResourceVersion += "x"
		if err := client.Tracker().Update(action.GetResource(), value, namespace); err != nil {
			return true, nil, err
		}
		return true, value, nil
	})
}

func TestFrontRepairLeasePairRejectsBusyOrReplacedLease(t *testing.T) {
	const ns, group = "fixture-system", "edge-group-fixture"
	ctx := context.Background()
	release, component, authority := frontRepairLeaseFixtures(ns, group)
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	stamp := metav1.NewMicroTime(now)
	authority.Spec.HolderIdentity, authority.Spec.RenewTime = stringPtr("ordinary-promotion"), &stamp
	client := fake.NewSimpleClientset(component, authority)
	trackFrontRepairLeaseCAS(t, client, ns)
	if pair, err := acquireFrontRepairLeases(ctx, client, release, group, clock); err == nil || pair != nil {
		t.Fatal("repair acquired a live ordinary authority lock")
	}
	leases := client.CoordinationV1().Leases(ns)
	check := func(name, holder string) *coordinationv1.Lease {
		t.Helper()
		value, err := leases.Get(ctx, name, metav1.GetOptions{})
		if err != nil || valueOrEmpty(value.Spec.HolderIdentity) != holder {
			t.Fatalf("Lease %s holder is not %q: %v", name, holder, err)
		}
		return value
	}
	check(component.Name, "") // failed second claim must unwind the first
	check(authority.Name, "ordinary-promotion")
	now = now.Add(3 * time.Minute)
	pair, err := acquireFrontRepairLeases(ctx, client, release, group, clock)
	if err != nil || pair.requireHeld(ctx, group) != nil {
		t.Fatalf("expired Lease pair could not be taken: %v", err)
	}
	if err := pair.requireHeld(ctx, "different-group"); err == nil {
		t.Fatal("repair lock could authorize a different Edge group")
	}
	now = now.Add(106 * time.Second)
	if err := pair.requireHeld(ctx, group); err == nil {
		t.Fatal("allowed operation without enough time left on the group Lease")
	}
	firstHolder := pair.holder
	now = now.Add(30 * time.Second)
	value := check(authority.Name, firstHolder)
	value.Spec.HolderIdentity, value.Spec.RenewTime = stringPtr("new-owner"), &metav1.MicroTime{Time: now}
	if _, err := leases.Update(ctx, value, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := pair.release(ctx); err == nil {
		t.Fatal("repair did not report lost authority ownership")
	}
	check(authority.Name, "new-owner")
	check(component.Name, "")
}

func TestFrontRepairLeasePairRecoversLostAcquisitionReplyWithoutReplay(t *testing.T) {
	const ns, group = "fixture-system", "edge-group-fixture"
	ctx := context.Background()
	release, component, authority := frontRepairLeaseFixtures(ns, group)
	client := fake.NewSimpleClientset(component, authority)
	trackFrontRepairLeaseCAS(t, client, ns)
	claims := 0
	client.PrependReactor("update", "leases", func(action k8stest.Action) (bool, runtime.Object, error) {
		value := action.(k8stest.UpdateAction).GetObject().(*coordinationv1.Lease).DeepCopy()
		if value.Name != authority.Name || valueOrEmpty(value.Spec.HolderIdentity) == "" {
			return false, nil, nil
		}
		claims++
		if value.UID != authority.UID || value.ResourceVersion != authority.ResourceVersion {
			t.Fatal("unexpected authority Lease acquisition replay")
		}
		value.ResourceVersion += "x"
		if err := client.Tracker().Update(action.GetResource(), value, ns); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("fixture network failure after Lease update")
	})
	pair, err := acquireFrontRepairLeases(ctx, client, release, group, time.Now)
	if err != nil || claims != 1 {
		t.Fatalf("could not recognize a lost Lease receipt without replay: writes=%d err=%v", claims, err)
	}
	if err := pair.release(ctx); err != nil {
		t.Fatal(err)
	}
}
