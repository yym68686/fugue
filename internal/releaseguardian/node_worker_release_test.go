package releaseguardian

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFixedNodeWorkerRevisionAcceptsNumericHashWithExactOwnership(t *testing.T) {
	controller := true
	owner := metav1.OwnerReference{Kind: "DaemonSet", Name: "fixture-worker-a", UID: "worker-controller-uid", Controller: &controller}
	const hash = "57b9c47f56"
	revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: owner.Name + "-" + hash, Namespace: "fixture-system", UID: "revision-uid",
		ResourceVersion: "42", OwnerReferences: []metav1.OwnerReference{owner},
	}}
	client := fake.NewSimpleClientset(revision)
	store, err := NewAuthorityStore(client, revision.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	got, err := store.readFixedNodeWorkerRevision(ctx, hash, &owner)
	if err != nil || got.UID != revision.UID {
		t.Fatalf("did not resolve the exact digit-leading DaemonSet revision: %v", err)
	}
	for _, invalid := range []string{"", "../old-worker", "57B9C47F56"} {
		if _, err := store.readFixedNodeWorkerRevision(ctx, invalid, &owner); err == nil {
			t.Fatalf("invalid revision hash accepted: %q", invalid)
		}
	}
	if len(client.Actions()) != 1 {
		t.Fatalf("invalid hashes reached the Kubernetes client: %v", client.Actions())
	}
	owner.UID = "replacement-controller-uid"
	if _, err := store.readFixedNodeWorkerRevision(ctx, hash, &owner); err == nil {
		t.Fatal("revision from a different DaemonSet incarnation accepted")
	}
}
