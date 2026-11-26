package releaseguardian

import (
	"context"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFixedNodeWorkerIsolatedDaemonSetScope(t *testing.T) {
	j := edgegroupfront.NodeWorkerJournal{
		FixedTransport: true, FixedRotation: true,
		Recipient: edgegroupfront.NativeRecipient{Group: "edge-group-test", Node: "node-a"},
		Candidate: edgegroupfront.ActivationRepairRelease{Slot: "a"},
	}
	r := declarativerelease.PlanRelease{ComponentID: "edge-worker-fixture",
		Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "worker-a",
			Namespace: "test", Container: "edge", RolloutMode: "on-delete"}}
	if !fixedWorkerReleaseScope(r, j, "worker-a", "test", true) || fixedWorkerReleaseScope(r, j, "different-worker", "test", true) ||
		fixedWorkerReleaseScope(r, j, "worker-a", "foreign", true) {
		t.Fatal("isolated release was not bound to its exact controller")
	}
	legacy := j
	legacy.FixedRotation = false
	if fixedWorkerReleaseScope(r, legacy, "worker-a", "test", true) {
		t.Fatal("legacy fixed Front accepted an independently rotated release")
	}
	group := r
	group.Transition = &declarativerelease.Transition{Type: "edge-group-ab",
		EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: j.Recipient.Group}}
	if !fixedWorkerReleaseScope(group, j, "worker-a", "test", false) ||
		fixedWorkerReleaseScope(group, j, "worker-a", "test", true) {
		t.Fatal("a different component must not use a group receipt to claim a fixed Worker")
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"fugue.io/edge-node": j.Recipient.Node,
			"fugue.io/edge-group-id": j.Recipient.Group, "fugue.io/edge-slot": "a"}},
		Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/hostname": j.Recipient.Node},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{
					{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{j.Recipient.Node}},
				}}},
			}}}},
	}
	if !fixedWorkerNodeTemplate(template, j) {
		t.Fatal("pinned node template rejected")
	}
	for _, change := range []func(*corev1.PodTemplateSpec){
		func(p *corev1.PodTemplateSpec) { p.Spec.Affinity = nil },
		func(p *corev1.PodTemplateSpec) { p.Spec.HostNetwork = true },
		func(p *corev1.PodTemplateSpec) { p.Spec.NodeSelector["kubernetes.io/hostname"] = "node-b" },
		func(p *corev1.PodTemplateSpec) { p.Labels["fugue.io/edge-slot"] = "b" },
		func(p *corev1.PodTemplateSpec) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values = []string{"node-a", "node-b"}
		},
	} {
		copy := template.DeepCopy()
		change(copy)
		if fixedWorkerNodeTemplate(*copy, j) {
			t.Fatal("drifted isolated node template accepted")
		}
	}
}

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
