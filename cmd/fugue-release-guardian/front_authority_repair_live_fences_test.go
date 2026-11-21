package main

import (
	"context"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFrontRepairRequiresActualFencedServingControlPlane(t *testing.T) {
	source := strings.Repeat("a", 40)
	rel := frontRepairFenceRelease{SourceCommit: source, ImageDigest: "sha256:" + strings.Repeat("b", 64), PlatformDigest: "sha256:" + strings.Repeat("c", 64)}
	policy := frontRepairReleasePolicy{ExecutorCommit: source, APIFence: rel, GuardianFence: rel}
	fixture := func(id string) (declarativerelease.Component, *appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Pod) {
		const ns = "fixture"
		const container = "main"
		repo := "registry.example.net/fixture/" + id
		image := repo + "@" + rel.ImageDigest
		uid, rsUID := types.UID(id+"-deployment"), types.UID(id+"-rs")
		labels := map[string]string{"component": id}
		annotations := map[string]string{"fugue.pro/source-commit": source, "fugue.pro/oci-revision": source}
		podSpec := corev1.PodSpec{Containers: []corev1.Container{{Name: container, Image: image}}}
		template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}, Spec: podSpec}
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: ns, UID: uid, Generation: 2, ResourceVersion: "7"},
			Spec:   appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}, Replicas: int32Ptr(1), Template: template},
			Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, AvailableReplicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1}}
		rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: id + "-replicas", Namespace: ns, UID: rsUID,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, appsv1.SchemeGroupVersion.WithKind("Deployment"))}},
			Spec: appsv1.ReplicaSetSpec{Template: template}}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: id + "-pod", Namespace: ns, UID: types.UID(id + "-pod"), Labels: labels, Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}}, Spec: podSpec,
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
				ContainerStatuses: []corev1.ContainerStatus{{Name: container, Ready: true, ImageID: repo + "@" + rel.PlatformDigest,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
		return declarativerelease.Component{ID: id, Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: ns, Name: id, Container: container},
			Artifact: declarativerelease.Artifact{Repository: repo}}, d.DeepCopy(), rs.DeepCopy(), pod.DeepCopy()
	}
	a, aD, aRS, aPod := fixture("api")
	for _, tc := range []struct {
		name   string
		mutate func(*appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Pod) []runtime.Object
		ok     bool
	}{
		{"live verified platform image", nil, true},
		{"new image built but old Deployment", func(d *appsv1.Deployment, _ *appsv1.ReplicaSet, _ *corev1.Pod) []runtime.Object {
			d.Spec.Template.Annotations["fugue.pro/source-commit"] = strings.Repeat("d", 40)
			return nil
		}, false},
		{"rolled back running digest", func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, p *corev1.Pod) []runtime.Object {
			p.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("d", 64)
			return nil
		}, false},
		{"unobserved generation", func(d *appsv1.Deployment, _ *appsv1.ReplicaSet, _ *corev1.Pod) []runtime.Object {
			d.Status.ObservedGeneration--
			return nil
		}, false},
		{"unready Pod", func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, p *corev1.Pod) []runtime.Object {
			p.Status.ContainerStatuses[0].Ready = false
			return nil
		}, false},
		{"Pod owner reused", func(_ *appsv1.Deployment, rs *appsv1.ReplicaSet, _ *corev1.Pod) []runtime.Object {
			rs.UID = "another"
			return nil
		}, false},
		{"ReplicaSet for a different Deployment", func(_ *appsv1.Deployment, rs *appsv1.ReplicaSet, _ *corev1.Pod) []runtime.Object {
			rs.OwnerReferences[0].UID = "another"
			return nil
		}, false},
		{"not all still selected Pods are proved", func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, p *corev1.Pod) []runtime.Object {
			old := p.DeepCopy()
			old.Name = "another"
			old.UID = "another"
			return []runtime.Object{old}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, d, rs, pod := fixture("release-guardian")
			objects := []runtime.Object{aD.DeepCopy(), aRS.DeepCopy(), aPod.DeepCopy(), d, rs, pod}
			if tc.mutate != nil {
				objects = append(objects, tc.mutate(d, rs, pod)...)
			}
			client := fake.NewSimpleClientset(objects...)
			err := requireLiveFrontRepairFences(context.Background(), client, declarativerelease.Registry{Components: []declarativerelease.Component{a, g}}, policy)
			if (err == nil) != tc.ok {
				t.Fatalf("fence acceptance: %v, want %v", err, tc.ok)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("live prerequisite check mutated cluster: %v", action)
				}
			}
		})
	}
	rollback := policy
	rollback.GuardianFence.SourceCommit = strings.Repeat("d", 40)
	if err := rollback.requireFenceRevisions(); err == nil {
		t.Fatal("a different running Guardian may not qualify the executor")
	}
}
