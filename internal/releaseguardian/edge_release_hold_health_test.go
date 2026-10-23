package releaseguardian

import (
	"context"
	"encoding/json"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHeldEdgeHealthChecksRequiredPodsInsteadOfWholeDaemonSetCounters(t *testing.T) {
	for _, scenario := range []string{"held", "no-hold", "missing-pod", "duplicate-node", "wrong-image", "wrong-runtime-image", "unready", "foreign-owner", "old-revision", "node-replaced", "controller-stale", "held-terminating"} {
		t.Run(scenario, func(t *testing.T) {
			group, image := "edge-pool-a", "ghcr.io/example/worker@"+testDigest
			target := declarativerelease.TargetIdentity{ConfigSHA: testSHA, ManifestSHA: testSHA, OCIRevision: testSHA, ImageRef: image}
			release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Kind: "DaemonSet", Name: "worker", Namespace: "system", Container: "edge"},
				Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: group, ExpectedNodes: 2}}}
			w := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "system", UID: "worker-uid", ResourceVersion: "10", Generation: 2, Annotations: map[string]string{"fugue.pro/production-config-sha": testSHA}},
				Spec:   appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker", "fugue.io/edge-group-id": group}, Annotations: map[string]string{"fugue.pro/oci-revision": testSHA}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "edge", Image: image}}}}},
				Status: appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1}}
			n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "healthy", UID: "healthy-uid", ResourceVersion: "20", Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/role.edge": "true"}}}
			held := n.DeepCopy()
			held.Name, held.UID = "held", "held-uid"
			raw, _ := json.Marshal(edgecohort.ReleaseHold{Schema: edgecohort.ReleaseHoldSchema, NodeUID: string(held.UID), EdgeGroupID: group, Reason: "quota_exhausted"})
			held.Annotations = map[string]string{edgecohort.ReleaseHoldAnnotation: string(raw)}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "healthy-pod", UID: "pod-uid", Namespace: "system", Labels: w.Spec.Template.Labels, Annotations: w.Spec.Template.Annotations, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(w, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}},
				Spec:   corev1.PodSpec{NodeName: n.Name, Containers: []corev1.Container{{Name: "edge", Image: image}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", ImageID: image, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
			old := pod.DeepCopy()
			old.Name, old.UID, old.Spec.NodeName = "held-pod", "held-pod-uid", held.Name
			old.Status.Conditions[0].Status = corev1.ConditionFalse
			old.Spec.Containers[0].Image = "old"
			objects := []runtime.Object{w, n, held, old}
			switch scenario {
			case "no-hold":
				held.Annotations = nil
			case "duplicate-node":
				duplicate := pod.DeepCopy()
				duplicate.Name, duplicate.UID = "extra", "extra-uid"
				objects = append(objects, duplicate)
			case "wrong-image":
				pod.Spec.Containers[0].Image = "old"
			case "wrong-runtime-image":
				pod.Status.ContainerStatuses[0].ImageID = "old"
			case "unready":
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
			case "foreign-owner":
				pod.OwnerReferences[0].UID = "other"
			case "old-revision":
				pod.Annotations = map[string]string{"fugue.pro/oci-revision": "old"}
			case "node-replaced":
				held.UID = "replacement"
			case "controller-stale":
				w.Status.ObservedGeneration = 1
			case "held-terminating":
				stamp := metav1.Now()
				old.DeletionTimestamp = &stamp
			}
			if scenario != "missing-pod" {
				objects = append(objects, pod)
			}
			client := fake.NewSimpleClientset(objects...)
			store := &KubeStore{client: client}
			evidence, handled, err := store.heldEdgeWorkloadHealth(context.Background(), release, target, w)
			if scenario == "held" || scenario == "held-terminating" {
				if err != nil || !handled || evidence.Desired != 1 || evidence.Ready != 1 {
					t.Fatal(evidence, handled, err)
				}
			} else if scenario == "no-hold" {
				if handled || err != nil {
					t.Fatal("legacy health changed without explicit intent", handled, err)
				}
			} else if err == nil {
				t.Fatal("unproven member health accepted")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("health mutated retained workload")
				}
			}
		})
	}
}
