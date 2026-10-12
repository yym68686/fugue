package releaseguardian

import (
	"context"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDaemonSetHealthSeparatesRetiredPlacementWithoutIgnoringRequiredPods(t *testing.T) {
	for _, scenario := range []string{"retired", "required", "not deleting", "foreign owner", "missing node", "unidentified node", "missing required pod", "incomplete rollout", "no selector"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Unix(800, 0).UTC()
			image := "ghcr.io/example/worker@" + testDigest
			target := declarativerelease.TargetIdentity{ConfigSHA: testSHA, ManifestSHA: testSHA, OCIRevision: testSHA, ImageRef: image}
			release := declarativerelease.PlanRelease{
				ComponentID: "worker", Workload: declarativerelease.Workload{Kind: "DaemonSet", Namespace: "system", Name: "worker", Container: "worker"},
				ArtifactTargets: []declarativerelease.ArtifactTarget{{Kind: "DaemonSet", Namespace: "system", Name: "worker", Container: "worker", ContainerType: "container"}},
				Health:          []declarativerelease.HealthProbe{{Type: "daemonset", Name: "worker"}},
			}
			workload := &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "system", UID: "workload-uid", Generation: 1, Annotations: map[string]string{"fugue.pro/production-config-sha": testSHA}},
				Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}}, Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}, Annotations: map[string]string{"fugue.pro/oci-revision": testSHA}},
					Spec:       corev1.PodSpec{NodeSelector: map[string]string{"pool": "enabled"}, Containers: []corev1.Container{{Name: "worker", Image: image}}},
				}},
				Status: appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 1, CurrentNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1},
			}
			ready := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "system", UID: "ready-uid", Labels: map[string]string{"app": "worker"}},
				Spec: corev1.PodSpec{NodeName: "current-node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "worker", ImageID: image, Ready: true}}}}
			terminating := ready.DeepCopy()
			terminating.Name, terminating.UID, terminating.Spec.NodeName = "retiring", "retiring-uid", "retiring-node"
			deletion := metav1.NewTime(now.Add(-time.Hour))
			terminating.DeletionTimestamp = &deletion
			terminating.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(workload, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}
			terminating.Status.Conditions[0].Status = corev1.ConditionFalse
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "retiring-node", UID: "node-uid", ResourceVersion: "20", Labels: map[string]string{"pool": "disabled"}}}
			switch scenario {
			case "required":
				node.Labels["pool"] = "enabled"
			case "not deleting":
				terminating.DeletionTimestamp = nil
			case "foreign owner":
				terminating.OwnerReferences[0].UID = "other-workload"
			case "unidentified node":
				node.ResourceVersion = ""
			case "incomplete rollout":
				workload.Status.NumberReady = 0
			case "no selector":
				workload.Spec.Template.Spec.NodeSelector = nil
			}
			objects := []runtime.Object{workload, terminating}
			if scenario != "missing required pod" {
				objects = append(objects, ready)
			}
			if scenario != "missing node" {
				objects = append(objects, node)
			}
			client := fake.NewSimpleClientset(objects...)
			store := &KubeStore{client: client}
			health := store.localHealth(context.Background(), release, target, nil, now)
			evidence, err := store.oneWorkloadHealth(context.Background(), release, target, "daemonset", "worker")
			if scenario == "retired" {
				if health.State != HealthHealthy || err != nil || len(evidence.RetiredPods) != 1 || evidence.RetiredPods[0].PodUID != "retiring-uid" || evidence.RetiredPods[0].NodeResourceVersion != "20" {
					t.Fatalf("retired placement was not independently attributed: health=%+v evidence=%+v err=%v", health, evidence, err)
				}
			} else if health.State != HealthDegraded || err == nil {
				t.Fatalf("required health constraint was bypassed: health=%+v err=%v", health, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("health inspection modified cluster: %v", action)
				}
			}
		})
	}
}
