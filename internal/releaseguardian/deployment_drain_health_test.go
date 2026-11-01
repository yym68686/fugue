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

func TestDeploymentHealthSeparatesDrainingPredecessorWithoutHidingFailures(t *testing.T) {
	for _, scenario := range []string{"draining predecessor", "current revision", "foreign deployment", "replaced ReplicaSet", "missing ReplicaSet", "active predecessor", "not deleting", "missing current Pod", "unready current Pod", "incomplete rollout"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Unix(900, 0).UTC()
			image := "ghcr.io/example/service@" + testDigest
			target := declarativerelease.TargetIdentity{ConfigSHA: testSHA, ManifestSHA: testSHA, OCIRevision: testSHA, ImageRef: image}
			release := declarativerelease.PlanRelease{
				ComponentID: "service", Workload: declarativerelease.Workload{Kind: "Deployment", Namespace: "system", Name: "service", Container: "service"},
				Health: []declarativerelease.HealthProbe{{Type: "deployment", Name: "service"}},
			}
			one, zero := int32(1), int32(0)
			workload := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "system", UID: "deployment-uid", Generation: 2, Annotations: map[string]string{"fugue.pro/production-config-sha": testSHA}},
				Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "service"}}, Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "service"}, Annotations: map[string]string{"fugue.pro/oci-revision": testSHA}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "service", Image: image}}},
				}},
				Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
			}
			set := &appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{Name: "predecessor", Namespace: "system", UID: "predecessor-uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(workload, appsv1.SchemeGroupVersion.WithKind("Deployment"))}},
				Spec:       appsv1.ReplicaSetSpec{Replicas: &zero, Template: *workload.Spec.Template.DeepCopy()},
			}
			set.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "old-hash"
			set.Spec.Template.Spec.Containers[0].Image = "ghcr.io/example/service@" + otherDigest
			ready := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "current", Namespace: "system", UID: "current-uid", Labels: map[string]string{"app": "service"}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
					ContainerStatuses: []corev1.ContainerStatus{{Name: "service", ImageID: image, Ready: true}}},
			}
			draining := ready.DeepCopy()
			draining.Name, draining.UID = "draining", "draining-uid"
			deletion := metav1.NewTime(now)
			draining.DeletionTimestamp = &deletion
			draining.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(set, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
			draining.Status.Conditions[0].Status = corev1.ConditionFalse
			switch scenario {
			case "current revision":
				set.Spec.Template.Spec.Containers[0].Image = image
			case "foreign deployment":
				set.OwnerReferences[0].UID = "foreign-deployment"
			case "replaced ReplicaSet":
				set.UID = "replacement-uid"
			case "active predecessor":
				set.Spec.Replicas = &one
			case "not deleting":
				draining.DeletionTimestamp = nil
			case "unready current Pod":
				ready.Status.Conditions[0].Status = corev1.ConditionFalse
			case "incomplete rollout":
				workload.Status.UpdatedReplicas = 0
			}
			objects := []runtime.Object{workload, draining}
			if scenario != "missing ReplicaSet" {
				objects = append(objects, set)
			}
			if scenario != "missing current Pod" {
				objects = append(objects, ready)
			}
			client := fake.NewSimpleClientset(objects...)
			store := &KubeStore{client: client}
			health := store.localHealth(context.Background(), release, target, nil, now)
			evidence, err := store.oneWorkloadHealth(context.Background(), release, target, "deployment", "service")
			if scenario == "draining predecessor" {
				if health.State != HealthHealthy || err != nil || len(evidence.RetiredPods) != 1 || evidence.RetiredPods[0].ReplicaSetUID != "predecessor-uid" || evidence.RetiredPods[0].DeploymentUID != "deployment-uid" {
					t.Fatalf("normal drain was not attributed: health=%+v evidence=%+v err=%v", health, evidence, err)
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
