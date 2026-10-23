package main

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHeldWorkerFinalObservationUsesSameFrozenMembersAsActivation(t *testing.T) {
	group, revision, image := "edge-pool-a", strings.Repeat("a", 40), "ghcr.io/example/worker@sha256:"+strings.Repeat("b", 64)
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "worker-b", Namespace: "system", Container: "edge"}, Transition: &declarativerelease.Transition{Type: "edge-group-ab", EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{GroupID: group, WorkerAName: "worker-a", WorkerBName: "worker-b", FrontName: "front", ExpectedNodes: 1}}}
	w := appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "system", UID: "ds-uid", ResourceVersion: "20", Generation: 2, Annotations: map[string]string{"fugue.pro/production-config-sha": revision}},
		Spec:   appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"fugue.io/edge-group-id": group}, Annotations: map[string]string{"fugue.pro/source-commit": revision, "fugue.pro/oci-revision": revision}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "edge", Image: image}}}}},
		Status: appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 1, NumberReady: 2, NumberAvailable: 2}}
	good := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-good", Namespace: "system", UID: "good-uid", Annotations: w.Spec.Template.Annotations, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&w, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}}, Spec: corev1.PodSpec{NodeName: "healthy"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", ImageID: image, Ready: true}}}}
	old := *good.DeepCopy()
	old.Name, old.UID, old.Spec.NodeName = "worker-held", "held-uid", "limited"
	old.Annotations = map[string]string{"fugue.pro/source-commit": strings.Repeat("c", 40)}
	workload, _ := json.Marshal(w)
	pods, _ := json.Marshal(corev1.PodList{Items: []corev1.Pod{good, old}})
	// Existing generic final observation still counts the retained old member.
	if _, err := parseObservation(workload, pods, release); err == nil {
		t.Fatal("fixture does not reproduce the mixed retained slot")
	}
	cluster := &kubectlCluster{edgeCohorts: map[string]edgecohort.Snapshot{group: {Group: group, Expected: 1, Suspended: map[string]bool{"limited": true}}}}
	filtered, count, err := cluster.edgeObservationMembers(release, workload, pods)
	if err != nil || count == nil || *count != 1 {
		t.Fatal(count, err)
	}
	result, err := parseObservationWithCohort(workload, filtered, release, count)
	if err != nil || result.Desired != 1 || result.Ready != 1 || result.Updated != 1 {
		t.Fatal("final observation still waits for a held process", result, err)
	}
	for _, scenario := range []string{"missing", "duplicate", "wrong-owner", "stale-node", "unready"} {
		p := *good.DeepCopy()
		values := []corev1.Pod{p, old}
		switch scenario {
		case "missing":
			values = values[1:]
		case "duplicate":
			duplicate := *good.DeepCopy()
			duplicate.Name, duplicate.UID = "duplicate", "duplicate-uid"
			values = append(values, duplicate)
		case "wrong-owner":
			values[0].OwnerReferences[0].UID = "other"
		case "stale-node":
			values[0].Spec.NodeName = ""
		case "unready":
			values[0].Status.Conditions[0].Status = corev1.ConditionFalse
		}
		raw, _ := json.Marshal(corev1.PodList{Items: values})
		filtered, count, err := cluster.edgeObservationMembers(release, workload, raw)
		if err == nil {
			_, err = parseObservationWithCohort(workload, filtered, release, count)
		}
		if err == nil {
			t.Fatal("unproven member accepted", scenario)
		}
	}
}
