package main

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func participationFixture() (appsv1.DaemonSet, corev1.PodList, corev1.NodeList, declarativerelease.PlanRelease) {
	revision := strings.Repeat("a", 40)
	image := "registry.example.test/cache@sha256:" + strings.Repeat("b", 64)
	ds := appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "system", UID: "workload-uid", Generation: 2, ResourceVersion: "12", Annotations: map[string]string{"fugue.pro/production-config-sha": revision}},
		Spec:   appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"fugue.pro/source-commit": revision, "fugue.pro/oci-revision": revision}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "cache", Image: image}}}}},
		Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, ObservedGeneration: 2, UpdatedNumberScheduled: 1, NumberReady: 1, NumberAvailable: 1}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "current", UID: "pod-uid", Annotations: ds.Spec.Template.Annotations, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&ds, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}}, Spec: corev1.PodSpec{NodeName: "online"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "cache", ImageID: image, Ready: true}}}}
	old := *pod.DeepCopy()
	old.Name, old.UID, old.Spec.NodeName = "retained", "old-pod-uid", "intermittent"
	old.Annotations = map[string]string{"fugue.pro/source-commit": strings.Repeat("c", 40)}
	old.Status.Conditions[0].Status = corev1.ConditionFalse
	nodes := corev1.NodeList{Items: []corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "online", UID: "online-uid"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "intermittent", UID: "intermittent-uid"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}}},
	}}
	r := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Kind: "DaemonSet", Name: "cache", Namespace: "system", Container: "cache", NodeParticipation: "reachable"}}
	return ds, corev1.PodList{Items: []corev1.Pod{pod, old}}, nodes, r
}

func TestNodeParticipationFreezesOfflineIdentityAcrossForwardRollbackAndRecovery(t *testing.T) {
	ds, pods, nodes, release := participationFixture()
	frozen, err := captureNodeParticipation(ds, pods, nodes)
	if err != nil || len(frozen.Required) != 1 || frozen.Deferred["intermittent"] != "intermittent-uid" {
		t.Fatal(frozen, err)
	}
	// Round-trip the receipt across prepare and execute process boundaries.
	raw, _ := json.Marshal(frozen)
	var restored declarativerelease.NodeParticipation
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"forward", "rollback", "returned-during-transaction"} {
		if stage == "returned-during-transaction" {
			nodes.Items[1].Status.Conditions[0].Status = corev1.ConditionTrue
		}
		filtered, err := filterNodeParticipation(restored, ds, pods, nodes)
		if err != nil {
			t.Fatal(stage, err)
		}
		w, _ := json.Marshal(ds)
		p, _ := json.Marshal(filtered)
		count := int32(len(restored.Required))
		observation, err := parseObservationWithCohort(w, p, release, &count)
		if err != nil || observation.Ready != 1 || observation.Desired != 1 {
			t.Fatal(stage, observation, err)
		}
	}
	// A new operation requires the returning node and its exact current code.
	next, err := captureNodeParticipation(ds, pods, nodes)
	if err != nil || len(next.Required) != 2 {
		t.Fatal(next, err)
	}
	filtered, _ := filterNodeParticipation(next, ds, pods, nodes)
	w, _ := json.Marshal(ds)
	p, _ := json.Marshal(filtered)
	count := int32(2)
	if _, err := parseObservationWithCohort(w, p, release, &count); err == nil {
		t.Fatal("returning stale process was reported healthy")
	}
}

func TestNodeParticipationNeverConvertsNewFailureIntoAnotherDeferredNode(t *testing.T) {
	ds, pods, nodes, release := participationFixture()
	frozen, err := captureNodeParticipation(ds, pods, nodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"new-outage", "stale-pod-ready", "new-node-uid", "foreign-owner", "missing-pod", "duplicate-pod", "missing-node", "changed-count", "partial-list"} {
		t.Run(scenario, func(t *testing.T) {
			d, p, n := *ds.DeepCopy(), *pods.DeepCopy(), *nodes.DeepCopy()
			switch scenario {
			case "new-outage":
				n.Items[0].Status.Conditions[0].Status = corev1.ConditionUnknown
				p.Items[0].Status.Conditions[0].Status = corev1.ConditionFalse
			case "stale-pod-ready":
				n.Items[0].Status.Conditions[0].Status = corev1.ConditionUnknown
			case "new-node-uid":
				n.Items[0].UID = "replacement"
			case "foreign-owner":
				p.Items[0].OwnerReferences[0].UID = "foreign"
			case "missing-pod":
				p.Items = p.Items[1:]
			case "duplicate-pod":
				p.Items = append(p.Items, *p.Items[0].DeepCopy())
			case "missing-node":
				n.Items = n.Items[:1]
			case "changed-count":
				d.Status.DesiredNumberScheduled++
			case "partial-list":
				n.Continue = "next"
			}
			filtered, err := filterNodeParticipation(frozen, d, p, n)
			if err == nil {
				w, _ := json.Marshal(d)
				raw, _ := json.Marshal(filtered)
				count := int32(1)
				_, err = parseObservationWithCohort(w, raw, release, &count)
			}
			if err == nil {
				t.Fatal("unproven membership accepted")
			}
		})
	}
	if _, err := captureNodeParticipation(ds, corev1.PodList{Items: pods.Items[:1]}, nodes); err == nil {
		t.Fatal("incomplete initial inventory accepted")
	}
	nodes.Items[0].Status.Conditions[0].Status = corev1.ConditionUnknown
	if _, err := captureNodeParticipation(ds, pods, nodes); err == nil {
		t.Fatal("empty live cohort accepted")
	}
}
