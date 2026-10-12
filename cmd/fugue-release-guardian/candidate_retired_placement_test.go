package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/releaseguardian"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCandidateCanaryRetiredPlacementRequiresExactCurrentMembership(t *testing.T) {
	for _, scenario := range []string{"retired", "required", "foreign owner", "no owner", "missing node", "unidentified node", "missing member", "stale controller", "changed selector", "empty selector", "wrong group", "workload deleting"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := releaseguardian.CandidateAuthority{GroupID: "edge-pool-a", WorkerSlot: releaseguardian.AuthoritySlotB, BundleGeneration: "candidate-generation-1"}
			image := "ghcr.io/example/worker@sha256:" + strings.Repeat("c", 64)
			active := candidateWorkerPod("worker-current", "current-node", "current-pod-uid", "b", strings.Repeat("d", 40), image)
			workload := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "fugue-system", UID: "workload-uid", ResourceVersion: "22", Generation: 3},
				Spec:   appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: active.Labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: active.Labels}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"placement": "enabled"}}}},
				Status: appsv1.DaemonSetStatus{ObservedGeneration: 3, DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberReady: 1}}
			active.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(workload, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}
			retired := active.DeepCopy()
			retired.Name, retired.UID, retired.Spec.NodeName = "worker-retained", "retired-pod-uid", "retired-node"
			deletion := metav1.NewTime(time.Now().Add(-time.Hour))
			retired.DeletionTimestamp = &deletion
			retired.Status.ContainerStatuses[0].Ready = false
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "retired-node", UID: "retired-node-uid", ResourceVersion: "25", Labels: map[string]string{"placement": "disabled"}}}
			switch scenario {
			case "required":
				node.Labels["placement"] = "enabled"
			case "foreign owner":
				retired.OwnerReferences[0].UID = "replaced-workload"
			case "no owner":
				retired.OwnerReferences = nil
			case "unidentified node":
				node.ResourceVersion = ""
			case "stale controller":
				workload.Status.ObservedGeneration = 2
			case "changed selector":
				workload.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"other": "worker"}}
			case "empty selector":
				workload.Spec.Template.Spec.NodeSelector = nil
			case "wrong group":
				workload.Spec.Template.Labels = map[string]string{"fugue.io/edge-group-id": "edge-pool-other", "fugue.io/edge-slot": "b"}
			case "workload deleting":
				workload.DeletionTimestamp = &deletion
			}
			objects := []runtime.Object{workload, retired}
			if scenario != "missing member" {
				objects = append(objects, active)
			}
			if scenario != "missing node" {
				objects = append(objects, node)
			}
			client := fake.NewSimpleClientset(objects...)
			cohort, err := observeCandidateWorkerCohort(context.Background(), client, "fugue-system", candidate)
			if scenario == "retired" {
				if err != nil || len(cohort.Instances) != 1 || cohort.Instances[0].PodUID != string(active.UID) {
					t.Fatalf("exact retired placement blocked required cohort: %+v %v", cohort, err)
				}
			} else if err == nil {
				t.Fatalf("unproven placement was omitted: %+v", cohort)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("observation mutated cluster: %v", action)
				}
			}
		})
	}
}
