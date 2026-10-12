package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

func requiredCandidateWorkerPods(ctx context.Context, client kubernetes.Interface, namespace string, candidate releaseguardian.CandidateAuthority, pods []corev1.Pod) ([]corev1.Pod, error) {
	checked := map[types.UID]bool{}
	retired := map[types.UID]bool{}
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			continue
		}
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "DaemonSet" || owner.UID == "" {
			return nil, errors.New("candidate retiring Pod lacks exact DaemonSet ownership")
		}
		if checked[owner.UID] {
			continue
		}
		if len(checked) >= 32 {
			return nil, errors.New("candidate retiring placement exceeds bounded workload reads")
		}
		checked[owner.UID] = true
		workload, err := client.AppsV1().DaemonSets(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil || workload.UID != owner.UID || workload.ResourceVersion == "" || workload.DeletionTimestamp != nil ||
			workload.Status.ObservedGeneration < workload.Generation || workload.Status.DesiredNumberScheduled < 1 ||
			workload.Spec.Template.Labels["fugue.io/edge-group-id"] != candidate.GroupID || workload.Spec.Template.Labels["fugue.io/edge-slot"] != string(candidate.WorkerSlot) {
			return nil, errors.New("candidate retiring placement lacks current matching workload")
		}
		selector, err := metav1.LabelSelectorAsSelector(workload.Spec.Selector)
		if err != nil || selector.Empty() {
			return nil, errors.New("candidate retiring workload has no exact Pod selector")
		}
		owned := []corev1.Pod{}
		for _, observed := range pods {
			if metav1.IsControlledBy(&observed, workload) {
				if !selector.Matches(labels.Set(observed.Labels)) {
					return nil, errors.New("candidate Pod differs from its current workload selector")
				}
				owned = append(owned, observed)
			}
		}
		retained, evidence, err := releaseguardian.FilterRetiredDaemonSetPods(ctx, client, owned, workload)
		if err != nil || len(evidence) == 0 || len(retained) != int(workload.Status.DesiredNumberScheduled) {
			return nil, errors.New("candidate retiring placement does not preserve every required member")
		}
		for _, item := range evidence {
			retired[types.UID(item.PodUID)] = true
		}
		encoded, err := json.Marshal(map[string]any{"observed_at": time.Now().UTC(), "group": candidate.GroupID, "candidate_record_digest": candidate.RecordDigest, "workload_uid": workload.UID, "workload_resource_version": workload.ResourceVersion, "desired": workload.Status.DesiredNumberScheduled, "retired": evidence})
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "candidate placement evidence: %s\n", encoded)
	}
	retained := make([]corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if !retired[pod.UID] {
			retained = append(retained, pod)
		}
	}
	return retained, nil
}
