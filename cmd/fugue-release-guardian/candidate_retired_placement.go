package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

func requiredCandidateWorkerPods(ctx context.Context, client kubernetes.Interface, namespace string, candidate releaseguardian.CandidateAuthority, pods []corev1.Pod) ([]corev1.Pod, error) {
	// The canary and activation must observe the same explicit membership.
	// A slow held node retains its local state and cannot reject a healthy
	// candidate merely because it has an older image or an unready process.
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"fugue.io/edge-group-id": candidate.GroupID}.AsSelector().String(), Limit: 1001})
	if err != nil || nodes.Continue != "" || len(nodes.Items) > 1000 {
		return nil, errors.New("candidate release hold inventory is incomplete")
	}
	held := map[string]corev1.Node{}
	for _, node := range nodes.Items {
		isHeld, err := edgecohort.ReleaseHeld(node, candidate.GroupID)
		if err != nil {
			return nil, err
		}
		if isHeld {
			if _, duplicate := held[node.Name]; duplicate {
				return nil, errors.New("candidate release hold identity is ambiguous")
			}
			held[node.Name] = node
		}
	}
	eligible := make([]corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if _, excluded := held[pod.Spec.NodeName]; !excluded {
			eligible = append(eligible, pod)
		}
	}
	pods = eligible
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
		desired := int(workload.Status.DesiredNumberScheduled)
		placement := labels.SelectorFromSet(workload.Spec.Template.Spec.NodeSelector)
		if len(workload.Spec.Template.Spec.NodeSelector) > 0 {
			for _, node := range held {
				if placement.Matches(labels.Set(node.Labels)) {
					desired--
				}
			}
		}
		retained, evidence, err := releaseguardian.FilterRetiredDaemonSetPods(ctx, client, owned, workload)
		if err != nil || desired < 1 || len(evidence) == 0 || len(retained) != desired {
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
