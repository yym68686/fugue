package releaseguardian

import (
	"context"
	"errors"
	"strings"

	"fugue/internal/declarativerelease"
	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// A held node still belongs to its DaemonSet and retains its running process.
// Aggregate DaemonSet counters therefore cannot describe the explicitly smaller
// release cohort. When holds exist, verify every required physical Pod instead.
// This is code health only: it grants no DNS, route or traffic authority.
func (store *KubeStore) heldEdgeWorkloadHealth(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, workload *appsv1.DaemonSet) (workloadHealthEvidence, bool, error) {
	var evidence workloadHealthEvidence
	if workload == nil || release.Transition == nil || release.Transition.Type != "edge-group-ab" || release.Transition.EdgeGroupAB == nil {
		return evidence, false, nil
	}
	transition := release.Transition.EdgeGroupAB
	nodes, err := store.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"fugue.io/edge-group-id": transition.GroupID}.AsSelector().String(), Limit: 1001})
	if err != nil || nodes.Continue != "" || len(nodes.Items) > 1000 {
		return evidence, true, errors.New("Edge health membership inventory is incomplete")
	}
	holds := 0
	heldUIDs := map[string]string{}
	for _, n := range nodes.Items {
		held, err := edgecohort.ReleaseHeld(n, transition.GroupID)
		if err != nil {
			return evidence, true, err
		}
		if held {
			holds++
			heldUIDs[n.Name] = string(n.UID)
		}
	}
	if holds == 0 {
		return evidence, false, nil
	}
	cohort, err := edgecohort.FromNodes(nodes.Items, transition.GroupID, transition.ExpectedNodes)
	if err != nil {
		return evidence, true, err
	}
	if workload.UID == "" || workload.ResourceVersion == "" || workload.DeletionTimestamp != nil || workload.Status.ObservedGeneration != workload.Generation || workload.Spec.Template.Labels["fugue.io/edge-group-id"] != transition.GroupID {
		return evidence, true, errors.New("held Edge health workload identity is incomplete")
	}
	selector, err := podTemplateLabelSelector(workload.Spec.Template.Labels)
	if err != nil {
		return evidence, true, err
	}
	pods, err := store.client.CoreV1().Pods(workload.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: 1001})
	if err != nil || pods.Continue != "" || len(pods.Items) > 1000 {
		return evidence, true, errors.New("held Edge health Pod inventory is incomplete")
	}
	active, retired, err := store.workloadHealthPods(ctx, cohort.FilterPods(pods.Items), workload)
	if err != nil || len(active) != cohort.Expected {
		return evidence, true, errors.New("held Edge health does not cover every required member")
	}
	known := map[string]string{}
	for _, n := range nodes.Items {
		if n.UID != "" && n.ResourceVersion != "" && n.DeletionTimestamp == nil && n.Labels["fugue.io/role.edge"] == "true" {
			known[n.Name] = string(n.UID)
		}
	}
	seen := map[string]bool{}
	requiredUIDs := map[string]string{}
	containers := artifactTargetContainers(release, "DaemonSet", workload.Name)
	if len(containers) == 0 {
		return evidence, true, errors.New("held Edge health has no immutable target")
	}
	for _, p := range active {
		if known[p.Spec.NodeName] == "" || seen[p.Spec.NodeName] || p.UID == "" || p.DeletionTimestamp != nil || !metav1.IsControlledBy(&p, workload) || !podReady(p) || p.Annotations["fugue.pro/oci-revision"] != target.OCIRevision {
			return evidence, true, errors.New("required Edge Pod identity or readiness differs")
		}
		seen[p.Spec.NodeName] = true
		requiredUIDs[p.Spec.NodeName] = known[p.Spec.NodeName]
		for name, kind := range containers {
			specs, statuses := p.Spec.Containers, p.Status.ContainerStatuses
			if kind == "init-container" {
				specs, statuses = p.Spec.InitContainers, p.Status.InitContainerStatuses
			}
			if containerImageMatches(specs, name, target.ImageRef) != nil || !heldEdgeContainerMatches(statuses, name, target.ImageRef, kind == "init-container") {
				return evidence, true, errors.New("required Edge process differs from immutable code target")
			}
		}
	}
	count := int32(cohort.Expected)
	evidence = workloadHealthEvidence{Kind: "DaemonSet", Name: workload.Name, Desired: count, Updated: count, Ready: count, Available: count, Generation: workload.Generation, RetiredPods: retired, RequiredNodeUIDs: requiredUIDs, ReleaseHoldNodeUIDs: heldUIDs}
	return evidence, true, nil
}

func heldEdgeContainerMatches(statuses []corev1.ContainerStatus, name, image string, init bool) bool {
	_, expected, found := strings.Cut(image, "@")
	if !found || !strings.HasPrefix(expected, "sha256:") || len(expected) != 71 {
		return false
	}
	for _, status := range statuses {
		if status.Name != name {
			continue
		}
		_, actual, found := strings.Cut(status.ImageID, "@")
		if !found || actual != expected {
			return false
		}
		if init {
			return status.State.Terminated != nil && status.State.Terminated.ExitCode == 0
		}
		return status.Ready && status.State.Running != nil
	}
	return false
}
