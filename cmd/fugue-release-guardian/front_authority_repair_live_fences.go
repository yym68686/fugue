package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"fugue/internal/declarativerelease"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const frontRepairControlRegistry = "deploy/releases/components.json"

type frontRepairFenceRelease struct {
	SourceCommit   string `json:"sourceCommit"`
	ImageDigest    string `json:"imageDigest"`
	PlatformDigest string `json:"platformDigest"`
}

func (r frontRepairFenceRelease) valid() bool {
	return exactSourceSHA(r.SourceCommit) && exactSHA256Digest(r.ImageDigest) && exactSHA256Digest(r.PlatformDigest)
}

func (policy frontRepairReleasePolicy) requireFenceRevisions() error {
	if !policy.APIFence.valid() || !policy.GuardianFence.valid() || policy.GuardianFence.SourceCommit != policy.ExecutorCommit {
		return errors.New("Front repair requires independently pinned API and Guardian fencing releases")
	}
	return nil
}

func loadFrontRepairControlRegistry() (declarativerelease.Registry, error) {
	f, err := os.Open(frontRepairControlRegistry)
	if err != nil {
		return declarativerelease.Registry{}, err
	}
	defer f.Close()
	return declarativerelease.DecodeRegistry(f)
}

// No workload or Pod is patched. Resolve exact component identities from the
// executor's immutable registry, not from a repair approval or Pod labels alone.
func requireLiveFrontRepairFences(ctx context.Context, client kubernetes.Interface, registry declarativerelease.Registry, policy frontRepairReleasePolicy) error {
	if err := policy.requireFenceRevisions(); err != nil {
		return err
	}
	if client == nil {
		return errors.New("Front repair fencing observation has no cluster reader")
	}
	for _, required := range []struct {
		id      string
		release frontRepairFenceRelease
	}{
		{"api", policy.APIFence}, {"release-guardian", policy.GuardianFence},
	} {
		found := 0
		for _, component := range registry.Components {
			if component.ID != required.id {
				continue
			}
			found++
			if err := observeFrontRepairFence(ctx, client, component, required.release); err != nil {
				return fmt.Errorf("Front repair %s fence not proven: %w", required.id, err)
			}
		}
		if found != 1 {
			return fmt.Errorf("Front repair requires exactly one declared %s fence", required.id)
		}
	}
	return nil
}

func observeFrontRepairFence(ctx context.Context, client kubernetes.Interface, component declarativerelease.Component, release frontRepairFenceRelease) error {
	workload := component.Workload
	if workload.Kind != "Deployment" || workload.APIVersion != "apps/v1" || workload.Namespace == "" ||
		workload.Name == "" || workload.Container == "" || component.Artifact.Repository == "" {
		return errors.New("invalid declared control-plane workload")
	}
	deployments := client.AppsV1().Deployments(workload.Namespace)
	current, err := deployments.Get(ctx, workload.Name, metav1.GetOptions{})
	if err != nil {
		return errors.New("control-plane deployment is unreadable")
	}
	desired := current.Spec.Replicas
	image := component.Artifact.Repository + "@" + release.ImageDigest
	selector, err := metav1.LabelSelectorAsSelector(current.Spec.Selector)
	if err != nil || selector.Empty() || current.UID == "" || current.ResourceVersion == "" ||
		current.DeletionTimestamp != nil || current.Generation < 1 || current.Status.ObservedGeneration != current.Generation ||
		desired == nil || *desired < 1 || *desired > 100 || current.Status.ReadyReplicas != *desired ||
		current.Status.UpdatedReplicas != *desired || current.Status.AvailableReplicas != *desired || current.Status.Replicas != *desired ||
		!frontRepairFenceAnnotations(current.Spec.Template.Annotations, release.SourceCommit) ||
		!frontRepairFenceContainerImage(current.Spec.Template.Spec.Containers, workload.Container, image) {
		return errors.New("control-plane deployment has not converged to the pinned immutable release")
	}
	options := metav1.ListOptions{LabelSelector: selector.String(), Limit: 101}
	pods, err := client.CoreV1().Pods(workload.Namespace).List(ctx, options)
	if err != nil || pods.Continue != "" || len(pods.Items) > int(*desired) {
		return errors.New("control-plane serving Pod set is unreadable, oversized, or includes old replicas")
	}
	replicaSets := client.AppsV1().ReplicaSets(workload.Namespace)
	owners := make(map[string]appsv1.ReplicaSet)
	seen := make(map[string]bool)
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if pod.UID == "" || seen[string(pod.UID)] || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning ||
			!frontRepairFencePodReady(pod, workload.Container, image, release) || owner == nil ||
			owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.UID == "" {
			return errors.New("a selected live Pod is not a ready, pinned control-plane replica")
		}
		seen[string(pod.UID)] = true
		rs, known := owners[owner.Name]
		if !known {
			got, err := replicaSets.Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil || got.DeletionTimestamp != nil {
				return errors.New("control-plane replica owner is unreadable or terminating")
			}
			rs = *got
			owners[owner.Name] = rs
		}
		parent := metav1.GetControllerOf(&rs)
		if rs.UID != owner.UID || parent == nil || parent.APIVersion != "apps/v1" || parent.Kind != "Deployment" ||
			parent.Name != current.Name || parent.UID != current.UID ||
			!frontRepairFenceAnnotations(rs.Spec.Template.Annotations, release.SourceCommit) ||
			!frontRepairFenceContainerImage(rs.Spec.Template.Spec.Containers, workload.Container, image) {
			return errors.New("control-plane Pod ownership is not tied to the pinned Deployment revision")
		}
	}
	if len(seen) != int(*desired) {
		return errors.New("control-plane release has missing live Pod coverage")
	}
	again, err := deployments.Get(ctx, workload.Name, metav1.GetOptions{})
	if err != nil || again.UID != current.UID || again.ResourceVersion != current.ResourceVersion {
		return errors.New("control-plane fence changed during observation")
	}
	return nil
}

func frontRepairFenceAnnotations(a map[string]string, source string) bool {
	return a["fugue.pro/source-commit"] == source && a["fugue.pro/oci-revision"] == source
}

func frontRepairFenceContainerImage(containers []corev1.Container, name, image string) bool {
	n := 0
	for _, c := range containers {
		if c.Name == name {
			if c.Image != image {
				return false
			}
			n++
		}
	}
	return n == 1
}

func frontRepairFencePodReady(pod *corev1.Pod, name, image string, release frontRepairFenceRelease) bool {
	if !frontRepairFenceAnnotations(pod.Annotations, release.SourceCommit) || !frontRepairFenceContainerImage(pod.Spec.Containers, name, image) {
		return false
	}
	podReady := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			podReady = true
		}
	}
	n := 0
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != name {
			continue
		}
		matches := func(digest string) bool {
			return status.ImageID == digest || strings.HasSuffix(status.ImageID, "@"+digest) || strings.HasSuffix(status.ImageID, "://"+digest)
		}
		if !status.Ready || status.State.Running == nil || (!matches(release.ImageDigest) && !matches(release.PlatformDigest)) {
			return false
		}
		n++
	}
	return podReady && n == 1
}
