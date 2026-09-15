package controller

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const managedAppVolumeBlockGracePeriod = 2 * time.Minute

// Mount failures are often transient (for example while CSI detaches an old
// volume). Fail only after the same current pod remains blocked across the
// grace period. A replacement pod or successful sandbox creation resets it.
type rolloutVolumeBlockTracker struct {
	podIdentity     string
	podName         string
	firstObservedAt time.Time
}

func (t *rolloutVolumeBlockTracker) observe(now time.Time, identity, message string) error {
	if message == "" || identity == "" {
		*t = rolloutVolumeBlockTracker{}
		return nil
	}
	if t.podIdentity != identity || t.firstObservedAt.IsZero() || now.Before(t.firstObservedAt) {
		*t = rolloutVolumeBlockTracker{podIdentity: identity, podName: strings.SplitN(identity, "/", 2)[0], firstObservedAt: now}
		return nil
	}
	if now.Sub(t.firstObservedAt) < managedAppVolumeBlockGracePeriod {
		return nil
	}
	return fmt.Errorf("rollout volume attachment blocked for %s: %s", managedAppVolumeBlockGracePeriod, message)
}

func deploymentVolumeBlockMessage(ctx context.Context, client *kubeClient, namespace string, pods []kubePod, deployment kubeDeployment, now time.Time) (string, string) {
	for _, pod := range pods {
		if !podHasDeploymentTemplateIdentity(pod, deployment) || !podWaitingForVolumes(pod) {
			continue
		}
		events, err := client.listEventsForObject(ctx, namespace, "Pod", pod.Metadata.Name)
		if err != nil {
			// Observability failure is not evidence of an application failure.
			// The normal rollout deadline still bounds the wait.
			continue
		}
		if message := podVolumeMountFailureMessage(pod, events, now); message != "" {
			return pod.Metadata.Name + "/" + pod.ObservedUID, message
		}
	}
	return "", ""
}

func podWaitingForVolumes(pod kubePod) bool {
	if !strings.EqualFold(pod.Status.Phase, "Pending") || pod.Metadata.DeletionTimestamp != "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == "PodReadyToStartContainers" && condition.Status == "True" {
			return false
		}
	}
	for _, statuses := range [][]kubeContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.State.Running != nil || status.State.Terminated != nil || status.LastState.Terminated != nil {
				return false
			}
		}
	}
	return true
}

func podVolumeMountFailureMessage(pod kubePod, events []kubeEvent, now time.Time) string {
	if !podWaitingForVolumes(pod) || pod.ObservedUID == "" {
		return ""
	}
	// Require current pod identity and recent events. Event names and pod names
	// alone are insufficient after recreation, and historical warnings must not
	// fail a rollout that has already recovered.
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.InvolvedObject.UID != pod.ObservedUID || event.InvolvedObject.Name != pod.Metadata.Name || event.InvolvedObject.Kind != "Pod" {
			continue
		}
		at := kubeEventTime(event)
		if at.IsZero() || at.Before(pod.Metadata.CreationTimestamp) || now.Sub(at) > 5*time.Minute || at.After(now.Add(time.Minute)) {
			continue
		}
		switch event.Reason {
		case "SuccessfulAttachVolume", "SuccessfulMountVolume":
			return ""
		case "FailedMount", "FailedAttachVolume":
			return fmt.Sprintf("pod %s on node %s: %s: %s", pod.Metadata.Name, pod.Spec.NodeName, event.Reason, event.Message)
		}
	}
	return ""
}
