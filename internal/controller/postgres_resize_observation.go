package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const managedPostgresMainContainerName = "postgres"

type managedPostgresResizeCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

type kubeResizePolicy struct {
	ResourceName  string `json:"resourceName,omitempty"`
	RestartPolicy string `json:"restartPolicy,omitempty"`
}

type kubeResizeContainerSpec struct {
	Name         string                   `json:"name"`
	Resources    kubeResourceRequirements `json:"resources,omitempty"`
	ResizePolicy []kubeResizePolicy       `json:"resizePolicy,omitempty"`
}

type kubeResizePod struct {
	Metadata struct {
		Namespace       string `json:"namespace,omitempty"`
		Name            string `json:"name"`
		UID             string `json:"uid,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
		Generation      int64  `json:"generation,omitempty"`
	} `json:"metadata"`
	Spec struct {
		NodeName   string                    `json:"nodeName,omitempty"`
		Containers []kubeResizeContainerSpec `json:"containers"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64              `json:"observedGeneration,omitempty"`
		Phase              string             `json:"phase,omitempty"`
		Conditions         []kubePodCondition `json:"conditions,omitempty"`
		ContainerStatuses  []struct {
			Name         string                    `json:"name"`
			Ready        bool                      `json:"ready,omitempty"`
			RestartCount int                       `json:"restartCount,omitempty"`
			Resources    *kubeResourceRequirements `json:"resources,omitempty"`
		} `json:"containerStatuses,omitempty"`
	} `json:"status"`
}

type managedPostgresResizeObservation struct {
	Namespace          string
	PodName            string
	PodUID             string
	ResourceVersion    string
	Generation         int64
	ObservedGeneration int64
	NodeName           string
	Phase              string
	PodReady           bool
	ContainerName      string
	ContainerReady     bool
	RestartCount       int
	DesiredResources   kubeResourceRequirements
	ActualResources    *kubeResourceRequirements
	ResizePolicy       []kubeResizePolicy
	Conditions         []managedPostgresResizeCondition
}

func observeManagedPostgresResize(pod kubeResizePod, containerName string) (managedPostgresResizeObservation, error) {
	containerName = strings.TrimSpace(containerName)
	if containerName == "" {
		return managedPostgresResizeObservation{}, fmt.Errorf("managed postgres resize observation requires a container name")
	}

	out := managedPostgresResizeObservation{
		Namespace:          strings.TrimSpace(pod.Metadata.Namespace),
		PodName:            strings.TrimSpace(pod.Metadata.Name),
		PodUID:             strings.TrimSpace(pod.Metadata.UID),
		ResourceVersion:    strings.TrimSpace(pod.Metadata.ResourceVersion),
		Generation:         pod.Metadata.Generation,
		ObservedGeneration: pod.Status.ObservedGeneration,
		NodeName:           strings.TrimSpace(pod.Spec.NodeName),
		Phase:              strings.TrimSpace(pod.Status.Phase),
		ContainerName:      containerName,
	}
	for _, condition := range pod.Status.Conditions {
		conditionType := strings.TrimSpace(condition.Type)
		if conditionType == "Ready" && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			out.PodReady = true
		}
		if conditionType != "PodResizePending" && conditionType != "PodResizeInProgress" {
			continue
		}
		out.Conditions = append(out.Conditions, managedPostgresResizeCondition{
			Type:    conditionType,
			Status:  strings.TrimSpace(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		})
	}

	foundSpec := false
	for _, container := range pod.Spec.Containers {
		if strings.TrimSpace(container.Name) != containerName {
			continue
		}
		foundSpec = true
		out.DesiredResources = cloneKubeResourceRequirements(container.Resources)
		out.ResizePolicy = append([]kubeResizePolicy(nil), container.ResizePolicy...)
		break
	}
	if !foundSpec {
		return managedPostgresResizeObservation{}, fmt.Errorf("managed postgres pod %s does not contain container %s", out.PodName, containerName)
	}

	foundStatus := false
	for _, status := range pod.Status.ContainerStatuses {
		if strings.TrimSpace(status.Name) != containerName {
			continue
		}
		foundStatus = true
		out.ContainerReady = status.Ready
		out.RestartCount = status.RestartCount
		if status.Resources != nil {
			resources := cloneKubeResourceRequirements(*status.Resources)
			out.ActualResources = &resources
		}
		break
	}
	if !foundStatus {
		return managedPostgresResizeObservation{}, fmt.Errorf("managed postgres pod %s does not report status for container %s", out.PodName, containerName)
	}
	return out, nil
}

func (c *kubeClient) getPodResizeState(ctx context.Context, namespace, name string) (kubeResizePod, bool, error) {
	var pod kubeResizePod
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(strings.TrimSpace(name))
	status, err := c.doJSON(ctx, http.MethodGet, apiPath, nil, &pod)
	if err != nil {
		if status == http.StatusNotFound {
			return kubeResizePod{}, false, nil
		}
		return kubeResizePod{}, false, err
	}
	return pod, true, nil
}

func cloneKubeResourceRequirements(in kubeResourceRequirements) kubeResourceRequirements {
	return kubeResourceRequirements{
		Requests: cloneKubeResourceStringMap(in.Requests),
		Limits:   cloneKubeResourceStringMap(in.Limits),
	}
}

func cloneKubeResourceStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
