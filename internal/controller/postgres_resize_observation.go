package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	managedPostgresMainContainerName   = "postgres"
	kubeStrategicMergePatchContentType = "application/strategic-merge-patch+json"
)

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

type kubeResizePodPatch struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		Containers []kubeResizeContainerSpec `json:"containers"`
	} `json:"spec"`
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

// patchPodContainerResources submits only CPU and memory resources through the
// Kubernetes Pod resize subresource. It deliberately requires the observed
// resourceVersion so a stale controller observation fails with a conflict
// instead of resizing a replacement Pod or overwriting a concurrent request.
// Callers must separately prove that the target workload and resize policy are
// eligible; this method is the narrow Kubernetes transport primitive only.
func (c *kubeClient) patchPodContainerResources(
	ctx context.Context,
	namespace, podName, resourceVersion, containerName string,
	resources kubeResourceRequirements,
) (kubeResizePod, error) {
	namespace = strings.TrimSpace(namespace)
	podName = strings.TrimSpace(podName)
	resourceVersion = strings.TrimSpace(resourceVersion)
	containerName = strings.TrimSpace(containerName)
	if namespace == "" || podName == "" || resourceVersion == "" || containerName == "" {
		return kubeResizePod{}, fmt.Errorf("pod resize requires namespace, pod name, resource version, and container name")
	}
	if err := validatePodResizeResources(resources); err != nil {
		return kubeResizePod{}, err
	}

	var patch kubeResizePodPatch
	patch.Metadata.ResourceVersion = resourceVersion
	patch.Spec.Containers = []kubeResizeContainerSpec{{
		Name:      containerName,
		Resources: cloneKubeResourceRequirements(resources),
	}}
	payload, err := json.Marshal(patch)
	if err != nil {
		return kubeResizePod{}, fmt.Errorf("marshal pod resize request: %w", err)
	}
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(podName) + "/resize"
	status, responseBody, err := c.doRaw(ctx, http.MethodPatch, apiPath, bytes.NewReader(payload), kubeStrategicMergePatchContentType)
	if err != nil {
		if status == http.StatusConflict {
			return kubeResizePod{}, fmt.Errorf("%w: %v", errKubeConflict, err)
		}
		return kubeResizePod{}, err
	}
	var pod kubeResizePod
	if err := json.Unmarshal(responseBody, &pod); err != nil {
		return kubeResizePod{}, fmt.Errorf("decode pod resize response: %w", err)
	}
	return pod, nil
}

func validatePodResizeResources(resources kubeResourceRequirements) error {
	if len(resources.Requests) == 0 && len(resources.Limits) == 0 {
		return fmt.Errorf("pod resize requires at least one CPU or memory request or limit")
	}
	quantities := map[string]map[string]resource.Quantity{
		"requests": {},
		"limits":   {},
	}
	for field, values := range map[string]map[string]string{
		"requests": resources.Requests,
		"limits":   resources.Limits,
	} {
		for resourceName, raw := range values {
			resourceName = strings.TrimSpace(resourceName)
			if resourceName != "cpu" && resourceName != "memory" {
				return fmt.Errorf("pod resize only supports CPU and memory, got %s.%s", field, resourceName)
			}
			quantity, err := resource.ParseQuantity(strings.TrimSpace(raw))
			if err != nil {
				return fmt.Errorf("parse pod resize %s.%s %q: %w", field, resourceName, raw, err)
			}
			if quantity.Sign() <= 0 {
				return fmt.Errorf("pod resize %s.%s must be positive", field, resourceName)
			}
			quantities[field][resourceName] = quantity
		}
	}
	for _, resourceName := range []string{"cpu", "memory"} {
		request, hasRequest := quantities["requests"][resourceName]
		limit, hasLimit := quantities["limits"][resourceName]
		if hasRequest && hasLimit && request.Cmp(limit) > 0 {
			return fmt.Errorf("pod resize %s request must not exceed its limit", resourceName)
		}
	}
	return nil
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
