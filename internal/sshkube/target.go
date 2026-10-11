package sshkube

import (
	"context"
	"fmt"
	"sort"

	"fugue/internal/managedssh"
	"fugue/internal/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

func ResolveTarget(ctx context.Context, client kubernetes.Interface, tenantID, appID, namespace, serviceName string, spec model.ManagedSSHIntentSpec, previous managedssh.Target) (managedssh.Target, error) {
	if client == nil || tenantID == "" || appID == "" || namespace == "" || serviceName == "" || managedssh.ValidateIntent(spec) != nil {
		return managedssh.Target{}, managedssh.ErrConfiguration
	}
	service, err := client.CoreV1().Services(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return managedssh.Target{}, err
	}
	if service.DeletionTimestamp != nil || service.UID == "" || service.Spec.Type == corev1.ServiceTypeExternalName || len(service.Spec.Selector) == 0 || service.Labels["fugue.pro/app-id"] != appID || service.Labels["fugue.pro/tenant-id"] != tenantID {
		return managedssh.Target{}, managedssh.ErrUnauthorized
	}
	endpoints, err := client.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"kubernetes.io/service-name": serviceName}.String(), Limit: 128})
	if err != nil {
		return managedssh.Target{}, err
	}
	if endpoints.Continue != "" {
		return managedssh.Target{}, fmt.Errorf("managed SSH target discovery exceeds the bounded endpoint set")
	}
	selector := labels.SelectorFromSet(service.Spec.Selector)
	potential := map[string]string{}
	for _, slice := range endpoints.Items {
		owned := false
		for _, owner := range slice.OwnerReferences {
			if owner.Kind == "Service" && owner.APIVersion == "v1" && owner.Name == service.Name && owner.UID == service.UID {
				owned = true
			}
		}
		if !owned {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || (endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) || (endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving) || len(endpoint.Addresses) == 0 {
				continue
			}
			ref := endpoint.TargetRef
			if ref == nil || ref.Kind != "Pod" || ref.Namespace != namespace || ref.Name == "" || ref.UID == "" {
				continue
			}
			if old, exists := potential[ref.Name]; exists && old != string(ref.UID) {
				return managedssh.Target{}, managedssh.ErrUnavailable
			}
			potential[ref.Name] = string(ref.UID)
		}
	}
	if len(potential) > 128 {
		return managedssh.Target{}, fmt.Errorf("managed SSH target discovery exceeds the bounded pod set")
	}
	names := make([]string, 0, len(potential))
	for name := range potential {
		names = append(names, name)
	}
	sort.Strings(names)
	var selected managedssh.Target
	for _, name := range names {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return managedssh.Target{}, err
		}
		if string(pod.UID) != potential[name] || pod.Status.Phase != corev1.PodRunning || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready {
			continue
		}
		target := managedssh.Target{TenantID: tenantID, AppID: appID, Namespace: namespace, PodName: pod.Name, PodUID: string(pod.UID), Container: spec.Container, User: spec.User, WorkingDirectory: spec.WorkingDirectory}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == spec.Container && status.Ready {
				target.ContainerID = status.ContainerID
			}
		}
		if target.ContainerID == "" || validatePodTarget(pod, target) != nil {
			continue
		}
		if target == previous {
			return target, nil
		}
		if selected.PodUID == "" {
			selected = target
		}
	}
	if selected.PodUID == "" {
		return managedssh.Target{}, managedssh.ErrUnavailable
	}
	return selected, nil
}
