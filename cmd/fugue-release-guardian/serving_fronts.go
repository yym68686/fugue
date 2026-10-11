package main

import (
	"context"
	"errors"
	"net"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (activator *frontAuthorityActivator) servingFrontPods(ctx context.Context) ([]corev1.Pod, error) {
	selector := labels.Set{"fugue.io/edge-group-id": activator.config.GroupID}.AsSelector().String()
	pods, err := activator.client.CoreV1().Pods(activator.config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 1601})
	if err != nil || pods.Continue != "" {
		return nil, errors.New("Front cohort is unavailable")
	}
	services, err := activator.client.CoreV1().Services(activator.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/managed-by=fugue-front-serving-transport", Limit: 301,
	})
	if err != nil || services.Continue != "" {
		return nil, errors.New("Front serving transport is unavailable")
	}
	var admitted map[string]bool
	if activator.cohort != nil {
		admitted = activator.cohort.Admitted
	}
	return selectServingFrontPodsWithMembers(activator.cohortPods(pods.Items), services.Items, activator.config.RouteAddress, activator.config.ExpectedNodes, admitted)
}

func selectServingFrontPods(pods []corev1.Pod, services []corev1.Service, routeAddress string, expectedNodes int) ([]corev1.Pod, error) {
	return selectServingFrontPodsWithMembers(pods, services, routeAddress, expectedNodes, nil)
}

func selectServingFrontPodsWithMembers(pods []corev1.Pod, services []corev1.Service, routeAddress string, expectedNodes int, admitted map[string]bool) ([]corev1.Pod, error) {
	routeHost, _, _ := net.SplitHostPort(routeAddress)
	managedRoute := false
	for _, service := range services {
		for _, address := range service.Spec.ExternalIPs {
			managedRoute = managedRoute || address == routeHost
		}
	}
	var selected, legacy []corev1.Pod
	for _, pod := range pods {
		isFront, hostFront := false, false
		for _, container := range pod.Spec.Containers {
			if container.Name != "edge-front" {
				continue
			}
			isFront = true
			for _, port := range container.Ports {
				hostFront = hostFront || port.HostPort == 443
			}
		}
		if !isFront {
			continue
		}
		if hostFront {
			legacy = append(legacy, pod)
		}
		for _, service := range services {
			if len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(pod.Labels)) {
				selected = append(selected, pod)
				break
			}
		}
	}
	if len(selected) == 0 && !managedRoute {
		selected = legacy
	} else if len(admitted) > 0 {
		selectedNodes := map[string]bool{}
		for _, pod := range selected {
			selectedNodes[pod.Spec.NodeName] = true
		}
		for _, pod := range legacy {
			if admitted[pod.Spec.NodeName] && !selectedNodes[pod.Spec.NodeName] {
				selected = append(selected, pod)
			}
		}
	}
	if len(selected) != expectedNodes || expectedNodes < 1 {
		return nil, errors.New("Front serving cohort size is invalid")
	}
	seenNodes := make(map[string]bool, expectedNodes)
	for _, pod := range selected {
		if pod.Spec.NodeName == "" || seenNodes[pod.Spec.NodeName] || pod.DeletionTimestamp != nil || pod.UID == "" {
			return nil, errors.New("Front serving cohort identity is ambiguous")
		}
		seenNodes[pod.Spec.NodeName] = true
	}
	return selected, nil
}
