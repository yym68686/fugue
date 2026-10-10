package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestServingFrontCohortUsesTransportSelectors(t *testing.T) {
	front := func(name, node, role string, hostPort int32) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), Labels: map[string]string{"role": role}},
			Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "edge-front", Ports: []corev1.ContainerPort{{ContainerPort: 443, HostPort: hostPort}}}}}}
	}
	pods := []corev1.Pod{front("legacy-a", "node-a", "legacy", 443), front("old-a", "node-a", "old", 0),
		front("selected-a", "node-a", "selected", 0), front("selected-b", "node-b", "selected", 0),
		front("legacy-b", "node-b", "legacy", 443)}
	services := []corev1.Service{{Spec: corev1.ServiceSpec{ExternalIPs: []string{"192.0.2.1"}, Selector: map[string]string{"role": "selected"}}}}
	selected, err := selectServingFrontPods(pods, services, "192.0.2.1:443", 2)
	if err != nil || len(selected) != 2 || selected[0].Name != "selected-a" || selected[1].Name != "selected-b" {
		t.Fatalf("serving transport did not isolate current executors: %+v %v", selected, err)
	}
	for name, changed := range map[string][]corev1.Pod{
		"missing":                 pods[:3],
		"duplicate":               append(append([]corev1.Pod(nil), pods...), front("duplicate", "node-a", "selected", 0)),
		"empty managed selection": {pods[0], pods[4]},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := selectServingFrontPods(changed, services, "192.0.2.1:443", 2); err == nil {
				t.Fatal("ambiguous or absent serving executor accepted")
			}
		})
	}
	selected, err = selectServingFrontPods(pods, nil, "192.0.2.1:443", 2)
	if err != nil || selected[0].Name != "legacy-a" || selected[1].Name != "legacy-b" {
		t.Fatalf("host port transport was not preserved: %+v %v", selected, err)
	}
}
