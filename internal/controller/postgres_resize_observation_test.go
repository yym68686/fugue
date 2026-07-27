package controller

import (
	"encoding/json"
	"testing"
)

func TestObserveManagedPostgresResizeUsesActualContainerResources(t *testing.T) {
	var pod kubeResizePod
	if err := json.Unmarshal([]byte(`{
		"metadata":{"namespace":"tenant-a","name":"database-1","uid":"pod-uid","resourceVersion":"42","generation":7},
		"spec":{"nodeName":"worker-a","containers":[{"name":"postgres","resources":{"requests":{"cpu":"100m","memory":"512Mi"},"limits":{"cpu":"500m","memory":"1Gi"}},"resizePolicy":[{"resourceName":"cpu","restartPolicy":"NotRequired"},{"resourceName":"memory","restartPolicy":"NotRequired"}]}]},
		"status":{"observedGeneration":7,"phase":"Running","conditions":[{"type":"Ready","status":"True"},{"type":"PodResizePending","status":"True","reason":"Deferred","message":"waiting for node capacity"}],"containerStatuses":[{"name":"postgres","ready":true,"restartCount":0,"resources":{"requests":{"cpu":"150m","memory":"512Mi"},"limits":{"cpu":"500m","memory":"1Gi"}}}]}
	}`), &pod); err != nil {
		t.Fatalf("decode pod: %v", err)
	}

	got, err := observeManagedPostgresResize(pod, managedPostgresMainContainerName)
	if err != nil {
		t.Fatalf("observe resize: %v", err)
	}
	if got.PodUID != "pod-uid" || got.ResourceVersion != "42" || got.Generation != 7 || got.ObservedGeneration != 7 {
		t.Fatalf("unexpected pod identity observation: %+v", got)
	}
	if !got.PodReady || !got.ContainerReady || got.RestartCount != 0 {
		t.Fatalf("unexpected readiness observation: %+v", got)
	}
	if got.DesiredResources.Requests["cpu"] != "100m" {
		t.Fatalf("expected desired CPU from pod spec, got %+v", got.DesiredResources)
	}
	if got.ActualResources == nil || got.ActualResources.Requests["cpu"] != "150m" {
		t.Fatalf("expected actual CPU from container status, got %+v", got.ActualResources)
	}
	if len(got.Conditions) != 1 || got.Conditions[0].Reason != "Deferred" {
		t.Fatalf("expected Deferred resize condition, got %+v", got.Conditions)
	}
	if len(got.ResizePolicy) != 2 || got.ResizePolicy[0].RestartPolicy != "NotRequired" {
		t.Fatalf("unexpected resize policy: %+v", got.ResizePolicy)
	}
}

func TestObserveManagedPostgresResizeRejectsMissingContainerStatus(t *testing.T) {
	pod := kubeResizePod{}
	pod.Metadata.Name = "database-1"
	pod.Spec.Containers = []kubeResizeContainerSpec{{Name: managedPostgresMainContainerName}}
	if _, err := observeManagedPostgresResize(pod, managedPostgresMainContainerName); err == nil {
		t.Fatal("expected missing container status to fail closed")
	}
}
