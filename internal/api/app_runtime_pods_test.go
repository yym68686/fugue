package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetAppRuntimePodsReturnsReplicaSetHistory(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	currentPod := fakePod("demo-8c9f6d74f7-abc12", "Running", time.Date(2026, 4, 15, 1, 5, 0, 0, time.UTC), containerName)
	currentPod.Metadata.Namespace = namespace
	currentPod.Metadata.OwnerReferences = []struct {
		Kind string `json:"kind,omitempty"`
		Name string `json:"name,omitempty"`
	}{{Kind: "ReplicaSet", Name: "demo-8c9f6d74f7"}}
	currentPod.Spec.NodeName = "gcp1"
	currentPod.Spec.Containers[0].Image = "ghcr.io/example/demo:v2"
	currentPod.Status.PodIP = "10.42.0.12"
	currentPod.Status.HostIP = "10.0.0.5"
	currentPod.Status.QOSClass = "Burstable"
	startedAt := time.Date(2026, 4, 15, 1, 5, 1, 0, time.UTC)
	currentPod.Status.StartTime = &startedAt
	currentPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:         containerName,
		Image:        "ghcr.io/example/demo:v2",
		Ready:        true,
		RestartCount: 1,
		State:        kubeRuntimeState{Running: &struct{}{}},
	}}
	fake.setPods(selector, []kubePodInfo{currentPod})

	oldReplicas := int32(0)
	newReplicas := int32(1)
	fake.setReplicaSets(selector, []appsv1.ReplicaSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "demo-7b7d9f8c5d",
				Namespace:         namespace,
				CreationTimestamp: metav1.NewTime(time.Date(2026, 4, 15, 0, 55, 0, 0, time.UTC)),
				Annotations:       map[string]string{"deployment.kubernetes.io/revision": "12"},
				OwnerReferences:   []metav1.OwnerReference{{Kind: "Deployment", Name: runtime.RuntimeResourceName(app.Name)}},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &oldReplicas,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: containerName, Image: "ghcr.io/example/demo:v1"}},
					},
				},
			},
			Status: appsv1.ReplicaSetStatus{
				Replicas:          0,
				ReadyReplicas:     0,
				AvailableReplicas: 0,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "demo-8c9f6d74f7",
				Namespace:         namespace,
				CreationTimestamp: metav1.NewTime(time.Date(2026, 4, 15, 1, 5, 0, 0, time.UTC)),
				Annotations:       map[string]string{"deployment.kubernetes.io/revision": "13"},
				OwnerReferences:   []metav1.OwnerReference{{Kind: "Deployment", Name: runtime.RuntimeResourceName(app.Name)}},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &newReplicas,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: containerName, Image: "ghcr.io/example/demo:v2"}},
					},
				},
			},
			Status: appsv1.ReplicaSetStatus{
				Replicas:          1,
				ReadyReplicas:     1,
				AvailableReplicas: 1,
			},
		},
	})
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-pods", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response model.AppRuntimePodInventory
	mustDecodeJSON(t, recorder, &response)
	if response.Component != "app" || response.Namespace != namespace || response.Container != runtime.RuntimeResourceName(app.Name) {
		t.Fatalf("unexpected runtime pod inventory header %+v", response)
	}
	if len(response.Groups) != 2 {
		t.Fatalf("expected two replica set groups, got %+v", response.Groups)
	}
	if response.Groups[0].OwnerName != "demo-8c9f6d74f7" || response.Groups[0].Revision != "13" || len(response.Groups[0].Pods) != 1 {
		t.Fatalf("expected current replica set group first, got %+v", response.Groups[0])
	}
	if response.Groups[1].OwnerName != "demo-7b7d9f8c5d" || response.Groups[1].Revision != "12" || len(response.Groups[1].Pods) != 0 {
		t.Fatalf("expected old replica set context without live pods, got %+v", response.Groups[1])
	}
}
