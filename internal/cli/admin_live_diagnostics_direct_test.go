package cli

import (
	"context"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDirectPlatformDiagnosticsFreezesTrustedTargetAndRunner(t *testing.T) {
	t.Parallel()
	digest := "sha256:" + strings.Repeat("a", 64)
	objects := []runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "fugue-fugue-api", Namespace: "fugue-system"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "registry.example/api@" + digest}}}}}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "fugue-system", UID: types.UID("pod-uid"), Labels: map[string]string{"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "api"}},
			Spec:       corev1.PodSpec{NodeName: "control-1", Containers: []corev1.Container{{Name: "api", Image: "registry.example/api@" + digest}}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "api", Ready: true, ContainerID: "containerd://abcdef1234567890", ImageID: "registry.example/api@" + digest}}},
		},
	}
	client := &directPlatformDiagnosticClient{client: fake.NewSimpleClientset(objects...), controlNS: "fugue-system", releaseInstance: "fugue"}
	response, err := client.Start(context.Background(), platformDiagnosticStartRequest{
		Target: platformDiagnosticTargetRequest{Type: livediagnostics.TargetPlatformComponent, Component: "api"},
		Kind:   livediagnostics.ProbeMemoryProfile, DurationSeconds: 300, SampleIntervalMilliseconds: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Session.ControlPath != "direct-kubernetes" || response.Session.Target.PodUID != "pod-uid" {
		t.Fatalf("unexpected session: %+v", response.Session)
	}
	job, err := client.client.BatchV1().Jobs("fugue-system").Get(context.Background(), response.Session.ID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Template.Spec.NodeName != "control-1" || job.Annotations[livediagnostics.TargetContainerIDAnnotation] != "containerd://abcdef1234567890" || job.Spec.Template.Spec.Containers[0].Image != "registry.example/api@"+digest {
		t.Fatalf("unexpected direct diagnostic job: %+v", job)
	}
}

func TestDirectPlatformDiagnosticsRejectsUntrustedHostProcess(t *testing.T) {
	t.Parallel()
	client := &directPlatformDiagnosticClient{client: fake.NewSimpleClientset(), controlNS: "fugue-system", releaseInstance: "fugue"}
	_, err := client.resolveTarget(context.Background(), platformDiagnosticTargetRequest{Type: livediagnostics.TargetNodeProcess, Node: "node-1", ProcessName: "bash"})
	if err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("expected allowlist error, got %v", err)
	}
}
