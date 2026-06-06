package observability

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKubernetesLogCollectorInjectsIdentityAndDeduplicates(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:                       true,
		QueueSize:                     4,
		MemoryLimitBytes:              4096,
		KubernetesLogsEnabled:         true,
		KubernetesLogMaxLinesPerCycle: 10,
		MaxPayloadBytes:               1024,
	}, nil)
	pipeline.ctx = context.Background()
	collector := newKubernetesLogCollectorWithClient(pipeline, nil)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fg-tenant",
			Name:      "demo-abc",
			Labels: map[string]string{
				kubernetesLabelFugueTenantID:  "tenant_123",
				kubernetesLabelFugueProjectID: "project_123",
				kubernetesLabelFugueAppID:     "app_123",
				kubernetesLabelComponent:      "runtime",
			},
		},
	}
	line := "2026-06-06T01:02:03Z request finished\n"

	if got := collector.ingestLogStream(context.Background(), strings.NewReader(line), pod, "app", 10); got != 1 {
		t.Fatalf("expected one ingested line, got %d", got)
	}
	if got := collector.ingestLogStream(context.Background(), strings.NewReader(line), pod, "app", 10); got != 0 {
		t.Fatalf("expected duplicate line to be skipped, got %d", got)
	}
	event := <-pipeline.queue
	if event.Timestamp.IsZero() || event.Timestamp.Format(time.RFC3339) != "2026-06-06T01:02:03Z" {
		t.Fatalf("expected Kubernetes timestamp to be preserved, got %s", event.Timestamp)
	}
	for key, want := range map[string]string{
		"namespace":  "fg-tenant",
		"pod":        "demo-abc",
		"container":  "app",
		"tenant_id":  "tenant_123",
		"project_id": "project_123",
		"app_id":     "app_123",
		"component":  "runtime",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
	if event.Message != "request finished" {
		t.Fatalf("unexpected message: %q", event.Message)
	}
	if pipeline.Snapshot().KubernetesLogLines != 1 {
		t.Fatalf("expected Kubernetes line counter to increase: %+v", pipeline.Snapshot())
	}
}

func TestKubernetesLogNamespaceFilter(t *testing.T) {
	exact := []string{"fugue-system"}
	prefixes := []string{"fg-"}
	for _, namespace := range []string{"fugue-system", "fg-tenant"} {
		if !kubernetesLogNamespaceAllowed(namespace, exact, prefixes) {
			t.Fatalf("expected namespace %q to be allowed", namespace)
		}
	}
	if kubernetesLogNamespaceAllowed("kube-system", exact, prefixes) {
		t.Fatal("expected kube-system to be filtered")
	}
}

func TestKubernetesLogAttributesUseOwnerAppForBackingService(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fg-tenant",
			Name:      "postgres-1",
			Labels: map[string]string{
				kubernetesLabelFugueTenantID:      "tenant_123",
				kubernetesLabelFugueProjectID:     "project_123",
				kubernetesLabelFugueOwnerAppID:    "app_123",
				kubernetesLabelBackingServiceType: "postgres",
			},
		},
	}
	attrs := kubernetesLogAttributes(pod, "postgres")
	if attrs["app_id"] != "app_123" || attrs["component"] != "postgres" {
		t.Fatalf("expected owner app and component attrs, got %+v", attrs)
	}
}

func TestKubernetesContainerHasLogsSkipsTerminatedContainers(t *testing.T) {
	pod := corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"},
					},
				},
			},
		},
	}
	if kubernetesContainerHasLogs(pod, "app") {
		t.Fatal("expected terminated container to be skipped by the live log collector")
	}
}

func TestBenignKubernetesLogReadErrorsAreIgnored(t *testing.T) {
	for _, message := range []string{
		`pods "old-pod" not found`,
		`container "app" in pod "old-pod" is terminated`,
		`container "app" is waiting to start`,
	} {
		if !isBenignKubernetesLogReadError(errString(message)) {
			t.Fatalf("expected benign error to be recognized: %s", message)
		}
	}
}

type errString string

func (e errString) Error() string {
	return string(e)
}
