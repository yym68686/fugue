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

func TestKubernetesContainerHasLogsIncludesTerminatedContainers(t *testing.T) {
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
	if !kubernetesContainerHasLogs(pod, "app") {
		t.Fatal("expected terminated container logs to remain eligible for collection")
	}
}

func TestKubernetesLogCollectorKeepsNewestLinesWhenCapped(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:                       true,
		QueueSize:                     4,
		MemoryLimitBytes:              4096,
		KubernetesLogsEnabled:         true,
		KubernetesLogMaxLinesPerCycle: 2,
		MaxPayloadBytes:               1024,
	}, nil)
	pipeline.ctx = context.Background()
	collector := newKubernetesLogCollectorWithClient(pipeline, nil)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fg-tenant",
			Name:      "demo-abc",
		},
	}
	lines := strings.Join([]string{
		"2026-06-06T01:02:01Z old-one",
		"2026-06-06T01:02:02Z old-two",
		"2026-06-06T01:02:03Z keep-one",
		"2026-06-06T01:02:04Z keep-two",
	}, "\n") + "\n"

	if got := collector.ingestLogStream(context.Background(), strings.NewReader(lines), pod, "app", 2); got != 2 {
		t.Fatalf("expected two ingested lines, got %d", got)
	}
	first := <-pipeline.queue
	second := <-pipeline.queue
	if first.Message != "keep-one" || second.Message != "keep-two" {
		t.Fatalf("expected newest capped lines to be ingested in order, got %q then %q", first.Message, second.Message)
	}
}

func TestKubernetesLogTailLinesForRequestUsesFairShareCap(t *testing.T) {
	cfg := Config{KubernetesLogTailLines: 2000}
	if got := kubernetesLogTailLinesForRequest(cfg, 125); got != 125 {
		t.Fatalf("expected request TailLines to use fair-share cap, got %d", got)
	}
	if got := kubernetesLogTailLinesForRequest(cfg, 5000); got != 2000 {
		t.Fatalf("expected request TailLines to keep configured cap, got %d", got)
	}
	if got := kubernetesLogTailLinesForRequest(Config{}, 3); got != 3 {
		t.Fatalf("expected default TailLines to still respect maxLines, got %d", got)
	}
	if got := kubernetesLogTailLinesForRequest(cfg, 0); got != 0 {
		t.Fatalf("expected zero maxLines to disable log request, got %d", got)
	}
}

func TestKubernetesLogCollectorRetainsPriorityRequestFactsWhenCapped(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:                       true,
		QueueSize:                     4,
		MemoryLimitBytes:              4096,
		KubernetesLogsEnabled:         true,
		KubernetesLogMaxLinesPerCycle: 1,
		MaxPayloadBytes:               2048,
	}, nil)
	pipeline.ctx = context.Background()
	collector := newKubernetesLogCollectorWithClient(pipeline, nil)
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fg-tenant",
			Name:      "edge-abc",
		},
	}
	lines := strings.Join([]string{
		`2026-06-06T01:02:01Z {"event_type":"request_fact","trace_id":"trace_123","request_id":"req_123","summary_json":"{\"edge_request_id\":\"edge_req_123\"}"}`,
		"2026-06-06T01:02:02Z noisy-one",
		"2026-06-06T01:02:03Z noisy-two",
	}, "\n") + "\n"

	if got := collector.ingestLogStream(context.Background(), strings.NewReader(lines), pod, "edge", 1); got != 2 {
		t.Fatalf("expected priority request fact plus newest capped line, got %d", got)
	}
	first := <-pipeline.queue
	second := <-pipeline.queue
	if first.Attributes["event_type"] != "request_fact" || first.Attributes["trace_id"] != "trace_123" {
		t.Fatalf("expected priority request_fact first, got %+v", first)
	}
	if second.Message != "noisy-two" {
		t.Fatalf("expected newest non-priority line second, got %q", second.Message)
	}
}

func TestKubernetesLogCollectorPriorityPassOnlyIngestsStructuredDataPlaneFacts(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        8,
		MemoryLimitBytes: 8192,
		MaxPayloadBytes:  2048,
	}, nil)
	pipeline.ctx = context.Background()
	collector := newKubernetesLogCollectorWithClient(pipeline, nil)
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "fugue-system", Name: "edge-worker-a"}}
	lines := strings.Join([]string{
		"2026-06-06T01:02:00Z noisy edge request text",
		`2026-06-06T01:02:01Z {"event_type":"request_fact","trace_id":"trace_webhook","request_id":"req_webhook","path_template":"/v1","summary_json":"{\"path\":\"/v1/webhook/provider\"}"}`,
		"2026-06-06T01:02:02Z another noisy edge request text",
		`2026-06-06T01:02:03Z {"event_type":"request_fact","trace_id":"trace_status","request_id":"req_status","path_template":"/v1","summary_json":"{\"path\":\"/v1/billing/topup/status?...\"}"}`,
	}, "\n") + "\n"

	result := collector.ingestLogStreamMode(context.Background(), strings.NewReader(lines), pod, "edge", 10, true)
	if result.scanned != 4 || result.ingested != 2 {
		t.Fatalf("expected four scanned lines and two priority facts, got %+v", result)
	}
	first := <-pipeline.queue
	second := <-pipeline.queue
	if first.Attributes["trace_id"] != "trace_webhook" || second.Attributes["trace_id"] != "trace_status" {
		t.Fatalf("unexpected priority facts: first=%+v second=%+v", first, second)
	}
}

func TestKubernetesLogPriorityTargetUsesExplicitDataPlaneMetadata(t *testing.T) {
	if kubernetesPublicDataPlaneSelector != "fugue.io/rollout-subsystem=public-data-plane" {
		t.Fatalf("unexpected priority Kubernetes label selector %q", kubernetesPublicDataPlaneSelector)
	}
	priority := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{kubernetesLabelRolloutSubsystem: kubernetesPublicDataPlane}}}
	if !kubernetesLogPriorityTarget(kubernetesLogTarget{pod: priority, container: "edge"}) {
		t.Fatal("expected public data-plane pod to use the priority collector")
	}
	normal := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{kubernetesLabelComponent: "runtime"}}}
	if kubernetesLogPriorityTarget(kubernetesLogTarget{pod: normal, container: "app"}) {
		t.Fatal("ordinary runtime pod must remain on the standard collector only")
	}
	if kubernetesLogPriorityTarget(kubernetesLogTarget{pod: priority, container: "caddy"}) {
		t.Fatal("non-producing data-plane sidecars must not consume the priority line budget")
	}
}

func TestKubernetesLogPriorityFairShareIsIndependentFromClusterPodCount(t *testing.T) {
	pipeline := NewPipeline(Config{
		KubernetesLogTailLines:        2000,
		KubernetesLogMaxLinesPerCycle: 20000,
	}, nil)
	collector := newKubernetesLogCollectorWithClient(pipeline, nil)
	if got := collector.kubernetesLogLinesPerContainer(600); got != 34 {
		t.Fatalf("expected cluster-wide fair share of 34 lines, got %d", got)
	}
	if got := collector.kubernetesLogLinesPerContainer(6); got != 2000 {
		t.Fatalf("expected six priority targets to retain the full 2000-line tail, got %d", got)
	}
	pipeline.kubernetesPriorityLines.Store(12)
	pipeline.kubernetesPriorityTruncations.Store(1)
	pipeline.kubernetesPriorityTargets.Store(6)
	metrics := pipeline.PrometheusMetrics()
	for _, want := range []string{
		"fugue_telemetry_pipeline_kubernetes_priority_log_lines_total 12",
		"fugue_telemetry_pipeline_kubernetes_priority_log_truncations_total 1",
		"fugue_telemetry_pipeline_kubernetes_priority_log_targets 6",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("expected priority collector metric %q, got:\n%s", want, metrics)
		}
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
