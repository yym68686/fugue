package observability

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCursorReadsFrontOfBacklogAndEqualTimestampOccurrences(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	line := ts.Format(time.RFC3339Nano) + " repeated\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("tailLines") || !r.URL.Query().Has("sinceTime") {
			t.Error("expected inclusive cursor, not newest tail", r.URL)
		}
		fmt.Fprint(w, strings.Repeat(line, 5))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(Config{Enabled: true, QueueSize: 32, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 2, KubernetesLogMaxLinesPerCycle: 2, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	for i := 0; i < 4; i++ {
		var b atomic.Int64
		b.Store(2)
		c.collectCursorTarget(t.Context(), target, &b)
	}
	if got := p.Snapshot().KubernetesLogLines; got != 5 {
		t.Fatalf("lost or duplicated equal-time lines: %d", got)
	}
}
func TestCursorDoesNotAdvanceOverQueueRejection(t *testing.T) {
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s one\n%s two\n", ts.Format(time.RFC3339Nano), ts.Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 1, MemoryLimitBytes: 1 << 20, KubernetesLogTailLines: 10, KubernetesLogPollInterval: time.Second}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	var budget atomic.Int64
	budget.Store(10)
	c.collectCursorTarget(t.Context(), target, &budget)
	if !c.cursors[logTargetKey(target)].Time.Equal(ts) {
		t.Fatal("cursor skipped rejected record")
	}
}

func TestCursorDoesNotRepollFinishedContainersOrCountOldUnavailableLogs(t *testing.T) {
	var calls atomic.Int32
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprintf(w, "%s final message\n", ts.Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true, QueueSize: 32}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", ContainerID: "instance", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(ts)}}}}}}, container: "app"}
	var budget atomic.Int64
	budget.Store(20)
	c.collectCursorTarget(t.Context(), target, &budget)
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 || p.Snapshot().KubernetesLogLines != 1 {
		t.Fatal("drained terminated container was reread")
	}
	target.pod.Status.ContainerStatuses[0].ContainerID = "old-instance"
	target.pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(ts.Add(-time.Hour))
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 {
		t.Fatal("queried terminated instance outside bootstrap window")
	}
}

func TestCursorUnavailableLogBodyIsCountedOnceAndBacksOff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, "unable to retrieve container logs for containerd://expired")
	}))
	defer server.Close()
	client, _ := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	p := NewPipeline(Config{Enabled: true}, nil)
	c := newKubernetesLogCollectorWithClient(p, client)
	target := kubernetesLogTarget{pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}}, container: "app"}
	var budget atomic.Int64
	budget.Store(20)
	c.collectCursorTarget(t.Context(), target, &budget)
	c.collectCursorTarget(t.Context(), target, &budget)
	if calls.Load() != 1 || p.kubernetesLogCursorGaps.Load() != 1 || p.kubernetesLogLines.Load() != 0 {
		t.Fatal("unavailable body replayed or ingested")
	}
}
